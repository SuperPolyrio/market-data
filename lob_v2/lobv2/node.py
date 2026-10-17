"""Independent plant processes and gateway, bounded TLS framed transport."""
import asyncio
from contextlib import suppress
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import ssl
import time
import uuid
import resource
from collections import deque

import websockets

from .book import digest, inspect
from .catalog import shards, validate_scope
from .core import Gateway, Plant
from .journal import Journal, atomic_json, read_journal, recover_open
from .wire import MAX_RECORD, SCHEMA_DIGEST, control, decode, encode, json_bytes, read_frame, send
from . import __version__

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def connection_credit(config):
    """Reserve one connection slot across processes using a tiny local file."""
    path = config.get("connection_rate_file")
    if not path:
        return
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    interval = 1/config.get("connections_per_second",10)
    while True:
        with path.open("a+") as file:
            fcntl.flock(file.fileno(),fcntl.LOCK_EX)
            file.seek(0)
            prior = float(file.read() or "0")
            now = time.monotonic()
            if path.stat().st_mtime < time.time()-now or prior-now > 2*interval:
                prior = 0  # previous boot or legacy cancelled reservations
            if prior <= now:
                file.seek(0)
                file.truncate()
                file.write(str(now+interval))
                file.flush()
                return
        # Reserve only the slot actually used. Cancelled connection tasks cannot
        # leave minutes of phantom reservations behind a plant restart.
        await asyncio.sleep(max(.001,prior-now)+random.uniform(0,.1*interval))


def tls_context(config, server=False):
    tls = config.get("tls")
    if not tls:
        if config.get("host", "127.0.0.1") not in ("127.0.0.1", "::1", "localhost"):
            raise ValueError("TLS_REQUIRED_OUTSIDE_LOOPBACK")
        return None
    purpose = ssl.Purpose.CLIENT_AUTH if server else ssl.Purpose.SERVER_AUTH
    ctx = ssl.create_default_context(purpose, cafile=tls["ca"])
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(tls["cert"], tls["key"])
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def transmit_snapshot(writer, reader, snapshot, session, queue=None):
    data = json_bytes(snapshot)
    if len(data) > 64*1024*1024:
        raise BufferError("SNAPSHOT_BUDGET_EXCEEDED")
    chunk_bytes = 256*1024
    chunks = (len(data)+chunk_bytes-1)//chunk_bytes
    header = {"session": session, "snapshot_id": snapshot["snapshot_id"], "bytes": len(data),
              "chunks": chunks, "sha256": hashlib.sha256(data).hexdigest()}
    await send(writer, control("BOOTSTRAP_BEGIN", header))
    for index in range(chunks):
        if queue is not None and queue.failed:
            raise BufferError("BOOTSTRAP_CANCELLED_BUFFER_OVERFLOW")
        await send(writer, encode("BOOTSTRAP_CHUNK", "transport", "", 0,
            {"snapshot_id": snapshot["snapshot_id"], "chunk_index": index},
            data[index*chunk_bytes:(index+1)*chunk_bytes]).frame)
    if queue is not None and queue.failed:
        raise BufferError("BOOTSTRAP_CANCELLED_BUFFER_OVERFLOW")
    await send(writer, control("BOOTSTRAP_END", header))
    ack = decode(await asyncio.wait_for(read_frame(reader), 30))
    if ack.kind != "BOOTSTRAP_ACK" or ack.body != {"session": session, "snapshot_id": snapshot["snapshot_id"]}:
        raise ValueError("INVALID_BOOTSTRAP_ACK")
    if queue is not None and queue.failed:
        raise BufferError("BOOTSTRAP_CANCELLED_BUFFER_OVERFLOW")
    await send(writer, control("BOOTSTRAP_READY",ack.body))


async def confirm_snapshot(reader,writer,snapshot,session):
    identity={"session":session,"snapshot_id":snapshot["snapshot_id"]}
    await send(writer,control("BOOTSTRAP_ACK",identity))
    ready=decode(await asyncio.wait_for(read_frame(reader),30))
    if ready.kind!="BOOTSTRAP_READY" or ready.body!=identity:
        raise ValueError("BOOTSTRAP_CANCELLED")


async def receive_snapshot(reader):
    begin = decode(await asyncio.wait_for(read_frame(reader), 30))
    header = begin.body
    if begin.kind != "BOOTSTRAP_BEGIN" or not 0 < header["bytes"] <= 64*1024*1024 or not 0 < header["chunks"] <= 256:
        raise ValueError("INVALID_BOOTSTRAP_BEGIN")
    data = bytearray()
    for index in range(header["chunks"]):
        rec = decode(await asyncio.wait_for(read_frame(reader), 30))
        if rec.kind != "BOOTSTRAP_CHUNK" or rec.body != {"snapshot_id": header["snapshot_id"], "chunk_index": index}:
            raise ValueError("INCOMPLETE_BOOTSTRAP")
        data.extend(rec.raw)
        if len(data) > header["bytes"]:
            raise ValueError("BOOTSTRAP_OVERSIZE")
    end = decode(await asyncio.wait_for(read_frame(reader), 30))
    if end.kind != "BOOTSTRAP_END" or end.body != header or len(data) != header["bytes"] or hashlib.sha256(data).hexdigest() != header["sha256"]:
        raise ValueError("BOOTSTRAP_DIGEST")
    snapshot = json.loads(data)
    if snapshot["snapshot_id"] != header["snapshot_id"]:
        raise ValueError("SNAPSHOT_ID_MISMATCH")
    return snapshot, header["session"]


async def serve(owner, config, stop):
    active_tasks = set()
    async def client(reader, writer):
        task = asyncio.current_task()
        active_tasks.add(task)
        subscription = None
        try:
            hello = decode(await asyncio.wait_for(read_frame(reader), 10))
            if hello.kind != "HELLO" or hello.body.get("schema") != 2:
                raise ValueError("HELLO_REQUIRED")
            requested_tape = hello.body.get("tape_uuid")
            if requested_tape and requested_tape != owner.journal.stream:
                await send(writer, control("RESET_REQUIRED", {"generation": owner.journal.stream,
                    "reason": "GENERATION_CHANGED", "old_tape_replay": "EXPLICIT_REPLAY_ONLY"}))
                return
            session = str(uuid.uuid4())
            snapshot, subscription = owner.bootstrap(hello.body.get("markets"), config["queue_bytes"])
            await asyncio.wait_for(transmit_snapshot(writer, reader, snapshot, session,subscription["queue"]), 30)
            while not stop.is_set():
                if subscription["queue"].failed:
                    raise BufferError("RESYNC_REQUIRED")
                try:
                    data = await asyncio.wait_for(subscription["queue"].get(), 5)
                except asyncio.TimeoutError:
                    await send(writer, control("KEEPALIVE", {"session": session}))
                    ack = decode(await asyncio.wait_for(read_frame(reader), 15))
                    if ack.kind != "FLOW_ACK" or ack.body.get("session") != session:
                        raise ValueError("STALE_ACK")
                    continue
                batch=[data]
                batch_bytes=len(data)
                # Flow credit covers a bounded batch, avoiding one process/network
                # round trip per event. Frames themselves remain byte-identical.
                while len(batch)<128 and batch_bytes<4*1024*1024:
                    try:
                        following=subscription["queue"].get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    batch.append(following)
                    batch_bytes+=len(following)
                for data in batch:
                    writer.write(data)
                await asyncio.wait_for(send(writer,control("FLOW_BATCH_END",{
                    "session":session,"records":len(batch)})),15)
                ack = decode(await asyncio.wait_for(read_frame(reader), 15))
                if ack.kind != "FLOW_ACK" or ack.body != {"session":session,"records":len(batch)}:
                    raise ValueError("STALE_ACK")
        except (OSError, ValueError, BufferError, asyncio.IncompleteReadError, asyncio.TimeoutError) as exc:
            with suppress(Exception):
                await send(writer, control("RESYNC_REQUIRED", {"reason": str(exc)}))
        finally:
            if subscription is not None:
                owner.subscribers.discard(subscription)
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
            active_tasks.discard(task)
    server = await asyncio.start_server(client, config["host"], config["port"], ssl=tls_context(config, True))
    try:
        async with server:
            await stop.wait()
    finally:
        server.close()
        await server.wait_closed()
        for task in tuple(active_tasks):
            task.cancel()
        await asyncio.gather(*active_tasks, return_exceptions=True)


async def lane_loop(plant, shard, lane, markets, config, stop):
    epoch = 0
    tokens = [t for market in markets for t in market["assets"]]
    while not stop.is_set():
        await connection_credit(config)
        epoch += 1
        connection = str(uuid.uuid4())
        recv_seq = 0
        heartbeat = None
        plant.connect(shard,lane,connection,epoch,markets)
        try:
            proxy = config.get("outbound_proxy", "env")
            if config.get("lane_proxies"):
                proxy = config["lane_proxies"][lane % len(config["lane_proxies"])]
            if proxy == "env":
                proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") or True
            elif proxy == "direct":
                proxy = None
            async with websockets.connect(config.get("ws_url",WS_URL), proxy=proxy,
                    max_size=config["ws_max_bytes"], max_queue=min(16,config["queue_bytes"]//config["ws_max_bytes"]), ping_interval=None,
                    compression="deflate", open_timeout=config.get("open_timeout_seconds",20), close_timeout=5) as ws:
                attempt = str(uuid.uuid4())
                command = {"type": "market", "assets_ids": tokens, "initial_dump": True,
                           "level": 2, "custom_feature_enabled": True}
                plant.journal.append("SUBSCRIPTION_WRITE_START", {"connection": connection,
                    "assets": tokens, "send_attempt_id": attempt})
                await ws.send(json_bytes(command).decode())
                plant.written(shard,lane,connection,tokens,attempt)
                last_pong = time.monotonic()
                last_receive = last_pong
                initial_pong = False
                pending_pings = deque()
                subscribe_mono = time.monotonic()
                async def ping():
                    nonlocal last_pong, subscribe_mono
                    while not stop.is_set():
                        await asyncio.sleep(config["heartbeat_seconds"])
                        timeout = config["pong_timeout_seconds"] if initial_pong else config.get("initial_pong_timeout_seconds",config["pong_timeout_seconds"])
                        # Initial dumps may precede PONG in the upstream send
                        # queue. Received native frames are also transport liveness;
                        # do not discard a progressing multi-chunk baseline.
                        if time.monotonic()-max(last_pong,last_receive) > timeout:
                            await ws.close(code=1011, reason="PONG_TIMEOUT")
                            return
                        proof=plant.transports.get((shard,lane),{})
                        if proof.get("healthy") and (not plant.healthy_transport(shard,lane) or
                                pending_pings and (time.monotonic_ns()-pending_pings[0])/1e6 > config.get("maximum_healthy_rtt_ms",2000)):
                            plant.transport_quality(shard,lane,connection,False,reason="PONG_FRESHNESS_DEADLINE")
                        if (pending_pings and time.monotonic()-subscribe_mono > config.get("backlog_reconnect_grace_seconds",60)
                                and (time.monotonic_ns()-pending_pings[0])/1e9 > config.get("maximum_receive_backlog_seconds",30)):
                            plant.journal.append("BACKLOG_RESYNC",{"connection":connection,
                                "oldest_ping_age_ms":(time.monotonic_ns()-pending_pings[0])/1e6,
                                "continuity":"UNKNOWN_RECONNECT_REQUIRES_NATIVE_BASELINE"})
                            await ws.close(code=1011,reason="RECEIVE_BACKLOG_RESET")
                            return
                        missing = [t for t in tokens if not plant.states[(shard,lane,t)]["book_seen"]]
                        if missing and time.monotonic()-subscribe_mono > config["first_snapshot_timeout_seconds"]:
                            plant.journal.append("SNAPSHOT_TIMEOUT", {"connection": connection,"missing_tokens":missing})
                            # Repair only missing initial baselines on the same lane.
                            # A permanently unavailable token must not reset healthy
                            # books for the other hundreds of tokens in this socket.
                            await ws.send(json_bytes({"operation":"unsubscribe","assets_ids":missing}).decode())
                            retry = str(uuid.uuid4())
                            plant.journal.append("SUBSCRIPTION_WRITE_START",{"connection":connection,
                                "assets":missing,"send_attempt_id":retry,"reason":"MISSING_BASELINE_REPAIR"})
                            await ws.send(json_bytes({"operation":"subscribe","assets_ids":missing,
                                "initial_dump":True,"level":2,"custom_feature_enabled":True}).decode())
                            plant.written(shard,lane,connection,missing,retry)
                            subscribe_mono = time.monotonic()
                        # Keep the mandatory heartbeat cadence during an initial
                        # dump. Identical PONGs are matched FIFO, never to the most
                        # recent PING (which could disguise receive backlog).
                        if len(pending_pings)>=32:
                            await ws.close(code=1011,reason="PONG_QUEUE_BUDGET")
                            return
                        sent_ns=time.monotonic_ns()
                        pending_pings.append(sent_ns)
                        plant.journal.append("PING_SEND",{"connection":connection,"send_mono_ns":sent_ns})
                        await ws.send("PING")
                heartbeat = asyncio.create_task(ping())
                async for message in ws:
                    utc, mono = time.time_ns(), time.monotonic_ns()
                    last_receive = time.monotonic()
                    recv_seq += 1
                    raw = message.encode() if isinstance(message,str) else message
                    if raw == b"PONG":
                        last_pong = time.monotonic()
                        initial_pong = True
                    raw_record=plant.ingest(shard,lane,connection,recv_seq,raw,utc,mono)
                    if raw==b"PONG" and pending_pings:
                        rtt_ms=(mono-pending_pings.popleft())/1e6
                        plant.transport_quality(shard,lane,connection,
                            rtt_ms<=config.get("maximum_healthy_rtt_ms",2000),rtt_ms=rtt_ms,
                            raw_ref={"plant":plant.journal.producer,"capture_run":raw_record.stream,
                                "capture_offset":raw_record.offset,"connection":connection,"recv_seq":recv_seq})
                    # recv() may complete synchronously for an already buffered burst.
                    # Yield at the complete application-message boundary so group sync,
                    # publishing and PING cannot be starved by a continuously hot socket.
                    await asyncio.sleep(0)
                    if stop.is_set():
                        break
        except asyncio.CancelledError:
            raise
        except (OSError, websockets.WebSocketException, TimeoutError, asyncio.TimeoutError) as exc:
            plant.journal.append("WS_ERROR", {"connection": connection, "error": str(exc)})
        finally:
            if heartbeat:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            plant.disconnect(shard,lane,connection,"WS_DISCONNECT")
        try:
            await asyncio.wait_for(stop.wait(), random.uniform(.2, min(20,2**min(epoch,4))))
        except asyncio.TimeoutError:
            pass


async def plant_session(gateway, plant, config, stop):
    while not stop.is_set():
        writer, session = None, None
        try:
            ctx = tls_context(config)
            reader, writer = await asyncio.wait_for(asyncio.open_connection(
                config["host"],config["port"],ssl=ctx,
                server_hostname=config.get("server_name","localhost") if ctx else None), 10)
            await send(writer, control("HELLO", {"schema": 2, "role": "gateway"}))
            snapshot, session = await receive_snapshot(reader)
            await confirm_snapshot(reader,writer,snapshot,session)
            gateway.start_session(plant, session)
            gateway.snapshot(plant,session,snapshot)
            batch_records=0
            while not stop.is_set():
                rec = decode(await asyncio.wait_for(read_frame(reader), 30))
                if rec.kind == "KEEPALIVE":
                    if rec.body.get("session") != session:
                        raise ValueError("STALE_SESSION")
                    if batch_records:
                        raise ValueError("INCOMPLETE_FLOW_BATCH")
                    await send(writer,control("FLOW_ACK",{"session":session}))
                elif rec.kind == "PLANT_EMIT":
                    gateway.accept(plant,session,rec)
                    batch_records+=1
                    if batch_records>128:
                        raise ValueError("FLOW_BATCH_BUDGET")
                    await asyncio.sleep(0)
                elif rec.kind=="FLOW_BATCH_END":
                    if rec.body!={"session":session,"records":batch_records} or not batch_records:
                        raise ValueError("FLOW_BATCH_MISMATCH")
                    await send(writer,control("FLOW_ACK",rec.body))
                    batch_records=0
                else:
                    raise ValueError("RESYNC_REQUIRED")
                # Session flow credit only. No durable input ACK or deletion permission.
        except asyncio.CancelledError:
            raise
        except (OSError, ValueError, asyncio.IncompleteReadError, asyncio.TimeoutError) as exc:
            gateway.journal.append("PLANT_SESSION_ERROR", {"plant": plant, "error": str(exc)})
        finally:
            if not stop.is_set():
                gateway.drop_session(plant,session)
            if writer:
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
        try:
            await asyncio.wait_for(stop.wait(), 1)
        except asyncio.TimeoutError:
            pass


def status(owner):
    usage = resource.getrusage(resource.RUSAGE_SELF)
    result = {"producer": owner.journal.producer, "stream": owner.journal.stream,
        "utc_ns": time.time_ns(), "mono_ns": time.monotonic_ns(),
        "journal_offset": owner.journal.offset, "durable_offset": owner.journal.durable_offset,
        "pending_bytes": owner.journal.pending_size, "syncs": owner.journal.syncs,
        "max_sync_batch_records": owner.journal.max_sync_batch, "failure": owner.journal.failure,
        "emits": owner.emits, "nonselected_groups": owner.nonselected, "resets": owner.resets,
        "committed_seq": owner.committed_seq,
        "committed_digest": {m:digest(s) for m,s in owner.committed.items()} if owner.runtime_config.get("metrics_digests",True) else {},
        "books": {m:inspect(s) for m,s in owner.committed.items()},
        "selected": {m:list(s) if isinstance(s,tuple) else s for m,s in owner.selected.items()},
        "publish_latency_ms": quantiles(owner.latencies_ms),
        "fsync_latency_ms": quantiles(owner.journal.sync_latencies_ms)}
    result["resources"] = {"max_rss_bytes":usage.ru_maxrss*1024,
        "cpu_user_seconds":usage.ru_utime,"cpu_system_seconds":usage.ru_stime}
    if isinstance(owner, Plant):
        result.update(raw_received=owner.raw_received, raw_bytes=owner.raw_bytes, parse_errors=owner.parse_errors,
            token_states=list(owner.states.values()), coverage_transitions=list(owner.history) if owner.runtime_config.get("metrics_history",True) else [],
            coverage_start_mono_ns=owner.start_mono_ns,
            source_book_digest={"|".join(map(str,k)):digest(b.snapshot()) for k,b in owner.books.items()} if owner.runtime_config.get("metrics_digests",True) else {})
    else:
        result.update(accepted=owner.accepted, retries=owner.retries, sessions=owner.sessions)
    return deepcopy_json(result)


def deepcopy_json(value):
    return json.loads(json_bytes(value))


def quantiles(values):
    if not values:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)
    return {"n": len(values), **{name:ordered[min(len(ordered)-1,int((len(ordered)-1)*fraction))]
            for name,fraction in (("p50",.50),("p95",.95),("p99",.99),("max",1.0))}}


def summarize_status(snapshot):
    result = {key:snapshot.get(key,0) for key in ("utc_ns","producer","stream","raw_received","raw_bytes",
        "pending_bytes","emits","parse_errors","local_spool_bytes","durable_offset","journal_offset")}
    states = snapshot.get("token_states",[])
    for output,key in (("written_observations","written"),("book_seen_observations","book_seen"),
            ("durable_baseline_observations","baseline_durable"),("ready_observations","ready")):
        result[output] = sum(bool(s.get(key)) for s in states)
    result["ready_tokens"] = len({s["token"] for s in states if s["ready"]})
    books = snapshot["books"]
    result["published_markets"] = sum(len(assets)==2 and all(b["ready"] and not b["crossed"] for b in assets.values()) for assets in books.values())
    result["published_tokens"] = 2*result["published_markets"]
    result["crossed_tokens"] = sum(b["crossed"] for assets in books.values() for b in assets.values())
    return result


async def metrics_loop(owner, root, stop):
    while not stop.is_set():
        config = owner.runtime_config
        spool = 0
        for path in (root/"journal").iterdir():
            try:
                spool += path.stat().st_size
            except FileNotFoundError:
                pass  # archive worker has just evicted a sealed file
        stat = os.statvfs(root)
        if spool > config.get("spool_max_bytes",4*1024**3) or stat.f_bavail*stat.f_frsize < config.get("minimum_free_bytes",2*1024**3):
            raise OSError("SPOOL_BUDGET_OR_FREE_SPACE_EXHAUSTED")
        snapshot = status(owner)
        snapshot["local_spool_bytes"] = spool
        atomic_json(root/"status.json",snapshot)
        atomic_json(root/"summary.json",summarize_status(snapshot))
        try:
            await asyncio.wait_for(stop.wait(),config.get("metrics_seconds",1))
        except asyncio.TimeoutError:
            pass


async def archive_loop(root, config, stop, owner=None):
    from .archive import build_archive, commit_archive, evict, progress
    from .cli import object_store
    if config.get("ssh_store"):
        from .ssh_store import SSHStore
        import threading
        store = SSHStore(config["ssh_store"])
        store.cancel_event = threading.Event()
    else:
        store = object_store(config["store_uri"])
    # One bounded worker per host/process; upload failure preserves journal and retries.
    while not stop.is_set():
        for path in sorted((root/"journal").glob("*.journal")):
            receipt = path.with_suffix(".archive.json")
            if receipt.exists() and (not config.get("evict_after_commit") or json.loads(receipt.read_bytes()).get("eviction")):
                continue
            def archive_one():
                packed = path.with_suffix(".lobseg")
                if receipt.exists():
                    saved = json.loads(receipt.read_bytes())
                    key,manifest = saved["manifest_key"],saved["manifest"]
                else:
                    try:
                        build_archive(path,packed)
                    except ValueError as exc:
                        if str(exc) != "EMPTY_JOURNAL":
                            raise
                        # Preserve a legacy header-only segment separately. It has
                        # no observations or offsets and cannot block later custody.
                        os.rename(path,path.with_suffix(".empty"))
                        from .journal import sync_dir
                        sync_dir(path.parent)
                        return
                    key,manifest = commit_archive(store,packed,"capture" if path.name.startswith("capture") else "tape")
                    saved = {"manifest_key":key,"manifest":manifest,"committed_utc_ns":time.time_ns(),
                        "local_journal_bytes":path.stat().st_size}
                    atomic_json(receipt,saved)
                if config.get("evict_after_commit"):
                    # This process owns the private journal directory under writer.lock.
                    # Replay consumers use immutable objects. All live subscriptions and
                    # callbacks own encoded frames in memory, never sealed file readers.
                    dependencies = []
                    if owner is None or (manifest["stream"] == owner.journal.stream and manifest["last"] > owner.journal.durable_offset):
                        dependencies.append("DURABLE_CALLBACK_CUT_NOT_RELEASED")
                    saved["eviction"] = evict(path,packed,store,key,active_dependencies=dependencies)
                    saved["eviction"]["dependency_evidence"] = "EXCLUSIVE_WRITER_PRIVATE_DIRECTORY_NO_FILE_REPLAY_READERS"
                    atomic_json(receipt,saved)
            try:
                future = asyncio.get_running_loop().run_in_executor(None,archive_one)
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    if hasattr(store,"cancel_event"):
                        store.cancel_event.set()
                    await future
                    raise
            except Exception as exc:
                atomic_json(root/"archive-error.json",{"error":str(exc),"path":str(path),"at_ns":time.time_ns(),
                    "action":"LOCAL_JOURNAL_RETAINED_RETRY_PENDING"})
                break
        receipts = [json.loads(p.read_bytes()) for p in (root/"journal").glob("*.archive.json")]
        streams = {}
        for row in receipts:
            streams.setdefault(row["manifest"]["stream"],[]).append(row["manifest"])
        unarchived = [p for p in (root/"journal").glob("*.journal") if not p.with_suffix(".archive.json").exists()]
        ranges = {}
        for stream,rows in streams.items():
            last = max(r["last"] for r in rows)
            local_last = max(last,owner.journal.durable_offset) if owner and stream == owner.journal.stream else last
            ranges[stream] = progress(rows,local_last)
            ranges[stream]["holes_before_last_commit"] = [hole for hole in ranges[stream]["holes"] if hole[0] <= last]
        atomic_json(root/"archive-progress.json",{
            "utc_ns":time.time_ns(),"backend":store.backend,"committed_segments":len(receipts),
            "journal_bytes_committed":sum(r.get("local_journal_bytes",0) for r in receipts),
            "object_bytes_committed":sum(r["manifest"]["object_bytes"] for r in receipts),
            "evicted_bytes":sum(r.get("eviction",{}).get("released_bytes",0) for r in receipts),
            "sealed_pending_bytes":sum(p.stat().st_size for p in unarchived),
            "sealed_oldest_age_seconds":max((time.time()-p.stat().st_mtime for p in unarchived),default=0),
            "streams":ranges})
        try:
            await asyncio.wait_for(stop.wait(),config.get("poll_seconds",2))
        except asyncio.TimeoutError:
            pass


async def run_node(role, node_id, config, scope, root, seconds=None):
    validate_scope(scope)
    root = Path(root)
    root.mkdir(parents=True,exist_ok=True)
    if config["queue_bytes"] < MAX_RECORD or not 0 < config["ws_max_bytes"] < MAX_RECORD:
        raise ValueError("INVALID_QUEUE_OR_WS_RECORD_BUDGET")
    lock = (root/"writer.lock").open("a")
    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    recovered = recover_open(root/"journal")
    journal = Journal(root/"journal",node_id,"capture" if role == "plant" else "tape",
        segment_bytes=config["segment_bytes"], segment_seconds=config["segment_seconds"],
        pending_bytes=config["queue_bytes"])
    if role == "plant":
        owner = Plant(journal,scope,config["lanes"])
        owner.require_transport_proof=config.get("require_transport_proof",True)
        owner.prefer_primary_lane=config.get("prefer_primary_lane",True)
        owner.maximum_transport_age_ns=int(config.get("transport_proof_max_age_seconds",30)*10**9)
    else:
        owner = Gateway(journal,scope)
        previous = recovered
        if not previous:
            paths = sorted((root/"journal").glob("*.journal"),key=lambda p:p.stat().st_mtime_ns)
            if paths:
                tail = list(read_journal(paths[-1]))
                if tail:
                    previous = [{"stream":tail[-1].stream,"last_recoverable_offset":tail[-1].offset}]
        journal.append("STREAM_RESTART", {"previous_recoverable_tapes":previous,
            "continuity": "RESET_REQUIRED", "client_received_position": "UNKNOWN"})
    owner.runtime_config = config
    package_root=Path(__file__).parents[1]
    source_sha={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((package_root/"lobv2").glob("*.py"))}
    owner.provenance={"code_version":__version__,"source_sha256":hashlib.sha256(json_bytes(source_sha)).hexdigest(),
        "config_sha256":hashlib.sha256(json_bytes(config)).hexdigest(),"schema_sha256":SCHEMA_DIGEST,
        "scope_sha256":scope.get("catalog_digest",hashlib.sha256(json_bytes(scope)).hexdigest())}
    journal.append("RUN_START",{"provenance":owner.provenance,"source_files":source_sha,
        "scope":scope,"stream_semantics":"SOURCE_SELECTED_L2","clock_quality":"LOCAL_WALL_UNVERIFIED"})
    atomic_json(root/"run.json", {"role":role,"node":node_id,"pid":os.getpid(),
        "scope":scope,"config":config,"stream":journal.stream,"start_ns":time.time_ns()})
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        loop.add_signal_handler(sig,stop.set)
    tasks = [asyncio.create_task(journal.run(stop,config["group_fsync_ms"]/1000)),
             asyncio.create_task(serve(owner,config["listen"],stop)),
             asyncio.create_task(metrics_loop(owner,root,stop))]
    if config.get("archive",{}).get("store_uri") or config.get("archive",{}).get("ssh_store"):
        tasks.append(asyncio.create_task(archive_loop(root,config["archive"],stop,owner)))
    if role == "plant":
        for shard, markets in enumerate(shards(scope,config["assets_per_connection"])):
            for lane in range(config["lanes"]):
                tasks.append(asyncio.create_task(lane_loop(owner,shard,lane,markets,config,stop)))
    else:
        tasks.extend(asyncio.create_task(plant_session(owner,p,endpoint,stop))
                     for p,endpoint in config["plants"].items())
    timer = asyncio.create_task(asyncio.sleep(seconds)) if seconds else None
    stopper = asyncio.create_task(stop.wait())
    try:
        done,_ = await asyncio.wait(tasks+[stopper]+([timer] if timer else []),return_when=asyncio.FIRST_COMPLETED)
        # Any worker error stops publication; a healthy sibling cannot hide a failed writer.
        for task in done:
            if task in tasks:
                task.result()
    finally:
        atomic_json(root/"before_shutdown.json",status(owner))
        stop.set()
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks,return_exceptions=True)
        for task in (timer,stopper):
            if task:
                task.cancel()
        await journal.close()
        atomic_json(root/"final.json",status(owner))
        lock.close()
