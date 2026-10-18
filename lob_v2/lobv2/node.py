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
import platform
import gc
from concurrent.futures import ThreadPoolExecutor
from collections import deque

import websockets

from .book import digest, inspect, state_view
from .catalog import shards, validate_scope
from .core import Gateway, Plant
from .journal import Journal, atomic_json, read_journal, recover_open, repair_lifecycle_outboxes
from .wire import MAX_RECORD, SCHEMA_DIGEST, control, decode, encode, json_bytes, read_frame, send
from . import __version__
from .telemetry import queue_snapshot

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


async def connection_credit(config):
    """Reserve one connection slot across processes using a tiny local file."""
    path = config.get("connection_rate_file")
    if not path:
        return
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    interval = 1/config.get("connections_per_second",10)
    def reserve():
        with path.open("a+") as file:
            try:fcntl.flock(file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return random.uniform(.01,.05)
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
                return 0
            return max(.001,prior-now)+random.uniform(0,.1*interval)
    while True:
        delay=await asyncio.to_thread(reserve)
        if not delay:return
        # Reserve only the slot actually used. Cancelled connection tasks cannot
        # leave minutes of phantom reservations behind a plant restart.
        await asyncio.sleep(delay)


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
    def encode_snapshot():
        value={**snapshot,"items":[{**item,"assets":state_view(item["assets"])} for item in snapshot["items"]]}
        data=bytearray()
        # Iterative encoding yields the GIL rather than holding it for an entire
        # multi-market json.dumps call in a supposedly isolated background thread.
        for piece in json.JSONEncoder(ensure_ascii=False,separators=(",",":"),sort_keys=True,allow_nan=False).iterencode(value):
            data.extend(piece.encode())
            if len(data)>64*1024*1024:raise BufferError("SNAPSHOT_BUDGET_EXCEEDED")
        return bytes(data)
    data = await asyncio.get_running_loop().run_in_executor(None,encode_snapshot)
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


async def decode_snapshot(data):
    """Yield only between complete markets while decoding a large bootstrap."""
    text=data.decode();decoder=json.JSONDecoder();size=len(text);index=0;result={}
    def whitespace(position):
        while position<size and text[position] in ' \r\n\t':position+=1
        return position
    index=whitespace(index)
    if index>=size or text[index]!='{':raise ValueError('INVALID_SNAPSHOT_OBJECT')
    index=whitespace(index+1);started=time.monotonic_ns()
    while index<size and text[index]!='}':
        key,index=decoder.raw_decode(text,index)
        if not isinstance(key,str) or key in result:raise ValueError('DUPLICATE_SNAPSHOT_FIELD')
        index=whitespace(index)
        if index>=size or text[index]!=':':raise ValueError('INVALID_SNAPSHOT_FIELD')
        index=whitespace(index+1)
        if key=='items':
            if index>=size or text[index]!='[':raise ValueError('INVALID_SNAPSHOT_ITEMS')
            items=[];index=whitespace(index+1)
            while index<size and text[index]!=']':
                item,index=decoder.raw_decode(text,index)
                if not isinstance(item,dict):raise ValueError('INVALID_SNAPSHOT_MARKET')
                items.append(item);index=whitespace(index)
                if time.monotonic_ns()-started>=2_000_000:
                    await asyncio.sleep(0);started=time.monotonic_ns()
                if index<size and text[index]==',':
                    index=whitespace(index+1)
                    if index>=size or text[index]==']':raise ValueError('TRAILING_SNAPSHOT_COMMA')
                else:break
            if index>=size or text[index]!=']':raise ValueError('INCOMPLETE_SNAPSHOT_ITEMS')
            result[key]=items;index+=1
        else:result[key],index=decoder.raw_decode(text,index)
        index=whitespace(index)
        if index<size and text[index]==',':
            index=whitespace(index+1)
            if index>=size or text[index]=='}':raise ValueError('TRAILING_SNAPSHOT_COMMA')
        else:break
    if index>=size or text[index]!='}' or whitespace(index+1)!=size:raise ValueError('INCOMPLETE_SNAPSHOT_OBJECT')
    return result


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
    snapshot = await decode_snapshot(data)
    if snapshot["snapshot_id"] != header["snapshot_id"]:
        raise ValueError("SNAPSHOT_ID_MISMATCH")
    return snapshot, header["session"]


async def serve(owner, config, stop):
    active_tasks = set()
    snapshot_slots=asyncio.Semaphore(1)
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
            async with snapshot_slots:
                snapshot, subscription = owner.bootstrap(hello.body.get("markets"), config["queue_bytes"],deferred=True)
                subscription['follow_scope']=bool(hello.body.get('follow_scope')) and hello.body.get('markets') is None
                await asyncio.wait_for(transmit_snapshot(writer, reader, snapshot, session,subscription["queue"]), 30)
                del snapshot  # Release frozen ladders after the cut+1 handoff.
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
    from .reader import session_manager
    await session_manager(plant,shard,lane,markets,config,stop)


async def qualification_loop(plant,stop):
    last_full=time.monotonic()
    while not stop.is_set():
        expected=time.monotonic()+.02
        await asyncio.sleep(.02)
        plant.loop_lag_ms=max(0.,(time.monotonic()-expected)*1000)
        plant.journal.metrics.observe("event_loop_lag",plant.loop_lag_ms)
        if time.monotonic()-last_full>=1:
            plant.dirty_transports.update(plant.transport_keys)
            last_full=time.monotonic()
        dirty=tuple(plant.dirty_transports)
        plant.dirty_transports.clear()
        started=time.monotonic_ns()
        for slot in dirty:
            for key in tuple(plant.transport_keys.get(slot,())):
                if key not in plant.books:continue
                plant.reconcile(key)
                if time.monotonic_ns()-started>2_000_000:
                    await asyncio.sleep(0)
                    started=time.monotonic_ns()


async def catalog_control_loop(owner,config,stop):
    path=Path(config['catalog_control_file']);head=Path(config['catalog_head_file'])
    stamp=None
    while not stop.is_set():
        def read():
            if not path.exists() or not head.exists():return None
            mtime=path.stat().st_mtime_ns
            if mtime==stamp:return None
            control=json.loads(path.read_bytes());commit=json.loads(head.read_bytes())
            if control['catalog_revision']>commit['catalog_revision']:return None
            digest=control.get('cohort_scope_digest')
            if digest and digest!=getattr(owner,'cohort_scope_digest',None):
                scope=json.loads(Path(config['scope_file']).read_bytes())
                if hashlib.sha256(json_bytes(scope['markets'])).hexdigest()!=digest:return None
                control['market_groups']=scope['markets']
            return mtime,control
        update=await asyncio.to_thread(read)
        if update:
            await owner.apply_catalog_control(update[1]);stamp=update[0]
            owner.cohort_scope_digest=update[1].get('cohort_scope_digest')
        try:await asyncio.wait_for(stop.wait(),1)
        except asyncio.TimeoutError:pass


async def scope_lanes_loop(plant,scope,config,stop):
    groups=shards(scope,config['assets_per_connection'])
    known=set(plant.scope)
    workers=[]
    def launch(shard):
        workers.extend(asyncio.create_task(lane_loop(plant,shard,lane,groups[shard],config,stop))
            for lane in range(config['lanes']))
    for shard in range(len(groups)):launch(shard)
    try:
        while not stop.is_set():
            for task in workers:
                if task.done():
                    task.result()
                    raise RuntimeError('LANE_MANAGER_STOPPED')
            for market in plant.scope.keys()-known:
                row=plant.scope[market]
                if sum(len(m['assets']) for m in groups[-1])+len(row['assets'])>config['assets_per_connection']:
                    groups.append([row]);launch(len(groups)-1)
                else:groups[-1].append(row)
                known.add(market)
            await asyncio.sleep(.25)
    finally:
        for task in workers:task.cancel()
        await asyncio.gather(*workers,return_exceptions=True)


async def await_catalog_members(owner,markets):
    deadline=time.monotonic()+5
    while not set(markets)<=owner.scope.keys():
        if time.monotonic()>=deadline:raise ValueError('CATALOG_SCOPE_NOT_YET_COMMITTED')
        await asyncio.sleep(.02)


async def plant_session(gateway, plant, config, stop):
    while not stop.is_set():
        writer, session = None, None
        try:
            if not hasattr(gateway,'bootstrap_slots'):gateway.bootstrap_slots=asyncio.Semaphore(1)
            async with gateway.bootstrap_slots:
                ctx = tls_context(config)
                reader, writer = await asyncio.wait_for(asyncio.open_connection(
                    config["host"],config["port"],ssl=ctx,
                    server_hostname=config.get("server_name","localhost") if ctx else None), 10)
                await send(writer, control("HELLO", {"schema": 2, "role": "gateway",'follow_scope':True}))
                snapshot, session = await receive_snapshot(reader)
                await confirm_snapshot(reader,writer,snapshot,session)
                gateway.start_session(plant, session)
                await await_catalog_members(gateway,[item['market'] for item in snapshot['items']])
                await gateway.snapshot_async(plant,session,snapshot)
                del snapshot  # A live session must not pin its initial wire ladders.
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
                    if rec.body['market'] not in gateway.scope:await await_catalog_members(gateway,[rec.body['market']])
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
        "catalog_revision":owner.catalog_revision,"scope_policy_version":owner.scope_policy_version,
        "blocked_markets":dict(owner.blocked_markets),"provenance":owner.provenance,
        "utc_ns": time.time_ns(), "mono_ns": time.monotonic_ns(),
        "journal_offset": owner.journal.offset, "durable_offset": owner.journal.durable_offset,
        "written_offset":owner.journal.written_offset,"applied_offset":owner.journal.applied_offset,
        "published_offset":owner.journal.published_offset,"cut_receipts":dict(owner.journal.cut_receipts),
        "journal_oldest_age_ms":(time.monotonic_ns()-owner.journal.pending[0].trace["journal_enqueue"])/1e6 if owner.journal.pending else 0.,
        "pending_records":len(owner.journal.pending),"admission_failure":owner.journal.admission_failure,
        "pending_publication_records":sum(r.kind in ("TAPE","PLANT_EMIT") for r in owner.journal.pending),
        "pending_bytes": owner.journal.pending_size, "syncs": owner.journal.syncs,
        "max_sync_batch_records": owner.journal.max_sync_batch, "failure": owner.journal.failure,
        "emits": owner.emits, "nonselected_groups": owner.nonselected, "resets": owner.resets,
        "committed_seq": dict(owner.committed_seq),
        "committed_digest": {m:digest(s) for m,s in owner.committed.items()} if owner.runtime_config.get("metrics_digests",True) else {},
        "books": {m:inspect(s) for m,s in owner.committed.items()},
        "selected": {m:list(s) if isinstance(s,tuple) else s for m,s in owner.selected.items()},
        "publish_latency_ms": quantiles(owner.latencies_ms),
        "fsync_latency_ms": quantiles(owner.journal.sync_latencies_ms),
        "stage_metrics":owner.journal.metrics.snapshot(),"stage_traces":list(owner.journal.stage_samples)[-256:],
        "subscription_queues":[queue_snapshot(s["queue"]) for s in owner.subscribers]}
    result["resources"] = {"max_rss_bytes":usage.ru_maxrss*1024,
        "cpu_user_seconds":usage.ru_utime,"cpu_system_seconds":usage.ru_stime}
    if isinstance(owner, Plant):
        result.update(raw_received=owner.raw_received, raw_bytes=owner.raw_bytes, parse_errors=owner.parse_errors,
            active_source_routes=[{'market':market,'lane':lane,'shard':key[1],
                'connection':owner.connections.get((key[1],lane))} for (market,lane),key in owner.active_routes.items()],
            token_states=[dict(s) for s in owner.states.values()], coverage_transitions=list(owner.history) if owner.runtime_config.get("metrics_history",True) else [],
            ingress_queues={str(k):queue_snapshot(q) for k,q in owner.ingress_queues.items()},event_loop_lag_ms=owner.loop_lag_ms,
            coverage_start_mono_ns=owner.start_mono_ns,
            source_book_digest={"|".join(map(str,k)):digest(b.snapshot()) for k,b in owner.books.items()} if owner.runtime_config.get("metrics_digests",True) else {})
    else:
        result.update(accepted=owner.accepted, retries=owner.retries, sessions=owner.sessions)
    return result


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
        "pending_bytes","emits","parse_errors","local_spool_bytes","durable_offset","journal_offset",
        "catalog_revision","scope_policy_version","provenance")}
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
    executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix="metrics")
    loop=asyncio.get_running_loop()
    last_trace=0
    import orjson
    while not stop.is_set():
        config = owner.runtime_config
        measured=time.monotonic_ns()
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
        owner.journal.metrics.observe('metrics_collection',(time.monotonic_ns()-measured)/1e6)
        snapshot["local_spool_bytes"] = spool
        summary=summarize_status(snapshot)
        def write_metrics():
            encoder=lambda value:orjson.dumps(value,option=orjson.OPT_SORT_KEYS)
            atomic_json(root/"status.json",snapshot,encoder=encoder)
            atomic_json(root/"summary.json",summary,encoder=encoder)
        future=loop.run_in_executor(executor,write_metrics)
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            await future
            executor.shutdown(wait=True)
            raise
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
    executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix="archive")
    # One bounded worker per host/process; upload failure preserves journal and retries.
    while not stop.is_set():
        for path in sorted((root/"journal").glob("*.journal")):
            receipt = path.with_suffix(".archive.json")
            if receipt.exists() and (not config.get("evict_after_commit") or json.loads(receipt.read_bytes()).get("eviction")):
                continue
            def archive_one():
                packed = path.with_suffix(".lobseg")
                def stage(name):
                    atomic_json(root/'archive-phase.json',{'utc_ns':time.time_ns(),'path':str(path),'stage':name,
                        'input_bytes':path.stat().st_size if path.exists() else 0,
                        'packed_bytes':packed.stat().st_size if packed.exists() else 0})
                if receipt.exists():
                    saved = json.loads(receipt.read_bytes())
                    key,manifest = saved["manifest_key"],saved["manifest"]
                else:
                    try:
                        stage('COMPRESS')
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
                    key,manifest = commit_archive(store,packed,"capture" if path.name.startswith("capture") else "tape",on_stage=stage)
                    saved = {"manifest_key":key,"manifest":manifest,"committed_utc_ns":time.time_ns(),
                        "local_journal_bytes":path.stat().st_size}
                    atomic_json(receipt,saved)
                if config.get("evict_after_commit"):
                    # This process owns the private journal directory under writer.lock.
                    # Replay consumers use immutable objects. All live subscriptions and
                    # callbacks own encoded frames in memory, never sealed file readers.
                    dependencies = []
                    lifecycle_cut=manifest.get('lifecycle_last_cut',0)
                    if lifecycle_cut:
                        life=root/'lifecycle'/(manifest['stream']+'.progress.json')
                        if not life.exists() or json.loads(life.read_bytes()).get('canonical_notification_cut',0)<lifecycle_cut:
                            dependencies.append('LIFECYCLE_OUTBOX_NOT_RECOVERABLE')
                    if owner is None or (manifest["stream"] == owner.journal.stream and manifest["last"] > owner.journal.applied_offset):
                        dependencies.append("DURABLE_CALLBACK_CUT_NOT_RELEASED")
                    saved["eviction"] = evict(path,packed,store,key,active_dependencies=dependencies)
                    saved["eviction"]["dependency_evidence"] = "EXCLUSIVE_WRITER_PRIVATE_DIRECTORY_NO_FILE_REPLAY_READERS"
                    atomic_json(receipt,saved)
                    stage('LOCAL_CACHE_RELEASED')
            try:
                def budgeted_archive():
                    from .ssh_store import archive_slot
                    atomic_json(root/'archive-phase.json',{'utc_ns':time.time_ns(),'path':str(path),'stage':'WAIT_BACKGROUND_BUDGET'})
                    with archive_slot(config,store):
                        return archive_one()
                future = asyncio.get_running_loop().run_in_executor(executor,budgeted_archive)
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
        def write_archive_progress():
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
                'open_tail_bytes':sum(p.stat().st_size for p in (root/'journal').glob('*.open')),
                'sealed_uncompressed_bytes':sum(p.stat().st_size for p in unarchived if not p.with_suffix('.lobseg').exists()),
                'compressed_pending_commit_bytes':sum(p.with_suffix('.lobseg').stat().st_size for p in unarchived if p.with_suffix('.lobseg').exists()),
                'committed_waiting_local_release_bytes':sum(p.stat().st_size for p in (root/'journal').glob('*.journal') if p.with_suffix('.archive.json').exists()),
                "streams":ranges})
        await asyncio.get_running_loop().run_in_executor(executor,write_archive_progress)
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
    if role=='plant':await asyncio.to_thread(repair_lifecycle_outboxes,root/'journal')
    journal = Journal(root/"journal",node_id,"capture" if role == "plant" else "tape",
        segment_bytes=config["segment_bytes"], segment_seconds=config["segment_seconds"],
        pending_bytes=config["queue_bytes"])
    if role == "plant":
        owner = Plant(journal,scope,config["lanes"])
        owner.require_transport_proof=config.get("require_transport_proof",True)
        owner.prefer_primary_lane=config.get("prefer_primary_lane",True)
        owner.maximum_transport_age_ns=int(config.get("transport_proof_max_age_seconds",30)*10**9)
        owner.defer_qualification=True
        owner.require_source_admission=True
        owner.require_local_progress=True
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
    owner.cohort_scope_digest=scope.get('catalog_digest')
    gc_started={}
    def gc_timing(phase,info):
        generation=info['generation']
        if phase=='start':gc_started[generation]=time.monotonic_ns()
        elif generation in gc_started:
            journal.metrics.observe('gc_pause_generation_'+str(generation),(time.monotonic_ns()-gc_started.pop(generation))/1e6)
    gc.callbacks.append(gc_timing)
    package_root=Path(__file__).parents[1]
    source_sha={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((package_root/"lobv2").glob("*.py"))}
    owner.provenance={"code_version":__version__,"source_sha256":hashlib.sha256(json_bytes(source_sha)).hexdigest(),
        "config_sha256":hashlib.sha256(json_bytes(config)).hexdigest(),"schema_sha256":SCHEMA_DIGEST,
        "scope_sha256":scope.get("catalog_digest",hashlib.sha256(json_bytes(scope)).hexdigest())}
    owner.provenance["clock_id"] = platform.node()+":"+Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    journal.append("RUN_START",{"provenance":owner.provenance,"source_files":source_sha,
        "runtime":{"python":platform.python_version(),"websockets":websockets.__version__,"websocket_implementation":websockets.connect.__module__,
            "max_size":config["ws_max_bytes"],"max_queue_frames":config.get("ws_max_queue",min(16,config["queue_bytes"]//config["ws_max_bytes"])),
            "write_limit":config.get("ws_write_limit",32768),"compression":"deflate","protocol_ping_interval":None,
            "text_ping_interval_seconds":config["heartbeat_seconds"],"group_fsync_ms":config["group_fsync_ms"]},
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
    if config.get('catalog_control_file'):
        tasks.append(asyncio.create_task(catalog_control_loop(owner,config,stop)))
    if role == "plant":
        tasks.append(asyncio.create_task(qualification_loop(owner,stop)))
        tasks.append(asyncio.create_task(scope_lanes_loop(owner,scope,config,stop)))
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
        gc.callbacks.remove(gc_timing)
        atomic_json(root/"final.json",status(owner))
        lock.close()
