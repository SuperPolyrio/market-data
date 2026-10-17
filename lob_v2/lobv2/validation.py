"""Real WS acceptance, source/tape replay, storage readback and explicit coverage."""
import asyncio
from collections import Counter, defaultdict
from copy import deepcopy
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import socket
import subprocess
import sys
import time

from .archive import DirectoryStore, build_archive, commit_archive, evict, progress, read_archive, restore
from .book import apply, digest, inspect
from .journal import atomic_json, read_journal
from .wire import decode, json_bytes


def all_records(root):
    for path in sorted(Path(root).rglob("*.journal")):
        yield from read_journal(path)


def reference_sources(records, scope, cutoff):
    """Independent simple Decimal dictionary replay, not SourceBook.observe."""
    states, active = {}, {}
    market_tokens = {m["market"]:m["assets"] for m in scope["markets"]}
    count = Counter()
    for rec in records:
        if rec.offset > cutoff:
            continue
        body = rec.body
        if rec.kind == "CONNECT":
            slot = (body["shard"],body["lane"])
            active[slot] = body["connection"]
            # Shard membership is identified by subsequent raw market incidence.
            for key in list(states):
                if key[1:] == slot:
                    del states[key]
        elif rec.kind == "DISCONNECT":
            slot = (body["shard"],body["lane"])
            if active.get(slot) == body["connection"]:
                for key,state in states.items():
                    if key[1:] == slot:
                        for book in state.values():
                            book["ready"] = False
        elif rec.kind == "RAW":
            count["raw"] += 1
            count["raw_bytes"] += len(rec.raw)
            slot = (body["shard"],body["lane"])
            if active.get(slot) != body["connection"] or rec.raw == b"PONG":
                continue
            try:
                events = json.loads(rec.raw,parse_float=str)
                events = events if isinstance(events,list) else [events]
                for index,event in enumerate(events):
                    market = event.get("market")
                    if market not in market_tokens:
                        continue
                    key = (market,*slot)
                    state = states.setdefault(key,{token:{"bids":{},"asks":{},"ready":False,
                        "snapshot_epoch":0,"last_source_time":None,"baseline_ref":None,"last_ref":None}
                        for token in market_tokens[market]})
                    ref = {"plant":rec.producer,"capture_run":rec.stream,"connection":body["connection"],
                           "recv_seq":body["recv_seq"],"capture_offset":rec.offset,"top_level_message_index":index}
                    if event["event_type"] == "book":
                        asset = event["asset_id"]
                        old = state[asset]
                        state[asset] = {"bids":{Decimal(x["price"]):Decimal(x["size"]) for x in event["bids"] if Decimal(x["size"])},
                            "asks":{Decimal(x["price"]):Decimal(x["size"]) for x in event["asks"] if Decimal(x["size"])},
                            "ready":True,"snapshot_epoch":old["snapshot_epoch"]+1,
                            "last_source_time":event.get("timestamp"),"baseline_ref":ref,"last_ref":ref}
                        count["snapshots"] += 1
                    elif event["event_type"] == "price_change":
                        rows = event["price_changes"]
                        if any(not state[row["asset_id"]]["ready"] for row in rows):
                            count["before_snapshot"] += 1
                            continue
                        updates = deepcopy(state)
                        for row in rows:
                            book = updates[row["asset_id"]]
                            side = book["bids" if row["side"]=="BUY" else "asks"]
                            p,s = Decimal(row["price"]),Decimal(row["size"])
                            if s: side[p] = s
                            else: side.pop(p,None)
                            book["last_source_time"],book["last_ref"] = event.get("timestamp"),ref
                        state = updates
                        states[key] = state
                        count["groups"] += 1
                        count["level_changes"] += len(rows)
                        terminal={row["asset_id"]:row for row in rows}
                        for asset,row in terminal.items():
                            for field,side,choose in (("best_bid","bids",max),("best_ask","asks",min)):
                                if row.get(field) is not None and state[asset][side]:
                                    count["bbo_comparisons"]+=1
                                    if choose(state[asset][side])!=Decimal(row[field]):
                                        count["bbo_mismatches"]+=1
            except (ValueError,KeyError,TypeError):
                count["uninterpreted"] += 1
    def number(value):
        result = format(value,"f")
        return result.rstrip("0").rstrip(".") if "." in result else result
    normalized = {}
    for key,state in states.items():
        normalized["|".join(map(str,key))] = {asset:{**{k:v for k,v in book.items() if k not in ("bids","asks")},
            "bids":[[number(p),number(s)] for p,s in sorted(book["bids"].items(),reverse=True)],
            "asks":[[number(p),number(s)] for p,s in sorted(book["asks"].items())]} for asset,book in state.items()}
    return normalized,dict(count)


def coverage(plant_statuses, scope, start_ns, end_ns):
    tokens = [token for market in scope["markets"] for token in market["assets"]]
    lane_states, timeline = {}, defaultdict(list)
    for plant, status in plant_statuses.items():
        lane_states[plant] = status["token_states"]
        for transition in status["coverage_transitions"]:
            shard,lane,token = transition["key"]
            timeline[(plant,lane,token)].append((transition["mono_ns"],transition["ready"]))
    def ready_at(plant,lane,token,at):
        current = False
        for when,ready in timeline[(plant,lane,token)]:
            if when <= at: current = ready
        return current
    boundaries = sorted({start_ns,end_ns,*[max(start_ns,min(end_ns,t)) for entries in timeline.values() for t,_ in entries]})
    one = two = pairs = both_pairs = 0
    lanes = sorted({state["lane"] for states in lane_states.values() for state in states})
    for left,right in zip(boundaries,boundaries[1:]):
        for token in tokens:
            plants_ready = [any(ready_at(plant,lane,token,left) for lane in lanes) for plant in plant_statuses]
            one += (right-left)*any(plants_ready)
            two += (right-left)*all(plants_ready)
        for market in scope["markets"]:
            pair_ready = [any(all(ready_at(plant,lane,t,left) for t in market["assets"]) for lane in lanes)
                          for plant in plant_statuses]
            pairs += (right-left)*any(pair_ready)
            both_pairs += (right-left)*all(pair_ready)
    denominator = len(tokens)*(end_ns-start_ns)
    market_denominator = len(scope["markets"])*(end_ns-start_ns)
    cut = {}
    for plant,states in lane_states.items():
        cut[plant] = {}
        for lane in lanes:
            subset = [s for s in states if s["lane"] == lane]
            sets = {name:sorted(s["token"] for s in subset if s.get(field)) for name,field in
                (("assigned","assigned"),("written","written"),("book_seen","book_seen"),
                 ("baseline_durable","baseline_durable"),("ready","ready"))}
            sets["expected"] = sorted(tokens)
            sets["not_written"] = sorted(set(tokens)-set(sets["written"]))
            sets["written_without_book"] = sorted(set(sets["written"])-set(sets["book_seen"]))
            sets["not_ready"] = sorted(set(tokens)-set(sets["ready"]))
            cut[plant][str(lane)] = sets
    return {"window_start_mono_ns":start_ns,"window_end_mono_ns":end_ns,
        "time_basis":"same_host_monotonic_small_acceptance", "expected_tokens":len(tokens),
        "expected_token_seconds":denominator/1e9,"one_plant_token_coverage":one/denominator if denominator else None,
        "both_plants_token_coverage":two/denominator if denominator else None,
        "one_plant_complete_market_coverage":pairs/market_denominator if market_denominator else None,
        "both_plants_complete_market_coverage":both_pairs/market_denominator if market_denominator else None,
        "at_cut":cut,"upstream_event_completeness":"UNPROVEN"}


def export_parquet(root, destination):
    import pyarrow as pa
    import pyarrow.parquet as pq
    destination.mkdir(parents=True,exist_ok=True)
    rows, ranges = [], defaultdict(list)
    # Explicit bounded offline projection, batch in 10k rows to fixed four buckets.
    writers = {}
    counts = Counter()
    input_buckets = {i:hashlib.sha256() for i in range(4)}
    row_bytes = 0
    schema = pa.schema([("stream",pa.string()),("offset",pa.uint64()),("producer",pa.string()),
        ("kind",pa.string()),("market",pa.string()),("market_seq",pa.uint64()),
        ("created_ns",pa.uint64()),("frame",pa.binary())])
    def flush():
        if not rows: return
        for bucket in range(4):
            selected = [r for b,r in rows if b==bucket]
            if selected:
                if bucket not in writers:
                    writers[bucket] = pq.ParquetWriter(destination/f"tape-events-bucket-{bucket}.parquet",schema,compression="zstd")
                writers[bucket].write_table(pa.Table.from_pylist(selected,schema=schema))
        rows.clear()
    sha = hashlib.sha256()
    try:
        for rec in all_records(root):
            if rec.kind != "TAPE": continue
            market = rec.body["market"]
            bucket = int.from_bytes(hashlib.sha256(market.encode()).digest()[:4],"big")%4
            if row_bytes+len(rec.frame)>32*1024*1024:
                flush()
                row_bytes=0
            rows.append((bucket,{"stream":rec.stream,"offset":rec.offset,"producer":rec.producer,
                "kind":rec.body["kind"],"market":market,"market_seq":rec.body["market_seq"],
                "created_ns":rec.created_ns,"frame":rec.frame}))
            row_bytes+=len(rec.frame)
            input_buckets[bucket].update(rec.frame)
            counts[rec.stream] += 1
            ranges[rec.stream].append(rec.offset)
            sha.update(rec.frame)
            if len(rows)>=10000:
                flush()
                row_bytes=0
        flush()
    finally:
        for writer in writers.values(): writer.close()
    recovered_count = 0
    for path in destination.glob("*.parquet"):
        bucket=int(path.stem.rsplit("-",1)[1])
        output_sha=hashlib.sha256()
        for batch in pq.ParquetFile(path).iter_batches(batch_size=1000):
            for row in batch.to_pylist():
                rec=decode(row["frame"])
                output_sha.update(rec.frame)
                recovered_count+=1
        if output_sha.digest()!=input_buckets[bucket].digest():
            raise ValueError("QUERY_FRAME_MISMATCH")
    input_count = sum(counts.values())
    if recovered_count != input_count:
        raise ValueError("QUERY_RANGE_COUNT_MISMATCH")
    receipt = {"view":"tape_events","view_version":2,"status":"QUERY_EXPORT_COMMITTED",
        "records":input_count,"input_recordstream_sha256":sha.hexdigest(),
        "ranges":{s:{"first":min(v),"last":max(v),"count":len(v)} for s,v in ranges.items()},
        "files":len(writers),"tape_frame_readback":"IDENTICAL"}
    atomic_json(destination/"query_commit.json",receipt)
    return receipt


def audit_run(root):
    scope = json.loads((root/"scope.json").read_bytes())
    checks, sources = {}, {}
    for plant in ("P","Q"):
        status = json.loads((root/plant/"before_shutdown.json").read_bytes())
        run = json.loads((root/plant/"run.json").read_bytes())
        records = list(all_records(root/plant))
        # A restart's before_shutdown describes only its current stream.
        records = [r for r in records if r.stream == status["stream"]]
        replay,counts = reference_sources(records,scope,status["journal_offset"])
        expected = status["source_book_digest"]
        actual = {key:digest(value) for key,value in replay.items()}
        keys = set(expected)|set(actual)
        mismatches = [key for key in keys if expected.get(key) != actual.get(key)]
        raw_ids = [(r.stream,r.body["connection"],r.body["recv_seq"]) for r in records if r.kind=="RAW"]
        raw_refs = [r.body["raw_ref"]["capture_offset"] for r in records if r.kind=="OBSERVATION_RESULT"]
        raw_offsets = {r.offset for r in records if r.kind=="RAW"}
        orphan = sorted(raw_offsets-set(raw_refs))
        checks[plant] = {"source_book_replay":"PASS" if not mismatches else "FAIL",
            "source_book_mismatches":mismatches,"raw_unique_identity":len(set(raw_ids))==len(raw_ids),
            "raw_dispositions_missing":orphan,"counts":counts,
            "max_sync_batch_records":status["max_sync_batch_records"],
            "parse_errors":status["parse_errors"],"raw_bytes":status["raw_bytes"]}
        live_cut=root/f"live-cut-{plant}.json"
        sources[plant] = json.loads(live_cut.read_bytes()) if live_cut.exists() else status
        transitions = [r.body for r in all_records(root/plant) if r.kind=="TOKEN_TRANSITION"]
        if transitions:
            sources[plant]["coverage_transitions"] = sorted(transitions,key=lambda row:row["mono_ns"])
    # A killed process cannot write its own final false transition. The independent
    # fault controller supplies the observed outage boundary; never bridge it.
    fault_path=root/"faults.json"
    if fault_path.exists():
        for fault in json.loads(fault_path.read_bytes()):
            if fault["kind"]=="PLANT_SIGKILL_RESTART":
                plant=fault["plant"]
                for state in sources[plant]["token_states"]:
                    sources[plant]["coverage_transitions"].append({"key":[state["shard"],state["lane"],state["token"]],
                        "ready":False,"mono_ns":fault["mono_ns"],"utc_ns":None,"reason":"EXTERNAL_SIGKILL_OBSERVED"})
                sources[plant]["coverage_transitions"].sort(key=lambda row:row["mono_ns"])
    gateway_status = json.loads((root/"G"/"before_shutdown.json").read_bytes())
    tape_records = list(all_records(root/"G"))
    states,seqs,epochs,routes = {},{},{},{}
    sequence_gaps, route_errors, corrupt = [],[],[]
    for rec in tape_records:
        if rec.kind != "TAPE" or rec.stream != gateway_status["stream"] or rec.offset > gateway_status["durable_offset"]:
            continue
        body,market = rec.body,rec.body["market"]
        if body["prev_market_seq"] != seqs.get(market,0) or body["market_seq"] != seqs.get(market,0)+1:
            sequence_gaps.append(rec.offset)
        if body["kind"] == "SOURCE_RESET":
            if set(body["assets"]) != set(body["scope_assets"]): corrupt.append(rec.offset)
            epochs[market],routes[market] = body["book_epoch"],body["route"]
        elif body["book_epoch"] != epochs.get(market) or body["route"] != routes.get(market):
            route_errors.append(rec.offset)
        states[market] = apply(states.get(market,{}),body)
        seqs[market] = body["market_seq"]
    tape_digest = {m:digest(s) for m,s in states.items()}
    checks["gateway"] = {"market_sequence_gaps":sequence_gaps,"route_qualification_errors":route_errors,
        "incomplete_resets":corrupt,"committed_replay_equal":tape_digest==gateway_status["committed_digest"],
        "records":sum(1 for r in tape_records if r.kind=="TAPE"),
        "source_resets":sum(1 for r in tape_records if r.kind=="TAPE" and r.body["kind"]=="SOURCE_RESET"),
        "generations":sorted({r.stream for r in tape_records}),
        "max_sync_batch_records":gateway_status["max_sync_batch_records"]}
    live_gateway=root/"live-cut-G.json"
    live_status=json.loads(live_gateway.read_bytes()) if live_gateway.exists() else gateway_status
    expected_markets={m["market"] for m in scope["markets"]}
    ready_markets={market for market,books in live_status["books"].items()
                   if len(books)==len(next(m["assets"] for m in scope["markets"] if m["market"]==market))
                   and all(book["ready"] for book in books.values())}
    checks["gateway"].update(expected_markets=len(expected_markets),committed_ready_markets_at_cut=len(ready_markets),
        ready_tokens_at_cut=sum(len(m["assets"]) for m in scope["markets"] if m["market"] in ready_markets),
        missing_markets_at_cut=sorted(expected_markets-ready_markets),
        publication_scope_complete=ready_markets==expected_markets)
    run = json.loads((root/"acceptance_config.json").read_bytes())
    cov = coverage(sources,scope,run["start_mono_ns"],min(s["mono_ns"] for s in sources.values()))
    atomic_json(root/"coverage.json",cov)
    atomic_json(root/"audit.json",checks)
    return {"checks":checks,"coverage":cov}


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1",0))
        return sock.getsockname()[1]


async def live_acceptance(args):
    root = Path(args.root).resolve()
    root.mkdir(parents=True,exist_ok=False)
    shutil.copyfile(args.scope,root/"scope.json")
    base = json.loads((Path(__file__).parents[1]/"config/base.json").read_bytes())
    base.update(lanes=args.lanes,assets_per_connection=args.capacity)
    ports = {name:unused_port() for name in ("P","Q","G")}
    # Isolated self-signed localhost CA/cert for mutual TLS in this run only.
    cert_dir = root/"tls"
    cert_dir.mkdir()
    subprocess.run(["openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","2",
        "-subj","/CN=localhost","-addext","subjectAltName=DNS:localhost,IP:127.0.0.1",
        "-keyout",str(cert_dir/"node.key"),"-out",str(cert_dir/"node.crt")],check=True,capture_output=True)
    os.chmod(cert_dir/"node.key",0o600)
    tls = {"ca":str(cert_dir/"node.crt"),"cert":str(cert_dir/"node.crt"),"key":str(cert_dir/"node.key")}
    for name in ("P","Q","G"):
        config = deepcopy(base)
        config["archive"]={"store_uri":str(root/"object-store"),"poll_seconds":2}
        config["listen"].update(port=ports[name],tls=tls)
        if name=="G":
            config["plants"] = {p:{"host":"127.0.0.1","port":ports[p],"tls":tls} for p in ("P","Q")}
        atomic_json(root/f"config-{name}.json",config)
    start = time.monotonic_ns()
    atomic_json(root/"acceptance_config.json",{"start_mono_ns":start,"start_utc_ns":time.time_ns(),
        "duration_seconds":args.seconds,"lanes":args.lanes,"capacity":args.capacity,"faults":args.faults,
        "host":platform.platform(),"python":sys.version,"network":"existing HTTPS_PROXY; official WS endpoint",
        "failure_domains":"three processes on same host and same existing egress; no host independence claim",
        "source_sha256":{str(p.relative_to(launcher_root())):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted([*launcher_root().glob("lobv2/*.py"),launcher_root()/"envelope.proto",launcher_root()/"requirements.lock"])} })
    processes,logs = {},{}
    consumer_stop=asyncio.Event()
    consumer_endpoint={"host":"127.0.0.1","port":ports["G"],"tls":tls}
    consumer_task=asyncio.create_task(record_consumer(consumer_endpoint,root/"consumer",consumer_stop))
    launcher = Path(__file__).parents[1]/"lobctl"
    async def start_node(name):
        logs[name] = (root/f"{name}.log").open("ab")
        processes[name] = await asyncio.create_subprocess_exec(sys.executable,str(launcher),
            "gateway" if name=="G" else "capture","--id",name,"--scope",str(root/"scope.json"),
            "--config",str(root/f"config-{name}.json"),"--root",str(root/name),
            stdout=logs[name],stderr=logs[name])
    faults = []
    async def wait_live(duration):
        deadline = time.monotonic()+duration
        while time.monotonic()<deadline:
            for name,proc in processes.items():
                if proc.returncode is not None:
                    raise RuntimeError(f"NODE_EXITED {name} {proc.returncode}; see {root/name}.log")
            await asyncio.sleep(min(1,max(0,deadline-time.monotonic())))
    try:
        for name in ("P","Q","G"): await start_node(name)
        if args.faults:
            await wait_live(args.seconds*.4)
            # SIGKILL one selected plant, then restart. This is not a power-loss test.
            selected_before = json.loads((root/"G/status.json").read_bytes()).get("selected",{})
            victim = Counter(selected_before.values()).most_common(1)[0][0] if selected_before else "P"
            killed = time.monotonic_ns()
            processes[victim].kill()
            await processes[victim].wait()
            logs[victim].close()
            processes.pop(victim)
            await wait_live(3)
            await start_node(victim)
            faults.append({"kind":"PLANT_SIGKILL_RESTART","plant":victim,"mono_ns":killed,
                           "selected_before":selected_before})
            await wait_live(args.seconds*.3)
            old = json.loads((root/"G/status.json").read_bytes())["stream"]
            processes["G"].kill()
            await processes["G"].wait()
            logs["G"].close()
            processes.pop("G")
            await start_node("G")
            faults.append({"kind":"GATEWAY_SIGKILL_RESTART","old_tape":old,"mono_ns":time.monotonic_ns()})
            await wait_live(max(10,args.seconds*.3-3))
        else:
            await wait_live(args.seconds)
    finally:
        atomic_json(root/"faults.json",faults)
        # Preserve a common live cut before sending any shutdown signals.
        for name in ("P","Q","G"):
            path=root/name/"status.json"
            if path.exists(): shutil.copyfile(path,root/f"live-cut-{name}.json")
        consumer_stop.set()
        consumer_task.cancel()
        await asyncio.gather(consumer_task,return_exceptions=True)
        # Gateway first, so plant shutdown does not make the sampled tape unavailable.
        for name in ("G","P","Q"):
            proc=processes.get(name)
            if proc and proc.returncode is None:
                proc.terminate()
                try: await asyncio.wait_for(proc.wait(),20)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
            if name in logs: logs[name].close()
    audit = audit_run(root)
    consumer=verify_consumer(root)
    query = export_parquet(root/"G",root/"query")
    store = DirectoryStore(root/"object-store")
    objects,totals = [],Counter()
    archive_start=time.monotonic()
    for name in ("P","Q","G"):
        manifests=defaultdict(list)
        for journal in sorted((root/name/"journal").glob("*.journal")):
            records=list(read_journal(journal))
            if not records: continue
            compressed=journal.with_suffix(".lobseg")
            metadata=build_archive(journal,compressed)
            key,manifest=commit_archive(store,compressed,"tape" if name=="G" else "capture")
            restored=root/"restored"/name/compressed.name
            restore(store,key,restored)
            reread=list(read_archive(restored))
            if [r.frame for r in records] != [r.frame for r in reread]:
                raise ValueError("ARCHIVE_FRAME_FIDELITY_FAILED")
            totals["journal_bytes"]+=journal.stat().st_size
            totals["archive_bytes"]+=compressed.stat().st_size
            totals["records"]+=len(records)
            manifests[manifest["stream"]].append(manifest)
            objects.append({"manifest":key,"node":name,"first":manifest["first"],"last":manifest["last"],
                "journal_bytes":journal.stat().st_size,"archive_bytes":compressed.stat().st_size,
                "frame_identity":"PASS","sha256":manifest["object_sha256"]})
        for stream,items in manifests.items():
            atomic_json(root/name/f"archive-progress-{stream}.json",progress(items,max(m["last"] for m in items)))
    totals["archive_seconds"]=time.monotonic()-archive_start
    totals["compression_ratio"]=totals["journal_bytes"]/totals["archive_bytes"] if totals["archive_bytes"] else 0
    # Exercise actual deletion only on a sealed, isolated copy; keep the acceptance source evidence.
    first_journal=next((root/"P/journal").glob("*.journal"))
    cleanup=root/"cleanup-proof"
    cleanup.mkdir()
    copy=cleanup/first_journal.name
    shutil.copyfile(first_journal,copy)
    packed=copy.with_suffix(".lobseg")
    build_archive(copy,packed)
    key,_=commit_archive(store,packed,"capture")
    receipt=evict(copy,packed,store,key,active_dependencies=[])
    restore(store,key,cleanup/"recovered-after-deletion.lobseg")
    atomic_json(root/"storage.json",{"objects":objects,"totals":dict(totals),"cleanup":receipt,
        "directory_backend":"PASS","REMOTE_ARCHIVED":"NOT_EVALUATED_NO_REMOTE_TEST_BUCKET",
        "permanent_host_loss":"NOT_EVALUATED","power_loss":"NOT_EVALUATED"})
    export_audits(root,objects)
    result={"run":str(root),"audit":audit["checks"],"storage":dict(totals),"query":query,
        "consumer":consumer,"coverage":{k:v for k,v in audit["coverage"].items() if k!="at_cut"}}
    atomic_json(root/"result.json",result)
    return result


def launcher_root():
    return Path(__file__).parents[1]


async def record_consumer(endpoint,root,stop):
    """Actual formal consumer: store the same frames delivered by durable publisher."""
    from .node import receive_snapshot,tls_context,confirm_snapshot
    from .wire import control,read_frame,send
    root.mkdir(parents=True,exist_ok=True)
    attempts=0
    while not stop.is_set():
        writer=None
        attempts+=1
        try:
            ctx=tls_context(endpoint)
            reader,writer=await asyncio.open_connection(endpoint["host"],endpoint["port"],ssl=ctx,
                server_hostname="localhost" if ctx else None)
            await send(writer,control("HELLO",{"schema":2,"role":"consumer"}))
            snapshot,session=await receive_snapshot(reader)
            await confirm_snapshot(reader,writer,snapshot,session)
            atomic_json(root/f"bootstrap-{attempts}.json",snapshot)
            states={item["market"]:item["assets"] for item in snapshot["items"]}
            seqs={item["market"]:item["cut"] for item in snapshot["items"]}
            batch_records=0
            with (root/f"received-{attempts}.frames").open("xb") as frames, (root/f"receipts-{attempts}.jsonl").open("x") as receipts:
                try:
                    while not stop.is_set():
                        data=await asyncio.wait_for(read_frame(reader),20)
                        recv_utc,recv_mono=time.time_ns(),time.monotonic_ns()
                        rec=decode(data)
                        if rec.kind=="TAPE":
                            body,market=rec.body,rec.body["market"]
                            if body["prev_market_seq"]!=seqs.get(market,0):
                                raise ValueError("CONSUMER_MARKET_SEQUENCE_GAP")
                            states[market]=apply(states.get(market,{}),body)
                            seqs[market]=body["market_seq"]
                            frames.write(data)
                            receipts.write(json.dumps({"stream":rec.stream,"offset":rec.offset,
                                "receive_utc_ns":recv_utc,"receive_mono_ns":recv_mono})+"\n")
                            batch_records+=1
                            if batch_records>128:raise ValueError("CONSUMER_FLOW_BUDGET")
                            await asyncio.sleep(0)
                        elif rec.kind=="FLOW_BATCH_END":
                            if rec.body!={"session":session,"records":batch_records} or not batch_records:
                                raise ValueError("CONSUMER_FLOW_MISMATCH")
                            await send(writer,control("FLOW_ACK",rec.body))
                            batch_records=0
                        elif rec.kind=="KEEPALIVE":
                            if batch_records:raise ValueError("CONSUMER_INCOMPLETE_BATCH")
                            await send(writer,control("FLOW_ACK",{"session":session}))
                        else:
                            raise ValueError("CONSUMER_RESYNC_REQUIRED")
                finally:
                    frames.flush(); os.fsync(frames.fileno())
                    receipts.flush(); os.fsync(receipts.fileno())
        except (OSError,ValueError,asyncio.TimeoutError,asyncio.IncompleteReadError) as exc:
            atomic_json(root/f"connection-{attempts}.json",{"error":str(exc),"at_ns":time.time_ns()})
        finally:
            if writer:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        try: await asyncio.wait_for(stop.wait(),.5)
        except asyncio.TimeoutError: pass


def verify_consumer(root):
    from .wire import FRAME,MAX_RECORD
    # Index only received identities; bounded by this intentionally small acceptance run.
    wanted={}
    for path in (root/"consumer").glob("*.frames"):
        with path.open("rb") as src:
            while True:
                head=src.read(FRAME.size)
                if not head: break
                length,_=FRAME.unpack(head)
                if length+FRAME.size>MAX_RECORD: raise ValueError("CONSUMER_RECEIPT_LENGTH")
                rec=decode(head+src.read(length))
                key=(rec.stream,rec.offset)
                if key in wanted and wanted[key]!=rec.frame:
                    raise ValueError("CONSUMER_IDENTITY_CONFLICT")
                wanted[key]=rec.frame
    compared=0
    for rec in all_records(root/"G"):
        key=(rec.stream,rec.offset)
        if key in wanted:
            if wanted[key]!=rec.frame: raise ValueError("LIVE_TAPE_FRAME_MISMATCH")
            compared+=1
    if compared!=len(wanted) or not compared:
        raise ValueError("CONSUMER_NO_MATCHED_TAPE_FRAMES")
    receipt={"received_frames":compared,"byte_fidelity":"PASS", "scope":"actual consumer receipts only"}
    atomic_json(root/"consumer-fidelity.json",receipt)
    return receipt


def export_audits(root,objects):
    import pyarrow as pa
    import pyarrow.parquet as pq
    scope=json.loads((root/"scope.json").read_bytes())
    rows,gaps,pairs,observations=[],[],[],[]
    for plant in ("P","Q"):
        live_cut=root/f"live-cut-{plant}.json"
        status=json.loads((live_cut if live_cut.exists() else root/plant/"before_shutdown.json").read_bytes())
        states=status["token_states"]
        for state in states:
            row={"plant":plant,"token":state["token"],"market":state["market"],"lane":state["lane"],
                 "connection":state["connection"],"assigned":state["assigned"],"written":state["written"],
                 "book_seen":state["book_seen"],"baseline_durable":state["baseline_durable"],
                 "ready":state["ready"],"status":state["status"],
                 "baseline_ref_json":json.dumps(state.get("baseline_ref")),"cut_mono_ns":status["mono_ns"]}
            rows.append(row)
            if not state["ready"]: gaps.append({**row,"reason":state["status"]})
        for lane in sorted({s["lane"] for s in states}):
            by_token={s["token"]:s for s in states if s["lane"]==lane}
            for market in scope["markets"]:
                selected=[by_token[t] for t in market["assets"]]
                pairs.append({"plant":plant,"lane":lane,"market":market["market"],
                    "pair_ready":all(s["ready"] for s in selected),
                    "pair_colocated":len({s["connection"] for s in selected})==1,
                    "tokens":market["assets"]})
        records=list(all_records(root/plant))
        dispositions={(r.stream,r.body["raw_ref"]["capture_offset"]):r.body["results"]
                      for r in records if r.kind=="OBSERVATION_RESULT"}
        for rec in records:
            if rec.kind!="RAW":continue
            refs=[o["manifest"] for o in objects if o["node"]==plant and rec.stream in o["manifest"] and o["first"]<=rec.offset<=o["last"]]
            observations.append({"plant":plant,"stream":rec.stream,"offset":rec.offset,
                "connection":rec.body["connection"],"recv_seq":rec.body["recv_seq"],"raw_bytes":len(rec.raw),
                "raw_sha256":hashlib.sha256(rec.raw).hexdigest(),"archive_manifests":refs,
                "disposition_json":json.dumps(dispositions.get((rec.stream,rec.offset),["NORMALIZATION_PENDING_CRASH_RECOVERED"]))})
    for name,data in (("token_state_at_cut",rows),("token_gap_attribution",gaps),
                      ("market_gap_attribution",pairs),("archive_observation_audit",observations)):
        # Empty gap tables retain the token schema, making an empty result unambiguous.
        table=pa.Table.from_pylist(data) if data else pa.Table.from_pylist(rows[:1]).slice(0,0)
        pq.write_table(table,root/f"{name}.parquet",compression="zstd")
