#!/usr/bin/env python3
"""Replay a fixed CaptureLog at controlled admission rates, with real group sync."""
import asyncio
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/".vendor"),str(ROOT)]
from lobv2.core import Plant
from lobv2.journal import Journal,atomic_json,read_journal
from lobv2.node import quantiles
from lobv2.archive import read_archive


async def benchmark(source,output,scope_path):
    scope=json.loads(Path(scope_path).read_bytes())
    all_input=list(read_archive(source)) if str(source).endswith(".lobseg") else list(read_journal(source))
    raws=[rec for rec in all_input if rec.kind=="RAW" and rec.raw!=b"PONG"]
    connections=Counter(rec.body["connection"] for rec in raws)
    connection=connections.most_common(1)[0][0]
    sample=[rec for rec in raws if rec.body["connection"]==connection]
    # Freeze observed source order and bytes. This is admission-rate stress,
    # not a claim of latency equivalence to accelerated wall-clock histories.
    input_sha=hashlib.sha256(b"".join(rec.raw for rec in sample)).hexdigest()
    observed_span=max(.001,(sample[-1].body["recv_mono_ns"]-sample[0].body["recv_mono_ns"])/1e9)
    base_rate=len(sample)/observed_span
    results=[]
    for multiplier in (1,1.5,2):
        journal=Journal(output/f"rate-{multiplier}","BENCH","capture")
        plant=Plant(journal,scope,1)
        plant.connect(0,0,"benchmark",1,scope["markets"])
        plant.written(0,0,"benchmark",[t for m in scope["markets"] for t in m["assets"]],"bench")
        stop=asyncio.Event()
        writer=asyncio.create_task(journal.run(stop,.020))
        # A representative 3-second slice at each rate keeps this bounded.
        rate=max(base_rate,150)*multiplier
        started=time.monotonic()
        groups=0
        first=sample[0].raw
        plant.ingest(0,0,"benchmark",1,first)
        candidates=sample[1:]
        if not candidates: raise ValueError("NO_DELTA_SAMPLE")
        for i in range(min(2000,int(rate*3))):
            deadline=started+(i+1)/rate
            await asyncio.sleep(max(0,deadline-time.monotonic()))
            rec=candidates[i%len(candidates)]
            plant.ingest(0,0,"benchmark",i+2,rec.raw)
            await asyncio.sleep(0)
            groups+=1
        stop.set(); await writer; await journal.close()
        elapsed=time.monotonic()-started
        results.append({"multiplier":multiplier,"offered_messages_per_second":rate,"admitted_messages":groups,"elapsed_seconds":elapsed,
            "messages_per_second":groups/elapsed,"syncs":journal.syncs,"max_sync_batch_records":journal.max_sync_batch,
            "decision_to_durable_ms":quantiles(plant.latencies_ms),"record_identity_count":journal.offset,
            "raw_received":plant.raw_received,"normalization_errors":plant.parse_errors,
            "pending_bytes_after_drain":journal.pending_size,"emitted_complete_groups":plant.emits})
    receipt={"input_file":str(source),"fixed_source_raw_sha256":input_sha,"source_messages":len(sample),
        "baseline_admission_messages_per_second":max(base_rate,150),
        "original_span_seconds":observed_span,"original_messages_per_second":base_rate,
        "mode":"CONTROLLED_ADMISSION_RATE_NOT_ORIGINAL_WALLCLOCK", "results":results}
    atomic_json(output/"benchmark.json",receipt)
    return receipt


if __name__=="__main__":
    print(json.dumps(asyncio.run(benchmark(Path(sys.argv[1]),Path(sys.argv[2]),Path(sys.argv[3]))),indent=2))
