#!/usr/bin/env python3
"""Freeze acceptance evidence and produce measurements, without promoting unknowns."""
from collections import Counter,defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/".vendor"),str(ROOT)]
from lobv2.journal import atomic_json
from lobv2.validation import all_records

NAMES=["accepted-2x1","accepted-2x2","accepted-faults","accepted-capacity-100",
       "accepted-capacity-250","accepted-capacity-500","current-build-2x2",
       "batched-2x1","batched-2x2","batched-faults","batched-capacity-500"]


def measurements(name):
    root=ROOT/"runs"/name
    result=json.loads((root/"result.json").read_bytes())
    config=json.loads((root/"acceptance_config.json").read_bytes())
    states={n:json.loads((root/f"live-cut-{n}.json").read_bytes()) for n in ("P","Q","G")}
    delays=[]
    first_book=[]
    for plant in ("P","Q"):
        for state in states[plant]["token_states"]:
            if state.get("book_seen_ns") and state.get("write_succeeded_ns"):
                first_book.append((state["book_seen_ns"]-state["write_succeeded_ns"])/1e9)
    terminal=defaultdict(dict)
    sequence_errors=[]
    all_tape_count=0
    pending_raw=0
    for plant in ("P","Q"):
        records=list(all_records(root/plant))
        processed={(r.stream,r.body["raw_ref"]["capture_offset"]) for r in records if r.kind=="OBSERVATION_RESULT"}
        pending_raw+=sum(r.kind=="RAW" and (r.stream,r.offset) not in processed for r in records)
    for rec in all_records(root/"G"):
        if rec.kind!="TAPE":continue
        all_tape_count+=1
        key=(rec.stream,rec.body["market"])
        previous=terminal[key].get("market_seq",0)
        if rec.body["prev_market_seq"]!=previous or rec.body["market_seq"]!=previous+1:
            sequence_errors.append([rec.stream,rec.offset])
        terminal[key]=rec.body
    faults=json.loads((root/"faults.json").read_bytes())
    kill=next((f for f in faults if f["kind"]=="PLANT_SIGKILL_RESTART"),None)
    if kill:
        resets=[r for r in all_records(root/"G") if r.kind=="TAPE" and r.body.get("reason")=="PLANT_DISCONNECT"
                and r.body.get("kind")=="SOURCE_RESET" and r.body["decision_mono_ns"]>=kill["mono_ns"]]
        delays=[(r.body["decision_mono_ns"]-kill["mono_ns"])/1e6 for r in resets]
    target=ROOT/"evidence"/name
    target.mkdir(parents=True,exist_ok=True)
    for file in root.iterdir():
        if file.is_file() and file.suffix in (".json",".parquet",".raw"):
            shutil.copyfile(file,target/file.name)
    for n in ("P","Q","G"):
        (target/n).mkdir(exist_ok=True)
        for filename in ("run.json","before_shutdown.json","final.json"):
            shutil.copyfile(root/n/filename,target/n/filename)
        for receipt in (root/n).glob("archive-progress-*.json"):
            shutil.copyfile(receipt,target/n/receipt.name)
    shutil.copytree(root/"object-store",target/"object-store",dirs_exist_ok=True)
    shutil.copytree(root/"consumer",target/"consumer",dirs_exist_ok=True)
    row={"name":name,"started_at":datetime.fromtimestamp(config["start_utc_ns"]/1e9,ZoneInfo("Asia/Shanghai")).isoformat(),
        "expected_tokens":result["coverage"]["expected_tokens"],"lanes_per_plant":config["lanes"],
        "window_seconds":(result["coverage"]["window_end_mono_ns"]-result["coverage"]["window_start_mono_ns"])/1e9,
        "source_written_by_route":{p:{str(lane):sum(s["written"] for s in state["token_states"] if s["lane"]==lane)
            for lane in range(config["lanes"])} for p,state in states.items() if p!="G"},
        "source_ready_by_route":{p:{str(lane):sum(s["ready"] for s in state["token_states"] if s["lane"]==lane)
            for lane in range(config["lanes"])} for p,state in states.items() if p!="G"},
        "gateway_ready_tokens":result["audit"]["gateway"]["ready_tokens_at_cut"],
        "one_plant_coverage":result["coverage"]["one_plant_token_coverage"],
        "both_plants_coverage":result["coverage"]["both_plants_token_coverage"],
        "raw_bytes":sum(s.get("raw_bytes",0) for s in states.values()),
        "journal_bytes":result["storage"]["journal_bytes"],"archive_bytes":result["storage"]["archive_bytes"],
        "compression_ratio":result["storage"]["compression_ratio"],
        "source_books_equal":all(result["audit"][p]["source_book_replay"]=="PASS" for p in ("P","Q")),
        "native_bbo_comparisons":sum(result["audit"][p]["counts"].get("bbo_comparisons",0) for p in ("P","Q")),
        "native_bbo_mismatches":sum(result["audit"][p]["counts"].get("bbo_mismatches",0) for p in ("P","Q")),
        "consumer_frames":result["consumer"]["received_frames"],"tape_records_all_generations":all_tape_count,
        "tape_sequence_errors_all_generations":sequence_errors,"normalization_pending_all_streams":pending_raw,
        "source_book_wait_seconds":{"min":min(first_book) if first_book else None,"max":max(first_book) if first_book else None},
        "max_rss_sum_bytes":sum(s["resources"]["max_rss_bytes"] for s in states.values()),
        "cpu_seconds_sum":sum(s["resources"]["cpu_user_seconds"]+s["resources"]["cpu_system_seconds"] for s in states.values()),
        "decision_to_durable_p99_ms":{n:s["publish_latency_ms"]["p99"] for n,s in states.items()},
        "plant_kill_to_reset_creation_ms":{"min":min(delays),"max":max(delays)} if delays else None,
        "REMOTE_ARCHIVED":"NOT_EVALUATED","production_readiness":"NOT_CERTIFIED"}
    atomic_json(target/"measurements.json",row)
    return row


if __name__=="__main__":
    rows=[measurements(name) for name in NAMES if (ROOT/"runs"/name/"result.json").exists()]
    atomic_json(ROOT/"evidence/measurements.json",rows)
    build={"code_version":"2.0.0","verifier_sha256":hashlib.sha256((ROOT/"lobv2/validation.py").read_bytes()).hexdigest(),
        "files":{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.rglob("*.py"))
                 if not any(part in (".vendor",".venv","runs","evidence","__pycache__") for part in p.relative_to(ROOT).parts)},
        "schema_sha256":hashlib.sha256((ROOT/"envelope.proto").read_bytes()).hexdigest(),
        "requirements_sha256":hashlib.sha256((ROOT/"requirements.installed.lock").read_bytes()).hexdigest()}
    atomic_json(ROOT/"evidence/build.json",build)
    print(json.dumps(rows,indent=2))
