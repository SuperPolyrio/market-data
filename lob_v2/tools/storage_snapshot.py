#!/usr/bin/env python3
"""Count actual local sealed/open backlog independently of asynchronous metrics."""
import argparse
import json
from pathlib import Path
import time


def snapshot(root):
    result={"utc_ns":time.time_ns(),"sealed_pending_bytes":0,"open_journal_bytes":0,
        "journal_bytes_committed":0,"object_bytes_committed":0,"evicted_bytes":0,"receipts":0,
        "oldest_sealed_seconds":0,"holes_before_last_commit":0}
    for directory in root.glob("[PQG]-*"):
        journal=directory/"journal"
        for path in journal.glob("*.archive.json"):
            receipt=json.loads(path.read_bytes())
            result["receipts"]+=1
            result["journal_bytes_committed"]+=receipt.get("local_journal_bytes",0)
            result["object_bytes_committed"]+=receipt["manifest"]["object_bytes"]
            result["evicted_bytes"]+=receipt.get("eviction",{}).get("released_bytes",0)
        for path in journal.glob("*.journal"):
            if not path.with_suffix(".archive.json").exists():
                try:
                    stat=path.stat()
                except FileNotFoundError:continue
                result["sealed_pending_bytes"]+=stat.st_size
                result["oldest_sealed_seconds"]=max(result["oldest_sealed_seconds"],time.time()-stat.st_mtime)
        for path in journal.glob("*.open"):
            try:result["open_journal_bytes"]+=path.stat().st_size
            except FileNotFoundError:pass
        progress=directory/"archive-progress.json"
        if progress.exists():
            result["holes_before_last_commit"]+=sum(len(s["holes_before_last_commit"]) for s in json.loads(progress.read_bytes())["streams"].values())
    return result


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--history",type=Path)
    args=parser.parse_args()
    result=snapshot(args.root)
    if args.history:
        with args.history.open("a") as out:out.write(json.dumps(result,separators=(",",":"))+"\n")
    print(json.dumps(result,indent=2))
