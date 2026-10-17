#!/usr/bin/env python3
"""Read committed remote archives and compare actual consumer frames byte for byte."""
import argparse
import json
from pathlib import Path
import sys
import time

PACKAGE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")),str(PACKAGE)]
from lobv2.archive import restore,read_archive
from lobv2.journal import atomic_json
from lobv2.ssh_store import SSHStore
from lobv2.wire import FRAME,decode


def verify(deployment, consumers, output):
    output.mkdir(parents=True,exist_ok=True)
    settings=json.loads((deployment/"settings.json").read_bytes())
    store=SSHStore(settings["archive"]["ssh_store"])
    results=[]
    for node in sorted(consumers.glob("G-*")):
        wanted={}
        for path in node.glob("*.frames"):
            with path.open("rb") as src:
                while header:=src.read(FRAME.size):
                    length,_=FRAME.unpack(header)
                    frame=header+src.read(length)
                    record=decode(frame)
                    wanted[(record.stream,record.offset)]=frame
        matched=set()
        segments=[]
        for path in (Path(settings["data_root"])/node.name/"journal").glob("*.archive.json"):
            receipt=json.loads(path.read_bytes())
            manifest=receipt["manifest"]
            if not any(s==manifest["stream"] and manifest["first"]<=o<=manifest["last"] for s,o in wanted):
                continue
            target=output/(node.name+"-"+manifest["stream"]+"-"+str(manifest["first"])+".lobseg")
            restore(store,receipt["manifest_key"],target)
            for record in read_archive(target):
                key=(record.stream,record.offset)
                if key in wanted:
                    if record.frame!=wanted[key]:raise ValueError("ORIGINAL_FRAME_MISMATCH")
                    matched.add(key)
            segments.append({"manifest_key":receipt["manifest_key"],"records":manifest["count"],
                "local_cache_evicted":bool(receipt.get("eviction"))})
        errors=[json.loads(p.read_bytes()) for p in node.glob("connection-*.json")]
        results.append({"node":node.name,"consumer_frames":len(wanted),"matched_original_frames":len(matched),
            "missing_original_frames":len(wanted)-len(matched),"connection_errors":errors,"segments":segments})
    report={"utc_ns":time.time_ns(),"results":results,
        "pass":bool(results) and all(r["consumer_frames"]>0 and not r["missing_original_frames"] and not r["connection_errors"] for r in results)}
    atomic_json(output/"report.json",report)
    print(json.dumps({"pass":report["pass"],"nodes":len(results),"consumer_frames":sum(r["consumer_frames"] for r in results),
        "matched_original_frames":sum(r["matched_original_frames"] for r in results),"missing_original_frames":sum(r["missing_original_frames"] for r in results)},indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--deployment",type=Path,required=True)
    parser.add_argument("--consumers",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    verify(args.deployment,args.consumers,args.output)
