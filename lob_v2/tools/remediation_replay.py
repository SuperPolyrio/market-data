#!/usr/bin/env python3
"""Compare every complete production Tape cut against a frozen implementation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

PACKAGE=Path(__file__).resolve().parents[1]


def worker(args):
    sys.path[:0]=[str(args.package),str(PACKAGE/".vendor312")]
    import lobv2.book as book
    from lobv2.archive import read_archive
    from lobv2.wire import json_bytes
    working,committed={},{}
    samples=[]
    sha=hashlib.sha256()
    counts={}
    skipped=0
    started=time.monotonic_ns()
    with args.output.with_suffix(".cuts.jsonl").open("w") as cuts:
        for rec in read_archive(args.input):
            if rec.kind!="TAPE":continue
            body=rec.body
            market=body["market"]
            if market not in committed and body["kind"]!="SOURCE_RESET":
                skipped+=1
                continue
            before=time.monotonic_ns()
            if hasattr(book,"apply_state"):
                prepared=book.prepare_record(body)
                working[market]=book.apply_state(working.get(market,{}),body,prepared)
                committed[market]=book.apply_state(committed.get(market,{}),body,prepared)
            else:
                working[market]=book.apply(working.get(market,{}),body)
                committed[market]=book.apply(committed.get(market,{}),body)
            elapsed=(time.monotonic_ns()-before)/1e6
            samples.append(elapsed)
            counts[body["kind"]]=counts.get(body["kind"],0)+1
            state=book.state_view(committed[market]) if hasattr(book,"state_view") else committed[market]
            value=json_bytes(state)
            sha.update(rec.frame)
            sha.update(value)
            cuts.write(json.dumps({"stream":rec.stream,"offset":rec.offset,"market":market,
                "market_seq":body["market_seq"],"book_epoch":body["book_epoch"],"state_sha256":hashlib.sha256(value).hexdigest()})+"\n")
    ordered=sorted(samples)
    result={"records":len(samples),"markets":len(committed),"kinds":counts,"skipped_before_reset":skipped,
        "input_sha256":hashlib.sha256(args.input.read_bytes()).hexdigest(),"every_cut_and_frame_sha256":sha.hexdigest(),
        "book_apply_ms":{label:ordered[int((len(ordered)-1)*fraction)] for label,fraction in (("p50",.5),("p95",.95),("p99",.99))},
        "book_apply_sum_ms":sum(samples),"max_ms":max(samples),"elapsed_seconds":(time.monotonic_ns()-started)/1e9,
        "measurement_scope":"prepare once and apply working+committed; excludes network, JSON decoding, journal I/O and verification hashing"}
    args.output.write_text(json.dumps(result,indent=2))
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--input",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--baseline",type=Path)
    p.add_argument("--package",type=Path)
    args=p.parse_args()
    if args.package:
        worker(args)
        return
    args.output.mkdir(parents=True,exist_ok=True)
    results={}
    for name,package in (("before",args.baseline),("after",PACKAGE)):
        output=args.output/(name+".json")
        subprocess.run([sys.executable,__file__,"--input",str(args.input),"--output",str(output),"--package",str(package)],check=True)
        results[name]=json.loads(output.read_bytes())
    same=results["before"]["every_cut_and_frame_sha256"]==results["after"]["every_cut_and_frame_sha256"]
    results.update(every_committed_cut_identical=same,book_apply_speedup=results["before"]["book_apply_sum_ms"]/results["after"]["book_apply_sum_ms"])
    (args.output/"report.json").write_text(json.dumps(results,indent=2))
    print(json.dumps(results,indent=2))
    if not same:raise SystemExit(1)


if __name__=="__main__":main()
