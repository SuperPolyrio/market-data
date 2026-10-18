#!/usr/bin/env python3
"""Replay an unchanged production Tape arrival schedule on the production disk."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

PACKAGE=Path(__file__).resolve().parents[1]


async def worker(args):
    sys.path[:0]=[str(args.package),str(PACKAGE/'.vendor312')]
    from lobv2.archive import read_archive
    from lobv2.core import Owner
    from lobv2.journal import Journal
    values=[r for r in read_archive(args.input) if r.kind=='TAPE']
    first=values[0].created_ns
    values=[r for r in values if r.created_ns-first<args.seconds*1e9]
    identities={r.body['market']:{'market':r.body['market'],'assets':r.body['scope_assets'],
        'version':r.body['market_version'],'asset_id_kind':r.body['asset_id_kind']} for r in values}
    root=args.data_root/str(uuid.uuid4());root.mkdir(parents=True)
    j=Journal(root,'OFFLINE_REPLAY','benchmark',pending_bytes=256*1024*1024)
    owner=Owner(j,{'markets':list(identities.values())},'BENCH_TAPE')
    owner.provenance={'mode':'OFFLINE_FROZEN_TAPE_REPLAY','input_sha256':hashlib.sha256(args.input.read_bytes()).hexdigest()}
    stop=asyncio.Event();sync=asyncio.create_task(j.run(stop,args.window/1000))
    start=time.monotonic_ns();producer_lags=[]
    try:
        for rec in values:
            deadline=start+rec.created_ns-first
            remaining=(deadline-time.monotonic_ns())/1e9
            if remaining>0:await asyncio.sleep(remaining)
            producer_lags.append(max(0.,(time.monotonic_ns()-deadline)/1e6))
            owner.emit(rec.body['market'],rec.body,rec.body['route'],reset=rec.body['kind']=='SOURCE_RESET')
            await asyncio.sleep(0)
        await j.sync()
    finally:
        stop.set();await sync;await j.close()
    samples=sorted(owner.latencies_ms)
    source_sha={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (args.package/'lobv2').glob('*.py')}
    result={'records':len(values),'completed_records':len(samples),'pending_records':len(j.pending),'source_files_sha256':source_sha,
        'input_sha256':hashlib.sha256(args.input.read_bytes()).hexdigest(),'window_ms':args.window,
        'same_production_filesystem_data_root':str(root),'recorded_arrival_duration_seconds':args.seconds,
        'elapsed_seconds':(time.monotonic_ns()-start)/1e9,'syncs':j.syncs,'max_sync_batch_records':j.max_sync_batch,
        'normalized_to_commit_ms':{name:samples[int((len(samples)-1)*q)] for name,q in [('p50',.5),('p95',.95),('p99',.99)]},
        'producer_schedule_lag_p99_ms':sorted(producer_lags)[int((len(producer_lags)-1)*.99)],
        'stage_metrics':j.metrics.snapshot() if hasattr(j,'metrics') else None,
        'scope':'One archived production gateway stream at its recorded 1x schedule. No native WS/network; production background archives remain running.'}
    result['local_50ms_p99_pass']=len(samples)==len(values) and result['normalized_to_commit_ms']['p99']<=50
    args.output.write_text(json.dumps(result,indent=2))


def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--data-root',type=Path,required=True);p.add_argument('--package',type=Path);p.add_argument('--baseline',type=Path);p.add_argument('--candidate',type=Path,default=PACKAGE)
    p.add_argument('--window',type=float,default=20);p.add_argument('--seconds',type=float,default=30)
    args=p.parse_args()
    if args.package:asyncio.run(worker(args));return
    args.output.mkdir(parents=True,exist_ok=True)
    results=[]
    for name,package,window in [('baseline',args.baseline,20),*[('candidate',args.candidate,w) for w in [2,5,10,20]]]:
        out=args.output/(name+'-'+str(window)+'.json')
        subprocess.run([sys.executable,__file__,'--input',str(args.input),'--output',str(out),'--data-root',str(args.data_root),
            '--package',str(package),'--window',str(window),'--seconds',str(args.seconds)],check=True)
        result=json.loads(out.read_bytes());results.append({'build':name,**result})
        print(name,window,result['normalized_to_commit_ms'],flush=True)
    (args.output/'report.json').write_text(json.dumps(results,indent=2))


if __name__=='__main__':main()
