#!/usr/bin/env python3
"""Persistent full-fleet observation; elapsed time never implies acceptance."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
import time

PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/'.vendor312'),str(PACKAGE)]
from lobv2.journal import atomic_json


def observe(deployment, detailed):
    settings=json.loads((deployment/'settings.json').read_bytes())
    data=Path(settings['data_root']);now=time.time_ns();rows=[];errors=[]
    for path in sorted((deployment/'instances').glob('*.json')):
        node=path.stem
        try:
            row=json.loads((data/node/'summary.json').read_bytes())
            row['telemetry_age_seconds']=(now-row['utc_ns'])/1e9
            if row['telemetry_age_seconds']>90:errors.append(node+':STALE_TELEMETRY')
            rows.append(row)
        except (OSError,ValueError) as exc:errors.append(node+':'+repr(exc))
    detail={}
    for node in detailed:
        try:
            row=json.loads((data/node/'status.json').read_bytes())
            detail[node]={key:row.get(key) for key in ('utc_ns','stream','provenance','publish_latency_ms',
                'pending_records','pending_publication_records','journal_oldest_age_ms','stage_metrics',
                'failure','admission_failure','resources','cut_receipts','catalog_revision')}
        except (OSError,ValueError) as exc:errors.append(node+':'+repr(exc))
    identities={r['producer']:{k:r['provenance'].get(k) for k in ('source_sha256','code_version','config_sha256')} for r in rows}
    fingerprint=hashlib.sha256(json.dumps(identities,sort_keys=True).encode()).hexdigest()
    return {'utc_ns':now,'nodes':rows,'detailed_nodes':detail,'errors':errors,'build_config_fingerprint':fingerprint,
        'versions':dict(Counter(r['provenance'].get('code_version') for r in rows)),
        'scope':'ALL_CONFIGURED_PRODUCTION_NODES; detailed latency only for explicitly listed canaries',
        'coverage_contract':'Counts are observations, not a same-catalog completeness certificate'}


def main():
    p=argparse.ArgumentParser();p.add_argument('--deployment',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--detailed',action='append',default=[])
    p.add_argument('--interval',type=float,default=60);args=p.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    state_path=args.output/'progress.json'
    state=json.loads(state_path.read_bytes()) if state_path.exists() else {}
    while True:
        before=time.monotonic();row=observe(args.deployment,args.detailed);now=row['utc_ns']
        if not state or state['build_config_fingerprint']!=row['build_config_fingerprint'] or now-state['last_sample_ns']>180*10**9:
            state={'continuous_window_start_ns':now,'samples':0,'duration_gate':'NOT_COMPLETE'}
        state.update(last_sample_ns=now,build_config_fingerprint=row['build_config_fingerprint'],
            versions=row['versions'],errors=row['errors'],samples=state['samples']+1)
        elapsed=(now-state['continuous_window_start_ns'])/1e9
        crossed=now//(86400*10**9)>state['continuous_window_start_ns']//(86400*10**9)
        state.update(elapsed_seconds=elapsed,crossed_utc_midnight=crossed,
            duration_gate='PASS' if elapsed>=86400 and crossed else 'NOT_COMPLETE',
            acceptance='PARTIAL',acceptance_reason='Duration, full-catalog coverage, standby, latency, archive drain and host recovery are separate gates')
        with (args.output/'samples.jsonl').open('ab') as out:
            out.write(json.dumps(row,separators=(',',':')).encode()+b'\n');out.flush();os.fsync(out.fileno())
        atomic_json(state_path,state)
        time.sleep(max(1,args.interval-(time.monotonic()-before)))


if __name__=='__main__':main()
