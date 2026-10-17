#!/usr/bin/env python3
"""Independently replay archived RAW and compare selected canonical plant books."""
import argparse,json,sys,time,random
from pathlib import Path
PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/(".vendor312" if sys.version_info >= (3,12) else ".vendor")),str(PACKAGE)]
from lobv2.archive import restore,read_archive
from lobv2.ssh_store import SSHStore
from lobv2.journal import atomic_json
from lobv2.validation import reference_sources
from lobv2.book import apply,digest


def run(root,output,current=False):
    output.mkdir(parents=True,exist_ok=True)
    settings=json.loads((root/'settings.json').read_bytes());store=SSHStore(settings['archive']['ssh_store']);reports=[]
    for node in ['P-0008','Q-0008']:
        receipts=[json.loads(p.read_bytes()) for p in (Path(settings['data_root'])/node/'journal').glob('*.archive.json')]
        candidates=[r for r in receipts if r['manifest']['first']==1 and r['manifest']['count']>10000]
        if current:
            stream=json.loads((Path(settings['data_root'])/node/'run.json').read_bytes())['stream']
            candidates=[r for r in candidates if r['manifest']['stream']==stream]
        if not candidates:raise ValueError('NO_COMPLETE_PREFIX_AVAILABLE: '+node)
        # A complete, immutable capture prefix, independent of cache eviction.
        receipt=max(candidates,key=lambda r:r['manifest']['count']);m=receipt['manifest'];target=output/(node+'-'+m['stream']+'.lobseg')
        if not target.exists():restore(store,receipt['manifest_key'],target)
        records=list(read_archive(target));cut=max(r.offset for r in records if r.kind=='OBSERVATION_RESULT');records=[r for r in records if r.offset<=cut]
        scope=next(r.body['scope'] for r in records if r.kind=='RUN_START')
        replay,counts=reference_sources(records,scope,cut)
        canonical={};routes={};seqs={};sequence_errors=[]
        for rec in records:
            if rec.kind!='PLANT_EMIT':continue
            b=rec.body;market=b['market']
            if b['prev_market_seq']!=seqs.get(market,0):sequence_errors.append(rec.offset)
            canonical[market]=apply(canonical.get(market,{}),b);routes[market]=b['route'];seqs[market]=b['market_seq']
        valid=[market for market,assets in canonical.items() if len(assets)==2 and all(a['ready'] for a in assets.values())]
        random.Random(20261017).shuffle(valid);sample=valid[:100];mismatches=[];lanes=set()
        if not sample:raise ValueError('NO_QUALIFIED_CANONICAL_SAMPLE: '+node)
        for market in sample:
            route=routes[market];key='|'.join(map(str,(market,route['shard'],route['lane'])));lanes.add(route['lane'])
            if key not in replay or digest(canonical[market])!=digest(replay[key]):mismatches.append(market)
        raw_identity=[];last={};raw_order_errors=[]
        for rec in records:
            if rec.kind!='RAW':continue
            b=rec.body;key=b['connection'];raw_identity.append((rec.stream,key,b['recv_seq']))
            if b['recv_seq']!=last.get(key,0)+1:raw_order_errors.append(rec.offset)
            last[key]=b['recv_seq']
        reports.append({'node':node,'capture_stream':m['stream'],'archive_manifest':receipt['manifest_key'],'prefix_cut':cut,
            'prefix_record_count':len(records),'sample_markets':len(sample),'sample_selected_lanes':sorted(lanes),
            'independent_raw_source_books':len(replay),'independent_reconstruction_counts':counts,
            'canonical_book_mismatches':mismatches,'market_sequence_errors':sequence_errors,
            'raw_order_errors':raw_order_errors,'raw_identities_unique':len(raw_identity)==len(set(raw_identity)),
            'code_source_sha256':next(r.body['source_files'] for r in records if r.kind=='RUN_START'),
            'scope':'archived prefix only; all native observations retained; no upstream-union completeness claim'})
    atomic_json(output/'report.json',{'utc_ns':time.time_ns(),'reports':reports})
    print(json.dumps(reports,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--deployment',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--current',action='store_true');args=parser.parse_args();run(args.deployment,args.output,args.current)
