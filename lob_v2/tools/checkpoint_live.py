#!/usr/bin/env python3
"""Hourly derived committed snapshots, published after their Tape prefix is archived."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time

PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/'.vendor312'),str(PACKAGE)]
import zstandard as zstd
from lobv2.archive import progress
from lobv2.journal import atomic_json,sync_dir
from lobv2.node import receive_snapshot,confirm_snapshot
from lobv2.projections import commit_projection
from lobv2.ssh_store import SSHStore,archive_slot
from lobv2.wire import control,send,json_bytes


def save(snapshot,node,output):
    if 'owner_record_cut' not in snapshot:raise ValueError('SOURCE_VERSION_HAS_NO_EXACT_TAPE_CUT')
    identity=snapshot['snapshot_id'];directory=output/'pending'/identity;directory.mkdir(parents=True)
    path=directory/'checkpoint.jsonl.zst'
    count=0;ready=0;sha=hashlib.sha256()
    with path.open('xb') as raw:
        with zstd.ZstdCompressor(level=3,write_checksum=True).stream_writer(raw,closefd=False) as packed:
            for item in snapshot['items']:
                row={'kind':'DERIVED_CHECKPOINT','native_event':False,'producer':node,'stream':snapshot['stream'],
                    'generated_utc_ns':snapshot['generated_utc_ns'],'catalog_revision':snapshot['catalog_revision'],
                    'market':item['market'],'market_seq':item['cut'],'source_epoch':item['book_epoch'],
                    'route':item['route'],'quality':item['quality'],'assets':item['assets'],
                    'original_baseline_refs':{token:asset.get('baseline_ref') for token,asset in item['assets'].items()},
                    'as_of_tape_cut':snapshot['owner_record_cut']}
                encoded=json_bytes(row)+b'\n';packed.write(encoded);sha.update(encoded);count+=1
                ready+=len(item['assets'])==2 and all(a['ready'] for a in item['assets'].values())
        raw.flush()
        import os
        os.fsync(raw.fileno())
    observed=hashlib.sha256();recovered=0
    with path.open('rb') as raw:
        import io
        with io.BufferedReader(zstd.ZstdDecompressor().stream_reader(raw)) as unpacked:
            for line in unpacked:observed.update(line);json.loads(line);recovered+=1
    if observed.digest()!=sha.digest() or recovered!=count:raise ValueError('CHECKPOINT_LOCAL_READBACK_MISMATCH')
    sync_dir(directory)
    receipt={'id':identity,'producer':node,'stream':snapshot['stream'],'tape_cut':snapshot['owner_record_cut'],
        'generated_utc_ns':snapshot['generated_utc_ns'],'catalog_revision':snapshot['catalog_revision'],
        'expected_markets':snapshot['expected_markets'],'markets_with_committed_state':count,'ready_markets':ready,
        'missing_baseline_markets':snapshot['expected_markets']-count,'native_events_created':0,
        'original_rows_sha256':sha.hexdigest(),'status':'LOCAL_DURABLE_WAITING_ARCHIVE_PREFIX'}
    atomic_json(directory/'checkpoint-receipt.json',receipt)
    return receipt


async def capture(deployment,output):
    report=[]
    for path in sorted((deployment/'instances').glob('G-*.json')):
        instance=json.loads(path.read_bytes());node=instance['node_id'];writer=None
        try:
            endpoint=instance['config']['listen']
            reader,writer=await asyncio.wait_for(asyncio.open_connection(endpoint['host'],endpoint['port']),5)
            await send(writer,control('HELLO',{'schema':2,'role':'checkpoint'}))
            snapshot,session=await asyncio.wait_for(receive_snapshot(reader),45)
            await confirm_snapshot(reader,writer,snapshot,session)
            # Close the cut+1 stream: this job stores an explicit cut, not a new source.
            writer.close();await writer.wait_closed();writer=None
            report.append(await asyncio.to_thread(save,snapshot,node,output))
        except Exception as exc:report.append({'producer':node,'status':'FAILED_RETRY_NEXT_WINDOW','error':repr(exc)})
        finally:
            if writer:writer.close()
    atomic_json(output/'capture-progress.json',{'utc_ns':time.time_ns(),'results':report})


def publish(settings,output):
    config={**settings['archive']['ssh_store'],'lock_root':str(output/'network-budget'),'concurrency':1}
    store=SSHStore(config);published=0;pending=0
    for directory in sorted((output/'pending').glob('*')):
        receipt_path=directory/'checkpoint-receipt.json'
        if not receipt_path.exists():continue
        row=json.loads(receipt_path.read_bytes());manifests=[]
        for path in (Path(settings['data_root'])/row['producer']/'journal').glob('*.archive.json'):
            m=json.loads(path.read_bytes())['manifest']
            if m['stream']==row['stream'] and m['first']<=row['tape_cut']:manifests.append(m)
        coverage=progress(manifests,max([row['tape_cut']]+[m['last'] for m in manifests]))
        if not row['tape_cut'] or coverage['remote_contiguous_offset']<row['tape_cut']:
            pending+=1;continue
        try:
            with archive_slot({'resource_lock_root':str(output/'budget'),'job_concurrency':1},store):
                result=commit_projection(store,directory,'live_checkpoint',sorted(manifests,key=lambda m:m['first']),
                    identity_context={'snapshot_id':row['id'],'as_of_tape_cut':row['tape_cut']})
            atomic_json(output/'receipts'/(row['id']+'.json'),{**row,'remote':result,'status':'REMOTE_ARCHIVED',
                'committed_utc_ns':time.time_ns(),'archive_prefix_at_commit':coverage['remote_contiguous_offset']})
            # Source Tape remains untouched. This private work directory is now
            # fully reconstructible from immutable, read-back verified query files.
            shutil.rmtree(directory);sync_dir(directory.parent);published+=1
        except Exception as exc:
            pending+=1;atomic_json(output/'error.json',{'utc_ns':time.time_ns(),'id':row['id'],'error':repr(exc),'input_retained':True})
    atomic_json(output/'archive-progress.json',{'utc_ns':time.time_ns(),'published_this_pass':published,'pending':pending,
        'complete_receipts':len(list((output/'receipts').glob('*.json'))),'scope':'DERIVED_CHECKPOINT_ONLY'})


async def run(args):
    args.output.mkdir(parents=True,exist_ok=True)
    settings=json.loads((args.deployment/'settings.json').read_bytes())
    # One checkpoint per UTC hour. Persist the completed window across restarts.
    window_path=args.output/'window.json'
    previous=json.loads(window_path.read_bytes())['hour'] if window_path.exists() else None
    while True:
        hour=time.time_ns()//(3600*10**9)
        if hour!=previous:
            await capture(args.deployment,args.output)
            previous=hour;atomic_json(window_path,{'hour':hour,'completed_ns':time.time_ns()})
        await asyncio.to_thread(publish,settings,args.output)
        if not args.watch:return
        await asyncio.sleep(30)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--deployment',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--watch',action='store_true')
    asyncio.run(run(parser.parse_args()))
