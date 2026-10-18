#!/usr/bin/env python3
"""Project real immutable archives, verify remote query objects, report separate cuts."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time

PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/'.vendor312'),str(PACKAGE)]
from lobv2.archive import restore,progress
from lobv2.journal import atomic_json
from lobv2.projections import observation_projection,tape_projection,checkpoint,commit_projection
from lobv2.ssh_store import SSHStore,archive_slot
from lobv2.wire import json_bytes


def project(settings,receipts,output):
    if settings['archive'].get('local_tier'):
        from archive_receiver import read,put
        class TierStore:
            get=staticmethod(read)
            put=staticmethod(put)
        store=TierStore()
    else:
        config={**settings['archive']['ssh_store'],'lock_root':str(output/'network-budget'),'concurrency':1}
        store=SSHStore(config)
    rows=[p if isinstance(p,dict) else json.loads(p.read_bytes()) for p in receipts]
    identities=[r['manifest_key'] for r in rows]
    job=hashlib.sha256(json_bytes(identities)).hexdigest()
    result_path=output/'receipts'/(job+'.json')
    if result_path.exists():return json.loads(result_path.read_bytes())
    began=time.time_ns()
    with archive_slot({'resource_lock_root':str(output/'budget'),'job_concurrency':1},store):
        with tempfile.TemporaryDirectory(dir=output,prefix='work-') as temporary:
            work=Path(temporary);inputs=[];manifests=[]
            for i,row in enumerate(rows):
                path=work/(str(i)+'.lobseg')
                manifest=restore(store,row['manifest_key'],path)
                if manifest['object_sha256']!=row['manifest']['object_sha256']:raise ValueError('SOURCE_RECEIPT_MISMATCH')
                inputs.append(path);manifests.append(manifest)
            if {m['archive_kind'] for m in manifests}=={'tape'}:
                view='tape_events';local=tape_projection(inputs,work/view)
                try:
                    derived=checkpoint(inputs,work/'checkpoint')
                    derived_remote=commit_projection(store,work/'checkpoint','checkpoint',manifests)
                except BufferError as exc:
                    derived={'status':'PARTIAL','error':str(exc),'source_tape_retained':True};derived_remote=None
            elif {m['archive_kind'] for m in manifests}=={'capture'}:
                view='observations';local=observation_projection(inputs,work/view);derived=None;derived_remote=None
            else:raise ValueError('MIXED_PROJECTION_INPUT_KINDS')
            remote=commit_projection(store,work/view,view,manifests)
            result={'started_ns':began,'completed_ns':time.time_ns(),'input_keys':identities,'input_manifests':manifests,
                'local_verification':local,'remote_query':remote,'derived_checkpoint':derived,
                'remote_checkpoint':derived_remote,'custody':'ARCHIVE_HOST_FULL_READBACK' if settings['archive'].get('local_tier') else 'REMOTE_SSH_FULL_READBACK',
                'scope':'EXACT_LISTED_INPUT_RANGES','archive_progress_unchanged':True}
            atomic_json(result_path,result)
            return result


def watch(deployment,output,partition=0,partitions=1):
    settings=json.loads((deployment/'settings.json').read_bytes())
    db=sqlite3.connect(output/'jobs.sqlite');db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS done(input_key TEXT PRIMARY KEY,receipt TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS ranges(input_key TEXT PRIMARY KEY,view TEXT,producer TEXT,stream TEXT,first INTEGER,last INTEGER)')
    db.execute('CREATE TABLE IF NOT EXISTS queued(input_key TEXT PRIMARY KEY,first_seen_ns INTEGER)')
    def record(result,path):
        with db:
            for key,m in zip(result['input_keys'],result['input_manifests']):
                db.execute('INSERT OR IGNORE INTO done VALUES (?,?)',(key,str(path)))
                db.execute('DELETE FROM queued WHERE input_key=?',(key,))
                db.execute('INSERT OR IGNORE INTO ranges VALUES (?,?,?,?,?,?)',(key,result['remote_query']['manifest']['view'],m['producer'],m['stream'],m['first'],m['last']))
    for path in (output/'receipts').glob('*.json'):record(json.loads(path.read_bytes()),path)
    last_progress=0
    queued_path=output/'queue-start.json'
    if not queued_path.exists():atomic_json(queued_path,{'utc_ns':time.time_ns()})
    queued_ns=json.loads(queued_path.read_bytes())['utc_ns']
    belongs=lambda key:int.from_bytes(hashlib.sha256(key.encode()).digest()[:8],'big')%partitions==partition
    while True:
        done={row[0] for row in db.execute('SELECT input_key FROM done')}
        pending=[]
        if settings['archive'].get('local_tier'):
            from archive_receiver import HOT,COLD,read
            index=sqlite3.connect('file:'+str(HOT/'cold-progress.sqlite')+'?mode=ro',uri=True)
            indexed=set()
            for key,size,first in index.execute('SELECT key,bytes,first_offset FROM commits'):
                indexed.add(key)
                if key not in done and belongs(key):
                    pending.append((key,{'manifest_key':key,'manifest':{'object_bytes':size,'first':first},
                        'committed_utc_ns':queued_ns,'lazy_manifest':True}))
            index.close()
            for path in (HOT/'commits').rglob('*.json'):
                key=str(path.relative_to(HOT))
                if key in done or key in indexed or not belongs(key):continue
                m=json.loads(read(key))
                pending.append((key,{'manifest_key':key,'manifest':{'object_bytes':m['object_bytes'],'first':m['first']},
                    'committed_utc_ns':path.stat().st_mtime_ns,'lazy_manifest':True}))
        else:
            for path in Path(settings['data_root']).glob('*/journal/*.archive.json'):
                row=json.loads(path.read_bytes())
                if row['manifest_key'] not in done and belongs(row['manifest_key']):pending.append((path,row))
        pending.sort(key=lambda pair:(pair[1]['manifest']['first']!=1,pair[1].get('committed_utc_ns',0)))
        with db:db.executemany('INSERT OR IGNORE INTO queued VALUES (?,?)',((row['manifest_key'],time.time_ns()) for _,row in pending))
        oldest=db.execute('SELECT MIN(first_seen_ns) FROM queued').fetchone()[0]
        queue={'utc_ns':time.time_ns(),'pending_segments':len(pending),
            'pending_input_object_bytes':sum(r['manifest']['object_bytes'] for _,r in pending),
            'oldest_pending_seconds':(time.time_ns()-oldest)/1e9 if oldest else 0,
            'completed_segments':len(done),'scope':'ALL_COMMITTED_PRODUCTION_ARCHIVES',
            'partition':partition,'partitions':partitions,'age_basis':'WORKER_QUEUE_FIRST_OBSERVED_FOR_HISTORICAL_INPUTS'}
        atomic_json(output/'queue.json',queue)
        for path,row in pending:
            try:
                if row.pop('lazy_manifest',False):row['manifest']=json.loads(read(row['manifest_key']))
                result=project(settings,[row],output)
                receipt=output/'receipts'/(hashlib.sha256(json_bytes(result['input_keys'])).hexdigest()+'.json')
                record(result,receipt)
                queue['pending_segments']-=1;queue['pending_input_object_bytes']-=row['manifest']['object_bytes'];queue['completed_segments']+=1
                if time.monotonic()-last_progress>=30:
                    streams={}
                    for view,producer,stream,first,last in db.execute('SELECT view,producer,stream,first,last FROM ranges'):
                        streams.setdefault(view+'/'+producer+'/'+stream,[]).append({'first':first,'last':last})
                    atomic_json(output/'progress.json',{'utc_ns':time.time_ns(),
                        'completed_segments':db.execute('SELECT COUNT(*) FROM done').fetchone()[0],
                        'streams':{key:progress(rows,max(r['last'] for r in rows)) for key,rows in streams.items()},
                        'contract':'QUERY_COMPLETION_SEPARATE_FROM_REMOTE_ARCHIVED'})
                    queue['utc_ns']=time.time_ns();atomic_json(output/'queue.json',queue)
                    last_progress=time.monotonic()
            except Exception as exc:
                atomic_json(output/'error.json',{'utc_ns':time.time_ns(),'input':str(path),'error':repr(exc),'action':'INPUT_RETAINED_RETRY_PENDING'})
                continue
            finally:
                if settings['archive'].get('local_tier'):
                    row['manifest']={k:row['manifest'][k] for k in ('first','object_bytes')}
        time.sleep(10)


def update_progress(output):
    streams={};counts={'tape_events':0,'observations':0}
    for path in (output/'receipts').glob('*.json'):
        result=json.loads(path.read_bytes());view=result['remote_query']['manifest']['view']
        counts[view]+=1
        for row in result['input_manifests']:
            streams.setdefault(view+'/'+row['producer']+'/'+row['stream'],[]).append(row)
    atomic_json(output/'progress.json',{'utc_ns':time.time_ns(),'query_jobs':counts,
        'streams':{key:progress(rows,max(r['last'] for r in rows)) for key,rows in streams.items()},
        'contract':'QUERY_COMPLETION_SEPARATE_FROM_REMOTE_ARCHIVED'})


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--deployment',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--receipt',type=Path,action='append');p.add_argument('--watch',action='store_true')
    p.add_argument('--partition',type=int,default=0);p.add_argument('--partitions',type=int,default=1)
    args=p.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    if not 0<=args.partition<args.partitions:p.error('invalid partition')
    if args.watch:watch(args.deployment,args.output,args.partition,args.partitions)
    else:
        if not args.receipt:p.error('--receipt or --watch required')
        result=project(json.loads((args.deployment/'settings.json').read_bytes()),args.receipt,args.output)
        update_progress(args.output)
        print(json.dumps({'view':result['remote_query']['manifest']['view'],'verification':result['local_verification'],
            'manifest':result['remote_query']['manifest_key'],'checkpoint':result['derived_checkpoint']},ensure_ascii=False))
