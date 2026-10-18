#!/usr/bin/env python3
"""Copy committed immutable objects to HDD, then release leased remote NVMe copies."""
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import time
import uuid
from archive_receiver import COLD,HOT,lease,mkdir,path_for,put_immutable,sync

STOP=False

def stop(*args):
    global STOP
    STOP=True


def atomic(path,data):
    mkdir(path.parent);tmp=path.with_name(path.name+'.tmp-'+str(uuid.uuid4()))
    with tmp.open('xb') as out:out.write(json.dumps(data,separators=(',',':')).encode());out.flush();os.fsync(out.fileno())
    os.replace(tmp,path);sync(path.parent)


def sha(path):
    result=hashlib.sha256()
    with path.open('rb') as src:
        for data in iter(lambda:src.read(1024**2),b''):result.update(data)
    return result.hexdigest()


def progress(db):
    result={}
    for producer,stream,first,last in db.execute('select producer,stream,first_offset,last_offset from commits order by producer,stream,first_offset'):
        row=result.setdefault(producer+'/'+stream,{'contiguous_offset':0,'committed_ranges':[],'holes':[]})
        ranges=row['committed_ranges']
        if ranges and ranges[-1][1]+1==first:ranges[-1][1]=last
        else:ranges.append([first,last])
    for row in result.values():
        cursor=1
        for first,last in row['committed_ranges']:
            if first>cursor:row['holes'].append([cursor,first-1])
            if first==row['contiguous_offset']+1:row['contiguous_offset']=last
            cursor=last+1
    return result


def mirror(path,db):
    key=str(path.relative_to(HOT))
    # Shared leases allow normal readback while the slower cold copy runs.
    with lease(key):
        if not path.exists():return
        encoded=path.read_bytes();manifest=json.loads(encoded)
    query=key.startswith('query/commits/')
    files=manifest['files'] if query else [{'key':manifest['data_key'],'bytes':manifest['object_bytes'],'sha256':manifest['object_sha256']}]
    for row in files:
        data_key=row['key']
        with lease(data_key):
            source=path_for(HOT,data_key)
            if not source.exists():source=path_for(COLD,data_key)
            if source.stat().st_size!=row['bytes'] or sha(source)!=row['sha256']:
                raise ValueError('HOT_OBJECT_CHECKSUM_MISMATCH_KEEP_ALL_COPIES')
            put_immutable(COLD,data_key,source.read_bytes())
        if sha(path_for(COLD,data_key))!=row['sha256']:
            raise ValueError('COLD_OBJECT_READBACK_MISMATCH')
    put_immutable(COLD,key,encoded)
    if path_for(COLD,key).read_bytes()!=encoded:raise ValueError('COLD_MANIFEST_READBACK_MISMATCH')
    if query:
        db.execute('CREATE TABLE IF NOT EXISTS query_commits(key TEXT PRIMARY KEY,manifest TEXT,bytes INTEGER)')
        db.execute('INSERT OR IGNORE INTO query_commits VALUES (?,?,?)',(key,encoded.decode(),sum(row['bytes'] for row in files)))
    else:db.execute('insert or ignore into commits values(?,?,?,?,?,?,?)',(key,manifest['producer'],manifest['stream'],manifest['first'],manifest['last'],manifest['count'],manifest['object_bytes']))
    db.commit()
    # Cold commit is already durable. Acquire exclusive leases only for releasing
    # individual HOT files, so HDD writes cannot stall producer readback.
    for local_key in [row['key'] for row in files]+[key]:
        with lease(local_key,True) as lock_path:
            local=path_for(HOT,local_key)
            if local.exists():local.unlink();sync(local.parent)
        lock_path.unlink(missing_ok=True)


def main():
    mkdir(HOT);mkdir(COLD)
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    db=sqlite3.connect(HOT/'cold-progress.sqlite')
    db.execute('pragma journal_mode=WAL');db.execute('pragma synchronous=FULL')
    db.execute('create table if not exists commits(key text primary key,producer text,stream text,first_offset integer,last_offset integer,records integer,bytes integer)')
    db.execute('CREATE TABLE IF NOT EXISTS query_commits(key TEXT PRIMARY KEY,manifest TEXT,bytes INTEGER)')
    # Existing manifests were independently verified by the original direct-HDD
    # publisher. Import their immutable commit evidence into the tier index.
    if not db.execute('select count(*) from commits').fetchone()[0]:
        for p in (COLD/'commits').rglob('*.json'):
            m=json.loads(p.read_bytes());db.execute('insert or ignore into commits values(?,?,?,?,?,?,?)',(str(p.relative_to(COLD)),m['producer'],m['stream'],m['first'],m['last'],m['count'],m['object_bytes']))
        db.commit()
    while not STOP:
        paths=sorted((HOT/'commits').rglob('*.json'),key=lambda p:p.stat().st_mtime_ns)
        error=None
        query_paths=sorted((HOT/'query/commits').rglob('*.json'),key=lambda p:p.stat().st_mtime_ns)
        for path in paths[:32]+query_paths[:2]:
            if STOP:break
            try:mirror(path,db)
            except Exception as exc:error={'error':repr(exc),'path':str(path),'utc_ns':time.time_ns()};atomic(HOT/'mirror-error.json',error)
        pending=list((HOT/'commits').rglob('*.json'))
        stat=os.statvfs(HOT)
        atomic(HOT/'cold-progress.json',{'utc_ns':time.time_ns(),'tier':'HDD_VERIFIED_IMMUTABLE_COMMIT',
            'cold_manifests':db.execute('select count(*) from commits').fetchone()[0],
            'cold_object_bytes':db.execute('select coalesce(sum(bytes),0) from commits').fetchone()[0],
            'query_cold_manifests':db.execute('SELECT COUNT(*) FROM query_commits').fetchone()[0],
            'query_cold_bytes':db.execute('SELECT COALESCE(SUM(bytes),0) FROM query_commits').fetchone()[0],
            'query_hot_commits_pending_cold':len(list((HOT/'query/commits').rglob('*.json'))),
            'hot_commits_pending_cold':len(pending),'oldest_hot_commit_seconds':max((time.time()-p.stat().st_mtime for p in pending),default=0),
            'nvme_available_bytes':stat.f_bavail*stat.f_frsize,'nvme_available_inodes':stat.f_favail,
            'nvme_reserved_bytes':2*1024**4,'last_pass_error':error,'streams':progress(db)})
        time.sleep(.1 if len(paths)>=32 else 2)
    db.close()


if __name__=='__main__':main()
