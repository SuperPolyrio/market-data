#!/usr/bin/env python3
"""Conditional immutable logical store: remote NVMe custody and HDD cold mirror."""
import base64
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

COLD=Path('/mnt/hdd22t/prediction-market-quant/lob_v2_archive')
HOT=Path('/mnt/polygon/lob-v2/remote-archive-hot')
MAX_BYTES=512*1024**2
MIN_FREE_BYTES=2*1024**4
MIN_FREE_INODES=100000


def sync(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def mkdir(path):
    missing=[]
    parent=path
    while not parent.exists():missing.append(parent);parent=parent.parent
    for directory in reversed(missing):directory.mkdir(exist_ok=True);sync(directory.parent)


def path_for(root,key):
    path=(root/key).resolve()
    if root not in path.parents:raise ValueError('INVALID_ARCHIVE_KEY')
    return path


@contextmanager
def lease(key,exclusive=False):
    directory=HOT/'.leases';mkdir(directory)
    path=directory/hashlib.sha256(key.encode()).hexdigest()
    fd=os.open(path,os.O_CREAT|os.O_RDWR,0o600)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield path
    finally:os.close(fd)


def put_immutable(root,key,data):
    path=path_for(root,key)
    if path.exists():
        if path.read_bytes()!=data:raise ValueError('IMMUTABLE_OBJECT_CONFLICT')
        return
    mkdir(path.parent)
    temporary=path.with_name(path.name+'.tmp-'+str(uuid.uuid4()))
    try:
        with temporary.open('xb') as out:
            out.write(data);out.flush();os.fsync(out.fileno())
        try:os.link(temporary,path);sync(path.parent)
        except FileExistsError:
            if path.read_bytes()!=data:raise ValueError('IMMUTABLE_OBJECT_CONFLICT')
    finally:temporary.unlink(missing_ok=True)


def read(key):
    with lease(key):
        for root in (HOT,COLD):
            path=path_for(root,key)
            try:
                if path.stat().st_size>MAX_BYTES:raise ValueError('OBJECT_TOO_LARGE')
                return path.read_bytes()
            except FileNotFoundError:continue
        raise FileNotFoundError('COMMITTED_OBJECT_NOT_FOUND: '+key)


def put(key,data):
    with lease(key,True):
        for root in (COLD,HOT):
            existing=path_for(root,key)
            if existing.exists():
                if existing.read_bytes()!=data:raise ValueError('IMMUTABLE_OBJECT_CONFLICT')
                return
        mkdir(HOT)
        stat=os.statvfs(HOT)
        if stat.f_bavail*stat.f_frsize<MIN_FREE_BYTES+4*MAX_BYTES or stat.f_favail<MIN_FREE_INODES:
            raise OSError('REMOTE_NVME_RESERVE_REACHED_LOCAL_JOURNAL_RETAIN_REQUIRED')
        put_immutable(HOT,key,data)


def main():
    request=json.loads(base64.urlsafe_b64decode(sys.argv[1]))
    if Path(request['root']).resolve()!=COLD:raise ValueError('UNAUTHORIZED_ARCHIVE_ROOT')
    key=request['key'];path_for(COLD,key);path_for(HOT,key)
    if request['operation']=='get':sys.stdout.buffer.write(read(key));return
    if request['operation']!='put':raise ValueError('UNKNOWN_OPERATION')
    data=sys.stdin.buffer.read(MAX_BYTES+1)
    if len(data)>MAX_BYTES:raise ValueError('OBJECT_TOO_LARGE')
    if len(data)!=request['bytes'] or hashlib.sha256(data).hexdigest()!=request['sha256']:
        raise ValueError('INCOMPLETE_OR_CORRUPT_UPLOAD_NOT_PUBLISHED')
    put(key,data)


if __name__=='__main__':main()
