#!/usr/bin/env python3
"""One archive job under its parent's global CPU/I/O admission budget."""
import json
import os
from pathlib import Path
import resource
import subprocess
import sys

PACKAGE=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PACKAGE/'.vendor312'),str(PACKAGE)]
from lobv2.archive_worker import archive_one

if __name__=='__main__':
    request=json.load(sys.stdin)
    os.nice(15)
    # Four globally admitted jobs use at most four cores; no multi-threaded codec.
    limit=request['config'].get('worker_memory_bytes',2*1024**3)
    resource.setrlimit(resource.RLIMIT_AS,(limit,limit))
    subprocess.run(['ionice','-c','3','-p',str(os.getpid())],check=True,capture_output=True)
    archive_one(**request)
    print(json.dumps({'status':'COMPLETED','pid':os.getpid(),'memory_limit_bytes':limit}))
