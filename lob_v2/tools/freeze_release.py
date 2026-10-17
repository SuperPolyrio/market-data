#!/usr/bin/env python3
"""Install one immutable commit without disturbing unrelated worktree changes."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

PACKAGE=Path(__file__).resolve().parents[1]


def freeze(commit):
    sha=subprocess.check_output(['git','rev-parse',commit],cwd=PACKAGE.parent,text=True).strip()
    parent=PACKAGE/'releases';parent.mkdir(exist_ok=True)
    target=parent/sha[:12]
    if target.exists():return target
    data=subprocess.check_output(['git','archive',sha,'lob_v2'],cwd=PACKAGE.parent)
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:archive.extractall(directory,filter='data')
        source=Path(directory)/'lob_v2'
        if not(source/'config/base.json').exists():
            source.joinpath('config').mkdir(exist_ok=True)
            baseline=PACKAGE/'runs/remediation-20261017/baseline/config/base.json'
            shutil.copy2(baseline,source/'config/base.json')
        (source/'.vendor312').symlink_to(PACKAGE/'.vendor312',target_is_directory=True)
        files={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.is_file() and '.vendor312' not in p.parts}
        (source/'release.json').write_text(json.dumps({'commit':sha,'files_sha256':files},sort_keys=True))
        os.rename(source,target)
    return target


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--commit',default='HEAD');args=p.parse_args()
    print(freeze(args.commit))
