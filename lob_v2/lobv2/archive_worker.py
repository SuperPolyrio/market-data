"""Bounded archive subprocess; the parent retains private journal ownership."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from .archive import build_archive, commit_archive, evict
from .journal import atomic_json


def archive_one(root, path, config, owner_cut):
    from .cli import object_store
    from .ssh_store import SSHStore
    root, path = Path(root), Path(path)
    receipt = path.with_suffix('.archive.json')
    store = SSHStore(config['ssh_store']) if config.get('ssh_store') else object_store(config['store_uri'])
    packed = path.with_suffix(".lobseg")
    def stage(name):
        atomic_json(root/'archive-phase.json',{'utc_ns':time.time_ns(),'path':str(path),'stage':name,
            'input_bytes':path.stat().st_size if path.exists() else 0,
            'packed_bytes':packed.stat().st_size if packed.exists() else 0})
    if receipt.exists():
        saved = json.loads(receipt.read_bytes())
        key,manifest = saved["manifest_key"],saved["manifest"]
    else:
        try:
            stage('COMPRESS')
            build_archive(path,packed)
        except ValueError as exc:
            if str(exc) != "EMPTY_JOURNAL":
                raise
            # Preserve a legacy header-only segment separately. It has
            # no observations or offsets and cannot block later custody.
            os.rename(path,path.with_suffix(".empty"))
            from .journal import sync_dir
            sync_dir(path.parent)
            return
        key,manifest = commit_archive(store,packed,"capture" if path.name.startswith("capture") else "tape",on_stage=stage)
        saved = {"manifest_key":key,"manifest":manifest,"committed_utc_ns":time.time_ns(),
            "local_journal_bytes":path.stat().st_size}
        atomic_json(receipt,saved)
    if config.get("evict_after_commit"):
        # This process owns the private journal directory under writer.lock.
        # Replay consumers use immutable objects. All live subscriptions and
        # callbacks own encoded frames in memory, never sealed file readers.
        dependencies = []
        lifecycle_cut=manifest.get('lifecycle_last_cut',0)
        if lifecycle_cut:
            life=root/'lifecycle'/(manifest['stream']+'.progress.json')
            if not life.exists() or json.loads(life.read_bytes()).get('canonical_notification_cut',0)<lifecycle_cut:
                dependencies.append('LIFECYCLE_OUTBOX_NOT_RECOVERABLE')
        if not owner_cut or (manifest["stream"] == owner_cut["stream"] and manifest["last"] > owner_cut["applied_offset"]):
            dependencies.append("DURABLE_CALLBACK_CUT_NOT_RELEASED")
        saved["eviction"] = evict(path,packed,store,key,active_dependencies=dependencies)
        saved["eviction"]["dependency_evidence"] = "EXCLUSIVE_WRITER_PRIVATE_DIRECTORY_NO_FILE_REPLAY_READERS"
        atomic_json(receipt,saved)
        stage('LOCAL_CACHE_RELEASED')


def run_isolated(root, path, config, owner_cut, store):
    request={'root':str(root),'path':str(path),'config':config,'owner_cut':owner_cut}
    launcher=Path(__file__).resolve().parents[1]/'tools/archive_job.py'
    with tempfile.TemporaryFile() as source:
        source.write(json.dumps(request).encode());source.seek(0)
        child=subprocess.Popen([sys.executable,str(launcher)],stdin=source,stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,start_new_session=True)
        started=time.monotonic()
        try:
            while True:
                if getattr(store,'cancelled',lambda:False)() or time.monotonic()-started>config.get('job_timeout_seconds',900):
                    raise OSError('ARCHIVE_PROCESS_CANCELLED_LOCAL_RETAINED')
                try:
                    stdout,stderr=child.communicate(timeout=.25);break
                except subprocess.TimeoutExpired:pass
            if child.returncode:raise OSError('ARCHIVE_PROCESS_FAILED: '+stderr.decode(errors='replace')[-1500:])
            return json.loads(stdout)
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGKILL);child.communicate()
