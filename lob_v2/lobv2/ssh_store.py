"""Immutable archive custody on an independent host, through existing SSH."""
import base64
import json
import shlex
import subprocess
import fcntl
from pathlib import Path
import time
import hashlib
import tempfile


class SSHStore:
    backend = "REMOTE_SSH_FULL_READBACK"

    def __init__(self, config):
        self.config = config
        self.cancel_event = None

    def cancelled(self):
        return self.cancel_event is not None and self.cancel_event.is_set()

    def request(self, operation, key, data=None):
        c = self.config
        request = {"operation":operation,"key":key,"root":c["root"]}
        if operation == "put":
            request.update(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
        payload = base64.urlsafe_b64encode(json.dumps(request).encode()).decode()
        command = shlex.join(["python3", c["helper"], payload])
        # A host-wide limit also covers independent plant processes. Waiting here
        # occupies only the archive executor; ingestion never waits for SSH.
        directory = Path(c["lock_root"])
        directory.mkdir(parents=True,exist_ok=True)
        index = int.from_bytes(hashlib.sha256(key.encode()).digest()[:4],"big") % c.get("concurrency",4)
        lock = (directory/str(index)).open("a")
        try:
            while True:
                if self.cancelled():
                    raise OSError("ARCHIVE_SHUTDOWN_LOCAL_RETAINED")
                try:
                    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(.05)
            # A seekable stdin owns the complete upload independently of parent
            # timeout polling. Retrying communicate(None) on a partially written
            # PIPE would stop servicing stdin and deadlock large uploads.
            with tempfile.TemporaryFile() as source:
                source.write(data or b"")
                source.seek(0)
                identity=hashlib.sha256((c["host"]+c["identity_file"]).encode()).hexdigest()[:12]
                control_path=str(directory/("ssh-"+identity+"-"+str(index)))
                child = subprocess.Popen(["ssh", "-i", c["identity_file"], "-o", "IdentitiesOnly=yes",
                    "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10",
                    "-o", "ControlMaster=auto", "-o", "ControlPersist=120", "-o", "ControlPath="+control_path,
                    "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
                    c["host"], command],stdin=source,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                started = time.monotonic()
                try:
                    while True:
                        if self.cancelled() or time.monotonic()-started > 120:
                            raise OSError("ARCHIVE_CANCELLED_OR_TIMED_OUT_LOCAL_RETAINED")
                        try:
                            stdout,stderr = child.communicate(timeout=.25)
                            break
                        except subprocess.TimeoutExpired:
                            pass
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.communicate()
            if child.returncode:
                raise OSError("REMOTE_ARCHIVE_ERROR: "+stderr.decode(errors="replace")[-1000:])
            return stdout
        finally:
            lock.close()

    def put(self, key, data):
        self.request("put", key, data)

    def get(self, key):
        return self.request("get", key)
