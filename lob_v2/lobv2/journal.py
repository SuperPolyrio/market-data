"""Length/CRC journals. A group sync covers multiple already processed groups."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import struct
import time
import uuid
from collections import deque

from .wire import FRAME, MAX_RECORD, SCHEMA_DIGEST, decode, encode, json_bytes, unframe

MAGIC = b"LOBV2J02"
HEADER = struct.Struct("<8sI")


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp-" + str(uuid.uuid4()))
    with tmp.open("xb") as out:
        out.write(json_bytes(value))
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, path)
    sync_dir(path.parent)


def read_journal(path, recover=False):
    """Recovery truncates only an incomplete final write; a bad CRC is fatal."""
    path = Path(path)
    with path.open("r+b" if recover else "rb") as src:
        head = src.read(HEADER.size)
        if len(head) != HEADER.size:
            raise ValueError("SHORT_HEADER")
        magic, length = HEADER.unpack(head)
        if magic != MAGIC or length > 65536:
            raise ValueError("INVALID_HEADER")
        encoded = src.read(length)
        if len(encoded) != length:
            raise ValueError("SHORT_HEADER")
        metadata = json.loads(encoded)
        if metadata.get("schema_digest") != SCHEMA_DIGEST:
            raise ValueError("SCHEMA_MISMATCH")
        while True:
            start = src.tell()
            head = src.read(FRAME.size)
            if not head:
                return
            if len(head) == FRAME.size:
                length, _ = FRAME.unpack(head)
                if length + FRAME.size > MAX_RECORD:
                    raise ValueError("INVALID_LENGTH")
                payload = src.read(length)
                if len(payload) == length:
                    data = head + payload
                    unframe(data)
                    yield decode(data)
                    continue
            if not recover:
                raise ValueError("INCOMPLETE_TAIL")
            removed = path.stat().st_size - start
            src.truncate(start)
            src.flush()
            os.fsync(src.fileno())
            atomic_json(path.with_suffix(".recovery.json"), {
                "truncated_bytes": removed, "tail_loss": "UNKNOWN_UNSYNCED_TAIL",
                "valid_bytes": start,
            })
            return


class Journal:
    def __init__(self, root, producer, kind, *, stream=None, segment_bytes=256*1024*1024,
                 segment_seconds=60, pending_bytes=64*1024*1024):
        if segment_bytes < MAX_RECORD + 65536 or pending_bytes < MAX_RECORD:
            raise ValueError("INVALID_JOURNAL_BUDGET")
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        sync_dir(self.root.parent)
        self.producer, self.kind = producer, kind
        self.stream = stream or str(uuid.uuid4())
        self.offset = self.durable_offset = 0
        self.max_bytes, self.max_age = segment_bytes, segment_seconds
        self.pending_limit = pending_bytes
        self.pending = deque()
        self.pending_size = 0
        self.callbacks = []
        self.syncs = self.max_sync_batch = 0
        self.sync_latencies_ms = deque(maxlen=65536)
        self.failure = None
        self.segment = 0
        self.fd = None
        self.closed = False
        self._sync_lock = asyncio.Lock()
        self._open()

    def _open(self):
        self.segment += 1
        self.path = self.root / f"{self.kind}-{self.stream}-{self.segment:06d}.open"
        self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        header = json_bytes({"version": 2, "producer": self.producer, "stream": self.stream,
                             "kind": self.kind, "first_offset": self.offset+1,
                             "schema_digest": SCHEMA_DIGEST, "created_ns": time.time_ns()})
        self._write(HEADER.pack(MAGIC, len(header)) + header)
        os.fdatasync(self.fd)
        sync_dir(self.root)
        self.size = HEADER.size + len(header)
        self.opened = time.monotonic()
        self.segment_records = 0

    def _write(self, data):
        view = memoryview(data)
        while view:
            wrote = os.write(self.fd, view)
            if wrote <= 0:
                raise OSError("SHORT_WRITE")
            view = view[wrote:]

    def append(self, kind, body, raw=b"", created_ns=None):
        if self.failure or self.closed:
            raise OSError("JOURNAL_UNAVAILABLE")
        if getattr(self,"policy_version",None):
            body={"policy_version":self.policy_version,**body}
        rec = encode(kind, self.producer, self.stream, self.offset+1, body, raw, created_ns)
        if self.pending_size + len(rec.frame) > self.pending_limit:
            raise BufferError("JOURNAL_PENDING_OVERFLOW")
        if self.segment_records and (self.size + len(rec.frame) > self.max_bytes or
                                     time.monotonic()-self.opened >= self.max_age):
            self._seal_file()
            self._open()
        try:
            self._write(rec.frame)
        except OSError as exc:
            self.failure = repr(exc)
            raise
        self.size += len(rec.frame)
        self.offset += 1
        self.segment_records += 1
        self.pending.append(rec)
        self.pending_size += len(rec.frame)
        return rec

    def _seal_file(self):
        os.fdatasync(self.fd)
        os.close(self.fd)
        self.fd = None
        os.rename(self.path, self.path.with_suffix(".journal"))
        sync_dir(self.root)

    async def sync(self):
        async with self._sync_lock:
            if self.failure:
                raise OSError(self.failure)
            if not self.pending:
                return
            cut = self.offset
            # dup owns the sync even if append rotates to another segment meanwhile.
            fd = os.dup(self.fd)
            started = time.monotonic_ns()
            def durable():
                try:
                    os.fdatasync(fd)
                finally:
                    os.close(fd)
            try:
                await asyncio.shield(asyncio.get_running_loop().run_in_executor(None, durable))
            except OSError as exc:
                self.failure = repr(exc)
                raise
            ready = []
            while self.pending and self.pending[0].offset <= cut:
                ready.append(self.pending.popleft())
            self.pending_size -= sum(len(rec.frame) for rec in ready)
            self.durable_offset = cut
            self.syncs += 1
            self.max_sync_batch = max(self.max_sync_batch, len(ready))
            self.sync_latencies_ms.append((time.monotonic_ns()-started)/1e6)
            for rec in ready:
                for callback in self.callbacks:
                    callback(rec)

    async def run(self, stop, interval=.020):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except asyncio.TimeoutError:
                pass
            await self.sync()
            if self.segment_records and not self.pending and time.monotonic()-self.opened >= self.max_age:
                self._seal_file()
                self._open()
        await self.sync()

    async def close(self):
        await self.sync()
        if self.fd is not None:
            if self.segment_records:
                self._seal_file()
            else:
                os.close(self.fd)
                self.fd = None
                os.rename(self.path,self.path.with_suffix(".empty"))
                sync_dir(self.root)
        self.closed = True


def recover_open(root):
    recovered = []
    for path in sorted(Path(root).glob("*.open")):
        records = list(read_journal(path, recover=True))
        target = path.with_suffix(".journal" if records else ".empty")
        os.rename(path, target)
        sync_dir(path.parent)
        recovered.append({"stream": records[-1].stream if records else "UNKNOWN",
                          "last_recoverable_offset": records[-1].offset if records else 0,
                          "path": str(target)})
    return recovered
