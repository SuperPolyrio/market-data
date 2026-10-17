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
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

from .wire import FRAME, MAX_RECORD, SCHEMA_DIGEST, decode, encode, json_bytes, unframe
from .telemetry import Metrics

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
        self.offset = self.written_offset = self.durable_offset = self.applied_offset = self.published_offset = 0
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
        self._io = ThreadPoolExecutor(max_workers=1, thread_name_prefix="journal-io")
        self._arrival = asyncio.Event()
        self.batch_records, self.batch_bytes = 1024, 4*1024*1024
        self.apply_budget_ns = 2_000_000
        self.cut_receipts = {}
        self.stage_samples = deque(maxlen=4096)
        self.admission_failure = None
        self.metrics = Metrics()
        self._open()

    def _open(self, first_offset=None):
        self.segment += 1
        self.path = self.root / f"{self.kind}-{self.stream}-{self.segment:06d}.open"
        self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        header = json_bytes({"version": 2, "producer": self.producer, "stream": self.stream,
                             "kind": self.kind, "first_offset": (self.pending[0].offset if self.pending else self.offset+1) if first_offset is None else first_offset,
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

    def append(self, kind, body, raw=b"", created_ns=None, prepared=None):
        if self.failure or self.admission_failure or self.closed:
            raise OSError("JOURNAL_UNAVAILABLE")
        if getattr(self,"policy_version",None):
            body={"policy_version":self.policy_version,**body}
        try:
            rec = encode(kind, self.producer, self.stream, self.offset+1, body, raw, created_ns, prepared)
        except (ValueError,TypeError) as exc:
            self.admission_failure = "ENCODING_REJECTED:"+repr(exc)
            raise
        if self.pending_size + len(rec.frame) > self.pending_limit:
            self.admission_failure = "JOURNAL_PENDING_OVERFLOW"
            raise BufferError("JOURNAL_PENDING_OVERFLOW")
        rec.trace["journal_enqueue"] = time.monotonic_ns()
        if kind in ("PLANT_EMIT","TAPE"):
            rec.trace["normalized_record_created"] = body["decision_mono_ns"]
            if body.get("input_timing") and body["input_timing"].get("clock_id") == body.get("clock_id"):
                rec.trace.update({k:v for k,v in body["input_timing"].items() if k!="clock_id"})
        self.offset += 1
        self.pending.append(rec)
        self.pending_size += len(rec.frame)
        self._arrival.set()
        return rec

    def receipt(self, rec):
        return {"log_id":self.producer+":"+self.kind,"log_generation":self.stream,
                "record_sequence":rec.offset,"segment":self.segment,"end_offset":self.size}

    def complete_durable(self, generation, cut, receipt):
        if generation != self.stream or cut <= self.durable_offset:
            self.metrics.counters["ignored_stale_sync_completion"] += 1
            return False
        if cut != self.inflight_cut or cut > self.written_offset or receipt["record_sequence"] != cut:
            raise ValueError("INVALID_SYNC_COMPLETION_CUT")
        self.durable_offset = cut
        self.cut_receipts["durable"] = receipt
        return True

    def _write_batch(self, records):
        """The sole writer/sync worker. Rotation and short writes stay ordered."""
        for rec in records:
            rec.trace["journal_dequeue"] = time.monotonic_ns()
            if self.segment_records and (self.size+len(rec.frame)>self.max_bytes or
                                         time.monotonic()-self.opened>=self.max_age):
                self._seal_file()
                self._open(rec.offset)
            rec.trace["write_start"] = time.monotonic_ns()
            self._write(rec.frame)
            self.size += len(rec.frame)
            self.segment_records += 1
            self.written_offset = rec.offset
            rec.trace["write_end"] = time.monotonic_ns()
            rec.trace["receipt"] = self.receipt(rec)
            self.cut_receipts["written"] = rec.trace["receipt"]
        start = time.monotonic_ns()
        os.fdatasync(self.fd)
        end = time.monotonic_ns()
        return start, end

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
            target, cancelled = self.offset, False
            while self.pending and self.pending[0].offset <= target:
                records, size = [], 0
                for rec in islice(self.pending,self.batch_records):
                    if rec.offset > target or records and size+len(rec.frame)>self.batch_bytes:
                        break
                    records.append(rec)
                    size += len(rec.frame)
                requested = time.monotonic_ns()
                self.inflight_cut = records[-1].offset
                future = asyncio.get_running_loop().run_in_executor(self._io,self._write_batch,records)
                try:
                    try:
                        syscall_start, syscall_end = await asyncio.shield(future)
                    except asyncio.CancelledError:
                        # A cancelled coroutine must not abandon an in-flight write
                        # and enqueue the same bytes again during shutdown.
                        syscall_start, syscall_end = await future
                        cancelled = True
                except BaseException as exc:
                    self.failure = repr(exc)
                    raise
                resumed = time.monotonic_ns()
                self.complete_durable(self.stream,records[-1].offset,records[-1].trace["receipt"])
                self.syncs += 1
                self.max_sync_batch = max(self.max_sync_batch,len(records))
                self.sync_latencies_ms.append((resumed-syscall_start)/1e6)
                budget_start = resumed
                for rec in records:
                    rec.trace.update(sync_requested=requested,sync_syscall_start=syscall_start,
                                     sync_syscall_end=syscall_end,sync_callback_resume=resumed)
                    try:
                        for callback in self.callbacks:
                            callback(rec)
                    except BaseException as exc:
                        self.failure = "COMMITTED_APPLY_FAILED:"+repr(exc)
                        raise
                    assert self.pending.popleft() is rec
                    self.pending_size -= len(rec.frame)
                    self.applied_offset = self.published_offset = rec.offset
                    self.cut_receipts["applied"] = self.cut_receipts["published"] = rec.trace["receipt"]
                    self.metrics.finish(rec)
                    if rec.offset%64==0 or rec.body.get("kind")=="SOURCE_RESET" or (
                        "committed_apply_end" in rec.trace and rec.trace["committed_apply_end"]-rec.body["decision_mono_ns"]>50_000_000):
                        self.stage_samples.append({"log_generation":self.stream,"record_sequence":rec.offset,"kind":rec.kind,
                            "event_kind":rec.body.get("kind"),"market":rec.body.get("market"),
                            "capture_ref":rec.body.get("raw_ref"),**rec.trace})
                    if time.monotonic_ns()-budget_start>=self.apply_budget_ns:
                        try:
                            await asyncio.sleep(0)
                        except asyncio.CancelledError:
                            cancelled = True
                        budget_start = time.monotonic_ns()
            if cancelled:
                raise asyncio.CancelledError

    async def run(self, stop, interval=.020):
        while not stop.is_set():
            if self.pending:
                remaining = self.pending[0].trace["journal_enqueue"]/1e9+interval-time.monotonic()
                if remaining>0:
                    try:
                        await asyncio.wait_for(stop.wait(),remaining)
                    except asyncio.TimeoutError:
                        pass
                await self.sync()
            else:
                self._arrival.clear()
                try:
                    await asyncio.wait_for(self._arrival.wait(),min(.2,self.max_age))
                except asyncio.TimeoutError:
                    pass
            if self.segment_records and not self.pending and time.monotonic()-self.opened >= self.max_age:
                async with self._sync_lock:
                    # No new writes run concurrently with the rotation barrier.
                    await asyncio.get_running_loop().run_in_executor(self._io,self._seal_file)
                    await asyncio.get_running_loop().run_in_executor(self._io,self._open)
        await self.sync()

    async def close(self):
        while self.pending:
            await self.sync()
        if self.fd is not None:
            if self.segment_records:
                await asyncio.get_running_loop().run_in_executor(self._io,self._seal_file)
            else:
                os.close(self.fd)
                self.fd = None
                os.rename(self.path,self.path.with_suffix(".empty"))
                sync_dir(self.root)
        self.closed = True
        self._io.shutdown(wait=True)


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
