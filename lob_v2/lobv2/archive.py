"""Independent Zstd blocks, indexed footer and last-created immutable commits."""
import base64
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import uuid

import google_crc32c
import zstandard as zstd

from .journal import atomic_json, read_journal, sync_dir
from .wire import FRAME, MAX_RECORD, SCHEMA_DIGEST, decode, json_bytes

MAGIC = b"LOBV2Z02"
TRAILER_MAGIC = b"LOBV2END"
BLOCK = struct.Struct("<IIIIQQ")
TRAILER = struct.Struct("<II8s")
ARCHIVE_MAX = 512*1024*1024
FOOTER_MAX = 8*1024*1024


def sha_file(path):
    sha = hashlib.sha256()
    with Path(path).open("rb") as src:
        for part in iter(lambda: src.read(1024*1024), b""):
            sha.update(part)
    return sha.hexdigest()


def build_archive(journal, destination, block_bytes=1024*1024, max_bytes=ARCHIVE_MAX):
    if not 1 <= block_bytes <= MAX_RECORD:
        raise ValueError("BLOCK_BUDGET")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_name(destination.name+".tmp-"+str(uuid.uuid4()))
    compressor = zstd.ZstdCompressor(level=3, write_checksum=True)
    index, buffer, records, stream_sha = [], bytearray(), [], hashlib.sha256()
    count = 0
    lifecycle_last_cut=0
    def flush(out):
        nonlocal buffer, records
        if not records:
            return
        raw = bytes(buffer)
        compressed = compressor.compress(raw)
        if out.tell()+BLOCK.size+len(compressed)+FOOTER_MAX+TRAILER.size > max_bytes:
            raise ValueError("ARCHIVE_TOO_LARGE_KEEP_JOURNAL")
        position = out.tell()
        first, last = records[0], records[-1]
        out.write(BLOCK.pack(len(compressed), len(raw), len(records),
                            google_crc32c.value(compressed), first.offset, last.offset))
        out.write(compressed)
        index.append({"position": position, "compressed_bytes": len(compressed), "raw_bytes": len(raw),
            "count": len(records), "first": first.offset, "last": last.offset,
            "min_utc_ns": min(r.created_ns for r in records), "max_utc_ns": max(r.created_ns for r in records),
            "markets": sorted({r.body["market"] for r in records if "market" in r.body}),
            "snapshots": [r.offset for r in records if r.body.get("kind") in ("SNAPSHOT", "SOURCE_RESET")]})
        buffer, records = bytearray(), []
    try:
        with tmp.open("xb") as out:
            out.write(MAGIC)
            for rec in read_journal(journal):
                if records and len(buffer)+len(rec.frame) > block_bytes:
                    flush(out)
                buffer.extend(rec.frame)
                records.append(rec)
                stream_sha.update(rec.frame)
                count += 1
                if rec.kind=='LIFECYCLE_CAPTURED':lifecycle_last_cut=rec.offset
                if count == 1:
                    producer, stream = rec.producer, rec.stream
            flush(out)
            if not count:
                raise ValueError("EMPTY_JOURNAL")
            footer = {"version": 2, "producer": producer, "stream": stream,
                "schema_digest": SCHEMA_DIGEST, "count": count, "blocks": index,
                "lifecycle_last_cut":lifecycle_last_cut,
                "first": index[0]["first"], "last": index[-1]["last"],
                "recordstream_sha256": stream_sha.hexdigest()}
            encoded = json_bytes(footer)
            if len(encoded) > FOOTER_MAX or out.tell()+len(encoded)+TRAILER.size > max_bytes:
                raise ValueError("FOOTER_TOO_LARGE")
            out.write(encoded)
            out.write(TRAILER.pack(len(encoded), google_crc32c.value(encoded), TRAILER_MAGIC))
            out.flush()
            os.fsync(out.fileno())
        # Independently decode before making the local container available.
        verify_archive(tmp)
        os.replace(tmp, destination)
        sync_dir(destination.parent)
        return {**footer, "sha256": sha_file(destination), "object_bytes": destination.stat().st_size}
    finally:
        tmp.unlink(missing_ok=True)


def footer_of(src):
    src.seek(0, 2)
    size = src.tell()
    if size > ARCHIVE_MAX or size < len(MAGIC)+TRAILER.size:
        raise ValueError("ARCHIVE_SIZE")
    src.seek(-TRAILER.size, 2)
    length, crc, magic = TRAILER.unpack(src.read(TRAILER.size))
    if magic != TRAILER_MAGIC or length > FOOTER_MAX or length > size-len(MAGIC)-TRAILER.size:
        raise ValueError("INVALID_FOOTER_LENGTH")
    start = size-TRAILER.size-length
    src.seek(start)
    encoded = src.read(length)
    if google_crc32c.value(encoded) != crc:
        raise ValueError("FOOTER_CRC")
    footer = json.loads(encoded)
    if footer["schema_digest"] != SCHEMA_DIGEST or len(footer["blocks"]) > 65536:
        raise ValueError("INVALID_FOOTER")
    src.seek(0)
    if src.read(len(MAGIC)) != MAGIC:
        raise ValueError("ARCHIVE_MAGIC")
    position = len(MAGIC)
    for block in footer["blocks"]:
        if block["position"] != position or not 0 < block["raw_bytes"] <= MAX_RECORD:
            raise ValueError("INVALID_BLOCK_INDEX")
        position += BLOCK.size+block["compressed_bytes"]
        if position > start:
            raise ValueError("BLOCK_OVERLAP")
    if position != start:
        raise ValueError("BLOCK_RANGE_MISMATCH")
    return footer


def read_archive(path):
    with Path(path).open("rb") as src:
        footer = footer_of(src)
        for block in footer["blocks"]:
            src.seek(block["position"])
            clen, rawlen, count, crc, first, last = BLOCK.unpack(src.read(BLOCK.size))
            if (clen,rawlen,count,first,last) != (block["compressed_bytes"],block["raw_bytes"],block["count"],block["first"],block["last"]):
                raise ValueError("BLOCK_HEADER_MISMATCH")
            if clen > ARCHIVE_MAX or rawlen > MAX_RECORD:
                raise ValueError("BLOCK_SIZE")
            compressed = src.read(clen)
            if len(compressed) != clen or google_crc32c.value(compressed) != crc:
                raise ValueError("BLOCK_CRC")
            raw = zstd.ZstdDecompressor().decompress(compressed, max_output_size=rawlen)
            if len(raw) != rawlen:
                raise ValueError("BLOCK_RAW_SIZE")
            position = 0
            block_records = []
            while position < len(raw):
                if position + FRAME.size > len(raw):
                    raise ValueError("BLOCK_SHORT_FRAME")
                length, _ = FRAME.unpack_from(raw, position)
                if length + FRAME.size > MAX_RECORD or position+FRAME.size+length > len(raw):
                    raise ValueError("BLOCK_INVALID_LENGTH")
                rec = decode(raw[position:position+FRAME.size+length])
                block_records.append(rec)
                position += FRAME.size+length
            if len(block_records) != count or block_records[0].offset != first or block_records[-1].offset != last:
                raise ValueError("BLOCK_RANGE_OR_COUNT")
            yield from block_records


def verify_archive(path):
    with Path(path).open("rb") as src:
        footer = footer_of(src)
    sha, count, previous = hashlib.sha256(), 0, footer["first"]-1
    for rec in read_archive(path):
        if rec.offset != previous+1 or rec.stream != footer["stream"] or rec.producer != footer["producer"]:
            raise ValueError("ARCHIVE_RECORD_IDENTITY")
        sha.update(rec.frame)
        previous, count = rec.offset, count+1
    if count != footer["count"] or sha.hexdigest() != footer["recordstream_sha256"]:
        raise ValueError("ARCHIVE_DIGEST_OR_COUNT")
    return footer


class DirectoryStore:
    """A qualification backend; same-host files are never labelled REMOTE_ARCHIVED."""
    backend = "DIRECTORY_TEST_BACKEND"

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key):
        if key.startswith("/") or ".." in Path(key).parts:
            raise ValueError("INVALID_OBJECT_KEY")
        return self.root / key

    def put(self, key, data):
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Last publication step is an atomic link of an already durable object.
        tmp = path.with_name(path.name+".tmp-"+str(uuid.uuid4()))
        try:
            with tmp.open("xb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            try:
                os.link(tmp, path)
                sync_dir(path.parent)
            except FileExistsError:
                if path.read_bytes() != data:
                    raise ValueError("IMMUTABLE_OBJECT_CONFLICT")
        finally:
            tmp.unlink(missing_ok=True)

    def get(self, key):
        return self._path(key).read_bytes()

    def keys(self, prefix):
        return sorted(str(p.relative_to(self.root)) for p in self._path(prefix).rglob("*.json"))


class S3Store:
    backend = "S3"

    def __init__(self, bucket, prefix, endpoint_url=None):
        import boto3
        from botocore.config import Config
        self.client = boto3.client("s3", endpoint_url=endpoint_url,
            config=Config(connect_timeout=5,read_timeout=10,retries={"mode":"standard","max_attempts":3}))
        self.bucket, self.prefix = bucket, prefix.strip("/")

    def put(self, key, data):
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
        try:
            self.client.put_object(Bucket=self.bucket, Key=self.prefix+"/"+key, Body=data,
                                   IfNoneMatch="*", ChecksumSHA256=checksum)
        except Exception:
            # Covers 412 and ambiguous timeouts; exact readback alone resolves success.
            if self.get(key) != data:
                raise
        if self.get(key) != data:
            raise ValueError("S3_FULL_READBACK_MISMATCH")

    def get(self, key):
        result = self.client.get_object(Bucket=self.bucket, Key=self.prefix+"/"+key, ChecksumMode="ENABLED")
        if result["ContentLength"] > ARCHIVE_MAX:
            raise ValueError("OBJECT_TOO_LARGE")
        return result["Body"].read(ARCHIVE_MAX+1)

    def keys(self, prefix):
        paginator = self.client.get_paginator("list_objects_v2")
        return sorted(row["Key"][len(self.prefix)+1:] for page in paginator.paginate(
            Bucket=self.bucket, Prefix=self.prefix+"/"+prefix) for row in page.get("Contents", []))


def commit_archive(store, path, kind, *, fail_before_manifest=False):
    metadata = verify_archive(path)
    data = Path(path).read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    stem = f"{kind}/{metadata['producer']}/{metadata['stream']}/{metadata['first']:020d}-{metadata['last']:020d}"
    key, manifest_key = "objects/"+stem+".lobseg", "commits/"+stem+".json"
    store.put(key, data)
    if store.get(key) != data:
        raise ValueError("OBJECT_READBACK_MISMATCH")
    records=read_archive(path)
    try:
        policy=next(records).body.get("policy_version","single_ready_route_v2.0.0")
    finally:
        records.close()
    manifest = {"version": 2, "archive_kind": kind, "backend": store.backend,
        "data_key": key, "object_sha256": sha, "object_bytes": len(data),
        "verification": "FULL_DOWNLOAD_DECOMPRESS", "policy_version": policy,
        **metadata}
    if fail_before_manifest:
        raise OSError("INJECTED_MANIFEST_FAILURE")
    store.put(manifest_key, json_bytes(manifest))
    if json.loads(store.get(manifest_key)) != manifest:
        raise ValueError("COMMIT_READBACK_MISMATCH")
    return manifest_key, manifest


def restore(store, manifest_key, destination):
    manifest = json.loads(store.get(manifest_key))
    data = store.get(manifest["data_key"])
    if len(data) != manifest["object_bytes"] or hashlib.sha256(data).hexdigest() != manifest["object_sha256"]:
        raise ValueError("REMOTE_OBJECT_MISMATCH")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())
    if verify_archive(destination)["recordstream_sha256"] != manifest["recordstream_sha256"]:
        raise ValueError("REMOTE_STREAM_MISMATCH")
    sync_dir(destination.parent)
    return manifest


def progress(manifests, local_last):
    intervals = sorted((m["first"],m["last"]) for m in manifests)
    prefix, holes, cursor, committed = 0, [], 1, 0
    for first,last in intervals:
        if first < cursor:
            raise ValueError("OVERLAPPING_COMMITTED_RANGES")
        if first > cursor:
            holes.append([cursor,first-1])
        if first == prefix+1:
            prefix = last
        committed += last-first+1
        cursor = last+1
    if cursor <= local_last:
        holes.append([cursor,local_last])
    return {"remote_contiguous_offset": prefix, "committed_intervals": intervals,
            "holes": holes, "uncommitted_records": local_last-committed}


def evict(journal, archive, store, manifest_key, *, active_dependencies):
    """Call only after the writer/reader owner has released this sealed segment."""
    if active_dependencies:
        raise ValueError("ACTIVE_LOCAL_DEPENDENCIES")
    if Path(journal).suffix != ".journal":
        raise ValueError("ONLY_SEALED_JOURNAL_CAN_BE_EVICTED")
    manifest = json.loads(store.get(manifest_key))
    remote = store.get(manifest["data_key"])
    if hashlib.sha256(remote).hexdigest() != manifest["object_sha256"]:
        raise ValueError("EVICTION_REMOTE_CHECKSUM")
    sha,count = hashlib.sha256(),0
    for rec in read_journal(journal):
        sha.update(rec.frame)
        count+=1
    if sha.hexdigest() != manifest["recordstream_sha256"] or count != manifest["count"]:
        raise ValueError("EVICTION_RANGE_MISMATCH")
    if sha_file(archive) != manifest["object_sha256"]:
        raise ValueError("EVICTION_LOCAL_ARCHIVE_MISMATCH")
    released = 0
    for path in (Path(journal),Path(archive)):
        released += path.stat().st_size
        path.unlink()
        sync_dir(path.parent)
    return {"released_bytes": released, "manifest": manifest_key, "active_dependencies": [],
            "archive_verification": "FULL_READBACK", "local_dependency_owner_released": True}
