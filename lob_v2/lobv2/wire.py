"""One protobuf envelope, one encoding, reused by journal and transport."""
import asyncio
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import struct
import time

import google_crc32c
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

MAX_RECORD = 16 * 1024 * 1024  # includes the eight framing bytes
FRAME = struct.Struct("<II")
SCHEMA_DIGEST = hashlib.sha256((Path(__file__).parents[1] / "envelope.proto").read_bytes()).hexdigest()
fd = descriptor_pb2.FileDescriptorProto(name="lob_v2.proto", package="polymarket.lob.v2", syntax="proto3")
msg = fd.message_type.add(name="Envelope")
for number, (name, typ) in enumerate([
    ("schema_version", 13), ("kind", 9), ("producer", 9), ("stream", 9),
    ("offset", 4), ("created_utc_ns", 4), ("body", 12), ("raw", 12),
], 1):
    msg.field.add(name=name, number=number, type=typ, label=1)
pool = descriptor_pool.DescriptorPool()
pool.Add(fd)
Envelope = message_factory.GetMessageClass(pool.FindMessageTypeByName("polymarket.lob.v2.Envelope"))


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()


def frame(payload):
    if len(payload) + FRAME.size > MAX_RECORD:
        raise ValueError("RECORD_TOO_LARGE")
    return FRAME.pack(len(payload), google_crc32c.value(payload)) + payload


def unframe(data):
    if len(data) < FRAME.size:
        raise ValueError("SHORT_FRAME")
    length, crc = FRAME.unpack_from(data)
    if length + FRAME.size > MAX_RECORD or length != len(data) - FRAME.size:
        raise ValueError("INVALID_LENGTH")
    payload = data[FRAME.size:]
    if google_crc32c.value(payload) != crc:
        raise ValueError("CRC_MISMATCH")
    return payload


@dataclass(frozen=True)
class Record:
    kind: str
    producer: str
    stream: str
    offset: int
    body: dict
    raw: bytes
    created_ns: int
    frame: bytes


def encode(kind, producer, stream, offset, body, raw=b"", created_ns=None):
    created_ns = time.time_ns() if created_ns is None else created_ns
    env = Envelope(schema_version=2, kind=kind, producer=producer, stream=stream,
                   offset=offset, created_utc_ns=created_ns, body=json_bytes(body), raw=raw)
    return Record(kind, producer, stream, offset, body, raw, created_ns,
                  frame(env.SerializeToString(deterministic=True)))


def decode(data):
    env = Envelope.FromString(unframe(data))
    if env.schema_version != 2:
        raise ValueError("UNSUPPORTED_SCHEMA")
    return Record(env.kind, env.producer, env.stream, env.offset,
                  json.loads(env.body), env.raw, env.created_utc_ns, data)


async def read_frame(reader):
    head = await reader.readexactly(FRAME.size)
    size, _ = FRAME.unpack(head)
    if size + FRAME.size > MAX_RECORD:
        raise ValueError("INVALID_LENGTH")
    data = head + await reader.readexactly(size)
    unframe(data)
    return data


async def send(writer, data):
    writer.write(data)
    await writer.drain()


def control(kind, body):
    return encode(kind, "transport", "", 0, body).frame


class ByteQueue:
    """Fail closed on overflow; no silently discarded complete groups."""
    def __init__(self, max_bytes=64 * 1024 * 1024, max_items=8192):
        if max_bytes < MAX_RECORD or max_items < 1:
            raise ValueError("QUEUE_SMALLER_THAN_LEGAL_RECORD")
        self.limit = max_bytes
        self.bytes = 0
        self.high_water = 0
        self.queue = asyncio.Queue(maxsize=max_items)
        self.failed = False

    def put(self, data):
        if self.failed or self.bytes + len(data) > self.limit or self.queue.full():
            self.failed = True
            raise BufferError("RESYNC_REQUIRED")
        self.queue.put_nowait(data)
        self.bytes += len(data)
        self.high_water = max(self.bytes, self.high_water)

    async def get(self):
        if self.failed:
            raise BufferError("RESYNC_REQUIRED")
        data = await self.queue.get()
        self.bytes -= len(data)
        return data

    def get_nowait(self):
        if self.failed:
            raise BufferError("RESYNC_REQUIRED")
        data=self.queue.get_nowait()
        self.bytes-=len(data)
        return data
