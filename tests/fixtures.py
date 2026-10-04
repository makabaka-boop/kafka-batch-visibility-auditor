"""Independent encoder for Kafka magic=2 RecordBatch test fixtures.

This module deliberately shares no code with ``kafka_audit``: the batch
layout, zigzag varints and CRC32C are re-implemented from the format
description so the tests exercise the auditor against an independently
written encoder. CRC32C here is computed bit-by-bit; the auditor uses a
table-driven version.
"""

import struct

CRC32C_POLY = 0x82F63B78

ABORT = 0
COMMIT = 1


def crc32c_bitwise(data: bytes, crc: int = 0) -> int:
    crc ^= 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ CRC32C_POLY if crc & 1 else crc >> 1
    return crc ^ 0xFFFFFFFF


def uvarint(n: int) -> bytes:
    assert n >= 0
    out = bytearray()
    while True:
        low = n & 0x7F
        n >>= 7
        if n:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def varint(n: int) -> bytes:
    """32-bit zigzag."""
    return uvarint((n << 1) ^ (n >> 31))


def varlong(n: int) -> bytes:
    """64-bit zigzag."""
    return uvarint((n << 1) ^ (n >> 63))


def _nullable(blob: bytes | None) -> bytes:
    if blob is None:
        return varint(-1)
    return varint(len(blob)) + blob


def record(
    offset_delta: int,
    key: bytes | None,
    value: bytes | None,
    timestamp_delta: int = 0,
    headers: tuple[tuple[bytes, bytes | None], ...] = (),
) -> bytes:
    body = bytearray()
    body.append(0)  # record attributes
    body += varlong(timestamp_delta)
    body += varint(offset_delta)
    body += _nullable(key)
    body += _nullable(value)
    body += varint(len(headers))
    for hkey, hvalue in headers:
        assert hkey is not None
        body += varint(len(hkey)) + hkey
        body += _nullable(hvalue)
    return varint(len(body)) + bytes(body)


def control_record(kind: int, coordinator_epoch: int = 0, timestamp: int = 2000) -> bytes:
    key = struct.pack(">hh", 0, kind)
    value = struct.pack(">hiq", 0, coordinator_epoch, timestamp)
    return record(0, key, value)


def batch(
    base_offset: int,
    records: list[bytes],
    last_offset_delta: int | None = None,
    *,
    producer_id: int = -1,
    producer_epoch: int = -1,
    base_sequence: int = -1,
    transactional: bool = False,
    control: bool = False,
    leader_epoch: int = 0,
    first_timestamp: int = 1000,
    max_timestamp: int = 1000,
    magic_override: int | None = None,
    attributes_override: int | None = None,
    crc_override: int | None = None,
    length_override: int | None = None,
) -> bytes:
    if last_offset_delta is None:
        last_offset_delta = len(records) - 1 if records else 0
    if attributes_override is not None:
        attributes = attributes_override
    else:
        attributes = (0x10 if transactional else 0) | (0x20 if control else 0)
    tail = struct.pack(
        ">h i q q q h i",
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
    ) + b"".join(records)
    crc = crc32c_bitwise(tail) if crc_override is None else crc_override
    magic = 2 if magic_override is None else magic_override
    batch_length = 4 + 1 + 4 + len(tail)
    if length_override is not None:
        batch_length = length_override
    return (
        struct.pack(">q i", base_offset, batch_length)
        + struct.pack(">i b I", leader_epoch, magic, crc)
        + tail
    )
