#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixture_encoder.py — 独立的 RecordBatch v2 编码器

与 kafka_audit.py 的解析器不共享任何代码：zigzag varint、逐位 (非查表) CRC32C、
批次头均按规范独立编码，用于生成测试用二进制夹具。
"""
import struct

CRC32C_POLY = 0x82F63B78


def crc32c_bitwise(data: bytes) -> int:
    """逐位 (非查表) CRC-32C，与审计器的表驱动实现相互独立。"""
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ CRC32C_POLY
            else:
                crc >>= 1
    return crc ^ 0xFFFFFFFF


def encode_uvarint(value: int) -> bytes:
    if value < 0:
        raise ValueError("uvarint 不能为负")
    out = bytearray()
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def encode_svarint(value: int) -> bytes:
    """zigzag 编码后有符号数 -> varint。"""
    return encode_uvarint((value << 1) ^ (value >> 63))


def _bytes_field(raw) -> bytes:
    if raw is None:
        return encode_svarint(-1)
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return encode_svarint(len(raw)) + raw


def encode_record(offset_delta, key=None, value=None, headers=(), timestamp_delta=0):
    """headers: [(key, value|None)]，key 为 str 或 bytes，不得为 None。"""
    body = bytearray()
    body.append(0)  # record attributes (保留位)
    body += encode_svarint(timestamp_delta)
    body += encode_svarint(offset_delta)
    body += _bytes_field(key)
    body += _bytes_field(value)
    body += encode_svarint(len(headers))
    for hkey, hval in headers:
        body += _bytes_field(hkey)
        body += _bytes_field(hval)
    return encode_svarint(len(body)) + bytes(body)


def encode_batch(base_offset, records, *, producer_id=-1, producer_epoch=-1,
                 base_sequence=-1, transactional=False, control=False,
                 partition_leader_epoch=0, first_timestamp=1_700_000_000_000,
                 max_timestamp=None, length_override=None, crc_override=None):
    """records 的 offset_delta 必须为 0..n-1（编码器按此推导 lastOffsetDelta）。

    length_override / crc_override 用于构造损坏夹具。
    """
    if not records:
        raise ValueError("批次至少含 1 条记录")
    attributes = (0x10 if transactional else 0) | (0x20 if control else 0)
    tail = struct.pack(">h", attributes)
    tail += struct.pack(">i", len(records) - 1)          # lastOffsetDelta
    tail += struct.pack(">q", first_timestamp)
    tail += struct.pack(">q", max_timestamp if max_timestamp is not None
                        else first_timestamp)
    tail += struct.pack(">q", producer_id)
    tail += struct.pack(">h", producer_epoch)
    tail += struct.pack(">i", base_sequence)
    tail += struct.pack(">i", len(records))              # recordsCount
    tail += b"".join(records)
    crc = crc32c_bitwise(tail) if crc_override is None else crc_override
    body = struct.pack(">i", partition_leader_epoch)
    body += b"\x02"                                       # magic = 2
    body += struct.pack(">I", crc)
    body += tail
    batch_length = len(body) if length_override is None else length_override
    return struct.pack(">q", base_offset) + struct.pack(">i", batch_length) + body


def encode_control_batch(base_offset, marker, *, producer_id, producer_epoch,
                         coordinator_epoch=0, partition_leader_epoch=0,
                         timestamp=1_700_000_000_000):
    """marker: "commit" | "abort"。key = version(0)+type；value = EndTxnMarker v1。"""
    marker_type = {"abort": 0, "commit": 1}[marker]
    key = struct.pack(">hh", 0, marker_type)
    value = struct.pack(">hiq", 1, coordinator_epoch, timestamp)
    rec = encode_record(0, key=key, value=value)
    return encode_batch(base_offset, [rec], producer_id=producer_id,
                        producer_epoch=producer_epoch, base_sequence=-1,
                        transactional=True, control=True,
                        partition_leader_epoch=partition_leader_epoch,
                        first_timestamp=timestamp)
