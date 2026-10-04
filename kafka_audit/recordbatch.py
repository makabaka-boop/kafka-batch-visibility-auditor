"""Hand-rolled parser for Kafka magic=2 (RecordBatch v2) uncompressed streams.

No Kafka client code is used anywhere in this package. Wire layout:

  baseOffset           int64
  batchLength          int32    byte count of everything after this field
  partitionLeaderEpoch int32
  magic                int8     must be 2
  crc                  uint32   CRC32C over [attributes .. end of batch]
  attributes           int16    bits 0-2 compression, bit 4 transactional,
                                bit 5 control
  lastOffsetDelta      int32
  firstTimestamp       int64
  maxTimestamp         int64
  producerId           int64
  producerEpoch        int16
  baseSequence         int32
  records              ...      batchLength - 45 bytes

Record layout (all lengths/deltas are zigzag varints unless noted):

  length          varint
  attributes      int8
  timestampDelta  varlong
  offsetDelta     varint
  keyLength       varint (-1 = null)
  key             bytes
  valueLength     varint (-1 = null)
  value           bytes
  headerCount     varint
  headers         { keyLength varint, key, valueLength varint (-1 = null),
                  value } ...

Control batches (attributes bit 5) hold exactly one record whose key is
  version int16, type int16   (0 = abort, 1 = commit)
and whose value is
  version int16, coordinatorEpoch int32, timestamp int64.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from .crc32c import crc32c

MAGIC_V2 = 2
MAX_BATCHES = 32

ATTR_COMPRESSION_MASK = 0x0007
ATTR_TRANSACTIONAL = 0x0010
ATTR_CONTROL = 0x0020

_BATCH_HEADER = struct.Struct(">q i i b I h i q q q h i")
BATCH_HEADER_SIZE = _BATCH_HEADER.size  # 57
# batchLength counts everything after its own field: leader epoch .. records.
MIN_BATCH_LENGTH = BATCH_HEADER_SIZE - 12  # 45, i.e. a batch with zero records

LENGTH_FIELD_POS = 8
MAGIC_FIELD_POS = 16
CRC_FIELD_POS = 17
CRC_COVERED_POS = 21  # attributes .. end of batch
ATTRIBUTES_FIELD_POS = 21
LAST_OFFSET_DELTA_POS = 23

_CONTROL_KEY = struct.Struct(">hh")  # version, type
CONTROL_VERSION = 0
CONTROL_ABORT = 0
CONTROL_COMMIT = 1


class AuditError(Exception):
    """Fatal audit error.

    Raised whenever the byte stream cannot be trusted past a point. The
    auditor never skips a corrupt region to continue scanning.
    """

    def __init__(self, message: str, byte_pos: int | None = None):
        self.byte_pos = byte_pos
        if byte_pos is not None:
            message = f"byte {byte_pos}: {message}"
        super().__init__(message)


@dataclass
class Header:
    key: bytes
    value: bytes | None


@dataclass
class Record:
    offset: int
    byte_pos: int  # absolute stream position of the record's length varint
    byte_size: int
    timestamp_delta: int
    offset_delta: int
    key: bytes | None
    value: bytes | None
    headers: list[Header]


@dataclass
class Batch:
    index: int
    byte_pos: int
    byte_size: int
    base_offset: int
    last_offset: int
    partition_leader_epoch: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    first_timestamp: int
    max_timestamp: int
    is_transactional: bool
    is_control: bool
    control_marker: str | None  # "commit" | "abort" | None
    records: list[Record]

    @property
    def end_offset(self) -> int:
        """Log offset of the first record after this batch."""
        return self.last_offset + 1


@dataclass
class ParseResult:
    batches: list[Batch]
    input_size: int
    verified_end_pos: int  # bytes: end of the last complete, CRC-verified batch
    verified_end_offset: int  # log offset after the last complete batch
    truncated_pos: int | None
    truncated_reason: str | None

    @property
    def truncated(self) -> bool:
        return self.truncated_pos is not None


# ---------------------------------------------------------------------------
# zigzag varints


def _read_unsigned_varint(buf: bytes, pos: int, limit: int, max_bytes: int) -> tuple[int, int]:
    result = 0
    shift = 0
    start = pos
    while True:
        if pos >= limit:
            raise AuditError(f"truncated varint (starts at byte {start})", start)
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift >= 7 * max_bytes:
            raise AuditError(
                f"varint longer than {max_bytes} bytes (starts at byte {start})", start
            )


def _zigzag(raw: int) -> int:
    return (raw >> 1) ^ -(raw & 1)


def read_varint(buf: bytes, pos: int, limit: int) -> tuple[int, int]:
    """Read a signed 32-bit zigzag varint."""
    raw, pos = _read_unsigned_varint(buf, pos, limit, 5)
    return _zigzag(raw), pos


def read_varlong(buf: bytes, pos: int, limit: int) -> tuple[int, int]:
    """Read a signed 64-bit zigzag varint."""
    raw, pos = _read_unsigned_varint(buf, pos, limit, 10)
    return _zigzag(raw), pos


# ---------------------------------------------------------------------------
# stream parsing


def parse_stream(data: bytes) -> ParseResult:
    """Parse and verify every complete batch in ``data``.

    The log is declared to start at offset 0, uncompressed and uncompacted,
    so base offsets must be perfectly contiguous. A trailing incomplete
    batch is *marked* (not fatal); any structural or checksum failure
    mid-stream is fatal and the scan halts — corrupt batches are never
    skipped to keep scanning.
    """
    batches: list[Batch] = []
    pos = 0
    expected_offset = 0
    truncated_pos = None
    truncated_reason = None
    while pos < len(data):
        if len(batches) >= MAX_BATCHES:
            raise AuditError(
                f"input holds more than the allowed {MAX_BATCHES} batches", pos
            )
        remaining = len(data) - pos
        if remaining < 12:
            truncated_pos = pos
            truncated_reason = (
                f"incomplete batch header: {remaining} byte(s) remain, 12 are "
                "needed for base offset and batch length"
            )
            break
        base_offset, batch_length = struct.unpack_from(">q i", data, pos)
        if batch_length < MIN_BATCH_LENGTH:
            raise AuditError(
                f"corrupt batch length {batch_length} (a magic=2 batch needs "
                f"at least {MIN_BATCH_LENGTH}); the scanner does not skip "
                "corrupt batches, audit halted",
                pos + LENGTH_FIELD_POS,
            )
        batch_end = pos + 12 + batch_length
        if batch_end > len(data):
            truncated_pos = pos
            truncated_reason = (
                f"batch declares batchLength={batch_length} but only "
                f"{remaining - 12} byte(s) remain after its length field"
            )
            break
        if base_offset != expected_offset:
            raise AuditError(
                f"batch base offset {base_offset} does not continue the log "
                f"at offset {expected_offset}; the input is declared "
                "uncompacted and contiguous, audit halted",
                pos,
            )
        batches.append(_parse_batch(data, pos, batch_end, len(batches)))
        expected_offset = batches[-1].end_offset
        pos = batch_end
    return ParseResult(
        batches=batches,
        input_size=len(data),
        verified_end_pos=pos,
        verified_end_offset=expected_offset,
        truncated_pos=truncated_pos,
        truncated_reason=truncated_reason,
    )


def _parse_batch(data: bytes, pos: int, batch_end: int, index: int) -> Batch:
    (
        base_offset,
        batch_length,
        leader_epoch,
        magic,
        crc,
        attributes,
        last_offset_delta,
        first_ts,
        max_ts,
        producer_id,
        producer_epoch,
        base_sequence,
    ) = _BATCH_HEADER.unpack_from(data, pos)

    if magic != MAGIC_V2:
        raise AuditError(
            f"unsupported batch magic {magic}, expected {MAGIC_V2}",
            pos + MAGIC_FIELD_POS,
        )
    codec = attributes & ATTR_COMPRESSION_MASK
    if codec:
        raise AuditError(
            f"batch uses compression codec {codec}; the auditor only accepts "
            "uncompressed input",
            pos + ATTRIBUTES_FIELD_POS,
        )
    computed = crc32c(data[pos + CRC_COVERED_POS : batch_end])
    if computed != crc:
        raise AuditError(
            f"CRC32C mismatch in batch {index}: stored 0x{crc:08X}, computed "
            f"0x{computed:08X}; corrupt batches are never skipped, audit "
            "halted",
            pos + CRC_FIELD_POS,
        )
    if last_offset_delta < 0:
        raise AuditError(
            f"negative lastOffsetDelta {last_offset_delta}",
            pos + LAST_OFFSET_DELTA_POS,
        )

    is_control = bool(attributes & ATTR_CONTROL)
    records = _parse_records(data, pos + BATCH_HEADER_SIZE, batch_end, base_offset)

    if records:
        if records[-1].offset_delta != last_offset_delta:
            raise AuditError(
                f"last record offset delta {records[-1].offset_delta} does "
                f"not match header lastOffsetDelta {last_offset_delta}",
                pos + LAST_OFFSET_DELTA_POS,
            )
    elif last_offset_delta != 0:
        raise AuditError(
            f"batch declares lastOffsetDelta {last_offset_delta} but "
            "contains no records",
            pos + LAST_OFFSET_DELTA_POS,
        )

    control_marker = _parse_control_marker(records, pos) if is_control else None

    return Batch(
        index=index,
        byte_pos=pos,
        byte_size=batch_end - pos,
        base_offset=base_offset,
        last_offset=base_offset + last_offset_delta,
        partition_leader_epoch=leader_epoch,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        base_sequence=base_sequence,
        first_timestamp=first_ts,
        max_timestamp=max_ts,
        is_transactional=bool(attributes & ATTR_TRANSACTIONAL),
        is_control=is_control,
        control_marker=control_marker,
        records=records,
    )


def _parse_control_marker(records: list[Record], batch_pos: int) -> str:
    if len(records) != 1:
        raise AuditError(
            f"control batch must hold exactly one record, found "
            f"{len(records)}",
            batch_pos,
        )
    record = records[0]
    key = record.key
    if key is None or len(key) < _CONTROL_KEY.size:
        raise AuditError(
            "control record carries no valid 4-byte key", record.byte_pos
        )
    version, control_type = _CONTROL_KEY.unpack_from(key, 0)
    if version != CONTROL_VERSION:
        raise AuditError(
            f"unsupported control key version {version}", record.byte_pos
        )
    if control_type == CONTROL_ABORT:
        return "abort"
    if control_type == CONTROL_COMMIT:
        return "commit"
    raise AuditError(
        f"unknown control record type {control_type}", record.byte_pos
    )


def _parse_records(
    data: bytes, pos: int, batch_end: int, base_offset: int
) -> list[Record]:
    records: list[Record] = []
    expected_delta = 0
    while pos < batch_end:
        rec_pos = pos
        length, pos = read_varint(data, pos, batch_end)
        if length <= 0:
            raise AuditError(f"corrupt record length {length}", rec_pos)
        rec_end = pos + length
        if rec_end > batch_end:
            raise AuditError(
                f"record declares {length} byte(s) and overruns the batch "
                f"end at byte {batch_end}",
                rec_pos,
            )
        pos += 1  # record attributes byte, currently unused
        timestamp_delta, pos = read_varlong(data, pos, rec_end)
        offset_delta, pos = read_varint(data, pos, rec_end)
        if offset_delta != expected_delta:
            raise AuditError(
                f"record offset delta {offset_delta} breaks the contiguous "
                f"uncompacted sequence (expected {expected_delta})",
                rec_pos,
            )
        key, pos = _read_nullable_bytes(data, pos, rec_end, "key")
        value, pos = _read_nullable_bytes(data, pos, rec_end, "value")
        header_count, pos = read_varint(data, pos, rec_end)
        if header_count < 0:
            raise AuditError(f"negative header count {header_count}", rec_pos)
        headers = []
        for _ in range(header_count):
            hk, pos = _read_nullable_bytes(data, pos, rec_end, "header key", nullable=False)
            hv, pos = _read_nullable_bytes(data, pos, rec_end, "header value")
            headers.append(Header(hk, hv))
        if pos != rec_end:
            raise AuditError(
                f"record length mismatch: {rec_end - pos} unconsumed byte(s) "
                "inside the record",
                rec_pos,
            )
        records.append(
            Record(
                offset=base_offset + offset_delta,
                byte_pos=rec_pos,
                byte_size=rec_end - rec_pos,
                timestamp_delta=timestamp_delta,
                offset_delta=offset_delta,
                key=key,
                value=value,
                headers=headers,
            )
        )
        expected_delta += 1
    return records


def _read_nullable_bytes(
    data: bytes, pos: int, limit: int, what: str, nullable: bool = True
) -> tuple[bytes | None, int]:
    length, pos = read_varint(data, pos, limit)
    if length == -1 and nullable:
        return None, pos
    if length < 0:
        raise AuditError(f"invalid {what} length {length}", pos)
    if pos + length > limit:
        raise AuditError(f"{what} overruns its enclosing record", pos)
    return data[pos : pos + length], pos + length
