"""CRC-32C (Castagnoli), table-driven.

Kafka RecordBatch (magic=2) checksums are CRC32C over every byte after the
crc field itself. Implemented by hand; no Kafka client code is involved.
"""

_POLY_REFLECTED = 0x82F63B78


def _build_table() -> tuple[int, ...]:
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ _POLY_REFLECTED if crc & 1 else crc >> 1
        table.append(crc)
    return tuple(table)


_TABLE = _build_table()


def crc32c(data: bytes, crc: int = 0) -> int:
    crc ^= 0xFFFFFFFF
    for byte in data:
        crc = _TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF
