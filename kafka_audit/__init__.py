"""Offline read_committed auditor for Kafka magic=2 RecordBatch streams."""

from .auditor import AuditReport, RecordVerdict, Transaction, audit
from .recordbatch import (
    AuditError,
    Batch,
    Header,
    ParseResult,
    Record,
    parse_stream,
)

__all__ = [
    "AuditError",
    "AuditReport",
    "Batch",
    "Header",
    "ParseResult",
    "Record",
    "RecordVerdict",
    "Transaction",
    "audit",
    "parse_stream",
]

__version__ = "1.0.0"
