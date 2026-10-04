"""Command line interface for the offline read_committed auditor."""

from __future__ import annotations

import argparse
import json
import sys

from .auditor import AuditReport, audit
from .recordbatch import AuditError, Batch

EXIT_OK = 0
EXIT_AUDIT_FAILED = 1
EXIT_NO_DELIVERY = 2  # high watermark beyond the verified region
EXIT_USAGE = 3


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="kafka-audit",
        description="Offline auditor: decide which records of a Kafka magic=2 "
        "uncompressed RecordBatch stream are deliverable under "
        "read_committed.",
    )
    parser.add_argument(
        "logfile", help="file with the raw batch stream (offset 0 first)"
    )
    parser.add_argument(
        "--hw",
        type=int,
        required=True,
        help="high watermark as a log offset; must land on a batch boundary",
    )
    parser.add_argument(
        "--json", action="store_true", help="emit JSON instead of text"
    )
    args = parser.parse_args(argv)

    try:
        with open(args.logfile, "rb") as fh:
            data = fh.read()
    except OSError as exc:
        print(f"error: cannot read {args.logfile}: {exc}", file=sys.stderr)
        return EXIT_USAGE

    try:
        report = audit(data, args.hw)
    except AuditError as exc:
        print(f"AUDIT FAILED: {exc}", file=sys.stderr)
        return EXIT_AUDIT_FAILED

    if args.json:
        print(_render_json(report))
    else:
        print(_render_text(report))
    return EXIT_OK if report.status == "ok" else EXIT_NO_DELIVERY


# ---------------------------------------------------------------------------
# text rendering


def _render_text(report: AuditReport) -> str:
    out: list[str] = []
    parsed = report.parsed
    out.append("== stream verification ==")
    out.append(
        f"input: {parsed.input_size} bytes, {len(parsed.batches)} complete "
        f"batch(es); verified region: bytes [0, {parsed.verified_end_pos}), "
        f"offsets [0, {parsed.verified_end_offset})"
    )
    if parsed.truncated:
        out.append(
            f"TRUNCATED TAIL at byte {parsed.truncated_pos}: "
            f"{parsed.truncated_reason} (marked, not scanned)"
        )
    out.append("")
    out.append("== batches ==")
    if not parsed.batches:
        out.append("(none)")
    for batch in parsed.batches:
        out.append(_format_batch(batch))

    if report.status != "ok":
        out.append("")
        out.append("== read_committed outcome ==")
        out.append(
            f"high watermark {report.high_watermark} exceeds the verified "
            f"region (verified up to offset {parsed.verified_end_offset})"
        )
        out.append("no delivery list is provided")
        return "\n".join(out)

    out.append("")
    out.append(f"== record verdicts (high watermark {report.high_watermark}) ==")
    if not report.verdicts:
        out.append("(no records before the high watermark)")
    for verdict in report.verdicts:
        record = verdict.record
        out.append(
            f"offset {record.offset:<4} byte {record.byte_pos:<5} "
            f"batch {verdict.batch_index:<2} {verdict.kind:<7} "
            f"{'VISIBLE' if verdict.deliverable else 'HIDDEN':<7} "
            f"key={_fmt_bytes(record.key)} value={_fmt_bytes(record.value)}"
        )
        for header in record.headers:
            out.append(
                f"        header {_fmt_bytes(header.key)} = "
                f"{_fmt_bytes(header.value)}"
            )
        out.append(f"        reason: {verdict.reason}")
    if report.unjudged_records:
        out.append("")
        out.append("== records at/after the high watermark (parsed, not judged) ==")
        for batch_index, record in report.unjudged_records:
            out.append(
                f"offset {record.offset:<4} byte {record.byte_pos:<5} "
                f"batch {batch_index}"
            )
    out.append("")
    out.extend(_transaction_lines(report))
    out.append("")
    out.append("== read_committed outcome ==")
    out.append(f"high watermark:     {report.high_watermark}")
    out.append(f"last stable offset: {report.last_stable_offset}")
    out.append(
        f"deliverable range:  offsets [0, {report.last_stable_offset})"
    )
    out.append(
        f"deliverable records ({len(report.deliverable_offsets)}): "
        + (" ".join(map(str, report.deliverable_offsets)) or "(none)")
    )
    hidden = [v.record.offset for v in report.verdicts if not v.deliverable]
    out.append(
        f"hidden records ({len(hidden)}): "
        + (" ".join(map(str, hidden)) or "(none)")
    )
    return "\n".join(out)


def _format_batch(batch: Batch) -> str:
    if batch.is_control:
        kind = f"CONTROL/{batch.control_marker}"
    elif batch.is_transactional:
        kind = "data/transactional"
    else:
        kind = "data/plain"
    producer = (
        f"producer={batch.producer_id} epoch={batch.producer_epoch}"
        if batch.producer_id >= 0
        else "producer=none"
    )
    return (
        f"#{batch.index:<2} bytes [{batch.byte_pos}, "
        f"{batch.byte_pos + batch.byte_size}) offsets "
        f"[{batch.base_offset}, {batch.last_offset}] {kind:<19} {producer}"
    )


def _transaction_lines(report: AuditReport) -> list[str]:
    lines = ["== transactions =="]
    if not report.transactions and report.open_transaction is None:
        lines.append("(none)")
    for txn in report.transactions:
        lines.append(
            f"producer {txn.producer_id} epoch {txn.producer_epoch}: first "
            f"offset {txn.first_offset}, {txn.outcome} at offset "
            f"{txn.closed_offset}"
        )
    if report.open_transaction is not None:
        txn = report.open_transaction
        lines.append(
            f"producer {txn.producer_id} epoch {txn.producer_epoch}: first "
            f"offset {txn.first_offset}, STILL OPEN at the high watermark "
            "(pins the last stable offset)"
        )
    return lines


def _fmt_bytes(blob: bytes | None, limit: int = 24) -> str:
    if blob is None:
        return "null"
    if not blob:
        return '""'
    try:
        text = blob.decode("utf-8")
        if all(ch.isprintable() for ch in text):
            if len(text) > limit:
                text = text[:limit] + "..."
            return f'"{text}"'
    except UnicodeDecodeError:
        pass
    shown = blob[:limit].hex()
    return f"0x{shown}" + ("..." if len(blob) > limit else "")


# ---------------------------------------------------------------------------
# JSON rendering


def _render_json(report: AuditReport) -> str:
    parsed = report.parsed
    doc = {
        "status": report.status,
        "inputBytes": parsed.input_size,
        "verifiedBytes": parsed.verified_end_pos,
        "verifiedEndOffset": parsed.verified_end_offset,
        "truncatedTail": (
            {"bytePos": parsed.truncated_pos, "reason": parsed.truncated_reason}
            if parsed.truncated
            else None
        ),
        "highWatermark": report.high_watermark,
        "lastStableOffset": report.last_stable_offset,
        "deliverableRange": (
            [0, report.last_stable_offset]
            if report.last_stable_offset is not None
            else None
        ),
        "deliverableOffsets": report.deliverable_offsets,
        "transactions": [_txn_json(t) for t in report.transactions],
        "openTransaction": (
            _txn_json(report.open_transaction)
            if report.open_transaction is not None
            else None
        ),
        "batches": [_batch_json(b) for b in parsed.batches],
        "records": [_verdict_json(v) for v in report.verdicts],
        "unjudgedRecords": [
            {"offset": r.offset, "bytePos": r.byte_pos, "batch": i}
            for i, r in report.unjudged_records
        ],
    }
    return json.dumps(doc, indent=2)


def _txn_json(txn) -> dict:
    return {
        "producerId": txn.producer_id,
        "epoch": txn.producer_epoch,
        "firstOffset": txn.first_offset,
        "outcome": txn.outcome,
        "closedOffset": txn.closed_offset,
    }


def _batch_json(batch: Batch) -> dict:
    return {
        "index": batch.index,
        "bytePos": batch.byte_pos,
        "byteSize": batch.byte_size,
        "baseOffset": batch.base_offset,
        "lastOffset": batch.last_offset,
        "producerId": batch.producer_id,
        "producerEpoch": batch.producer_epoch,
        "transactional": batch.is_transactional,
        "control": batch.is_control,
        "controlMarker": batch.control_marker,
        "recordCount": len(batch.records),
    }


def _verdict_json(verdict) -> dict:
    record = verdict.record
    return {
        "offset": record.offset,
        "bytePos": record.byte_pos,
        "batch": verdict.batch_index,
        "kind": verdict.kind,
        "key": _hex(record.key),
        "value": _hex(record.value),
        "headers": [
            {"key": h.key.hex(), "value": _hex(h.value)}
            for h in record.headers
        ],
        "deliverable": verdict.deliverable,
        "reason": verdict.reason,
    }


def _hex(blob: bytes | None) -> str | None:
    return None if blob is None else blob.hex()
