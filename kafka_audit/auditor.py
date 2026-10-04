"""read_committed semantics over parsed magic=2 batches.

Rules enforced here:

* At most one transaction may be open at a time. A transaction is opened by
  a batch with the transactional attribute and identified by its
  (producer id, producer epoch) pair; later transactional batches with the
  same pair continue it.
* A commit/abort control marker must match the currently open transaction's
  producer id and epoch. A marker with no corresponding open transaction,
  or with the wrong epoch, is rejected (fatal).
* Committed transaction records are deliverable; aborted ones are hidden;
  control markers are never business records.
* A transaction still open at the high watermark pins the last stable
  offset (LSO) to its first record's offset: nothing at or past the LSO is
  deliverable, including non-transactional records interleaved afterwards.
"""

from __future__ import annotations

from dataclasses import dataclass

from .recordbatch import AuditError, Batch, ParseResult, Record, parse_stream


@dataclass
class Transaction:
    producer_id: int
    producer_epoch: int
    first_offset: int
    first_byte_pos: int
    outcome: str = "open"  # "open" | "committed" | "aborted"
    closed_offset: int | None = None


@dataclass
class RecordVerdict:
    record: Record
    batch_index: int
    kind: str  # "data" | "control"
    deliverable: bool
    reason: str


@dataclass
class AuditReport:
    high_watermark: int
    status: str  # "ok" | "hw_beyond_verified"
    parsed: ParseResult
    considered_batches: list[Batch]
    unjudged_records: list[tuple[int, Record]]  # (batch index, record)
    transactions: list[Transaction]
    open_transaction: Transaction | None
    last_stable_offset: int | None
    verdicts: list[RecordVerdict]
    deliverable_offsets: list[int] | None


def audit(data: bytes, high_watermark: int) -> AuditReport:
    """Audit a magic=2 uncompressed batch stream under read_committed rules.

    Raises AuditError on any structural, checksum or transaction-protocol
    violation; the auditor never skips a corrupt region to keep scanning.
    """
    if high_watermark < 0:
        raise AuditError("high watermark must be >= 0")
    parsed = parse_stream(data)

    if high_watermark > parsed.verified_end_offset:
        # The watermark points past everything that could be verified:
        # judging anything would certify bytes never seen.
        return AuditReport(
            high_watermark=high_watermark,
            status="hw_beyond_verified",
            parsed=parsed,
            considered_batches=[],
            unjudged_records=_all_records(parsed.batches),
            transactions=[],
            open_transaction=None,
            last_stable_offset=None,
            verdicts=[],
            deliverable_offsets=None,
        )

    boundaries = {0}
    for batch in parsed.batches:
        boundaries.add(batch.end_offset)
    if high_watermark not in boundaries:
        raise AuditError(
            f"high watermark {high_watermark} does not fall on a batch "
            f"boundary; known boundaries: {sorted(boundaries)}"
        )

    considered = [b for b in parsed.batches if b.end_offset <= high_watermark]
    beyond = [b for b in parsed.batches if b.base_offset >= high_watermark]

    transactions, open_txn, record_txn = _track_transactions(considered)
    lso = open_txn.first_offset if open_txn is not None else high_watermark
    verdicts = _judge(considered, record_txn, open_txn, lso)
    deliverable = [v.record.offset for v in verdicts if v.deliverable]

    return AuditReport(
        high_watermark=high_watermark,
        status="ok",
        parsed=parsed,
        considered_batches=considered,
        unjudged_records=_all_records(beyond),
        transactions=transactions,
        open_transaction=open_txn,
        last_stable_offset=lso,
        verdicts=verdicts,
        deliverable_offsets=deliverable,
    )


def _all_records(batches: list[Batch]) -> list[tuple[int, Record]]:
    return [(batch.index, record) for batch in batches for record in batch.records]


def _track_transactions(
    batches: list[Batch],
) -> tuple[list[Transaction], Transaction | None, dict[int, Transaction]]:
    transactions: list[Transaction] = []
    open_txn: Transaction | None = None
    record_txn: dict[int, Transaction] = {}
    for batch in batches:
        if batch.is_control:
            marker = batch.control_marker
            if open_txn is None:
                raise AuditError(
                    f"{marker} marker at offset {batch.base_offset} has no "
                    "corresponding open transaction; marker rejected",
                    batch.byte_pos,
                )
            if (batch.producer_id, batch.producer_epoch) != (
                open_txn.producer_id,
                open_txn.producer_epoch,
            ):
                raise AuditError(
                    f"{marker} marker names producer id {batch.producer_id} "
                    f"epoch {batch.producer_epoch} but the open transaction "
                    f"belongs to producer id {open_txn.producer_id} epoch "
                    f"{open_txn.producer_epoch}; marker rejected",
                    batch.byte_pos,
                )
            open_txn.outcome = "committed" if marker == "commit" else "aborted"
            open_txn.closed_offset = batch.base_offset
            transactions.append(open_txn)
            open_txn = None
        elif batch.is_transactional:
            if batch.producer_id < 0 or batch.producer_epoch < 0:
                raise AuditError(
                    "transactional batch without a valid producer id/epoch",
                    batch.byte_pos,
                )
            if open_txn is None:
                open_txn = Transaction(
                    producer_id=batch.producer_id,
                    producer_epoch=batch.producer_epoch,
                    first_offset=batch.base_offset,
                    first_byte_pos=batch.byte_pos,
                )
            elif (open_txn.producer_id, open_txn.producer_epoch) != (
                batch.producer_id,
                batch.producer_epoch,
            ):
                raise AuditError(
                    f"transactional batch of producer id {batch.producer_id} "
                    f"epoch {batch.producer_epoch} starts while producer id "
                    f"{open_txn.producer_id} epoch {open_txn.producer_epoch} "
                    "still has an open transaction; at most one concurrent "
                    "transaction is allowed",
                    batch.byte_pos,
                )
            for record in batch.records:
                record_txn[record.offset] = open_txn
    return transactions, open_txn, record_txn


def _judge(
    batches: list[Batch],
    record_txn: dict[int, Transaction],
    open_txn: Transaction | None,
    lso: int,
) -> list[RecordVerdict]:
    verdicts: list[RecordVerdict] = []
    for batch in batches:
        for record in batch.records:
            if batch.is_control:
                verdicts.append(
                    RecordVerdict(
                        record,
                        batch.index,
                        "control",
                        False,
                        f"{batch.control_marker} control marker: transaction "
                        "control records are never delivered as business "
                        "records",
                    )
                )
                continue
            txn = record_txn.get(record.offset)
            if txn is not None:
                who = f"producer {txn.producer_id} epoch {txn.producer_epoch}"
                if txn.outcome == "committed":
                    verdicts.append(
                        RecordVerdict(
                            record, batch.index, "data", True,
                            f"record of committed transaction ({who})",
                        )
                    )
                elif txn.outcome == "aborted":
                    verdicts.append(
                        RecordVerdict(
                            record, batch.index, "data", False,
                            f"record of aborted transaction ({who}): hidden",
                        )
                    )
                else:
                    verdicts.append(
                        RecordVerdict(
                            record, batch.index, "data", False,
                            f"record of transaction ({who}) still open at "
                            "the high watermark: hidden",
                        )
                    )
                continue
            if record.offset < lso:
                verdicts.append(
                    RecordVerdict(
                        record, batch.index, "data", True,
                        "non-transactional record below the last stable "
                        "offset",
                    )
                )
            else:
                verdicts.append(
                    RecordVerdict(
                        record, batch.index, "data", False,
                        f"non-transactional record at/past the last stable "
                        f"offset {lso}: an open transaction (producer "
                        f"{open_txn.producer_id} epoch "
                        f"{open_txn.producer_epoch}) started at offset "
                        f"{open_txn.first_offset} and holds the line",
                    )
                )
    return verdicts
