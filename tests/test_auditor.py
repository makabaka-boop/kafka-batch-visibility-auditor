"""Tests for the offline read_committed auditor.

All binary fixtures are produced by tests/fixtures.py, an encoder written
independently from the auditor's parser. No Kafka broker or client is
involved anywhere.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from kafka_audit import AuditError, audit
from kafka_audit.crc32c import crc32c
from kafka_audit.recordbatch import read_varint, read_varlong

from tests import fixtures as fx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# stream builders


def plain_batch(base, values, **kw):
    recs = [fx.record(i, b"k%d" % (base + i), v) for i, v in enumerate(values)]
    return fx.batch(base, recs, **kw)


def txn_batch(base, values, pid, epoch, seq):
    recs = [fx.record(i, b"t%d" % (base + i), v) for i, v in enumerate(values)]
    return fx.batch(
        base, recs,
        producer_id=pid, producer_epoch=epoch, base_sequence=seq,
        transactional=True,
    )


def marker_batch(base, kind, pid, epoch):
    return fx.batch(
        base, [fx.control_record(kind)],
        producer_id=pid, producer_epoch=epoch, control=True,
    )


def cross_batch_commit_stream():
    """A transaction spanning two data batches, committed in a third."""
    return b"".join([
        plain_batch(0, [b"A0", b"A1"]),          # offsets 0-1
        txn_batch(2, [b"T0", b"T1"], 100, 0, 0),  # offsets 2-3, txn opens
        txn_batch(4, [b"T2"], 100, 0, 2),         # offset 4, txn continues
        plain_batch(5, [b"C0"]),                  # offset 5
        marker_batch(6, fx.COMMIT, 100, 0),       # offset 6
        plain_batch(7, [b"D0"]),                  # offset 7
    ])


def abort_stream():
    return b"".join([
        txn_batch(0, [b"T0", b"T1"], 7, 0, 0),   # offsets 0-1
        plain_batch(2, [b"N0"]),                  # offset 2
        marker_batch(3, fx.ABORT, 7, 0),          # offset 3
        plain_batch(4, [b"N1"]),                  # offset 4
    ])


def open_txn_stream():
    """Open transaction with plain records interleaved after it."""
    return b"".join([
        plain_batch(0, [b"N0"]),                 # offset 0
        txn_batch(1, [b"T0", b"T1"], 9, 1, 0),   # offsets 1-2, txn opens
        plain_batch(3, [b"N1", b"N2"]),          # offsets 3-4, interleaved
        txn_batch(5, [b"T2"], 9, 1, 2),          # offset 5, txn continues
    ])


def verdicts_by_offset(report):
    return {v.record.offset: v for v in report.verdicts}


# ---------------------------------------------------------------------------
# primitives


class PrimitivesTest(unittest.TestCase):
    def test_crc32c_known_vector(self):
        self.assertEqual(crc32c(b"123456789"), 0xE3069283)
        self.assertEqual(fx.crc32c_bitwise(b"123456789"), 0xE3069283)

    def test_crc32c_implementations_agree(self):
        for payload in (b"", b"\x00", bytes(range(256)), os.urandom(1024)):
            self.assertEqual(crc32c(payload), fx.crc32c_bitwise(payload))

    def test_varint_round_trip(self):
        for n in (0, 1, -1, 63, -64, 64, 300, -300, 2**31 - 1, -(2**31)):
            enc = fx.varint(n)
            dec, pos = read_varint(enc, 0, len(enc))
            self.assertEqual((dec, pos), (n, len(enc)))

    def test_varlong_round_trip(self):
        for n in (0, 1, -1, 2**63 - 1, -(2**63), 123456789012345):
            enc = fx.varlong(n)
            dec, pos = read_varlong(enc, 0, len(enc))
            self.assertEqual((dec, pos), (n, len(enc)))


# ---------------------------------------------------------------------------
# transaction semantics


class CrossBatchCommitTest(unittest.TestCase):
    def test_commit_spanning_batches_is_delivered(self):
        report = audit(cross_batch_commit_stream(), 8)
        self.assertEqual(report.status, "ok")
        self.assertEqual(report.last_stable_offset, 8)
        self.assertEqual(report.deliverable_offsets, [0, 1, 2, 3, 4, 5, 7])
        self.assertEqual(len(report.transactions), 1)
        txn = report.transactions[0]
        self.assertEqual(
            (txn.producer_id, txn.producer_epoch, txn.first_offset, txn.outcome),
            (100, 0, 2, "committed"),
        )
        by_offset = verdicts_by_offset(report)
        self.assertFalse(by_offset[6].deliverable)
        self.assertIn("control", by_offset[6].reason)
        for off in (2, 3, 4):
            self.assertIn("committed transaction", by_offset[off].reason)

    def test_byte_positions_are_absolute(self):
        report = audit(cross_batch_commit_stream(), 8)
        parsed_positions = [v.record.byte_pos for v in report.verdicts]
        self.assertEqual(parsed_positions, sorted(parsed_positions))
        for batch in report.parsed.batches:
            for record in batch.records:
                self.assertGreaterEqual(record.byte_pos, batch.byte_pos)
                self.assertLess(
                    record.byte_pos, batch.byte_pos + batch.byte_size
                )


class AbortTest(unittest.TestCase):
    def test_aborted_transaction_is_fully_hidden(self):
        report = audit(abort_stream(), 5)
        self.assertEqual(report.last_stable_offset, 5)
        self.assertEqual(report.deliverable_offsets, [2, 4])
        by_offset = verdicts_by_offset(report)
        for off in (0, 1):
            self.assertFalse(by_offset[off].deliverable)
            self.assertIn("aborted transaction", by_offset[off].reason)
        self.assertIn("abort control marker", by_offset[3].reason)
        self.assertEqual(report.transactions[0].outcome, "aborted")


class OpenTransactionTest(unittest.TestCase):
    def test_open_transaction_pins_last_stable_offset(self):
        report = audit(open_txn_stream(), 6)
        self.assertEqual(report.last_stable_offset, 1)
        self.assertEqual(report.deliverable_offsets, [0])
        self.assertIsNotNone(report.open_transaction)
        by_offset = verdicts_by_offset(report)
        for off in (1, 2, 5):
            self.assertIn("still open", by_offset[off].reason)
        for off in (3, 4):
            # plain records after the open transaction may not pass the LSO
            self.assertFalse(by_offset[off].deliverable)
            self.assertIn("last stable offset", by_offset[off].reason)

    def test_commit_after_hw_leaves_transaction_undecided(self):
        # The commit marker sits beyond the high watermark: the transaction
        # is undecided in the judged region and pins the LSO.
        stream = open_txn_stream() + marker_batch(6, fx.COMMIT, 9, 1)
        report = audit(stream, 6)
        self.assertEqual(report.last_stable_offset, 1)
        self.assertEqual(report.deliverable_offsets, [0])
        self.assertEqual(
            [r.offset for _, r in report.unjudged_records], [6]
        )


class TransactionProtocolViolationTest(unittest.TestCase):
    def test_commit_without_open_transaction_rejected(self):
        stream = marker_batch(0, fx.COMMIT, 1, 0)
        with self.assertRaisesRegex(AuditError, "no corresponding open"):
            audit(stream, 1)

    def test_abort_without_open_transaction_rejected(self):
        stream = plain_batch(0, [b"N0"]) + marker_batch(1, fx.ABORT, 1, 0)
        with self.assertRaisesRegex(AuditError, "no corresponding open"):
            audit(stream, 2)

    def test_marker_with_wrong_epoch_rejected(self):
        stream = txn_batch(0, [b"T0"], 3, 0, 0) + marker_batch(1, fx.COMMIT, 3, 1)
        with self.assertRaisesRegex(AuditError, "rejected"):
            audit(stream, 2)

    def test_marker_with_wrong_producer_rejected(self):
        stream = txn_batch(0, [b"T0"], 3, 0, 0) + marker_batch(1, fx.COMMIT, 4, 0)
        with self.assertRaisesRegex(AuditError, "rejected"):
            audit(stream, 2)

    def test_second_concurrent_transaction_rejected(self):
        stream = txn_batch(0, [b"T0"], 3, 0, 0) + txn_batch(1, [b"X0"], 4, 0, 0)
        with self.assertRaisesRegex(AuditError, "at most one concurrent"):
            audit(stream, 2)

    def test_same_transaction_continues_across_batches(self):
        # same producer id + epoch: a continuation, not a second transaction
        stream = (
            txn_batch(0, [b"T0"], 3, 0, 0)
            + txn_batch(1, [b"T1"], 3, 0, 1)
            + marker_batch(2, fx.COMMIT, 3, 0)
        )
        report = audit(stream, 3)
        self.assertEqual(report.deliverable_offsets, [0, 1])


# ---------------------------------------------------------------------------
# corruption and truncation


class CorruptionTest(unittest.TestCase):
    def test_negative_batch_length_rejected(self):
        stream = fx.batch(0, [fx.record(0, b"k", b"v")], length_override=-4)
        with self.assertRaisesRegex(AuditError, "corrupt batch length"):
            audit(stream, 0)

    def test_undersized_batch_length_rejected(self):
        stream = fx.batch(0, [fx.record(0, b"k", b"v")], length_override=10)
        with self.assertRaisesRegex(AuditError, "corrupt batch length"):
            audit(stream, 0)

    def test_oversized_batch_length_marks_truncated_tail(self):
        good = plain_batch(0, [b"A0", b"A1"])
        stream = good + fx.batch(
            2, [fx.record(0, b"x", b"X")], length_override=5000
        )
        report = audit(stream, 2)
        self.assertTrue(report.parsed.truncated)
        self.assertIn("5000", report.parsed.truncated_reason)
        # verified region intact: delivery still allowed
        self.assertEqual(report.deliverable_offsets, [0, 1])

    def test_truncated_mid_batch_marks_tail(self):
        good = plain_batch(0, [b"A0"])
        second = plain_batch(1, [b"B0", b"B1"])
        stream = good + second[: len(second) - 3]
        report = audit(stream, 1)
        self.assertTrue(report.parsed.truncated)
        self.assertEqual(report.deliverable_offsets, [0])

    def test_crc_mismatch_halts_scan(self):
        b0 = plain_batch(0, [b"A0"])
        b1 = bytearray(plain_batch(1, [b"B0"]))
        b1[-1] ^= 0xFF  # corrupt payload: CRC no longer matches
        b2 = plain_batch(2, [b"C0"])
        # the auditor must halt, never skip batch 1 and continue with batch 2
        with self.assertRaisesRegex(AuditError, "CRC32C"):
            audit(b0 + bytes(b1) + b2, 3)

    def test_corrupt_record_length_rejected(self):
        # valid CRC over corrupt content: the record claims ~1MB
        bad_record = fx.varint(10**6) + b"\x00"
        stream = fx.batch(0, [bad_record], last_offset_delta=0)
        with self.assertRaisesRegex(AuditError, "overruns"):
            audit(stream, 1)

    def test_last_offset_delta_mismatch_rejected(self):
        stream = fx.batch(
            0, [fx.record(0, b"k", b"v")], last_offset_delta=3
        )
        with self.assertRaisesRegex(AuditError, "lastOffsetDelta"):
            audit(stream, 1)

    def test_bad_magic_rejected(self):
        stream = fx.batch(0, [fx.record(0, b"k", b"v")], magic_override=1)
        with self.assertRaisesRegex(AuditError, "magic"):
            audit(stream, 1)

    def test_compressed_batch_rejected(self):
        stream = fx.batch(0, [fx.record(0, b"k", b"v")], attributes_override=0x01)
        with self.assertRaisesRegex(AuditError, "compression"):
            audit(stream, 1)

    def test_non_contiguous_offsets_rejected(self):
        stream = plain_batch(0, [b"A0"]) + plain_batch(5, [b"B0"])
        with self.assertRaisesRegex(AuditError, "does not continue"):
            audit(stream, 6)

    def test_too_many_batches_rejected(self):
        stream = b"".join(plain_batch(i, [b"x"]) for i in range(33))
        with self.assertRaisesRegex(AuditError, "32"):
            audit(stream, 33)


# ---------------------------------------------------------------------------
# high watermark handling


class HighWatermarkTest(unittest.TestCase):
    def test_hw_not_on_boundary_rejected(self):
        stream = plain_batch(0, [b"A0", b"A1"])
        with self.assertRaisesRegex(AuditError, "batch boundary"):
            audit(stream, 1)

    def test_hw_beyond_verified_region_withholds_delivery(self):
        stream = plain_batch(0, [b"A0"]) + fx.batch(
            1, [fx.record(0, b"b", b"B")], length_override=5000
        )
        report = audit(stream, 5)
        self.assertEqual(report.status, "hw_beyond_verified")
        self.assertIsNone(report.deliverable_offsets)
        self.assertIsNone(report.last_stable_offset)

    def test_hw_beyond_intact_stream_withholds_delivery(self):
        report = audit(plain_batch(0, [b"A0"]), 7)
        self.assertEqual(report.status, "hw_beyond_verified")
        self.assertIsNone(report.deliverable_offsets)

    def test_records_beyond_hw_are_listed_not_judged(self):
        stream = plain_batch(0, [b"A0"]) + plain_batch(1, [b"B0"])
        report = audit(stream, 1)
        self.assertEqual(report.deliverable_offsets, [0])
        self.assertEqual([r.offset for _, r in report.unjudged_records], [1])

    def test_hw_zero_delivers_nothing(self):
        report = audit(plain_batch(0, [b"A0"]), 0)
        self.assertEqual(report.status, "ok")
        self.assertEqual(report.deliverable_offsets, [])


# ---------------------------------------------------------------------------
# record content


class RecordContentTest(unittest.TestCase):
    def test_headers_and_nulls_parsed(self):
        rec = fx.record(
            0, None, None, headers=((b"trace", b"abc"), (b"null-val", None))
        )
        report = audit(fx.batch(0, [rec]), 1)
        record = report.verdicts[0].record
        self.assertIsNone(record.key)
        self.assertIsNone(record.value)
        self.assertEqual(
            [(h.key, h.value) for h in record.headers],
            [(b"trace", b"abc"), (b"null-val", None)],
        )
        self.assertTrue(report.verdicts[0].deliverable)


# ---------------------------------------------------------------------------
# CLI


class CliTest(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "kafka_audit", *args],
            capture_output=True, text=True, cwd=ROOT,
        )

    def write_tmp(self, data):
        fd, path = tempfile.mkstemp(suffix=".bin")
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        self.addCleanup(os.unlink, path)
        return path

    def test_cli_ok(self):
        path = self.write_tmp(cross_batch_commit_stream())
        proc = self.run_cli(path, "--hw", "8")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("deliverable range:  offsets [0, 8)", proc.stdout)
        self.assertIn("0 1 2 3 4 5 7", proc.stdout)

    def test_cli_json(self):
        path = self.write_tmp(abort_stream())
        proc = self.run_cli(path, "--hw", "5", "--json")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        doc = json.loads(proc.stdout)
        self.assertEqual(doc["deliverableOffsets"], [2, 4])
        self.assertEqual(doc["lastStableOffset"], 5)

    def test_cli_no_delivery_exit_code(self):
        path = self.write_tmp(plain_batch(0, [b"A0"]))
        proc = self.run_cli(path, "--hw", "9")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("no delivery list", proc.stdout)

    def test_cli_audit_failure_exit_code(self):
        path = self.write_tmp(
            fx.batch(0, [fx.record(0, b"k", b"v")], length_override=-1)
        )
        proc = self.run_cli(path, "--hw", "0")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("AUDIT FAILED", proc.stderr)


if __name__ == "__main__":
    unittest.main()
