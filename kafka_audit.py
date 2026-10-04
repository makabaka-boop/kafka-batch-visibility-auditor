#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kafka_audit.py — Kafka magic=2 RecordBatch 离线审计器 (read_committed 可见性判定)

不依赖任何 Kafka 客户端，手工解析字节流:
  * RecordBatch v2 头部: baseOffset / batchLength / partitionLeaderEpoch / magic /
    CRC32C / attributes / lastOffsetDelta / 时间戳 / producerId / producerEpoch /
    baseSequence / recordsCount
  * CRC32C (Castagnoli) 校验 —— 中段失败即停，禁止跳过继续扫描
  * Record: 有符号 (zigzag) varint 长度、timestampDelta、offsetDelta、key/value、headers
  * 控制批次 (COMMIT / ABORT 标记)，标记不暴露为业务记录

判定模型 (read_committed):
  * 至多一个同时进行的事务；其他生产者的普通记录可穿插
  * 以 (producerId, producerEpoch) 关联连续事务；epoch 错误或无对应事务的标记 -> 拒绝
  * 中止事务的记录全部隐藏；未决事务从首条记录起压住 LSO，
    其后的普通记录同样不得越过该界限提前交付
  * 仅 high watermark 之前的完整批次参与判断；HW 超出已验证区域 -> 不提供交付列表
  * 尾部不完整明确标记

退出码: 0 = 已给出交付列表; 2 = 拒绝交付; 1 = 用法 / IO 错误
"""
from __future__ import annotations

import argparse
import json
import struct
import sys
from dataclasses import dataclass, field

BATCH_HEADER_LEN = 61                 # baseOffset .. recordsCount
BATCH_LENGTH_MIN = BATCH_HEADER_LEN - 12   # 49: 空记录区时的最小 batchLength
MAX_BATCHES = 32                      # 输入前提: 最多 32 个批次

ATTR_COMPRESSION_MASK = 0x0007
ATTR_TRANSACTIONAL = 0x0010
ATTR_CONTROL = 0x0020

CONTROL_TYPE_NAMES = {0: "abort", 1: "commit"}

STATUS_CN = {
    "deliverable": "可交付",
    "hidden": "隐藏",
    "unevaluated": "未判定",
    "error": "错误",
}


# ------------------------------------------------------------------ CRC32C
def _build_crc32c_table():
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
        table.append(crc)
    return table


_CRC32C_TABLE = _build_crc32c_table()


def crc32c(data, value=0):
    """CRC-32C (Castagnoli)，表驱动实现。"""
    crc = (value ^ 0xFFFFFFFF) & 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return (crc ^ 0xFFFFFFFF) & 0xFFFFFFFF


class BatchFormatError(Exception):
    """批次格式损坏：扫描必须立即停止，禁止跳过。"""


# ------------------------------------------------------------------ 数据模型
@dataclass
class Record:
    index_in_batch: int
    offset: int                 # 绝对 offset = baseOffset + offsetDelta
    byte_pos: int               # 记录在字节流中的起始位置 (length varint 处)
    byte_len: int
    timestamp_delta: int
    offset_delta: int
    key: bytes | None
    value: bytes | None
    headers: list               # [(key_str, value_bytes|None)]
    txn: dict | None = None     # 分析阶段关联的事务
    kind: str = "data"          # data | control
    status: str | None = None   # deliverable | hidden | unevaluated | error
    reason: str = ""


@dataclass
class Batch:
    index: int
    byte_start: int
    byte_end: int               # 不含
    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    crc_stored: int
    crc_computed: int
    attributes: int
    last_offset_delta: int
    first_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    records_count: int
    transactional: bool
    control: bool
    control_type: str | None    # "commit" | "abort" | None
    records: list = field(default_factory=list)

    @property
    def last_offset(self):
        return self.base_offset + self.last_offset_delta


# ------------------------------------------------------------------ varint
def _read_uvarint(data, pos, end):
    result = 0
    shift = 0
    while True:
        if pos >= end:
            raise BatchFormatError(f"varint 截断于字节 {pos}")
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7
        if shift >= 64:
            raise BatchFormatError(f"varint 过长（字节 {pos} 附近）")


def _read_svarint(data, pos, end):
    """zigzag 有符号 varint。"""
    raw, pos = _read_uvarint(data, pos, end)
    return (raw >> 1) ^ -(raw & 1), pos


def _read_bytes(data, pos, end):
    n, pos = _read_svarint(data, pos, end)
    if n == -1:
        return None, pos
    if n < -1:
        raise BatchFormatError(f"字节字段长度非法: {n}")
    if pos + n > end:
        raise BatchFormatError("字节字段超出记录/批次边界")
    return data[pos:pos + n], pos + n


# ------------------------------------------------------------------ 解析
def _parse_record(data, pos, batch_end, base_offset, index):
    rec_start = pos
    length, pos = _read_svarint(data, pos, batch_end)
    if length < 0:
        raise BatchFormatError(f"记录长度为负: {length}")
    body_end = pos + length
    if body_end > batch_end:
        raise BatchFormatError(f"记录声明长度 {length} 超出批次剩余空间")
    if pos >= body_end:
        raise BatchFormatError("记录体为空")
    # attributes (int8, 保留位) —— 读取但不参与判定
    pos += 1
    timestamp_delta, pos = _read_svarint(data, pos, body_end)
    offset_delta, pos = _read_svarint(data, pos, body_end)
    key, pos = _read_bytes(data, pos, body_end)
    value, pos = _read_bytes(data, pos, body_end)
    header_count, pos = _read_svarint(data, pos, body_end)
    if header_count < 0:
        raise BatchFormatError(f"headers 数量非法: {header_count}")
    headers = []
    for _ in range(header_count):
        hkey, pos = _read_bytes(data, pos, body_end)
        if hkey is None:
            raise BatchFormatError("header key 不得为 null")
        hval, pos = _read_bytes(data, pos, body_end)
        headers.append((hkey.decode("utf-8", "replace"), hval))
    if pos != body_end:
        raise BatchFormatError(
            f"记录体未精确消费: 声明 {length} 字节，剩余 {body_end - pos} 字节")
    rec = Record(
        index_in_batch=index,
        offset=base_offset + offset_delta,
        byte_pos=rec_start,
        byte_len=body_end - rec_start,
        timestamp_delta=timestamp_delta,
        offset_delta=offset_delta,
        key=key, value=value, headers=headers,
    )
    return rec, body_end


def _parse_batch(data, start, index, base_offset, batch_length):
    end = start + 12 + batch_length
    partition_leader_epoch, = struct.unpack_from(">i", data, start + 12)
    magic = data[start + 16]
    if magic != 2:
        raise BatchFormatError(f"magic={magic}，本审计器仅支持 magic=2")
    crc_stored, = struct.unpack_from(">I", data, start + 17)
    crc_computed = crc32c(data[start + 21:end])
    if crc_computed != crc_stored:
        raise BatchFormatError(
            f"CRC32C 校验失败: 存储 0x{crc_stored:08X} != 计算 0x{crc_computed:08X}")
    (attributes, last_offset_delta, first_ts, max_ts,
     producer_id, producer_epoch, base_sequence,
     records_count) = struct.unpack_from(">hiqqqhii", data, start + 21)
    compression = attributes & ATTR_COMPRESSION_MASK
    if compression != 0:
        raise BatchFormatError(f"压缩批次 (compression={compression})，与“未压缩”前提不符")
    transactional = bool(attributes & ATTR_TRANSACTIONAL)
    control = bool(attributes & ATTR_CONTROL)
    if control and not transactional:
        raise BatchFormatError("控制批次未置 transactional 标志，格式非法")
    if transactional and producer_id < 0:
        raise BatchFormatError("事务批次缺少有效 producerId")
    if records_count < 1:
        raise BatchFormatError(f"recordsCount={records_count} 非法")
    if control and records_count != 1:
        raise BatchFormatError("控制批次必须且只能含 1 条记录")
    records = []
    pos = start + BATCH_HEADER_LEN
    for i in range(records_count):
        rec, pos = _parse_record(data, pos, end, base_offset, i)
        records.append(rec)
    if pos != end:
        raise BatchFormatError(f"记录区未精确填满批次: 剩余 {end - pos} 字节")
    if records[-1].offset_delta != last_offset_delta:
        raise BatchFormatError(
            f"lastOffsetDelta={last_offset_delta} 与末条记录 "
            f"offsetDelta={records[-1].offset_delta} 不一致")
    for i, rec in enumerate(records):
        if rec.offset_delta != i:
            raise BatchFormatError(
                f"记录 offsetDelta={rec.offset_delta} 不连续（期望 {i}；前提：无压实）")
    control_type = None
    if control:
        key = records[0].key
        if key is None or len(key) != 4:
            raise BatchFormatError("控制记录 key 必须为 4 字节 (version + type)")
        _version, ctype = struct.unpack(">hh", key)
        if ctype not in CONTROL_TYPE_NAMES:
            raise BatchFormatError(f"未知控制标记类型: {ctype}")
        control_type = CONTROL_TYPE_NAMES[ctype]
        records[0].kind = "control"
    return Batch(
        index=index, byte_start=start, byte_end=end,
        base_offset=base_offset, batch_length=batch_length,
        partition_leader_epoch=partition_leader_epoch,
        crc_stored=crc_stored, crc_computed=crc_computed,
        attributes=attributes, last_offset_delta=last_offset_delta,
        first_timestamp=first_ts, max_timestamp=max_ts,
        producer_id=producer_id, producer_epoch=producer_epoch,
        base_sequence=base_sequence, records_count=records_count,
        transactional=transactional, control=control,
        control_type=control_type, records=records,
    )


def parse_batches(data):
    """顺序解析并校验全部批次。返回 (batches, stop)。

    stop: {"reason": "eof"|"incomplete-tail"|"corrupt", "byte_pos", "detail"}
    任何损坏都导致立即停止 —— 禁止跳过失败批次继续扫描。
    """
    batches = []
    pos = 0
    expected_base = 0
    stop = {"reason": "eof", "byte_pos": len(data), "detail": "正常结束 (EOF)"}
    while pos < len(data):
        remaining = len(data) - pos
        if remaining < 12:
            stop = {"reason": "incomplete-tail", "byte_pos": pos,
                    "detail": f"尾部仅剩 {remaining} 字节，不足批次头前缀 (12 字节)"}
            break
        base_offset, batch_length = struct.unpack_from(">qi", data, pos)
        if batch_length < BATCH_LENGTH_MIN:
            stop = {"reason": "corrupt", "byte_pos": pos,
                    "detail": f"batchLength={batch_length} 小于最小合法值 "
                              f"{BATCH_LENGTH_MIN}（长度字段损坏，停止扫描，不跳过）"}
            break
        if pos + 12 + batch_length > len(data):
            stop = {"reason": "incomplete-tail", "byte_pos": pos,
                    "detail": f"batchLength={batch_length} 需要 {12 + batch_length} 字节，"
                              f"剩余仅 {remaining}（尾部不完整）"}
            break
        try:
            batch = _parse_batch(data, pos, len(batches), base_offset, batch_length)
        except BatchFormatError as exc:
            stop = {"reason": "corrupt", "byte_pos": pos,
                    "detail": f"{exc}（停止扫描，不跳过）"}
            break
        if batch.base_offset != expected_base:
            stop = {"reason": "corrupt", "byte_pos": pos,
                    "detail": f"baseOffset={batch.base_offset} 与期望连续 offset "
                              f"{expected_base} 不符（前提：无压实/无缺失；停止扫描）"}
            break
        batches.append(batch)
        pos = batch.byte_end
        expected_base = batch.last_offset + 1
    return batches, stop


# ------------------------------------------------------------------ 分析
def _mark(batch, status, reason):
    for rec in batch.records:
        rec.status = status
        rec.reason = reason


def _run_transactions(batches, hw, transactions, errors):
    """在 HW 之前的已验证批次上关联事务；返回 (lso, effective_limit)。

    协议违规（epoch 错误 / 无对应事务的标记 / 并发事务）记入 errors —— 拒绝交付。
    """
    open_txn = None
    epoch_by_pid = {}
    for batch in batches:
        if batch.base_offset >= hw:
            continue                                # HW 之外的批次不参与判断
        if batch.control:
            rec = batch.records[0]
            marker = batch.control_type.upper()
            if open_txn is None:
                errors.append(
                    f"offset {batch.base_offset}: {marker} 标记无对应开放事务，拒绝")
                rec.status = "hidden"
                rec.reason = f"{marker} 控制标记（无对应事务，协议错误）"
                continue
            if (batch.producer_id, batch.producer_epoch) != \
               (open_txn["pid"], open_txn["epoch"]):
                errors.append(
                    f"offset {batch.base_offset}: {marker} 标记 "
                    f"pid={batch.producer_id} epoch={batch.producer_epoch} 与开放事务 "
                    f"pid={open_txn['pid']} epoch={open_txn['epoch']} 不匹配"
                    f"（epoch 错误），拒绝")
                rec.status = "hidden"
                rec.reason = f"{marker} 控制标记（pid/epoch 不匹配，协议错误）"
                continue
            open_txn["outcome"] = "committed" if batch.control_type == "commit" else "aborted"
            open_txn["marker_offset"] = batch.base_offset
            rec.status = "hidden"
            rec.reason = f"{marker} 控制标记，不属业务记录"
            open_txn = None
        elif batch.transactional:
            pid, epoch = batch.producer_id, batch.producer_epoch
            known = epoch_by_pid.get(pid)
            if known is not None and known != epoch:
                errors.append(
                    f"offset {batch.base_offset}: pid={pid} 出现 epoch={epoch}，"
                    f"与已建立的 epoch={known} 冲突（epoch 错误），拒绝")
                _mark(batch, "error", "epoch 冲突（协议错误）")
                continue
            epoch_by_pid[pid] = epoch
            if open_txn is not None and \
               (open_txn["pid"], open_txn["epoch"]) != (pid, epoch):
                errors.append(
                    f"offset {batch.base_offset}: 事务 pid={pid} epoch={epoch} 与未结束的 "
                    f"pid={open_txn['pid']} epoch={open_txn['epoch']} 并发，"
                    f"违反单事务前提，拒绝")
                _mark(batch, "error", "并发事务（协议错误）")
                continue
            if open_txn is None:
                open_txn = {"pid": pid, "epoch": epoch,
                            "first_offset": batch.base_offset,
                            "record_offsets": [], "outcome": "open",
                            "marker_offset": None}
                transactions.append(open_txn)
            for rec in batch.records:
                rec.txn = open_txn
                open_txn["record_offsets"].append(rec.offset)
    lso = open_txn["first_offset"] if open_txn is not None else None
    effective = min(lso, hw) if lso is not None else hw
    return lso, effective


def _assign_statuses(batches, hw, effective_limit):
    for batch in batches:
        for rec in batch.records:
            if rec.status is not None:
                continue
            if batch.base_offset >= hw:
                rec.status = "unevaluated"
                rec.reason = f"位于 high watermark {hw} 之外，不参与判断"
            elif rec.txn is not None:
                t = rec.txn
                tag = f"事务 pid={t['pid']} epoch={t['epoch']}"
                if t["outcome"] == "committed":
                    if rec.offset < effective_limit:
                        rec.status = "deliverable"
                        rec.reason = f"{tag} 已提交"
                    else:
                        rec.status = "hidden"
                        rec.reason = f"{tag} 已提交，但越过 LSO={effective_limit}"
                elif t["outcome"] == "aborted":
                    rec.status = "hidden"
                    rec.reason = f"{tag} 已中止，全部隐藏"
                else:
                    rec.status = "hidden"
                    rec.reason = (f"{tag} 未决（无提交/中止标记，"
                                  f"LSO 压住在 {t['first_offset']}）")
            elif rec.offset < effective_limit:
                rec.status = "deliverable"
                rec.reason = "普通记录，位于上界之内"
            else:
                rec.status = "hidden"
                rec.reason = (f"普通记录越过 LSO={effective_limit}"
                              f"（被未决事务压住），不得提前交付")


def audit(data, hw, path="<memory>"):
    batches, stop = parse_batches(data)
    errors, warnings = [], []

    if batches:
        verified_end_offset = batches[-1].last_offset + 1
        verified_end_byte = batches[-1].byte_end
    else:
        verified_end_offset = 0
        verified_end_byte = 0

    if len(batches) > MAX_BATCHES:
        warnings.append(f"批次数量 {len(batches)} 超出输入前提 (<= {MAX_BATCHES})")
    if stop["reason"] == "incomplete-tail":
        warnings.append(f"尾部不完整（已明确标记）: 字节 {stop['byte_pos']} 起: {stop['detail']}")
    elif stop["reason"] == "corrupt":
        warnings.append(f"校验失败，扫描停止（未跳过、未继续）: "
                        f"字节 {stop['byte_pos']} 起: {stop['detail']}")

    boundaries = {0, verified_end_offset} | {b.base_offset for b in batches}
    hw_ok = True
    if hw > verified_end_offset:
        errors.append(f"high watermark {hw} 超过已验证区域末端 "
                      f"{verified_end_offset}，不提供交付列表")
        hw_ok = False
    elif hw not in boundaries:
        errors.append(f"high watermark {hw} 未落在批次边界上"
                      f"（已知边界: {sorted(boundaries)}），不提供交付列表")
        hw_ok = False

    transactions = []
    lso = None
    effective_limit = None
    deliverable = None

    if hw_ok:
        lso, effective_limit = _run_transactions(batches, hw, transactions, errors)
        _assign_statuses(batches, hw, effective_limit)
        if not errors:
            offsets = [rec.offset for b in batches if b.base_offset < hw
                       for rec in b.records if rec.status == "deliverable"]
            hidden_in_range = [
                {"offset": rec.offset, "reason": rec.reason}
                for b in batches if b.base_offset < hw
                for rec in b.records
                if rec.offset < effective_limit and rec.status != "deliverable"
            ]
            deliverable = {"range_start": 0, "range_end": effective_limit,
                           "offsets": offsets, "hidden_in_range": hidden_in_range}
    else:
        for b in batches:
            for rec in b.records:
                rec.status = "unevaluated"
                rec.reason = "high watermark 无效，未判定"

    return {
        "file": path,
        "size": len(data),
        "high_watermark": hw,
        "batch_count": len(batches),
        "batches": [_batch_to_json(b) for b in batches],
        "scan_stop": stop,
        "verified_region": {"end_offset": verified_end_offset,
                            "end_byte": verified_end_byte},
        "transactions": [_txn_to_json(t) for t in transactions],
        "lso": lso,
        "effective_limit": effective_limit,
        "records": [_rec_to_json(b, rec) for b in batches for rec in b.records],
        "deliverable": deliverable,
        "warnings": warnings,
        "errors": errors,
    }


# ------------------------------------------------------------------ 输出
def _batch_to_json(b):
    return {
        "index": b.index,
        "byte_start": b.byte_start,
        "byte_end": b.byte_end,
        "base_offset": b.base_offset,
        "last_offset": b.last_offset,
        "records_count": b.records_count,
        "producer_id": b.producer_id,
        "producer_epoch": b.producer_epoch,
        "base_sequence": b.base_sequence,
        "partition_leader_epoch": b.partition_leader_epoch,
        "transactional": b.transactional,
        "control": b.control,
        "control_type": b.control_type,
        "crc_stored": b.crc_stored,
        "crc_computed": b.crc_computed,
        "crc_ok": b.crc_stored == b.crc_computed,
    }


def _txn_to_json(t):
    return {
        "producer_id": t["pid"],
        "producer_epoch": t["epoch"],
        "first_offset": t["first_offset"],
        "record_offsets": t["record_offsets"],
        "outcome": t["outcome"],
        "marker_offset": t["marker_offset"],
    }


def _rec_to_json(batch, rec):
    return {
        "offset": rec.offset,
        "byte_pos": rec.byte_pos,
        "byte_len": rec.byte_len,
        "batch_index": batch.index,
        "kind": rec.kind,
        "key": rec.key.hex() if rec.key is not None else None,
        "value": rec.value.hex() if rec.value is not None else None,
        "headers": [{"key": k, "value": v.hex() if v is not None else None}
                    for k, v in rec.headers],
        "txn": {"producer_id": rec.txn["pid"],
                "producer_epoch": rec.txn["epoch"]} if rec.txn else None,
        "status": rec.status,
        "reason": rec.reason,
    }


def render_text(result):
    L = []
    add = L.append
    add("=" * 74)
    add("Kafka RecordBatch 离线审计 (magic=2 / read_committed / 手工解析，无客户端)")
    add(f"文件: {result['file']}  ({result['size']} 字节)")
    add(f"high watermark: {result['high_watermark']}")
    add("-" * 74)
    add("[1] 批次扫描 (顺序校验；损坏即停，禁止跳过)")
    if not result["batches"]:
        add("  (无完整批次)")
    for b in result["batches"]:
        if b["control"]:
            kind = f"控制({b['control_type'].upper()})"
        elif b["transactional"]:
            kind = "事务"
        else:
            kind = "普通"
        pid = f"pid={b['producer_id']}"
        if b["producer_id"] >= 0:
            pid += f" epoch={b['producer_epoch']}"
        add(f"  批次 #{b['index']:<2} 字节[{b['byte_start']:>5}, {b['byte_end']:>5}) "
            f"offset {b['base_offset']:>3}..{b['last_offset']:>3} "
            f"记录 {b['records_count']:>2}  {kind:<14} {pid:<20} CRC32C OK")
    stop = result["scan_stop"]
    add(f"  扫描停止: {stop['detail']}  [reason={stop['reason']}, 字节 {stop['byte_pos']}]")
    vr = result["verified_region"]
    add(f"  已验证区域: offset [0, {vr['end_offset']}), 字节 [0, {vr['end_byte']})")

    add("[2] 事务关联 (按 producer id / epoch)")
    if not result["transactions"]:
        add("  (无事务)")
    for t in result["transactions"]:
        outcome = {
            "committed": f"已提交 (标记 offset={t['marker_offset']})",
            "aborted": f"已中止 (标记 offset={t['marker_offset']})",
            "open": "未决 (无标记)",
        }[t["outcome"]]
        add(f"  pid={t['producer_id']} epoch={t['producer_epoch']}  "
            f"首条 offset={t['first_offset']}  记录 {t['record_offsets']}  -> {outcome}")

    add("[3] 记录判定")
    add(f"  {'offset':>6}  {'字节位置':>8}  {'类别':<6}  {'判定':<8}  原因")
    for r in result["records"]:
        kind = "控制" if r["kind"] == "control" else "数据"
        add(f"  {r['offset']:>6}  {r['byte_pos']:>8}  {kind:<6}  "
            f"{STATUS_CN[r['status']]:<8}  {r['reason']}")

    add("[4] 结论")
    if result["effective_limit"] is None:
        add("  LSO / 生效上界: 未判定 (high watermark 无效)")
    else:
        if result["lso"] is None:
            add(f"  LSO: 无未决事务 (视为 HW={result['high_watermark']})")
        else:
            add(f"  LSO: {result['lso']} (未决事务首条记录)")
        add(f"  生效上界 min(LSO, HW) = {result['effective_limit']}")
    d = result["deliverable"]
    if d is None:
        add("  可交付范围: 不予提供 (见错误)")
    else:
        add(f"  可交付范围: offset [{d['range_start']}, {d['range_end']})")
        offs = " ".join(map(str, d["offsets"])) or "(空)"
        add(f"  可交付业务记录 {len(d['offsets'])} 条: {offs}")
        if d["hidden_in_range"]:
            hid = "; ".join(f"offset {h['offset']} ({h['reason']})"
                            for h in d["hidden_in_range"])
            add(f"  范围内隐藏: {hid}")
        else:
            add("  范围内隐藏: 无")

    if result["warnings"]:
        add("[警告]")
        for w in result["warnings"]:
            add(f"  - {w}")
    if result["errors"]:
        add("[错误]")
        for e in result["errors"]:
            add(f"  - {e}")
    add("=" * 74)
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="kafka_audit",
        description="Kafka magic=2 RecordBatch 离线审计器 (read_committed)")
    ap.add_argument("file", help="RecordBatch 字节流文件 (从 offset 0 起)")
    ap.add_argument("--hw", type=int, required=True,
                    help="high watermark (必须落在批次边界上)")
    ap.add_argument("--json", action="store_true", help="以 JSON 输出审计结果")
    args = ap.parse_args(argv)
    if args.hw < 0:
        ap.error("--hw 必须 >= 0")
    try:
        with open(args.file, "rb") as fh:
            data = fh.read()
    except OSError as exc:
        print(f"无法读取文件 {args.file}: {exc}", file=sys.stderr)
        return 1
    result = audit(data, args.hw, args.file)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(render_text(result))
    return 0 if result["deliverable"] is not None else 2


if __name__ == "__main__":
    sys.exit(main())
