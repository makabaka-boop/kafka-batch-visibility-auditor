# Kafka RecordBatch 离线审计器 (read_committed)

离线命令行工具：读取一个分区从 offset 0 起的 magic=2 未压缩 RecordBatch 字节流，
**不使用 Kafka 客户端**，手工解析并判定哪些业务记录可按 `read_committed` 交付。

## 用法

```bash
python3 kafka_audit.py <文件> --hw <high watermark> [--json]
```

- `--hw`：high watermark，必须落在批次边界上；只有它之前的完整批次参与判断
- `--json`：输出结构化结果（批次 / 记录 / 事务 / LSO / 可交付范围 / 警告 / 错误）

退出码：`0` = 已给出交付列表；`2` = 拒绝交付（损坏影响判断 / 事务协议违规 / HW 无效）；`1` = 用法或 IO 错误。

## 输入前提

- 从 offset 0 起、未压缩、未日志压实、offset 连续、至多 32 个批次
- 至多一个同时进行的事务；其他生产者的普通记录可穿插

## 手工解析内容

- 批次头：`baseOffset / batchLength / partitionLeaderEpoch / magic / CRC32C /
  attributes / lastOffsetDelta / 时间戳 / producerId / producerEpoch /
  baseSequence / recordsCount`
- CRC32C (Castagnoli) 逐批校验 —— 中段失败立即停止，**禁止跳过继续扫描**
- 记录：有符号 (zigzag) varint 长度、`timestampDelta`、`offsetDelta`、key/value、headers
- 控制批次：COMMIT / ABORT 标记（key = version + type），不暴露为业务记录

## 判定规则 (read_committed)

- 以 `(producerId, producerEpoch)` 关联连续事务；同 pid 的 epoch 冲突、
  标记与开放事务不匹配、无对应事务的标记、并发事务 → **拒绝**（退出码 2）
- 中止事务的记录全部隐藏；已提交事务的记录可见
- 未决事务从首条记录起压住 LSO，其后的普通记录同样不得越过该界限
- 尾部不完整明确标记；HW 超过已验证区域 → 不提供交付列表
- 每条记录输出：原始 offset、字节位置、可见/隐藏原因；结论给出 LSO、
  生效上界 `min(LSO, HW)`、可交付范围与可交付 offset 列表

## 文件

| 文件 | 说明 |
|---|---|
| `kafka_audit.py` | 审计器（解析 + 判定 + 报告，表驱动 CRC32C） |
| `fixture_encoder.py` | 独立编码器（逐位 CRC32C，与审计器零共享代码） |
| `make_fixtures.py` | 生成 `fixtures/*.bin` 二进制夹具 |
| `test_auditor.py` | 测试：子进程跑 CLI + JSON 断言，不启动真实 broker |

## 测试

```bash
python3 make_fixtures.py          # 生成 fixtures/
python3 -m unittest test_auditor -v
```

夹具覆盖：跨批提交（含 HW 落在提交标记之前的未决情形）、中止、开放事务后穿插
普通消息（LSO 压住）、中段损坏长度、尾部截断、中段 CRC 失败（禁止跳过）、
无对应事务的标记、epoch 错误、并发事务、HW 越界 / 不在边界、headers 与 null 字段。
