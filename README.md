# kafka-audit — Kafka magic=2 read_committed 离线审计器

离线命令行工具：读取一个分区导出的 Kafka magic=2 未压缩 RecordBatch 字节流，
自行解析（不依赖任何 Kafka 客户端），判定哪些业务记录可以按 `read_committed`
语义交付，并给出每条记录的判定理由。

## 输入假设

- 字节流从 offset 0 开始，未压缩（attributes 压缩位必须为 0）、未经过日志压实，
  因此各批次 base offset 必须连续；
- 最多 32 个批次，超出即拒绝；
- high watermark（`--hw`）以日志偏移量给出，且必须落在批次边界上；
  只有 HW 之前的完整批次参与判定。

## 用法

```bash
python3 -m kafka_audit <logfile> --hw <offset> [--json]
# 或安装后： kafka-audit <logfile> --hw <offset>
```

退出码：

| 码 | 含义 |
|----|------|
| 0  | 审计通过，已给出可交付列表（尾部不完整会被标记但不影响已验证区域） |
| 1  | 审计失败：结构损坏 / CRC 校验失败 / 事务协议违规（扫描即中止，绝不跳过继续） |
| 2  | high watermark 超过已验证区域，不提供交付列表 |
| 3  | 用法或文件读取错误 |

## 判定规则

- 事务以 (producer id, producer epoch) 关联；同一时间至多一个开放事务，
  出现第二个并发事务即拒绝；
- commit/abort 控制标记必须匹配当前开放事务的 producer id/epoch，
  错误 epoch 或无对应事务的标记一律拒绝；控制标记本身绝不作为业务记录交付；
- 已提交事务的记录可见；已中止事务的记录全部隐藏；
- 未结束事务从其首条记录起压住 last stable offset（LSO），
  之后穿插的普通记录同样不得越过 LSO 提前交付；
- 可交付范围 = `[0, LSO)` 内除去中止事务记录与控制标记的全部记录。

## 错误处理

- 批次长度、记录长度、varint、offset 连续性等结构错误：致命，立即中止；
- CRC32C 校验失败：致命，禁止跳过损坏批次继续扫描；
- 尾部不完整（最后一个批次被截断）：明确标记为 truncated tail，
  已验证区域内的判定仍然有效；若 HW 指向已验证区域之外，则不提供交付列表。

## 输出

文本模式分区展示：流验证结果（含截断标记）、批次清单、每条记录的
原始 offset／字节位置／VISIBLE 或 HIDDEN 及原因、事务清单，
以及最终的 LSO、可交付范围与可交付 offset 列表。`--json` 输出同等信息的
机器可读形式。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试不启动真实 broker：`tests/fixtures.py` 是一个与解析器**零共享代码**的
独立编码器（CRC32C 用逐位算法，审计器用查表法），覆盖跨批提交、中止、
开放事务后穿插普通消息、损坏长度、CRC 中段失败、错误 epoch／无对应事务的
标记、HW 越界与不在批次边界等场景。

## 目录结构

```
kafka_audit/
  crc32c.py       CRC32C（查表法）
  recordbatch.py  zigzag varint、magic=2 批次/记录/控制标记解析
  auditor.py      read_committed 语义判定（事务跟踪、LSO、逐条判定）
  cli.py          命令行界面（文本 / JSON）
tests/
  fixtures.py     独立实现的二进制夹具编码器
  test_auditor.py 单元测试与 CLI 测试
```
