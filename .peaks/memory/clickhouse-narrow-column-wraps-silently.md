---
name: clickhouse-narrow-column-wraps-silently
description: 越界写入 ClickHouse 的窄类型列会静默回绕而非报错
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-clickhouse-job.md
---

ClickHouse 在把超出目标列范围的值写入**更窄**的类型时，**静默回绕**而非报错 —— 这与「数据库会拒绝非法值」的直觉相反，且回绕后的值看起来是合法数据。

实测（ClickHouse 24.8.14.39，经真实 GORM 写入路径）：向 `quota Int32` 列写入 `3000000000` → **`err = nil`，落库 `-1294967296`**。原生 `VALUES`、`CAST`、`INSERT … SELECT` 三条路径**同样不报错**（`ARGUMENT_OUT_OF_BOUND` 无法复现）。

**How to apply:** 判断某列宽度是否够用时，**不能假设越界写入会失败并被发现** —— 它会静默变成负数或截断值混入数据。要证明「够用」，须核对业务上界（本项目 `common/quota_math.go` 的 `MaxQuota = math.MaxInt32`）而非依赖数据库报错。另注意同一实测批次确认：`ALTER TABLE … MODIFY COLUMN` 放宽数值类型（Int32→Int64）**可用**，但**不是** metadata-only —— 会重写该列的全部 part（1M 行约 2s，异步不阻塞启动）。
