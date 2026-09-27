---
name: skipping-index-benefit-formula
description: 加跳数索引前先用公式估收益，低基数列零收益
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-clickhouse-job.md
---

给 ClickHouse 的 MergeTree 加跳数索引（skipping index）前，先按公式估收益，不要凭「加了索引就会快」下结论。

**边界公式**：granule 内 G 行、被索引列基数 U 时，无法排除该 granule 的概率 ≈ `1 − e^(−G/U)`（实测系统性低 1–2 个百分点，因 bloom 假阳性）。

**实测（1M 行，`G = 8192`，`ORDER BY (created_at, request_id)`）**：

| 列 | `minmax` | `bloom_filter(0.01)` | `set(8)` |
|---|---|---|---|
| `user_id`（高基数） | **剪掉约 1%** | **约 18.5%** | — |
| `type`（8 个取值） | **0%** | **0%** | **0%** |

结论：① **高基数列用 `bloom_filter`，别用 `minmax`** —— `minmax` 只在列与排序键**相关**时才有用，与排序键无关的列上几乎全废；② **低基数列加任何跳数索引都是零收益** —— 取值频率远高于「每 granule 一次」时，每个 granule 必含全部值，任何 granule 级索引都排不掉。

**How to apply:** 加跳数索引前先算 `1 − e^(−G/U)`：U ≥ G 时收益接近 0，不值得加。若目标是「按某列过滤不再全范围扫描」而该列与排序键无关，**跳数索引不是解法** —— 真正的解法是改排序键（只能重建表）或调小 `index_granularity`。另注意 `MATERIALIZE INDEX` 会让索引覆盖既有 part，但它**不是代价幂等**的：重复或并发调用会真的重跑 mutation 重写 part。
