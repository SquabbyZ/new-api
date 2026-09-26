---
name: perf-metrics-batch-pattern-vs-quota-data
description: "pkg/perf_metrics 是本仓库唯一正确的批量聚合管道（内存累加、定时 drain、失败回填、原子 upsert、Redis 计数），quota_data 是反例。"
metadata:
  type: convention
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff.md
---

批量聚合的写入路径应当对齐 pkg/perf_metrics 的模式：热路径只碰内存、定时 drain、失败回填、原子 upsert、计数走 Redis 天然跨节点聚合。quota_data 的写法是反例。

`pkg/perf_metrics/` 是本仓库**唯一正确的批量聚合管道**，生产可用。新增或重写任何计数/聚合写入路径都应对齐它的模式：

- 热路径只碰内存（`sync.Map` + 原子计数器桶），不每请求落库
- 定时 drain 已关闭的时间桶（`flush.go` 的 `flushLoop`）
- **刷写失败把计数回填**到桶里（`flush.go:54` 的 `bucket.addCounters(drained)`），不丢数据
- 原子 upsert（`clause.OnConflict`）
- 计数走 Redis → **天然跨节点聚合**
- 保留期由设置驱动，自带清理

`model/usedata.go` 的 `quota_data` 是反例：`CacheQuotaData` 是进程内普通 map（非 Redis），刷写对每键走 `First` + `Create`/`Updates` 两次往返（非原子，有重复插入竞态），`Create` 错误未检查且缓存无条件清空（静默丢数据），键基数为 用户×模型×分组×令牌×渠道×节点 组合数导致串行往返随量级增长。

**How to apply:** 改动 `quota_data` 或新增聚合表时按 perf_metrics 模式重写，不要复制 usedata 的写法。`perf_metrics` 本身不需要迁往 ClickHouse —— 它的形态已等价于 `SummingMergeTree` 想解决的问题且已解决（基数有界、批量、原子、跨节点）。
