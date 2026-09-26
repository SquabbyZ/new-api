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

`model/usedata.go` 的 `quota_data` 是反例：`CacheQuotaData` 是进程内普通 map（非 Redis），刷写对每键走 `First` + `Create`/`Updates` 两次往返（非原子，有重复插入竞态），键基数为 用户×模型×分组×令牌×渠道×节点 组合数导致串行往返随量级增长。

**⚠️ 给 `quota_data` 套用 perf_metrics 模式时有一个硬阻塞：它没有唯一索引。** 实测 `QuotaData` 结构体的 8 列业务键（`user_id` / `username` / `model_name` / `created_at` / `use_group` / `token_id` / `channel_id` / `node_name`）上**不存在任何唯一索引** —— 现有标签全是非唯一的 `index:`（`idx_qdt_model_user_name` 只覆盖 model_name+username，`idx_qdt_created_at` 只覆盖 created_at）。没有唯一约束，`clause.OnConflict` 在 PostgreSQL 上直接报错、MySQL 的 `ON DUPLICATE KEY UPDATE` 只命中主键、SQLite 需要唯一索引。**因此「给 quota_data 改原子 upsert」必然隐含一次 schema 变更**；而 schema 变更按 `AGENTS.md` 需在全新库与「上一版本升级而来的库」上各验证、启动两次证明幂等，且**现有生产表可能已因当前竞态含重复行 —— 直接加唯一索引会导致迁移失败**，须先去重。属需要用户在场的独立 slice。

**How to apply:** 改动 `quota_data` 或新增聚合表时按 perf_metrics 模式重写，不要复制 usedata 的写法 —— 但**必须先读上面那段**：把 `clause.OnConflict` 用于 `quota_data` 前，先确认唯一索引是否存在，不存在就不要做（会隐含 schema 变更）。`perf_metrics` 本身不需要迁往 ClickHouse —— 它的形态已等价于 `SummingMergeTree` 想解决的问题且已解决（基数有界、批量、原子、跨节点）。

**已完成的部分（slice rid-dbhard-s3，2026-09-26）：** 静默丢数据已修 —— `Create` 与 `increaseQuotaData` 的错误现在被检查并以 `common.SysError` 上报（含 user_id / model_name / created_at），失败的键**保留在缓存**待下次刷写，只有成功落库的键才 `delete`，成功日志仅在零失败时打印。**仍未解决**（需唯一索引）：跨节点并发重复插入竞态。`First` 探测失败曾会静默走 `Create` 插重复行导致看板双计，已在同 slice 的修复轮中处理。

**⚠️ 该修复引入的取舍（slice rid-dbhard-s3 QA 实测，必须知情）：** 失败键留存 + 重试使落库语义变为 **at-least-once**。在**模糊失败**（服务端已提交、客户端报错）下，`UPDATE ... SET count = count + ?` 会在下一轮**重复累加** —— 三方言实测：正确应 6/600/90，实测 7/700/130。只有 `Updates` 分支如此；`Create` 分支自愈（下轮探测到行已存在，改走 UPDATE，不插第二行）。即：该修复把「静默丢数据」换成了模糊失败下的「静默多计」，两者都错、方向相反。**修它需要幂等键或唯一索引**，同受本文档开头那段硬阻塞约束，属需用户在场的独立 slice。

**另有一处待清理项（QA 发现，LOW）：** 新增的 `common.SysError` 未走仓库既有脱敏层 `sanitizeDBError`（`model/gorm_logger.go`，正为「驱动错误内联数据值」而设），直接打印原始 err。上界仅为看板业务键字面值（非凭据），故 LOW。修法是一行：把 `err` 换成 `sanitizeDBError(err)`。
