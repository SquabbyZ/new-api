---
name: consume-log-write-path-sync-single-row
description: "计费请求日志写入是请求关键路径上的同步单行 INSERT，无队列无批量；改远端库前须先异步批量化，且不得丢失 quota_saturation 标记。"
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff.md
---

计费请求的日志写入是请求关键路径上的同步单行插入，没有队列也没有批量。把日志库指向远端会让每个计费请求的尾延迟增加一个网络往返。

`relay/*_handler.go` → `service.PostTextConsumeQuota` → `model.RecordConsumeLog` → `createLog` → `LOG_DB.Create(log)` 是**同步单行 INSERT**。`service/text_quota.go` 内无 `gopool`、无 `go func`、无 `defer`。

含义：把日志库指向远端（尤其 ClickHouse）会让**每个计费请求的尾延迟增加一个链路 RTT**，同时 ClickHouse 的单行插入会产生海量小 part 并触发 `too many parts`（GORM 的 clickhouse 驱动 `driver/clickhouse@v0.6.0` 无任何 batch 逻辑）。当前 `LOG_SQL_DSN` 未设时 `LOG_DB = DB` 同一条连接，所以这笔成本是隐藏的。

**Billing constraint（依据 `.agents/rules/billing.md`）:** `attachQuotaSaturation`（`service/log_info_generate.go`）必须在**写 consume/task log 之前**运行，把 `common.QuotaClamp` 标记嵌进日志的 `other.admin_info.quota_saturation` 并发 `logger.LogWarn`。改造日志写入路径时该标记必须随日志行一起携带、不得因异步化丢失或延迟告警。

**How to apply:** 这是切 ClickHouse 的前置项，也与延迟优化是同一件事。属计费路径改动，动手前必须完整读 `.agents/rules/billing.md`。模式参考 `pkg/perf_metrics/flush.go`。
