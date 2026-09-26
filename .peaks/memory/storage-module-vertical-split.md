---
name: storage-module-vertical-split
description: "存储按模块垂直切分：事务域用主库 DB，可观测明细域（logs/audit_logs）用 ClickHouse 日志库，两域无重叠数据。含边界实测与 2 处跨库组合点。"
metadata:
  type: decision
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff.md
---

new-api 的存储架构按模块垂直切分，不是交叉使用。事务域用主库，可观测明细域（日志与审计）用独立日志库，两域没有任何一张表重叠。

new-api 的存储架构是**按模块垂直切分**，不是交叉使用：`DB`（主库，事务域）与 `LOG_DB`（日志库，可观测域）由 `SQL_DSN` / `LOG_SQL_DSN` 分别指定。未设 `LOG_SQL_DSN` 时 `LOG_DB = DB` 优雅降级。

实测边界：`LOG_DB` 只有 `logs` 与 `audit_logs` 两张表；主库 35 张表，无一张重叠；在 `service/` 与 `middleware/` 中搜索全部日志库引用，唯一命中是日志清理任务自身 —— **没有任何业务或计费判定读日志库**，日志库是纯可观测数据，事实来源（额度）在 `users.quota` / `tokens`。

只有 2 处跨库组合点，且都是应用层拼装而非 SQL JOIN：`model/log.go:542`（补渠道名，开启 `MemoryCacheEnabled` 时走缓存）、`model/audit_log.go:215`（补用户角色）。

**How to apply:** 新增功能若同时需要日志与业务数据，必须照这两处做「两次独立查询 + 内存拼装」；跨库实例没有 SQL JOIN。任何数据都不允许同时属于两个域 —— 这是该架构全部价值所在。若某计费判定改从日志库读取，切分立即失去正确性保证。
