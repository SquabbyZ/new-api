---
name: deleteoldlogbatch-limit-dropped-on-pg-sqlite
description: "GORM deleteClauses 不含 LIMIT，PostgreSQL 与 SQLite 方言下 Delete().Limit(n) 生成的 SQL 无 LIMIT，分批删除静默退化为全量删除。"
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff.md
---

GORM 的删除子句列表不含 LIMIT。因此在 PostgreSQL 与 SQLite 上调用 Delete 并附加 Limit 时，生成的 SQL 会静默丢弃 LIMIT，分批删除退化成一次性删除全部过期行。

`model/log.go:734` 的 `LOG_DB.Where("created_at < ?", t).Limit(limit).Delete(&Log{})` 在 PostgreSQL 与 SQLite 上**生成的 SQL 不含 LIMIT**。

根因（GORM v1.25.12 源码实测）：`callbacks/callbacks.go:10` 的 `deleteClauses = []string{"DELETE","FROM","WHERE"}` 不含 `LIMIT`；`statement.go:469` 的 `Build(clauses...)` 只输出传入名字的 clause。方言驱动各自覆盖：MySQL `mysql.go:64` 含 `LIMIT`（生效），PostgreSQL `postgres.go:77` 与 glebarez/sqlite `sqlite.go:61` 都不含（静默丢弃）。

后果：PG/SQLite 上单条 DELETE 删掉**全部**过期行 —— 巨型事务、WAL 暴涨、长事务持锁、表膨胀需 VACUUM。`RowsAffected` 返回全量导致进度状态机一次跳到完成，`service/system_task.go:22` 的 `logCleanupBatchSize = 100` 形同虚设。MySQL 是另一极端：每批 100 行且每批写一次状态更新。

**How to apply:** 需要分批删除时不要依赖 GORM 的 `Limit` 子句。改用带主键子查询的删除或按时间分片，并对目标方言实测生成的 SQL（`gorm.Session{DryRun:true}` 可无库验证）。另注意 `model/usedata.go:120` 的 `Create` 错误未检查、`:123` 无条件清空缓存，同属此类的静默失败。
