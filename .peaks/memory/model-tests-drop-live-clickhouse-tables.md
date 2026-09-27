---
name: model-tests-drop-live-clickhouse-tables
description: "model 包的 ClickHouse 测试用 currentDatabase() + 裸表名 DROP TABLE，TEST_CLICKHOUSE_DSN 指向哪个库就在哪个库删表；指到活库会删掉后端正在用的真表。"
metadata:
  type: lesson
---

`model/clickhouse_log_test.go` 一类的测试对 ClickHouse 的写法是：

```sql
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'logs'
DROP TABLE IF EXISTS logs
DROP TABLE audit_logs
```

**表名是裸的、库名取 `currentDatabase()`** —— 也就是 `TEST_CLICKHOUSE_DSN` 里那个库。所以「DSN 指向哪个库，测试就在哪个库里建表/删表」。

**本会话实际发生过一次**：我把 `TEST_CLICKHOUSE_DSN` 指成 `.../newapi_logs` —— 那正是**运行中的后端在用的库** —— 于是测试跑完，`logs` 与 `audit_logs` 两张真表被删了。后端的 ClickHouse 写入随之静默失效（表只在**启动时**建，进程不重启就不会重建），而 `/api/setup` 等接口一切正常，**从表面完全看不出**。

**取证手段（定位这类事故的关键）**：ClickHouse 自己的 `system.query_log` 留有完整审计：

```sql
SELECT event_time, query_kind, substring(query,1,140)
FROM system.query_log
WHERE type='QueryFinish' AND event_time > now() - INTERVAL 4 HOUR
  AND query ILIKE '%DROP TABLE%'
ORDER BY event_time ASC;
```

据此可以精确到秒地看到「谁、什么时候、删了哪张表」，并与各子代理的执行时间窗对应。

**恢复方式**：重启后端即可 —— `migrateClickHouseLogDB()` 与 `MigrateAuditLogs()` 的 `CREATE TABLE IF NOT EXISTS` 会把表按当前 DDL 重建。**数据本身不丢的前提是没人往里写过**（本次因系统处于未初始化状态而无损）。

**附带副作用**：`controller` 侧的审计测试会**按纳秒建库**（`newapi_audit_<UnixNano>`）且**不清理**，本会话留下了 **18 个**空库。跑完这类测试后应主动检查并清理。

**How to apply:** 跑 `model` / `controller` 包的数据库测试前，`TEST_CLICKHOUSE_DSN` **必须指向专用或临时库，绝不能指向任何活库**（后端在用的库尤其）。派发子代理做数据库验证时，**提示词里要写清这一点** —— 本会话的教训正是「我在派发提示词里给了错的 DSN」，而不是子代理判断失误。跑完数据库测试后，顺手查一次 `system.databases` 与 `newapi_logs` 的表是否还在。
