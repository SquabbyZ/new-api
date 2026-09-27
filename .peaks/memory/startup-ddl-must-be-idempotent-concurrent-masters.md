---
name: startup-ddl-must-be-idempotent-concurrent-masters
description: 启动路径里的 DDL 必须写成幂等形式，因为并发 master 启动是本项目默认
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-clickhouse-job.md
---

本项目 `IsMasterNode = os.Getenv("NODE_TYPE") != "slave"` —— **未设置时默认全部节点都是 master**。因此**多个节点会并发执行启动期迁移**，启动路径里的每一条 DDL 都必须写成幂等形式，否则会变成「默认部署即崩」。

实测（slice rid-ch-s5）：ClickHouse 的 `ADD INDEX`（**不带** `IF NOT EXISTS`）在 8 个**栅栏同步**并发节点上，**7/8 以 `Code: 44 … index with this name already exists` 失败**；该错误被上抛到 `main.go` 的 `common.FatalLog` → **进程退出**。加上 `IF NOT EXISTS` 后 8/8 成功且索引恰好 1 条。

同类要求：`CREATE TABLE IF NOT EXISTS`（已是）、`MODIFY COLUMN`（实测重复执行 exit 0，幂等）、`ADD INDEX IF NOT EXISTS`。**必须实测每一条**，不能假设。

**How to apply:** 往启动/迁移路径加 DDL 时，逐条确认幂等性，并用**栅栏同步的并发探针**验证（先让 N 个节点全部探测、再同时下发 —— 顺序调用测不出该竞态，`-race` 对进程间竞态也无效）。另注意 `ADD INDEX IF NOT EXISTS` 配**不同定义**时是静默 no-op、保留旧定义，不会纠正既有索引。
