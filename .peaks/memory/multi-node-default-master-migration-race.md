---
name: multi-node-default-master-migration-race
description: "NODE_TYPE 未设即 master，多节点会并发执行迁移；连接池 1000 每进程按节点数放大；一致性为 60 秒轮询。"
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff.md
---

多节点部署时若未设置 NODE_TYPE，每个节点都会被视为 master 并在启动时并发执行数据库迁移，可能造成 DDL 锁争用或重复约束。

`common/init.go:89`：`IsMasterNode = os.Getenv("NODE_TYPE") != "slave"` —— **未设置时默认为 master**。而 `InitDB` 只在 `common.IsMasterNode` 为假时提前返回，即从节点不跑迁移。

因此多节点部署若忘记给节点设 `NODE_TYPE=slave`，所有进程会在启动时**同时执行 `migrateDB()`**，含 `AutoMigrate` 与手写 `ALTER TABLE`，并发 DDL 在 PG 上争 `ACCESS EXCLUSIVE` 锁、在 MySQL 上可能死锁或产生重复约束。

同时 `SQL_MAX_OPEN_CONNS` 默认 **1000，且每进程**（`model/main.go:211-213` 与 `258-260`，`InitDB` 与 `InitLogDB` 各设一次）→ 单节点潜在 2000 条连接，N 节点线性放大。PG 每连接是一个后端进程。PG 路径已启用 `PreferSimpleProtocol` 关闭命名预处理语句，正是为兼容 PgBouncer 这类事务池代理。

**How to apply:** 多节点部署显式指定唯一 master，其余全部 `NODE_TYPE=slave`；按节点数重算 `SQL_MAX_OPEN_CONNS` / `SQL_MAX_IDLE_CONNS` 并在远端库前置连接池。另注意多节点一致性模型是 **60 秒轮询**（`SyncChannelCache` / `SyncOptions` / `authz.StartPolicySync`，`common/init.go:110` 的 `SYNC_FREQUENCY`），渠道与授权变更最长 60 秒才传播到所有节点 —— 不要做「立即生效」的运维承诺。
