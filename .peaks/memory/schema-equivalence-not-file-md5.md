---
name: schema-equivalence-not-file-md5
description: 判定迁移或 schema 是否等价，不要用文件 md5
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-job.md
---

判定迁移或 schema 是否等价不能用数据库文件 md5，因为同一二进制重复运行的 md5 本身就不同。

slice `rid-dbhard-s6` 中 QA 发现 SQLite 库文件 md5 在改动前后不同 —— 仅凭此极易误判为「迁移行为被改变」，从而否定该 slice 的核心承诺。QA 用**重复运行对照**（同一二进制跑两次 md5 也不同）证明这是运行间不确定性（页分配/空闲页布局），与改动无关。

**How to apply:** 证明「迁移/schema 未变」的有效口径是**逐列 schema 与行数对照**，不是文件级 md5。本项目实测有效证据：MySQL 459 列行、PostgreSQL 459 列行 + 185 索引、SQLite 191 schema 对象 + 38 表行数，两侧全部 IDENTICAL。看到 md5 差异时先做重复运行对照，再下结论。
