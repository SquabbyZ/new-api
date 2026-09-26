---
name: db-errors-must-use-sanitizedberror
description: 驱动错误应走 sanitizeDBError，不要直接打印原始 err
metadata:
  type: convention
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-job.md
---

打印数据库错误应走仓库既有的脱敏层，因为驱动错误会内联数据值；直接打印原始错误功能上正常，但会漏掉该保护。

`model/gorm_logger.go` 提供 `sanitizeDBError`，其存在目的正是**驱动错误会内联数据值**。新增任何打印数据库错误的日志时应当使用它，而不是直接 `%s` 原始 `err`。

本项目 slice `rid-dbhard-s3` 新增的 `common.SysError` 未走该层，由 QA 独立发现（严重度 LOW：上界仅为看板业务键字面值，无凭据），修法是一行 —— 把 `err` 换成 `sanitizeDBError(err)`。

**How to apply:** 写涉及 DB 错误的日志前先 `grep -rn sanitizeDBError` 确认用法，与既有调用点保持一致。这条容易漏，因为直接打印 err 在功能上完全正常、只有从「驱动错误可能内联数据」的角度才看得出问题。
