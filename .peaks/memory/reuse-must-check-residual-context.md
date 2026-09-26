---
name: reuse-must-check-residual-context
description: 复用上一轮的提示词或结论时必须检查残留上下文
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-job.md
---

复用上一轮的提示词、约束或结论时，必须逐条确认哪些对当前目标不适用并显式删除，否则残留上下文会误导下游。

本项目 job 中，编排器把上一 slice 的 QA 派发提示词用 `sed` 替换 rid 后复用，结果**正文里仍带着上一 slice 特有的约束**（无唯一索引、`clause.OnConflict`、`sanitizeDBError`），而那些对当前 slice 完全不适用，会误导子代理。派发前校验发现并废弃重写。

同类的第二例：编排器写入的 fresh-context 指令要求「计数器原子累加用 `clause.OnConflict`」，而这与后来某个 slice 的硬性约束（`quota_data` 无唯一索引、禁用 OnConflict）**正面对冲** —— 由 RD 主动指出。

**How to apply:** 复用上一轮的提示词、约束、结论或检查清单时，逐条确认哪几条对当前目标**不适用**，并显式删除或标注例外，而不是整体继承。对派发出的提示词做一次机械校验（例如断言不含上一轮的专有术语）成本极低、收益明显。
