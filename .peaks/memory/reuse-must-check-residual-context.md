---
name: reuse-must-check-residual-context
description: 复用上一轮的提示词或结论时必须检查残留上下文
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-job.md
---

复用上一轮的提示词、约束或结论时，必须逐条确认哪些对当前目标不适用并显式删除，否则残留上下文会误导下游。

**不要 `sed` 复用提示词 —— 每次重写。** 本项目 job 中，编排器**两次**用 `sed` 把上一 slice 的 QA 派发提示词替换 rid 后复用（一次 S3→S6、一次 S6→S4），**两次正文都仍带着上一 slice 特有的约束**，而那些对当前 slice 完全不适用：
- S3→S6 那次残留了「无唯一索引 / `clause.OnConflict` / `sanitizeDBError`」，对 S6（迁移告警）无意义
- S6→S4 那次残留了「`NODE_TYPE` / advisory lock / 迁移零改变」，对 S4（日志批量化）无意义

两次都在**派发前校验**时发现并废弃重写。**校验拦住了错误，但产生错误的行为模式没有消失** —— 只要还用 sed 复用，就会继续产生。

同类的第三例：编排器写入的 fresh-context 指令要求「计数器原子累加用 `clause.OnConflict`」，而这与后来某个 slice 的硬性约束（`quota_data` 无唯一索引、禁用 OnConflict）**正面对冲** —— 由 RD 主动指出。

**How to apply:**
1. **每个 slice 的派发提示词都重写，不要 sed 复用。** 复用省下的那点时间，远小于一个带错约束的子代理浪费的算力。
2. 若确实要复用，先列出**当前 slice 的全部约束**，再逐条对照旧提示词，把不适用的显式删除 —— 不要反向做（从旧的删，容易漏）。
3. 派发前做一次机械校验，断言提示词**含**当前 slice 的关键标识（DSN、目标文件名）且**不含**其他 slice 的专有术语。注意校验会误报：CLI 会自动注入项目记忆索引，其中出现的术语不是泄漏。
4. 同样的纪律适用于约束、结论、检查清单的跨 slice 继承 —— 显式删除或标注例外，不要整体继承。
