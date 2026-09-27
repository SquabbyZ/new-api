---
name: clickhouse-sort-key-cannot-be-altered-in-place
description: "ClickHouse 的 MergeTree 排序键实质无法原地修改，给已有列或带默认值的新列加进排序键都会被拒；补 user_id 这类过滤列的剪枝应改用跳数索引，改排序键只能重建表迁移数据。"
metadata:
  type: lesson
---

ClickHouse 的 MergeTree 排序键实质无法原地修改。要给日志表的 `user_id`、`type` 这类过滤列加剪枝能力，正解是加跳数索引，不是改排序键。

在真实 ClickHouse 24.8.14.39 上实测（临时表结构照抄本项目 `logs` 的 DDL），三条改排序键的路径全部被拒：

| 操作 | 结果 |
|---|---|
| `ALTER TABLE t MODIFY ORDER BY (created_at, request_id, user_id)` | ❌ `Code: 36 — Existing column user_id is used in the expression that was added to the sorting key. You can add expressions that use only the newly added columns.` |
| 先 `ADD COLUMN user_id_k` 再 `MODIFY ORDER BY (... user_id_k)` | ❌ 同上 —— 「新建」指**同一条 ALTER 内**新增 |
| 同条 ALTER 内 `ADD COLUMN gk String DEFAULT ''` + `MODIFY ORDER BY (... gk)` | ❌ `Code: 36 — Newly added column gk has a default expression, so adding expressions that use it to the sorting key is forbidden.` |

实测后 `system.tables.sorting_key` 始终是 `created_at, request_id`——**未变**。日志表的列几乎都带 `DEFAULT`，所以第三条限制基本封死了这条路。

**可行的两条路：**

1. **跳数索引（推荐，已实测可行）**：`ALTER TABLE logs ADD INDEX idx_user_id user_id TYPE minmax GRANULARITY 4` + `MATERIALIZE INDEX idx_user_id`，两条都成功；`EXPLAIN indexes=1` 显示 planner 确实查询该 `Skip` 段。**无需改排序键、无需迁数据。**
2. **重建表 + 迁数据**（重操作，需用户在场）：旧表 → 建新表（目标 ORDER BY）→ `INSERT SELECT` → 原子 `RENAME`。对 3 个月存量是大量 I/O 加写入空窗。

**同一批实测还确认**：`ALTER TABLE t MODIFY COLUMN quota Int64`（Int32 → Int64 放宽）**成功**，`system.columns` 确认类型已变。

**How to apply:** 遇到「某列过滤慢、想加进排序键」时，先按第 1 条加跳数索引，不要假设改排序键是小改动 —— 它会把你引向数据迁移。另注意跳数索引的收益依数据分布而定：`minmax` 只在 granule 内取值范围窄时才真正跳过，必须实测收益边界，不能只证明「索引存在」。同类事实：ClickHouse 的 `ALTER ... MODIFY COLUMN` 放宽数值类型可用，与本条形成对照。
