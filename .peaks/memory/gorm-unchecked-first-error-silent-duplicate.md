---
name: gorm-unchecked-first-error-silent-duplicate
description: 未检查 First 错误会让探测失败退化为静默插入重复行
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-job.md
---

GORM 的「先探测后插入」写法若未检查探测调用的错误，瞬时失败会给已存在的键插入重复行且写入成功无声，表现为静默双计。

GORM 的「先 `First` 探测、不存在则 `Create`」这种 upsert 形态有一个静默陷阱：**若 `First` 的错误未被检查**，连接中断/超时等**瞬时失败**会让 `quotaDataDB.Id` 保持 0，从而走 `Create` —— 给一个**已存在的键插入重复行，且写入成功无声**。下游把它当成两笔数据，表现为静默双计。

正确写法是区分两类结果：`errors.Is(err, gorm.ErrRecordNotFound)` 是**正常路径**（新键，走 `Create`）；其它错误是**失败**（应上报并跳过本轮，不能落到 `Create`）。

本项目 `model/usedata.go` 的 `SaveQuotaDataCache` 曾有此问题，已在 slice `rid-dbhard-s3` 的修复轮关闭（`model/main.go` 同批次无关）。

**How to apply:** 审阅或编写「先查后写」形态时，逐一确认探测调用的错误被检查并且 `ErrRecordNotFound` 与其它错误被分开处理。另注意同类形态的可修性判断陷阱：把「幂等 upsert 的并发安全需要唯一索引」与「探测失败就不写」混为一谈是错的 —— 后者是**少做一次写**，不需要任何索引。
