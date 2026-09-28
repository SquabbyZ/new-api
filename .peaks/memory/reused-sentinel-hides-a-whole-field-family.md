---
name: reused-sentinel-hides-a-whole-field-family
description: "一个被复用的哨兵值（`maxValue == 0` 表示「无上界」）会让一整族字段集体失去边界；修法是删掉哨兵本身，而不是逐颗补参数。"
metadata:
  type: lesson
---

slice `rid-abuse-alerting`（异常用量告警）的校验器里有一个共用函数：

```go
// max 为 0 表示无上界。
func checkAbuseAlertIntRange(key, value string, minValue int, maxValue int, extra func(int) error) error {
	...
	if maxValue > 0 && parsed > maxValue { return fmt.Errorf(...) }
```

`min_*` 一族**四个调用点全都传 `0`**，于是**四颗旋钮集体无界**。

**后果不是「不够严」，是「静默关掉」**：`min_baseline_requests` 写成一个极大的数之后，
基线门恒不成立 ⇒ **四条规则一起停摆**，而 API 一律报 `insufficient_baseline`
（读起来是「基线还在积累」）—— **撒谎的是展示层**。评审的探针实测：validator 收下
`9223372036854775807`，经**生产** `UpdateOption` 写入 `1000000000` 后，一个每基线窗
1000 次请求的令牌**基线门仍为 false**。

**注意发现顺序**：这不是第一次。同一个 slice 里，**先**修了 `baseline_windows` 缺 gate 检查
（会让三条规则静默全关），**之后**才发现 `min_*` 一族是同一个 `maxValue == 0` 语义的产物。
**一个被复用的哨兵值会掩盖一整族字段** —— 修完一颗还会冒出下一颗。

**修法（结构性，不是补参数）**：
1. 给全族各自补上界（由**各自的内置默认值** × 常量推导，避免「默认值改了、上界没改」两处漂移）。
2. **删掉哨兵本身**：`if maxValue > 0 && …` → `if parsed > maxValue`，让 `maxValue` 必须是
   真实上界。

**如实记录这条修复的边界**（RD 声称「新键忘记写上界 ⇒ 测试立刻红」，QA 证明**只有一半成立**）：
- **漏传参数**确实编译不过（`not enough arguments`）；
- 但**现实的忘记形态**是照抄既有调用点写成 `..., 0, 0, nil` —— QA 加了一个这样的假想键，
  `./model/` 与 `./setting/...` **全绿**，因为**没有任何测试枚举那个 switch**。
- 真正的收益是**性质不同**的一条：忘记写上界**不再能静默关掉检测**，而是让该键只能取 0
  （即最大灵敏度）—— **从 fail-open 变成 fail-closed**。

**How to apply:** 看到 `someFlag == 0` 表示「无限制/不适用」这类哨兵，**先去数它被传了几次**
—— 如果超过一次，那多半是一族字段集体缺边界，而不是一个字段写错。修的时候**删掉哨兵**，
否则下一个新增的调用点会重演。另外：任何「这样改就再也不会漏了」的声称，**都要问「测试真的
会红吗」** —— 编译器能挡住的和你以为它能挡住的不一定是同一件事。
