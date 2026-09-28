---
name: ctx-timeout-does-not-bound-connect
description: "`context.WithTimeout` 只约束查询本身，不约束建立连接；握手阶段的读超时取自驱动自己的选项，实测把 3s 的期限变成 30s。给日志库调用设期限，必须由调用方自己收结果。"
metadata:
  type: lesson
---

slice `rid-abuse-alerting`（异常用量告警）的收尾性能评审把一处日志库调用标为
**"Not measured — verified by reading"**，理由是「3s 超时 + 软失败已经把最坏情况限住」。
补测后**结论相反**。

**实测**（ClickHouse 24.8.14.39，真实驱动）：在一个**接受 TCP 但既不读也不写也不关**的
监听上把 `LOG_DB` 指过去 —— 这是防火墙/代理最常见的失效形态 —— 然后计时：

| 形态 | 实测耗时 |
|---|---|
| 直接调用（`ctx` 3s） | **30.009062s** |
| 由调用方收结果（`select` on `ctx.Done()`） | **3.0005305s** |

原因写在驱动日志里：`handshake: failed to read packet …: i/o timeout`。
`context.WithTimeout(ctx, 3*time.Second)` 只约束**查询**，而**建立连接**（握手）的读超时
取自驱动自己的选项（clickhouse-go 默认 30s），**ctx 期限被完全盖过**。

含义：`GET /api/option/` 在日志库「不应答」时真的会挂 **30 秒**，而它正被整个设置页依赖
（`useSystemOptions`：`staleTime 5min` + 默认 `refetchOnWindowFocus`）。代码旁那句注释
「不应该让请求挂在那里」在当时是**假的**。

**修法**：把探针放进一个**带缓冲（容量 1）**的 channel 的 goroutine 里，`select` 在
「结果」与 `ctx.Done()` 之间取先到者。带缓冲是关键 —— 即使调用方已经放弃等待，被放弃的
探针也一定能把结果放进去并退出，不会永久阻塞在发送上（QA 实测：5 次调用后 goroutine
5→10，40 秒后回到 5，不泄漏）。

**How to apply:** 给任何数据库/网络客户端的调用设「不许超过 N 秒」时，**不要以为 ctx 就够**
—— 连接建立阶段的超时往往来自驱动自己的选项。要么由调用方 `select` 收结果（上面的写法），
要么显式配置驱动的 dial/read 超时（注意那通常是**全部**该类调用共享的接线，影响面更大）。
**并且**：凡是「读代码得到的印象」，在被实测之前都只是假设 —— 这条结论正是被一次实测推翻的。
