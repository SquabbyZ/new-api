---
name: franz-go-default-maxversions-vs-kafka-3-7
description: "franz-go 默认 MaxVersions 会向 Kafka 3.7 发它不支持的请求版本，broker 直接关连接而不回 UNSUPPORTED_VERSION，症状伪装成超时或 SASL 缺失。"
metadata:
  type: lesson
---

`franz-go`（Go 的 Kafka 客户端）默认 `MaxVersions` 取 `kversion.Stable()`，v1.19.0 下会发出 **`ApiVersions v4`** 与 **`Metadata v13`**。而 **Kafka 3.7 不认识这两个版本** —— 关键在于它**不回 `UNSUPPORTED_VERSION`，而是直接关闭连接**。于是版本协商永远无法完成，客户端只能一遍遍重试。

## 两个会把人带偏的症状

同一个根因，两个完全不同的误导性表象（均在真实 broker 上实测）：

| 操作 | 报错 |
|---|---|
| 生产 | `records have timed out before they were able to be produced` ← 看起来像网络慢或 broker 挂了 |
| **`Ping`** | **`happens when SASL is required but not provided: is SASL missing?`** ← **看起来像认证配置问题** |

第二条尤其危险：**在做生产之前的连通性探测（Ping）的人，会被引去排查 SASL** —— 而 SASL 与真正的原因毫无关系。broker 侧日志才给出真相：

```
InvalidRequestException: Received request api key METADATA with version 13 which is not enabled
```

## 修法

显式钉住版本上限，**生产端与消费端都要设**：

```go
kgo.MaxVersions(kversion.V3_7_0())
```

（本仓库 `pkg/logkafka` 的 `producerOpts` 与 `consumerOpts` 两处都已打上。）

实测对照：默认版本下 produce 报超时；钉住后返回 `<nil>`。`kversion.Stable()` 给出 apiVersions=4 / metadata=13，`V3_7_0()` 给出 3 / 12。

## 为什么这条值得记

它的**症状与根因相距极远**，而且**两个症状各自指向错误的方向**（网络 / SASL）。没有 broker 侧日志的话，靠客户端报错几乎不可能定位。凡是「客户端报的错指向 A，真实原因在 B」的坑，都值得先记下来。

**How to apply:** 接入任何 Kafka 客户端库时，**不要依赖它的默认协议版本协商** —— 显式钉到目标 broker 的版本。连不上时，**先看 broker 侧日志**（`InvalidRequestException` / `UNSUPPORTED_VERSION` 之类），不要只顺着客户端报错的方向排查。若用的是 franz-go，检查 `kgo.MaxVersions` 是否被设置过。
