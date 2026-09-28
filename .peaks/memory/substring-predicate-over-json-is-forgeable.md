---
name: substring-predicate-over-json-is-forgeable
description: "在序列化 blob 上用裸子串做谓词，会被「客户端可控字段的值」伪造；改用带引号与冒号的 JSON key 形态可以免疫，因为序列化会把值里的 `\"` 转义成 `\\\"`。"
metadata:
  type: lesson
---

slice `rid-abuse-alerting` 要在 ClickHouse 里筛出「额度被钳过」的日志行以排除它们。该标记
嵌在 `log.other` 这个 JSON 串里（`service/log_info_generate.go` 的 `attachQuotaSaturation`
写进 `other.admin_info.quota_saturation`）。原实现用 `other LIKE '%quota_saturation%'`。

**这是可被客户端伪造的漏报通道。** `other` 里同时含 `request_path`，而它来自
`ctx.Request.URL.Path`（经 `POST /v1beta/models/*path` 可达）。于是**任何调用方只要把自己的
请求路径写成含 `quota_saturation` 的样子，就能让自己的消费行被判为「饱和行」而从比率运算里
剔除 —— 也就是压掉自己的消费告警**。对一个「唯一目的就是发现盗刷」的功能，这直接废掉它。

**修法：把谓词从裸子串换成 JSON key 形态** —— `"quota_saturation":`（**带引号与冒号**）。

**为什么不可伪造**：`common.Marshal` 会把字符串值里的 `"` 转义成 `\"`。攻击者输入
`a"quota_saturation":b`，落库串是 `a\"quota_saturation\":b` —— `quota_saturation` 后面跟的是
`\` 而不是 `"`，**匹配不上**。一个未被转义的 `"` 在 JSON 文本里只能是**结构分隔符**，而结构
分隔符的位置由序列化器决定，不由客户端决定；值被引号包住，其后只可能是 `,` 或 `}`，
**永远不是 `:`**。

**QA 穷举了 25 种伪造形态**（`\"quota_saturation\":`、`\\"…\\":`、`x","quota_saturation":"y`、
`{"quota_saturation":…}`、`","admin_info":{…},"z":"`、真引号字符、NUL、非法 UTF-8 等），
同时写入 `request_path` / `response_model` / `upstream_model_name` / `po` / `billing_model`
五个字段，经**真实** `LogOther.JSONString()` → `common.Marshal`：**0/25 命中**。

**两个容易踩的坑**（QA 与 RD 各踩了一次）：

1. **证明可信度取决于夹具是否真的含引号与反斜杠。** RD 原来的 AC 夹具值里**既无 `"` 也无
   `\``，所以它只证明了「裸子串不匹配」，**根本证明不了「转义挡得住 key 形态」**。
2. **夹具的 SQL 字面量不转义反斜杠会让探针得假结果。** `abuseFixtureSQLString` 不转义
   `\`，而 CH 字面量会把 `\"` 吃成 `"` ⇒ 得假结果。改用**参数绑定**后才成立。

**How to apply:** 在序列化文本上做筛选时，**先问这个文本里有没有客户端可控的字段**。有，
就**不能**用裸子串 —— 用该序列化格式的**结构形态**（key + 分隔符）。并且验证时要**真的去
构造伪造输入**，注意夹具本身是否具备证明力（值里有没有引号/反斜杠），必要时用参数绑定
而不是手拼字面量。
