---
name: dispatch-prompt-must-name-every-gated-artifact
description: "派单提示词若没点名某个门禁要求的工件，就没有任何人会产出它；代码与验证会一路绿灯，直到最后 `verify-pipeline` 卡住。本项目一个 slice 因此漏了四份工件。"
metadata:
  type: convention
---

`peaks workflow verify-pipeline` 按**字面路径**校验工件。而这些路径**不会**从
子代理的技能契约里自动推导出来 —— 它们是**派单提示词的输出契约**决定的。

slice `rid-abuse-alerting` 因此漏了**四份**工件，而每一份都不是子代理的失职：

| 漏掉的工件 | 阶段 | 为什么没人产出 |
|---|---|---|
| `audit/security-<rid>.md` | RD | 派单只点名了 `rd/code-review-*` 与 `rd/karpathy-review-*` |
| `audit/perf-<rid>.md` | RD | 同上 |
| `qa/test-cases/<rid>.md` | QA | 派单只点名了 `qa/test-reports/*` 与两条 `*-findings.md` |
| `qa/requests/<rid>.md` | QA | 该文件是**预建模板**，派单从未要求填它 |

**症状极具误导性**：`verify-pipeline` 报的是「RD evidence missing / QA evidence missing」，
读起来像「评审没做」；实际上**评审做了两轮，只是没落在它要的那个文件名上**。而代码在
`git log` 里看起来已经完全交付了。

**另一个更隐蔽的变体**：那些**预建模板**（`.peaks/_runtime/<sid>/<role>/requests/<n>-<rid>.md`）
是**空白模板**，子代理不会主动去填一份没人要求它填的模板。
`peaks request transition --role qa --state verdict-issued` 会因模板里的 `<id>` 占位符
报 `LINT_GATE_FAILED` —— 而错误信息只说「2 lint error(s)」，要再跑
`peaks request lint <rid> --role qa --project .` 才看得到是哪两行。

**How to apply:** 每个 slice 的派单提示词，其「输出契约」表必须**逐字列出该 slice 会触发的
所有门禁工件路径**（含 `audit/`、`qa/test-cases/`、`qa/requests/`、以及各 `*-review-*`），
并写明档名按字面校验。**不要**依赖「技能契约里应该有」—— 每次派单都要重新核对一遍
`verify-pipeline` 的完整清单。收尾时**先跑 `verify-pipeline`**（而不是等状态跃迁时才撞见），
它会在代码提交之前就把缺口暴露出来。
