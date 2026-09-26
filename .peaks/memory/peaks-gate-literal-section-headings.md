---
name: peaks-gate-literal-section-headings
description: peaks 门禁要求工件含特定字面标题，缺则报成缺少整份文件
metadata:
  type: convention
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff-job.md
---

peaks 的过渡门禁按字面标题校验工件，且把「缺小节」报成「缺整份文件」，容易让人往错方向排查。

peaks 的 `request transition` 门禁按**字面标题**校验工件，且把「缺小节」报成「缺文件」——错误信息形如「N artifact(s) missing」，实际原因是 `missing section(s): X`。真实需求（本项目实测）：

- `rd/bug-analysis.md` → `## Root cause`、`## Fix approach`
- `rd/code-review-<rid>.md` → `## CRITICAL`（声明有无 CRITICAL/HIGH）、`## Findings`
- `rd/karpathy-review-<rid>.md` → `## Karpathy-Gate` 及四条准则标题（`## Think Before Coding` / `## Simplicity First` / `## Surgical Changes` / `## Goal-Driven Execution`）
- `audit/security-<rid>.md` → `## Verdict`、`## Findings`
- `audit/perf-<rid>.md` → `## Results`（或 `## Baseline`；无性能面则正文写 `N/A — no perf surface`）
- `prd/handoff-<rid>.md` → frontmatter 需 `schemaVersion: 2` + `requestId` / `scope` / `files` / `handoffPath` / `handoffHash` / `decisions` / `risks` / `nextActions` / `gateEvidence`
- `qa/test-reports/<rid>.md` → `## Test execution`、`## Verdict`
- `qa/test-cases/<rid>.md` → `## Test cases`（**但该工件的另一条检查要求 JS 惯用式 `test(` / `it(`，Go 项目结构上不可能出现 —— 实测同一形态的 S1 通过、S2 被拒，不可依赖 `## Test cases` 单独通过，需 `--allow-incomplete --reason`**）
- `rd/requests/001-<rid>.md` 与 `qa/requests/001-<rid>.md` → 必须填满模板，不得残留 `<...>` 占位符

**How to apply:** 把这些字面标题**直接写进子代理的派发提示词**（本项目 S2 起如此做后，RD 一次产出合规工件，而 S1 花了 6 轮补标题）。`--allow-incomplete` 需配 `--reason`，理由会被记入工件，故理由要写成事实陈述。

另注意 `FILE_SIZE_VIOLATION`（阈值 800 行）是对**被改动文件**的检查：既有超限的文件一旦被触碰就会触发，与本次新增行数无关。本项目 `model/main.go` 即为此情形（HEAD 已 830 行）。
