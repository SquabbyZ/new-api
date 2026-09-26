---
name: peaks-runtime-not-gitignored-in-this-project
description: "本项目 .peaks/_runtime 与 .codegraph/config.json 均未被 gitignore，与 peaks-code SKILL 声称不符；peaks workspace init 会修改根 .gitignore。"
metadata:
  type: lesson
  sourceArtifact: .peaks/_runtime/2026-09-26-session-f5e6af/txt/handoff.md
---

本项目没有忽略 peaks 的运行时目录。评审产物、handoff 与 RD 工件都会以未跟踪文件出现在 git status，一次 git add 就会把它们提交进去。

`peaks workspace init` 写入的 peaks-loop 托管片段只忽略 `.claude/settings.local.json`、`.peaks/.claude-settings-template.json`、`.codegraph/config.json.bak`。实测 `git check-ignore` 判定 `.peaks/_runtime/<sessionId>/**` 与 `.codegraph/config.json` **均不被忽略**，`.peaks/` 下也没有自己的 `.gitignore`。

但 peaks-code 的 SKILL.md 声称「ALL reviewable artifact dirs live under `.peaks/_runtime/<sessionId>/<role>/...` (gitignored)」。在本项目这不成立 —— 评审产物、handoff、RD/QA 工件都会以未跟踪文件出现在 `git status`，`git add .` 会把它们提交进去。

**How to apply:** 在本项目沉淀 peaks 工件后，提交前检查 `git status` 是否出现 `.peaks/_runtime/`。若要长期使用 peaks 流程，向根 `.gitignore` 补一条 `.peaks/_runtime/`（注意保留 peaks 托管片段的注释边界，不要手改该片段内部）。另注意 `peaks workspace init` 会修改受版本控制的根 `.gitignore`，这属于该命令的正常副作用。
