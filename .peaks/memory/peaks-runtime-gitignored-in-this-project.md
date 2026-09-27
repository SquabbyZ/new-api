---
name: peaks-runtime-is-gitignored-in-this-project
description: "本项目现已忽略 .peaks/_runtime/ 与 .peaks/_sub_agents/，但 .peaks/memory/ 是跟踪的（沉淀要被提交）；另 peaks workspace init 会改动受版本控制的根 .gitignore。"
metadata:
  type: convention
---

`.peaks/` 下三类路径的忽略状态**各不相同**，提交前必须分清：

| 路径 | 忽略？ | 依据 | 含义 |
|---|---|---|---|
| `.peaks/_runtime/` | **是** | `.gitignore:52` | 评审产物 / handoff / RD / QA 工件不会进 git status |
| `.peaks/_sub_agents/` | **是** | `.gitignore:55` | 派发记录与心跳不进 git status |
| `.peaks/memory/` | **否**（跟踪） | 无忽略规则 | 沉淀**就是**要提交的，`MEMORY.md` / `index.json` / 各条 `.md` 都进版本控制 |

实测（`git check-ignore -v`）：`.peaks/_runtime/<sid>/rd/bug-analysis.md` 命中 `.gitignore:52`；`.peaks/_sub_agents/<sid>/active-dispatches.json` 命中 `.gitignore:55`；`.peaks/memory/MEMORY.md` 不被忽略。

**历史**（避免下次误判）：这两条忽略规则**不是** peaks-loop 托管的 —— 托管片段只管 `.claude/settings.local.json`、`.peaks/.claude-settings-template.json`、`.codegraph/config.json.bak`。曾经 `.peaks/_runtime/` 确实不被忽略（评审产物会以未跟踪文件出现在 git status，`git add .` 会把它们提交进去），本项目因此在 2026-09-26 的会话里手工补了 52 / 55 两行予以解决。所以若你看到某条旧记录说「本项目不忽略 peaks 运行时目录」，那说的是**补规则之前**的状态。

`peaks workspace init` 会**改动受版本控制的根 `.gitignore`**（它要写托管片段），这属于该命令的正常副作用，不是污染 —— 但 `git status` 里会因此出现 `.gitignore` 的改动，不要误当作自己的改动回滚掉。托管片段有 `>>> / <<<` 注释边界，**不要手改片段内部**；要持久化的自定义规则写在边界之外（52 / 55 两行就在边界之外）。

**How to apply:** 提交前跑 `git status --porcelain`。看到 `.peaks/memory/**` 是**预期**的（那是要提交的沉淀）；看到 `.peaks/_runtime/**` 或 `.peaks/_sub_agents/**` 则说明忽略规则被人删了或不生效，需要先查 `.gitignore` 再决定。区分「我的改动」与「工具链副作用」（`.gitignore` 被 `workspace init` 改、codegraph 产物）时，用 `git diff --stat` 逐文件确认，不要一刀切。
