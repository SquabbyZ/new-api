---
schemaVersion: 1
capturedAt: 2026-09-26T16:22:00.000Z
archetype: full-stack
confidence: high
techStack:
  backend: Go 1.25.1 + Gin + GORM v2
  frontend: React 19 + TypeScript + Rsbuild 2 + TanStack (Router/Query/Table) + Zustand + Base UI + Tailwind CSS 4
  databases: SQLite / MySQL >= 5.7.8 / PostgreSQL >= 9.6 (主库); + ClickHouse (日志库)
  cache: Redis (go-redis) + 进程内缓存
  packageManager: bun (frontend)
architecture: 模块垂直切分的双库网关 —— 事务域走主库 DB，可观测明细域走日志库 LOG_DB
karpathySelfCheck:
  thinkBeforeCoding: 见 .peaks/standards/common/coding-style.md
  simplicityFirst: 见 .peaks/standards/common/coding-style.md
  surgicalChanges: 见 .peaks/standards/common/code-review.md
  goalDrivenExecution: 见 .peaks/standards/common/code-review.md
---

# Project Scan

> 本文件由父会话**手工修正**于 2026-09-26。`peaks workspace init` 自动生成的版本是 0-1 bootstrap 占位，内容错误（见文末「扫描器缺陷」）。手工修正的理由与证据记录在下。

## Archetype

| Field | Value |
|---|---|
| Type | `full-stack` |
| Confidence | `high` |
| CLI 原判定 | `unknown` / `frontendOnly: true` / `integrationMode: prd-only` |

## Project mode

| Field | Value |
|---|---|
| Integration mode | `full-stack` |
| Integration mode reason | 本仓库内含完整后端（Go + Gin，`controller/` `service/` `model/` `router/` `relay/` 共 1012 个 .go 文件） |
| Frontend-only | `false` |
| Reason | 后端在仓库内，且另有前端 `web/`（1346 个 ts/tsx） |

**为何与 CLI 输出不一致**：`peaks scan archetype` 实测返回 `frontendOnly: true` / `integrationMode: prd-only`，依据是 `no-backend-no-swagger`。该判定对本仓库不成立，见文末「扫描器缺陷」。父会话已按证据覆盖，并在 `.agents` 记忆 `storage-module-vertical-split` 与 `peaks-runtime-not-gitignored-in-this-project` 中留档。

## Tech stack

| Concern | Value |
|---|---|
| Language | Go 1.25.1（后端，含独立模块 `relaykit/`）；TypeScript（前端 `web/`） |
| Package manager | bun（前端，`web/`）；Go modules（后端） |
| Node runtime | 由 `web/` 决定（见 `web/package.json`） |
| Build | `go build ./...`（根模块）；`cd relaykit && GOWORK=off go build ./...`（独立模块）；`bun run build`（前端） |
| Tests | `go test ./...`；前端 vitest（见 `web/`） |

## Library versions

| Package | Pinned range | Major | Scope | Ecosystem |
|---|---|---|---|---|
| gorm.io/gorm | v1.25.12 | v1 | 后端 | Go |
| gorm.io/driver/mysql | v1.5.7 | v1 | 后端 | Go |
| gorm.io/driver/postgres | v1.5.9 | v1 | 后端 | Go |
| gorm.io/driver/clickhouse | v0.6.0 | v0 | 后端（日志库） | Go |
| github.com/glebarez/sqlite | v1.11.0 | v1 | 后端 | Go |
| github.com/ClickHouse/clickhouse-go/v2 | v2.46.0 | v2 | 后端 | Go |

> 前端依赖见 `web/package.json`。`peaks scan libraries` 在仓库根找不到 `package.json` 因而返回空 —— 前端 manifest 在 `web/` 子目录。

## Architecture

模块垂直切分的**双库**网关，不是交叉使用同一份数据：

- **事务域 → 主库 `DB`**（`SQL_DSN`）：35 张表。身份（users / sessions / passkey / 2FA / oauth）、网关配置（channels / abilities / options / casbin / authz）、计费账本（quota / tokens / redemptions / subscription*）、任务（tasks / system_tasks / system_task_locks）。
- **可观测·明细域 → 日志库 `LOG_DB`**（`LOG_SQL_DSN`）：仅 `logs` 与 `audit_logs` 两张表。可指向 ClickHouse。未设 `LOG_SQL_DSN` 时 `LOG_DB = DB`（优雅降级）。
- **可观测·聚合域 → 主库**：`quota_data`（小时级看板聚合，由 `model/usedata.go` 刷写）。
- **监控域 → 主库**：`perf_metrics`（时间桶计数，`pkg/perf_metrics/` 的批量 flush 是正确的聚合先例）。
- **独立模块**：`relaykit/` 承载协议 DTO 与转换，MUST 可独立构建（`GOWORK=off`）。
- **JS 任务插件**：`plugins/tasks/`，经 `pkg/jsplugin/`（Sobek）执行。
- **多节点**：`NODE_TYPE != "slave"` 即 master（未设时**默认为 master**）；配置传播靠 60 秒轮询（`SYNC_FREQUENCY`）；热路径强一致靠 Redis。

**边界不变量**：没有任何一张表同时属于两个域；没有任何业务或计费判定读日志库（日志库是纯可观测数据，非事实来源）。跨库组合只在应用层做（两次查询 + 内存拼装），共 2 处：`model/log.go`（补渠道名）、`model/audit_log.go`（补用户角色）。

## Karpathy self-check

| Guideline | Where enforced |
|---|---|
| §1 Think Before Coding | `AGENTS.md` Rules / `.peaks/standards/common/coding-style.md` |
| §2 Simplicity First | `.peaks/standards/common/coding-style.md`（含「避免只被单处调用的包级 helper」） |
| §3 Surgical Changes | `.peaks/standards/common/code-review.md` |
| §4 Goal-Driven Execution | `.peaks/standards/common/code-review.md` |

## 扫描器缺陷（供后续 session 参考）

`peaks scan archetype`（peaks-loop 4.0.54，`dist/services/scan/archetype-service.js`）对本仓库结构性误判，根因是该检测器只认 JS 生态：

- `BACKEND_FRAMEWORK` 列表仅 express/koa/fastify/hapi/restify/next —— **不含 gin/Go**
- `BACKEND_DIR_CANDIDATES = ['server','backend','api','apps/server','apps/api','packages/server','packages/api']` —— 本仓库用 `controller/service/router/relay`
- `SWAGGER_CANDIDATE_PATHS` 含 `docs/openapi.json` 但**不含 `docs/openapi/api.json`**，而本仓库的 OpenAPI 规格正是后者
- `readPackageJsonDeps` 与 `countSrcFiles` 只在仓库根查找 —— `web/package.json` 与 `web/src/` 全部不可见

后果：`hasBackendFramework=false` + `hasSwaggerOrProto=false` → `noBackend && !hasSwaggerOrProto` → `frontendOnly: true` / `integrationMode: prd-only`。**对本仓库完全错误。**

缓解：本文件已按证据覆盖为 `full-stack`。若后续 session 重新执行 `peaks project context` 刷新扫描，会被重置回错误的占位内容 —— 届时需再次手工修正，或在上游修复检测器（把 gin 与 `docs/openapi/*.json` 纳入）。

## Refresh procedure

1. 不要直接依赖 `peaks project context` / `peaks workspace init` 的自动输出 —— 对本仓库它是错的。
2. 若要刷新，请以本文件的「Architecture」与「扫描器缺陷」两节为准做手工核对。
3. 上游修复检测器后，本节的缓解步骤可移除。
