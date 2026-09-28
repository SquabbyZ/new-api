/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/

// 规则清单**全部**来自后端下发的伪键 AbuseAlertMap。前端不维护任何规则 id 清单、
// 不按命名约定拼 option key —— 那是第二份可漂移的清单，与 error-log-rules 的处理一致。

export type AbuseAlertSwitchSource = 'default' | 'setting'

// 一条规则的开关。`enabled` 是该规则自己的设置值（不是与总开关的合成值），
// 所以总开关关掉时页面上仍然看得见管理员保存过什么。
export type AbuseAlertRuleSwitch = {
  optionKey: string
  enabled: boolean
  source: AbuseAlertSwitchSource
}

export type AbuseAlertRuleThresholds = {
  ratio?: number
  minConsumeQuota?: number
  minRequests?: number
  minErrors?: number
  lookbackHours?: number
}

export type AbuseAlertRule = {
  id: string
  titleKey: string
  // false 表示这条信号在当前数据里算不出来。这类行**不渲染任何控件**，
  // 只显示 reasonKey —— 渲染一个灰掉的开关会被读成「以后能用」。
  computable: boolean
  severity: string
  reasonKey: string
  switch: AbuseAlertRuleSwitch | null
  thresholds: AbuseAlertRuleThresholds | null
  dependsOn: string[]
  source: string
}

export type AbuseAlertLogRetention = {
  retentionDays: number
  requiredMinutes: number
  requiredHours: number
  sufficient: boolean
}

export type AbuseAlertMap = {
  enabled: boolean
  notifyEnabled: boolean
  generatedAt: number
  baselineReady: boolean
  baselineReadyAt: number
  logRetention: AbuseAlertLogRetention
  rules: AbuseAlertRule[]
}

export type AbuseAlertEvidence = {
  current: number
  baselineMedian: number
  ratio: number
  direction: string
}

export type AbuseAlertFinding = {
  rule: string
  severity: string
  tokenId: number
  // tokenName / username 为空表示令牌已被删除。该行仍然显示并标注，不隐藏 ——
  // 丢掉一条发现等于静默隐藏盗刷。
  tokenName: string
  userId: number
  username: string
  modelName: string | null
  windowStart: number
  windowEnd: number
  evidence: AbuseAlertEvidence
}

export type AbuseAlertSkip = {
  rule: string
  tokenId: number
  reason: string
}

export type AbuseAlertReport = {
  // 后端**实际使用**的回看小时数（hours 超上限时是钳制后的值）与窗口长度，
  // 页面据此显示真正被扫描的范围 —— 否则运维者会以为看的是他请求的那个范围。
  hours: number
  windowMinutes: number
  generatedAt: number
  baselineReady: boolean
  baselineReadyAt: number
  // 最近一次**计划任务**扫描的完成时间（0 = 从未扫描过）。有了它，页面才能把
  // 「没有通知」区分成「没触发」「触发了但没发出去」「根本没扫」。
  lastScheduledScanAt: number
  notifyChannelReady: boolean
  logRetention: AbuseAlertLogRetention
  findings: AbuseAlertFinding[]
  skipped: AbuseAlertSkip[]
  excludedSaturatedRows: number
}
