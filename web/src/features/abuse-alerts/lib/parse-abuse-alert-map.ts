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
import type {
  AbuseAlertLogRetention,
  AbuseAlertMap,
  AbuseAlertRule,
  AbuseAlertRuleSwitch,
  AbuseAlertRuleThresholds,
} from '../types'

const ABUSE_ALERT_MAP_OPTION_KEY = 'AbuseAlertMap'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function toNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.filter((item): item is string => typeof item === 'string')
}

/**
 * 解析一条规则的开关。
 *
 * 缺失 `switch`（旧后端）或 `switch === null` ⇒ null，页面不渲染开关，其余照旧。
 * `optionKey` 由后端下发，前端不按命名约定拼接。
 */
function toRuleSwitch(raw: unknown): AbuseAlertRuleSwitch | null {
  if (!isRecord(raw)) return null
  if (typeof raw.option_key !== 'string') return null

  return {
    optionKey: raw.option_key,
    enabled: raw.enabled === true,
    source: raw.source === 'setting' ? 'setting' : 'default',
  }
}

function toRuleThresholds(raw: unknown): AbuseAlertRuleThresholds | null {
  if (!isRecord(raw)) return null

  const thresholds: AbuseAlertRuleThresholds = {
    ratio: toNumber(raw.ratio),
    minConsumeQuota: toNumber(raw.min_consume_quota),
    minRequests: toNumber(raw.min_requests),
    minErrors: toNumber(raw.min_errors),
    lookbackHours: toNumber(raw.lookback_hours),
  }
  const hasAny = Object.values(thresholds).some((value) => value !== undefined)
  return hasAny ? thresholds : null
}

function toRule(raw: unknown): AbuseAlertRule | null {
  if (!isRecord(raw)) return null
  if (typeof raw.id !== 'string' || typeof raw.titleKey !== 'string') return null

  const computable = raw.computable === true
  return {
    id: raw.id,
    titleKey: raw.titleKey,
    computable,
    severity: typeof raw.severity === 'string' ? raw.severity : '',
    reasonKey: typeof raw.reasonKey === 'string' ? raw.reasonKey : '',
    // 不可计算的规则**必须**没有开关：即使后端误下发了一个，也不渲染控件。
    switch: computable ? toRuleSwitch(raw.switch) : null,
    thresholds: toRuleThresholds(raw.thresholds),
    dependsOn: toStringArray(raw.depends_on),
    source: typeof raw.source === 'string' ? raw.source : '',
  }
}

function toLogRetention(raw: unknown): AbuseAlertLogRetention {
  if (!isRecord(raw)) {
    return {
      retentionDays: 0,
      requiredMinutes: 0,
      requiredHours: 0,
      sufficient: true,
    }
  }
  return {
    retentionDays: toNumber(raw.retention_days) ?? 0,
    requiredMinutes: toNumber(raw.required_minutes) ?? 0,
    requiredHours: toNumber(raw.required_hours) ?? 0,
    sufficient: raw.sufficient !== false,
  }
}

/**
 * 从 `GET /api/option/` 的返回里取出异常用量告警的规则映射表。
 *
 * 只认伪键 `AbuseAlertMap`。不要读裸键 `abuse_alert_setting.*`：这些键未被显式保存时
 * 的值是字面量字符串 `"null"`，按缺省类型解析会把 `"null"` 变成 `false` / `0`，
 * 于是默认开的规则会在页面上显示成关闭。
 *
 * 返回 `null` 表示后端没有下发这个键（旧后端）或内容不合法 —— 此时页面必须显示
 * 错误态，而不是渲染一个可能不真实的开关。
 */
export function parseAbuseAlertMap(
  options: Array<{ key: string; value: string }> | undefined
): AbuseAlertMap | null {
  const raw = options?.find((option) => option.key === ABUSE_ALERT_MAP_OPTION_KEY)
  if (!raw) return null

  let parsed: unknown
  try {
    parsed = JSON.parse(raw.value)
  } catch {
    return null
  }
  if (!isRecord(parsed) || !Array.isArray(parsed.rules)) return null

  return {
    enabled: parsed.enabled === true,
    notifyEnabled: parsed.notify_enabled !== false,
    generatedAt: toNumber(parsed.generated_at) ?? 0,
    baselineReady: parsed.baseline_ready === true,
    baselineReadyAt: toNumber(parsed.baseline_ready_at) ?? 0,
    logRetention: toLogRetention(parsed.log_retention),
    rules: parsed.rules
      .map(toRule)
      .filter((rule): rule is AbuseAlertRule => rule !== null),
  }
}
