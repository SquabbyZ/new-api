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
  AbuseAlertEvidence,
  AbuseAlertFinding,
  AbuseAlertReport,
  AbuseAlertSkip,
} from '../types'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function toNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

function toText(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function toEvidence(raw: unknown): AbuseAlertEvidence {
  if (!isRecord(raw)) {
    return { current: 0, baselineMedian: 0, ratio: 0, direction: 'up' }
  }
  return {
    current: toNumber(raw.current),
    baselineMedian: toNumber(raw.baseline_median),
    ratio: toNumber(raw.ratio),
    direction: toText(raw.direction) || 'up',
  }
}

function toFinding(raw: unknown): AbuseAlertFinding | null {
  if (!isRecord(raw)) return null
  if (typeof raw.rule !== 'string' || typeof raw.token_id !== 'number') return null

  return {
    rule: raw.rule,
    severity: toText(raw.severity),
    tokenId: raw.token_id,
    tokenName: toText(raw.token_name),
    userId: toNumber(raw.user_id),
    username: toText(raw.username),
    modelName: typeof raw.model_name === 'string' ? raw.model_name : null,
    windowStart: toNumber(raw.window_start),
    windowEnd: toNumber(raw.window_end),
    evidence: toEvidence(raw.evidence),
  }
}

function toSkip(raw: unknown): AbuseAlertSkip | null {
  if (!isRecord(raw)) return null
  if (typeof raw.rule !== 'string' || typeof raw.token_id !== 'number') return null
  return {
    rule: raw.rule,
    tokenId: raw.token_id,
    reason: toText(raw.reason),
  }
}

/**
 * 解析只读发现端点的返回。
 *
 * 增量刷新时这一条必须能区分「后端返回了空 findings」与「后端没返回」：
 * 前者是「检测过了，没有发现」，后者是加载失败。所以这里对缺失的数组
 * 返回空数组、对整个载荷不合法返回 null，由调用方决定显示哪一种状态。
 */
export function parseAbuseAlertReport(raw: unknown): AbuseAlertReport | null {
  if (!isRecord(raw)) return null
  if (!Array.isArray(raw.findings) || !Array.isArray(raw.skipped)) return null

  return {
    hours: toNumber(raw.hours),
    windowMinutes: toNumber(raw.window_minutes),
    generatedAt: toNumber(raw.generated_at),
    baselineReady: raw.baseline_ready === true,
    baselineReadyAt: toNumber(raw.baseline_ready_at),
    lastScheduledScanAt: toNumber(raw.last_scheduled_scan_at),
    notifyChannelReady: raw.notify_channel_ready === true,
    logRetention: isRecord(raw.log_retention)
      ? {
          retentionDays: toNumber(raw.log_retention.retention_days),
          requiredMinutes: toNumber(raw.log_retention.required_minutes),
          requiredHours: toNumber(raw.log_retention.required_hours),
          sufficient: raw.log_retention.sufficient !== false,
        }
      : {
          retentionDays: 0,
          requiredMinutes: 0,
          requiredHours: 0,
          sufficient: true,
        },
    findings: raw.findings
      .map(toFinding)
      .filter((finding): finding is AbuseAlertFinding => finding !== null),
    skipped: raw.skipped
      .map(toSkip)
      .filter((skip): skip is AbuseAlertSkip => skip !== null),
    excludedSaturatedRows: toNumber(raw.excluded_saturated_rows),
  }
}
