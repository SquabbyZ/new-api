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
import type { ErrorLogEntry, ErrorLogGate, ErrorLogMap } from '../types'

const ERROR_LOG_MAP_OPTION_KEY = 'ErrorLogMap'

const ERROR_LOG_GATES = new Set<string>([
  'global_switch',
  'per_error_flag',
  'before_record_point',
  'hardcoded_no_record',
  'local_error',
  'other_switch',
])

function isGate(value: string): value is ErrorLogGate {
  return ERROR_LOG_GATES.has(value)
}

function toEntry(raw: unknown): ErrorLogEntry | null {
  if (typeof raw !== 'object' || raw === null) return null

  const entry = raw as Record<string, unknown>
  if (typeof entry.id !== 'string' || typeof entry.titleKey !== 'string') {
    return null
  }

  return {
    id: entry.id,
    titleKey: entry.titleKey,
    recorded: entry.recorded === true,
    gates: Array.isArray(entry.gates)
      ? entry.gates.filter(
          (gate): gate is ErrorLogGate => typeof gate === 'string' && isGate(gate)
        )
      : [],
    reasonKey: typeof entry.reasonKey === 'string' ? entry.reasonKey : '',
    source: typeof entry.source === 'string' ? entry.source : '',
  }
}

/**
 * 从 `GET /api/option/` 的返回里取出错误日志映射表。
 *
 * 只认伪键 `ErrorLogMap`。不要读裸键 `error_log_setting.enabled`：该键未被显式保存时
 * 的值是字面量字符串 `"null"`，按 bool 解析会得到 `false`，于是 `ERROR_LOG_ENABLED=true`
 * 的部署会在页面上显示成关闭。
 *
 * 返回 `null` 表示后端没有下发这个键（旧后端）或内容不合法。此时页面必须展示错误态，
 * 而不是渲染一个可能不真实的开关。
 */
export function parseErrorLogMap(
  options: Array<{ key: string; value: string }> | undefined
): ErrorLogMap | null {
  const raw = options?.find((option) => option.key === ERROR_LOG_MAP_OPTION_KEY)
  if (!raw) return null

  let parsed: unknown
  try {
    parsed = JSON.parse(raw.value)
  } catch {
    return null
  }
  if (typeof parsed !== 'object' || parsed === null) return null

  const payload = parsed as Record<string, unknown>
  if (typeof payload.enabled !== 'boolean' || !Array.isArray(payload.entries)) {
    return null
  }

  return {
    enabled: payload.enabled,
    enabledSource: payload.enabled_source === 'setting' ? 'setting' : 'env',
    envVar: typeof payload.env_var === 'string' ? payload.env_var : '',
    entries: payload.entries
      .map(toEntry)
      .filter((entry): entry is ErrorLogEntry => entry !== null),
  }
}
