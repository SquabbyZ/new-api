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

// 判定机制的稳定标识，与后端 setting/operation_setting/error_log_setting.go 的
// ErrorLogGate* 常量一一对应。前端只提供这些标识的文案，不维护任何条目清单 ——
// 条目集合与它们的 titleKey / reasonKey 全部由后端下发。
export type ErrorLogGate =
  | 'global_switch'
  | 'per_error_flag'
  | 'before_record_point'
  | 'hardcoded_no_record'
  | 'local_error'
  | 'other_switch'

export const GATE_LABEL_KEYS: Record<ErrorLogGate, string> = {
  global_switch: 'Error log switch is on',
  per_error_flag: 'The error is not marked as unrecordable',
  before_record_point: 'The failure returns before the record point',
  hardcoded_no_record: 'Hardcoded as not recorded',
  local_error: 'Local validation error',
  other_switch: 'Handled by another log switch',
}

export type ErrorLogEntry = {
  id: string
  titleKey: string
  recorded: boolean
  gates: ErrorLogGate[]
  reasonKey: string
  source: string
}

// enabledSource 说明当前生效值从哪来：管理员保存过设置，还是仍在跟随环境变量。
export type ErrorLogSource = 'env' | 'setting'

export type ErrorLogMap = {
  enabled: boolean
  enabledSource: ErrorLogSource
  envVar: string
  entries: ErrorLogEntry[]
}
