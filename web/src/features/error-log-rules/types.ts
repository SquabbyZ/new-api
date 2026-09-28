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
  | 'category_switch'
  | 'before_record_point'
  | 'local_error'
  | 'other_switch'

export const GATE_LABEL_KEYS: Record<ErrorLogGate, string> = {
  global_switch: 'Error log switch is on',
  category_switch: 'This category is enabled',
  before_record_point: 'The failure returns before the record point',
  local_error: 'Local validation error',
  other_switch: 'Handled by another log switch',
}

// 一个类别的采集开关。`enabled` 是该类自己的设置值（不是与总开关的合成值），
// 所以总开关关掉时页面上仍然看得见管理员保存过什么。
export type ErrorLogCategorySwitch = {
  category: string
  optionKey: string
  enabled: boolean
  source: 'default' | 'setting'
}

export type ErrorLogEntry = {
  id: string
  titleKey: string
  // 当前是否真的会记录：全局开关 ∧ 该类开关。只用于「会不会记录」列。
  recorded: boolean
  gates: ErrorLogGate[]
  // 可切换的类别：该类为什么默认关 / 开启它要知道什么。
  // 不可切换的类别：它为什么无法切换。
  reasonKey: string
  source: string
  // null 表示这一类结构上不可切换，页面不渲染任何控件（不是渲染灰掉的开关）。
  categorySwitch: ErrorLogCategorySwitch | null
}

// enabledSource 说明当前生效值从哪来：管理员保存过设置，还是仍在跟随环境变量。
export type ErrorLogSource = 'env' | 'setting'

export type ErrorLogMap = {
  enabled: boolean
  enabledSource: ErrorLogSource
  envVar: string
  entries: ErrorLogEntry[]
}
