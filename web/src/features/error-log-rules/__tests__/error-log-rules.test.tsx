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
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import i18n, { createInstance } from 'i18next'
import { I18nextProvider, initReactI18next } from 'react-i18next'
import { describe, expect, test, vi } from 'vitest'

import { ErrorLogRulesTable } from '../components/error-log-rules-table'
import { ErrorLogRulesSection } from '../index'
import { parseErrorLogMap } from '../lib/parse-error-log-map'
import type { ErrorLogEntry } from '../types'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const mocks = vi.hoisted(() => ({
  optionsQuery: {
    data: { data: [] } as { data: Array<{ key: string; value: string }> },
    isPending: false,
    isError: false,
    refetch: vi.fn(),
  },
  updateOption: {
    isPending: false,
    mutate: vi.fn(),
    variables: undefined as { key?: string } | undefined,
  },
}))

vi.mock('@/features/system-settings/hooks/use-system-options', () => ({
  useSystemOptions: () => mocks.optionsQuery,
}))
vi.mock('@/features/system-settings/hooks/use-update-option', () => ({
  useUpdateOption: () => mocks.updateOption,
}))

void i18n.use(initReactI18next).init({
  lng: 'en',
  resources: { en: { translation: {} } },
  fallbackLng: 'en',
  interpolation: { escapeValue: false },
})

const payload = {
  enabled: true,
  enabled_source: 'env',
  env_var: 'ERROR_LOG_ENABLED',
  entries: [
    {
      id: 'relay_upstream_error',
      titleKey:
        'Upstream channel error (after a channel is selected), including failures without a specific category',
      recorded: true,
      category_switch: {
        category: 'relay_upstream_error',
        option_key: 'error_log_setting.record_relay_upstream_error',
        enabled: true,
        source: 'default',
      },
      gates: ['global_switch', 'category_switch'],
      reasonKey: '',
      source: 'controller/relay.go:209',
    },
    {
      id: 'insufficient_wallet_quota',
      titleKey: 'Insufficient wallet quota or pre-consume failure',
      recorded: false,
      category_switch: {
        category: 'insufficient_wallet_quota',
        option_key: 'error_log_setting.record_insufficient_wallet_quota',
        enabled: false,
        source: 'setting',
      },
      gates: ['global_switch', 'category_switch'],
      reasonKey:
        'Insufficient quota is a normal business outcome, not a fault. Enabling it may flood the error log with routine failures.',
      source: 'service/billing_session.go:237,396,402',
    },
    {
      id: 'no_available_channel',
      titleKey: 'No available channel in the group',
      recorded: false,
      category_switch: null,
      gates: ['before_record_point'],
      reasonKey:
        'This failure returns before the record point, so no error log is written.',
      source: 'controller/relay.go:163-168 (break at :167)',
    },
  ],
}

function parsePayload(raw: unknown) {
  return parseErrorLogMap([{ key: 'ErrorLogMap', value: JSON.stringify(raw) }])
}

function renderTable(entries: ErrorLogEntry[]) {
  const onCategoryChange = vi.fn()
  render(
    <ErrorLogRulesTable entries={entries} onCategoryChange={onCategoryChange} />
  )
  return onCategoryChange
}

describe('parseErrorLogMap', () => {
  test('reads the effective value from the ErrorLogMap pseudo key', () => {
    const parsed = parseErrorLogMap([
      { key: 'ErrorLogMap', value: JSON.stringify(payload) },
    ])

    expect(parsed).not.toBeNull()
    expect(parsed?.enabled).toBe(true)
    expect(parsed?.enabledSource).toBe('env')
    expect(parsed?.envVar).toBe('ERROR_LOG_ENABLED')
    expect(parsed?.entries).toHaveLength(3)
  })

  test('reads each category switch from the backend payload', () => {
    const parsed = parsePayload(payload)

    expect(parsed?.entries[0].categorySwitch).toEqual({
      category: 'relay_upstream_error',
      optionKey: 'error_log_setting.record_relay_upstream_error',
      enabled: true,
      source: 'default',
    })
    expect(parsed?.entries[1].categorySwitch?.source).toBe('setting')
    expect(parsed?.entries[1].categorySwitch?.enabled).toBe(false)
  })

  test('treats a missing category_switch as not toggleable, for older backends', () => {
    const [first] = payload.entries
    const { category_switch: _omitted, ...withoutSwitch } = first
    const parsed = parsePayload({ ...payload, entries: [withoutSwitch] })

    expect(parsed?.entries[0].categorySwitch).toBeNull()
  })

  test('keeps the switch on when the raw option key is the literal string null', () => {
    const parsed = parseErrorLogMap([
      { key: 'error_log_setting.enabled', value: 'null' },
      { key: 'ErrorLogMap', value: JSON.stringify(payload) },
    ])

    expect(parsed?.enabled).toBe(true)
  })

  test('returns null when the backend does not send the key', () => {
    expect(
      parseErrorLogMap([{ key: 'LogConsumeEnabled', value: 'true' }])
    ).toBeNull()
    expect(parseErrorLogMap(undefined)).toBeNull()
  })

  test('returns null when the payload is not valid JSON', () => {
    expect(
      parseErrorLogMap([{ key: 'ErrorLogMap', value: 'not json' }])
    ).toBeNull()
  })

  test('returns null when the payload omits the effective value', () => {
    expect(
      parseErrorLogMap([
        { key: 'ErrorLogMap', value: JSON.stringify({ entries: [] }) },
      ])
    ).toBeNull()
  })

  test('drops unknown gates instead of rendering an untranslatable token', () => {
    const parsed = parseErrorLogMap([
      {
        key: 'ErrorLogMap',
        value: JSON.stringify({
          ...payload,
          entries: [
            { ...payload.entries[0], gates: ['global_switch', 'future_gate'] },
          ],
        }),
      },
    ])

    expect(parsed?.entries[0].gates).toEqual(['global_switch'])
  })
})

describe('ErrorLogRulesTable', () => {
  test('renders a switch only for the rows the backend marks as toggleable', () => {
    const parsed = parsePayload(payload)
    expect(parsed).not.toBeNull()

    renderTable(parsed?.entries ?? [])

    expect(
      screen.getByText(
        'Upstream channel error (after a channel is selected), including failures without a specific category'
      )
    ).toBeInTheDocument()
    expect(
      screen.getByText('No available channel in the group')
    ).toBeInTheDocument()
    // 3 行里只有 2 行有开关；不可切换的那行不渲染任何控件（包括灰掉的开关）。
    expect(screen.getAllByRole('switch')).toHaveLength(2)
    expect(screen.queryAllByRole('checkbox')).toHaveLength(0)
  })

  test('shows the saved setting, not the composed value, on each switch', () => {
    const parsed = parsePayload(payload)
    const entries = parsed?.entries ?? []

    renderTable(entries)

    const switches = screen.getAllByRole('switch')
    expect(switches[0]).toBeChecked()
    expect(switches[1]).not.toBeChecked()
    // 「会不会记录」列显示的是合成值（全局 ∧ 该类），与开关的设置值是两回事。
    expect(screen.getAllByText('No')).toHaveLength(2)
    expect(screen.getAllByText('Yes')).toHaveLength(1)
  })

  test('writes the option key the backend sent, never a locally derived one', async () => {
    const user = userEvent.setup()
    const parsed = parsePayload(payload)
    const onCategoryChange = renderTable(parsed?.entries ?? [])

    await user.click(screen.getAllByRole('switch')[1])

    expect(onCategoryChange).toHaveBeenCalledWith(
      'error_log_setting.record_insufficient_wallet_quota',
      true
    )
  })

  test('disables only the row whose switch is being saved', () => {
    const parsed = parsePayload(payload)
    const entries = parsed?.entries ?? []

    render(
      <ErrorLogRulesTable
        entries={entries}
        pendingOptionKey='error_log_setting.record_insufficient_wallet_quota'
        onCategoryChange={vi.fn()}
      />
    )

    // Base UI 的 Switch 不是原生 input，禁用态落在 aria-disabled 上。
    const switches = screen.getAllByRole('switch')
    expect(switches[1]).toHaveAttribute('aria-disabled', 'true')
    expect(switches[0]).not.toHaveAttribute('aria-disabled', 'true')
  })
})

/**
 * 后端形状的转写：`setting/operation_setting/error_log_setting.go` 的
 * `ErrorLogEntries()` 下发 11 条，其中 5 条带 `category_switch`（可切换），
 * 6 条没有（结构上不可切换）。只转写与「几条能切换」有关的字段。
 */
function productionEntry(id: string, titleKey: string, toggleable: boolean) {
  return {
    id,
    titleKey,
    category_switch: toggleable
      ? {
          category: id,
          option_key: `error_log_setting.record_${id}`,
          enabled: false,
          source: 'default',
        }
      : null,
  }
}

const productionShape = {
  enabled: true,
  enabled_source: 'env',
  env_var: 'ERROR_LOG_ENABLED',
  entries: [
    productionEntry(
      'relay_upstream_error',
      'Upstream channel error (after a channel is selected), including failures without a specific category',
      true
    ),
    productionEntry(
      'task_upstream_error',
      'Async task submission upstream error',
      true
    ),
    productionEntry(
      'insufficient_wallet_quota',
      'Insufficient wallet quota or pre-consume failure',
      true
    ),
    productionEntry(
      'insufficient_subscription_quota',
      'Insufficient subscription quota',
      true
    ),
    productionEntry(
      'token_preconsume_failed',
      'Token pre-consume failure',
      true
    ),
    productionEntry(
      'no_available_channel',
      'No available channel in the group',
      false
    ),
    productionEntry(
      'tiered_billing_prepare_failed',
      'Tiered billing preparation failure',
      false
    ),
    productionEntry(
      'request_body_read_failed',
      'Request body read failure or oversized body',
      false
    ),
    productionEntry(
      'task_local_error',
      'Task submission local validation error',
      false
    ),
    productionEntry(
      'responses_ws_dispatch_error',
      'Responses WebSocket internal dispatch failure',
      false
    ),
    productionEntry(
      'other_log_types',
      'Other log types (consume / login / audit / top-up / task billing)',
      false
    ),
  ],
}

const COPY_KEY =
  'Five categories can be toggled individually below. The other six cannot, and each row says why.'

const COPY_PATTERN =
  /^([A-Za-z]+) categories can be toggled individually below\. The other ([A-Za-z]+) cannot, and each row says why\.$/

const NUMBER_WORDS = [
  'zero',
  'one',
  'two',
  'three',
  'four',
  'five',
  'six',
  'seven',
  'eight',
  'nine',
  'ten',
  'eleven',
  'twelve',
]

/** 把渲染出来的文案里写的数量读回来，好和下发数据推导出的数量对照。 */
function describedCounts(copy: string) {
  const match = COPY_PATTERN.exec(copy.trim())
  if (match === null) return null
  return {
    toggleable: NUMBER_WORDS.indexOf(match[1].toLowerCase()),
    fixed: NUMBER_WORDS.indexOf(match[2].toLowerCase()),
  }
}

function entryCounts(entries: ErrorLogEntry[]) {
  const toggleable = entries.filter(
    (entry) => entry.categorySwitch !== null
  ).length
  return { toggleable, fixed: entries.length - toggleable }
}

function useProductionShape() {
  mocks.optionsQuery.data = {
    data: [{ key: 'ErrorLogMap', value: JSON.stringify(productionShape) }],
  }
}

const LOCALES = ['en', 'zh', 'zh-TW', 'fr', 'ru', 'ja', 'vi']

function loadLocale(locale: string) {
  return JSON.parse(
    fs.readFileSync(
      path.resolve(
        __dirname,
        '..',
        '..',
        '..',
        'i18n',
        'locales',
        `${locale}.json`
      ),
      'utf8'
    )
  ) as { translation: Record<string, string> }
}

describe('ErrorLogRulesSection copy', () => {
  test('states the counts the backend actually sends', () => {
    useProductionShape()
    const entries = parsePayload(productionShape)?.entries ?? []

    render(<ErrorLogRulesSection />)

    expect(entries).toHaveLength(11)
    const copy = screen.getByText(
      /categories can be toggled individually below/
    ).textContent

    // 页面说的数量必须是下发数据推导出来的数量，而不是写死在心里的数字。
    expect(describedCounts(copy ?? '')).toEqual(entryCounts(entries))
    expect(entryCounts(entries)).toEqual({ toggleable: 5, fixed: 6 })
  })

  test.each(LOCALES)(
    'renders the %s translation of the copy, not the English fallback',
    async (locale) => {
      const resources = loadLocale(locale)
      const instance = createInstance()
      await instance.init({
        lng: locale,
        fallbackLng: 'en',
        resources: { [locale]: resources },
        interpolation: { escapeValue: false },
      })
      useProductionShape()

      render(
        <I18nextProvider i18n={instance}>
          <ErrorLogRulesSection />
        </I18nextProvider>
      )

      const translated = resources.translation[COPY_KEY]
      expect(translated).toBeTruthy()
      expect(screen.getByText(translated)).toBeInTheDocument()
      // 非英文 locale 若渲染的是英文原文，就说明这条译文根本没被请求到。
      if (locale !== 'en') expect(translated).not.toBe(COPY_KEY)
    }
  )
})
