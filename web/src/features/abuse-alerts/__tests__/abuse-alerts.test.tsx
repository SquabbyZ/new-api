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
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import i18n from 'i18next'
import { initReactI18next, I18nextProvider } from 'react-i18next'
import { beforeAll, describe, expect, test, vi } from 'vitest'

import { AbuseRulesTable } from '../components/abuse-rules-table'
import { AbuseAlertsSection } from '../index'
import { parseAbuseAlertMap } from '../lib/parse-abuse-alert-map'
import type { AbuseAlertRule } from '../types'

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
  // 默认永不 resolve：发现列表停在加载态，页面顶部那些从伪键来的提示
  // 因此可以被单独断言。需要发现列表时用 mockResolvedValue 换成一份报告。
  getAbuseAlerts: vi.fn((): Promise<unknown> => new Promise(() => {})),
}))

vi.mock('@/features/system-settings/hooks/use-system-options', () => ({
  useSystemOptions: () => mocks.optionsQuery,
}))
vi.mock('@/features/system-settings/hooks/use-update-option', () => ({
  useUpdateOption: () => mocks.updateOption,
}))
vi.mock('../api', () => ({
  getAbuseAlerts: mocks.getAbuseAlerts,
}))

void i18n.use(initReactI18next).init({
  lng: 'en',
  // 空资源 ⇒ key 原样返回（不插值），于是断言的字面量与英文源串一致。
  // 唯一例外是下面这条带占位符的文案：它必须真的把**后端报的** hours / minutes
  // 插进去，所以给它一条与 key 同形的翻译，让 i18next 走插值路径。
  resources: {
    en: {
      translation: {
        'Scanning the last {{hours}} hours in {{minutes}}-minute windows':
          'Scanning the last {{hours}} hours in {{minutes}}-minute windows',
      },
    },
  },
  fallbackLng: 'en',
  interpolation: { escapeValue: false },
})

beforeAll(() => {
  // i18next 在空资源下把 key 原样返回，因此断言的字面量与英文源串一致。
  expect(i18n.t('Consume rate spike')).toBe('Consume rate spike')
})

/** 后端下发的伪键形状（与 AbuseAlertMapPayload 一致）。 */
const productionShape = {
  enabled: true,
  notify_enabled: true,
  generated_at: 1790000000,
  baseline_ready: false,
  baseline_ready_at: 1790003600,
  log_retention: {
    retention_days: 3,
    required_minutes: 70,
    required_hours: 24,
    sufficient: false,
  },
  rules: [
    {
      id: 'consume_spike',
      titleKey: 'Consume rate spike',
      computable: true,
      severity: 'high',
      switch: {
        option_key: 'abuse_alert_setting.rule_consume_spike_enabled',
        enabled: true,
        source: 'default',
      },
      thresholds: { ratio: 5, min_consume_quota: 500000 },
      depends_on: ['logs.quota', 'logs.created_at'],
      source: 'model/log_alert.go',
    },
    {
      id: 'source_anomaly',
      titleKey: 'Source anomaly (new IP / ASN / region)',
      computable: false,
      reasonKey:
        'Not computable: request IP is stored only when the token owner enables the record_ip_log privacy setting.',
      // 后端刻意下发 null。即使它下发了别的形状前端也必须丢弃 —— 见下面对应用例。
      switch: null,
    },
    {
      id: 'concurrency_anomaly',
      titleKey: 'Concurrency anomaly (one token from many sources)',
      computable: false,
      reasonKey:
        'Not computable: the system keeps no per-token in-flight tracking.',
      switch: null,
    },
  ],
}

function parsePayload(raw: unknown) {
  return parseAbuseAlertMap([
    { key: 'AbuseAlertMap', value: JSON.stringify(raw) },
  ])
}

function useProductionShape() {
  mocks.optionsQuery.data = {
    data: [{ key: 'AbuseAlertMap', value: JSON.stringify(productionShape) }],
  }
  mocks.getAbuseAlerts.mockImplementation(() => new Promise(() => {}))
}

function renderSection() {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <I18nextProvider i18n={i18n}>
        <AbuseAlertsSection />
      </I18nextProvider>
    </QueryClientProvider>
  )
}

function renderRulesTable(rules: AbuseAlertRule[]) {
  const onRuleChange = vi.fn()
  render(
    <I18nextProvider i18n={i18n}>
      <AbuseRulesTable rules={rules} onRuleChange={onRuleChange} />
    </I18nextProvider>
  )
  return onRuleChange
}

describe('parseAbuseAlertMap', () => {
  test('reads the rules from the AbuseAlertMap pseudo key only', () => {
    const parsed = parsePayload(productionShape)

    expect(parsed).not.toBeNull()
    expect(parsed?.rules.map((rule) => rule.id)).toEqual([
      'consume_spike',
      'source_anomaly',
      'concurrency_anomaly',
    ])
  })

  test('keeps the option key the backend sent instead of rebuilding it', () => {
    const parsed = parsePayload(productionShape)

    expect(parsed?.rules[0].switch?.optionKey).toBe(
      'abuse_alert_setting.rule_consume_spike_enabled'
    )
  })

  test('returns null when the backend does not send the key', () => {
    expect(parseAbuseAlertMap([{ key: 'ErrorLogMap', value: '{}' }])).toBeNull()
    expect(parseAbuseAlertMap(undefined)).toBeNull()
  })

  test('returns null on a malformed payload instead of guessing', () => {
    expect(
      parseAbuseAlertMap([{ key: 'AbuseAlertMap', value: 'not json' }])
    ).toBeNull()
    expect(
      parseAbuseAlertMap([{ key: 'AbuseAlertMap', value: '{"rules":"nope"}' }])
    ).toBeNull()
  })

  test('drops the switch of a rule the backend marks as not computable', () => {
    const parsed = parsePayload({
      ...productionShape,
      rules: [
        {
          id: 'source_anomaly',
          titleKey: 'Source anomaly (new IP / ASN / region)',
          computable: false,
          switch: {
            option_key: 'abuse_alert_setting.source_anomaly',
            enabled: true,
          },
        },
      ],
    })

    // 后端误下发开关时也必须丢弃：渲染一个永远不会生效的控件比不渲染更糟。
    expect(parsed?.rules[0].switch).toBeNull()
  })
})

describe('AbuseRulesTable', () => {
  test('renders a switch for a computable rule and reports the backend key', async () => {
    const parsed = parsePayload(productionShape)
    const onRuleChange = renderRulesTable(
      (parsed?.rules ?? []).filter((rule) => rule.computable)
    )

    const toggles = screen.getAllByRole('switch')
    expect(toggles).toHaveLength(1)

    await userEvent.click(toggles[0])
    expect(onRuleChange).toHaveBeenCalledWith(
      'abuse_alert_setting.rule_consume_spike_enabled',
      false
    )
  })

  test('renders no control at all for a rule that cannot be computed', () => {
    const parsed = parsePayload(productionShape)
    const rules = (parsed?.rules ?? []).filter((rule) => !rule.computable)

    renderRulesTable(rules)

    // 不得出现 disabled switch —— 一个灰掉的开关会被读成「以后能用」。
    expect(screen.queryAllByRole('switch')).toHaveLength(0)
    expect(
      screen.getByText(/Not computable: request IP is stored only when/)
    ).toBeInTheDocument()
  })
})

describe('AbuseAlertsSection', () => {
  test('shows the error state and no controls when the backend does not send the map', () => {
    mocks.optionsQuery.data = { data: [] }
    renderSection()

    expect(
      screen.getByText('Failed to load the abuse alert rules')
    ).toBeInTheDocument()
    expect(screen.queryAllByRole('switch')).toHaveLength(0)
  })

  test('lists every rule the backend sent, including the uncomputable ones', () => {
    useProductionShape()
    renderSection()

    expect(screen.getByText('Consume rate spike')).toBeInTheDocument()
    expect(
      screen.getByText('Source anomaly (new IP / ASN / region)')
    ).toBeInTheDocument()
    expect(
      screen.getByText('Concurrency anomaly (one token from many sources)')
    ).toBeInTheDocument()
  })

  test('shows the baseline hint instead of claiming everything is fine', () => {
    useProductionShape()
    renderSection()

    expect(
      screen.getByText(/does not yet hold enough history/)
    ).toBeInTheDocument()
    // 「还没有判定」时页面上不得出现「本窗口内无发现」。这一条**有判别力**：
    // 报告还没回来时发现表格根本不渲染，所以该文案不可能出现；一旦有人在拿不到
    // 报告时也渲染一个空表格，这条就会红 —— 那正是「什么都没看」被伪装成
    // 「一切正常」的时刻。
    expect(screen.queryByText('No finding in this window')).toBeNull()
  })

  test('claims "no finding" only once a scan actually returned', async () => {
    useProductionShape()
    mocks.getAbuseAlerts.mockResolvedValue({
      hours: 24,
      windowMinutes: 10,
      generatedAt: 1790000000,
      baselineReady: true,
      baselineReadyAt: 0,
      lastScheduledScanAt: 1789999700,
      notifyChannelReady: true,
      logRetention: {
        retentionDays: 30,
        requiredMinutes: 70,
        requiredHours: 24,
        sufficient: true,
      },
      findings: [],
      skipped: [],
      excludedSaturatedRows: 0,
    })
    renderSection()

    // 这一条钉住的是此前**没有任何用例覆盖过**的那一面：`baseline_ready=true` +
    // 空发现必须真的显示「本窗口内无发现」。改掉 emptyContent、或不再渲染表格，
    // 它就会红。
    expect(
      await screen.findByText('No finding in this window')
    ).toBeInTheDocument()
  })

  test('shows the retention hint when the log retention is too short', () => {
    useProductionShape()
    renderSection()

    expect(
      screen.getByText(/shorter than what detection needs/)
    ).toBeInTheDocument()
  })

  test('renders the findings and the skipped entries once the scan returns', async () => {
    useProductionShape()
    mocks.getAbuseAlerts.mockResolvedValue({
      hours: 24,
      windowMinutes: 10,
      generatedAt: 1790000000,
      baselineReady: true,
      baselineReadyAt: 0,
      lastScheduledScanAt: 1789999700,
      notifyChannelReady: true,
      logRetention: {
        retentionDays: 30,
        requiredMinutes: 70,
        requiredHours: 24,
        sufficient: true,
      },
      findings: [
        {
          rule: 'consume_spike',
          severity: 'high',
          tokenId: 9002,
          tokenName: 'prod-key',
          userId: 42,
          username: 'customer-a',
          modelName: null,
          windowStart: 1789999800,
          windowEnd: 1790000400,
          evidence: {
            current: 2000000,
            baselineMedian: 10000,
            ratio: 200,
            direction: 'up',
          },
        },
      ],
      skipped: [
        { rule: 'consume_spike', tokenId: 9008, reason: 'insufficient_baseline' },
      ],
      excludedSaturatedRows: 3,
    })
    renderSection()

    expect(await screen.findByText('prod-key')).toBeInTheDocument()
    expect(screen.getByText('customer-a')).toBeInTheDocument()
    expect(screen.getByText('consume_spike')).toBeInTheDocument()
    // skipped 必须一起显示，否则「没有发现」与「没有检测」看起来一样。
    expect(screen.getByText(/9008/)).toBeInTheDocument()
    // PRD 边界 case：`hours` 超上限时后端钳制返回，页面必须显示**实际使用**的窗口
    // （这里是后端报的 24 小时 / 10 分钟窗口），否则运维者以为看的是他请求的范围。
    // 用正则而不是整串精确匹配：这一段与「上次扫描时间」同在 `<p>` 里。
    expect(
      screen.getByText(/Scanning the last 24 hours in 10-minute windows/)
    ).toBeInTheDocument()
  })
})
