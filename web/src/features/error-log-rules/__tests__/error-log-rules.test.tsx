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
import { render, screen } from '@testing-library/react'
import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import { describe, expect, test } from 'vitest'

import { ErrorLogRulesTable } from '../components/error-log-rules-table'
import { parseErrorLogMap } from '../lib/parse-error-log-map'

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
      titleKey: 'Upstream channel error (after a channel is selected)',
      recorded: true,
      gates: ['global_switch', 'per_error_flag'],
      reasonKey: '',
      source: 'controller/relay.go:297',
    },
    {
      id: 'insufficient_wallet_quota',
      titleKey: 'Insufficient wallet quota or pre-consume failure',
      recorded: false,
      gates: ['hardcoded_no_record'],
      reasonKey: 'Insufficient quota is a normal business outcome, not a fault.',
      source: 'service/billing_session.go:237,262,396,402',
    },
  ],
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
    expect(parsed?.entries).toHaveLength(2)
  })

  test('keeps the switch on when the raw option key is the literal string null', () => {
    const parsed = parseErrorLogMap([
      { key: 'error_log_setting.enabled', value: 'null' },
      { key: 'ErrorLogMap', value: JSON.stringify(payload) },
    ])

    expect(parsed?.enabled).toBe(true)
  })

  test('returns null when the backend does not send the key', () => {
    expect(parseErrorLogMap([{ key: 'LogConsumeEnabled', value: 'true' }])).toBeNull()
    expect(parseErrorLogMap(undefined)).toBeNull()
  })

  test('returns null when the payload is not valid JSON', () => {
    expect(parseErrorLogMap([{ key: 'ErrorLogMap', value: 'not json' }])).toBeNull()
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
          entries: [{ ...payload.entries[0], gates: ['global_switch', 'future_gate'] }],
        }),
      },
    ])

    expect(parsed?.entries[0].gates).toEqual(['global_switch'])
  })
})

describe('ErrorLogRulesTable', () => {
  test('renders one row per backend entry with no switchable control', () => {
    const parsed = parseErrorLogMap([
      { key: 'ErrorLogMap', value: JSON.stringify(payload) },
    ])
    expect(parsed).not.toBeNull()

    render(<ErrorLogRulesTable entries={parsed?.entries ?? []} />)

    expect(
      screen.getByText('Upstream channel error (after a channel is selected)')
    ).toBeInTheDocument()
    expect(
      screen.getByText('Insufficient wallet quota or pre-consume failure')
    ).toBeInTheDocument()
    expect(screen.queryAllByRole('switch')).toHaveLength(0)
    expect(screen.queryAllByRole('checkbox')).toHaveLength(0)
  })
})
