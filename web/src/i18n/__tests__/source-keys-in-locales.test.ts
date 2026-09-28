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

import { describe, expect, test } from 'vitest'

import { STATIC_I18N_KEYS } from '../static-keys'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const SRC_DIR = path.resolve(__dirname, '..', '..')
const LOCALES_DIR = path.resolve(__dirname, '..', 'locales')

const LOCALES = ['en', 'zh', 'zh-TW', 'fr', 'ru', 'ja', 'vi']

// `t('...')` / `t("...")`; the `\b` keeps `split(`-style calls out because the
// preceding character of a longer identifier is a word character.
const T_CALL_PATTERNS = [
  /\bt\(\s*'((?:[^'\\]|\\.)*)'/g,
  /\bt\(\s*"((?:[^"\\]|\\.)*)"/g,
]

const SKIP_DIRS = new Set(['__tests__', 'locales', 'node_modules'])

/**
 * Pre-existing literals that no locale file carries, so every language renders
 * the English source string. They were measured while landing this guard and
 * belong to screens this guard's change does not own; localising them is a
 * separate change. The list is meant to shrink — delete an entry once its
 * strings reach the locale files. Anything not listed here fails the test.
 */
const KNOWN_UNTRANSLATED_LITERALS = new Set([
  'Expand all',
  'Select preset',
  'Length',
  'Delete model "{{name}}"? This cannot be undone.',
  'Are you sure you want to delete "{{name}}"? Users who authenticated with this provider will no longer be able to log in.',
  '{{count}} announcements deleted. Click "Save Settings" to apply.',
  '{{count}} API entries deleted. Click "Save Settings" to apply.',
  '{{count}} FAQs deleted. Click "Save Settings" to apply.',
  '{{count}} groups deleted. Click "Save Settings" to apply.',
])

function collectSourceFiles(dir: string, files: string[] = []): string[] {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (SKIP_DIRS.has(entry.name)) continue
      collectSourceFiles(path.join(dir, entry.name), files)
      continue
    }
    if (!/\.tsx?$/.test(entry.name)) continue
    // Test files legitimately contain sample payload strings; they are not copy.
    if (/\.(test|spec)\.tsx?$/.test(entry.name)) continue
    files.push(path.join(dir, entry.name))
  }
  return files
}

/**
 * Every `t('…')` literal in the source, unescaped.
 *
 * This is the direction `bun run i18n:sync` cannot check: that script only
 * compares the locale files against each other, so a source literal that was
 * never translated passes it. Requesting a literal the locales do not carry is
 * exactly the F7 defect this guards against.
 */
function collectSourceKeys(): Map<string, string[]> {
  const seen = new Map<string, string[]>()
  for (const file of collectSourceFiles(SRC_DIR)) {
    const text = fs.readFileSync(file, 'utf8')
    const relative = path.relative(SRC_DIR, file)
    for (const pattern of T_CALL_PATTERNS) {
      for (const match of text.matchAll(pattern)) {
        const key = match[1].replaceAll(/\\(.)/g, '$1')
        const locations = seen.get(key) ?? []
        locations.push(relative)
        seen.set(key, locations)
      }
    }
  }
  return seen
}

function readTranslations(locale: string): Record<string, unknown> {
  const raw = fs.readFileSync(path.join(LOCALES_DIR, `${locale}.json`), 'utf8')
  return (
    (JSON.parse(raw) as { translation?: Record<string, unknown> })
      .translation ?? {}
  )
}

describe('source translation keys exist in every locale', () => {
  const sourceKeys = collectSourceKeys()
  const translations = new Map(
    LOCALES.map((locale) => [locale, readTranslations(locale)])
  )
  // Keys reached through `t(variable)` rather than a literal are declared here.
  const requestedKeys = [
    ...new Set([...sourceKeys.keys(), ...STATIC_I18N_KEYS]),
  ].filter((key) => !KNOWN_UNTRANSLATED_LITERALS.has(key))

  test('the scan actually found the source, not an empty set', () => {
    expect(requestedKeys.length).toBeGreaterThan(1000)
  })

  test.each(LOCALES)('%s carries every key the source requests', (locale) => {
    const translation = translations.get(locale) ?? {}
    const missing = requestedKeys.filter(
      (key) => !Object.hasOwn(translation, key)
    )

    expect(
      missing,
      `Missing from ${locale}.json (first requester in parens): ${missing
        .map((key) => `${key} (${sourceKeys.get(key)?.[0] ?? 'static-keys'})`)
        .join('\n')}`
    ).toEqual([])
  })
})
