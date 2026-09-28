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
import { api } from '@/lib/api'
import { requireServerSuccess } from '@/lib/server-error-message'

import { parseAbuseAlertReport } from './lib/parse-abuse-alert-report'
import type { AbuseAlertReport } from './types'

/**
 * 读取一次发现列表。这是一个**只读** GET：不写任何状态、不发通知。
 *
 * 后端会把 `hours` 钳到上限后返回实际使用的值，所以这里不做本地钳制 ——
 * 前端复刻一遍上限就是第二份会漂移的清单。
 */
export async function getAbuseAlerts(hours: number): Promise<AbuseAlertReport> {
  const response = await api.get<{
    success: boolean
    message: string
    data: unknown
  }>('/api/abuse-alerts/', { params: { hours } })

  const envelope = requireServerSuccess(response.data)
  const report = parseAbuseAlertReport(envelope.data)
  if (report === null) {
    throw new Error('The backend returned an unexpected abuse alert payload')
  }
  return report
}
