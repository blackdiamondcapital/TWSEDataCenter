/**
 * 主站 StockChartECharts 所需 API 適配層。
 * 行情改走權證 /warrants/timeseries；其餘功能以安全 stub 降級。
 */
import { fetchTimeseries, fetchMasterSearch } from '../api.js'

function toNum(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function toIsoDate(v) {
  if (!v) return null
  if (typeof v === 'string') {
    const m = v.match(/^(\d{4}-\d{2}-\d{2})/)
    return m ? m[1] : null
  }
  try {
    const d = v instanceof Date ? v : new Date(v)
    if (Number.isNaN(d.getTime())) return null
    const pad = (n) => String(n).padStart(2, '0')
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
  } catch {
    return null
  }
}

function mapWarrantRow(item) {
  const time = toIsoDate(item.trade_date || item.time || item.date)
  const open = toNum(item.open_price ?? item.open)
  const high = toNum(item.high_price ?? item.high)
  const low = toNum(item.low_price ?? item.low)
  const close = toNum(item.close_price ?? item.close)
  const volume = toNum(item.volume) || 0
  return { time, open, high, low, close, volume }
}

function periodToLimitDays(period) {
  const p = String(period || '1D')
  if (p === '1W') return 750
  if (p === '1M') return 750
  return 750
}

/** 日 K → 週／月 K 聚合（權證後端僅有日線） */
function aggregateBars(rows, period) {
  const p = String(period || '1D')
  if (p === '1D' || !rows.length) return rows

  const buckets = new Map()
  for (const row of rows) {
    if (!row?.time) continue
    const d = new Date(`${row.time}T00:00:00Z`)
    if (Number.isNaN(d.getTime())) continue
    let key
    if (p === '1W') {
      // ISO week: Monday-based
      const day = d.getUTCDay() || 7
      const monday = new Date(d)
      monday.setUTCDate(d.getUTCDate() - (day - 1))
      key = toIsoDate(monday)
    } else {
      key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-01`
    }
    if (!key) continue
    const prev = buckets.get(key)
    if (!prev) {
      buckets.set(key, { ...row, time: key })
      continue
    }
    const open = prev.open ?? row.open
    const close = row.close ?? prev.close
    const highs = [prev.high, row.high].filter((x) => x != null)
    const lows = [prev.low, row.low].filter((x) => x != null)
    buckets.set(key, {
      time: key,
      open,
      high: highs.length ? Math.max(...highs) : null,
      low: lows.length ? Math.min(...lows) : null,
      close,
      volume: (prev.volume || 0) + (row.volume || 0),
    })
  }
  return [...buckets.values()].sort((a, b) => String(a.time).localeCompare(String(b.time)))
}

export async function fetchStockPriceHistory(symbol, period = '1D', opts = {}) {
  const code = String(symbol || '').trim()
  if (!code) return []

  const end = opts && typeof opts === 'object' ? opts.end || '' : ''
  const limitDays = periodToLimitDays(period)

  const params = { code, limitDays }
  if (end) {
    params.end = end
    // 往前多抓一段供 maybeLoadOlder 合併
    try {
      const d = new Date(`${end}T00:00:00Z`)
      if (!Number.isNaN(d.getTime())) {
        d.setUTCDate(d.getUTCDate() - (limitDays - 1))
        params.start = toIsoDate(d)
      }
    } catch {
      /* ignore */
    }
  }

  const resp = await fetchTimeseries(params)
  const rows = Array.isArray(resp?.data) ? resp.data.map(mapWarrantRow).filter((r) => r.time) : []
  rows.sort((a, b) => String(a.time).localeCompare(String(b.time)))
  return aggregateBars(rows, period)
}

export async function fetchStockQuote() {
  return null
}

export async function fetchRankings() {
  return []
}

export async function fetchComparison() {
  return { data: [] }
}

export async function fetchAiTechnicalAnalysis() {
  return { success: false, data: null }
}

export async function fetchUserChartSettings() {
  return { value: null }
}

export async function saveUserChartSettings() {
  return { success: true }
}

export async function fetchStockMaster() {
  try {
    const resp = await fetchMasterSearch({ q: '', limit: 200 })
    const list = resp?.data || resp?.items || resp?.rows || []
    if (!Array.isArray(list)) return []
    return list.map((item) => ({
      symbol: String(item.warrant_code || item.code || item.symbol || '').toUpperCase(),
      name: item.warrant_name || item.name || '',
      short_name: item.warrant_name || item.name || '',
    })).filter((x) => x.symbol)
  } catch {
    return []
  }
}
