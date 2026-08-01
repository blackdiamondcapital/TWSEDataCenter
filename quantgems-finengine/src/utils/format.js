export function formatMoney(value, { isEps = false } = {}) {
  if (value == null || Number.isNaN(Number(value))) return '—'
  const n = Number(value)
  if (isEps) {
    return n.toLocaleString('zh-TW', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })
  }
  const abs = Math.abs(n)
  const sign = n < 0 ? '-' : ''
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)} 兆`
  if (abs >= 1e8) return `${sign}${(abs / 1e8).toFixed(2)} 億`
  if (abs >= 1e4) return `${sign}${(abs / 1e4).toFixed(2)} 萬`
  return `${sign}${abs.toLocaleString('zh-TW', { maximumFractionDigits: 0 })}`
}

export function formatPct(value, digits = 1) {
  if (value == null || Number.isNaN(Number(value))) return '—'
  const n = Number(value)
  // ratios stored as 0.xx
  const pct = Math.abs(n) <= 5 ? n * 100 : n
  return `${pct.toFixed(digits)}%`
}

export function formatRatio(key, value) {
  if (value == null || Number.isNaN(Number(value))) return '—'
  const n = Number(value)
  if (key === 'current_ratio' || key === 'quick_ratio') {
    return n.toFixed(2)
  }
  return formatPct(n)
}

export function changeClass(curr, prev) {
  if (curr == null || prev == null) return ''
  if (curr > prev) return 'up'
  if (curr < prev) return 'down'
  return ''
}

export function changePct(curr, prev) {
  if (curr == null || prev == null || prev === 0) return null
  return ((curr - prev) / Math.abs(prev)) * 100
}
