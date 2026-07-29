/** 權證技術指標純函式（UI 對外名稱：多空線／多空趨勢線） */

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

/** @param {Array<{open?:any,high?:any,low?:any,close?:any,volume?:any,time?:any}>} bars */
export function mapBars(series) {
  return (series || [])
    .map((d) => {
      const time = d.trade_date || d.time || d.date
      const open = num(d.open_price ?? d.open)
      const high = num(d.high_price ?? d.high)
      const low = num(d.low_price ?? d.low)
      const close = num(d.close_price ?? d.close)
      const volume = num(d.volume)
      return { time, open, high, low, close, volume }
    })
    .filter((b) => b.time)
}

export function calcSMA(values, period) {
  const out = new Array(values.length).fill(null)
  const p = Math.max(1, Math.floor(period))
  for (let i = 0; i < values.length; i++) {
    if (i < p - 1) continue
    let sum = 0
    let ok = true
    for (let j = 0; j < p; j++) {
      const v = values[i - j]
      if (v == null) {
        ok = false
        break
      }
      sum += v
    }
    if (ok) out[i] = Number((sum / p).toFixed(4))
  }
  return out
}

function calcWMA(arr, period) {
  const out = new Array(arr.length).fill(null)
  const p = Math.max(1, Math.floor(period))
  if (p <= 1) {
    for (let i = 0; i < arr.length; i++) out[i] = arr[i]
    return out
  }
  const weightSum = (p * (p + 1)) / 2
  for (let i = p - 1; i < arr.length; i++) {
    let acc = 0
    let valid = true
    for (let j = 0; j < p; j++) {
      const v = arr[i - j]
      if (v == null) {
        valid = false
        break
      }
      acc += v * (p - j)
    }
    out[i] = valid ? acc / weightSum : null
  }
  return out
}

/**
 * 多空線（主圖疊加；演算法為 Hull MA，對外不顯示此名稱）
 * @returns {{ base: (number|null)[], up: (number|null)[], flatDown: (number|null)[] }}
 */
export function calcDuoKongLine(closes, period = 77) {
  const n = Math.max(2, Math.floor(period))
  const n2 = Math.max(1, Math.floor(n / 2))
  const ns = Math.max(1, Math.floor(Math.sqrt(n)))
  const wmaN = calcWMA(closes, n)
  const wmaN2 = calcWMA(closes, n2)
  const diff = closes.map((_, i) => {
    const a = wmaN2[i]
    const b = wmaN[i]
    return a != null && b != null ? 2 * a - b : null
  })
  const raw = calcWMA(diff, ns)
  const base = raw.map((v) => (v == null ? null : Number(v.toFixed(4))))
  const up = new Array(base.length).fill(null)
  const flatDown = new Array(base.length).fill(null)
  for (let i = 0; i < base.length; i++) {
    const v = base[i]
    if (v == null) continue
    const prev = i > 0 ? base[i - 1] : null
    const rising = prev != null && v > prev
    if (rising) {
      if (prev != null) up[i - 1] = prev
      up[i] = v
    } else {
      if (prev != null) flatDown[i - 1] = prev
      flatDown[i] = v
    }
  }
  return { base, up, flatDown }
}

/** 多空趨勢線：同一套多空線演算法，用於副圖 */
export function calcDuoKongTrend(closes, period = 24) {
  return calcDuoKongLine(closes, period)
}

export function calcKD(bars, n = 9, m1 = 3, m2 = 3) {
  const k = new Array(bars.length).fill(null)
  const d = new Array(bars.length).fill(null)
  let prevK = 50
  let prevD = 50
  for (let i = 0; i < bars.length; i++) {
    if (i < n - 1) continue
    let lowest = Infinity
    let highest = -Infinity
    let ok = true
    for (let j = 0; j < n; j++) {
      const b = bars[i - j]
      if (b.low == null || b.high == null || b.close == null) {
        ok = false
        break
      }
      lowest = Math.min(lowest, b.low)
      highest = Math.max(highest, b.high)
    }
    if (!ok) continue
    const close = bars[i].close
    const rsv = highest === lowest ? 0 : ((close - lowest) / (highest - lowest)) * 100
    const kVal = (prevK * (m1 - 1) + rsv) / m1
    const dVal = (prevD * (m2 - 1) + kVal) / m2
    prevK = kVal
    prevD = dVal
    k[i] = Number(kVal.toFixed(2))
    d[i] = Number(dVal.toFixed(2))
  }
  return { k, d }
}

function calcWilderRma(values, period) {
  const out = new Array(values.length).fill(null)
  const p = Math.max(1, Math.floor(period))
  let sum = 0
  let count = 0
  let prev = null
  for (let i = 0; i < values.length; i++) {
    const v = values[i]
    if (v == null) {
      out[i] = null
      continue
    }
    if (prev == null) {
      sum += v
      count += 1
      if (count === p) {
        prev = sum / p
        out[i] = prev
      }
    } else {
      prev = (prev * (p - 1) + v) / p
      out[i] = prev
    }
  }
  return out
}

export function calcRSI(closes, period = 14) {
  const gains = new Array(closes.length).fill(null)
  const losses = new Array(closes.length).fill(null)
  for (let i = 1; i < closes.length; i++) {
    const c = closes[i]
    const p = closes[i - 1]
    if (c == null || p == null) continue
    const ch = c - p
    gains[i] = Math.max(ch, 0)
    losses[i] = Math.max(-ch, 0)
  }
  const avgGain = calcWilderRma(gains, period)
  const avgLoss = calcWilderRma(losses, period)
  return avgGain.map((g, i) => {
    const l = avgLoss[i]
    if (g == null || l == null) return null
    if (g === 0 && l === 0) return 50
    if (l === 0) return 100
    return Number((100 - 100 / (1 + g / l)).toFixed(2))
  })
}

function calcEMA(prices, period) {
  const out = new Array(prices.length).fill(null)
  const p = Math.max(1, Math.floor(period))
  const mult = 2 / (p + 1)
  for (let i = 0; i < prices.length; i++) {
    if (i < p - 1) continue
    if (i === p - 1) {
      let sum = 0
      let ok = true
      for (let j = 0; j < p; j++) {
        if (prices[i - j] == null) {
          ok = false
          break
        }
        sum += prices[i - j]
      }
      out[i] = ok ? sum / p : null
    } else if (out[i - 1] != null && prices[i] != null) {
      out[i] = (prices[i] - out[i - 1]) * mult + out[i - 1]
    }
  }
  return out
}

export function calcMACD(closes, fast = 12, slow = 26, signalPeriod = 9) {
  const fastEMA = calcEMA(closes, fast)
  const slowEMA = calcEMA(closes, slow)
  const dif = closes.map((_, i) =>
    fastEMA[i] != null && slowEMA[i] != null ? fastEMA[i] - slowEMA[i] : null,
  )
  const dea = new Array(dif.length).fill(null)
  const valid = []
  let firstIdx = -1
  const mult = 2 / (signalPeriod + 1)
  for (let i = 0; i < dif.length; i++) {
    if (dif[i] == null) continue
    valid.push(dif[i])
    if (valid.length === 1) firstIdx = i
    if (valid.length < signalPeriod) continue
    if (valid.length === signalPeriod) {
      dea[i] = valid.reduce((a, b) => a + b, 0) / signalPeriod
    } else {
      let prev = null
      for (let j = i - 1; j >= firstIdx; j--) {
        if (dea[j] != null) {
          prev = dea[j]
          break
        }
      }
      if (prev != null) dea[i] = (dif[i] - prev) * mult + prev
    }
  }
  const hist = dif.map((v, i) =>
    v != null && dea[i] != null ? Number((v - dea[i]).toFixed(4)) : null,
  )
  return {
    dif: dif.map((v) => (v == null ? null : Number(v.toFixed(4)))),
    dea: dea.map((v) => (v == null ? null : Number(v.toFixed(4)))),
    hist,
  }
}
