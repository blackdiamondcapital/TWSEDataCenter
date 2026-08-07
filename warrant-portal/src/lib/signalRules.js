/** 權證技術訊號列（依最新 bar 判斷） */

function lastFinite(arr, from = arr.length - 1) {
  for (let i = from; i >= 0; i--) {
    const v = arr[i]
    if (typeof v === 'number' && Number.isFinite(v)) return { i, v }
  }
  return null
}

function prevFinite(arr, before) {
  return lastFinite(arr, before - 1)
}

/**
 * @returns {Array<{ id: string, title: string, direction: 'bull'|'bear'|'neutral', detail: string }>}
 */
export function buildWarrantSignals({
  closes,
  duoKongBase,
  kd,
  rsi,
  macd,
}) {
  const signals = []
  const lastClose = lastFinite(closes)
  const lastDk = lastFinite(duoKongBase || [])
  const prevDk = lastDk ? prevFinite(duoKongBase, lastDk.i) : null

  if (lastClose && lastDk) {
    if (lastClose.v > lastDk.v) {
      signals.push({
        id: 'price-vs-dk',
        title: '價格 vs 多空線',
        direction: 'bull',
        detail: `收盤 ${Number(lastClose.v).toFixed(2)} 站上多空線 ${Number(lastDk.v).toFixed(2)}`,
      })
    } else if (lastClose.v < lastDk.v) {
      signals.push({
        id: 'price-vs-dk',
        title: '價格 vs 多空線',
        direction: 'bear',
        detail: `收盤 ${Number(lastClose.v).toFixed(2)} 跌破多空線 ${Number(lastDk.v).toFixed(2)}`,
      })
    } else {
      signals.push({
        id: 'price-vs-dk',
        title: '價格 vs 多空線',
        direction: 'neutral',
        detail: '收盤貼近多空線',
      })
    }
  } else {
    signals.push({
      id: 'price-vs-dk',
      title: '價格 vs 多空線',
      direction: 'neutral',
      detail: '資料不足',
    })
  }

  if (lastDk && prevDk) {
    if (lastDk.v > prevDk.v) {
      signals.push({
        id: 'dk-slope',
        title: '多空線斜率',
        direction: 'bull',
        detail: '多空線持續上彎',
      })
    } else if (lastDk.v < prevDk.v) {
      signals.push({
        id: 'dk-slope',
        title: '多空線斜率',
        direction: 'bear',
        detail: '多空線下彎',
      })
    } else {
      signals.push({
        id: 'dk-slope',
        title: '多空線斜率',
        direction: 'neutral',
        detail: '多空線走平',
      })
    }
  } else {
    signals.push({
      id: 'dk-slope',
      title: '多空線斜率',
      direction: 'neutral',
      detail: '資料不足',
    })
  }

  const kArr = kd?.k || []
  const dArr = kd?.d || []
  const lastK = lastFinite(kArr)
  const lastD = lastFinite(dArr)
  const prevK = lastK ? prevFinite(kArr, lastK.i) : null
  const prevD = lastD ? prevFinite(dArr, lastD.i) : null
  if (lastK && lastD && prevK && prevD) {
    const crossUp = prevK.v <= prevD.v && lastK.v > lastD.v
    const crossDown = prevK.v >= prevD.v && lastK.v < lastD.v
    if (crossUp) {
      signals.push({
        id: 'kd',
        title: 'KD',
        direction: 'bull',
        detail: `黃金交叉 K=${lastK.v} D=${lastD.v}`,
      })
    } else if (crossDown) {
      signals.push({
        id: 'kd',
        title: 'KD',
        direction: 'bear',
        detail: `死亡交叉 K=${lastK.v} D=${lastD.v}`,
      })
    } else {
      signals.push({
        id: 'kd',
        title: 'KD',
        direction: 'neutral',
        detail: `K=${lastK.v} D=${lastD.v}`,
      })
    }
  } else {
    signals.push({ id: 'kd', title: 'KD', direction: 'neutral', detail: '資料不足' })
  }

  const lastRsi = lastFinite(rsi || [])
  if (lastRsi) {
    if (lastRsi.v >= 70) {
      signals.push({
        id: 'rsi',
        title: 'RSI',
        direction: 'bear',
        detail: `超買區 ${lastRsi.v}`,
      })
    } else if (lastRsi.v <= 30) {
      signals.push({
        id: 'rsi',
        title: 'RSI',
        direction: 'bull',
        detail: `超賣區 ${lastRsi.v}`,
      })
    } else {
      signals.push({
        id: 'rsi',
        title: 'RSI',
        direction: 'neutral',
        detail: `中性 ${lastRsi.v}`,
      })
    }
  } else {
    signals.push({ id: 'rsi', title: 'RSI', direction: 'neutral', detail: '資料不足' })
  }

  const dif = macd?.dif || []
  const dea = macd?.dea || []
  const hist = macd?.hist || []
  const lastDif = lastFinite(dif)
  const lastDea = lastFinite(dea)
  const lastHist = lastFinite(hist)
  const prevDif = lastDif ? prevFinite(dif, lastDif.i) : null
  const prevDea = lastDea ? prevFinite(dea, lastDea.i) : null
  if (lastDif && lastDea && prevDif && prevDea) {
    const crossUp = prevDif.v <= prevDea.v && lastDif.v > lastDea.v
    const crossDown = prevDif.v >= prevDea.v && lastDif.v < lastDea.v
    if (crossUp) {
      signals.push({
        id: 'macd',
        title: 'MACD',
        direction: 'bull',
        detail: 'DIF 上穿 DEA',
      })
    } else if (crossDown) {
      signals.push({
        id: 'macd',
        title: 'MACD',
        direction: 'bear',
        detail: 'DIF 下穿 DEA',
      })
    } else if (lastHist && lastHist.v > 0) {
      signals.push({
        id: 'macd',
        title: 'MACD',
        direction: 'bull',
        detail: `柱體為正 ${lastHist.v}`,
      })
    } else if (lastHist && lastHist.v < 0) {
      signals.push({
        id: 'macd',
        title: 'MACD',
        direction: 'bear',
        detail: `柱體為負 ${lastHist.v}`,
      })
    } else {
      signals.push({
        id: 'macd',
        title: 'MACD',
        direction: 'neutral',
        detail: '走勢中性',
      })
    }
  } else {
    signals.push({ id: 'macd', title: 'MACD', direction: 'neutral', detail: '資料不足' })
  }

  return signals
}
