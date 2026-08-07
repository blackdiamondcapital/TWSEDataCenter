<script setup>
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'
import {
  mapBars,
  calcSMA,
  calcDuoKongLine,
  calcDuoKongTrend,
  calcKD,
  calcRSI,
  calcMACD,
} from '../lib/indicators'
import { buildWarrantSignals } from '../lib/signalRules'
import {
  CHART_THEME,
  DEFAULT_MA_PERIODS,
  DEFAULT_DUO_KONG_PERIOD,
  DEFAULT_DUO_KONG_TREND_PERIOD,
} from '../lib/chartTheme'

const props = defineProps({
  code: { type: String, default: '' },
  name: { type: String, default: '' },
  series: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  periodDays: { type: Number, default: 120 },
})

const emit = defineEmits(['update:periodDays', 'fullscreen-change'])

const rootRef = ref(null)
const isFullscreen = ref(false)

const LS = {
  k: 'warrantChartShowK',
  ma: 'warrantChartShowMA',
  dk: 'warrantChartShowDuoKong',
  vol: 'warrantChartShowVolume',
  dkt: 'warrantChartShowDuoKongTrend',
  kd: 'warrantChartShowKD',
  rsi: 'warrantChartShowRSI',
  macd: 'warrantChartShowMACD',
  dkPeriod: 'warrantChartDuoKongPeriod',
  dktPeriod: 'warrantChartDuoKongTrendPeriod',
}

function lsBool(key, fallback) {
  const v = localStorage.getItem(key)
  if (v === null) return fallback
  return v === 'true'
}

function lsPeriod(key, fallback) {
  const raw = localStorage.getItem(key)
  if (raw == null) return fallback
  const n = Number(raw)
  // 舊預設 55 → 對齊主站 77
  if (key === LS.dkPeriod && n === 55) return fallback
  return Number.isFinite(n) && n >= 2 ? n : fallback
}

const showK = ref(lsBool(LS.k, true))
const showMA = ref(lsBool(LS.ma, true))
const showDuoKong = ref(lsBool(LS.dk, true))
const showVolume = ref(lsBool(LS.vol, true))
const showDuoKongTrend = ref(lsBool(LS.dkt, false))
const showKD = ref(lsBool(LS.kd, false))
const showRSI = ref(lsBool(LS.rsi, false))
const showMACD = ref(lsBool(LS.macd, false))
const duoKongPeriod = ref(lsPeriod(LS.dkPeriod, DEFAULT_DUO_KONG_PERIOD))
const duoKongTrendPeriod = ref(lsPeriod(LS.dktPeriod, DEFAULT_DUO_KONG_TREND_PERIOD))
const showControls = ref(false)

const chartRef = ref(null)
let chartInstance = null

const periods = [
  { label: '60日', days: 60 },
  { label: '120日', days: 120 },
  { label: '250日', days: 250 },
]

function persist(key, val) {
  localStorage.setItem(key, String(val))
}

function isMobile() {
  return typeof window !== 'undefined' && window.innerWidth <= 768
}

watch(showK, (v) => persist(LS.k, v))
watch(showMA, (v) => {
  persist(LS.ma, v)
  if (v && isMobile() && showDuoKong.value) {
    showDuoKong.value = false
  }
})
watch(showDuoKong, (v) => {
  persist(LS.dk, v)
  if (v && isMobile() && showMA.value) {
    showMA.value = false
  }
})
watch(showVolume, (v) => persist(LS.vol, v))
watch(showDuoKongTrend, (v) => persist(LS.dkt, v))
watch(showKD, (v) => persist(LS.kd, v))
watch(showRSI, (v) => persist(LS.rsi, v))
watch(showMACD, (v) => persist(LS.macd, v))
watch(duoKongPeriod, (v) => {
  const n = Number(v)
  if (!Number.isFinite(n) || n < 2) {
    duoKongPeriod.value = DEFAULT_DUO_KONG_PERIOD
    return
  }
  persist(LS.dkPeriod, Math.round(n))
})
watch(duoKongTrendPeriod, (v) => {
  const n = Number(v)
  if (!Number.isFinite(n) || n < 2) {
    duoKongTrendPeriod.value = DEFAULT_DUO_KONG_TREND_PERIOD
    return
  }
  persist(LS.dktPeriod, Math.round(n))
})

const bars = computed(() => mapBars(props.series))
const closes = computed(() => bars.value.map((b) => b.close))

const duoKong = computed(() =>
  calcDuoKongLine(closes.value, duoKongPeriod.value),
)
const duoKongTrend = computed(() =>
  calcDuoKongTrend(closes.value, duoKongTrendPeriod.value),
)
const maSeries = computed(() =>
  DEFAULT_MA_PERIODS.map((p, i) => ({
    period: p,
    name: `MA${p}`,
    data: calcSMA(closes.value, p),
    color: CHART_THEME.ma[i] || CHART_THEME.ma[0],
  })),
)
const kd = computed(() => calcKD(bars.value))
const rsi = computed(() => calcRSI(closes.value, 14))
const macd = computed(() => calcMACD(closes.value))

const visibleSignals = computed(() => {
  const all = buildWarrantSignals({
    closes: closes.value,
    duoKongBase: duoKong.value.base,
    kd: kd.value,
    rsi: rsi.value,
    macd: macd.value,
  })
  return all.filter((s) => {
    if (s.id === 'price-vs-dk' || s.id === 'dk-slope') return showDuoKong.value
    if (s.id === 'kd') return showKD.value
    if (s.id === 'rsi') return showRSI.value
    if (s.id === 'macd') return showMACD.value
    return true
  })
})

const signals = visibleSignals

const ohlcCount = computed(
  () => bars.value.filter((b) => b.open != null && b.close != null).length,
)

const chartHeight = computed(() => {
  if (isFullscreen.value) return '100%'
  const n =
    (showVolume.value ? 1 : 0) +
    (showDuoKongTrend.value ? 1 : 0) +
    (showKD.value ? 1 : 0) +
    (showRSI.value ? 1 : 0) +
    (showMACD.value ? 1 : 0)
  return `${Math.max(360, 320 + n * 88)}px`
})

function applyCssFullscreen(el) {
  if (!el) return
  el.style.position = 'fixed'
  el.style.inset = '0'
  el.style.width = '100vw'
  el.style.height = '100svh'
  el.style.zIndex = '9999'
  el.style.margin = '0'
  el.style.borderRadius = '0'
  el.style.border = 'none'
  el.style.background = '#0b1220'
  document.documentElement.classList.add('warrant-ta-fs')
  document.body.classList.add('warrant-ta-fs')
}

function clearCssFullscreen(el) {
  if (!el) return
  el.style.position = ''
  el.style.inset = ''
  el.style.width = ''
  el.style.height = ''
  el.style.zIndex = ''
  el.style.margin = ''
  el.style.borderRadius = ''
  el.style.border = ''
  el.style.background = ''
  document.documentElement.classList.remove('warrant-ta-fs')
  document.body.classList.remove('warrant-ta-fs')
}

function enterFullscreen() {
  if (isFullscreen.value) return
  isFullscreen.value = true
  applyCssFullscreen(rootRef.value)
  emit('fullscreen-change', true)
  requestAnimationFrame(() => {
    chartInstance?.resize()
    renderChart()
    try {
      rootRef.value?.requestFullscreen?.()
    } catch (_) {
      /* CSS fullscreen is enough */
    }
  })
}

function exitFullscreen() {
  if (!isFullscreen.value) return
  isFullscreen.value = false
  clearCssFullscreen(rootRef.value)
  emit('fullscreen-change', false)
  try {
    if (document.fullscreenElement) document.exitFullscreen?.()
  } catch (_) {
    /* ignore */
  }
  requestAnimationFrame(() => {
    chartInstance?.resize()
    renderChart()
  })
}

function toggleFullscreen() {
  if (isFullscreen.value) exitFullscreen()
  else enterFullscreen()
}

function onKeydown(e) {
  if (e.key === 'Escape' && isFullscreen.value) {
    e.preventDefault()
    exitFullscreen()
  }
}

function onNativeFsChange() {
  // 使用者按 Esc／瀏覽器退出原生全螢幕時，同步關閉 CSS 覆蓋
  if (!document.fullscreenElement && isFullscreen.value) {
    isFullscreen.value = false
    clearCssFullscreen(rootRef.value)
    emit('fullscreen-change', false)
    requestAnimationFrame(() => {
      chartInstance?.resize()
      renderChart()
    })
  }
}

defineExpose({ enterFullscreen, exitFullscreen, toggleFullscreen, isFullscreen })

function disposeChart() {
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
}

function fmtVol(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return ''
  if (Math.abs(n) >= 1e4) return `${(n / 1e4).toFixed(1)}萬`
  return String(Math.round(n))
}

function setPeriod(days) {
  if (days === props.periodDays) return
  emit('update:periodDays', days)
}

function renderChart() {
  if (!chartRef.value) return
  if (!chartInstance) chartInstance = echarts.init(chartRef.value)

  const data = bars.value
  if (!data.length) {
    chartInstance.clear()
    chartInstance.setOption(
      {
        backgroundColor: 'transparent',
        title: {
          text: props.loading ? '載入中…' : '選擇一檔權證查看技術分析',
          left: 'center',
          top: 'middle',
          textStyle: { color: '#8fa3b3', fontSize: 14, fontWeight: 500 },
        },
      },
      { notMerge: true },
    )
    return
  }

  const dates = data.map((d) => String(d.time).slice(0, 10))
  const candle = data.map((d) => {
    if (d.open == null || d.close == null || d.low == null || d.high == null) {
      return [null, null, null, null]
    }
    return [d.open, d.close, d.low, d.high]
  })
  const volumes = data.map((d) => d.volume)
  const volColors = data.map((d) => {
    if (d.open == null || d.close == null) return 'rgba(148,183,205,0.35)'
    return d.close >= d.open ? 'rgba(239,68,68,0.55)' : 'rgba(34,197,94,0.55)'
  })

  const subIds = []
  if (showVolume.value) subIds.push('volume')
  if (showDuoKongTrend.value) subIds.push('dkt')
  if (showKD.value) subIds.push('kd')
  if (showRSI.value) subIds.push('rsi')
  if (showMACD.value) subIds.push('macd')

  const topPad = 8
  const mainRatio =
    subIds.length === 0 ? 1 : subIds.length === 1 ? 0.62 : subIds.length === 2 ? 0.5 : 0.42
  const grids = []
  const xAxes = []
  const yAxes = []
  const series = []

  // 底部留 dataZoom slider
  const usable = 82
  const mainH = subIds.length === 0 ? usable - 4 : Math.max(34, Math.round(usable * mainRatio))
  let cursor = topPad
  grids.push({ left: 56, right: 18, top: `${cursor}%`, height: `${mainH}%` })
  const mainIdx = 0
  cursor += mainH + 2

  const subH =
    subIds.length === 0
      ? 0
      : Math.max(9, Math.floor((usable - cursor + topPad) / subIds.length) - 1)
  const subIndex = {}
  subIds.forEach((id) => {
    const idx = grids.length
    subIndex[id] = idx
    grids.push({ left: 56, right: 18, top: `${cursor}%`, height: `${subH}%` })
    cursor += subH + 1.5
  })

  // xAxes
  for (let i = 0; i < grids.length; i++) {
    xAxes.push({
      type: 'category',
      data: dates,
      gridIndex: i,
      boundaryGap: true,
      axisTick: { show: i === grids.length - 1, alignWithLabel: true },
      axisLine: { lineStyle: { color: '#5d7384' } },
      axisLabel: {
        show: i === grids.length - 1,
        color: '#9bb0c0',
        hideOverlap: true,
        formatter: (v) => String(v || '').slice(5),
      },
    })
  }

  // main y
  yAxes.push({
    type: 'value',
    gridIndex: mainIdx,
    scale: true,
    axisLabel: { color: '#9bb0c0' },
      splitLine: { lineStyle: { color: 'rgba(100,200,255,0.12)' } },
  })

  if (showK.value && ohlcCount.value > 0) {
    series.push({
      name: 'K線',
      type: 'candlestick',
      xAxisIndex: mainIdx,
      yAxisIndex: mainIdx,
      data: candle,
      itemStyle: {
        color: CHART_THEME.up,
        color0: CHART_THEME.down,
        borderColor: CHART_THEME.up,
        borderColor0: CHART_THEME.down,
      },
      z: 2,
    })
  } else {
    // fallback close line when no OHLC
    series.push({
      name: '收盤價',
      type: 'line',
      xAxisIndex: mainIdx,
      yAxisIndex: mainIdx,
      data: closes.value,
      showSymbol: false,
      lineStyle: { width: 2, color: '#e2e8f0' },
      z: 2,
      connectNulls: false,
    })
  }

  if (showMA.value) {
    maSeries.value.forEach((m) => {
      series.push({
        name: m.name,
        type: 'line',
        xAxisIndex: mainIdx,
        yAxisIndex: mainIdx,
        data: m.data,
        showSymbol: false,
        lineStyle: { width: 1.2, color: m.color },
        itemStyle: { color: m.color },
        z: 3,
        connectNulls: false,
      })
    })
  }

  if (showDuoKong.value) {
    const p = duoKongPeriod.value
    series.push({
      name: `多空線(${p})`,
      type: 'line',
      xAxisIndex: mainIdx,
      yAxisIndex: mainIdx,
      data: duoKong.value.up,
      showSymbol: false,
      lineStyle: { width: 2, color: '#ef4444' },
      itemStyle: { color: '#ef4444' },
      z: 4,
      connectNulls: false,
    })
    series.push({
      name: `多空線(${p})`,
      type: 'line',
      xAxisIndex: mainIdx,
      yAxisIndex: mainIdx,
      data: duoKong.value.flatDown,
      showSymbol: false,
      lineStyle: { width: 2, color: '#22c55e' },
      itemStyle: { color: '#22c55e' },
      z: 4,
      connectNulls: false,
      tooltip: { show: false },
    })
  }

  // volume
  if (showVolume.value && subIndex.volume != null) {
    const gi = subIndex.volume
    yAxes.push({
      type: 'value',
      gridIndex: gi,
      axisLabel: { color: '#9bb0c0', formatter: fmtVol },
      splitLine: { show: false },
    })
    series.push({
      name: '成交量',
      type: 'bar',
      xAxisIndex: gi,
      yAxisIndex: yAxes.length - 1,
      data: volumes.map((v, i) => ({
        value: v,
        itemStyle: { color: volColors[i] },
      })),
      barMaxWidth: 10,
      z: 1,
    })
  }

  // 多空趨勢線 sub
  if (showDuoKongTrend.value && subIndex.dkt != null) {
    const gi = subIndex.dkt
    const p = duoKongTrendPeriod.value
    yAxes.push({
      type: 'value',
      gridIndex: gi,
      scale: true,
      axisLabel: { color: '#9bb0c0' },
      splitLine: { lineStyle: { color: 'rgba(148,183,205,0.08)' } },
    })
    const yi = yAxes.length - 1
    series.push({
      name: `多空趨勢線(${p})`,
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: duoKongTrend.value.up,
      showSymbol: false,
      lineStyle: { width: 1.8, color: '#ef4444' },
      connectNulls: false,
    })
    series.push({
      name: `多空趨勢線(${p})`,
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: duoKongTrend.value.flatDown,
      showSymbol: false,
      lineStyle: { width: 1.8, color: '#22c55e' },
      connectNulls: false,
      tooltip: { show: false },
    })
  }

  if (showKD.value && subIndex.kd != null) {
    const gi = subIndex.kd
    yAxes.push({
      type: 'value',
      gridIndex: gi,
      min: 0,
      max: 100,
      axisLabel: { color: '#9bb0c0' },
      splitLine: { lineStyle: { color: 'rgba(148,183,205,0.08)' } },
    })
    const yi = yAxes.length - 1
    series.push({
      name: 'K',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: kd.value.k,
      showSymbol: false,
      lineStyle: { width: 1.4, color: CHART_THEME.kdK },
      connectNulls: false,
    })
    series.push({
      name: 'D',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: kd.value.d,
      showSymbol: false,
      lineStyle: { width: 1.4, color: CHART_THEME.kdD },
      connectNulls: false,
    })
  }

  if (showRSI.value && subIndex.rsi != null) {
    const gi = subIndex.rsi
    yAxes.push({
      type: 'value',
      gridIndex: gi,
      min: 0,
      max: 100,
      axisLabel: { color: '#9bb0c0' },
      splitLine: { lineStyle: { color: 'rgba(148,183,205,0.08)' } },
    })
    const yi = yAxes.length - 1
    series.push({
      name: 'RSI',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: rsi.value,
      showSymbol: false,
      lineStyle: { width: 1.5, color: CHART_THEME.rsi },
      markLine: {
        silent: true,
        symbol: 'none',
        lineStyle: { type: 'dashed', color: 'rgba(148,183,205,0.35)' },
        data: [{ yAxis: 70 }, { yAxis: 30 }],
        label: { show: false },
      },
      connectNulls: false,
    })
  }

  if (showMACD.value && subIndex.macd != null) {
    const gi = subIndex.macd
    yAxes.push({
      type: 'value',
      gridIndex: gi,
      scale: true,
      axisLabel: { color: '#9bb0c0' },
      splitLine: { lineStyle: { color: 'rgba(148,183,205,0.08)' } },
    })
    const yi = yAxes.length - 1
    series.push({
      name: 'MACD柱',
      type: 'bar',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: macd.value.hist.map((v) => ({
        value: v,
        itemStyle: {
          color: v == null ? 'transparent' : v >= 0 ? 'rgba(239,68,68,0.55)' : 'rgba(34,197,94,0.55)',
        },
      })),
      barMaxWidth: 8,
    })
    series.push({
      name: 'DIF',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: macd.value.dif,
      showSymbol: false,
      lineStyle: { width: 1.3, color: CHART_THEME.dif },
      connectNulls: false,
    })
    series.push({
      name: 'DEA',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: macd.value.dea,
      showSymbol: false,
      lineStyle: { width: 1.3, color: CHART_THEME.dea },
      connectNulls: false,
    })
  }

  const legendData = []
  if (showK.value && ohlcCount.value > 0) legendData.push('K線')
  else legendData.push('收盤價')
  if (showMA.value) legendData.push(...maSeries.value.map((m) => m.name))
  if (showDuoKong.value) legendData.push(`多空線(${duoKongPeriod.value})`)
  if (showVolume.value) legendData.push('成交量')
  if (showDuoKongTrend.value) legendData.push(`多空趨勢線(${duoKongTrendPeriod.value})`)
  if (showKD.value) legendData.push('K', 'D')
  if (showRSI.value) legendData.push('RSI')
  if (showMACD.value) legendData.push('MACD柱', 'DIF', 'DEA')

  const xAxisIndexes = xAxes.map((_, i) => i)
  chartInstance.setOption(
    {
      backgroundColor: 'transparent',
      animationDuration: 220,
      axisPointer: { link: [{ xAxisIndex: 'all' }] },
      tooltip: {
        trigger: 'axis',
        backgroundColor: 'rgba(10, 16, 22, 0.94)',
        borderColor: 'rgba(148,183,205,0.25)',
        textStyle: { color: '#eef5f8', fontSize: 12 },
        axisPointer: { type: 'cross', lineStyle: { color: CHART_THEME.cross } },
      },
      legend: {
        data: [...new Set(legendData)],
        top: 0,
        left: 'center',
        itemWidth: 10,
        itemHeight: 8,
        itemGap: 12,
        textStyle: { color: '#c2cce0', fontSize: 11 },
      },
      grid: grids,
      xAxis: xAxes,
      yAxis: yAxes,
      series,
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: xAxisIndexes,
          start: 0,
          end: 100,
        },
        {
          type: 'slider',
          xAxisIndex: xAxisIndexes,
          height: 18,
          bottom: 6,
          start: 0,
          end: 100,
          borderColor: 'rgba(148,183,205,0.25)',
          backgroundColor: 'rgba(8,14,20,0.55)',
          fillerColor: 'rgba(0,212,255,0.12)',
          handleStyle: { color: '#00d4ff', borderColor: '#00d4ff' },
          moveHandleStyle: { color: 'rgba(0,212,255,0.45)' },
          dataBackground: {
            lineStyle: { color: 'rgba(148,183,205,0.35)' },
            areaStyle: { color: 'rgba(148,183,205,0.12)' },
          },
          selectedDataBackground: {
            lineStyle: { color: 'rgba(0,212,255,0.55)' },
            areaStyle: { color: 'rgba(0,212,255,0.18)' },
          },
          textStyle: { color: '#8fa3b3', fontSize: 10 },
        },
      ],
    },
    { notMerge: true },
  )
}

function handleResize() {
  chartInstance?.resize()
  renderChart()
}

onMounted(() => {
  renderChart()
  window.addEventListener('resize', handleResize)
  window.addEventListener('keydown', onKeydown)
  document.addEventListener('fullscreenchange', onNativeFsChange)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  window.removeEventListener('keydown', onKeydown)
  document.removeEventListener('fullscreenchange', onNativeFsChange)
  clearCssFullscreen(rootRef.value)
  disposeChart()
})

watch(
  () => [
    props.code,
    props.series,
    props.loading,
    showK.value,
    showMA.value,
    showDuoKong.value,
    showVolume.value,
    showDuoKongTrend.value,
    showKD.value,
    showRSI.value,
    showMACD.value,
    duoKongPeriod.value,
    duoKongTrendPeriod.value,
    isFullscreen.value,
  ],
  () => renderChart(),
  { deep: true },
)
</script>

<template>
  <div ref="rootRef" class="tech panel" :class="{ 'is-fullscreen': isFullscreen }">
    <div class="head">
      <div class="title-row">
        <h2>技術分析</h2>
        <span class="muted" v-if="code">{{ code }} · {{ name || '' }}</span>
      </div>
      <div class="head-actions">
        <div class="periods" role="group" aria-label="期間">
          <button
            v-for="p in periods"
            :key="p.days"
            type="button"
            class="chip"
            :class="{ active: periodDays === p.days }"
            @click="setPeriod(p.days)"
          >
            {{ p.label }}
          </button>
        </div>
        <button
          type="button"
          class="icon-btn"
          :class="{ active: showControls }"
          title="指標參數"
          aria-label="指標參數"
          @click="showControls = !showControls"
        >
          <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">
            <path
              fill="currentColor"
              d="M4 7h10v2H4V7zm12 0h4v2h-4V7zM4 15h4v2H4v-2zm6 0h10v2H10v-2zm7.5-11a1.5 1.5 0 110 3 1.5 1.5 0 010-3zM6.5 13a1.5 1.5 0 110 3 1.5 1.5 0 010-3z"
            />
          </svg>
        </button>
        <button
          type="button"
          class="icon-btn"
          :title="isFullscreen ? '退出全螢幕' : '全螢幕'"
          :aria-label="isFullscreen ? '退出全螢幕' : '全螢幕'"
          @click="toggleFullscreen"
        >
          <svg v-if="!isFullscreen" viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">
            <path
              fill="currentColor"
              d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"
            />
          </svg>
          <svg v-else viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">
            <path
              fill="currentColor"
              d="M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z"
            />
          </svg>
        </button>
      </div>
    </div>

    <div class="toggles" role="group" aria-label="指標開關">
      <button type="button" class="chip" :class="{ active: showK }" @click="showK = !showK">K線</button>
      <button type="button" class="chip" :class="{ active: showMA }" @click="showMA = !showMA">MA</button>
      <button type="button" class="chip" :class="{ active: showDuoKong }" @click="showDuoKong = !showDuoKong">多空線</button>
      <button type="button" class="chip" :class="{ active: showVolume }" @click="showVolume = !showVolume">成交量</button>
      <button type="button" class="chip" :class="{ active: showDuoKongTrend }" @click="showDuoKongTrend = !showDuoKongTrend">多空趨勢線</button>
      <button type="button" class="chip" :class="{ active: showKD }" @click="showKD = !showKD">KD</button>
      <button type="button" class="chip" :class="{ active: showRSI }" @click="showRSI = !showRSI">RSI</button>
      <button type="button" class="chip" :class="{ active: showMACD }" @click="showMACD = !showMACD">MACD</button>
    </div>

    <div v-if="showControls" class="params">
      <label>
        多空線週期
        <input v-model.number="duoKongPeriod" type="number" min="2" max="250" step="1" />
      </label>
      <label>
        多空趨勢線週期
        <input v-model.number="duoKongTrendPeriod" type="number" min="2" max="120" step="1" />
      </label>
    </div>

    <p v-if="code && !loading && ohlcCount === 0 && bars.length" class="hint">
      此檔暫無完整 OHLC，改以收盤價線顯示；可切換較長期間或同步最新成交。
    </p>

    <div class="chart-stage">
      <div ref="chartRef" class="chart-box" :style="{ height: chartHeight }"></div>
    </div>

    <div class="signals">
      <div class="signals-head">訊號列</div>
      <div class="signal-list">
        <div
          v-for="s in signals"
          :key="s.id"
          class="signal"
          :class="s.direction"
        >
          <span class="sig-title">{{ s.title }}</span>
          <span class="sig-detail">{{ s.detail }}</span>
        </div>
        <div v-if="!signals.length" class="signal muted-empty">
          <span class="sig-detail">開啟對應指標後顯示訊號</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.tech {
  padding: 1rem 1.1rem 1.1rem;
}
.tech.is-fullscreen {
  display: flex;
  flex-direction: column;
  padding: 0.75rem 1rem 1rem;
  overflow: auto;
}
.head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.55rem;
  flex-shrink: 0;
}
.head-actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.45rem;
}
.title-row {
  display: flex;
  align-items: baseline;
  gap: 0.75rem;
}
.head h2 {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 600;
}
.muted {
  color: #8fa3b3;
  font-size: 0.85rem;
}
.periods {
  display: flex;
  gap: 0.3rem;
}
.chip,
.icon-btn {
  border: 1px solid rgba(148, 183, 205, 0.22);
  background: rgba(8, 14, 20, 0.45);
  color: #a8bac8;
  border-radius: 999px;
  padding: 0.26rem 0.72rem;
  font-size: 0.78rem;
  line-height: 1.2;
  cursor: pointer;
  transition: border-color 0.15s ease, color 0.15s ease, background 0.15s ease;
}
.chip:hover,
.icon-btn:hover {
  border-color: rgba(0, 212, 255, 0.45);
  color: #e8f7ff;
}
.chip.active,
.icon-btn.active {
  border-color: rgba(0, 212, 255, 0.7);
  color: #00d4ff;
  background: rgba(0, 212, 255, 0.12);
}
.icon-btn {
  width: 32px;
  height: 32px;
  padding: 0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: 10px;
}
.toggles {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
  margin-bottom: 0.55rem;
  flex-shrink: 0;
}
.params {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem 1.1rem;
  margin: -0.15rem 0 0.65rem;
  padding: 0.55rem 0.7rem;
  border: 1px solid rgba(148, 183, 205, 0.14);
  border-radius: 10px;
  background: rgba(8, 14, 20, 0.35);
  flex-shrink: 0;
}
.params label {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.78rem;
  color: #9bb0c0;
}
.params input {
  width: 64px;
  border: 1px solid rgba(148, 183, 205, 0.28);
  border-radius: 6px;
  background: rgba(2, 8, 14, 0.65);
  color: #e8f7ff;
  padding: 0.22rem 0.4rem;
  font-size: 0.8rem;
}
.hint {
  margin: 0 0 0.5rem;
  font-size: 0.78rem;
  color: #38bdf8;
  flex-shrink: 0;
}
.chart-stage {
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
.chart-box {
  width: 100%;
  min-height: 360px;
}
.is-fullscreen .chart-stage {
  flex: 1 1 auto;
}
.is-fullscreen .chart-box {
  flex: 1 1 auto;
  min-height: calc(100svh - 240px);
  height: calc(100svh - 240px) !important;
}
.signals {
  margin-top: 0.75rem;
  border-top: 1px solid rgba(148, 183, 205, 0.14);
  padding-top: 0.65rem;
  flex-shrink: 0;
}
.signals-head {
  font-size: 0.82rem;
  color: #8fa3b3;
  margin-bottom: 0.45rem;
}
.signal-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 0.45rem;
}
.signal {
  border: 1px solid rgba(148, 183, 205, 0.16);
  border-radius: 8px;
  padding: 0.45rem 0.55rem;
  background: rgba(8, 14, 20, 0.45);
}
.signal .sig-title {
  display: block;
  font-size: 0.75rem;
  color: #8fa3b3;
  margin-bottom: 0.15rem;
}
.signal .sig-detail {
  font-size: 0.84rem;
  color: #e2e8f0;
}
.signal.bull {
  border-color: rgba(239, 68, 68, 0.35);
}
.signal.bull .sig-detail {
  color: #fca5a5;
}
.signal.bear {
  border-color: rgba(34, 197, 94, 0.35);
}
.signal.bear .sig-detail {
  color: #86efac;
}
.muted-empty .sig-detail {
  color: #8fa3b3;
}
</style>
