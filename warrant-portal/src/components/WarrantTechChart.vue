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

const props = defineProps({
  code: { type: String, default: '' },
  name: { type: String, default: '' },
  series: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  periodDays: { type: Number, default: 120 },
})

const emit = defineEmits(['update:periodDays'])

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

const showK = ref(lsBool(LS.k, true))
const showMA = ref(lsBool(LS.ma, true))
const showDuoKong = ref(lsBool(LS.dk, false))
const showVolume = ref(lsBool(LS.vol, true))
const showDuoKongTrend = ref(lsBool(LS.dkt, false))
const showKD = ref(lsBool(LS.kd, false))
const showRSI = ref(lsBool(LS.rsi, false))
const showMACD = ref(lsBool(LS.macd, false))
const duoKongPeriod = ref(Number(localStorage.getItem(LS.dkPeriod) || 55))
const duoKongTrendPeriod = ref(Number(localStorage.getItem(LS.dktPeriod) || 24))

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

const bars = computed(() => mapBars(props.series))
const closes = computed(() => bars.value.map((b) => b.close))

const duoKong = computed(() =>
  calcDuoKongLine(closes.value, duoKongPeriod.value),
)
const duoKongTrend = computed(() =>
  calcDuoKongTrend(closes.value, duoKongTrendPeriod.value),
)
const ma5 = computed(() => calcSMA(closes.value, 5))
const ma10 = computed(() => calcSMA(closes.value, 10))
const ma20 = computed(() => calcSMA(closes.value, 20))
const ma60 = computed(() => calcSMA(closes.value, 60))
const kd = computed(() => calcKD(bars.value))
const rsi = computed(() => calcRSI(closes.value, 14))
const macd = computed(() => calcMACD(closes.value))

const signals = computed(() =>
  buildWarrantSignals({
    closes: closes.value,
    duoKongBase: duoKong.value.base,
    kd: kd.value,
    rsi: rsi.value,
    macd: macd.value,
  }),
)

const ohlcCount = computed(
  () => bars.value.filter((b) => b.open != null && b.close != null).length,
)

const chartHeight = computed(() => {
  const n =
    (showVolume.value ? 1 : 0) +
    (showDuoKongTrend.value ? 1 : 0) +
    (showKD.value ? 1 : 0) +
    (showRSI.value ? 1 : 0) +
    (showMACD.value ? 1 : 0)
  return `${Math.max(360, 320 + n * 88)}px`
})

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

  const mainH = subIds.length === 0 ? 78 : Math.max(36, Math.round(78 * mainRatio))
  let cursor = topPad
  grids.push({ left: 56, right: 18, top: `${cursor}%`, height: `${mainH}%` })
  const mainIdx = 0
  cursor += mainH + 2

  const subH =
    subIds.length === 0
      ? 0
      : Math.max(10, Math.floor((88 - cursor) / subIds.length) - 1)
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
    splitLine: { lineStyle: { color: 'rgba(148,183,205,0.12)' } },
  })

  if (showK.value && ohlcCount.value > 0) {
    series.push({
      name: 'K線',
      type: 'candlestick',
      xAxisIndex: mainIdx,
      yAxisIndex: mainIdx,
      data: candle,
      itemStyle: {
        color: '#ef4444',
        color0: '#22c55e',
        borderColor: '#ef4444',
        borderColor0: '#22c55e',
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
    const mas = [
      { name: 'MA5', data: ma5.value, color: '#fbbf24' },
      { name: 'MA10', data: ma10.value, color: '#38bdf8' },
      { name: 'MA20', data: ma20.value, color: '#c084fc' },
      { name: 'MA60', data: ma60.value, color: '#fb7185' },
    ]
    mas.forEach((m) => {
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
      lineStyle: { width: 1.4, color: '#fbbf24' },
      connectNulls: false,
    })
    series.push({
      name: 'D',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: kd.value.d,
      showSymbol: false,
      lineStyle: { width: 1.4, color: '#38bdf8' },
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
      lineStyle: { width: 1.5, color: '#c084fc' },
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
      lineStyle: { width: 1.3, color: '#fbbf24' },
      connectNulls: false,
    })
    series.push({
      name: 'DEA',
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: yi,
      data: macd.value.dea,
      showSymbol: false,
      lineStyle: { width: 1.3, color: '#38bdf8' },
      connectNulls: false,
    })
  }

  const legendData = []
  if (showK.value && ohlcCount.value > 0) legendData.push('K線')
  else legendData.push('收盤價')
  if (showMA.value) legendData.push('MA5', 'MA10', 'MA20', 'MA60')
  if (showDuoKong.value) legendData.push(`多空線(${duoKongPeriod.value})`)
  if (showVolume.value) legendData.push('成交量')
  if (showDuoKongTrend.value) legendData.push(`多空趨勢線(${duoKongTrendPeriod.value})`)
  if (showKD.value) legendData.push('K', 'D')
  if (showRSI.value) legendData.push('RSI')
  if (showMACD.value) legendData.push('MACD柱', 'DIF', 'DEA')

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
        axisPointer: { type: 'cross', lineStyle: { color: 'rgba(46,211,198,0.35)' } },
      },
      legend: {
        data: [...new Set(legendData)],
        top: 0,
        left: 'center',
        itemWidth: 10,
        itemHeight: 8,
        itemGap: 12,
        textStyle: { color: '#c5d4de', fontSize: 11 },
      },
      grid: grids,
      xAxis: xAxes,
      yAxis: yAxes,
      series,
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: xAxes.map((_, i) => i),
          start: 0,
          end: 100,
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
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
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
  ],
  () => renderChart(),
  { deep: true },
)
</script>

<template>
  <div class="tech panel">
    <div class="head">
      <div class="title-row">
        <h2>技術分析</h2>
        <span class="muted" v-if="code">{{ code }} · {{ name || '' }}</span>
      </div>
      <div class="periods">
        <button
          v-for="p in periods"
          :key="p.days"
          type="button"
          :class="{ active: periodDays === p.days }"
          @click="setPeriod(p.days)"
        >
          {{ p.label }}
        </button>
      </div>
    </div>

    <div class="toggles">
      <label><input v-model="showK" type="checkbox" /> K線</label>
      <label><input v-model="showMA" type="checkbox" /> MA</label>
      <label><input v-model="showDuoKong" type="checkbox" /> 多空線</label>
      <label><input v-model="showVolume" type="checkbox" /> 成交量</label>
      <label><input v-model="showDuoKongTrend" type="checkbox" /> 多空趨勢線</label>
      <label><input v-model="showKD" type="checkbox" /> KD</label>
      <label><input v-model="showRSI" type="checkbox" /> RSI</label>
      <label><input v-model="showMACD" type="checkbox" /> MACD</label>
    </div>

    <p v-if="code && !loading && ohlcCount === 0 && bars.length" class="hint">
      此檔暫無完整 OHLC，改以收盤價線顯示；可切換較長期間或同步最新成交。
    </p>

    <div ref="chartRef" class="chart-box" :style="{ height: chartHeight }"></div>

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
      </div>
    </div>
  </div>
</template>

<style scoped>
.tech {
  padding: 1rem 1.1rem 1.1rem;
}
.head {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.55rem;
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
  gap: 0.35rem;
}
.periods button {
  border: 1px solid rgba(148, 183, 205, 0.28);
  background: rgba(12, 22, 30, 0.55);
  color: #c5d4de;
  border-radius: 6px;
  padding: 0.28rem 0.65rem;
  font-size: 0.8rem;
  cursor: pointer;
}
.periods button.active {
  border-color: #2ed3c6;
  color: #2ed3c6;
}
.toggles {
  display: flex;
  flex-wrap: wrap;
  gap: 0.55rem 0.9rem;
  margin-bottom: 0.55rem;
  font-size: 0.82rem;
  color: #c5d4de;
}
.toggles label {
  display: inline-flex;
  align-items: center;
  gap: 0.28rem;
  cursor: pointer;
  user-select: none;
}
.hint {
  margin: 0 0 0.5rem;
  font-size: 0.78rem;
  color: #f0b429;
}
.chart-box {
  width: 100%;
  min-height: 360px;
}
.signals {
  margin-top: 0.75rem;
  border-top: 1px solid rgba(148, 183, 205, 0.14);
  padding-top: 0.65rem;
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
</style>
