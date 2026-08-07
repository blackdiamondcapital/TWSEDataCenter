<script setup>
import { ref, onMounted, watch, onBeforeUnmount } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  code: { type: String, default: '' },
  name: { type: String, default: '' },
  series: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
})

const chartRef = ref(null)
let chartInstance = null

function disposeChart() {
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
}

function fmtAxis(v) {
  const n = Number(v)
  if (!Number.isFinite(n)) return ''
  const abs = Math.abs(n)
  if (abs >= 1e8) return `${(n / 1e8).toFixed(1)}億`
  if (abs >= 1e4) return `${(n / 1e4).toFixed(1)}萬`
  return String(Math.round(n))
}

function renderChart() {
  if (!chartRef.value) return
  if (!chartInstance) chartInstance = echarts.init(chartRef.value)

  const data = Array.isArray(props.series) ? props.series : []
  if (!data.length) {
    chartInstance.clear()
    chartInstance.setOption({
      backgroundColor: 'transparent',
      title: {
        text: props.loading ? '載入中…' : '選擇一檔權證查看走勢',
        left: 'center',
        top: 'middle',
        textStyle: { color: '#8fa3b3', fontSize: 14, fontWeight: 500 },
      },
    }, { notMerge: true })
    return
  }

  const dates = data.map((d) => d.trade_date)
  const turnovers = data.map((d) => d.turnover ?? null)
  const volumes = data.map((d) => d.volume ?? null)
  const closes = data.map((d) => d.close_price ?? null)
  const hasClose = closes.some((v) => v != null)

  const legendData = hasClose ? ['收盤價', '成交金額', '成交張數'] : ['成交金額', '成交張數']

  const yAxis = [
    {
      type: 'value',
      name: '金額',
      nameTextStyle: { color: '#2ed3c6', fontSize: 11 },
      axisLine: { show: true, lineStyle: { color: '#2ed3c6' } },
      axisLabel: { color: '#9bb0c0', formatter: fmtAxis },
      splitLine: { lineStyle: { color: 'rgba(148,183,205,0.12)' } },
    },
    {
      type: 'value',
      name: '張數',
      nameTextStyle: { color: '#f0b429', fontSize: 11 },
      axisLine: { show: true, lineStyle: { color: '#f0b429' } },
      axisLabel: { color: '#9bb0c0', formatter: fmtAxis },
      splitLine: { show: false },
    },
  ]

  const series = [
    {
      name: '成交張數',
      type: 'bar',
      yAxisIndex: 1,
      data: volumes,
      barMaxWidth: 14,
      itemStyle: { color: 'rgba(240, 180, 41, 0.45)' },
      z: 1,
    },
    {
      name: '成交金額',
      type: 'line',
      yAxisIndex: 0,
      data: turnovers,
      showSymbol: false,
      smooth: 0.2,
      lineStyle: { width: 2.5, color: '#2ed3c6' },
      itemStyle: { color: '#2ed3c6' },
      z: 3,
    },
  ]

  if (hasClose) {
    yAxis.push({
      type: 'value',
      name: '收盤',
      offset: 48,
      nameTextStyle: { color: '#94a3b8', fontSize: 11 },
      axisLine: { show: false },
      axisLabel: { color: '#9bb0c0' },
      splitLine: { show: false },
    })
    series.unshift({
      name: '收盤價',
      type: 'line',
      yAxisIndex: 2,
      data: closes,
      showSymbol: false,
      smooth: 0.2,
      lineStyle: { width: 2, color: '#e2e8f0' },
      itemStyle: { color: '#e2e8f0' },
      z: 2,
    })
  }

  chartInstance.setOption({
    backgroundColor: 'transparent',
    animationDuration: 280,
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(10, 16, 22, 0.92)',
      borderColor: 'rgba(148,183,205,0.25)',
      textStyle: { color: '#eef5f8', fontSize: 12 },
      axisPointer: { type: 'line', lineStyle: { color: 'rgba(46,211,198,0.35)' } },
      valueFormatter: (v) => (v == null ? '—' : Number(v).toLocaleString()),
    },
    legend: {
      data: legendData,
      top: 4,
      left: 'center',
      icon: 'roundRect',
      itemWidth: 12,
      itemHeight: 8,
      itemGap: 18,
      textStyle: { color: '#c5d4de', fontSize: 12 },
    },
    grid: {
      left: 58,
      right: hasClose ? 72 : 52,
      top: 40,
      bottom: 28,
      containLabel: false,
    },
    xAxis: {
      type: 'category',
      data: dates,
      boundaryGap: true,
      axisTick: { alignWithLabel: true },
      axisLine: { lineStyle: { color: '#5d7384' } },
      axisLabel: {
        color: '#9bb0c0',
        hideOverlap: true,
        formatter: (v) => String(v || '').slice(5),
      },
    },
    yAxis,
    series,
  }, { notMerge: true })
}

function handleResize() {
  chartInstance?.resize()
}

onMounted(() => {
  renderChart()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  disposeChart()
})

watch(() => [props.code, props.series, props.loading], () => renderChart(), { deep: true })
</script>

<template>
  <div class="chart panel">
    <div class="head">
      <h2>走勢</h2>
      <span class="muted" v-if="code">{{ code }} · {{ name || '' }}</span>
    </div>
    <div ref="chartRef" class="chart-box"></div>
  </div>
</template>

<style scoped>
.chart { padding: 1rem 1.1rem 1.1rem; }
.head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 0.35rem;
}
.head h2 {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 600;
}
.chart-box {
  width: 100%;
  height: 320px;
}
</style>
