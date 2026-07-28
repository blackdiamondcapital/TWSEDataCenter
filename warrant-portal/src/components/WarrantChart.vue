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

function renderChart() {
  if (!chartRef.value) return
  if (!chartInstance) chartInstance = echarts.init(chartRef.value)

  const data = Array.isArray(props.series) ? props.series : []
  if (!data.length) {
    chartInstance.clear()
    chartInstance.setOption({
      title: {
        text: props.loading ? '載入中…' : '選擇一檔權證查看走勢',
        left: 'center',
        top: 'middle',
        textStyle: { color: '#8fa3b3', fontSize: 14, fontWeight: 500 },
      },
    })
    return
  }

  const dates = data.map((d) => d.trade_date)
  const turnovers = data.map((d) => d.turnover ?? null)
  const volumes = data.map((d) => d.volume ?? null)
  const closes = data.map((d) => d.close_price ?? null)
  const hasClose = closes.some((v) => v != null)

  const legend = hasClose ? ['收盤價', '成交金額', '成交張數'] : ['成交金額', '成交張數']
  const series = []
  if (hasClose) {
    series.push({
      name: '收盤價',
      type: 'line',
      smooth: true,
      yAxisIndex: 2,
      data: closes,
      showSymbol: false,
      lineStyle: { width: 2, color: '#f0b429' },
    })
  }
  series.push(
    {
      name: '成交金額',
      type: 'line',
      smooth: true,
      yAxisIndex: 0,
      data: turnovers,
      showSymbol: false,
      lineStyle: { width: 2, color: '#2ed3c6' },
    },
    {
      name: '成交張數',
      type: 'bar',
      yAxisIndex: 1,
      data: volumes,
      itemStyle: { color: 'rgba(240, 180, 41, 0.55)' },
      barMaxWidth: 16,
    },
  )

  const yAxis = [
    {
      type: 'value',
      name: '金額',
      axisLine: { lineStyle: { color: '#2ed3c6' } },
      axisLabel: { color: '#c5d4de' },
      splitLine: { lineStyle: { color: 'rgba(148,183,205,0.12)' } },
    },
    {
      type: 'value',
      name: '張數',
      axisLine: { lineStyle: { color: '#f0b429' } },
      axisLabel: { color: '#c5d4de' },
      splitLine: { show: false },
    },
  ]
  if (hasClose) {
    yAxis.push({
      type: 'value',
      name: '價',
      offset: 55,
      axisLine: { lineStyle: { color: '#f0b429' } },
      axisLabel: { color: '#c5d4de' },
      splitLine: { show: false },
    })
  }

  chartInstance.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      valueFormatter: (v) => (v == null ? '—' : Number(v).toLocaleString()),
    },
    legend: { data: legend, textStyle: { color: '#c5d4de' } },
    grid: { left: 56, right: hasClose ? 88 : 56, top: 42, bottom: 36 },
    xAxis: {
      type: 'category',
      data: dates,
      axisLine: { lineStyle: { color: '#5d7384' } },
      axisLabel: { color: '#9bb0c0' },
    },
    yAxis,
    series,
  }, true)
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
  margin-bottom: 0.4rem;
}
.head h2 {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 600;
}
.chart-box {
  width: 100%;
  height: 300px;
}
</style>
