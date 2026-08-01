<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  trend: { type: Array, default: () => [] },
})

const el = ref(null)
let chart = null

function render() {
  if (!el.value) return
  if (!chart) chart = echarts.init(el.value, null, { renderer: 'canvas' })
  const labels = props.trend.map((d) => d.periodLabel || d.period)
  const revenue = props.trend.map((d) => d.revenue)
  const net = props.trend.map((d) => d.net_profit)
  const eps = props.trend.map((d) => d.eps)

  chart.setOption({
    backgroundColor: 'transparent',
    animationDuration: 650,
    grid: { left: 48, right: 48, top: 36, bottom: 36 },
    legend: {
      top: 0,
      textStyle: { color: '#8b929e', fontFamily: 'Noto Sans TC' },
      itemWidth: 12,
      itemHeight: 8,
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#10151f',
      borderColor: 'rgba(45,212,191,0.28)',
      textStyle: { color: '#e8e4dc', fontFamily: 'Noto Sans TC' },
    },
    xAxis: {
      type: 'category',
      data: labels,
      axisLine: { lineStyle: { color: 'rgba(232,228,220,0.12)' } },
      axisLabel: { color: '#8b929e', fontFamily: 'JetBrains Mono', fontSize: 11 },
    },
    yAxis: [
      {
        type: 'value',
        name: '金額',
        nameTextStyle: { color: '#8b929e', fontSize: 11 },
        splitLine: { lineStyle: { color: 'rgba(232,228,220,0.06)' } },
        axisLabel: {
          color: '#8b929e',
          fontFamily: 'JetBrains Mono',
          fontSize: 10,
          formatter: (v) => {
            const a = Math.abs(v)
            if (a >= 1e12) return `${(v / 1e12).toFixed(1)}兆`
            if (a >= 1e8) return `${(v / 1e8).toFixed(0)}億`
            return String(v)
          },
        },
      },
      {
        type: 'value',
        name: 'EPS',
        nameTextStyle: { color: '#8b929e', fontSize: 11 },
        splitLine: { show: false },
        axisLabel: { color: '#8b929e', fontFamily: 'JetBrains Mono', fontSize: 10 },
      },
    ],
    series: [
      {
        name: '營收',
        type: 'bar',
        data: revenue,
        itemStyle: { color: 'rgba(45, 212, 191, 0.55)' },
        barMaxWidth: 28,
      },
      {
        name: '淨利',
        type: 'line',
        data: net,
        smooth: 0.25,
        symbol: 'circle',
        symbolSize: 6,
        lineStyle: { color: '#d4a574', width: 2 },
        itemStyle: { color: '#d4a574' },
      },
      {
        name: 'EPS',
        type: 'line',
        yAxisIndex: 1,
        data: eps,
        smooth: 0.25,
        symbol: 'diamond',
        symbolSize: 7,
        lineStyle: { color: '#e8e4dc', width: 1.5, type: 'dashed' },
        itemStyle: { color: '#e8e4dc' },
      },
    ],
  })
}

function onResize() {
  chart?.resize()
}

onMounted(() => {
  render()
  window.addEventListener('resize', onResize)
})

watch(() => props.trend, render, { deep: true })

onBeforeUnmount(() => {
  window.removeEventListener('resize', onResize)
  chart?.dispose()
  chart = null
})
</script>

<template>
  <div class="chart-wrap">
    <div class="head">
      <h3 class="display">趨勢切面</h3>
      <p class="muted">近十二期營收／淨利／EPS</p>
    </div>
    <div ref="el" class="chart" />
  </div>
</template>

<style scoped>
.chart-wrap {
  border: 1px solid var(--line);
  padding: 1rem 1rem 0.5rem;
  background: linear-gradient(180deg, rgba(14, 17, 24, 0.7), rgba(7, 8, 12, 0.35));
}

.head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 0.25rem;
}

h3 {
  margin: 0;
  font-size: 1.05rem;
  letter-spacing: 0.06em;
}

p { margin: 0; font-size: 0.78rem; }

.chart {
  width: 100%;
  height: 300px;
}
</style>
