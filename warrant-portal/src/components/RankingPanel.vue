<script setup>
import { computed } from 'vue'

const props = defineProps({
  rows: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  selectedCode: { type: String, default: '' },
  metric: { type: String, default: 'turnover' },
  heatType: { type: String, default: '' },
  apiType: { type: String, default: null },
  errorText: { type: String, default: '' },
})

const emit = defineEmits(['select'])

function fmt(n) {
  if (n == null || Number.isNaN(Number(n))) return '—'
  return Number(n).toLocaleString()
}

const subtitle = computed(() => {
  const metricLabel = props.metric === 'volume' ? '依成交張數' : '依成交金額'
  const t = props.heatType || props.apiType
  if (t === '認購') return `${metricLabel} · 認購`
  if (t === '認售') return `${metricLabel} · 認售`
  return metricLabel
})
</script>

<template>
  <div class="rank panel">
    <div class="head">
      <h2>當日熱度</h2>
      <span class="muted">{{ subtitle }}</span>
    </div>
    <div v-if="loading" class="empty muted">載入排行…</div>
    <div v-else-if="!rows.length" class="empty muted">
      {{ errorText || (heatType ? `目前沒有符合「${heatType}」的成交熱度` : '尚無成交資料，請先同步最新成交') }}
    </div>
    <div v-else class="table-wrap">
      <table class="data">
        <thead>
          <tr>
            <th>#</th>
            <th>代號</th>
            <th>名稱</th>
            <th>類型</th>
            <th>收盤</th>
            <th>{{ metric === 'volume' ? '張數' : '金額' }}</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in rows"
            :key="`${row.market}-${row.warrant_code}`"
            :class="{ selected: selectedCode === row.warrant_code }"
            @click="emit('select', row)"
          >
            <td class="mono">{{ row.rank }}</td>
            <td class="mono">{{ row.warrant_code }}</td>
            <td>{{ row.warrant_name }}</td>
            <td>
              <span class="tag" :class="row.warrant_type === '認售' ? 'put' : 'call'">
                {{ row.warrant_type || '—' }}
              </span>
            </td>
            <td class="num mono">{{ row.close_price == null ? '—' : Number(row.close_price).toLocaleString(undefined, { maximumFractionDigits: 2 }) }}</td>
            <td class="num mono">
              {{ metric === 'volume' ? fmt(row.volume) : fmt(row.turnover) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.rank { padding: 1rem 1.1rem 1.1rem; }
.head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  margin-bottom: 0.75rem;
}
.head h2 {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 600;
}
.empty {
  padding: 2rem 1rem;
  text-align: center;
}
.table-wrap { max-height: min(42vh, 420px); }
</style>
