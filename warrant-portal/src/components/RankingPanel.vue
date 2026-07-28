<script setup>
const props = defineProps({
  rows: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  selectedCode: { type: String, default: '' },
  metric: { type: String, default: 'turnover' },
})

const emit = defineEmits(['select'])

function fmt(n) {
  if (n == null || Number.isNaN(Number(n))) return '—'
  return Number(n).toLocaleString()
}
</script>

<template>
  <div class="rank panel">
    <div class="head">
      <h2>當日熱度</h2>
      <span class="muted">{{ metric === 'volume' ? '依成交張數' : '依成交金額' }}</span>
    </div>
    <div v-if="loading" class="empty muted">載入排行…</div>
    <div v-else-if="!rows.length" class="empty muted">尚無成交資料，請先同步最新成交</div>
    <div v-else class="table-wrap">
      <table class="data">
        <thead>
          <tr>
            <th>#</th>
            <th>代號</th>
            <th>名稱</th>
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
