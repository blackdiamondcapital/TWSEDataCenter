<script setup>
import { computed } from 'vue'

const props = defineProps({
  rows: { type: Array, default: () => [] },
  total: { type: Number, default: 0 },
  page: { type: Number, default: 1 },
  pageSize: { type: Number, default: 50 },
  loading: { type: Boolean, default: false },
  selectedCode: { type: String, default: '' },
})

const emit = defineEmits(['select', 'page'])

const pageCount = computed(() => Math.max(1, Math.ceil((props.total || 0) / props.pageSize)))

function fmt(n, digits = 2) {
  if (n == null || Number.isNaN(Number(n))) return '—'
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: digits })
}

function onRow(row) {
  emit('select', row)
}
</script>

<template>
  <div class="screener panel">
    <div class="head">
      <h2>發行主檔</h2>
      <span class="muted">共 {{ total.toLocaleString() }} 檔</span>
    </div>

    <div v-if="loading" class="empty muted">查詢中…</div>
    <div v-else-if="!rows.length" class="empty muted">沒有符合條件的權證</div>
    <div v-else class="table-wrap">
      <table class="data">
        <thead>
          <tr>
            <th>市場</th>
            <th>代號</th>
            <th>名稱</th>
            <th>類型</th>
            <th>標的</th>
            <th>履約價</th>
            <th>行使比</th>
            <th>到期日</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in rows"
            :key="`${row.market}-${row.warrant_code}`"
            :class="{ selected: selectedCode === row.warrant_code }"
            @click="onRow(row)"
          >
            <td><span class="tag market">{{ row.market }}</span></td>
            <td class="mono">{{ row.warrant_code }}</td>
            <td>{{ row.warrant_name }}</td>
            <td>
              <span class="tag" :class="row.warrant_type === '認售' ? 'put' : 'call'">
                {{ row.warrant_type || '—' }}
              </span>
            </td>
            <td>{{ row.underlying_name || row.underlying_code || '—' }}</td>
            <td class="num mono">{{ fmt(row.latest_exercise_price) }}</td>
            <td class="num mono">{{ fmt(row.latest_exercise_ratio, 4) }}</td>
            <td class="mono">{{ row.expiry_date || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="pager" v-if="total > pageSize">
      <button :disabled="page <= 1" @click="emit('page', page - 1)">上一頁</button>
      <span class="muted">{{ page }} / {{ pageCount }}</span>
      <button :disabled="page >= pageCount" @click="emit('page', page + 1)">下一頁</button>
    </div>
  </div>
</template>

<style scoped>
.screener { padding: 1rem 1.1rem 1.1rem; }
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
  padding: 2.5rem 1rem;
  text-align: center;
}
.pager {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.85rem;
  margin-top: 0.85rem;
}
</style>
