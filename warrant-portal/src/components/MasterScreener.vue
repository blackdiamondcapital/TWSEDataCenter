<script setup>
import { computed } from 'vue'

const props = defineProps({
  rows: { type: Array, default: () => [] },
  total: { type: Number, default: 0 },
  page: { type: Number, default: 1 },
  pageSize: { type: Number, default: 50 },
  loading: { type: Boolean, default: false },
  selectedCode: { type: String, default: '' },
  sort: { type: String, default: 'expiry' },
  sortDir: { type: String, default: 'asc' },
})

const emit = defineEmits(['select', 'page', 'sort'])

const columns = [
  { key: 'market', label: '市場' },
  { key: 'code', label: '代號' },
  { key: 'name', label: '名稱' },
  { key: 'type', label: '類型' },
  { key: 'underlying', label: '標的' },
  { key: 'close', label: '收盤', align: 'num' },
  { key: 'volume', label: '成交量', align: 'num' },
  { key: 'exercise', label: '履約價', align: 'num' },
  { key: 'days', label: '到期天數', align: 'num' },
  { key: 'ratio', label: '行使比', align: 'num' },
  { key: 'expiry', label: '到期日' },
]

const pageCount = computed(() => Math.max(1, Math.ceil((props.total || 0) / props.pageSize)))

function fmt(n, digits = 2) {
  if (n == null || Number.isNaN(Number(n))) return '—'
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: digits })
}

function daysClass(days) {
  if (days == null) return ''
  if (days <= 7) return 'days-urgent'
  if (days <= 30) return 'days-soon'
  return ''
}

function onRow(row) {
  emit('select', row)
}

function sortIndicator(key) {
  if (props.sort !== key) return ''
  return props.sortDir === 'desc' ? '↓' : '↑'
}

function onSort(key) {
  if (props.sort === key) {
    emit('sort', { sort: key, sortDir: props.sortDir === 'asc' ? 'desc' : 'asc' })
  } else {
    emit('sort', { sort: key, sortDir: 'asc' })
  }
}
</script>

<template>
  <div class="screener panel">
    <div class="head">
      <h2>發行主檔</h2>
      <span class="muted">未到期 {{ total.toLocaleString() }} 檔</span>
    </div>

    <div v-if="loading" class="empty muted">查詢中…</div>
    <div v-else-if="!rows.length" class="empty muted">沒有符合條件的權證</div>
    <div v-else class="table-wrap">
      <table class="data">
        <thead>
          <tr>
            <th
              v-for="col in columns"
              :key="col.key"
              class="sortable"
              :class="[{ active: sort === col.key }, col.align]"
              @click="onSort(col.key)"
            >
              <span class="th-label">{{ col.label }}</span>
              <span class="th-ind" :class="{ on: sort === col.key }">{{ sortIndicator(col.key) || '↕' }}</span>
            </th>
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
            <td class="underlying">
              <span v-if="row.underlying_code" class="mono code">{{ row.underlying_code }}</span>
              <span>{{ row.underlying_name || '—' }}</span>
            </td>
            <td class="num mono">{{ fmt(row.close_price, 2) }}</td>
            <td class="num mono">{{ fmt(row.volume, 0) }}</td>
            <td class="num mono">{{ fmt(row.latest_exercise_price) }}</td>
            <td class="num mono" :class="daysClass(row.days_to_expiry)">
              {{ row.days_to_expiry == null ? '—' : row.days_to_expiry }}
            </td>
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
.days-soon { color: #38bdf8; font-weight: 600; }
.days-urgent { color: #ff6b6b; font-weight: 700; }
.underlying {
  display: flex;
  flex-direction: column;
  gap: 0.1rem;
  line-height: 1.25;
}
.underlying .code {
  font-size: 0.78rem;
  opacity: 0.75;
}

th.sortable {
  cursor: pointer;
  user-select: none;
  color: #e8f4ff;
  transition: color 0.15s, background 0.15s, box-shadow 0.15s;
}
th.sortable:hover {
  color: #7dd3fc;
  background: rgba(56, 189, 248, 0.16);
}
th.sortable.active {
  color: #38bdf8;
  background: rgba(56, 189, 248, 0.14);
  box-shadow: inset 0 -2px 0 #38bdf8;
}
th.sortable.num {
  text-align: right;
}
.th-label {
  margin-right: 0.28rem;
  font-weight: 650;
}
.th-ind {
  display: inline-block;
  min-width: 0.9em;
  font-size: 0.78rem;
  color: #9ec9e8;
  opacity: 0.85;
  font-weight: 700;
}
.th-ind.on {
  color: #7dd3fc;
  opacity: 1;
}
</style>
