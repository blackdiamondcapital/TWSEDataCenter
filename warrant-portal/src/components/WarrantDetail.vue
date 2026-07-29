<script setup>
import { computed } from 'vue'

const props = defineProps({
  detail: { type: Object, default: null },
  loading: { type: Boolean, default: false },
})

const emit = defineEmits(['close'])

const fields = computed(() => {
  const d = props.detail
  if (!d) return []
  let days = d.days_to_expiry
  if (days == null && d.expiry_date) {
    const t = Date.parse(d.expiry_date)
    if (Number.isFinite(t)) {
      const today = new Date()
      today.setHours(0, 0, 0, 0)
      days = Math.round((t - today.getTime()) / 86400000)
    }
  }
  return [
    ['市場', d.market],
    ['代號', d.warrant_code],
    ['名稱', d.warrant_name],
    ['類型', d.warrant_type],
    ['類別', d.warrant_category],
    ['標的代號', d.underlying_code],
    ['標的', d.underlying_name],
    ['最新收盤', d.latest_close_price],
    ['漲跌', d.latest_price_change],
    ['最近成交日', d.latest_trade_date],
    ['履約價', d.latest_exercise_price],
    ['行使比例', d.latest_exercise_ratio],
    ['發行量', d.issuance_units_thousand ?? d.accumulated_issuance ?? d.issuance],
    ['發行日', d.issue_date || d.listed_date || d.exercise_start_date],
    ['最後交易日', d.last_trade_date],
    ['到期日', d.expiry_date],
    ['到期天數', days],
    ['出表日', d.report_date],
  ].filter(([, v]) => v != null && v !== '')
})

function fmt(v) {
  if (typeof v === 'number') return v.toLocaleString()
  return v
}
</script>

<template>
  <aside class="detail panel" v-if="detail || loading">
    <div class="head">
      <h2>權證詳情</h2>
      <button class="ghost" @click="emit('close')">關閉</button>
    </div>
    <div v-if="loading" class="muted">載入詳情…</div>
    <template v-else-if="detail">
      <dl class="grid">
        <template v-for="([label, value]) in fields" :key="label">
          <dt>{{ label }}</dt>
          <dd class="mono">{{ fmt(value) }}</dd>
        </template>
      </dl>
      <div v-if="detail.recent_trades?.length" class="recent">
        <h3>最近成交</h3>
        <ul>
          <li v-for="t in detail.recent_trades" :key="t.trade_date">
            <span>{{ t.trade_date }}</span>
            <span class="mono">收 {{ t.close_price ?? '—' }}</span>
            <span class="mono">金額 {{ t.turnover?.toLocaleString?.() ?? '—' }}</span>
            <span class="mono">量 {{ t.volume?.toLocaleString?.() ?? '—' }}</span>
          </li>
        </ul>
      </div>
    </template>
  </aside>
</template>

<style scoped>
.detail {
  padding: 1rem 1.1rem 1.2rem;
  position: sticky;
  top: 1rem;
}
.head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 0.85rem;
}
.head h2 {
  margin: 0;
  font-size: 1.05rem;
}
button.ghost {
  background: transparent;
  padding: 0.35rem 0.65rem;
}
.grid {
  display: grid;
  grid-template-columns: 7rem 1fr;
  gap: 0.45rem 0.75rem;
  margin: 0;
}
.grid dt {
  color: var(--text-dim);
  font-size: 0.85rem;
}
.grid dd {
  margin: 0;
  font-size: 0.92rem;
}
.recent { margin-top: 1.1rem; }
.recent h3 {
  margin: 0 0 0.5rem;
  font-size: 0.95rem;
}
.recent ul {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 0.4rem;
}
.recent li {
  display: grid;
  grid-template-columns: 6.5rem 4.5rem 1fr 1fr;
  gap: 0.5rem;
  font-size: 0.82rem;
  color: var(--text-dim);
}
</style>
