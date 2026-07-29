<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({
  detail: { type: Object, default: null },
  loading: { type: Boolean, default: false },
  /** 技術分析全螢幕時改為浮在底部 */
  overlay: { type: Boolean, default: false },
})

const emit = defineEmits(['close', 'open-chart'])

const expanded = ref(false)
const tradesOpen = ref(false)

watch(
  () => props.detail?.warrant_code,
  () => {
    expanded.value = false
    tradesOpen.value = false
  },
)

const daysToExpiry = computed(() => {
  const d = props.detail
  if (!d) return null
  if (d.days_to_expiry != null) return d.days_to_expiry
  if (!d.expiry_date) return null
  const t = Date.parse(d.expiry_date)
  if (!Number.isFinite(t)) return null
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  return Math.round((t - today.getTime()) / 86400000)
})

const isCall = computed(() => {
  const t = String(props.detail?.warrant_type || '')
  return t.includes('認購') || t.toLowerCase() === 'call'
})

const headline = computed(() => {
  const d = props.detail
  if (!d) return ''
  return [d.warrant_code, d.warrant_name].filter(Boolean).join(' · ')
})

const metrics = computed(() => {
  const d = props.detail
  if (!d) return []
  return [
    { label: '收盤', value: fmt(d.latest_close_price) },
    { label: '履約價', value: fmt(d.latest_exercise_price) },
    { label: '行使比例', value: fmt(d.latest_exercise_ratio) },
    { label: '到期天數', value: daysToExpiry.value != null ? String(daysToExpiry.value) : null },
    { label: '到期日', value: d.expiry_date || null },
  ].filter((m) => m.value != null && m.value !== '')
})

const moreFields = computed(() => {
  const d = props.detail
  if (!d) return []
  return [
    ['市場', d.market],
    ['類別', d.warrant_category],
    ['標的', [d.underlying_code, d.underlying_name].filter(Boolean).join(' ')],
    ['漲跌', d.latest_price_change],
    ['最近成交日', d.latest_trade_date],
    ['發行量', d.issuance_units_thousand ?? d.accumulated_issuance ?? d.issuance],
    ['發行日', d.issue_date || d.listed_date || d.exercise_start_date],
    ['最後交易日', d.last_trade_date],
    ['出表日', d.report_date],
  ].filter(([, v]) => v != null && v !== '')
})

function fmt(v) {
  if (v == null || v === '') return null
  if (typeof v === 'number') return v.toLocaleString()
  return String(v)
}
</script>

<template>
  <aside
    v-if="detail || loading"
    class="sheet"
    :class="{ overlay, expanded }"
  >
    <div class="sheet-bar">
      <div class="id-block">
        <div class="id-top">
          <span v-if="detail?.market" class="pill market">{{ detail.market }}</span>
          <span
            v-if="detail?.warrant_type"
            class="pill type"
            :class="isCall ? 'call' : 'put'"
          >{{ detail.warrant_type }}</span>
          <strong class="title">{{ loading ? '載入詳情…' : headline }}</strong>
        </div>
        <p v-if="detail && !loading" class="sub">
          標的
          <span class="mono">{{ detail.underlying_code }}</span>
          {{ detail.underlying_name || '' }}
        </p>
      </div>

      <div v-if="!loading && metrics.length" class="metrics">
        <div v-for="m in metrics" :key="m.label" class="metric">
          <span class="m-label">{{ m.label }}</span>
          <span class="m-value mono">{{ m.value }}</span>
        </div>
      </div>

      <div class="actions">
        <button
          v-if="!loading && moreFields.length"
          type="button"
          class="ghost"
          @click="expanded = !expanded"
        >
          {{ expanded ? '收合' : '更多參數' }}
        </button>
        <button
          v-if="!overlay"
          type="button"
          class="ghost accent"
          @click="emit('open-chart')"
        >
          技術分析
        </button>
        <button type="button" class="ghost" @click="emit('close')">關閉</button>
      </div>
    </div>

    <div v-if="expanded && !loading && detail" class="sheet-extra">
      <div class="extra-cards">
        <div v-for="([label, value]) in moreFields" :key="label" class="extra-card">
          <span class="m-label">{{ label }}</span>
          <span class="m-value mono">{{ fmt(value) }}</span>
        </div>
      </div>

      <div v-if="detail.recent_trades?.length" class="trades">
        <button type="button" class="trades-toggle" @click="tradesOpen = !tradesOpen">
          最近成交（{{ detail.recent_trades.length }}）
          <span>{{ tradesOpen ? '▴' : '▾' }}</span>
        </button>
        <ul v-if="tradesOpen">
          <li v-for="t in detail.recent_trades" :key="t.trade_date">
            <span>{{ t.trade_date }}</span>
            <span class="mono">收 {{ t.close_price ?? '—' }}</span>
            <span class="mono">金額 {{ t.turnover?.toLocaleString?.() ?? '—' }}</span>
            <span class="mono">量 {{ t.volume?.toLocaleString?.() ?? '—' }}</span>
          </li>
        </ul>
      </div>
    </div>
  </aside>
</template>

<style scoped>
.sheet {
  border: 1px solid rgba(148, 183, 205, 0.16);
  border-radius: 14px;
  background: rgba(8, 14, 22, 0.92);
  backdrop-filter: blur(10px);
  box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28);
  overflow: hidden;
}
.sheet.overlay {
  position: fixed;
  left: 50%;
  bottom: 0.85rem;
  transform: translateX(-50%);
  width: min(1100px, calc(100vw - 1.5rem));
  z-index: 10050;
  border-color: rgba(0, 212, 255, 0.28);
}
.sheet-bar {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(0, 1.8fr) auto;
  gap: 0.75rem 1rem;
  align-items: center;
  padding: 0.7rem 0.9rem;
}
.id-block {
  min-width: 0;
}
.id-top {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.35rem 0.45rem;
}
.title {
  font-size: 0.95rem;
  font-weight: 650;
  color: #e8f1f7;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 100%;
}
.sub {
  margin: 0.25rem 0 0;
  font-size: 0.78rem;
  color: #8fa3b3;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.pill {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.12rem 0.48rem;
  font-size: 0.7rem;
  font-weight: 600;
  border: 1px solid transparent;
  flex-shrink: 0;
}
.pill.market {
  color: #7dd3fc;
  background: rgba(56, 189, 248, 0.12);
  border-color: rgba(56, 189, 248, 0.28);
}
.pill.type.call {
  color: #fda4af;
  background: rgba(239, 68, 68, 0.12);
  border-color: rgba(239, 68, 68, 0.3);
}
.pill.type.put {
  color: #86efac;
  background: rgba(34, 197, 94, 0.12);
  border-color: rgba(34, 197, 94, 0.3);
}
.metrics {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 0.35rem;
}
.metric,
.extra-card {
  min-width: 0;
  border: 1px solid rgba(148, 183, 205, 0.12);
  border-radius: 8px;
  padding: 0.28rem 0.4rem;
  background: rgba(2, 8, 14, 0.35);
}
.m-label {
  display: block;
  font-size: 0.68rem;
  color: #8fa3b3;
  margin-bottom: 0.08rem;
}
.m-value {
  display: block;
  font-size: 0.84rem;
  color: #eef5f8;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
  justify-content: flex-end;
}
button.ghost {
  background: transparent;
  border: 1px solid rgba(148, 183, 205, 0.22);
  color: #c2cce0;
  border-radius: 8px;
  padding: 0.32rem 0.6rem;
  font-size: 0.78rem;
  cursor: pointer;
}
button.ghost:hover {
  border-color: rgba(0, 212, 255, 0.45);
  color: #e8f7ff;
}
button.ghost.accent {
  border-color: rgba(0, 212, 255, 0.45);
  color: #00d4ff;
  background: rgba(0, 212, 255, 0.08);
}
.sheet-extra {
  border-top: 1px solid rgba(148, 183, 205, 0.12);
  padding: 0.65rem 0.9rem 0.85rem;
  max-height: min(42vh, 320px);
  overflow: auto;
}
.extra-cards {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.4rem;
}
.trades {
  margin-top: 0.35rem;
}
.trades-toggle {
  width: 100%;
  display: flex;
  justify-content: space-between;
  background: transparent;
  border: 0;
  border-top: 1px solid rgba(148, 183, 205, 0.12);
  color: #9bb0c0;
  padding: 0.55rem 0 0.2rem;
  font-size: 0.8rem;
  cursor: pointer;
}
.trades ul {
  list-style: none;
  margin: 0.35rem 0 0;
  padding: 0;
  display: grid;
  gap: 0.3rem;
}
.trades li {
  display: grid;
  grid-template-columns: 6.5rem 4.5rem 1fr 1fr;
  gap: 0.45rem;
  font-size: 0.78rem;
  color: #8fa3b3;
}
@media (max-width: 900px) {
  .sheet-bar {
    grid-template-columns: 1fr;
  }
  .metrics {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
  .actions {
    justify-content: flex-start;
  }
  .extra-cards {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
@media (max-width: 560px) {
  .metrics,
  .extra-cards {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .trades li {
    grid-template-columns: 1fr 1fr;
  }
}
</style>
