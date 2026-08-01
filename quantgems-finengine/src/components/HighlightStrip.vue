<script setup>
import { computed } from 'vue'
import { formatMoney, formatRatio } from '../utils/format'

const props = defineProps({
  highlight: { type: Object, default: null },
  ratios: { type: Array, default: () => [] },
})

const latestRatio = computed(() => props.ratios?.[0] || null)

const tiles = computed(() => {
  const h = props.highlight || {}
  const r = latestRatio.value || {}
  return [
    { k: '營收', v: formatMoney(h.revenue), sub: h.periodLabel || '' },
    { k: '營業利益', v: formatMoney(h.op_profit), sub: h.periodLabel || '' },
    { k: '本期淨利', v: formatMoney(h.net_profit), sub: h.periodLabel || '' },
    { k: 'EPS', v: formatMoney(h.eps, { isEps: true }), sub: '基本' },
    { k: '毛利率', v: formatRatio('gross_margin', r.gross_margin), sub: r.periodLabel || '' },
    { k: 'ROE', v: formatRatio('roe', r.roe), sub: r.periodLabel || '' },
  ]
})
</script>

<template>
  <div class="strip" v-if="highlight">
    <div v-for="t in tiles" :key="t.k" class="tile">
      <div class="k">{{ t.k }}</div>
      <div class="v mono">{{ t.v }}</div>
      <div class="s muted">{{ t.sub }}</div>
    </div>
  </div>
</template>

<style scoped>
.strip {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 0;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.tile {
  padding: 1rem 0.9rem;
  border-right: 1px solid var(--line);
  min-width: 0;
}

.tile:last-child { border-right: none; }

.k {
  font-size: 0.72rem;
  letter-spacing: 0.12em;
  color: var(--muted);
}

.v {
  margin-top: 0.35rem;
  font-size: clamp(1rem, 1.8vw, 1.25rem);
  font-weight: 600;
  color: var(--paper);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.s {
  margin-top: 0.2rem;
  font-size: 0.72rem;
  font-family: var(--mono);
}

@media (max-width: 960px) {
  .strip { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .tile:nth-child(3n) { border-right: none; }
  .tile:nth-child(n + 4) { border-top: 1px solid var(--line); }
}

@media (max-width: 520px) {
  .strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .tile { border-right: 1px solid var(--line); }
  .tile:nth-child(2n) { border-right: none; }
  .tile:nth-child(n + 3) { border-top: 1px solid var(--line); }
}
</style>
