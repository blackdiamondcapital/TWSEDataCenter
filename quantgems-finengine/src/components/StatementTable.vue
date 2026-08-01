<script setup>
import { computed } from 'vue'
import { changeClass, changePct, formatMoney } from '../utils/format'

const props = defineProps({
  payload: { type: Object, default: null },
  loading: { type: Boolean, default: false },
})

const periods = computed(() => props.payload?.periods || [])
const sections = computed(() => props.payload?.sections || [])

function cell(item, periodValue) {
  return item.values?.[periodValue]
}

function delta(item, periodValue, idx) {
  if (idx >= periods.value.length - 1) return null
  const curr = cell(item, periodValue)
  const prev = cell(item, periods.value[idx + 1]?.value)
  return changePct(curr, prev)
}
</script>

<template>
  <div class="table-shell">
    <div v-if="loading" class="state muted">讀取報表中…</div>
    <div v-else-if="!payload || !sections.length" class="state muted">尚無此報表資料</div>
    <div v-else class="scroller">
      <table>
        <thead>
          <tr>
            <th class="sticky col-item">科目</th>
            <th
              v-for="p in periods"
              :key="p.value"
              class="col-num mono"
            >
              {{ p.label }}
            </th>
          </tr>
        </thead>
        <tbody>
          <template v-for="sec in sections" :key="sec.id">
            <tr class="section-row">
              <td :colspan="periods.length + 1">{{ sec.section }}</td>
            </tr>
            <tr
              v-for="item in sec.items"
              :key="item.key"
              :class="{ emphasis: item.emphasis }"
            >
              <td class="sticky col-item">
                <span class="label">{{ item.label }}</span>
                <span class="key muted mono">{{ item.key }}</span>
              </td>
              <td
                v-for="(p, idx) in periods"
                :key="p.value"
                class="col-num mono"
                :class="changeClass(cell(item, p.value), cell(item, periods[idx + 1]?.value))"
              >
                <div class="val">{{ formatMoney(cell(item, p.value), { isEps: item.isEps }) }}</div>
                <div v-if="delta(item, p.value, idx) != null" class="delta">
                  {{ delta(item, p.value, idx) > 0 ? '+' : '' }}{{ delta(item, p.value, idx).toFixed(1) }}%
                </div>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.table-shell {
  border: 1px solid var(--line);
  background: rgba(10, 12, 18, 0.55);
  min-height: 280px;
}

.state {
  padding: 2.5rem 1.25rem;
  text-align: center;
}

.scroller {
  overflow: auto;
  max-height: min(70vh, 720px);
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 720px;
}

th, td {
  padding: 0.7rem 0.9rem;
  border-bottom: 1px solid rgba(232, 228, 220, 0.06);
  vertical-align: top;
}

thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #10151f;
  color: var(--muted);
  font-weight: 500;
  font-size: 0.78rem;
  letter-spacing: 0.04em;
  border-bottom: 1px solid var(--line-strong);
}

.col-item {
  text-align: left;
  min-width: 180px;
}

.sticky {
  position: sticky;
  left: 0;
  z-index: 1;
  background: #0c1018;
}

thead .sticky {
  z-index: 3;
  background: #10151f;
}

.col-num {
  text-align: right;
  min-width: 118px;
}

.section-row td {
  background: linear-gradient(90deg, rgba(45, 212, 191, 0.1), transparent 70%);
  color: var(--aqua);
  font-family: var(--display);
  font-size: 0.85rem;
  letter-spacing: 0.12em;
  padding-top: 1rem;
  border-bottom-color: rgba(45, 212, 191, 0.18);
}

.emphasis .label {
  color: var(--paper);
  font-weight: 600;
}

.emphasis .val {
  color: var(--brass);
  font-weight: 600;
}

.label {
  display: block;
}

.key {
  display: block;
  font-size: 0.68rem;
  margin-top: 0.15rem;
  opacity: 0.7;
}

.val { font-size: 0.92rem; }

.delta {
  font-size: 0.68rem;
  opacity: 0.8;
  margin-top: 0.1rem;
}

.up .delta { color: var(--up); }
.down .delta { color: var(--down); }
</style>
