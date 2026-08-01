<script setup>
import { onMounted, ref, watch } from 'vue'
import { api } from '../api'
import SymbolSearch from './SymbolSearch.vue'
import HighlightStrip from './HighlightStrip.vue'
import TrendChart from './TrendChart.vue'
import StatementTable from './StatementTable.vue'

const props = defineProps({
  seedCode: { type: String, default: '2330' },
})
defineEmits(['back'])

const code = ref(props.seedCode || '2330')
const symbol = ref(null)
const overview = ref(null)
const kind = ref('income')
const statement = ref(null)
const loadingOverview = ref(false)
const loadingStatement = ref(false)
const error = ref('')

const tabs = [
  { id: 'income', label: '損益表' },
  { id: 'balance', label: '資產負債' },
  { id: 'cashflow', label: '現金流量' },
]

async function loadAll(nextCode) {
  const c = (nextCode || code.value || '').trim()
  if (!c) return
  code.value = c
  error.value = ''
  loadingOverview.value = true
  loadingStatement.value = true
  try {
    const ov = await api.overview(c)
    overview.value = ov
    symbol.value = ov.symbol
    const st = await api.statement(c, kind.value, 8)
    statement.value = st
  } catch (e) {
    error.value = e?.message || '載入失敗'
    overview.value = null
    statement.value = null
  } finally {
    loadingOverview.value = false
    loadingStatement.value = false
  }
}

async function loadStatement() {
  if (!code.value) return
  loadingStatement.value = true
  error.value = ''
  try {
    statement.value = await api.statement(code.value, kind.value, 8)
  } catch (e) {
    error.value = e?.message || '報表載入失敗'
    statement.value = null
  } finally {
    loadingStatement.value = false
  }
}

function onSelect(item) {
  loadAll(item.code)
}

watch(kind, () => {
  loadStatement()
})

onMounted(() => loadAll(props.seedCode || '2330'))
</script>

<template>
  <section class="engine">
    <header class="bar">
      <button class="ghost" type="button" @click="$emit('back')">← 首頁</button>
      <div class="brand-line">
        <span class="mark display">QUANTGEMS</span>
        <span class="sep muted">/</span>
        <span class="prod">財報引擎</span>
      </div>
      <div class="spacer" />
    </header>

    <div class="search-block">
      <SymbolSearch :initial="code" @select="onSelect" />
    </div>

    <div v-if="error" class="error">{{ error }}</div>

    <div v-if="symbol" class="identity anim-rise">
      <div>
        <h1 class="display">
          <span class="code mono">{{ symbol.code }}</span>
          {{ symbol.label }}
        </h1>
        <p class="muted meta">
          <span v-if="symbol.market">{{ symbol.market }}</span>
          <span v-if="symbol.industry"> · {{ symbol.industry }}</span>
          <span v-if="overview?.latestPeriod"> · 最新 {{ overview.latestPeriod }}</span>
        </p>
      </div>
      <p class="tagline muted">季報切面 · 多期對照</p>
    </div>

    <HighlightStrip
      v-if="overview?.highlight"
      :highlight="overview.highlight"
      :ratios="overview.ratios"
    />

    <div class="grid">
      <TrendChart v-if="overview?.trend?.length" :trend="overview.trend" />
      <div class="statement-pane">
        <div class="tabs">
          <button
            v-for="t in tabs"
            :key="t.id"
            class="tab"
            :class="{ active: kind === t.id }"
            type="button"
            @click="kind = t.id"
          >
            {{ t.label }}
          </button>
        </div>
        <StatementTable :payload="statement" :loading="loadingStatement || loadingOverview" />
      </div>
    </div>
  </section>
</template>

<style scoped>
.engine {
  min-height: 100vh;
  min-height: 100dvh;
  padding: clamp(1rem, 2.5vw, 1.75rem);
  max-width: 1280px;
  margin: 0 auto;
}

.bar {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.brand-line {
  display: flex;
  align-items: baseline;
  gap: 0.45rem;
  letter-spacing: 0.08em;
}

.mark {
  color: var(--aqua);
  font-weight: 800;
  font-size: 1rem;
}

.prod {
  color: var(--brass);
  font-size: 0.92rem;
}

.spacer { flex: 1; }

.search-block {
  max-width: 560px;
  margin-bottom: 1.5rem;
}

.identity {
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  gap: 1rem;
  margin-bottom: 1.25rem;
}

h1 {
  margin: 0;
  font-size: clamp(1.6rem, 3.5vw, 2.4rem);
  letter-spacing: 0.02em;
  font-weight: 700;
}

.code {
  color: var(--aqua);
  margin-right: 0.35rem;
}

.meta {
  margin: 0.35rem 0 0;
  font-size: 0.88rem;
}

.tagline {
  font-family: var(--mono);
  font-size: 0.75rem;
  letter-spacing: 0.08em;
}

.grid {
  display: grid;
  gap: 1.25rem;
  margin-top: 1.25rem;
}

.statement-pane {
  display: grid;
  gap: 0.75rem;
}

.tabs {
  display: flex;
  gap: 1.35rem;
  border-bottom: 1px solid var(--line);
}

.error {
  color: var(--down);
  border: 1px solid rgba(224, 122, 106, 0.35);
  background: rgba(224, 122, 106, 0.08);
  padding: 0.75rem 1rem;
  margin-bottom: 1rem;
  font-size: 0.9rem;
}

@media (min-width: 1100px) {
  .grid {
    grid-template-columns: 1fr;
  }
}
</style>
