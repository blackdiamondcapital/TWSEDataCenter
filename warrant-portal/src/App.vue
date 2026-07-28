<script setup>
import { ref, reactive, onMounted, watch } from 'vue'
import {
  fetchPortalStats,
  fetchMasterSearch,
  fetchMasterDetail,
  fetchDates,
  fetchRankings,
  fetchTimeseries,
  importLatestWarrants,
} from './api'
import MasterScreener from './components/MasterScreener.vue'
import RankingPanel from './components/RankingPanel.vue'
import WarrantChart from './components/WarrantChart.vue'
import WarrantDetail from './components/WarrantDetail.vue'

const stats = ref(null)
const statusText = ref('')
const importing = ref(false)

const filters = reactive({
  q: '',
  market: 'both',
  type: '',
  expiryFrom: '',
  expiryTo: '',
  sort: 'expiry',
  sortDir: 'asc',
  page: 1,
  pageSize: 50,
})

const masterRows = ref([])
const masterTotal = ref(0)
const loadingMaster = ref(false)

const dates = ref([])
const selectedDate = ref('')
const heatMarket = ref('both')
const metric = ref('turnover')
const rankings = ref([])
const loadingRankings = ref(false)

const selected = ref(null)
const detail = ref(null)
const loadingDetail = ref(false)
const timeseries = ref([])
const loadingSeries = ref(false)

async function loadStats() {
  try {
    stats.value = await fetchPortalStats()
  } catch (err) {
    console.error(err)
    statusText.value = `統計載入失敗：${err.message}`
  }
}

async function loadMaster() {
  loadingMaster.value = true
  try {
    const data = await fetchMasterSearch({
      q: filters.q || undefined,
      market: filters.market,
      type: filters.type || undefined,
      expiryFrom: filters.expiryFrom || undefined,
      expiryTo: filters.expiryTo || undefined,
      sort: filters.sort,
      sortDir: filters.sortDir,
      page: filters.page,
      pageSize: filters.pageSize,
    })
    masterRows.value = data.data || []
    masterTotal.value = data.total || 0
    statusText.value = `主檔 ${data.total?.toLocaleString?.() || 0} 檔 · 顯示第 ${data.page} 頁`
  } catch (err) {
    console.error(err)
    masterRows.value = []
    masterTotal.value = 0
    statusText.value = `主檔查詢失敗：${err.message}`
  } finally {
    loadingMaster.value = false
  }
}

async function loadDates() {
  try {
    const list = await fetchDates(120, heatMarket.value)
    dates.value = list
    if (list.length && !selectedDate.value) selectedDate.value = list[0]
  } catch (err) {
    console.error(err)
  }
}

async function loadRankings() {
  loadingRankings.value = true
  try {
    const data = await fetchRankings({
      date: selectedDate.value || undefined,
      metric: metric.value,
      market: heatMarket.value,
      limit: 80,
    })
    rankings.value = data.rows || []
    if (data.date) selectedDate.value = data.date
  } catch (err) {
    console.error(err)
    rankings.value = []
    statusText.value = `排行失敗：${err.message}`
  } finally {
    loadingRankings.value = false
  }
}

async function selectWarrant(row) {
  if (!row?.warrant_code) return
  selected.value = row
  loadingDetail.value = true
  loadingSeries.value = true
  timeseries.value = []
  try {
    const [detailResp, seriesResp] = await Promise.all([
      fetchMasterDetail(row.warrant_code).catch(() => null),
      fetchTimeseries({ code: row.warrant_code, limitDays: 90 }),
    ])
    detail.value = detailResp?.data || {
      market: row.market,
      warrant_code: row.warrant_code,
      warrant_name: row.warrant_name,
      warrant_type: row.warrant_type,
      underlying_name: row.underlying_name,
      underlying_code: row.underlying_code,
      latest_exercise_price: row.latest_exercise_price,
      latest_exercise_ratio: row.latest_exercise_ratio,
      expiry_date: row.expiry_date,
      issuance: row.issuance,
    }
    timeseries.value = seriesResp.data || []
  } catch (err) {
    console.error(err)
    statusText.value = `載入詳情失敗：${err.message}`
  } finally {
    loadingDetail.value = false
    loadingSeries.value = false
  }
}

function onSearch() {
  filters.page = 1
  loadMaster()
}

function onPage(p) {
  filters.page = p
  loadMaster()
}

async function onImportLatest() {
  importing.value = true
  statusText.value = '正在同步 TWSE 最新成交…'
  try {
    const resp = await importLatestWarrants()
    statusText.value = `${resp.message || '匯入完成'}${resp.tradeDate ? `（${resp.tradeDate}）` : ''}`
    await loadDates()
    await loadRankings()
    await loadStats()
  } catch (err) {
    statusText.value = `匯入失敗：${err.message}`
  } finally {
    importing.value = false
  }
}

watch(metric, () => loadRankings())
watch(heatMarket, async () => {
  selectedDate.value = ''
  await loadDates()
  await loadRankings()
})
watch(() => [filters.sort, filters.sortDir], () => {
  filters.page = 1
  loadMaster()
})

onMounted(async () => {
  await Promise.all([loadStats(), loadMaster(), loadDates()])
  await loadRankings()
})
</script>

<template>
  <div class="app">
    <header class="hero">
      <div class="hero-copy">
        <p class="eyebrow">BlackDiamond · Warrant Radar</p>
        <h1>權證雷達</h1>
        <p class="lede">篩選全市場發行主檔，並追蹤當日成交熱度與單檔走勢。</p>
      </div>
      <div class="hero-stats" v-if="stats">
        <div class="stat">
          <span class="label">主檔總數</span>
          <strong>{{ stats.total_master?.toLocaleString?.() }}</strong>
        </div>
        <div class="stat">
          <span class="label">上市</span>
          <strong>{{ stats.twse?.master_total?.toLocaleString?.() }}</strong>
        </div>
        <div class="stat">
          <span class="label">上櫃</span>
          <strong>{{ stats.tpex?.master_total?.toLocaleString?.() }}</strong>
        </div>
        <div class="stat">
          <span class="label">最新成交日</span>
          <strong>{{ stats.twse?.latest_trade_date || stats.tpex?.latest_trade_date || '—' }}</strong>
        </div>
      </div>
    </header>

    <section class="search-bar panel">
      <div class="search-main">
        <label>搜尋標的／股票代號／權證代號／名稱</label>
        <input
          v-model="filters.q"
          placeholder="例如：2330、台積電、03002T、群益"
          @keyup.enter="onSearch"
        />
      </div>
      <div class="filters">
        <div>
          <label>市場</label>
          <select v-model="filters.market">
            <option value="both">全市場</option>
            <option value="twse">上市 TWSE</option>
            <option value="tpex">上櫃 TPEX</option>
          </select>
        </div>
        <div>
          <label>類型</label>
          <select v-model="filters.type">
            <option value="">全部</option>
            <option value="認購">認購</option>
            <option value="認售">認售</option>
          </select>
        </div>
        <div>
          <label>到期起</label>
          <input type="date" v-model="filters.expiryFrom" />
        </div>
        <div>
          <label>到期迄</label>
          <input type="date" v-model="filters.expiryTo" />
        </div>
        <div>
          <label>排序</label>
          <select v-model="filters.sort">
            <option value="expiry">到期日</option>
            <option value="days">到期天數</option>
            <option value="exercise">履約價</option>
            <option value="code">代號</option>
            <option value="name">名稱</option>
          </select>
        </div>
        <div>
          <label>升降冪</label>
          <select v-model="filters.sortDir">
            <option value="asc">升冪 ↑</option>
            <option value="desc">降冪 ↓</option>
          </select>
        </div>
      </div>
      <div class="actions">
        <button class="primary" @click="onSearch">搜尋主檔</button>
        <button :disabled="importing" @click="onImportLatest">
          {{ importing ? '同步中…' : '同步最新成交' }}
        </button>
      </div>
    </section>

    <p class="status muted">{{ statusText }}</p>

    <div class="workspace">
      <div class="col-main">
        <MasterScreener
          :rows="masterRows"
          :total="masterTotal"
          :page="filters.page"
          :page-size="filters.pageSize"
          :loading="loadingMaster"
          :selected-code="selected?.warrant_code || ''"
          @select="selectWarrant"
          @page="onPage"
        />

        <div class="heat-controls panel">
          <div>
            <label>熱度日期</label>
            <select v-model="selectedDate" @change="loadRankings">
              <option v-for="d in dates" :key="d" :value="d">{{ d }}</option>
            </select>
          </div>
          <div>
            <label>熱度市場</label>
            <select v-model="heatMarket">
              <option value="both">全市場</option>
              <option value="twse">上市</option>
              <option value="tpex">上櫃</option>
            </select>
          </div>
          <div class="metric-toggle">
            <label>指標</label>
            <div class="btns">
              <button :class="{ active: metric === 'turnover' }" @click="metric = 'turnover'">成交金額</button>
              <button :class="{ active: metric === 'volume' }" @click="metric = 'volume'">成交張數</button>
            </div>
          </div>
        </div>

        <div class="heat-grid">
          <RankingPanel
            :rows="rankings"
            :loading="loadingRankings"
            :selected-code="selected?.warrant_code || ''"
            :metric="metric"
            @select="selectWarrant"
          />
          <WarrantChart
            :code="selected?.warrant_code || ''"
            :name="selected?.warrant_name || ''"
            :series="timeseries"
            :loading="loadingSeries"
          />
        </div>
      </div>

      <div class="col-side">
        <WarrantDetail
          :detail="detail"
          :loading="loadingDetail"
          @close="detail = null"
        />
      </div>
    </div>
  </div>
</template>

<style scoped>
.app {
  width: min(1280px, calc(100% - 2rem));
  margin: 0 auto;
  padding: 1.5rem 0 3rem;
}

.hero {
  display: grid;
  grid-template-columns: 1.3fr 1fr;
  gap: 1.5rem;
  align-items: end;
  margin-bottom: 1.25rem;
  animation: rise 0.7s ease both;
}
.eyebrow {
  margin: 0 0 0.35rem;
  color: var(--cyan);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-size: 0.78rem;
  font-weight: 600;
}
.hero h1 {
  margin: 0;
  font-family: 'Newsreader', Georgia, serif;
  font-size: clamp(2.4rem, 5vw, 3.4rem);
  font-weight: 600;
  letter-spacing: -0.02em;
  animation: rise 0.85s ease both;
}
.lede {
  margin: 0.55rem 0 0;
  color: var(--text-dim);
  max-width: 34rem;
  font-size: 1.02rem;
}
.hero-stats {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.75rem;
}
.stat {
  background: rgba(16, 24, 32, 0.7);
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 0.85rem 1rem;
}
.stat .label {
  display: block;
  color: var(--text-dim);
  font-size: 0.78rem;
  margin-bottom: 0.25rem;
}
.stat strong {
  font-size: 1.25rem;
  font-variant-numeric: tabular-nums;
}

.search-bar {
  display: grid;
  gap: 0.9rem;
  padding: 1rem 1.1rem 1.15rem;
  margin-bottom: 0.75rem;
  animation: rise 1s ease both;
}
.search-main label,
.filters label,
.heat-controls label {
  display: block;
  margin-bottom: 0.35rem;
  color: var(--text-dim);
  font-size: 0.8rem;
}
.filters {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 0.75rem;
}
.actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
}
.status {
  margin: 0 0 1rem;
  min-height: 1.25rem;
  font-size: 0.9rem;
}

.workspace {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 300px;
  gap: 1rem;
  align-items: start;
}
.col-main {
  display: grid;
  gap: 1rem;
}
.heat-controls {
  display: grid;
  grid-template-columns: 1fr 1fr 1.2fr;
  gap: 0.85rem;
  padding: 0.9rem 1.05rem;
}
.metric-toggle .btns {
  display: flex;
  gap: 0.45rem;
}
.heat-grid {
  display: grid;
  grid-template-columns: 0.9fr 1.1fr;
  gap: 1rem;
}

@keyframes rise {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

@media (max-width: 980px) {
  .hero,
  .workspace,
  .heat-grid,
  .filters,
  .heat-controls {
    grid-template-columns: 1fr;
  }
  .col-side { order: -1; }
}
</style>
