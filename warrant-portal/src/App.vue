<script setup>
import { ref, reactive, computed, onMounted, watch, nextTick } from 'vue'
import {
  fetchPortalStats,
  fetchMasterSearch,
  fetchMasterDetail,
  fetchDates,
  fetchRankings,
  importLatestWarrants,
} from './api'
import { buildOAuthStartUrl } from './lib/oauthStart'
import { useAuth } from './lib/auth'
import MasterScreener from './components/MasterScreener.vue'
import RankingPanel from './components/RankingPanel.vue'
import StockChartECharts from './components/StockChartECharts.vue'
import WarrantDetail from './components/WarrantDetail.vue'
import PwaInstallPrompt from './components/PwaInstallPrompt.vue'

const {
  isAuthenticated,
  displayName,
  planLabel,
  isAdmin,
  user,
  setToken,
  fetchCurrentUser,
  logout,
  consumeOAuthCallbackFromUrl,
} = useAuth()

const authStatus = ref('')

function handleGoogleLogin() {
  const url = buildOAuthStartUrl('google')
  window.location.href = url
}

async function handleLogout() {
  try {
    // 登出後關閉技術分析全螢幕
    if (chartFullscreen.value) {
      document.exitFullscreen?.().catch?.(() => {})
      chartFullscreen.value = false
    }
  } catch {
    /* ignore */
  }
  await logout()
  authStatus.value = '已登出'
  statusText.value = '已登出，技術分析需重新登入後才能查看'
}

const stats = ref(null)
const statusText = ref('')
const importing = ref(false)

const filters = reactive({
  q: '',
  market: 'both',
  type: '',
  expiryFrom: '',
  expiryTo: '',
  closeMin: '',
  closeMax: '',
  exerciseMin: '',
  exerciseMax: '',
  daysMin: '',
  daysMax: '',
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
const heatType = ref('') // '' | '認購' | '認售'
const metric = ref('turnover')
const rankings = ref([])
const loadingRankings = ref(false)

const selected = ref(null)
const detail = ref(null)
const loadingDetail = ref(false)
const techChartRef = ref(null)
const chartFullscreen = ref(false)

async function loadStats() {
  try {
    stats.value = await fetchPortalStats()
  } catch (err) {
    console.error(err)
    statusText.value = `統計載入失敗：${err.message}`
  }
}

/** 全市場最新成交日：取上市／上櫃兩者較新者（勿只用 TWSE，否則上櫃較新時會顯示過舊） */
const latestTradeDate = computed(() => {
  const a = stats.value?.twse?.latest_trade_date
  const b = stats.value?.tpex?.latest_trade_date
  if (a && b) return a >= b ? a : b
  return a || b || '—'
})

function numOrUndef(v) {
  if (v === '' || v == null) return undefined
  const n = Number(v)
  return Number.isFinite(n) ? n : undefined
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
      closeMin: numOrUndef(filters.closeMin),
      closeMax: numOrUndef(filters.closeMax),
      exerciseMin: numOrUndef(filters.exerciseMin),
      exerciseMax: numOrUndef(filters.exerciseMax),
      daysMin: numOrUndef(filters.daysMin),
      daysMax: numOrUndef(filters.daysMax),
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

let rankingsReqId = 0
const rankingsMeta = ref({ kind: 'all', type: null })
const rankingsError = ref('')

function heatTypeToKind(v) {
  if (v === '認購') return 'call'
  if (v === '認售') return 'put'
  return 'all'
}

function setHeatType(next) {
  heatType.value = next
  loadRankings()
}

function setMetric(next) {
  metric.value = next
  loadRankings()
}

async function loadRankings() {
  const reqId = ++rankingsReqId
  const expectedKind = heatTypeToKind(heatType.value)
  const expectedType = heatType.value || null
  loadingRankings.value = true
  rankingsError.value = ''
  try {
    const requestOnce = async (dateOverride) =>
      fetchRankings({
        date: dateOverride,
        metric: metric.value,
        market: heatMarket.value,
        type: expectedKind === 'all' ? '' : expectedKind,
        limit: 80,
      })

    let data = await requestOnce(selectedDate.value || undefined)
    if (reqId === rankingsReqId && !(data.rows || []).length && selectedDate.value) {
      data = await requestOnce(undefined)
      if (data.date) selectedDate.value = data.date
    }
    if (reqId !== rankingsReqId) return

    // 顯示層防線：若回傳混入錯類型，依類型／名稱再過濾
    let rows = data.rows || []
    if (expectedType === '認購') {
      rows = rows.filter(
        (r) => r.warrant_type === '認購' || String(r.warrant_name || '').includes('購'),
      )
    } else if (expectedType === '認售') {
      rows = rows.filter(
        (r) => r.warrant_type === '認售' || String(r.warrant_name || '').includes('售'),
      )
    }

    rankings.value = rows.map((r, i) => ({ ...r, rank: i + 1 }))
    rankingsMeta.value = { kind: data.kind || expectedKind, type: data.type || expectedType }
    if (data.date) selectedDate.value = data.date
    if (!rows.length) {
      rankingsError.value = expectedType
        ? `沒有「${expectedType}」成交熱度（${data.date || selectedDate.value || '—'}）`
        : `沒有成交資料（日期 ${data.date || selectedDate.value || '—'}）`
    }
  } catch (err) {
    if (reqId !== rankingsReqId) return
    console.error(err)
    rankings.value = []
    rankingsError.value = err.message || '排行查詢失敗'
    statusText.value = `排行失敗：${err.message}`
  } finally {
    if (reqId === rankingsReqId) loadingRankings.value = false
  }
}

function requireLoginForChart() {
  if (isAuthenticated.value) return true
  authStatus.value = '技術分析需先登入（Google）'
  statusText.value = '技術分析需先登入後才能查看'
  return false
}

async function selectWarrant(row) {
  if (!row?.warrant_code) return
  selected.value = row
  loadingDetail.value = true
  try {
    const detailResp = await fetchMasterDetail(row.warrant_code).catch(() => null)
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
      days_to_expiry: row.days_to_expiry,
      issuance: row.issuance,
      close_price: row.close_price,
      latest_close_price: row.close_price,
      volume: row.volume,
    }
    // 主檔列表已有的行情欄位併入詳情，供全螢幕基本資料列顯示
    if (detail.value && row) {
      detail.value = {
        ...detail.value,
        close_price: detail.value.close_price ?? row.close_price,
        latest_close_price: detail.value.latest_close_price ?? row.close_price,
        days_to_expiry: detail.value.days_to_expiry ?? row.days_to_expiry,
        volume: detail.value.volume ?? row.volume,
      }
    }
    await nextTick()
    // 技術分析需登入；指標權限比照主站 Pro／Lite
    if (!requireLoginForChart()) return
    techChartRef.value?.enterFullscreen?.()
  } catch (err) {
    console.error(err)
    statusText.value = `載入詳情失敗：${err.message}`
  } finally {
    loadingDetail.value = false
  }
}

function onChartFullscreenChange(active) {
  chartFullscreen.value = !!active
}

function openTechChart() {
  if (!requireLoginForChart()) return
  techChartRef.value?.enterFullscreen?.()
}

function closeDetail() {
  detail.value = null
}

function onSearch() {
  filters.page = 1
  loadMaster()
}

function clearFundamentalFilters() {
  filters.expiryFrom = ''
  filters.expiryTo = ''
  filters.closeMin = ''
  filters.closeMax = ''
  filters.exerciseMin = ''
  filters.exerciseMax = ''
  filters.daysMin = ''
  filters.daysMax = ''
  filters.page = 1
  loadMaster()
}

function onPage(p) {
  filters.page = p
  loadMaster()
}

function onMasterSort({ sort, sortDir }) {
  filters.sort = sort
  filters.sortDir = sortDir
}

async function onImportLatest() {
  if (!isAdmin.value) {
    statusText.value = '同步最新成交僅限管理員'
    return
  }
  importing.value = true
  statusText.value = '正在同步上市 MI_INDEX 與上櫃日行情…'
  try {
    const resp = await importLatestWarrants()
    statusText.value = `${resp.message || '匯入完成'}`
    await loadDates()
    await loadRankings()
    await loadStats()
  } catch (err) {
    statusText.value = `匯入失敗：${err.message}`
  } finally {
    importing.value = false
  }
}

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
  const oauth = consumeOAuthCallbackFromUrl()
  if (oauth.error) {
    authStatus.value = `登入失敗：${oauth.error}`
  } else if (oauth.token) {
    setToken(oauth.token)
    const me = await fetchCurrentUser()
    authStatus.value = me.success ? `已用 Google 登入（${planLabel.value}）` : (me.error || '登入狀態同步失敗')
  } else if (isAuthenticated.value && !user.value) {
    await fetchCurrentUser()
  }

  await Promise.all([loadStats(), loadMaster(), loadDates()])
  await loadRankings()
})
</script>

<template>
  <div class="app">
    <PwaInstallPrompt />
    <div class="topbar">
      <a class="home-link" href="https://www.quantgems.com/" target="_blank" rel="noopener">
        QuantGems® 主站
      </a>
      <div class="auth-box">
        <template v-if="isAuthenticated">
          <span class="auth-user">
            <span class="auth-name">{{ displayName }}</span>
            <span class="plan-pill">{{ planLabel }}</span>
          </span>
          <button type="button" class="btn-auth ghost" @click="handleLogout">登出</button>
        </template>
        <button v-else type="button" class="btn-auth google" @click="handleGoogleLogin">
          <svg class="g-icon" viewBox="0 0 24 24" aria-hidden="true">
            <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
            <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
            <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
            <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
          </svg>
          Google 登入
        </button>
      </div>
    </div>
    <p v-if="authStatus" class="auth-status muted">{{ authStatus }}</p>

    <header class="hero">
      <div class="hero-copy">
        <div class="brand-row">
          <img class="brand-mark" src="/favicon.svg" alt="QuantGems Warrant Radar" width="44" height="44" />
          <div>
            <p class="eyebrow">QuantGems · Warrant Radar</p>
            <h1>權證雷達</h1>
          </div>
        </div>
        <p class="lede">篩選全市場發行主檔，追蹤當日成交熱度，並以全螢幕技術分析檢視單檔走勢。</p>
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
          <strong>{{ latestTradeDate }}</strong>
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
          <label>排序</label>
          <select v-model="filters.sort">
            <option value="expiry">到期日</option>
            <option value="days">到期天數</option>
            <option value="exercise">履約價</option>
            <option value="ratio">行使比</option>
            <option value="close">收盤</option>
            <option value="volume">成交量</option>
            <option value="code">代號</option>
            <option value="name">名稱</option>
            <option value="market">市場</option>
            <option value="type">類型</option>
            <option value="underlying">標的</option>
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

      <div class="fund-block">
        <div class="fund-head">
          <h3>基本面條件</h3>
          <span class="muted">依收盤、履約價、到期天數、到期日區間篩選</span>
        </div>
        <div class="fund-grid">
          <div class="range-field">
            <label>收盤</label>
            <div class="range-inputs">
              <input v-model="filters.closeMin" type="number" step="any" min="0" placeholder="最低" />
              <span>–</span>
              <input v-model="filters.closeMax" type="number" step="any" min="0" placeholder="最高" />
            </div>
          </div>
          <div class="range-field">
            <label>履約價</label>
            <div class="range-inputs">
              <input v-model="filters.exerciseMin" type="number" step="any" min="0" placeholder="最低" />
              <span>–</span>
              <input v-model="filters.exerciseMax" type="number" step="any" min="0" placeholder="最高" />
            </div>
          </div>
          <div class="range-field">
            <label>到期天數</label>
            <div class="range-inputs">
              <input v-model="filters.daysMin" type="number" step="1" min="0" placeholder="最低" />
              <span>–</span>
              <input v-model="filters.daysMax" type="number" step="1" min="0" placeholder="最高" />
            </div>
          </div>
          <div class="range-field">
            <label>到期日</label>
            <div class="range-inputs">
              <input v-model="filters.expiryFrom" type="date" />
              <span>–</span>
              <input v-model="filters.expiryTo" type="date" />
            </div>
          </div>
        </div>
      </div>

      <div class="actions">
        <button class="primary" @click="onSearch">搜尋主檔</button>
        <button type="button" @click="clearFundamentalFilters">清除基本面條件</button>
        <button
          v-if="isAdmin"
          :disabled="importing"
          @click="onImportLatest"
        >
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
          :sort="filters.sort"
          :sort-dir="filters.sortDir"
          @select="selectWarrant"
          @page="onPage"
          @sort="onMasterSort"
        />

        <WarrantDetail
          v-if="!chartFullscreen && (detail || loadingDetail)"
          :detail="detail"
          :loading="loadingDetail"
          @close="closeDetail"
          @open-chart="openTechChart"
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
            <label>類型</label>
            <div class="btns">
              <button type="button" :class="{ active: heatType === '' }" @click="setHeatType('')">全部</button>
              <button type="button" :class="{ active: heatType === '認購' }" @click="setHeatType('認購')">認購</button>
              <button type="button" :class="{ active: heatType === '認售' }" @click="setHeatType('認售')">認售</button>
            </div>
          </div>
          <div class="metric-toggle">
            <label>指標</label>
            <div class="btns">
              <button type="button" :class="{ active: metric === 'turnover' }" @click="setMetric('turnover')">成交金額</button>
              <button type="button" :class="{ active: metric === 'volume' }" @click="setMetric('volume')">成交張數</button>
            </div>
          </div>
        </div>

        <div class="heat-grid">
          <RankingPanel
            :rows="rankings"
            :loading="loadingRankings"
            :selected-code="selected?.warrant_code || ''"
            :metric="metric"
            :heat-type="heatType"
            :api-type="rankingsMeta.type"
            :error-text="rankingsError"
            @select="selectWarrant"
          />
          <div class="chart-gate">
            <StockChartECharts
              v-if="isAuthenticated"
              ref="techChartRef"
              class="warrant-stock-chart"
              :symbol="selected?.warrant_code || ''"
              :stock-name="selected?.warrant_name || ''"
              :warrant-info="detail"
              period="1D"
              :fullscreen-search-enabled="false"
              @fullscreen-change="onChartFullscreenChange"
            />
            <div v-else class="chart-login panel">
              <h3>技術分析需登入</h3>
              <p class="muted">登入後可查看權證 K 線與技術指標；神奇 K／階梯線／進階指標權限比照 QuantGems 主站方案。</p>
              <button type="button" class="primary" @click="handleGoogleLogin">Google 登入</button>
              <a
                class="pricing-link"
                href="https://www.quantgems.com/?view=pricing"
                target="_blank"
                rel="noopener noreferrer"
              >查看方案</a>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.app {
  width: min(1280px, calc(100% - 2rem));
  margin: 0 auto;
  padding: 0.75rem 0 3rem;
}

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 0.85rem;
}
.home-link {
  color: var(--text-dim);
  font-size: 0.86rem;
  text-decoration: none;
}
.home-link:hover { color: var(--cyan-bright, #38bdf8); }
.auth-box {
  display: flex;
  align-items: center;
  gap: 0.65rem;
}
.auth-user {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  font-size: 0.88rem;
}
.auth-name { color: var(--text); }
.plan-pill {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  padding: 0.15rem 0.45rem;
  border-radius: 999px;
  color: #0f172a;
  background: var(--btn-gradient, linear-gradient(135deg, #38bdf8, #818cf8));
}
.btn-auth {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  padding: 0.45rem 0.85rem;
  border-radius: 999px;
  font-size: 0.86rem;
  font-weight: 600;
}
.btn-auth.google {
  background: rgba(255, 255, 255, 0.06);
  border: 1px solid rgba(66, 133, 244, 0.45);
  color: #e2e8f0;
}
.btn-auth.google:hover {
  border-color: #4285F4;
  background: rgba(66, 133, 244, 0.12);
}
.btn-auth.ghost {
  background: transparent;
  border: 1px solid var(--line);
  color: var(--text-dim);
}
.g-icon { width: 16px; height: 16px; }
.auth-status {
  margin: 0 0 0.75rem;
  font-size: 0.85rem;
}

.hero {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 1.15rem;
  margin-bottom: 1.25rem;
  animation: rise 0.7s ease both;
}
.eyebrow {
  margin: 0 0 0.2rem;
  color: var(--cyan-bright);
  letter-spacing: 0.06em;
  text-transform: none;
  font-size: 0.78rem;
  font-weight: 600;
}
.brand-row {
  display: flex;
  align-items: center;
  gap: 0.85rem;
}
.brand-mark {
  width: 44px;
  height: 44px;
  border-radius: 12px;
  box-shadow: 0 0 24px rgba(0, 212, 255, 0.35), 0 0 0 1px rgba(0, 212, 255, 0.25);
  flex-shrink: 0;
}
.hero h1 {
  margin: 0;
  font-family: inherit;
  font-size: clamp(1.85rem, 3.6vw, 2.55rem);
  font-weight: 800;
  letter-spacing: -0.02em;
  background: var(--brand-gradient);
  -webkit-background-clip: text;
  background-clip: text;
  color: transparent;
  animation: rise 0.85s ease both;
}
.lede {
  margin: 0.7rem 0 0;
  color: var(--text-dim);
  max-width: 36rem;
  font-size: 0.98rem;
}
.hero-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.75rem;
}
.stat {
  background: rgba(7, 11, 20, 0.72);
  border: 1px solid var(--line-strong);
  border-radius: 12px;
  padding: 0.85rem 1rem;
  box-shadow: var(--shadow);
  transition: border-color 0.2s, transform 0.15s;
}
.stat:hover {
  border-color: rgba(0, 212, 255, 0.45);
  transform: translateY(-1px);
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
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.75rem;
}
.fund-block {
  border-top: 1px solid rgba(148, 183, 205, 0.14);
  padding-top: 0.85rem;
  display: grid;
  gap: 0.65rem;
}
.fund-head {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 0.55rem 0.85rem;
}
.fund-head h3 {
  margin: 0;
  font-size: 0.92rem;
  font-weight: 650;
  color: #e8f4ff;
}
.fund-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.75rem;
}
.range-field label {
  display: block;
  margin-bottom: 0.35rem;
  color: var(--text-dim);
  font-size: 0.8rem;
}
.range-inputs {
  display: grid;
  grid-template-columns: 1fr auto 1fr;
  gap: 0.35rem;
  align-items: center;
}
.range-inputs span {
  color: #8fa3b3;
  font-size: 0.8rem;
  text-align: center;
}
.range-inputs input {
  min-width: 0;
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
  grid-template-columns: minmax(0, 1fr);
  gap: 1rem;
  align-items: start;
}
.col-main {
  display: grid;
  gap: 1rem;
}
.heat-controls {
  display: grid;
  grid-template-columns: 1fr 1fr 1.2fr 1.2fr;
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
  align-items: stretch;
}
.chart-gate {
  min-height: 520px;
  width: 100%;
  display: flex;
  flex-direction: column;
}
.warrant-stock-chart {
  min-height: 520px;
  width: 100%;
  flex: 1;
}
.chart-login {
  flex: 1;
  min-height: 520px;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
  gap: 0.65rem;
  padding: 1.4rem 1.35rem;
}
.chart-login h3 {
  margin: 0;
  font-size: 1.05rem;
  font-weight: 650;
}
.chart-login p {
  margin: 0;
  max-width: 34rem;
  line-height: 1.55;
  font-size: 0.88rem;
}
.chart-login .pricing-link {
  color: #7dd3fc;
  font-size: 0.84rem;
  text-decoration: none;
}
.chart-login .pricing-link:hover {
  text-decoration: underline;
}
:global(body.warrant-ta-fs),
:global(html.warrant-ta-fs) {
  overflow: hidden;
}

@keyframes rise {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

@media (max-width: 980px) {
  .hero-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .workspace,
  .heat-grid,
  .filters,
  .fund-grid,
  .heat-controls {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 1100px) and (min-width: 721px) {
  .fund-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
