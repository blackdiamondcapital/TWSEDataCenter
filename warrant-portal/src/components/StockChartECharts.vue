<script setup>
import { ref, reactive, onMounted, watch, onUnmounted, nextTick, computed } from 'vue'
import * as echarts from 'echarts'
import { fetchStockPriceHistory, fetchStockQuote, fetchRankings, fetchComparison, fetchAiTechnicalAnalysis, fetchUserChartSettings, saveUserChartSettings, fetchStockMaster } from '../services/api'
import { useAuth } from '../stores/auth'
import { resolveUserAccess, LITE_FREE_PRO_UPGRADE_MESSAGE, canUseMagicKAndLadder, canUseProChartFeatures } from '../utils/planAccess.js'
import { useWatchlistSpeechRecognition, extractStockCodesFromSpeech, preprocessSpeechTranscriptForStock } from '../composables/useWatchlistSpeech.js'
import { useQuadLayoutAvailable } from '../composables/useQuadLayoutAllowed.js'

const pad2 = (n) => String(n).padStart(2, '0')
function toIsoDateOnly(value) {
  try {
    if (!value) return null
    if (typeof value === 'string') {
      const m = value.match(/^(\d{4})-(\d{2})-(\d{2})/)
      if (m) return `${m[1]}-${m[2]}-${m[3]}`
      const d = new Date(value)
      if (!Number.isNaN(d.getTime())) return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
      return null
    }
    const d = value instanceof Date ? value : new Date(value)
    if (!Number.isNaN(d.getTime())) return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
    return null
  } catch {
    return null
  }
}

const props = defineProps({
  symbol: { type: String, default: '' },
  stockName: { type: String, default: '' },
  period: { type: String, default: '1D' },
  carouselEnabled: { type: Boolean, default: false },
  carouselIndex: { type: Number, default: 0 },
  carouselLength: { type: Number, default: 0 },
  carouselPlaying: { type: Boolean, default: false },
  fullscreenSearchEnabled: { type: Boolean, default: false },
  /** 全螢幕主機 2×2 格內：精簡表頭＋圖面優先 */
  multiTileMode: { type: Boolean, default: false },
  /** App 全螢幕四分割格：顯示查股列（與 multiTile 並用） */
  fsHostQuadCell: { type: Boolean, default: false },
  /** 四分割格內圖表：不向上 emit fullscreen-change，避免關閉整層 chart-page */
  fsSuppressHostSync: { type: Boolean, default: false },
  /** 權證雷達：全螢幕內顯示權證基本資料 */
  warrantInfo: { type: Object, default: null },
})

const emit = defineEmits(['search-symbol', 'update:period', 'carousel-prev', 'carousel-next', 'carousel-toggle', 'fullscreen-change', 'requestFsQuad'])
const { quadLayoutAvailable } = useQuadLayoutAvailable()

function warrantFmtNum(v, digits) {
  if (v == null || v === '') return null
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  if (digits == null) return n.toLocaleString()
  return n.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

const warrantFsChips = computed(() => {
  const d = props.warrantInfo
  if (!d || typeof d !== 'object') return []
  let days = d.days_to_expiry
  if (days == null && d.expiry_date) {
    const t = Date.parse(d.expiry_date)
    if (Number.isFinite(t)) {
      const today = new Date()
      today.setHours(0, 0, 0, 0)
      days = Math.round((t - today.getTime()) / 86400000)
    }
  }
  const close = d.latest_close_price ?? d.close_price
  return [
    { label: '市場', value: d.market || null },
    { label: '類型', value: d.warrant_type || null },
    {
      label: '標的',
      value: [d.underlying_code, d.underlying_name].filter(Boolean).join(' ') || null,
    },
    { label: '收盤', value: warrantFmtNum(close, 2) },
    { label: '履約價', value: warrantFmtNum(d.latest_exercise_price, 2) },
    { label: '行使比', value: warrantFmtNum(d.latest_exercise_ratio, 4) },
    { label: '到期天數', value: days != null ? String(days) : null },
    { label: '到期日', value: d.expiry_date || null },
    {
      label: '發行量',
      value: warrantFmtNum(
        d.issuance_units_thousand ?? d.accumulated_issuance ?? d.issuance,
      ),
    },
    {
      label: '發行日',
      value: d.issue_date || d.listed_date || d.exercise_start_date || null,
    },
  ].filter((c) => c.value != null && c.value !== '')
})

const warrantFsTitle = computed(() => {
  const d = props.warrantInfo
  if (!d) return ''
  return [d.warrant_code, d.warrant_name].filter(Boolean).join(' · ')
})

const warrantInfoOpen = ref(false)

const { user } = useAuth()

const CHART_SETTINGS_VERSION = 1
let chartSettingsSyncInFlight = false
let chartSettingsSyncTimer = null
let chartSettingsLoadedOnce = false

function hasAuthToken() {
  try {
    return typeof localStorage !== 'undefined' && !!localStorage.getItem('quantgem_auth_token')
  } catch {
    return false
  }
}

async function flushChartSettingsToServerNow() {
  if (!hasAuthToken()) return
  try {
    const payload = collectChartSettingsFromLocalStorage()
    await saveUserChartSettings(payload)
  } catch (_) {}
}

const beforeLogoutHandler = () => {
  if (chartSettingsSyncTimer) {
    try { clearTimeout(chartSettingsSyncTimer) } catch (_) {}
    chartSettingsSyncTimer = null
  }
  flushChartSettingsToServerNow()
}

onMounted(() => {
  try {
    if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
      window.addEventListener('quantgems:before-logout', beforeLogoutHandler)
      window.addEventListener('pointermove', onCrosshairLookupNavPointerMove, true)
      window.addEventListener('pointerup', onCrosshairLookupNavPointerUp, true)
      window.addEventListener('pointercancel', onCrosshairLookupNavPointerUp, true)
    }
  } catch (_) {}
})

function collectChartSettingsFromLocalStorage() {
  const items = {}
  try {
    if (typeof localStorage === 'undefined') return { version: CHART_SETTINGS_VERSION, items }
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i)
      if (!k) continue
      if (k.startsWith('chart') || k.startsWith('diagSr')) {
        items[k] = localStorage.getItem(k)
      }
    }
  } catch (_) {}
  return { version: CHART_SETTINGS_VERSION, items }
}

function applyChartSettingsToLocalStorage(payload) {
  try {
    const version = Number(payload?.version || 0)
    const items = payload?.items
    if (version !== CHART_SETTINGS_VERSION) return false
    if (!items || typeof items !== 'object') return false
    for (const [k, v] of Object.entries(items)) {
      if (typeof k !== 'string') continue
      if (!(k.startsWith('chart') || k.startsWith('diagSr'))) continue
      if (v === null || v === undefined) continue
      try { localStorage.setItem(k, String(v)) } catch (_) {}
    }
    return true
  } catch (_) {
    return false
  }
}

function hydrateChartSettingsFromLocalStorage() {
  try {
    showMainK.value = localStorage.getItem('chartShowMainK') !== 'false'
    showVolume.value = localStorage.getItem('chartShowVolume') !== 'false'
    showKD.value = localStorage.getItem('chartShowKD') === 'true'
    showMACD.value = localStorage.getItem('chartShowMACD') === 'true'
    showRSI.value = localStorage.getItem('chartShowRSI') === 'true'
    showCCI.value = localStorage.getItem('chartShowCCI') === 'true'
    showBB.value = localStorage.getItem('chartShowBB') === 'true'
    showGoldenWave.value = localStorage.getItem('chartShowGoldenWave') === 'true'
    showVPVR.value = localStorage.getItem('chartShowVPVR') === 'true'
    showFib.value = localStorage.getItem('chartShowFib') === 'true'
    showDiagSR.value = localStorage.getItem('chartShowDiagSR') === 'true'
    showHMA.value = localStorage.getItem('chartShowHMA') === 'true'
    showHMAInd.value = localStorage.getItem('chartShowHMAInd') === 'true'
    showReversal.value = localStorage.getItem('chartShowReversal') === 'true'
    showReversalUp.value = localStorage.getItem('chartShowReversalUp') !== 'false'
    showReversalDown.value = localStorage.getItem('chartShowReversalDown') !== 'false'

    const visible = parseInt(localStorage.getItem('chartDesiredKCount') || '120', 10)
    if (Number.isFinite(visible) && visible > 0) {
      desiredKPref.value = Math.max(1, visible)
      desiredKCount.value = Math.max(1, visible)
    }

    const suffix = getCurrentTfSuffix()

    kdParams.value.period = parseInt(localStorage.getItem(`chartKDPeriod${suffix}`) || localStorage.getItem('chartKDPeriod') || '9')
    kdParams.value.k = parseInt(localStorage.getItem(`chartKDK${suffix}`) || localStorage.getItem('chartKDK') || '3')
    kdParams.value.d = parseInt(localStorage.getItem(`chartKDD${suffix}`) || localStorage.getItem('chartKDD') || '3')

    macdParams.value.fast = parseInt(localStorage.getItem(`chartMACDFast${suffix}`) || localStorage.getItem('chartMACDFast') || '12')
    macdParams.value.slow = parseInt(localStorage.getItem(`chartMACDSlow${suffix}`) || localStorage.getItem('chartMACDSlow') || '26')
    macdParams.value.signal = parseInt(localStorage.getItem(`chartMACDSignal${suffix}`) || localStorage.getItem('chartMACDSignal') || '9')
    const parseMacdDisplaySelection = (storedValue) => {
      const raw = String(storedValue || '').trim().toLowerCase()
      if (!raw || raw === 'all') return ['dif', 'macd', 'osc']
      const values = raw.split(',').map(v => v.trim()).filter(v => ['dif', 'macd', 'osc'].includes(v))
      return values.length ? Array.from(new Set(values)) : ['dif', 'macd', 'osc']
    }
    macdDisplayMode.value = parseMacdDisplaySelection(localStorage.getItem(`chartMACDDisplay${suffix}`) || localStorage.getItem('chartMACDDisplay') || 'all')
    macdLineWidths.value.dif = parseFloat(localStorage.getItem(`chartMACDDifWidth${suffix}`) || localStorage.getItem('chartMACDDifWidth') || '2')
    macdLineWidths.value.macd = parseFloat(localStorage.getItem(`chartMACDSignalWidth${suffix}`) || localStorage.getItem('chartMACDSignalWidth') || '2')
    macdHistHeight.value = parseFloat(localStorage.getItem(`chartMACDHistHeight${suffix}`) || localStorage.getItem('chartMACDHistHeight') || '0.7')
    macdOscStyle.value.barWidth = parseFloat(localStorage.getItem(`chartMACDHistBarWidth${suffix}`) || localStorage.getItem('chartMACDHistBarWidth') || '60')
    macdOscStyle.value.colorUp = localStorage.getItem(`chartMACDHistColorUp${suffix}`) || localStorage.getItem('chartMACDHistColorUp') || '#ff5050'
    macdOscStyle.value.colorDown = localStorage.getItem(`chartMACDHistColorDown${suffix}`) || localStorage.getItem('chartMACDHistColorDown') || '#10b981'
    macdOscStyle.value.opacityUp = parseFloat(localStorage.getItem(`chartMACDHistOpacityUp${suffix}`) || localStorage.getItem('chartMACDHistOpacityUp') || '1')
    macdOscStyle.value.opacityDown = parseFloat(localStorage.getItem(`chartMACDHistOpacityDown${suffix}`) || localStorage.getItem('chartMACDHistOpacityDown') || '1')

    rsiParams.value.period = parseInt(localStorage.getItem(`chartRSIPeriod${suffix}`) || localStorage.getItem('chartRSIPeriod') || '14')
    rsiParams.value.overbought = parseInt(localStorage.getItem(`chartRSIOverbought${suffix}`) || localStorage.getItem('chartRSIOverbought') || '70')
    rsiParams.value.oversold = parseInt(localStorage.getItem(`chartRSIOversold${suffix}`) || localStorage.getItem('chartRSIOversold') || '30')

    bbParams.value.period = parseInt(localStorage.getItem(`chartBBPeriod${suffix}`) || localStorage.getItem('chartBBPeriod') || '20')
    bbParams.value.mult = parseFloat(localStorage.getItem(`chartBBMult${suffix}`) || localStorage.getItem('chartBBMult') || '2')
    bbParams.value.colorUpper = localStorage.getItem(`chartBBColorUpper${suffix}`) || localStorage.getItem('chartBBColorUpper') || BB_DEFAULT_COLORS.colorUpper
    bbParams.value.colorMid = localStorage.getItem(`chartBBColorMid${suffix}`) || localStorage.getItem('chartBBColorMid') || BB_DEFAULT_COLORS.colorMid
    bbParams.value.colorLower = localStorage.getItem(`chartBBColorLower${suffix}`) || localStorage.getItem('chartBBColorLower') || BB_DEFAULT_COLORS.colorLower

    maParams.value.ma1 = parseInt(localStorage.getItem(`chartMA1${suffix}`) || localStorage.getItem('chartMA1') || '5')
    maParams.value.ma2 = parseInt(localStorage.getItem(`chartMA2${suffix}`) || localStorage.getItem('chartMA2') || '10')
    maParams.value.ma3 = parseInt(localStorage.getItem(`chartMA3${suffix}`) || localStorage.getItem('chartMA3') || '20')
    maParams.value.ma4 = parseInt(localStorage.getItem(`chartMA4${suffix}`) || localStorage.getItem('chartMA4') || '30')
    maParams.value.ma5 = parseInt(localStorage.getItem(`chartMA5${suffix}`) || localStorage.getItem('chartMA5') || '60')

    showMA1.value = localStorage.getItem('chartShowMA1') !== 'false'
    showMA2.value = localStorage.getItem('chartShowMA2') !== 'false'
    showMA3.value = localStorage.getItem('chartShowMA3') !== 'false'
    showMA4.value = localStorage.getItem('chartShowMA4') !== 'false'
    showMA5.value = localStorage.getItem('chartShowMA5') !== 'false'
  } catch (_) {
  }
  enforceProChartAccess()
}

async function syncChartSettingsFromServer() {
  if (!hasAuthToken()) return
  if (chartSettingsSyncInFlight) return
  chartSettingsSyncInFlight = true
  try {
    const { value } = await fetchUserChartSettings()
    if (value) {
      const applied = applyChartSettingsToLocalStorage(value)
      if (applied) {
        chartSettingsLoadedOnce = true
        hydrateChartSettingsFromLocalStorage()
      }
    }
  } catch (_) {
  } finally {
    chartSettingsSyncInFlight = false
  }
}

function scheduleSyncChartSettingsToServer() {
  if (!hasAuthToken()) return
  if (chartSettingsSyncTimer) {
    try { clearTimeout(chartSettingsSyncTimer) } catch (_) {}
  }
  chartSettingsSyncTimer = setTimeout(async () => {
    if (!hasAuthToken()) return
    try {
      const payload = collectChartSettingsFromLocalStorage()
      await saveUserChartSettings(payload)
    } catch (_) {}
  }, 800)
}

watch(() => user?.value?.id, async (uid, prev) => {
  if (!uid || uid === prev) return
  try {
    await syncChartSettingsFromServer()
    if (!loading.value && chartData.value.length > 0) {
      renderChart()
    }
  } catch (_) {}
}, { immediate: false })

const isLiteFreePlan = computed(() => resolveUserAccess(user?.value).isLiteFree)
const canUseProTech = computed(() => canUseProChartFeatures(user?.value))
const canUsePrimeTech = computed(() => canUseProChartFeatures(user?.value))
const canUseHeikinLadder = computed(() => canUseMagicKAndLadder(user?.value))
const isFreeTech = computed(() => false)

const upgradeModalOpen = ref(false)
const upgradeModalTitle = ref('')
const upgradeModalMessage = ref('')

function closeUpgradeModal() {
  upgradeModalOpen.value = false
}

function goToPricing() {
  try {
    window.open('https://www.quantgems.com/?view=pricing', '_blank', 'noopener,noreferrer')
  } catch {
    try {
      window.location.assign('https://www.quantgems.com/?view=pricing')
    } catch {
      // ignore
    }
  }
}

function alertUpgrade(minPlanLabel) {
  const msg = !canUseProTech.value
    ? LITE_FREE_PRO_UPGRADE_MESSAGE
    : `此功能需要 ${minPlanLabel} 方案，請升級後使用`
  if (isFullscreen.value) {
    upgradeModalTitle.value = '需要升級方案'
    upgradeModalMessage.value = msg
    upgradeModalOpen.value = true
    return
  }
  window.alert(msg)
}

function handleTechGate(minPlanLabel, allowed, event) {
  if (allowed) return
  try { event?.preventDefault?.() } catch {}
  alertUpgrade(minPlanLabel)
}

const SPECIAL_SYMBOL_NAMES = new Map([
  ['^TWII', '加權指數'],
  ['TWII', '加權指數']
])

function getSpecialSymbolName(symbol) {
  const key = String(symbol || '').toUpperCase().trim()
  return SPECIAL_SYMBOL_NAMES.get(key) || ''
}

// Resolve stock name if not provided by parent
const resolvedName = ref('')
const displayedTitleSymbol = ref(String(props.symbol || '').toUpperCase())
const displayedTitleName = ref((props.stockName && props.stockName.trim()) || '')
let symbolChangeRequestId = 0
/** 換股重設週期時擋住 selectedPeriodKey watcher，避免與 symbol watcher 雙重 loadChartData */
let suppressSelectedPeriodAutoLoad = false
/** 換股資料尚未完成時，禁止手機鍵盤／viewport 事件插入額外重繪。 */
let symbolSwitchingInProgress = false
const normalizedSymbol = computed(() => String(props.symbol || '').toUpperCase())
const specialName = computed(() => getSpecialSymbolName(normalizedSymbol.value))
const displayName = computed(() => {
  if (displayedTitleName.value && displayedTitleName.value.trim()) return displayedTitleName.value.trim()
  return getSpecialSymbolName(displayedTitleSymbol.value)
})

function normalizeStockName(raw) {
  if (!raw) return ''
  let s = String(raw).trim()
  // 去掉常見的公司尾碼，讓標題名稱更接近自選股技術分析顯示風格
  s = s.replace(/股份有限公司/g, '').trim()
  return s
}

const baseSymbolForTitle = computed(() => {
  const raw = String(displayedTitleSymbol.value || normalizedSymbol.value || '').toUpperCase()
  const base = raw.split('.')[0]
  return base || raw
})

const stockTitle = computed(() => {
  const code = baseSymbolForTitle.value
  const name = normalizeStockName(displayName.value)
  if (name && name !== code) {
    return `${code} ${name}`
  }
  return code
})

const canNavigateCarousel = computed(() => props.carouselLength > 1)
const carouselIndicator = computed(() => {
  if (!props.carouselLength) return '輪播 0 / 0'
  const current = Math.min(props.carouselIndex + 1, props.carouselLength)
  return `輪播 ${current} / ${props.carouselLength}`
})

const fullscreenSearchSymbol = ref('')
const fullscreenSearchInput = ref(null)
/** Fullscreen stock search: switch between keyboard code entry and voice. */
const fullscreenSearchInputMode = ref('keyboard')
const mobileToolbarSearchOpen = ref(false)

const showMobileToolbarSearchTrigger = computed(() => {
  return !!props.fullscreenSearchEnabled && isFullscreen.value && useMobileKlineDropdown.value
})

const showFullscreenSearchBox = computed(() => {
  return !!props.fullscreenSearchEnabled &&
    (isFullscreen.value || useMobileKlineDropdown.value)
})

let suppressFullscreenSearchBlurResizeUntil = 0

function suppressFullscreenSearchBlurResize(duration = 420) {
  suppressFullscreenSearchBlurResizeUntil = Date.now() + duration
}

function shouldSuppressFullscreenSearchBlurResize() {
  return suppressFullscreenSearchBlurResizeUntil > Date.now()
}

function prepareMobileToolbarSearchUiTransition() {
  suppressFullscreenSearchBlurResize()
}

async function openMobileToolbarSearch() {
  mobileToolbarSearchOpen.value = true
  if (controlPanelOpen.value) {
    controlPanelOpen.value = false
    persistControlPanelOpenState()
  }
  fsSpeechRec.errorMessage.value = ''
  await ensureFullscreenSearchMaster()
  if (showMobileToolbarSearchTrigger.value) {
    return
  }
  await nextTick()
  const el = fullscreenSearchInput.value
  if (el && typeof el.focus === 'function') {
    el.focus()
  }
}

function closeMobileToolbarSearch() {
  const wasOpen = mobileToolbarSearchOpen.value
  if (wasOpen) {
    suppressFullscreenSearchBlurResize()
  }
  mobileToolbarSearchOpen.value = false
  try {
    fsSpeechRec.stop()
  } catch (_) {}
  fsSpeechRec.errorMessage.value = ''
  fullscreenSearchInputMode.value = 'keyboard'
}

function toggleMobileToolbarSearch() {
  if (mobileToolbarSearchOpen.value) {
    closeMobileToolbarSearch()
    return
  }
  void openMobileToolbarSearch()
}

function handleFullscreenSearch() {
  fsSpeechRec.errorMessage.value = ''
  const rawText = String(fullscreenSearchSymbol.value || '').trim()
  if (!rawText) return
  ensureFullscreenSearchMaster().then(() => {
    const resolved = resolveFullscreenStockSearch(rawText)
    if (resolved.special) {
      emitFullscreenSearchSymbol('TWII')
      return
    }
    if (/^\d{4,5}$/.test(String(resolved.raw || ''))) {
      emitFullscreenSearchSymbol(resolved.raw)
      return
    }
    if (resolved.matchedByName && resolved.raw) {
      emitFullscreenSearchSymbol(resolved.raw)
      return
    }
    fsSpeechRec.errorMessage.value = '無法對應股票代號或名稱，請再試一次'
    nextTick(() => {
      const el = fullscreenSearchInput.value
      if (el && typeof el.focus === 'function') {
        el.focus()
      }
    })
  })
}

function blurFullscreenSearchInput() {
  suppressFullscreenSearchBlurResize()
  const el = fullscreenSearchInput.value
  if (el && typeof el.blur === 'function') {
    el.blur()
  }
}

function shouldRefocusFullscreenSearchInput() {
  if (fullscreenSearchInputMode.value === 'voice') return false
  if (useMobileKlineDropdown.value) return false
  if (isMobileViewport()) return false
  return true
}

/** Fullscreen search: speech + name resolve (same strategy as watchlist lookup). */
const FULLSCREEN_SEARCH_SPECIAL = {
  TWII: { querySymbol: '^TWII', displaySymbol: '^TWII', name: '加權指數' },
  '^TWII': { querySymbol: '^TWII', displaySymbol: '^TWII', name: '加權指數' },
}

const FULLSCREEN_SEARCH_POPULAR = [
  { symbol: 'TWII', name: '加權指數' },
  { symbol: '2330', name: '台積電' },
  { symbol: '2317', name: '鴻海' },
  { symbol: '2454', name: '聯發科' },
  { symbol: '2308', name: '台達電' },
  { symbol: '2882', name: '國泰金' },
  { symbol: '2412', name: '中華電' },
  { symbol: '2886', name: '兆豐金' },
  { symbol: '2891', name: '中信金' },
  { symbol: '1301', name: '台塑' },
  { symbol: '1303', name: '南亞' },
]

function normalizeFsMasterSymbol(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/\.(TW|TWO)$/i, '')
}

const fullscreenSearchMaster = ref([])
let fullscreenSearchMasterLoading = false
let fullscreenSearchMasterReady = false

async function ensureFullscreenSearchMaster() {
  if (!props.fullscreenSearchEnabled || fullscreenSearchMasterLoading || fullscreenSearchMasterReady) return
  fullscreenSearchMasterLoading = true
  try {
    const list = await fetchStockMaster()
    if (Array.isArray(list)) {
      fullscreenSearchMaster.value = list
        .map((item) => {
          const symbol = normalizeFsMasterSymbol(item.symbol || item.code)
          const shortName = String(item.short_name ?? item.shortName ?? item.abbr ?? '').trim()
          const fullName = String(
            item.full_name ?? item.company_name ?? item.stock_name ?? item.companyName ?? ''
          ).trim()
          const generic = String(item.name ?? '').trim()
          const name = shortName || fullName || generic
          return { symbol, name, shortName, fullName, generic }
        })
        .filter((s) => s.symbol)
    }
  } catch (_) {
    fullscreenSearchMaster.value = []
  } finally {
    fullscreenSearchMasterReady = true
    fullscreenSearchMasterLoading = false
  }
}

function fsNormStockNameKey(str) {
  let s = String(str || '').trim().toLowerCase()
  if (!s) return ''
  s = s.replace(/\u81fa/g, '\u53f0')
  s = s.replace(/[\s\u3000，。．、；：'"''\u201c\u201d\u2018\u2019·\-]/g, '')
  const suffixes = [
    '\u80a1\u4efd\u6709\u9650\u516c\u53f8',
    '\u6709\u9650\u516c\u53f8',
    '\u80a1\u4efd\u6709\u9650',
    '\u80a1\u4efd\u516c\u53f8',
    '\u63a7\u80a1\u516c\u53f8',
    '\u96c6\u5718',
    '\u63a7\u80a1',
    '\u4f01\u696d',
    '\u516c\u53f8',
    '\u80a1\u4efd',
  ]
  let prev = ''
  while (s !== prev) {
    prev = s
    for (const suf of suffixes) {
      if (s.endsWith(suf)) {
        s = s.slice(0, -suf.length)
        break
      }
    }
  }
  return s.trim()
}

function resolveFullscreenStockSearch(rawValue) {
  const rawText = String(rawValue || '').trim()
  if (!rawText) return { raw: '', special: null, matchedByName: false }

  const upperText = rawText.toUpperCase()
  const normalizedCode = upperText.replace(/\.(TW|TWO)$/i, '')
  const special = FULLSCREEN_SEARCH_SPECIAL[normalizedCode] || FULLSCREEN_SEARCH_SPECIAL[upperText]
  if (/^\d{4,5}$/.test(normalizedCode) || special) {
    return {
      raw: special ? 'TWII' : normalizedCode,
      special: special || null,
      matchedByName: false,
    }
  }

  const normalizedName = rawText.toLowerCase()
  const nameKeyQuery = fsNormStockNameKey(rawText)
  const pool = []
  const seen = new Set()

  const addCandidate = (symbol, name) => {
    const sym = String(symbol || '')
      .trim()
      .toUpperCase()
      .replace(/\.(TW|TWO)$/i, '')
    const nm = String(name || '').trim()
    if (!sym || !nm) return
    const dedupe = `${sym}\n${nm}`
    if (seen.has(dedupe)) return
    seen.add(dedupe)
    pool.push({ symbol: sym, name: nm })
  }

  function addMasterNameCandidates(item) {
    if (!item?.symbol) return
    const rows = [item.shortName, item.fullName, item.name, item.generic]
    const uniq = new Set()
    for (const r of rows) {
      const t = String(r || '').trim()
      if (t) uniq.add(t)
    }
    uniq.forEach((nm) => addCandidate(item.symbol, nm))
  }

  FULLSCREEN_SEARCH_POPULAR.forEach((item) => addCandidate(item.symbol, item.name))
  fullscreenSearchMaster.value.forEach((item) => addMasterNameCandidates(item))

  const exactMatch = pool.find((item) => {
    if (item.name.toLowerCase() === normalizedName) return true
    return nameKeyQuery.length >= 2 && fsNormStockNameKey(item.name) === nameKeyQuery
  })
  if (exactMatch) {
    return {
      raw: exactMatch.symbol,
      special: FULLSCREEN_SEARCH_SPECIAL[exactMatch.symbol] || null,
      matchedByName: true,
    }
  }

  const partialMatches = pool.filter((item) => item.name.toLowerCase().includes(normalizedName))
  if (partialMatches.length === 1) {
    const matched = partialMatches[0]
    return {
      raw: matched.symbol,
      special: FULLSCREEN_SEARCH_SPECIAL[matched.symbol] || null,
      matchedByName: true,
    }
  }

  if (nameKeyQuery.length >= 2) {
    const keyPartials = pool.filter((item) => {
      const nk = fsNormStockNameKey(item.name)
      return nk.length >= 2 && nk.includes(nameKeyQuery)
    })
    if (keyPartials.length === 1) {
      const matched = keyPartials[0]
      return {
        raw: matched.symbol,
        special: FULLSCREEN_SEARCH_SPECIAL[matched.symbol] || null,
        matchedByName: true,
      }
    }
  }

  if (nameKeyQuery.length >= 2) {
    const embedded = pool.filter((item) => {
      const nk = fsNormStockNameKey(item.name)
      return nk.length >= 2 && nameKeyQuery.includes(nk)
    })
    if (embedded.length === 1) {
      const matched = embedded[0]
      return {
        raw: matched.symbol,
        special: FULLSCREEN_SEARCH_SPECIAL[matched.symbol] || null,
        matchedByName: true,
      }
    }
    if (embedded.length > 1) {
      embedded.sort((a, b) => fsNormStockNameKey(b.name).length - fsNormStockNameKey(a.name).length)
      const longestNk = fsNormStockNameKey(embedded[0].name)
      const top = embedded.filter((item) => fsNormStockNameKey(item.name) === longestNk)
      if (top.length === 1) {
        const matched = top[0]
        return {
          raw: matched.symbol,
          special: FULLSCREEN_SEARCH_SPECIAL[matched.symbol] || null,
          matchedByName: true,
        }
      }
    }
  }

  return { raw: normalizedCode, special: null, matchedByName: false }
}

const fsSpeechRec = useWatchlistSpeechRecognition()
const fsSpeechListening = fsSpeechRec.listening
const fsSpeechError = fsSpeechRec.errorMessage
const fsSpeechSupported = fsSpeechRec.supported

function emitFullscreenSearchSymbol(sym) {
  const s = String(sym || '').trim()
  if (!s) return
  // 手機送出查詢後鍵盤收合會連續觸發 blur / window.resize / visualViewport.resize。
  // 這段期間保留舊畫布，交由新股票資料完成後一次更新，避免連續清畫布造成閃爍。
  suppressSearchLayoutUntil = Date.now() + 2000
  const shouldCloseMobilePanel = showMobileToolbarSearchTrigger.value && mobileToolbarSearchOpen.value
  if (!shouldCloseMobilePanel) {
    blurFullscreenSearchInput()
  }
  emit('search-symbol', s.toUpperCase())
  if (shouldCloseMobilePanel) {
    closeMobileToolbarSearch()
  }
  fullscreenSearchSymbol.value = ''
  nextTick(() => {
    if (shouldCloseMobilePanel) return
    if (!shouldRefocusFullscreenSearchInput()) return
    const el = fullscreenSearchInput.value
    if (el && typeof el.focus === 'function') {
      el.focus()
    }
  })
}

function applyFullscreenSpeechTranscript(text) {
  fsSpeechRec.errorMessage.value = ''
  const pre = preprocessSpeechTranscriptForStock(text).trim()
  if (!pre) return
  const { codes } = extractStockCodesFromSpeech(pre)
  if (codes.length >= 1) {
    emitFullscreenSearchSymbol(codes[0])
    return
  }
  const resolved = resolveFullscreenStockSearch(pre)
  if (resolved.special) {
    emitFullscreenSearchSymbol('TWII')
    return
  }
  if (/^\d{4,5}$/.test(String(resolved.raw || ''))) {
    emitFullscreenSearchSymbol(resolved.raw)
    return
  }
  if (resolved.matchedByName && resolved.raw) {
    emitFullscreenSearchSymbol(resolved.raw)
    return
  }
  fsSpeechRec.errorMessage.value = '無法對應股票代號或名稱，請再試一次'
}

function toggleFullscreenSpeechSearch() {
  if (fsSpeechListening.value) {
    fsSpeechRec.stop()
    fullscreenSearchInputMode.value = 'keyboard'
    return
  }
  fullscreenSearchInputMode.value = 'voice'
  blurFullscreenSearchInput()
  fsSpeechRec.errorMessage.value = ''
  ensureFullscreenSearchMaster().then(() => {
    fsSpeechRec.start((t) => applyFullscreenSpeechTranscript(t))
  })
}

function setFullscreenSearchInputMode(mode) {
  const next = mode === 'voice' ? 'voice' : 'keyboard'
  if (fullscreenSearchInputMode.value === next) return
  fullscreenSearchInputMode.value = next
  if (next === 'keyboard') {
    try {
      fsSpeechRec.stop()
    } catch (_) {}
    fsSpeechRec.errorMessage.value = ''
    nextTick(() => {
      const el = fullscreenSearchInput.value
      if (el && typeof el.focus === 'function') {
        el.focus()
      }
    })
  } else {
    fsSpeechRec.errorMessage.value = ''
  }
}

watch(() => props.fullscreenSearchEnabled, (en) => {
  if (en) ensureFullscreenSearchMaster()
}, { immediate: true })

watch(fsSpeechListening, (listening) => {
  if (listening) return
  nextTick(() => {
    setTimeout(() => {
      try {
        if (chartInstance) handleWindowResize()
      } catch (_) {}
    }, 200)
  })
})

function extractName(obj) {
  if (!obj) return ''
  const keys = ['short_name','shortName','name','longName','companyName','company_name','stock_name','displayName','symbolName','title']
  for (const k of keys) {
    const v = obj?.[k]
    if (v && String(v).trim()) return String(v).trim()
  }
  if (obj?.data && typeof obj.data === 'object') {
    for (const k of keys) {
      const v = obj.data?.[k]
      if (v && String(v).trim()) return String(v).trim()
    }
  }
  return ''
}

async function resolveStockName(symbol) {
  try {
    if (!symbol) { resolvedName.value = ''; return }
    const upper = String(symbol).toUpperCase()
    if (SPECIAL_SYMBOL_NAMES.has(upper)) {
      resolvedName.value = SPECIAL_SYMBOL_NAMES.get(upper) || ''
      return
    }
    // If parent already provided, keep it
    if (props.stockName && props.stockName.trim()) { resolvedName.value = props.stockName.trim(); return }
    const baseSym = String(symbol).split('.')?.[0]
    const candidates = Array.from(new Set([
      String(symbol),
      baseSym,
      `${baseSym}.TW`,
      `${baseSym}.TWO`,
      `${baseSym}.TSE`,
      `${baseSym}.TPEX`
    ]))
    resolvedName.value = ''
    for (const s of candidates) {
      try {
        const quote = await fetchStockQuote(s)
        const name = extractName(quote)
        if (name) { resolvedName.value = name; break }
      } catch (_) {}
    }
    // Fallback 2: comparison API (reads from tw_stock_symbols meta)
    if (!resolvedName.value) {
      try {
        const comp = await fetchComparison({ symbols: candidates, period: 'daily' })
        const rows = Array.isArray(comp?.data) ? comp.data : []
        const found = rows.find(r => (r?.short_name && String(r.short_name).trim()) || (r?.name && String(r.name).trim()))
        if (found) {
          resolvedName.value = (found.short_name && String(found.short_name).trim()) || (found.name && String(found.name).trim()) || ''
        }
      } catch (_) {}
    }
    if (!resolvedName.value) {
      const periods = ['daily','weekly','monthly','quarterly','yearly']
      for (const p of periods) {
        const list = await fetchRankings({ period: p, market: 'all', limit: 1200 })
        const found = Array.isArray(list) ? list.find(r => String(r.symbol).split('.')?.[0] === baseSym) : null
        if (found && (found.short_name || found.shortName || found.name)) {
          resolvedName.value = found.short_name || found.shortName || found.name
          break
        }
      }
    }
  } catch (e) {
    resolvedName.value = ''
  }
}

onMounted(async () => {
  await resolveStockName(props.symbol)
  displayedTitleSymbol.value = String(props.symbol || '').toUpperCase()
  displayedTitleName.value = (props.stockName && props.stockName.trim()) || resolvedName.value || getSpecialSymbolName(props.symbol)
  loadDrawingsFromStorage(props.symbol)
})

watch(() => props.symbol, async (newSymbol, oldSymbol) => {
  if (!newSymbol || newSymbol === oldSymbol) return
  const requestId = ++symbolChangeRequestId
  symbolSwitchingInProgress = true
  displayedTitleSymbol.value = String(newSymbol || '').toUpperCase()
  displayedTitleName.value =
    (props.stockName && props.stockName.trim()) ||
    getSpecialSymbolName(newSymbol) ||
    ''

  try {
    // 換股時重設預設週期，但擋住 selectedPeriodKey watcher，避免與下方 loadChartData 雙重載入／閃爍
    const nextPeriodKey = resolveInitialKey(props.period)
    if (selectedPeriodKey.value !== nextPeriodKey) {
      suppressSelectedPeriodAutoLoad = true
      selectedPeriodKey.value = nextPeriodKey
      await nextTick()
      suppressSelectedPeriodAutoLoad = false
    }
    loadParamsForCurrentPeriod()
    mobilePinnedHoverIdx.value = null
    mobileTooltipDismissed.value = false
    loadDrawingsFromStorage(newSymbol)

    const namePromise = resolveStockName(newSymbol)
    await loadChartData()
    if (requestId !== symbolChangeRequestId) return
    try { await namePromise } catch (_) {}
    if (requestId !== symbolChangeRequestId) return
    const nextTitleName = (props.stockName && props.stockName.trim()) || resolvedName.value || getSpecialSymbolName(newSymbol)
    displayedTitleName.value = nextTitleName
  } finally {
    if (requestId === symbolChangeRequestId) {
      suppressSelectedPeriodAutoLoad = false
      symbolSwitchingInProgress = false
    }
  }
})

watch(() => props.stockName, async (newName) => {
  if (String(props.symbol || '').toUpperCase() !== displayedTitleSymbol.value) return
  if (newName && newName.trim()) {
    resolvedName.value = newName.trim()
    displayedTitleName.value = newName.trim()
    return
  }
  resolvedName.value = ''
  await resolveStockName(props.symbol)
  if (String(props.symbol || '').toUpperCase() !== displayedTitleSymbol.value) return
  displayedTitleName.value = resolvedName.value || getSpecialSymbolName(props.symbol)
})

async function runAiTechnicalAnalysis() {
  try {
    controlPanelOpen.value = false
    persistControlPanelOpenState()
    if (showMobileFsToolbarCollapseUi.value) {
      mobileFsToolbarCollapsed.value = true
    }
  } catch (_) {}
  aiModalOpen.value = true
  aiLoading.value = true
  aiError.value = ''
  aiText.value = ''
  aiUsage.value = null
  try {
    const payload = {
      symbol: props.symbol,
      name: displayName.value,
      timeframe: selectedPeriodKey.value,
      snapshot: aiSnapshot.value,
    }
    const json = await fetchAiTechnicalAnalysis(payload)
    const text = json?.data?.text || ''
    aiUsage.value = json?.data?.usage || null
    aiText.value = text
    if (!text) {
      aiError.value = 'AI 回覆為空'
    }
  } catch (e) {
    try {
      const msg = e?.message ? String(e.message) : ''
      if (/HTTP\s*429/.test(msg)) {
        aiError.value = '今日 AI 分析額度已用完，請明天再試或升級方案'
      } else if (/HTTP\s*401/.test(msg)) {
        aiError.value = '登入後可使用 AI 分析'
      } else {
        aiError.value = msg || 'AI 分析失敗'
      }
    } catch {
      aiError.value = 'AI 分析失敗'
    }
  } finally {
    aiLoading.value = false
  }
}

const chartData = ref([])
/** 未過濾的原始 K 線；全螢幕時若末根 OHLC 與前一根相同會從 chartData 剔除 */
const rawChartData = ref([])
const loading = ref(false)
const chartError = ref('')
const chartErrorDetail = ref('')
let chartDataRequestId = 0
const chartDataCache = new Map()
const CHART_DATA_CACHE_LIMIT = 80
const chartContainer = ref(null)
/** ECharts init 專用內層節點（避免 Vue 與查價鈕 DOM 被 init 清空） */
const chartMountEl = ref(null)
const rootEl = ref(null)
const drawingMenuVisible = ref(false)
const drawingMenuPosition = reactive({ x: 0, y: 0 })
const drawingMode = ref('')
const drawingDraftStart = ref(null)
const chartDrawings = ref([])
let nextDrawingId = 1
const fullscreenHostState = {
  el: null,
  style: '',
}
let chartInstance = null
let dataZoomListener = null
let dataZoomIndexes = []
let chartContextMenuListener = null
let chartZrClickListener = null

/** After setOption, ECharts may settle to a shorter canvas; re-run layout up to this depth */
let subplotRelayoutDepth = 0

let renderRetryRafId = null
let renderRetryCount = 0
let renderRetryTimerId = null

function setChartError(message, error = null) {
  chartError.value = String(message || '圖表無法顯示')
  const raw = error?.message ?? error
  chartErrorDetail.value = raw ? String(raw).slice(0, 240) : ''
}

function clearChartError() {
  chartError.value = ''
  chartErrorDetail.value = ''
}

function describeChartLoadError(error) {
  const message = String(error?.message || error || '')
  if (/401|unauthorized/i.test(message)) return '登入狀態已失效，請重新登入'
  if (/403|forbidden/i.test(message)) return '目前帳號沒有讀取圖表資料的權限'
  if (/404|not found/i.test(message)) return '找不到這檔股票的歷史資料'
  if (/429|too many requests/i.test(message)) return '查詢次數過多，請稍後再試'
  if (/quiet hours|DB_QUIET_HOURS/i.test(message)) return '資料庫目前在維護時段，暫時無法讀取'
  if (/failed to fetch|network|load failed/i.test(message)) return '網路連線失敗，無法取得圖表資料'
  if (/5\d\d|service unavailable/i.test(message)) return '圖表資料服務暫時無法使用'
  return '圖表資料載入失敗'
}

function retryChartLoad() {
  clearChartError()
  const symbol = String(props.symbol || '').trim().toUpperCase()
  const option = periodKeyMap.get(selectedPeriodKey.value)
  const periodValue = option?.period ?? DEFAULT_PERIOD
  chartDataCache.delete(`${symbol}|${periodValue}`)
  loadChartData()
}

function resetRenderRetryState() {
  if (renderRetryRafId != null) {
    try { cancelAnimationFrame(renderRetryRafId) } catch (_) {}
    renderRetryRafId = null
  }
  if (renderRetryTimerId != null) {
    try { clearTimeout(renderRetryTimerId) } catch (_) {}
    renderRetryTimerId = null
  }
  renderRetryCount = 0
}

function isDomMeasurable(dom) {
  try {
    const w = Number(dom?.clientWidth || 0)
    const h = Number(dom?.clientHeight || 0)
    return w > 0 && h > 0
  } catch (_) {
    return false
  }
}

/** Pixels of `el` that intersect the mobile visual viewport (avoids 100svh vs real chrome mismatch). */
function getChartVisibleHeightPx(el) {
  try {
    if (!el || typeof el.getBoundingClientRect !== 'function') return 0
    const r = el.getBoundingClientRect()
    const vv = typeof window !== 'undefined' ? window.visualViewport : null
    let clipTop = 0
    let clipBottom = typeof window !== 'undefined' ? Number(window.innerHeight) || 0 : 0
    if (vv && Number.isFinite(vv.height) && vv.height > 0) {
      clipTop = Number(vv.offsetTop) || 0
      clipBottom = clipTop + Number(vv.height)
    }
    if (!Number.isFinite(clipBottom) || clipBottom <= 0) return 0
    const visTop = Math.max(r.top, clipTop)
    const visBottom = Math.min(r.bottom, clipBottom)
    const h = visBottom - visTop
    return Number.isFinite(h) && h > 0 ? h : 0
  } catch (_) {
    return 0
  }
}

function clearForcedFullscreenChartLayout() {
  const root = rootEl.value
  const wrapper = root?.querySelector?.('.chart-wrapper')
  const container = chartContainer.value
  try {
    root?.style?.removeProperty('--chart-container-height')
    root?.style?.removeProperty('--chart-container-min-height')
  } catch (_) {}
  for (const el of [wrapper, container]) {
    try {
      el?.style?.removeProperty('flex')
      el?.style?.removeProperty('width')
      el?.style?.removeProperty('min-width')
      el?.style?.removeProperty('max-width')
      el?.style?.removeProperty('height')
      el?.style?.removeProperty('min-height')
      el?.style?.removeProperty('max-height')
      el?.style?.removeProperty('overflow')
      el?.style?.removeProperty('box-sizing')
    } catch (_) {}
  }
}

function syncFullscreenChartViewportLayout(reason = '') {
  const root = rootEl.value
  const container = chartContainer.value
  const wrapper = root?.querySelector?.('.chart-wrapper')
  if (!root || !container || !wrapper) return false
  if (!isFullscreen.value) {
    return isDomMeasurable(container)
  }

  let rootW = 0
  let rootH = 0
  try {
    const rect = root.getBoundingClientRect?.()
    rootW = Math.max(rootW, Number(rect?.width || 0))
    rootH = Math.max(rootH, Number(rect?.height || 0))
  } catch (_) {}
  try {
    rootW = Math.max(rootW, Number(root.clientWidth || 0))
    rootH = Math.max(rootH, Number(root.clientHeight || 0))
  } catch (_) {}
  try {
    if (typeof window !== 'undefined') {
      rootW = Math.max(rootW, Number(window.visualViewport?.width || 0), Number(window.innerWidth || 0))
      rootH = Math.max(rootH, Number(window.visualViewport?.height || 0), Number(window.innerHeight || 0))
    }
  } catch (_) {}
  if (!(rootW > 0) || !(rootH > 0)) return false

  let headerH = 0
  try {
    const header = root.querySelector?.('.chart-header')
    const headerRect = header?.getBoundingClientRect?.()
    headerH = Math.max(headerH, Number(headerRect?.height || 0), Number(header?.clientHeight || 0))
  } catch (_) {}

  const resolvedWidth = Math.max(160, Math.round(rootW))
  const resolvedHeight = Math.max(220, Math.round(rootH - headerH))
  const widthPx = `${resolvedWidth}px`
  const heightPx = `${resolvedHeight}px`

  try {
    root.style.setProperty('--chart-container-height', heightPx, 'important')
    root.style.setProperty('--chart-container-min-height', heightPx, 'important')
  } catch (_) {}

  for (const el of [wrapper, container]) {
    try {
      el.style.setProperty('flex', '1 1 0', 'important')
      el.style.setProperty('width', widthPx, 'important')
      el.style.setProperty('min-width', widthPx, 'important')
      el.style.setProperty('max-width', widthPx, 'important')
      el.style.setProperty('height', heightPx, 'important')
      el.style.setProperty('min-height', heightPx, 'important')
      el.style.setProperty('max-height', heightPx, 'important')
      el.style.setProperty('box-sizing', 'border-box', 'important')
    } catch (_) {}
  }
  try {
    wrapper.style.setProperty('overflow', 'hidden', 'important')
  } catch (_) {}

  if (!isDomMeasurable(container) && reason) {
    console.warn('[StockChart debug] syncFullscreenChartViewportLayout unresolved', {
      symbol: props.symbol,
      reason,
      isFullscreen: isFullscreen.value,
      rootClientWidth: root.clientWidth,
      rootClientHeight: root.clientHeight,
      wrapperClientWidth: wrapper.clientWidth,
      wrapperClientHeight: wrapper.clientHeight,
      containerClientWidth: container.clientWidth,
      containerClientHeight: container.clientHeight,
      headerHeight: headerH,
      appliedWidth: resolvedWidth,
      appliedHeight: resolvedHeight,
    })
  }

  return isDomMeasurable(container)
}

function scheduleRenderRetry() {
  if (renderRetryRafId != null || renderRetryTimerId != null) return
  if (renderRetryCount >= 12) {
    console.warn('[StockChart debug] scheduleRenderRetry limit reached', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      renderRetryCount,
    })
    setChartError('圖表容器尺寸異常，無法建立畫布', '請旋轉螢幕、離開全螢幕後再試，或按下重新載入')
    return
  }
  const queueRetryFrame = () => {
    renderRetryRafId = requestAnimationFrame(() => {
      renderRetryRafId = null
      renderRetryCount += 1
      try { syncFullscreenChartViewportLayout(`retry-${renderRetryCount}`) } catch (_) {}
      if (!loading.value && chartData.value && chartData.value.length > 0) {
        renderChart()
      }
    })
  }
  if (renderRetryCount >= 4) {
    renderRetryTimerId = setTimeout(() => {
      renderRetryTimerId = null
      queueRetryFrame()
    }, 120)
    return
  }
  queueRetryFrame()
}

function isChartDisposed(inst) {
  try {
    return !!(inst && typeof inst.isDisposed === 'function' && inst.isDisposed())
  } catch (_) {
    return true
  }
}

function ensureChartInstance() {
  const shell = chartContainer.value
  const dom = chartMountEl.value
  if (!shell || !dom) {
    console.warn('[StockChart debug] ensureChartInstance: missing chart mount', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
    })
    return null
  }

  try { syncFullscreenChartViewportLayout('ensureChartInstance') } catch (_) {}

  if (!isDomMeasurable(dom)) {
    const rect = dom.getBoundingClientRect?.()
    console.warn('[StockChart debug] ensureChartInstance: dom not measurable', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      clientWidth: dom.clientWidth,
      clientHeight: dom.clientHeight,
      rect: rect ? { width: rect.width, height: rect.height, top: rect.top, left: rect.left } : null,
    })
    return null
  }

  try {
    const existing = echarts.getInstanceByDom(dom)
    if (existing && !isChartDisposed(existing)) {
      chartInstance = existing
      console.log('[StockChart debug] ensureChartInstance: reuse existing instance', {
        symbol: props.symbol,
        isFullscreen: isFullscreen.value,
        chartDataLength: chartData.value.length,
        clientWidth: dom.clientWidth,
        clientHeight: dom.clientHeight,
      })
      return chartInstance
    }
  } catch (_) {}

  if (chartInstance && isChartDisposed(chartInstance)) {
    chartInstance = null
  }
  if (!chartInstance) {
    try {
      chartInstance = echarts.init(dom)
      console.log('[StockChart debug] ensureChartInstance: created new instance', {
        symbol: props.symbol,
        isFullscreen: isFullscreen.value,
        chartDataLength: chartData.value.length,
        clientWidth: dom.clientWidth,
        clientHeight: dom.clientHeight,
      })
    } catch (e) {
      console.error('Failed to initialize ECharts:', e)
      setChartError('圖表初始化失敗', e)
      chartInstance = null
    }
  }
  return chartInstance
}

/**
 * 首次繪圖可完整建立 option；後續換股則以 replaceMerge 原子替換主要圖表元件。
 * 避免 setOption(option, true) 先清空 canvas，再繪新資料所產生的白／暗閃。
 */
function setChartOptionWithoutBlank(inst, option) {
  let hasExistingSeries = false
  try {
    const current = inst?.getOption?.()
    hasExistingSeries = Array.isArray(current?.series) && current.series.length > 0
  } catch (_) {}

  if (!hasExistingSeries) {
    inst.setOption(option, true)
    return
  }

  inst.setOption(option, {
    notMerge: false,
    lazyUpdate: false,
    silent: true,
    replaceMerge: ['grid', 'xAxis', 'yAxis', 'dataZoom', 'legend', 'graphic', 'series'],
  })
}

function safeResize() {
  const inst = chartInstance
  if (!inst || isChartDisposed(inst)) {
    console.warn('[StockChart debug] safeResize skipped: no live instance', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      hasInstance: !!inst,
      chartDataLength: chartData.value.length,
    })
    return
  }
  try {
    const dom = inst.getDom?.() || chartContainer.value
    if (dom && !isDomMeasurable(dom)) {
      const rect = dom.getBoundingClientRect?.()
      console.warn('[StockChart debug] safeResize skipped: dom not measurable', {
        symbol: props.symbol,
        isFullscreen: isFullscreen.value,
        clientWidth: dom.clientWidth,
        clientHeight: dom.clientHeight,
        rect: rect ? { width: rect.width, height: rect.height, top: rect.top, left: rect.left } : null,
      })
      return
    }
  } catch (_) {}
  try {
    const dom = inst.getDom?.() || chartContainer.value
    const rect = dom?.getBoundingClientRect?.()
    console.log('[StockChart debug] safeResize running', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      clientWidth: dom?.clientWidth,
      clientHeight: dom?.clientHeight,
      rect: rect ? { width: rect.width, height: rect.height, top: rect.top, left: rect.left } : null,
    })
    inst.resize()
  } catch (e) {
    console.error('[StockChart debug] safeResize error', e)
  }
}

/** After the mobile keyboard closes, viewport settles late — full layout pass (safeResize alone can leave stale option sizing). */
function onFullscreenSearchInputBlur() {
  if (fsSpeechListening.value) return
  if (Date.now() < suppressSearchLayoutUntil) return
  if (shouldSuppressFullscreenSearchBlurResize()) return
  requestAnimationFrame(() => {
    try { handleWindowResize() } catch (_) {}
  })
  setTimeout(() => {
    try { handleWindowResize() } catch (_) {}
  }, 220)
}

let hoverUpdateRafId = null
let pendingHoverIdx = null
const mobilePinnedHoverIdx = ref(null)
const mobileTooltipDismissed = ref(false)
const crosshairLookupNavCollapsed = ref(true)
const crosshairLookupNavEl = ref(null)
const crosshairLookupNavPosition = reactive({ x: null, y: null })
const crosshairLookupNavDrag = reactive({
  active: false,
  pointerId: null,
  startClientX: 0,
  startClientY: 0,
  originX: 0,
  originY: 0,
  moved: false,
})
let crosshairLookupNavSuppressClickUntil = 0
/** 手機／全螢幕下拉版面：關閉後滑動僅平移縮放，不顯示十字線與軸向 tooltip */
const klineCrosshairLookupEnabled = ref(true)

let buildLegendTextUpdatesAtFn = null
let updateMaLegendStateAtFn = null
let buildLegendsAtFn = null
const selectedDrawingId = ref(null)
const mobileMoveDrawingId = ref(null)
const drawingMenuTargetId = ref(null)
const drawingDragState = reactive({ active: false, drawingId: null, handle: '', moved: false, pending: false, startX: 0, startY: 0, isTouch: false })
let chartZrMouseDownListener = null
let chartZrMouseMoveListener = null
let chartZrMouseUpListener = null
let chartTouchStartListener = null
let chartTouchMoveListener = null
let chartTouchEndListener = null
let chartTouchCancelListener = null
let chartCrosshairTouchStartListener = null
let chartCrosshairTouchMoveListener = null
let chartCrosshairTouchEndListener = null
let chartTooltipCloseClickListener = null
/** Mobile: while dragging inside dataZoom (pan/zoom), suppress axis tooltip so it does not cover candles */
let mobileChartFingerDown = false
/** 響應式：縮放／橫滑拖曳時隱藏查價小卡並停止圖例跟隨，避免與均線列閃爍重疊 */
const suppressMobileTooltipDuringChartPanRef = ref(false)
let mobileCrosshairTouchAnchor = null
/** 手機迷你均線 scroll 列：垂直命中區（相對於 chart DOM 頂端的 px），供滑動換頁 */
const qgMaLegendStripLayout = { active: false, yMin: 0, yMax: 0, legendGraphicTop: 0, dockTopPx: 0 }
/** 手機布林圖例：垂直堆疊列（均線 → 查價 → 布林 → K 線） */
const qgBbLegendLayout = { active: false, bandPx: 0, topPx: 0, left: '12%', mobileSide: 'left' }

/**
 * 與均線圖例文字左緣對齊的 px（均線 strip：left 3% + padding + line icon）。
 * 供布林圖例／查價 dock 共用。
 */
function resolveMaAlignedLegendLeftPx(widthHint) {
  const w = Number(widthHint)
  const base = (Number.isFinite(w) && w > 0 ? w : 360) * 0.03
  // legend-ma-main-r1/r2: left 3%, padding-left 4, itemWidth 11, 再加小間距
  return Math.max(8, Math.round(base + 4 + 11 + 2))
}

function resolveBbLegendLeft(alignToMaStrip, widthHint) {
  if (!alignToMaStrip) return '12%'
  // 與均線圖例文字左緣對齊（含手機布林垂直堆疊）
  return resolveMaAlignedLegendLeftPx(widthHint)
}
let mobileMaLegendSwipeTouch = null

function clientYInMaLegendStrip(clientY, chartDom) {
  const s = qgMaLegendStripLayout
  if (!s.active || !chartDom || typeof clientY !== 'number') return false
  const r = chartDom.getBoundingClientRect?.()
  if (!r || !Number.isFinite(r.top)) return false
  const y = clientY - r.top
  return y >= s.yMin && y <= s.yMax
}

/** 與 `.mobile-pinned-lookup-aside` dock 區塊高度一致（供對齊均線列／主圖栅） */
const PINNED_LOOKUP_ASIDE_DOCK_HEIGHT_PX = 26

function dispatchMaLegendHorizontalScroll(chartInst, dx) {
  if (!chartInst || typeof dx !== 'number' || isChartDisposed(chartInst)) return false
  if (!qgMaLegendStripLayout.active) return false
  try {
    const opt = chartInst.getOption?.()?.legend
    const legs = Array.isArray(opt) ? opt : opt ? [opt] : []
    const leg = legs.find((l) => l && l.id === 'legend-ma-main') || legs[0]
    if (!leg || leg.type !== 'scroll') return false
    const dataLen = Array.isArray(leg.data) ? leg.data.length : 0
    if (dataLen < 2) return false
    const cur = Number(leg.scrollDataIndex) || 0
    const step = dx < 0 ? 1 : -1
    const next = Math.min(Math.max(cur + step, 0), Math.max(0, dataLen - 1))
    if (next === cur) return false
    chartInst.dispatchAction({
      type: 'legendScroll',
      scrollDataIndex: next,
      legendId: 'legend-ma-main',
    })
    return true
  } catch (_) {
    return false
  }
}
const drawingStylePalette = ['#fbbf24', '#60a5fa', '#ef4444', '#f472b6', '#34d399', '#c4b5fd', '#f87171']
const drawingLongPressState = reactive({
  active: false,
  triggered: false,
  startX: 0,
  startY: 0,
  clientX: 0,
  clientY: 0,
  drawingId: null,
  suppressClick: false,
})
let drawingLongPressTimer = null

function defaultDrawingColor(type) {
  if (type === 'horizontal') return '#60a5fa'
  if (type === 'vertical') return '#f472b6'
  if (type === 'rect') return '#34d399'
  if (type === 'channel') return '#c4b5fd'
  return '#fbbf24'
}

function normalizeDrawingStyle(drawing) {
  const style = drawing?.style && typeof drawing.style === 'object' ? drawing.style : {}
  const width = Number(style.width)
  return {
    color: typeof style.color === 'string' && style.color.trim() ? style.color : defaultDrawingColor(drawing?.type),
    width: Number.isFinite(width) ? Math.max(1, Math.min(6, width)) : 2,
    dash: style.dash === 'dashed' ? 'dashed' : 'solid',
  }
}

function normalizeDrawingRecord(drawing) {
  if (!drawing || typeof drawing !== 'object') return null
  return { ...drawing, style: normalizeDrawingStyle(drawing) }
}

const selectedDrawing = computed(() => {
  return (chartDrawings.value || []).find((item) => item?.id === selectedDrawingId.value) || null
})

function updateSelectedDrawingStyle(patch) {
  if (selectedDrawingId.value == null) return
  replaceDrawingById(selectedDrawingId.value, (drawing) => ({
    ...drawing,
    style: {
      ...normalizeDrawingStyle(drawing),
      ...patch,
    },
  }))
  if (!loading.value && chartData.value.length > 0) renderChart()
}

function isDesktopFullscreenDrawingEnabled() {
  try {
    return !!isFullscreen.value && !isMobileViewport()
  } catch (_) {
    return false
  }
}

function isMobileFullscreenDrawingEnabled() {
  try {
    return false
  } catch (_) {
    return false
  }
}

function isAnyFullscreenDrawingEnabled() {
  return isDesktopFullscreenDrawingEnabled() || isMobileFullscreenDrawingEnabled()
}

function clearDrawingLongPressTimer() {
  if (drawingLongPressTimer != null) {
    try { clearTimeout(drawingLongPressTimer) } catch (_) {}
    drawingLongPressTimer = null
  }
}

function resetDrawingLongPressState() {
  clearDrawingLongPressTimer()
  drawingLongPressState.active = false
  drawingLongPressState.triggered = false
  drawingLongPressState.startX = 0
  drawingLongPressState.startY = 0
  drawingLongPressState.clientX = 0
  drawingLongPressState.clientY = 0
  drawingLongPressState.drawingId = null
}

function resetDrawingDragState() {
  drawingDragState.active = false
  drawingDragState.drawingId = null
  drawingDragState.handle = ''
  drawingDragState.moved = false
  drawingDragState.pending = false
  drawingDragState.startX = 0
  drawingDragState.startY = 0
  drawingDragState.isTouch = false
}

function triggerDrawingHapticFeedback(duration = 12) {
  try {
    if (typeof navigator === 'undefined' || typeof navigator.vibrate !== 'function') return
    navigator.vibrate(duration)
  } catch (_) {}
}

function isMobileMoveArmed(id = selectedDrawingId.value) {
  return id != null && mobileMoveDrawingId.value === id
}

function toggleMobileMoveMode(id = selectedDrawingId.value) {
  if (id == null) {
    mobileMoveDrawingId.value = null
    return
  }
  mobileMoveDrawingId.value = mobileMoveDrawingId.value === id ? null : id
}

function scheduleDrawingLongPress(clientX, clientY, drawingId = null) {
  resetDrawingLongPressState()
  drawingLongPressState.active = true
  drawingLongPressState.startX = clientX
  drawingLongPressState.startY = clientY
  drawingLongPressState.clientX = clientX
  drawingLongPressState.clientY = clientY
  drawingLongPressState.drawingId = drawingId ?? null
  drawingLongPressTimer = setTimeout(() => {
    if (!drawingLongPressState.active) return
    drawingLongPressState.triggered = true
    drawingLongPressState.suppressClick = true
    openDrawingMenuAt(drawingLongPressState.clientX, drawingLongPressState.clientY, drawingLongPressState.drawingId)
    clearDrawingLongPressTimer()
  }, 460)
}

function openDrawingMenuAt(clientX, clientY, targetId = null) {
  const wrapperRect = rootEl.value?.querySelector?.('.chart-wrapper')?.getBoundingClientRect?.()
  const fallbackRect = chartInstance?.getDom?.()?.getBoundingClientRect?.()
  const rect = wrapperRect || fallbackRect
  const width = Number(rect?.width || 0)
  const height = Number(rect?.height || 0)
  const rawX = Math.max(12, Number(clientX || 0) - Number(rect?.left || 0))
  const rawY = Math.max(12, Number(clientY || 0) - Number(rect?.top || 0))
  drawingMenuPosition.x = width > 0 ? Math.min(width - 12, rawX) : rawX
  drawingMenuPosition.y = height > 0 ? Math.min(height - 12, rawY) : rawY
  drawingMenuTargetId.value = targetId ?? null
  if (targetId != null) selectedDrawingId.value = targetId
  drawingMenuVisible.value = true
}

function closeDrawingMenu() {
  drawingMenuVisible.value = false
  drawingMenuTargetId.value = null
}

function cancelDrawingMode() {
  drawingMode.value = ''
  drawingDraftStart.value = null
}

function resetDrawingUi() {
  mobileMoveDrawingId.value = null
  closeDrawingMenu()
  cancelDrawingMode()
}

function getDrawingStorageKey(symbolRaw = props.symbol) {
  const key = String(symbolRaw || '').trim().toUpperCase() || 'UNKNOWN'
  return `chartDrawings:${key}`
}

function persistDrawingsToStorage() {
  try {
    localStorage.setItem(getDrawingStorageKey(), JSON.stringify(chartDrawings.value || []))
  } catch (_) {}
}

function loadDrawingsFromStorage(symbolRaw = props.symbol) {
  try {
    const raw = localStorage.getItem(getDrawingStorageKey(symbolRaw))
    const parsed = JSON.parse(raw || '[]')
    const list = Array.isArray(parsed)
      ? parsed.map((v) => normalizeDrawingRecord(v)).filter(v => v && typeof v === 'object')
      : []
    chartDrawings.value = list
    let maxId = 0
    for (const item of list) {
      const id = Number(item?.id)
      if (Number.isFinite(id) && id > maxId) maxId = id
    }
    nextDrawingId = Math.max(1, maxId + 1)
    selectedDrawingId.value = null
    resetDrawingUi()
  } catch (_) {
    chartDrawings.value = []
    nextDrawingId = 1
    selectedDrawingId.value = null
    resetDrawingUi()
  }
}

function updateDrawings(mutator) {
  const next = typeof mutator === 'function' ? mutator(Array.isArray(chartDrawings.value) ? chartDrawings.value : []) : mutator
  chartDrawings.value = Array.isArray(next) ? next.map((item) => normalizeDrawingRecord(item)).filter(Boolean) : []
  persistDrawingsToStorage()
}

function replaceDrawingById(id, updater) {
  updateDrawings((list) => list.map((item) => {
    if (item?.id !== id) return item
    return typeof updater === 'function' ? updater(item) : item
  }))
}

function selectDrawing(id) {
  if (id == null || mobileMoveDrawingId.value != null && mobileMoveDrawingId.value !== id) {
    mobileMoveDrawingId.value = null
  }
  selectedDrawingId.value = id ?? null
  if (!loading.value && chartData.value.length > 0) {
    renderChart()
  }
}

function deleteDrawingById(id) {
  if (id == null) return
  updateDrawings((list) => list.filter((item) => item?.id !== id))
  if (mobileMoveDrawingId.value === id) mobileMoveDrawingId.value = null
  if (selectedDrawingId.value === id) selectedDrawingId.value = null
  resetDrawingUi()
  if (!loading.value && chartData.value.length > 0) {
    renderChart()
  }
}

function clearAllDrawings() {
  updateDrawings([])
  mobileMoveDrawingId.value = null
  selectedDrawingId.value = null
  resetDrawingUi()
  if (!loading.value && chartData.value.length > 0) {
    renderChart()
  }
}

function startDrawingMode(mode) {
  drawingMode.value = mode
  drawingDraftStart.value = null
  selectedDrawingId.value = null
  closeDrawingMenu()
}

function toggleDrawingMode(mode) {
  if (drawingMode.value === mode) {
    cancelDrawingMode()
  } else {
    startDrawingMode(mode)
  }
  closeFsToolsMenu()
  if (!loading.value && chartData.value.length > 0) {
    renderChart()
  }
}

function stopDrawingModeFromToolbar() {
  cancelDrawingMode()
  closeFsToolsMenu()
  if (!loading.value && chartData.value.length > 0) {
    renderChart()
  }
}

function startTrendLineDrawing() {
  startDrawingMode('trend')
}

function startHorizontalLineDrawing() {
  startDrawingMode('horizontal')
}

function startVerticalLineDrawing() {
  startDrawingMode('vertical')
}

function startRectDrawing() {
  startDrawingMode('rect')
}

function startChannelDrawing() {
  startDrawingMode('channel')
}

function buildDrawingInstructionText() {
  if (drawingMode.value === 'trend') {
    return drawingDraftStart.value ? '請左鍵點選終點' : '請左鍵點選起點'
  }
  if (drawingMode.value === 'horizontal') {
    return '請左鍵點選價格位置'
  }
  if (drawingMode.value === 'vertical') {
    return '請左鍵點選時間位置'
  }
  if (drawingMode.value === 'rect') {
    return drawingDraftStart.value ? '請左鍵點選矩形對角點' : '請左鍵點選矩形第一個角'
  }
  if (drawingMode.value === 'channel') {
    if (!drawingDraftStart.value) return '請左鍵點選通道起點'
    if (!drawingDraftStart.value.endIndex && drawingDraftStart.value.endIndex !== 0) return '請左鍵點選通道終點'
    return '請左鍵點選平行通道寬度'
  }
  return ''
}

function clampDrawingIndex(rawIndex, len) {
  const idx = Number(rawIndex)
  if (!Number.isFinite(idx)) return 0
  return Math.max(0, Math.min(len - 1, Math.round(idx)))
}

function resolveDrawingXIndex(rawX, dates) {
  const len = Array.isArray(dates) ? dates.length : 0
  if (!len) return 0
  if (typeof rawX === 'number') return clampDrawingIndex(rawX, len)
  const index = dates.indexOf(rawX)
  if (index >= 0) return index
  return 0
}

function getMainPriceBounds() {
  let minPrice = Infinity
  let maxPrice = -Infinity
  for (const row of chartData.value || []) {
    const low = Number(row?.low ?? row?.[2])
    const high = Number(row?.high ?? row?.[3])
    if (Number.isFinite(low) && low < minPrice) minPrice = low
    if (Number.isFinite(high) && high > maxPrice) maxPrice = high
  }
  if (!Number.isFinite(minPrice) || !Number.isFinite(maxPrice) || maxPrice <= minPrice) {
    return { minPrice: 0, maxPrice: 1 }
  }
  return { minPrice, maxPrice }
}

function getDrawingGeometry(inst, drawing, dates) {
  if (!inst || !drawing || !Array.isArray(dates) || !dates.length) return null
  const { minPrice, maxPrice } = getMainPriceBounds()
  let gridRect = null
  try {
    gridRect = inst.getModel?.()?.getComponent?.('grid', 0)?.coordinateSystem?.getRect?.() || null
  } catch (_) {
    gridRect = null
  }
  const toPixel = (xIndex, price) => {
    try {
      return inst.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [clampDrawingIndex(xIndex, dates.length), Number(price)])
    } catch (_) {
      return null
    }
  }
  if (drawing.type === 'trend') {
    const start = toPixel(drawing.startIndex, drawing.startPrice)
    const end = toPixel(drawing.endIndex, drawing.endPrice)
    if (!Array.isArray(start) || !Array.isArray(end)) return null
    return {
      lines: [{ x1: start[0], y1: start[1], x2: end[0], y2: end[1] }],
      handles: {
        start: { x: start[0], y: start[1] },
        end: { x: end[0], y: end[1] },
      },
    }
  }
  if (drawing.type === 'horizontal') {
    const left = toPixel(0, drawing.price)
    const right = toPixel(dates.length - 1, drawing.price)
    if (!Array.isArray(left) || !Array.isArray(right)) return null
    const x1 = Number.isFinite(gridRect?.x) ? Number(gridRect.x) : left[0]
    const x2 = Number.isFinite(gridRect?.x) && Number.isFinite(gridRect?.width)
      ? Number(gridRect.x) + Number(gridRect.width)
      : right[0]
    return {
      lines: [{ x1, y1: left[1], x2, y2: right[1] }],
      handles: {
        body: { x: (x1 + x2) / 2, y: left[1] },
      },
    }
  }
  if (drawing.type === 'vertical') {
    const top = toPixel(drawing.xIndex, maxPrice)
    const bottom = toPixel(drawing.xIndex, minPrice)
    if (!Array.isArray(top) || !Array.isArray(bottom)) return null
    return {
      lines: [{ x1: top[0], y1: top[1], x2: bottom[0], y2: bottom[1] }],
      handles: {
        body: { x: top[0], y: (top[1] + bottom[1]) / 2 },
      },
    }
  }
  if (drawing.type === 'rect') {
    const start = toPixel(drawing.startIndex, drawing.startPrice)
    const end = toPixel(drawing.endIndex, drawing.endPrice)
    if (!Array.isArray(start) || !Array.isArray(end)) return null
    const x = Math.min(start[0], end[0])
    const y = Math.min(start[1], end[1])
    const width = Math.abs(end[0] - start[0])
    const height = Math.abs(end[1] - start[1])
    return {
      rect: { x, y, width, height },
      handles: {
        start: { x: start[0], y: start[1] },
        end: { x: end[0], y: end[1] },
        body: { x: x + width / 2, y: y + height / 2 },
      },
    }
  }
  if (drawing.type === 'channel') {
    const start = toPixel(drawing.startIndex, drawing.startPrice)
    const end = toPixel(drawing.endIndex, drawing.endPrice)
    const start2 = toPixel(drawing.startIndex, Number(drawing.startPrice) + Number(drawing.offsetPrice || 0))
    const end2 = toPixel(drawing.endIndex, Number(drawing.endPrice) + Number(drawing.offsetPrice || 0))
    if (!Array.isArray(start) || !Array.isArray(end) || !Array.isArray(start2) || !Array.isArray(end2)) return null
    return {
      lines: [
        { x1: start[0], y1: start[1], x2: end[0], y2: end[1] },
        { x1: start2[0], y1: start2[1], x2: end2[0], y2: end2[1] },
      ],
      polygon: [[start[0], start[1]], [end[0], end[1]], [end2[0], end2[1]], [start2[0], start2[1]]],
      handles: {
        start: { x: start[0], y: start[1] },
        end: { x: end[0], y: end[1] },
        body: { x: (start[0] + end[0] + start2[0] + end2[0]) / 4, y: (start[1] + end[1] + start2[1] + end2[1]) / 4 },
        offset: { x: (start2[0] + end2[0]) / 2, y: (start2[1] + end2[1]) / 2 },
      },
    }
  }
  return null
}

function distancePointToSegment(px, py, x1, y1, x2, y2) {
  const dx = x2 - x1
  const dy = y2 - y1
  if (dx === 0 && dy === 0) return Math.hypot(px - x1, py - y1)
  const t = Math.max(0, Math.min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
  const sx = x1 + t * dx
  const sy = y1 + t * dy
  return Math.hypot(px - sx, py - sy)
}

function isPointInPolygon(x, y, points) {
  if (!Array.isArray(points) || points.length < 3) return false
  let inside = false
  for (let i = 0, j = points.length - 1; i < points.length; j = i++) {
    const xi = Number(points[i]?.[0])
    const yi = Number(points[i]?.[1])
    const xj = Number(points[j]?.[0])
    const yj = Number(points[j]?.[1])
    const intersects = ((yi > y) !== (yj > y)) && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-9) + xi)
    if (intersects) inside = !inside
  }
  return inside
}

function findDrawingHit(inst, x, y, { handlesOnly = false } = {}) {
  const dates = chartData.value.map(d => d.time)
  let best = null
  for (const drawing of chartDrawings.value || []) {
    const geometry = getDrawingGeometry(inst, drawing, dates)
    if (!geometry) continue
    for (const [handleName, point] of Object.entries(geometry.handles || {})) {
      const dist = Math.hypot(x - point.x, y - point.y)
      if (dist <= 11 && (!best || dist < best.distance)) {
        best = { drawingId: drawing.id, handle: handleName, distance: dist }
      }
    }
    if (handlesOnly) continue
    if (geometry.polygon && isPointInPolygon(x, y, geometry.polygon)) {
      const dist = 0
      if (!best || dist < best.distance) best = { drawingId: drawing.id, handle: 'body', distance: dist }
    }
    if (geometry.rect) {
      const rx = geometry.rect.x
      const ry = geometry.rect.y
      const rw = geometry.rect.width
      const rh = geometry.rect.height
      const within = x >= rx - 8 && x <= rx + rw + 8 && y >= ry - 8 && y <= ry + rh + 8
      if (within) {
        const dist = Math.min(Math.abs(x - rx), Math.abs(x - (rx + rw)), Math.abs(y - ry), Math.abs(y - (ry + rh)))
        if (!best || dist < best.distance) best = { drawingId: drawing.id, handle: 'body', distance: dist }
      }
    }
    for (const line of geometry.lines || []) {
      const dist = distancePointToSegment(x, y, line.x1, line.y1, line.x2, line.y2)
      if (dist <= 9 && (!best || dist < best.distance)) {
        best = { drawingId: drawing.id, handle: 'body', distance: dist }
      }
    }
  }
  return best
}

function getSnapPriceForIndex(xIndex, rawPrice) {
  const row = chartData.value?.[clampDrawingIndex(xIndex, chartData.value.length || 0)]
  const price = Number(rawPrice)
  if (!row || !Number.isFinite(price)) return price
  const candidates = [
    Number(row?.open),
    Number(row?.high),
    Number(row?.low),
    Number(row?.close),
  ].filter((value) => Number.isFinite(value))
  if (!candidates.length) return price
  let best = candidates[0]
  let bestDistance = Math.abs(price - best)
  for (let i = 1; i < candidates.length; i++) {
    const distance = Math.abs(price - candidates[i])
    if (distance < bestDistance) {
      best = candidates[i]
      bestDistance = distance
    }
  }
  const candleRange = Math.max(...candidates) - Math.min(...candidates)
  const threshold = Math.max(0.15, candleRange * 0.18)
  return bestDistance <= threshold ? best : price
}

function screenPixelToDataCoord(inst, offsetX, offsetY) {
  try {
    const dataCoord = inst.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [offsetX, offsetY])
    if (!Array.isArray(dataCoord) || dataCoord.length < 2) return null
    const xIndex = resolveDrawingXIndex(dataCoord[0], chartData.value.map(d => d.time))
    const price = getSnapPriceForIndex(xIndex, Number(dataCoord[1]))
    return {
      xIndex,
      price,
    }
  } catch (_) {
    return null
  }
}

function applyDrawingDrag(drawing, handle, xIndex, price) {
  if (!drawing) return drawing
  if (drawing.type === 'trend') {
    if (handle === 'start') return { ...drawing, startIndex: xIndex, startPrice: price }
    if (handle === 'end') return { ...drawing, endIndex: xIndex, endPrice: price }
    if (handle === 'body') {
      const dx = xIndex - Number(drawing.startIndex)
      const dy = price - Number(drawing.startPrice)
      return {
        ...drawing,
        startIndex: Number(drawing.startIndex) + dx,
        endIndex: Number(drawing.endIndex) + dx,
        startPrice: Number(drawing.startPrice) + dy,
        endPrice: Number(drawing.endPrice) + dy,
      }
    }
  }
  if (drawing.type === 'horizontal') return { ...drawing, price }
  if (drawing.type === 'vertical') return { ...drawing, xIndex }
  if (drawing.type === 'rect') {
    if (handle === 'start') return { ...drawing, startIndex: xIndex, startPrice: price }
    if (handle === 'end') return { ...drawing, endIndex: xIndex, endPrice: price }
    if (handle === 'body') {
      const dx = xIndex - Number(drawing.startIndex)
      const dy = price - Number(drawing.startPrice)
      return {
        ...drawing,
        startIndex: Number(drawing.startIndex) + dx,
        endIndex: Number(drawing.endIndex) + dx,
        startPrice: Number(drawing.startPrice) + dy,
        endPrice: Number(drawing.endPrice) + dy,
      }
    }
  }
  if (drawing.type === 'channel') {
    if (handle === 'start') return { ...drawing, startIndex: xIndex, startPrice: price }
    if (handle === 'end') return { ...drawing, endIndex: xIndex, endPrice: price }
    if (handle === 'body') {
      const dx = xIndex - Number(drawing.startIndex)
      const dy = price - Number(drawing.startPrice)
      return {
        ...drawing,
        startIndex: Number(drawing.startIndex) + dx,
        endIndex: Number(drawing.endIndex) + dx,
        startPrice: Number(drawing.startPrice) + dy,
        endPrice: Number(drawing.endPrice) + dy,
      }
    }
    if (handle === 'offset') {
      const basePrice = Number(drawing.startPrice) + ((Number(drawing.endPrice) - Number(drawing.startPrice)) / Math.max(1, Number(drawing.endIndex) - Number(drawing.startIndex) || 1)) * (xIndex - Number(drawing.startIndex))
      return { ...drawing, offsetPrice: price - basePrice }
    }
  }
  return drawing
}

function buildUserDrawingGraphics(inst, dates) {
  if (!inst || !Array.isArray(dates) || dates.length === 0) return []
  const graphics = []
  for (const drawing of chartDrawings.value) {
    if (!drawing || !drawing.type) continue
    const geometry = getDrawingGeometry(inst, drawing, dates)
    if (!geometry) continue
    const selected = selectedDrawingId.value === drawing.id
    const style = normalizeDrawingStyle(drawing)
    const stroke = style.color
    const lineWidth = selected ? Math.min(8, style.width + 0.8) : style.width
    const lineDash = style.dash === 'dashed' ? [8, 6] : undefined
    if (geometry.polygon) {
      graphics.push({
        type: 'polygon',
        id: `user-drawing-fill-${drawing.id}`,
        z: 155,
        zlevel: 3,
        silent: true,
        shape: { points: geometry.polygon },
        style: { fill: selected ? 'rgba(148, 163, 184, 0.18)' : 'rgba(148, 163, 184, 0.10)' },
      })
    }
    if (geometry.rect) {
      graphics.push({
        type: 'rect',
        id: `user-drawing-${drawing.id}`,
        z: 160,
        zlevel: 4,
        silent: true,
        shape: geometry.rect,
        style: {
          stroke,
          fill: selected ? 'rgba(148, 163, 184, 0.10)' : 'rgba(148, 163, 184, 0.05)',
          lineWidth,
          lineDash,
        },
      })
    }
    for (const [index, line] of (geometry.lines || []).entries()) {
      graphics.push({
        type: 'line',
        id: `user-drawing-${drawing.id}-${index}`,
        z: 160,
        zlevel: 4,
        silent: true,
        shape: line,
        style: {
          stroke,
          lineWidth,
          lineDash,
          shadowBlur: selected ? 10 : 8,
          shadowColor: selected ? 'rgba(148, 163, 184, 0.28)' : 'rgba(15, 23, 42, 0.18)',
        },
      })
    }
    if (selected) {
      for (const [handleName, point] of Object.entries(geometry.handles || {})) {
        graphics.push({
          type: 'circle',
          id: `user-drawing-handle-${drawing.id}-${handleName}`,
          z: 170,
          zlevel: 5,
          silent: true,
          shape: { cx: point.x, cy: point.y, r: 5.5 },
          style: {
            fill: '#0f172a',
            stroke,
            lineWidth: 2,
          },
        })
      }
    }
  }
  return graphics
}

function syncMobileCrosshairFromTouch(inst, dom, touch) {
  try {
    if (!inst || !dom || !touch) return false
    if (!klineCrosshairLookupEnabled.value) return false
    if (suppressMobileTooltipDuringChartPanRef.value) return false
    if (drawingMode.value) return false
    if (drawingDragState.active || drawingDragState.pending) return false
    mobileTooltipDismissed.value = false
    const rect = dom.getBoundingClientRect?.()
    const offsetX = Number(touch.clientX) - Number(rect?.left || 0)
    const offsetY = Number(touch.clientY) - Number(rect?.top || 0)
    if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) return false
    const containMain = inst.containPixel?.({ gridIndex: 0 }, [offsetX, offsetY])
    if (!containMain) return false
    const coord = screenPixelToDataCoord(inst, offsetX, offsetY)
    if (!coord || !Number.isFinite(coord.xIndex)) return false
    const axisValue = chartData.value?.[coord.xIndex]?.time
    if (axisValue == null) return false
    mobilePinnedHoverIdx.value = coord.xIndex
    scheduleHoverOverlayUpdate(coord.xIndex)
    inst.dispatchAction({
      type: 'updateAxisPointer',
      currTrigger: 'touch',
      x: offsetX,
      y: offsetY
    })
    if (shouldSuppressEchartsTooltipForMobileCrosshairDock() || shouldShowPinnedLookupAsideLayout()) {
      try { inst.dispatchAction({ type: 'hideTip' }) } catch (_) {}
    } else {
      inst.dispatchAction({
        type: 'showTip',
        seriesIndex: 0,
        dataIndex: coord.xIndex,
        position: [offsetX, offsetY]
      })
    }
    return true
  } catch (_) {
    return false
  }
}

function clearMobileCrosshair(inst) {
  try {
    if (!inst) return
    if (klineCrosshairLookupEnabled.value && applyPinnedMobileTooltip(inst)) return
    inst.dispatchAction({ type: 'hideTip' })
  } catch (_) {}
}

function syncChartInteractionBindings() {
  const inst = chartInstance && !isChartDisposed(chartInstance) ? chartInstance : ensureChartInstance()
  if (!inst) return
  const dom = inst.getDom?.()
  if (dom && chartTooltipCloseClickListener) {
    dom.removeEventListener('click', chartTooltipCloseClickListener)
  }
  if (dom && chartContextMenuListener) {
    dom.removeEventListener('contextmenu', chartContextMenuListener)
  }
  if (chartZrClickListener && inst.getZr?.()) {
    try { inst.getZr().off('click', chartZrClickListener) } catch (_) {}
  }
  if (chartZrMouseDownListener && inst.getZr?.()) {
    try { inst.getZr().off('mousedown', chartZrMouseDownListener) } catch (_) {}
  }
  if (chartZrMouseMoveListener && inst.getZr?.()) {
    try { inst.getZr().off('mousemove', chartZrMouseMoveListener) } catch (_) {}
  }
  if (chartZrMouseUpListener && inst.getZr?.()) {
    try { inst.getZr().off('mouseup', chartZrMouseUpListener) } catch (_) {}
  }
  if (dom && chartTouchStartListener) {
    dom.removeEventListener('touchstart', chartTouchStartListener)
  }
  if (dom && chartTouchMoveListener) {
    dom.removeEventListener('touchmove', chartTouchMoveListener)
  }
  if (dom && chartTouchEndListener) {
    dom.removeEventListener('touchend', chartTouchEndListener)
  }
  if (dom && chartTouchCancelListener) {
    dom.removeEventListener('touchcancel', chartTouchCancelListener)
  }
  if (dom && chartCrosshairTouchStartListener) {
    dom.removeEventListener('touchstart', chartCrosshairTouchStartListener)
  }
  if (dom && chartCrosshairTouchMoveListener) {
    dom.removeEventListener('touchmove', chartCrosshairTouchMoveListener)
  }
  if (dom && chartCrosshairTouchEndListener) {
    dom.removeEventListener('touchend', chartCrosshairTouchEndListener)
    dom.removeEventListener('touchcancel', chartCrosshairTouchEndListener)
  }

  chartContextMenuListener = null

  chartZrMouseDownListener = (payload) => {
    if (!isDesktopFullscreenDrawingEnabled()) return
    if (drawingMode.value) return
    const offsetX = Number(payload?.offsetX)
    const offsetY = Number(payload?.offsetY)
    if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) return
    const hit = findDrawingHit(inst, offsetX, offsetY)
    if (!hit) return
    drawingDragState.active = true
    drawingDragState.drawingId = hit.drawingId
    drawingDragState.handle = hit.handle
    drawingDragState.moved = false
    drawingDragState.pending = false
    drawingDragState.startX = 0
    drawingDragState.startY = 0
    drawingDragState.isTouch = false
    selectedDrawingId.value = hit.drawingId
  }

  chartZrMouseMoveListener = (payload) => {
    if (!drawingDragState.active) return
    const offsetX = Number(payload?.offsetX)
    const offsetY = Number(payload?.offsetY)
    if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) return
    const coord = screenPixelToDataCoord(inst, offsetX, offsetY)
    if (!coord || !Number.isFinite(coord.price)) return
    drawingDragState.moved = true
    replaceDrawingById(drawingDragState.drawingId, (drawing) => applyDrawingDrag(drawing, drawingDragState.handle, coord.xIndex, coord.price))
    renderChart()
  }

  chartZrMouseUpListener = () => {
    if (!drawingDragState.active) return
    resetDrawingDragState()
  }

  chartTouchStartListener = (event) => {
    if (!isMobileFullscreenDrawingEnabled()) return
    if (drawingMode.value) return
    if (drawingMenuVisible.value) return
    const touch = event?.touches?.[0]
    if (!touch || Number(event?.touches?.length || 0) !== 1) {
      resetDrawingLongPressState()
      return
    }
    const rect = dom?.getBoundingClientRect?.()
    const offsetX = Number(touch.clientX) - Number(rect?.left || 0)
    const offsetY = Number(touch.clientY) - Number(rect?.top || 0)
    if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) return
    const hit = findDrawingHit(inst, offsetX, offsetY)
    const canStartTouchMove = hit && isMobileMoveArmed(hit.drawingId)
    if (canStartTouchMove) {
      drawingDragState.pending = true
      drawingDragState.active = false
      drawingDragState.drawingId = hit.drawingId
      drawingDragState.handle = hit.handle
      drawingDragState.moved = false
      drawingDragState.startX = Number(touch.clientX)
      drawingDragState.startY = Number(touch.clientY)
      drawingDragState.isTouch = true
      selectedDrawingId.value = hit.drawingId
    } else {
      resetDrawingDragState()
    }
    scheduleDrawingLongPress(Number(touch.clientX), Number(touch.clientY), hit?.drawingId ?? null)
  }

  chartTouchMoveListener = (event) => {
    const touch = event?.touches?.[0]
    if (!touch || Number(event?.touches?.length || 0) !== 1) {
      resetDrawingDragState()
      resetDrawingLongPressState()
      return
    }
    const clientX = Number(touch.clientX)
    const clientY = Number(touch.clientY)
    if (drawingDragState.pending && drawingDragState.isTouch) {
      const moved = Math.hypot(
        clientX - Number(drawingDragState.startX || 0),
        clientY - Number(drawingDragState.startY || 0),
      )
      if (moved > 14) {
        drawingDragState.active = true
        drawingDragState.pending = false
        drawingDragState.moved = true
        triggerDrawingHapticFeedback()
        resetDrawingLongPressState()
      }
    }
    if (drawingDragState.active && drawingDragState.isTouch) {
      const rect = dom?.getBoundingClientRect?.()
      const offsetX = clientX - Number(rect?.left || 0)
      const offsetY = clientY - Number(rect?.top || 0)
      if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) return
      const coord = screenPixelToDataCoord(inst, offsetX, offsetY)
      if (!coord || !Number.isFinite(coord.price)) return
      drawingDragState.moved = true
      try { event.preventDefault() } catch (_) {}
      replaceDrawingById(drawingDragState.drawingId, (drawing) => applyDrawingDrag(drawing, drawingDragState.handle, coord.xIndex, coord.price))
      renderChart()
      return
    }
    if (!drawingLongPressState.active) return
    drawingLongPressState.clientX = clientX
    drawingLongPressState.clientY = clientY
    const moved = Math.hypot(
      drawingLongPressState.clientX - Number(drawingLongPressState.startX || 0),
      drawingLongPressState.clientY - Number(drawingLongPressState.startY || 0),
    )
    if (moved > 14) resetDrawingLongPressState()
  }

  chartTouchEndListener = () => {
    const shouldSuppress = drawingLongPressState.triggered || drawingDragState.moved
    const shouldKeepSuppress = drawingLongPressState.triggered
    const movedDrawingId = drawingDragState.moved ? drawingDragState.drawingId : null
    resetDrawingDragState()
    resetDrawingLongPressState()
    if (movedDrawingId != null && mobileMoveDrawingId.value === movedDrawingId) mobileMoveDrawingId.value = null
    if (shouldSuppress) drawingLongPressState.suppressClick = true
    if (!shouldKeepSuppress) return
  }

  chartTouchCancelListener = () => {
    resetDrawingDragState()
    resetDrawingLongPressState()
  }

  chartCrosshairTouchStartListener = (event) => {
    const touch = event?.touches?.[0]
    if (!touch || Number(event?.touches?.length || 0) !== 1) return
    mobileMaLegendSwipeTouch = null
    if (qgMaLegendStripLayout.active && clientYInMaLegendStrip(Number(touch.clientY), dom)) {
      mobileMaLegendSwipeTouch = {
        sx: Number(touch.clientX),
        sy: Number(touch.clientY),
      }
    }
    if (!klineCrosshairLookupEnabled.value) return
    mobileChartFingerDown = true
    suppressMobileTooltipDuringChartPanRef.value = false
    mobileCrosshairTouchAnchor = { x: Number(touch.clientX), y: Number(touch.clientY) }
    syncMobileCrosshairFromTouch(inst, dom, touch)
  }

  chartCrosshairTouchMoveListener = (event) => {
    const touch = event?.touches?.[0]
    if (!touch || Number(event?.touches?.length || 0) !== 1) {
      mobileChartFingerDown = false
      suppressMobileTooltipDuringChartPanRef.value = false
      mobileCrosshairTouchAnchor = null
      mobileMaLegendSwipeTouch = null
      clearMobileCrosshair(inst)
      return
    }
    if (mobileMaLegendSwipeTouch && qgMaLegendStripLayout.active) {
      const sdx = Number(touch.clientX) - mobileMaLegendSwipeTouch.sx
      const sdy = Number(touch.clientY) - mobileMaLegendSwipeTouch.sy
      if (Math.abs(sdx) > 20 && Math.abs(sdx) > Math.abs(sdy) * 1.25) {
        suppressMobileTooltipDuringChartPanRef.value = true
        try {
          event.preventDefault()
        } catch (_) {}
        try {
          inst.dispatchAction({ type: 'hideTip' })
        } catch (_) {}
        return
      }
    }
    if (!klineCrosshairLookupEnabled.value) return
    const anchor = mobileCrosshairTouchAnchor
    if (anchor && !suppressMobileTooltipDuringChartPanRef.value) {
      const dx = Number(touch.clientX) - anchor.x
      const dy = Number(touch.clientY) - anchor.y
      // Horizontal drag before first dataZoom tick still triggers tooltip spam — suppress early.
      if (Math.abs(dx) > 18 && Math.abs(dx) > Math.abs(dy) * 1.2) {
        suppressMobileTooltipDuringChartPanRef.value = true
        try {
          inst.dispatchAction({ type: 'hideTip' })
        } catch (_) {}
      }
    }
    if (suppressMobileTooltipDuringChartPanRef.value) {
      return
    }
    const handled = syncMobileCrosshairFromTouch(inst, dom, touch)
    if (handled) {
      try { event.preventDefault() } catch (_) {}
    }
  }

  chartCrosshairTouchEndListener = (event) => {
    try {
      const touch = event?.changedTouches?.[0]
      if (touch && mobileMaLegendSwipeTouch && qgMaLegendStripLayout.active) {
        const dx = Number(touch.clientX) - mobileMaLegendSwipeTouch.sx
        const dy = Number(touch.clientY) - mobileMaLegendSwipeTouch.sy
        if (Math.abs(dx) >= 34 && Math.abs(dx) > Math.abs(dy) * 1.15) {
          dispatchMaLegendHorizontalScroll(inst, dx)
        }
      }
    } catch (_) {}
    mobileMaLegendSwipeTouch = null
    const hadPanSuppress = suppressMobileTooltipDuringChartPanRef.value
    mobileChartFingerDown = false
    suppressMobileTooltipDuringChartPanRef.value = false
    mobileCrosshairTouchAnchor = null
    if (klineCrosshairLookupEnabled.value) {
      if (hadPanSuppress) {
        mobilePinnedHoverIdx.value = null
      }
      clearMobileCrosshair(inst)
    }
  }

  chartZrClickListener = (payload) => {
    if (!isAnyFullscreenDrawingEnabled()) return
    if (drawingLongPressState.suppressClick) {
      drawingLongPressState.suppressClick = false
      return
    }
    const offsetX = Number(payload?.offsetX)
    const offsetY = Number(payload?.offsetY)
    if (!Number.isFinite(offsetX) || !Number.isFinite(offsetY)) return
    if (!drawingMode.value) {
      if (drawingDragState.moved) return
      const hit = findDrawingHit(inst, offsetX, offsetY)
      selectDrawing(hit?.drawingId ?? null)
      return
    }
    resetDrawingLongPressState()
    try {
      if (!inst.containPixel({ gridIndex: 0 }, [offsetX, offsetY])) return
    } catch (_) {
      return
    }
    const coord = screenPixelToDataCoord(inst, offsetX, offsetY)
    if (!coord || !Number.isFinite(coord.price)) return
    const xIndex = coord.xIndex
    const price = coord.price

    if (drawingMode.value === 'horizontal') {
      const id = nextDrawingId++
      updateDrawings((list) => [...list, {
        id,
        type: 'horizontal',
        price,
      }])
      selectedDrawingId.value = id
      cancelDrawingMode()
      renderChart()
      return
    }

    if (drawingMode.value === 'vertical') {
      const id = nextDrawingId++
      updateDrawings((list) => [...list, { id, type: 'vertical', xIndex }])
      selectedDrawingId.value = id
      cancelDrawingMode()
      renderChart()
      return
    }

    if (drawingMode.value === 'trend') {
      if (!drawingDraftStart.value) {
        drawingDraftStart.value = { xIndex, price }
        return
      }
      const id = nextDrawingId++
      updateDrawings((list) => [...list, {
        id,
        type: 'trend',
        startIndex: drawingDraftStart.value.xIndex,
        startPrice: drawingDraftStart.value.price,
        endIndex: xIndex,
        endPrice: price,
      }])
      selectedDrawingId.value = id
      cancelDrawingMode()
      renderChart()
      return
    }

    if (drawingMode.value === 'rect') {
      if (!drawingDraftStart.value) {
        drawingDraftStart.value = { xIndex, price }
        return
      }
      const id = nextDrawingId++
      updateDrawings((list) => [...list, {
        id,
        type: 'rect',
        startIndex: drawingDraftStart.value.xIndex,
        startPrice: drawingDraftStart.value.price,
        endIndex: xIndex,
        endPrice: price,
      }])
      selectedDrawingId.value = id
      cancelDrawingMode()
      renderChart()
      return
    }

    if (drawingMode.value === 'channel') {
      if (!drawingDraftStart.value) {
        drawingDraftStart.value = { xIndex, price }
        return
      }
      if (drawingDraftStart.value.endIndex === undefined) {
        drawingDraftStart.value = {
          ...drawingDraftStart.value,
          endIndex: xIndex,
          endPrice: price,
        }
        return
      }
      const dx = xIndex - Number(drawingDraftStart.value.xIndex)
      const slope = dx === 0 ? 0 : (Number(drawingDraftStart.value.endPrice) - Number(drawingDraftStart.value.price)) / dx
      const basePriceAtX = Number(drawingDraftStart.value.price) + slope * (xIndex - Number(drawingDraftStart.value.xIndex))
      const id = nextDrawingId++
      updateDrawings((list) => [...list, {
        id,
        type: 'channel',
        startIndex: drawingDraftStart.value.xIndex,
        startPrice: drawingDraftStart.value.price,
        endIndex: drawingDraftStart.value.endIndex,
        endPrice: drawingDraftStart.value.endPrice,
        offsetPrice: price - basePriceAtX,
      }])
      selectedDrawingId.value = id
      cancelDrawingMode()
      renderChart()
    }
  }

  if (chartZrClickListener && inst.getZr?.()) {
    try { inst.getZr().on('click', chartZrClickListener) } catch (_) {}
  }
  if (chartZrMouseDownListener && inst.getZr?.()) {
    try { inst.getZr().on('mousedown', chartZrMouseDownListener) } catch (_) {}
  }
  if (chartZrMouseMoveListener && inst.getZr?.()) {
    try { inst.getZr().on('mousemove', chartZrMouseMoveListener) } catch (_) {}
  }
  if (chartZrMouseUpListener && inst.getZr?.()) {
    try { inst.getZr().on('mouseup', chartZrMouseUpListener) } catch (_) {}
  }
  if (dom && chartTouchStartListener) {
    dom.addEventListener('touchstart', chartTouchStartListener, { passive: true })
  }
  if (dom && chartTouchMoveListener) {
    dom.addEventListener('touchmove', chartTouchMoveListener, { passive: false })
  }
  if (dom && chartTouchEndListener) {
    dom.addEventListener('touchend', chartTouchEndListener, { passive: true })
  }
  if (dom && chartTouchCancelListener) {
    dom.addEventListener('touchcancel', chartTouchCancelListener, { passive: true })
  }
  if (dom && chartCrosshairTouchStartListener) {
    dom.addEventListener('touchstart', chartCrosshairTouchStartListener, { passive: true })
  }
  if (dom && chartCrosshairTouchMoveListener) {
    dom.addEventListener('touchmove', chartCrosshairTouchMoveListener, { passive: false })
  }
  if (dom && chartCrosshairTouchEndListener) {
    dom.addEventListener('touchend', chartCrosshairTouchEndListener, { passive: true })
    dom.addEventListener('touchcancel', chartCrosshairTouchEndListener, { passive: true })
  }
  chartTooltipCloseClickListener = (event) => {
    const closeBtn = event?.target?.closest?.('.mobile-chart-tooltip-close')
    if (!closeBtn) return
    mobileTooltipDismissed.value = true
    try {
      inst.dispatchAction({ type: 'hideTip' })
    } catch (_) {}
    try {
      event.preventDefault()
      event.stopPropagation()
    } catch (_) {}
  }
  if (dom && chartTooltipCloseClickListener) {
    dom.addEventListener('click', chartTooltipCloseClickListener)
  }
}

function handleGlobalPointerDown(event) {
  if (!drawingMenuVisible.value) return
  const target = event?.target
  if (target?.closest?.('.chart-drawing-context-menu')) return
  closeDrawingMenu()
}

function scheduleHoverOverlayUpdate(idx) {
  pendingHoverIdx = idx
  if (hoverUpdateRafId != null) return
  hoverUpdateRafId = requestAnimationFrame(() => {
    hoverUpdateRafId = null
    const i = pendingHoverIdx
    pendingHoverIdx = null
    if (!chartInstance || typeof i !== 'number') return
    if (!buildLegendTextUpdatesAtFn || !updateMaLegendStateAtFn || !buildLegendsAtFn) return
    try {
      const textUpdates = buildLegendTextUpdatesAtFn(i)
      updateMaLegendStateAtFn(i)
      const legendsAt = buildLegendsAtFn(i)
      const legends = Array.isArray(legendsAt) ? legendsAt : []
      const maLegend = legends.filter(
        (l) =>
          l.id === 'legend-ma-main' ||
          l.id === 'legend-ma-main-r1' ||
          l.id === 'legend-ma-main-r2'
      )
      const patch = {}
      if (Array.isArray(textUpdates) && textUpdates.length) patch.graphic = textUpdates
      if (maLegend.length) patch.legend = maLegend
      if (patch.graphic || patch.legend) {
        // Force legend to re-render; otherwise formatter text may stay stale.
        chartInstance.setOption(patch, { notMerge: false, lazyUpdate: true, silent: true, replaceMerge: ['legend'] })
      }
    } catch (_) {}
  })
}

function usePinnedMobileTooltip() {
  return isMobileViewport() || (isFullscreen.value && useMobileKlineDropdown.value)
}

/** 手機查價改由主圖上方 Vue dock 呈現；勿用 suppressMobileTooltipDuringChartPanRef 關閉 dock，否則會退回 ECharts tooltip（y=24）與均線雙列圖例重疊。 */
function shouldShowPinnedLookupAsideLayout() {
  const len = Array.isArray(chartData.value) ? chartData.value.length : 0
  return !!(
    len > 0 &&
    usePinnedMobileTooltip() &&
    !drawingMode.value &&
    !mobileTooltipDismissed.value &&
    mobilePinnedHoverIdx.value != null &&
    klineCrosshairLookupEnabled.value
  )
}

/** 手機／全螢幕窄 UI：查價線模式只使用 Vue dock，停用 ECharts 內建 tooltip（避免雙列 HTML 卡與上方空白）。 */
function shouldSuppressEchartsTooltipForMobileCrosshairDock() {
  return !!(
    usePinnedMobileTooltip() &&
    klineCrosshairLookupEnabled.value &&
    !drawingMode.value
  )
}

function patchChartCrosshairOption(inst) {
  if (!inst || isChartDisposed(inst)) return
  const drawing = drawingMode.value
  const crossOn = !drawing && klineCrosshairLookupEnabled.value
  const pinnedTip = usePinnedMobileTooltip()
  const suppressDockTip = shouldSuppressEchartsTooltipForMobileCrosshairDock()
  try {
    inst.setOption({
      tooltip: {
        show: !drawing && !suppressDockTip,
        alwaysShowContent:
          pinnedTip &&
          !suppressDockTip &&
          !shouldShowPinnedLookupAsideLayout() &&
          !mobileTooltipDismissed.value &&
          mobilePinnedHoverIdx.value != null &&
          klineCrosshairLookupEnabled.value,
        axisPointer: {
          type: 'cross',
          show: crossOn,
        },
      },
      axisPointer: {
        show: crossOn,
        link: [{ xAxisIndex: 'all' }],
      },
    }, false, true)
  } catch (_) {}
}

function toggleKlineCrosshairLookup() {
  const next = !klineCrosshairLookupEnabled.value
  klineCrosshairLookupEnabled.value = next
  const inst = chartInstance && !isChartDisposed(chartInstance) ? chartInstance : null
  if (!next) {
    mobilePinnedHoverIdx.value = null
    mobileTooltipDismissed.value = false
    try {
      inst?.dispatchAction({ type: 'hideTip' })
    } catch (_) {}
  }
  patchChartCrosshairOption(inst)
  try {
    let iw = Number(window.innerWidth) || 0
    const vw = Number(window.visualViewport?.width)
    const w = Number.isFinite(vw) && vw > 0 ? Math.min(iw, vw) : iw
    if (w > 0 && w <= 768 && chartData.value?.length && showHMA.value) {
      renderChart()
    }
  } catch (_) {}
}

function dismissMobilePinnedLookupAside() {
  mobileTooltipDismissed.value = true
  const inst = chartInstance && !isChartDisposed(chartInstance) ? chartInstance : null
  try {
    inst?.dispatchAction?.({ type: 'hideTip' })
  } catch (_) {}
  patchChartCrosshairOption(inst)
}

function zoomPinnedLookupAsideIn() {
  if (loading.value || drawingMode.value || !chartData.value.length) return
  decrementKCount()
}

function zoomPinnedLookupAsideOut() {
  if (loading.value || drawingMode.value || !chartData.value.length) return
  incrementKCount()
}

function toggleCrosshairLookupNavCollapsed() {
  crosshairLookupNavCollapsed.value = !crosshairLookupNavCollapsed.value
  nextTick(() => {
    clampCrosshairLookupNavPositionToBounds()
  })
}

const crosshairLookupNavStyle = computed(() => {
  if (!Number.isFinite(crosshairLookupNavPosition.x) || !Number.isFinite(crosshairLookupNavPosition.y)) {
    return undefined
  }
  return {
    left: `${Math.round(crosshairLookupNavPosition.x)}px`,
    top: `${Math.round(crosshairLookupNavPosition.y)}px`,
    right: 'auto',
    bottom: 'auto',
  }
})

function clampCrosshairLookupNavPoint(x, y) {
  const container = chartContainer.value
  const navEl = crosshairLookupNavEl.value
  if (!container || !navEl) return null
  const maxX = Math.max(0, container.clientWidth - navEl.offsetWidth)
  const maxY = Math.max(0, container.clientHeight - navEl.offsetHeight)
  return {
    x: Math.min(Math.max(0, x), maxX),
    y: Math.min(Math.max(0, y), maxY),
  }
}

function resolveCrosshairLookupNavOrigin() {
  const container = chartContainer.value
  const navEl = crosshairLookupNavEl.value
  if (!container || !navEl) return null
  const containerRect = container.getBoundingClientRect?.()
  const navRect = navEl.getBoundingClientRect?.()
  if (!containerRect || !navRect) return null
  const x = navRect.left - containerRect.left
  const y = navRect.top - containerRect.top
  return clampCrosshairLookupNavPoint(x, y)
}

function clampCrosshairLookupNavPositionToBounds() {
  if (!Number.isFinite(crosshairLookupNavPosition.x) || !Number.isFinite(crosshairLookupNavPosition.y)) return
  const next = clampCrosshairLookupNavPoint(crosshairLookupNavPosition.x, crosshairLookupNavPosition.y)
  if (!next) return
  crosshairLookupNavPosition.x = next.x
  crosshairLookupNavPosition.y = next.y
}

function stopCrosshairLookupNavDrag() {
  crosshairLookupNavDrag.active = false
  crosshairLookupNavDrag.pointerId = null
}

function onCrosshairLookupNavPointerMove(event) {
  if (!crosshairLookupNavDrag.active) return
  if (crosshairLookupNavDrag.pointerId != null && event.pointerId !== crosshairLookupNavDrag.pointerId) return
  const dx = Number(event.clientX) - crosshairLookupNavDrag.startClientX
  const dy = Number(event.clientY) - crosshairLookupNavDrag.startClientY
  if (!crosshairLookupNavDrag.moved && (Math.abs(dx) > 4 || Math.abs(dy) > 4)) {
    crosshairLookupNavDrag.moved = true
  }
  const next = clampCrosshairLookupNavPoint(crosshairLookupNavDrag.originX + dx, crosshairLookupNavDrag.originY + dy)
  if (!next) return
  crosshairLookupNavPosition.x = next.x
  crosshairLookupNavPosition.y = next.y
  if (crosshairLookupNavDrag.moved) {
    crosshairLookupNavSuppressClickUntil = Date.now() + 250
  }
}

function onCrosshairLookupNavPointerUp(event) {
  if (!crosshairLookupNavDrag.active) return
  if (crosshairLookupNavDrag.pointerId != null && event.pointerId !== crosshairLookupNavDrag.pointerId) return
  stopCrosshairLookupNavDrag()
}

function onCrosshairLookupNavPointerDown(event) {
  if (event.button != null && event.button !== 0) return
  const origin = resolveCrosshairLookupNavOrigin()
  if (!origin) return
  crosshairLookupNavPosition.x = origin.x
  crosshairLookupNavPosition.y = origin.y
  crosshairLookupNavDrag.active = true
  crosshairLookupNavDrag.pointerId = event.pointerId ?? null
  crosshairLookupNavDrag.startClientX = Number(event.clientX) || 0
  crosshairLookupNavDrag.startClientY = Number(event.clientY) || 0
  crosshairLookupNavDrag.originX = origin.x
  crosshairLookupNavDrag.originY = origin.y
  crosshairLookupNavDrag.moved = false
}

function onCrosshairLookupNavClickCapture(event) {
  if (Date.now() <= crosshairLookupNavSuppressClickUntil) {
    event.preventDefault()
    event.stopPropagation()
  }
}

function clampHoverIndex(idx) {
  const len = Array.isArray(chartData.value) ? chartData.value.length : 0
  if (!len) return null
  const n = Number(idx)
  if (!Number.isFinite(n)) return Math.max(0, len - 1)
  return Math.min(len - 1, Math.max(0, Math.round(n)))
}

function applyPinnedMobileTooltip(inst, rawIdx = null) {
  try {
    if (!inst || !usePinnedMobileTooltip()) return false
    if (!klineCrosshairLookupEnabled.value) return false
    if (mobileTooltipDismissed.value) return false
    const sourceIdx = rawIdx != null ? rawIdx : mobilePinnedHoverIdx.value
    if (sourceIdx == null) return false
    const idx = clampHoverIndex(sourceIdx)
    if (idx == null) return false
    mobilePinnedHoverIdx.value = idx
    scheduleHoverOverlayUpdate(idx)
    if (shouldSuppressEchartsTooltipForMobileCrosshairDock() || shouldShowPinnedLookupAsideLayout()) {
      try { inst.dispatchAction({ type: 'hideTip' }) } catch (_) {}
      return true
    }
    inst.dispatchAction({
      type: 'showTip',
      seriesIndex: 0,
      dataIndex: idx,
    })
    return true
  } catch (_) {
    return false
  }
}

/** 查價線模式下：對齊十字線／tooltip 至指定 K 線索引（供右下快捷鈕） */
function syncCrosshairLookupToIndex(rawIdx) {
  const inst = chartInstance && !isChartDisposed(chartInstance) ? chartInstance : null
  const len = Array.isArray(chartData.value) ? chartData.value.length : 0
  if (!inst || len === 0 || loading.value || drawingMode.value || !klineCrosshairLookupEnabled.value) return
  const idx = clampHoverIndex(rawIdx)
  if (idx == null) return
  mobileTooltipDismissed.value = false
  mobilePinnedHoverIdx.value = idx
  const row = chartData.value[idx]
  const yRef = Number(row?.close ?? row?.open ?? row?.high ?? row?.low ?? 0)
  const cursorTrig = usePinnedMobileTooltip() ? 'touch' : 'mousemove'
  try {
    const pt = inst.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [idx, yRef])
    if (Array.isArray(pt) && pt.length >= 2 && Number.isFinite(pt[0]) && Number.isFinite(pt[1])) {
      inst.dispatchAction({
        type: 'updateAxisPointer',
        currTrigger: cursorTrig,
        x: pt[0],
        y: pt[1],
      })
    }
  } catch (_) {}
  try {
    if (shouldSuppressEchartsTooltipForMobileCrosshairDock() || shouldShowPinnedLookupAsideLayout()) {
      inst.dispatchAction({ type: 'hideTip' })
    } else {
      inst.dispatchAction({
        type: 'showTip',
        seriesIndex: 0,
        dataIndex: idx,
      })
    }
  } catch (_) {}
  scheduleHoverOverlayUpdate(idx)
}

function crosshairLookupResolvedIdx() {
  const len = Array.isArray(chartData.value) ? chartData.value.length : 0
  if (!len) return null
  let cur = mobilePinnedHoverIdx.value
  if (cur == null || !Number.isFinite(cur)) cur = len - 1
  return clampHoverIndex(cur)
}

function stepCrosshairLookup(delta) {
  const len = chartData.value.length
  if (!len || loading.value || drawingMode.value || !klineCrosshairLookupEnabled.value) return
  const cur = crosshairLookupResolvedIdx()
  if (cur == null) return
  syncCrosshairLookupToIndex(cur + delta)
}

/** 將 dataZoom 視窗對齊最新一根 K（維持目前根數設定），不寫入偏好／伺服器 */
function snapDataZoomToTrailingWindow() {
  const inst = chartInstance && !isChartDisposed(chartInstance) ? chartInstance : null
  const len = Array.isArray(chartData.value) ? chartData.value.length : 0
  if (!inst || len === 0) return
  let count = Number(desiredKCount.value)
  if (!Number.isFinite(count) || count <= 0) {
    count = Math.min(len, 120)
  }
  count = Math.max(1, Math.min(len, Math.floor(count)))
  const endIdx = len - 1
  const startIdx = Math.max(0, endIdx - (count - 1))
  let idxList = []
  try {
    const dzArr = inst.getOption()?.dataZoom
    if (Array.isArray(dzArr) && dzArr.length) {
      idxList = dzArr.map((_, i) => i)
    }
  } catch (_) {}
  if (!idxList.length) {
    idxList = dataZoomIndexes.length ? [...dataZoomIndexes] : [0, 1]
  }
  for (const idx of idxList) {
    try {
      inst.dispatchAction({
        type: 'dataZoom',
        dataZoomIndex: idx,
        startValue: startIdx,
        endValue: endIdx
      })
    } catch (_) {}
  }
  try {
    updateVisibleKCountFromOption()
  } catch (_) {}
}

function jumpCrosshairLookupLatest() {
  const len = chartData.value.length
  if (!len || loading.value || drawingMode.value || !klineCrosshairLookupEnabled.value) return
  const narrowMobile =
    typeof window !== 'undefined' &&
    window.matchMedia &&
    window.matchMedia('(max-width: 768px)').matches
  if (narrowMobile) {
    snapDataZoomToTrailingWindow()
    requestAnimationFrame(() => {
      syncCrosshairLookupToIndex(len - 1)
    })
    return
  }
  syncCrosshairLookupToIndex(len - 1)
}

function crosshairLookupCanStep(delta) {
  const len = chartData.value.length
  if (!len || loading.value || drawingMode.value || !klineCrosshairLookupEnabled.value) return false
  const cur = crosshairLookupResolvedIdx()
  if (cur == null) return false
  const next = cur + delta
  return next >= 0 && next <= len - 1 && next !== cur
}

function crosshairLookupCanJumpLatest() {
  const len = chartData.value.length
  if (!len || loading.value || drawingMode.value || !klineCrosshairLookupEnabled.value) return false
  const cur = crosshairLookupResolvedIdx()
  return cur != null && cur !== len - 1
}

let vpvrUpdateRafId = null
let pendingVpvrUpdate = null

function scheduleVpvrUpdate(vpvrNext) {
  pendingVpvrUpdate = vpvrNext
  if (vpvrUpdateRafId != null) return
  vpvrUpdateRafId = requestAnimationFrame(() => {
    vpvrUpdateRafId = null
    const next = pendingVpvrUpdate
    pendingVpvrUpdate = null
    if (!chartInstance || !next) return
    try {
      chartInstance.setOption({
        series: [{ id: 'series-vpvr', data: next }]
      }, false, true)
    } catch (_) {}
  })
}

const hasSubplots = computed(() => [showHA.value, showKD.value, showRSI.value, showCCI.value, showMACD.value, showHMAInd.value, showGoldenWave.value, showVolume.value].some(Boolean))

const storedPanelWeights = (() => {
  try {
    const raw = localStorage.getItem('chartPanelWeights')
    const parsed = raw ? JSON.parse(raw) : null
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch (_) {
    return {}
  }
})()

const panelWeights = ref(storedPanelWeights)
const panelLayout = ref({ ids: [], heightsPx: [], splitterTopsPx: [], availableForGrids: 0, minsPx: [] })

const splitDragging = ref(false)
const activeSplitterIndex = ref(-1)
let splitStartClientY = 0
let splitStartHeights = []

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
}

function normalizePanelWeights(ids) {
  const next = {}
  for (const id of ids) {
    const w = Number(panelWeights.value?.[id])
    next[id] = Number.isFinite(w) && w > 0 ? w : 0
  }
  let sum = Object.values(next).reduce((a, b) => a + b, 0)
  if (!(sum > 0)) {
    for (const id of ids) next[id] = 1
    sum = ids.length
  }
  for (const id of ids) next[id] = next[id] / sum
  panelWeights.value = next
  try { localStorage.setItem('chartPanelWeights', JSON.stringify(next)) } catch (_) {}
  return next
}

function ensurePanelWeights(ids) {
  const current = panelWeights.value && typeof panelWeights.value === 'object' ? { ...panelWeights.value } : {}
  const next = {}
  let hasAny = false
  let sumExisting = 0
  let countExisting = 0
  for (const id of ids) {
    const w = Number(current[id])
    if (Number.isFinite(w) && w > 0) {
      next[id] = w
      hasAny = true
      sumExisting += w
      countExisting += 1
    }
  }
  if (!hasAny) {
    // default: main slightly larger, others equal
    if (ids.includes('main')) {
      next.main = 0.55
      const rest = ids.filter(i => i !== 'main')
      const each = rest.length ? 0.45 / rest.length : 0
      for (const id of rest) next[id] = each
    } else {
      const each = ids.length ? 1 / ids.length : 0
      for (const id of ids) next[id] = each
    }
  } else {
    // Fill newly enabled panels with an average share so they don't get 0 height
    const avg = countExisting > 0 ? (sumExisting / countExisting) : 1
    for (const id of ids) {
      if (!(id in next)) {
        next[id] = avg
      }
    }
  }
  panelWeights.value = next
  return normalizePanelWeights(ids)
}

function computeHeightsFromWeights(ids, weights01, availableForGrids, minsPx) {
  const base = ids.map(id => Number(weights01?.[id]) || 0)
  const sum = base.reduce((a, b) => a + b, 0) || 1
  let heights = base.map(w => (w / sum) * availableForGrids)

  // enforce mins with redistribution
  const fixed = new Array(ids.length).fill(false)
  for (let iter = 0; iter < ids.length + 2; iter++) {
    let changed = false
    for (let i = 0; i < ids.length; i++) {
      if (fixed[i]) continue
      const minH = minsPx[i] || 0
      if (heights[i] < minH) {
        heights[i] = minH
        fixed[i] = true
        changed = true
      }
    }
    if (!changed) break
    const used = heights.reduce((a, b) => a + b, 0)
    const remaining = Math.max(0, availableForGrids - used)
    const flexIdx = fixed.map((f, i) => (!f ? i : -1)).filter(i => i >= 0)
    if (!flexIdx.length) break
    const flexW = flexIdx.reduce((a, i) => a + (base[i] || 1), 0) || flexIdx.length
    for (const i of flexIdx) {
      const w = base[i] || 1
      heights[i] = heights[i] + (remaining * w) / flexW
    }
  }
  const total = heights.reduce((a, b) => a + b, 0)
  if (total > 0 && Math.abs(total - availableForGrids) > 1) {
    const scale = availableForGrids / total
    heights = heights.map(h => h * scale)
  }
  return heights
}

function onSplitterPointerDown(index, e) {
  if (!isFullscreen.value) return
  if (!panelLayout.value?.splitterTopsPx?.length) return
  if (index < 0 || index >= panelLayout.value.splitterTopsPx.length) return
  e.preventDefault()
  splitDragging.value = true
  activeSplitterIndex.value = index
  splitStartClientY = e.clientY
  splitStartHeights = Array.isArray(panelLayout.value.heightsPx) ? [...panelLayout.value.heightsPx] : []
  try { (e.currentTarget)?.setPointerCapture?.(e.pointerId) } catch (_) {}
}

function onSplitterPointerMove(e) {
  if (!splitDragging.value) return
  const idx = activeSplitterIndex.value
  if (idx < 0) return
  const meta = panelLayout.value
  const ids = Array.isArray(meta?.ids) ? meta.ids : []
  const available = Number(meta?.availableForGrids) || 0
  const mins = Array.isArray(meta?.minsPx) ? meta.minsPx : []
  if (!ids.length || !(available > 0) || splitStartHeights.length !== ids.length) return

  const dy = e.clientY - splitStartClientY
  const totalPair = (splitStartHeights[idx] || 0) + (splitStartHeights[idx + 1] || 0)
  const minA = mins[idx] || 0
  const minB = mins[idx + 1] || 0
  const maxA = Math.max(minA, totalPair - minB)
  let newA = clamp((splitStartHeights[idx] || 0) + dy, minA, maxA)
  let newB = totalPair - newA

  const nextHeights = [...splitStartHeights]
  nextHeights[idx] = newA
  nextHeights[idx + 1] = newB

  const nextWeights = {}
  for (let i = 0; i < ids.length; i++) {
    nextWeights[ids[i]] = nextHeights[i] / available
  }
  panelWeights.value = nextWeights
  normalizePanelWeights(ids)

  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}

function onSplitterPointerUp(e) {
  if (!splitDragging.value) return
  splitDragging.value = false
  activeSplitterIndex.value = -1
  try { (e.currentTarget)?.releasePointerCapture?.(e.pointerId) } catch (_) {}
}

 let resizeRafId = null
/** Last innerWidth when we ran a full resize+render; used to still run on orientation / real layout changes while skipping keyboard noise. */
let lastHandledInnerWidth = 0

function handleWindowResize() {
  try {
    updateIsMobileUi()
  } catch (_) {}
  try {
    updateFullscreenKeyListener()
  } catch (_) {}
  const iw = typeof window !== 'undefined' ? Number(window.innerWidth) || 0 : 0
  const widthJump = lastHandledInnerWidth > 0 && Math.abs(iw - lastHandledInnerWidth) >= 8
  const firstPass = lastHandledInnerWidth === 0
  // 手機鍵盤開合只改高度；在任何畫布尺寸同步之前返回，保留目前畫面。
  if (!widthJump && shouldSuppressChartLayoutForSearchUi()) {
    console.warn('[StockChart debug] handleWindowResize suppressed', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      innerWidth: iw,
      lastHandledInnerWidth,
      widthJump,
      firstPass,
    })
    return
  }
  const layoutReady = (() => {
    try {
      return syncFullscreenChartViewportLayout('window-resize')
    } catch (_) {
      return false
    }
  })()
  if (!chartInstance) {
    console.warn('[StockChart debug] handleWindowResize skipped: no chartInstance', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      innerWidth: typeof window !== 'undefined' ? window.innerWidth : null,
      innerHeight: typeof window !== 'undefined' ? window.innerHeight : null,
      layoutReady,
    })
    if (layoutReady && !loading.value && chartData.value.length > 0) {
      requestAnimationFrame(() => {
        if (!loading.value && chartData.value.length > 0) {
          try { renderChart() } catch (_) {}
        }
      })
    }
    return
  }

  if (resizeRafId != null) {
    cancelAnimationFrame(resizeRafId)
  }
  resizeRafId = requestAnimationFrame(() => {
    resizeRafId = null
    console.log('[StockChart debug] handleWindowResize RAF running', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      innerWidth: typeof window !== 'undefined' ? window.innerWidth : null,
      innerHeight: typeof window !== 'undefined' ? window.innerHeight : null,
    })
    try {
      syncFullscreenChartViewportLayout('window-resize-raf')
      chartInstance.resize()
      lastHandledInnerWidth = typeof window !== 'undefined' ? Number(window.innerWidth) || 0 : lastHandledInnerWidth
    } catch (_) {}
    if (!loading.value && chartData.value.length > 0) {
      setTimeout(() => {
        if (!loading.value && chartData.value.length > 0) {
          try { renderChart() } catch (_) {}
        }
      }, 120)
    }
  })
}

function handleEscKey(e) {
  const targetTag = String(e?.target?.tagName || '').toLowerCase()
  const isTypingTarget = targetTag === 'input' || targetTag === 'textarea' || e?.target?.isContentEditable
  if (!isTypingTarget && (e?.key === 'Delete' || e?.key === 'Backspace') && isDesktopFullscreenDrawingEnabled() && selectedDrawingId.value != null) {
    e.preventDefault()
    deleteDrawingById(selectedDrawingId.value)
    return
  }
  // 全螢幕時優先「一次 Escape 回到主畫面」：先前若先 match 繪圖／側欄會 return，導致需按第二次才能 toggleFullscreen
  if (e?.key === 'Escape' && isFullscreen.value && !isTypingTarget) {
    e.preventDefault()
    try {
      if (mobileToolbarSearchOpen.value) closeMobileToolbarSearch()
    } catch (_) {}
    try {
      if (fsToolsMenuOpen.value) fsToolsMenuOpen.value = false
    } catch (_) {}
    try {
      if (klineModeMenuOpen.value) klineModeMenuOpen.value = false
    } catch (_) {}
    try {
      if (aiModalOpen.value) aiModalOpen.value = false
    } catch (_) {}
    if (drawingMenuVisible.value || drawingMode.value) {
      resetDrawingUi()
    }
    if (controlPanelOpen.value) {
      controlPanelOpen.value = false
      try {
        applySheetStage('half')
      } catch (_) {}
      persistControlPanelOpenState()
    }
    toggleFullscreen()
    return
  }
  if (e?.key === 'Escape' && (drawingMenuVisible.value || drawingMode.value)) {
    resetDrawingUi()
    return
  }
  if (e?.key === 'Escape' && controlPanelOpen.value) {
    toggleControlPanel()
    return
  }
}

// chart mode: 'standard' | 'heikin'
const chartMode = ref('standard')

// Indicator toggles
// 主K線顯示開關（預設顯示，localStorage 設為 'false' 才隱藏）
const showMainK = ref(localStorage.getItem('chartShowMainK') !== 'false')
const showVolume = ref(localStorage.getItem('chartShowVolume') !== 'false')
const showKD = ref(localStorage.getItem('chartShowKD') === 'true')
const showMACD = ref(localStorage.getItem('chartShowMACD') === 'true')
// RSI 副圖顯示開關
const showRSI = ref(localStorage.getItem('chartShowRSI') === 'true')
// 四狀態動能 / 行家指標已移除
const showM4Momentum = ref(false)
const showCCI = ref(localStorage.getItem('chartShowCCI') === 'true')
// 布林通道（主圖疊加）顯示開關
const showBB = ref(localStorage.getItem('chartShowBB') === 'true')
const showExpert = ref(false)
const showVPVR = ref(localStorage.getItem('chartShowVPVR') === 'true')
const showFib = ref(localStorage.getItem('chartShowFib') === 'true')
const showDiagSR = ref(localStorage.getItem('chartShowDiagSR') === 'true')
const diagSrParams = reactive({
  lookback: Number(localStorage.getItem('diagSrLookback') ?? '5') || 5,
  maxSegments: Number(localStorage.getItem('diagSrMaxSegments') ?? '3') || 3,
  windowSize: Number(localStorage.getItem('diagSrWindowSize') ?? '120') || 120,
  minSpan: Number(localStorage.getItem('diagSrMinSpan') ?? '5') || 5
})

const orcStyle = reactive({
  barWidthPct: Number(localStorage.getItem('chartOrcBarWidthPct') ?? '60') || 60,
  colorNeg: localStorage.getItem('chartOrcColorNeg') || '#22c55e',
  colorMid: localStorage.getItem('chartOrcColorMid') || '#f59e0b',
  colorPos: localStorage.getItem('chartOrcColorPos') || '#ef4444',
})

function saveOrcStyle() {
  localStorage.setItem('chartOrcBarWidthPct', String(orcStyle.barWidthPct))
  localStorage.setItem('chartOrcColorNeg', String(orcStyle.colorNeg || ''))
  localStorage.setItem('chartOrcColorMid', String(orcStyle.colorMid || ''))
  localStorage.setItem('chartOrcColorPos', String(orcStyle.colorPos || ''))
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}

const DEFAULT_DIAG_SR = Object.freeze({
  lookback: 5,
  maxSegments: 3,
  windowSize: 120,
  minSpan: 5
})

const aiModalOpen = ref(false)
const aiLoading = ref(false)
const aiError = ref('')
const aiText = ref('')
const aiUsage = ref(null)
const aiSnapshot = ref(null)
const aiVisibleStartIdx = ref(0)
const aiVisibleEndIdx = ref(0)
// HMA 指標開關
const showHMA = ref(localStorage.getItem('chartShowHMA') === 'true')
const showHMAInd = ref(localStorage.getItem('chartShowHMAInd') === 'true')
const showReversal = ref(localStorage.getItem('chartShowReversal') === 'true')
// 轉折線上下漲獨立開關（預設皆開啟）
const showReversalUp = ref(localStorage.getItem('chartShowReversalUp') !== 'false')
const showReversalDown = ref(localStorage.getItem('chartShowReversalDown') !== 'false')
// Tower (寶塔線) sub-chart toggle
const showHA = ref(false)

// 切換為原始K線（上方按鈕）
function setStandardMode() {
  showReversal.value = false
  showMainK.value = true
  localStorage.setItem('chartShowMainK', showMainK.value.toString())
  localStorage.setItem('chartShowReversal', showReversal.value.toString())
  chartMode.value = 'standard'
  if (chartData.value.length > 0) {
    renderChart()
  }
}

// 切換為神奇K線（上方按鈕）
function setHeikinMode() {
  if (!canUseHeikinLadder.value) {
    chartMode.value = 'standard'
    alertUpgrade('Pro')
    return
  }
  showReversal.value = false
  showMainK.value = true
  localStorage.setItem('chartShowMainK', showMainK.value.toString())
  localStorage.setItem('chartShowReversal', showReversal.value.toString())
  chartMode.value = 'heikin'
  if (chartData.value.length > 0) {
    renderChart()
  }
}

// 從上方按鈕切換轉折線
function toggleReversalFromToolbar() {
  if (!canUseHeikinLadder.value) {
    showReversal.value = false
    localStorage.setItem('chartShowReversal', 'false')
    localStorage.setItem('chartShowMainK', showMainK.value.toString())
    alertUpgrade('Pro')
    return
  }
  const prev = showReversal.value
  showReversal.value = !showReversal.value
  localStorage.setItem('chartShowReversal', showReversal.value.toString())

  if (!prev && showReversal.value) {
    // 只看轉折線時隱藏主K線，並自動開啟多空線
    showMainK.value = false
    showHMA.value = true
    localStorage.setItem('chartShowHMA', 'true')
  } else if (prev && !showReversal.value) {
    // 關閉轉折線時恢復主K線（多空線保持使用者原本狀態）
    showMainK.value = true
  }
  localStorage.setItem('chartShowMainK', showMainK.value.toString())
  if (chartData.value.length > 0) {
    renderChart()
  }
}

// 從控制面板切換轉折線總開關（共用 toolbar 邏輯）
function toggleReversalFromPanel() {
  toggleReversalFromToolbar()
}

/** K 線模式下拉選單：原始 / 神奇 / 階梯（與原本工具列按鈕同一套邏輯） */
const klineToolbarMode = computed({
  get() {
    if (showReversal.value) return 'reversal'
    if (chartMode.value === 'heikin') return 'heikin'
    return 'standard'
  },
  set(v) {
    if (v === 'standard') {
      setStandardMode()
    } else if (v === 'heikin') {
      setHeikinMode()
    } else if (v === 'reversal') {
      if (!showReversal.value) toggleReversalFromToolbar()
    }
  },
})

const klineModeMenuOpen = ref(false)
let klineModeMenuDocTid = null

function closeKlineModeMenu() {
  klineModeMenuOpen.value = false
}

function toggleKlineModeMenu() {
  klineModeMenuOpen.value = !klineModeMenuOpen.value
}

function onKlineModeMenuDocPointerDown(e) {
  if (!klineModeMenuOpen.value) return
  const t = e?.target
  if (typeof t?.closest === 'function' && t.closest('.kline-mode-icon-dropdown')) return
  closeKlineModeMenu()
}

function pickKlineToolbarMode(mode) {
  if (mode === 'standard' || mode === 'heikin' || mode === 'reversal') {
    klineToolbarMode.value = mode
  }
  closeKlineModeMenu()
}

const klineModeTriggerIconClass = computed(() => {
  if (showReversal.value) return 'fas fa-wave-square'
  if (chartMode.value === 'heikin') return 'fas fa-magic'
  return 'fas fa-chart-bar'
})

watch(klineModeMenuOpen, (_open) => {
  if (klineModeMenuDocTid != null) {
    try {
      clearTimeout(klineModeMenuDocTid)
    } catch (_) {}
    klineModeMenuDocTid = null
  }
  try {
    document.removeEventListener('pointerdown', onKlineModeMenuDocPointerDown, true)
  } catch (_) {}
  if (typeof document === 'undefined') return
  if (open) {
    klineModeMenuDocTid = setTimeout(() => {
      klineModeMenuDocTid = null
      document.addEventListener('pointerdown', onKlineModeMenuDocPointerDown, true)
    }, 32)
  }
})

// 儲存轉折線上漲/下跌顯示偏好
function saveReversalVisibility() {
  localStorage.setItem('chartShowReversalUp', showReversalUp.value.toString())
  localStorage.setItem('chartShowReversalDown', showReversalDown.value.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}

// Fullscreen state
const isFullscreen = ref(false)
const nativeFullscreenActive = ref(false)
/** 全螢幕工具列可橫向捲動時，對齊左側讓根數／週期 chip 先出現在可視區 */
const frequencyControlsRef = ref(null)

function scrollFrequencyControlsToStart() {
  const el = frequencyControlsRef.value
  if (!el || typeof el.scrollLeft !== 'number') return
  el.scrollLeft = 0
}

watch(isFullscreen, (value) => {
  if (!props.fsSuppressHostSync) {
    emit('fullscreen-change', !!value)
  }
  applyChartDataFromRaw()
  if (value) {
    resetRenderRetryState()
    updateUseMobileKlineDropdown()
    if (useMobileKlineDropdown.value) {
      mobileFsToolbarCollapsed.value = false
      controlPanelOpen.value = false
      persistControlPanelOpenState()
    }
    nextTick(() => {
      requestAnimationFrame(() => {
        scrollFrequencyControlsToStart()
        try { syncFullscreenChartViewportLayout('watch-isFullscreen-enter') } catch (_) {}
        try { safeResize() } catch (_) {}
        if (!loading.value && chartData.value.length > 0) {
          try { renderChart() } catch (_) {}
        }
      })
    })
  } else {
    resetRenderRetryState()
    clearForcedFullscreenChartLayout()
    closeMobileToolbarSearch()
    mobileFsToolbarCollapsed.value = false
    if (!loading.value && chartData.value.length > 0) {
      nextTick(() => {
        try { renderChart() } catch (_) {}
      })
    }
  }
})

function isTwiiChartSymbol(sym) {
  try {
    let s = String(sym || '').trim().toUpperCase().replace(/\.(TW|TWO)$/i, '')
    const base = (s.split('.')[0] || s).trim()
    return base === '^TWII' || base === 'TWII'
  } catch (_) {
    return false
  }
}

function twiiOhlcFieldEqual(a, b) {
  const va = Number(a)
  const vb = Number(b)
  if (!Number.isFinite(va) || !Number.isFinite(vb)) return false
  return Math.abs(va - vb) < 0.005
}

/** 末根 OHLC 與前一根完全相同時剔除（假日重複報價等） */
function dropDuplicateLastBarIfSameOhlc(rows) {
  const list = Array.isArray(rows) ? rows.slice() : []
  if (list.length < 2) return list
  const last = list[list.length - 1]
  const prev = list[list.length - 2]
  const sameBar = ['open', 'high', 'low', 'close'].every((key) => twiiOhlcFieldEqual(last?.[key], prev?.[key]))
  return sameBar ? list.slice(0, -1) : list
}

function sanitizeChartRows(rows, symbol, { fullscreen = false } = {}) {
  let list = Array.isArray(rows) ? rows.slice() : []
  if (isTwiiChartSymbol(symbol) || fullscreen) {
    list = dropDuplicateLastBarIfSameOhlc(list)
  }
  return list
}

function applyChartDataFromRaw() {
  chartData.value = sanitizeChartRows(rawChartData.value, props.symbol, { fullscreen: isFullscreen.value })
}

const visibleKCount = ref(0)
let storedVisiblePref = parseInt(localStorage.getItem('chartDesiredKCount') || '120', 10)
if (!Number.isFinite(storedVisiblePref) || storedVisiblePref <= 0) {
  storedVisiblePref = 120
}
if (storedVisiblePref < 120) {
  storedVisiblePref = 120
  localStorage.setItem('chartDesiredKCount', storedVisiblePref.toString())
}
const desiredKPref = ref(storedVisiblePref)
const desiredKCount = ref(desiredKPref.value)

function computeVisibleCount(startIndex, endIndex, total) {
  const len = Math.max(0, total || 0)
  if (len === 0) return 0
  const s = Math.max(0, Math.min(len - 1, Math.floor(Number(startIndex) || 0)))
  const e = Math.max(0, Math.min(len - 1, Math.floor(Number(endIndex) || 0)))
  return Math.max(0, e - s + 1)
}

function updateVisibleKCountFromOption() {
  try {
    if (!chartInstance) { visibleKCount.value = chartData.value.length; return }
    const opt = chartInstance.getOption ? chartInstance.getOption() : null
    const dzList = Array.isArray(opt?.dataZoom) ? opt.dataZoom : []
    const dz = dzList.find(d => d.type === 'slider') || dzList[0]
    const len = chartData.value.length
    let startVal = dz?.startValue
    let endVal = dz?.endValue
    if (typeof startVal !== 'number' || typeof endVal !== 'number') {
      const st = typeof dz?.start === 'number' ? dz.start : 0
      const en = typeof dz?.end === 'number' ? dz.end : 100
      startVal = Math.round((st / 100) * Math.max(0, len - 1))
      endVal = Math.round((en / 100) * Math.max(0, len - 1))
    }
    visibleKCount.value = computeVisibleCount(startVal, endVal, len)
  } catch (_) {
    visibleKCount.value = chartData.value.length
  }
}

function applyDesiredKCount() {
  const len = chartData.value.length
  if (!chartInstance || len === 0) {
    visibleKCount.value = len
    desiredKCount.value = len
    return
  }
  let count = Number(desiredKCount.value)
  if (!Number.isFinite(count) || count <= 0) {
    count = len
  }
  count = Math.max(1, Math.min(len, Math.floor(count)))
  const endIdx = len - 1
  const startIdx = Math.max(0, endIdx - (count - 1))
  desiredKPref.value = count
  desiredKCount.value = count
  localStorage.setItem('chartDesiredKCount', count.toString())
  scheduleSyncChartSettingsToServer()

  const indexes = dataZoomIndexes.length ? dataZoomIndexes : [0]
  for (const idx of indexes) {
    try {
      chartInstance.dispatchAction({
        type: 'dataZoom',
        dataZoomIndex: idx,
        startValue: startIdx,
        endValue: endIdx
      })
    } catch (_) {}
  }
}

function decrementKCount() {
  const current = Number(desiredKCount.value) || 120
  const step = current > 100 ? 20 : 10
  desiredKCount.value = Math.max(20, current - step)
  applyDesiredKCount()
}

function incrementKCount() {
  const current = Number(desiredKCount.value) || 120
  const step = current >= 100 ? 20 : 10
  desiredKCount.value = Math.min(chartData.value.length || 999, current + step)
  applyDesiredKCount()
}

// KD Parameters
const kdParams = ref({
  period: parseInt(localStorage.getItem('chartKDPeriod') || '9'),
  k: parseInt(localStorage.getItem('chartKDK') || '3'),
  d: parseInt(localStorage.getItem('chartKDD') || '3')
})
// KD midline (50) toggle
const kdMidline = ref(localStorage.getItem('chartKDMidline') === 'true')
// KD K 線 / D 線顯示開關
const showKLine = ref(localStorage.getItem('chartShowKLine') !== 'false')
const showDLine = ref(localStorage.getItem('chartShowDLine') !== 'false')
const kdBold = ref(localStorage.getItem('chartKDBold') === 'true')

// MACD Parameters
const macdParams = ref({
  fast: parseInt(localStorage.getItem('chartMACDFast') || '12'),
  slow: parseInt(localStorage.getItem('chartMACDSlow') || '26'),
  signal: parseInt(localStorage.getItem('chartMACDSignal') || '9')
})
const parseMacdDisplaySelection = (storedValue) => {
  const raw = String(storedValue || '').trim().toLowerCase()
  if (!raw || raw === 'all') return ['dif', 'macd', 'osc']
  const values = raw.split(',').map(v => v.trim()).filter(v => ['dif', 'macd', 'osc'].includes(v))
  return values.length ? Array.from(new Set(values)) : ['dif', 'macd', 'osc']
}
const hasMacdDisplay = (key) => Array.isArray(macdDisplayMode.value) && macdDisplayMode.value.includes(key)
const toggleMacdDisplay = (key) => {
  const current = Array.isArray(macdDisplayMode.value) ? [...macdDisplayMode.value] : ['dif', 'macd', 'osc']
  if (current.includes(key)) {
    if (current.length === 1) return
    macdDisplayMode.value = current.filter(item => item !== key)
  } else {
    macdDisplayMode.value = [...current, key]
  }
  saveMACDParams()
}
const macdDisplayMode = ref(parseMacdDisplaySelection(localStorage.getItem('chartMACDDisplay') || 'all'))
const macdLineWidths = ref({
  dif: parseFloat(localStorage.getItem('chartMACDDifWidth') || '2'),
  macd: parseFloat(localStorage.getItem('chartMACDSignalWidth') || '2')
})
const macdHistHeight = ref(parseFloat(localStorage.getItem('chartMACDHistHeight') || '0.7'))
const macdOscStyle = ref({
  barWidth: parseFloat(localStorage.getItem('chartMACDHistBarWidth') || '60'),
  colorUp: localStorage.getItem('chartMACDHistColorUp') || '#ff5050',
  colorDown: localStorage.getItem('chartMACDHistColorDown') || '#10b981',
  opacityUp: parseFloat(localStorage.getItem('chartMACDHistOpacityUp') || '1'),
  opacityDown: parseFloat(localStorage.getItem('chartMACDHistOpacityDown') || '1'),
})

function hexToRgba(hex, alpha = 1) {
  const raw = String(hex || '').trim().replace('#', '')
  const normalized = raw.length === 3
    ? raw.split('').map((ch) => ch + ch).join('')
    : raw
  if (!/^[0-9a-fA-F]{6}$/.test(normalized)) {
    return `rgba(255, 255, 255, ${alpha})`
  }
  const r = parseInt(normalized.slice(0, 2), 16)
  const g = parseInt(normalized.slice(2, 4), 16)
  const b = parseInt(normalized.slice(4, 6), 16)
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

// 4-state Momentum / Expert：已從 UI 移除且預設不啟用，但保留參數避免舊引用造成前端崩潰
const m4MomentumParams = ref({
  fast: parseInt(localStorage.getItem('chartM4Fast') || '12'),
  slow: parseInt(localStorage.getItem('chartM4Slow') || '26'),
  signal: parseInt(localStorage.getItem('chartM4Signal') || '9'),
  minChange: parseFloat(localStorage.getItem('chartM4MinChange') || '0'),
  useZeroFilter: localStorage.getItem('chartM4UseZeroFilter') !== 'false',
  autoSqueeze: localStorage.getItem('chartM4AutoSqueeze') !== 'false',
  squeezeTh: parseFloat(localStorage.getItem('chartM4SqueezeTh') || '0.2'),
  autoTrend: localStorage.getItem('chartM4AutoTrend') !== 'false',
  trendTh: parseFloat(localStorage.getItem('chartM4TrendTh') || '0.6'),
  colorUp: localStorage.getItem('chartM4ColorUp') || '#ef4444',
  colorDown: localStorage.getItem('chartM4ColorDown') || '#22c55e',
  colorRange: localStorage.getItem('chartM4ColorRange') || '#1d4ed8',
  colorSqueeze: localStorage.getItem('chartM4ColorSqueeze') || '#38bdf8',
  bandOpacity: parseFloat(localStorage.getItem('chartM4BandOpacity') || '0.40')
})

const expertParams = ref({
  length: parseInt(localStorage.getItem('chartExpertLength') || '13')
})

const cciParams = ref({
  period: (() => {
    try {
      const suffix = getCurrentTfSuffix()
      const raw = localStorage.getItem(`chartCCIPeriod${suffix}`) || localStorage.getItem('chartCCIPeriod') || '100'
      const p = Math.floor(Number(raw))
      return Number.isFinite(p) && p > 0 ? p : 100
    } catch (_) {
      return 100
    }
  })()
})

// RSI Parameters（副圖）
const rsiParams = ref({
  period: (() => {
    try {
      const suffix = getCurrentTfSuffix()
      const raw = localStorage.getItem(`chartRSIPeriod${suffix}`) || localStorage.getItem('chartRSIPeriod') || '14'
      const p = Math.floor(Number(raw))
      return Number.isFinite(p) && p > 0 ? p : 14
    } catch (_) {
      return 14
    }
  })(),
  overbought: (() => {
    try {
      const raw = localStorage.getItem('chartRSIOverbought') || '70'
      const v = Math.floor(Number(raw))
      return Number.isFinite(v) && v > 0 && v <= 100 ? v : 70
    } catch (_) {
      return 70
    }
  })(),
  oversold: (() => {
    try {
      const raw = localStorage.getItem('chartRSIOversold') || '30'
      const v = Math.floor(Number(raw))
      return Number.isFinite(v) && v >= 0 && v < 100 ? v : 30
    } catch (_) {
      return 30
    }
  })(),
})

// Bollinger Bands Parameters（主圖疊加）
const BB_DEFAULT_COLORS = {
  colorUpper: '#ef4444',
  colorMid: '#2563eb',
  colorLower: '#22c55e',
}

function loadBbColor(key, fallback) {
  try {
    const suffix = getCurrentTfSuffix()
    const raw = localStorage.getItem(`${key}${suffix}`) || localStorage.getItem(key) || fallback
    const s = String(raw || '').trim()
    return /^#[0-9a-fA-F]{6}$/.test(s) ? s : fallback
  } catch (_) {
    return fallback
  }
}

function getBbLineColors() {
  return {
    upper: bbParams?.value?.colorUpper || BB_DEFAULT_COLORS.colorUpper,
    mid: bbParams?.value?.colorMid || BB_DEFAULT_COLORS.colorMid,
    lower: bbParams?.value?.colorLower || BB_DEFAULT_COLORS.colorLower,
  }
}

/** 布林圖例 rich 樣式：上／中／下與線色同步（hover 更新時也即時讀取） */
function getBbRich(fontSize = 12) {
  const { upper, mid, lower } = getBbLineColors()
  const markSize = Math.max(fontSize + 2, 14)
  return {
    bbLabel: { color: 'rgba(226,232,240,0.88)', fill: 'rgba(226,232,240,0.88)', fontWeight: 600, fontSize },
    bbUpper: { color: upper, fill: upper, fontWeight: 700, fontSize },
    bbMid: { color: mid, fill: mid, fontWeight: 700, fontSize },
    bbLower: { color: lower, fill: lower, fontWeight: 700, fontSize },
    bbUpperMark: { color: upper, fill: upper, fontWeight: 900, fontSize: markSize },
    bbMidMark: { color: mid, fill: mid, fontWeight: 900, fontSize: markSize },
    bbLowerMark: { color: lower, fill: lower, fontWeight: 900, fontSize: markSize },
  }
}

const bbParams = ref({
  period: (() => {
    try {
      const suffix = getCurrentTfSuffix()
      const raw = localStorage.getItem(`chartBBPeriod${suffix}`) || localStorage.getItem('chartBBPeriod') || '20'
      const p = Math.floor(Number(raw))
      return Number.isFinite(p) && p > 1 ? p : 20
    } catch (_) {
      return 20
    }
  })(),
  mult: (() => {
    try {
      const suffix = getCurrentTfSuffix()
      const raw = localStorage.getItem(`chartBBMult${suffix}`) || localStorage.getItem('chartBBMult') || '2'
      const m = Number(raw)
      return Number.isFinite(m) && m > 0 ? m : 2
    } catch (_) {
      return 2
    }
  })(),
  colorUpper: loadBbColor('chartBBColorUpper', BB_DEFAULT_COLORS.colorUpper),
  colorMid: loadBbColor('chartBBColorMid', BB_DEFAULT_COLORS.colorMid),
  colorLower: loadBbColor('chartBBColorLower', BB_DEFAULT_COLORS.colorLower),
})

// 黃金波段指標 Toggle and Parameters
const showGoldenWave = ref(localStorage.getItem('chartShowGoldenWave') === 'true')

function enforceProChartAccess() {
  if (canUseProTech.value) return
  const disableToggle = (toggleRef, storageKey) => {
    if (!toggleRef.value) return
    toggleRef.value = false
    if (storageKey) {
      try { localStorage.setItem(storageKey, 'false') } catch (_) {}
    }
  }
  if (chartMode.value === 'heikin') chartMode.value = 'standard'
  disableToggle(showReversal, 'chartShowReversal')
  disableToggle(showHMA, 'chartShowHMA')
  disableToggle(showHMAInd, 'chartShowHMAInd')
  disableToggle(showGoldenWave, 'chartShowGoldenWave')
  disableToggle(showCCI, 'chartShowCCI')
  disableToggle(showVPVR, 'chartShowVPVR')
  disableToggle(showFib, 'chartShowFib')
  disableToggle(showDiagSR, 'chartShowDiagSR')
}

enforceProChartAccess()

watch(canUseProTech, (allowed) => {
  if (allowed) return
  enforceProChartAccess()
  if (chartData.value?.length > 0 && chartInstance) {
    renderChart()
  }
})

/** Max simultaneous sub-charts: HA, KD, RSI, CCI, MACD, HMAInd, GoldenWave, Volume */
const MAX_SUB_CHARTS = 5
const subplotToggleOrder = Object.freeze([
  { ref: showHA, storageKey: 'chartShowHA' },
  { ref: showKD, storageKey: 'chartShowKD' },
  { ref: showRSI, storageKey: 'chartShowRSI' },
  { ref: showCCI, storageKey: 'chartShowCCI' },
  { ref: showMACD, storageKey: 'chartShowMACD' },
  { ref: showHMAInd, storageKey: 'chartShowHMAInd' },
  { ref: showGoldenWave, storageKey: 'chartShowGoldenWave' },
  { ref: showVolume, storageKey: 'chartShowVolume' },
])

function getSubplotToggleCount() {
  return subplotToggleOrder.reduce((n, { ref }) => n + (ref.value ? 1 : 0), 0)
}

let subplotLimitGuard = false

function persistSubplotStorageKey(storageKey, val) {
  try {
    localStorage.setItem(storageKey, String(Boolean(val)))
  } catch (_) {}
}

function trimSubplotsExcessFromEndSilent() {
  while (getSubplotToggleCount() > MAX_SUB_CHARTS) {
    let turnedOff = false
    for (let i = subplotToggleOrder.length - 1; i >= 0; i--) {
      const { ref, storageKey } = subplotToggleOrder[i]
      if (ref.value) {
        ref.value = false
        persistSubplotStorageKey(storageKey, false)
        turnedOff = true
        break
      }
    }
    if (!turnedOff) break
  }
}

function alertSubplotLimit() {
  const msg = '\u6700\u591a\u540c\u6642\u958b\u555f 5 \u500b\u526f\u5716\uff0c\u5df2\u81ea\u52d5\u95dc\u9589\u512a\u5148\u5ea6\u8f03\u4f4e\u7684\u526f\u5716\uff08\u6210\u4ea4\u91cf\u2192\u9ec3\u91d1\u6ce2\u6bb5\u2192\u591a\u7a7a\u8da8\u52e2\u2192MACD\u2192ORC/CCI\u2192RSI\u2192\u4e09\u968e\u9ede\u9663\u2192KD\u2192\u5bf6\u5854\u7dda\uff09\u3002'
  if (isFullscreen.value) {
    upgradeModalTitle.value = '\u526f\u5716\u4e0a\u9650'
    upgradeModalMessage.value = msg
    upgradeModalOpen.value = true
    return
  }
  window.alert(msg)
}

watch(
  () => subplotToggleOrder.map(({ ref }) => ref.value),
  (newVals, oldVals) => {
    if (subplotLimitGuard) return
    if (getSubplotToggleCount() <= MAX_SUB_CHARTS) return

    subplotLimitGuard = true
    try {
      const prevOn = oldVals ? oldVals.filter(Boolean).length : MAX_SUB_CHARTS
      trimSubplotsExcessFromEndSilent()
      if (oldVals && prevOn <= MAX_SUB_CHARTS) {
        alertSubplotLimit()
      }
      for (const { ref, storageKey } of subplotToggleOrder) {
        persistSubplotStorageKey(storageKey, ref.value)
      }
      scheduleSyncChartSettingsToServer()
      if (chartData.value && chartData.value.length > 0) {
        renderChart()
      }
    } finally {
      subplotLimitGuard = false
    }
  },
  { flush: 'sync' }
)

const goldenWaveParams = ref({
  fastMa: 30,
  slowMa: 100,
  fastMa2: 130,
  slowMa2: 140,
  multiMa: 18,
  waveMa2: 18,
  waveMa3: 18,
  boxPeriod: 78,
  showDifLine: true,
  showMa2Line: false,
  difLineColor: '#1e3a8a',
  ma2LineColor: '#ef4444',
  difLineWidth: 3,
  ma2LineWidth: 3,
  barUpColor: '#dc2626',
  barStopUpColor: '#22c55e',
  barDownColor: '#1144B0',
  barStopDownColor: '#06b6d4',
  barScale: 1
})

function saveGoldenWaveParams() {
  const difW = Number(goldenWaveParams.value.difLineWidth)
  const ma2W = Number(goldenWaveParams.value.ma2LineWidth)
  goldenWaveParams.value.difLineWidth = Number.isFinite(difW) ? Math.max(0.5, Math.min(10, difW)) : 3
  goldenWaveParams.value.ma2LineWidth = Number.isFinite(ma2W) ? Math.max(0.5, Math.min(10, ma2W)) : 3

  const suffix = getCurrentTfSuffix()
  const setTf = (baseKey, val) => {
    try { localStorage.setItem(`${baseKey}${suffix}`, String(val)) } catch (_) {}
  }

  setTf('chartGWFastMa', goldenWaveParams.value.fastMa)
  setTf('chartGWSlowMa', goldenWaveParams.value.slowMa)
  setTf('chartGWFastMa2', goldenWaveParams.value.fastMa2)
  setTf('chartGWSlowMa2', goldenWaveParams.value.slowMa2)
  setTf('chartGWMultiMa', goldenWaveParams.value.multiMa)
  setTf('chartGWWaveMa2', goldenWaveParams.value.waveMa2)
  setTf('chartGWWaveMa3', goldenWaveParams.value.waveMa3)
  setTf('chartGWBoxPeriod', goldenWaveParams.value.boxPeriod)
  setTf('chartGWShowDifLine', goldenWaveParams.value.showDifLine)
  setTf('chartGWShowMa2Line', goldenWaveParams.value.showMa2Line)
  setTf('chartGWDifLineColor', goldenWaveParams.value.difLineColor)
  setTf('chartGWMa2LineColor', goldenWaveParams.value.ma2LineColor)
  setTf('chartGWDifLineWidth', goldenWaveParams.value.difLineWidth)
  setTf('chartGWMa2LineWidth', goldenWaveParams.value.ma2LineWidth)
  setTf('chartGWBarUpColor', goldenWaveParams.value.barUpColor)
  setTf('chartGWBarStopUpColor', goldenWaveParams.value.barStopUpColor)
  setTf('chartGWBarDownColor', goldenWaveParams.value.barDownColor)
  setTf('chartGWBarStopDownColor', goldenWaveParams.value.barStopDownColor)
  setTf('chartGWBarScale', goldenWaveParams.value.barScale)
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}

function resetGoldenWaveParams() {
  const suffix = getCurrentTfSuffix()
  const keys = [
    'chartGWFastMa',
    'chartGWSlowMa',
    'chartGWFastMa2',
    'chartGWSlowMa2',
    'chartGWMultiMa',
    'chartGWWaveMa2',
    'chartGWWaveMa3',
    'chartGWBoxPeriod',
    'chartGWShowDifLine',
    'chartGWShowMa2Line',
    'chartGWDifLineColor',
    'chartGWMa2LineColor',
    'chartGWDifLineWidth',
    'chartGWMa2LineWidth',
    'chartGWBarUpColor',
    'chartGWBarStopUpColor',
    'chartGWBarDownColor',
    'chartGWBarStopDownColor',
    'chartGWBarScale'
  ]
  for (const k of keys) {
    try { localStorage.removeItem(`${k}${suffix}`) } catch (_) {}
  }
  Object.assign(goldenWaveParams.value, {
    fastMa: 30,
    slowMa: 100,
    fastMa2: 130,
    slowMa2: 140,
    multiMa: 18,
    waveMa2: 18,
    waveMa3: 18,
    boxPeriod: 78,
    showDifLine: true,
    showMa2Line: false,
    difLineColor: '#1e3a8a',
    ma2LineColor: '#ef4444',
    difLineWidth: 3,
    ma2LineWidth: 3,
    barUpColor: '#dc2626',
    barStopUpColor: '#22c55e',
    barDownColor: '#1144B0',
    barStopDownColor: '#06b6d4',
    barScale: 1
  })
  saveGoldenWaveParams()
}

// MA Parameters
const maParams = ref({
  ma1: parseInt(localStorage.getItem('chartMA1') || '5'),
  ma2: parseInt(localStorage.getItem('chartMA2') || '10'),
  ma3: parseInt(localStorage.getItem('chartMA3') || '20'),
  ma4: parseInt(localStorage.getItem('chartMA4') || '30'),
  ma5: parseInt(localStorage.getItem('chartMA5') || '60')
})

// MA 顯示開關
const showMA1 = ref(localStorage.getItem('chartShowMA1') !== 'false')
const showMA2 = ref(localStorage.getItem('chartShowMA2') !== 'false')
const showMA3 = ref(localStorage.getItem('chartShowMA3') !== 'false')
const showMA4 = ref(localStorage.getItem('chartShowMA4') !== 'false')
const showMA5 = ref(localStorage.getItem('chartShowMA5') !== 'false')

// Tower (寶塔線) Parameters
const towerParams = ref({
  // 翻色所需額外突破比例（例如 0.01 = 1%）
  buffer: parseFloat(localStorage.getItem('chartTowerBuffer') || '0.01')
})

const vpvrParams = ref({
  bins: parseInt(localStorage.getItem('chartVPVRBins') || '0'),
  maxWidthPct: parseFloat(localStorage.getItem('chartVPVRMaxWidthPct') || '0.62'),
  minWidthPct: parseFloat(localStorage.getItem('chartVPVRMinWidthPct') || '0.02'),
  fillColor: localStorage.getItem('chartVPVRFillColor') || '#2563eb',
  fillAlpha: parseFloat(localStorage.getItem('chartVPVRFillAlpha') || '0.30'),
  strokeColor: localStorage.getItem('chartVPVRStrokeColor') || '#60a5fa',
  strokeAlpha: parseFloat(localStorage.getItem('chartVPVRStrokeAlpha') || '0.50')
})

// HMA Parameters
const hmaParams = ref({
  period: parseInt(localStorage.getItem('chartHMAPeriod') || '77', 10)
})

const hmaIndParams = ref({
  period: parseInt(localStorage.getItem('chartHMAIndPeriod') || '24', 10),
  // 預設：上升紅、下跌綠
  upColor: localStorage.getItem('chartHMAIndUpColor') || '#ef4444',
  downColor: localStorage.getItem('chartHMAIndDownColor') || '#22c55e',
  // yAxis padding（百分比），越小越「拉開」趨勢段
  paddingPct: parseFloat(localStorage.getItem('chartHMAIndPaddingPct') || '3'),
  zeroVisible: localStorage.getItem('chartHMAIndZeroVisible') === 'true'
})

// 若曾使用舊版預設（上升藍 #3b82f6 / 下跌紅 #ef4444），同步改成新預設「上升紅、下跌綠」
if (hmaIndParams.value.upColor === '#3b82f6' && hmaIndParams.value.downColor === '#ef4444') {
  hmaIndParams.value.upColor = '#ef4444'
  hmaIndParams.value.downColor = '#22c55e'
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartHMAIndUpColor${suffix}`, hmaIndParams.value.upColor)
  localStorage.setItem(`chartHMAIndDownColor${suffix}`, hmaIndParams.value.downColor)
}

// Help popover state
const helpOpenKey = ref('') // '', 'fib', 'diagSR'
function toggleHelp(key) {
  helpOpenKey.value = helpOpenKey.value === key ? '' : key
}

// 圖表控制：單一全螢幕／一般圖與 localStorage 同步；多格（2×2／四分割）每實例獨立，不共用同一開關
const controlPanelOpen = ref(
  props.multiTileMode ? false : (localStorage.getItem('chartControlPanelOpen') === 'true')
)

function persistControlPanelOpenState() {
  if (props.multiTileMode) return
  try {
    localStorage.setItem('chartControlPanelOpen', String(controlPanelOpen.value))
  } catch (_) {}
}

const controlPanelEl = ref(null)
/** 視窗寬度 ≤768px（matchMedia）：全螢幕時工具列網格／搜尋列等版面旗標 */
const useMobileKlineDropdown = ref(false)

function updateUseMobileKlineDropdown() {
  try {
    useMobileKlineDropdown.value = !!(window.matchMedia && window.matchMedia('(max-width: 768px)').matches)
  } catch (_) {
    useMobileKlineDropdown.value = false
  }
}

/** Narrow screens: hide carousel toolbar for weighted index only (1/1 bar overlaps subplots). Mobile (≤768px): hide carousel while chart control panel is open. */
const showFullscreenCarouselControls = computed(() => {
  if (!props.carouselEnabled || !isFullscreen.value) return false
  if (isMobileMaHmaExclusiveMode() && isTwiiChartSymbol(props.symbol)) return false
  if (useMobileKlineDropdown.value && controlPanelOpen.value) return false
  return true
})

/** Mobile fullscreen (max-width 768px): toolbar collapse / expand */
const showMobileFsToolbarCollapseUi = computed(() => isFullscreen.value && useMobileKlineDropdown.value)
/** 與貼底 tooltip／觸控十字線相同的版面：顯示十字線查價開關 */
const showKlineCrosshairToggle = computed(() => isMobileViewport() || (isFullscreen.value && useMobileKlineDropdown.value))
/** 與 renderChart 內 showMainArea 一致：有主圖區塊時才顯示查價導覽（疊在主圖 K 線區，非成交量副圖） */
const showMainPlotForCrosshairNav = computed(() => {
  const reversalLines = showReversal.value && (showReversalUp.value || showReversalDown.value)
  return !!(
    showMainK.value ||
    reversalLines ||
    showHMA.value ||
    showVPVR.value ||
    showFib.value ||
    showDiagSR.value
  )
})

const showPinnedLookupAside = computed(() => shouldShowPinnedLookupAsideLayout())

function pinnedLookupAsideSymbolIsIndex(sym) {
  const s = String(sym || '').trim().toUpperCase()
  return s.startsWith('^') || s === 'TWII'
}

const pinnedLookupAsideDisplay = computed(() => {
  if (!showPinnedLookupAside.value) return null
  const idx = clampHoverIndex(mobilePinnedHoverIdx.value)
  if (idx == null) return null
  const row = chartData.value[idx]
  if (!row) return null

  const open = Number(row.open)
  const high = Number(row.high)
  const low = Number(row.low)
  const close = Number(row.close)
  let pctNum = null
  let pctStr = ''
  let deltaNum = null
  let deltaStr = ''
  if (idx > 0 && chartData.value[idx - 1]) {
    const prevClose = Number(chartData.value[idx - 1].close)
    if (Number.isFinite(prevClose) && Number.isFinite(close)) {
      deltaNum = close - prevClose
      deltaStr = `${deltaNum >= 0 ? '+' : ''}${deltaNum.toFixed(2)}`
      if (prevClose !== 0) {
        pctNum = (deltaNum / prevClose) * 100
        pctStr = `${pctNum >= 0 ? '+' : ''}${pctNum.toFixed(2)}%`
      }
    }
  }

  let volLine = ''
  const rawVolume = Number(row.volume)
  if (Number.isFinite(rawVolume)) {
    if (pinnedLookupAsideSymbolIsIndex(props.symbol)) {
      const yi = rawVolume / 1e3
      const valueText = yi >= 1e4 ? `${(yi / 1e4).toFixed(1)} 兆` : `${yi.toFixed(1)} 億`
      volLine = `金額 ${valueText}`
    } else {
      volLine = `量 ${Math.round(rawVolume / 1000).toLocaleString()} 張`
    }
  }

  return {
    dateStr: row.time != null ? String(row.time) : '',
    open: Number.isFinite(open) ? open.toFixed(2) : '--',
    high: Number.isFinite(high) ? high.toFixed(2) : '--',
    low: Number.isFinite(low) ? low.toFixed(2) : '--',
    close: Number.isFinite(close) ? close.toFixed(2) : '--',
    pctStr,
    deltaStr,
    pctUp:
      deltaNum != null && Number.isFinite(deltaNum)
        ? deltaNum >= 0
        : pctNum != null && Number.isFinite(pctNum) && pctNum >= 0,
    volLine,
  }
})

/**
 * 手機股票代號上移版面：有無查價 dock 都套用，避免標題壓到均線圖例。
 * （查價顯示時原本就靠 mobile-pinned-inline；未顯示時也要同樣上移。）
 */
const liftMobileStockTitleAboveMa = computed(() => {
  if (showPinnedLookupAside.value && pinnedLookupAsideDisplay.value) return true
  if (props.multiTileMode && !isFullscreen.value) return false
  return isMobileViewport()
})

/** 查價小卡：疊在主圖容器內（均線 scroll 列下方、主圖栅上方），排除多格非全螢幕 */
const showPinnedDockedLookupAside = computed(() => {
  if (!showPinnedLookupAside.value || !pinnedLookupAsideDisplay.value) return false
  if (props.multiTileMode && !isFullscreen.value) return false
  return true
})

function syncPinnedLookupAsideDockPosition() {
  try {
    const root = chartContainer.value
    const inst = chartInstance
    if (!root || !inst || isChartDisposed(inst)) return

    if (!showPinnedDockedLookupAside.value) {
      root.style.removeProperty('--qg-pinned-aside-top')
      root.style.removeProperty('--qg-pinned-aside-left')
      root.style.removeProperty('--qg-pinned-aside-dock-transform')
      return
    }

    const gridRect = inst.getModel()?.getComponent?.('grid', 0)?.coordinateSystem?.getRect?.()
    const gy = Number(gridRect?.y)
    if (!Number.isFinite(gy)) return

    const asidePx = PINNED_LOOKUP_ASIDE_DOCK_HEIGHT_PX
    const gapAbovePlot = 6
    const gutter = 6

    let gridLeftPx = Math.round((root.clientWidth || 0) * 0.05)
    try {
      if (Number.isFinite(Number(gridRect?.x))) {
        gridLeftPx = Math.round(Number(gridRect.x))
      }
    } catch (_) {}

    /**
     * 手機垂直堆疊：均線在上、查價 dock 在均線下方並貼近布林，
     * dock 不可再被夾回均線列。
     */
    const stackedDockTopPx = (() => {
      if (!qgMaLegendStripLayout.active) return null
      const fromLayout = Number(qgMaLegendStripLayout.dockTopPx)
      if (Number.isFinite(fromLayout) && fromLayout >= 0) {
        return Math.max(4, Math.round(fromLayout))
      }
      const legendTop = Number(qgMaLegendStripLayout.legendGraphicTop)
      if (!Number.isFinite(legendTop) || legendTop <= 0) return null
      const maStripRowStepPx = Math.round(9 + 10)
      return Math.max(4, Math.round(legendTop + 2 * maStripRowStepPx + 2))
    })()

    let topPx
    if (stackedDockTopPx != null) {
      topPx = stackedDockTopPx
      // 確保 dock 底緣仍在主圖 grid 上方
      const maxDockTop = Math.max(4, Math.round(gy - gapAbovePlot - asidePx))
      topPx = Math.min(topPx, maxDockTop)
    } else {
      let bandTop = Math.max(6, gy - 54)
      let bandBot = gy - gapAbovePlot
      topPx = bandTop + Math.max(0, (bandBot - bandTop - asidePx) / 2)
      if (!(bandBot > bandTop)) {
        topPx = Math.max(6, gy - asidePx - gapAbovePlot)
      } else if (bandBot - bandTop < asidePx) {
        topPx = Math.max(bandTop, gy - asidePx - 2)
      }
    }

    root.style.setProperty('--qg-pinned-aside-top', `${Math.round(topPx)}px`)
    // 手機均線 strip 開啟時：查價 dock 左緣對齊均線文字（與布林相同基準）
    const dockLeftPx = qgMaLegendStripLayout.active
      ? resolveMaAlignedLegendLeftPx(root.clientWidth)
      : Math.max(gutter, gridLeftPx)
    root.style.setProperty('--qg-pinned-aside-left', `${Math.round(dockLeftPx)}px`)
    root.style.setProperty('--qg-pinned-aside-dock-transform', 'none')
  } catch (_) {}
}

watch(
  showPinnedLookupAside,
  (aside) => {
    const inst = chartInstance && !isChartDisposed(chartInstance) ? chartInstance : null
    if (!inst) return
    patchChartCrosshairOption(inst)
    if (aside) {
      try { inst.dispatchAction({ type: 'hideTip' }) } catch (_) {}
    } else {
      requestAnimationFrame(() => {
        try {
          if (
            usePinnedMobileTooltip() &&
            mobilePinnedHoverIdx.value != null &&
            !mobileTooltipDismissed.value &&
            klineCrosshairLookupEnabled.value
          ) {
            applyPinnedMobileTooltip(inst)
          }
        } catch (_) {}
      })
    }
    nextTick(() =>
      requestAnimationFrame(() => syncPinnedLookupAsideDockPosition())
    )
  },
  { flush: 'post' }
)

// 此狀態會被下方 watch 的 getter 立即讀取，必須在註冊 watcher 前初始化。
// 否則全新載入圖表元件時會觸發 TDZ ReferenceError，導致 ECharts 完全無法掛載。
let maCollapsedByHma = false
const maLegendCollapsed = ref(false)

watch(
  () => ({
    docked: showPinnedDockedLookupAside.value,
    tick: pinnedLookupAsideDisplay.value?.dateStr,
    maCollapsed: maLegendCollapsed.value,
    fs: isFullscreen.value,
    mt: props.multiTileMode,
    bb: showBB.value,
  }),
  () => {
    nextTick(() =>
      requestAnimationFrame(() => syncPinnedLookupAsideDockPosition())
    )
  },
  { flush: 'post' }
)

const mobileFsToolbarCollapsed = ref(false)

watch(mobileFsToolbarCollapsed, () => {
  if (!isFullscreen.value || !useMobileKlineDropdown.value) return
  if (mobileFsToolbarCollapsed.value) {
    closeMobileToolbarSearch()
  }
  nextTick(() => {
    try {
      const el = chartContainer.value
      if (el) {
        if (el.dataset.qgFitVp === '1') {
          el.style.removeProperty('min-height')
          el.style.removeProperty('height')
          el.removeAttribute('data-qg-fit-vp')
        }
        el.style.removeProperty('--chart-container-height')
        el.style.removeProperty('--chart-container-min-height')
      }
    } catch (_) {}
    // Class + flex layout for .mobile-fs-toolbar-collapsed need a frame before clientHeight is trustworthy
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        try {
          handleWindowResize()
        } catch (_) {}
      })
    })
  })
})

/** 全螢幕：壓縮 / 匯出 / 四分割 圖示合併為單一下拉選單；選單 Teleport 至 body 避免多層 overflow 裁切 */
const fsToolsMenuOpen = ref(false)
const fsToolsDropdownRef = ref(null)
const fsToolsMenuPanelRef = ref(null)
const fsToolsMenuFixedStyle = ref({})
let fsToolsMenuDocListenerTid = null

function buildFsToolsMenuFixedStyleFromRect(rect) {
  if (!rect || typeof window === 'undefined') {
    return { display: 'none' }
  }
  const gap = 6
  return {
    position: 'fixed',
    display: 'block',
    top: `${Math.round(rect.bottom + gap)}px`,
    right: `${Math.round(window.innerWidth - rect.right)}px`,
    minWidth: '220px',
    zIndex: '50000',
  }
}

function updateFsToolsMenuLayout() {
  if (!fsToolsMenuOpen.value) {
    fsToolsMenuFixedStyle.value = { display: 'none' }
    return
  }
  const wrap = fsToolsDropdownRef.value
  if (!wrap || typeof window === 'undefined') {
    return
  }
  try {
    const r = wrap.getBoundingClientRect()
    fsToolsMenuFixedStyle.value = buildFsToolsMenuFixedStyleFromRect(r)
  } catch {
    fsToolsMenuFixedStyle.value = { display: 'none' }
  }
}

function closeFsToolsMenu() {
  fsToolsMenuOpen.value = false
  fsToolsMenuFixedStyle.value = { display: 'none' }
}

function toggleFsToolsMenu(event) {
  if (fsToolsMenuOpen.value) {
    closeFsToolsMenu()
    return
  }
  const triggerRect = event?.currentTarget?.getBoundingClientRect?.()
  if (triggerRect) {
    fsToolsMenuFixedStyle.value = buildFsToolsMenuFixedStyleFromRect(triggerRect)
  } else {
    updateFsToolsMenuLayout()
  }
  fsToolsMenuOpen.value = true
}

function onFsToolsMenuDocPointerDown(e) {
  if (!fsToolsMenuOpen.value) return
  const t = e?.target
  if (!t) return
  // ref 在 Teleport 首幀可能仍為 null，用 closest 避免誤判「點外」而秒關選單
  if (typeof t.closest === 'function') {
    if (t.closest('.fs-tools-menu--teleport')) return
    if (t.closest('.fs-tools-dropdown')) return
  }
  const wrap = fsToolsDropdownRef.value
  const panel = fsToolsMenuPanelRef.value
  if (wrap && wrap.contains(t)) return
  if (panel && panel.contains(t)) return
  closeFsToolsMenu()
}

let fsToolsMenuLayoutListenersBound = false
function onFsToolsMenuWindowRelayout() {
  updateFsToolsMenuLayout()
}

watch(fsToolsMenuOpen, (open) => {
  if (fsToolsMenuDocListenerTid != null) {
    try {
      clearTimeout(fsToolsMenuDocListenerTid)
    } catch (_) {}
    fsToolsMenuDocListenerTid = null
  }
  try {
    document.removeEventListener('pointerdown', onFsToolsMenuDocPointerDown, true)
  } catch (_) {}
  if (fsToolsMenuLayoutListenersBound) {
    try {
      window.removeEventListener('resize', onFsToolsMenuWindowRelayout)
      window.removeEventListener('scroll', onFsToolsMenuWindowRelayout, true)
    } catch (_) {}
    try {
      window.visualViewport?.removeEventListener?.('resize', onFsToolsMenuWindowRelayout)
    } catch (_) {}
    fsToolsMenuLayoutListenersBound = false
  }
  if (typeof document === 'undefined') return
  if (open) {
    nextTick(() => {
      requestAnimationFrame(() => {
        updateFsToolsMenuLayout()
        requestAnimationFrame(() => {
          updateFsToolsMenuLayout()
        })
      })
    })
    try {
      window.addEventListener('resize', onFsToolsMenuWindowRelayout)
      window.addEventListener('scroll', onFsToolsMenuWindowRelayout, true)
    } catch (_) {}
    try {
      window.visualViewport?.addEventListener?.('resize', onFsToolsMenuWindowRelayout)
    } catch (_) {}
    fsToolsMenuLayoutListenersBound = true
    // 延遲到手勢結束後再掛點外關閉，避免與觸發鈕同一次 touch/click 衝突
    fsToolsMenuDocListenerTid = setTimeout(() => {
      fsToolsMenuDocListenerTid = null
      document.addEventListener('pointerdown', onFsToolsMenuDocPointerDown, true)
    }, 32)
  }
})

watch(isFullscreen, (v) => {
  if (!v) closeFsToolsMenu()
  closeKlineModeMenu()
})

const isMobileUi = ref(false)
const panelDragging = ref(false)
const panelDragY = ref(0)
const sheetStage = ref('half')
const sheetHeightPx = ref(0)
let panelTouchStartY = 0
let panelTouchStartX = 0
let panelDragMaxY = 0
let panelTouchDy = 0
let panelPointerId = null
let panelPointerCandidate = false
let panelStartSheetHeight = 0

function updateIsMobileUi() {
  try {
    isMobileUi.value = window.matchMedia && window.matchMedia('(max-width: 640px)').matches
  } catch (_) {
    isMobileUi.value = false
  }
  updateUseMobileKlineDropdown()
}

watch(isMobileUi, (narrow) => {
  if (!narrow) closeKlineModeMenu()
})

function isMobileViewport() {
  try {
    const iw = Number(window.innerWidth) || 0
    const vw = Number(window.visualViewport?.width)
    // Some WebViews report an oversized visualViewport.width; min() avoids missing narrow layouts.
    const w = Number.isFinite(vw) && vw > 0 ? Math.min(iw, vw) : iw
    return Number.isFinite(w) ? w <= 640 : false
  } catch (_) {
    return false
  }
}

function getViewportHeightPx() {
  try {
    const h = Number(window.visualViewport?.height ?? window.innerHeight)
    return Number.isFinite(h) && h > 0 ? h : 0
  } catch (_) {
    return 0
  }
}

function getSheetHeights() {
  const vh = getViewportHeightPx()
  const collapsed = 72
  const half = vh ? Math.round(vh * 0.55) : 520
  const full = vh ? Math.round(vh * 0.92) : 780
  return {
    collapsed,
    half: Math.max(collapsed, half),
    full: Math.max(collapsed, full),
  }
}

function applySheetStage(stage) {
  sheetStage.value = stage
  const { collapsed, half, full } = getSheetHeights()
  if (stage === 'collapsed') sheetHeightPx.value = collapsed
  else if (stage === 'full') sheetHeightPx.value = full
  else sheetHeightPx.value = half
}

const controlPanelStyle = computed(() => {
  if (!isMobileViewport()) return undefined
  const { full } = getSheetHeights()
  const h = Math.max(0, Math.min(full, Number(sheetHeightPx.value) || 0))
  return {
    height: `${h || full}px`,
    transition: panelDragging.value ? 'none' : 'height 180ms ease-out'
  }
})

const controlPanelClass = computed(() => {
  if (!isMobileViewport()) return undefined
  return {
    'is-sheet': true,
    'is-collapsed': sheetStage.value === 'collapsed',
    'is-half': sheetStage.value === 'half',
    'is-full': sheetStage.value === 'full',
  }
})

/** 多格＋非全螢幕＋桌機：圖表控制掛在 chart-wrapper 內，不蓋滿整個瀏覽器視窗 */
const useMultiTileEmbeddedPanel = computed(() => {
  return !!props.multiTileMode && !isFullscreen.value && !isMobileViewport()
})

function onControlPanelPointerDown(e) {
  if (!controlPanelOpen.value) return
  if (!isMobileViewport()) return
  if (!e || e.button != null && e.button !== 0) return
  try {
    const target = e?.target
    const tag = String(target?.tagName || '').toUpperCase()
    const isFormControl = ['INPUT', 'TEXTAREA', 'SELECT', 'BUTTON', 'A'].includes(tag)
    if (isFormControl) return
    // Only allow dragging from header/drag handle area to avoid scroll conflict.
    const inHeader = !!(target && (target.closest?.('.sheet__header') || target.closest?.('.panel-drag-area') || target.closest?.('.panel-header')))
    if (!inHeader) return
  } catch (_) {}

  panelPointerCandidate = true
  panelDragging.value = false
  panelPointerId = e.pointerId
  panelTouchStartY = e.clientY
  panelTouchStartX = e.clientX
  panelDragY.value = 0
  panelStartSheetHeight = Number(sheetHeightPx.value) || getSheetHeights().half
  try { controlPanelEl.value?.setPointerCapture?.(e.pointerId) } catch (_) {}
}

function onControlPanelPointerMove(e) {
  if (!panelPointerCandidate) return
  if (!isMobileViewport()) return
  if (panelPointerId != null && e.pointerId !== panelPointerId) return
  const dyRaw = e.clientY - panelTouchStartY
  const dxRaw = e.clientX - panelTouchStartX
  if (!panelDragging.value) {
    const adx = Math.abs(dxRaw)
    const ady = Math.abs(dyRaw)
    // require vertical intent and a small movement threshold
    if (ady < 8 || ady < adx) return
    panelDragging.value = true
  }
  panelTouchDy = dyRaw
  try { e.preventDefault() } catch (_) {}
  // Height-based sheet drag: swipe up -> taller; swipe down -> shorter
  const { collapsed, full } = getSheetHeights()
  const next = Math.max(collapsed, Math.min(full, panelStartSheetHeight - dyRaw))
  sheetHeightPx.value = next
}

function onControlPanelPointerUp(e) {
  if (!panelPointerCandidate) return
  if (panelPointerId != null && e.pointerId !== panelPointerId) return

  const dyRaw = e.clientY - panelTouchStartY
  panelPointerCandidate = false
  panelDragging.value = false
  panelPointerId = null
  panelDragY.value = 0
  const { collapsed, half, full } = getSheetHeights()
  const h = Number(sheetHeightPx.value) || half
  const mid1 = (collapsed + half) / 2
  const mid2 = (half + full) / 2
  if (h < mid1) {
    applySheetStage('collapsed')
  } else if (h < mid2) {
    applySheetStage('half')
  } else {
    applySheetStage('full')
  }

  // Down-swipe from collapsed closes
  if (sheetStage.value === 'collapsed' && dyRaw > 120) {
    controlPanelOpen.value = false
    persistControlPanelOpenState()
  }
}

// Panel section collapse states
const panelSections = ref({
  ma: localStorage.getItem('chartPanelMA') === 'true',
  indicators: localStorage.getItem('chartPanelIndicators') === 'true'
})

function toggleControlPanel() {
  controlPanelOpen.value = !controlPanelOpen.value
  if (controlPanelOpen.value) warrantInfoOpen.value = false
  if (!controlPanelOpen.value) {
    applySheetStage('half')
  } else {
    try {
      if (isMobileViewport()) {
        applySheetStage('half')
      }
    } catch (_) {}
  }
  persistControlPanelOpenState()
}

function toggleWarrantInfoPanel() {
  if (!warrantFsChips.value.length) return
  warrantInfoOpen.value = !warrantInfoOpen.value
  if (warrantInfoOpen.value) {
    controlPanelOpen.value = false
    persistControlPanelOpenState()
  }
}

function closeWarrantInfoPanel() {
  warrantInfoOpen.value = false
}

watch(isFullscreen, (fs) => {
  if (!fs) warrantInfoOpen.value = false
})

watch(
  () => props.warrantInfo?.warrant_code,
  () => {
    warrantInfoOpen.value = false
  },
)

watch(() => controlPanelOpen.value, (open) => {
  if (open && showMobileToolbarSearchTrigger.value) {
    closeMobileToolbarSearch()
  }
  if (!isMobileViewport()) return
  try {
    const el = document.documentElement
    const body = document.body
    if (open) {
      applySheetStage(sheetStage.value || 'half')
      if (el) el.style.overflow = 'hidden'
      if (body) body.style.overflow = 'hidden'
    } else {
      if (el) el.style.overflow = ''
      if (body) body.style.overflow = ''
    }
  } catch (_) {}
})

watch(aiModalOpen, (open) => {
  try {
    const el = document.documentElement
    const body = document.body
    if (open) {
      controlPanelOpen.value = false
      persistControlPanelOpenState()
      if (showMobileFsToolbarCollapseUi.value) {
        mobileFsToolbarCollapsed.value = true
      }
      if (el) el.style.overflow = 'hidden'
      if (body) body.style.overflow = 'hidden'
      return
    }
    if (controlPanelOpen.value && isMobileViewport()) return
    if (el) el.style.overflow = ''
    if (body) body.style.overflow = ''
  } catch (_) {}
})

function toggleControlPanelExpanded() {
  if (!isMobileViewport()) return
  if (sheetStage.value === 'full') applySheetStage('half')
  else if (sheetStage.value === 'collapsed') applySheetStage('half')
  else applySheetStage('full')
}

function togglePanelSection(section) {
  panelSections.value[section] = !panelSections.value[section]
  localStorage.setItem(`chartPanel${section.charAt(0).toUpperCase() + section.slice(1)}`, panelSections.value[section].toString())
}

/** Per-indicator parameter section expand/collapse (default expanded; persisted in localStorage). */
const INDICATOR_DETAIL_KEYS = [
  'reversal',
  'kd',
  'rsi',
  'bb',
  'macd',
  'hmaInd',
  'goldenWave',
  'cci',
  'vpvr',
  'diagSR',
]

function loadIndicatorDetailExpanded() {
  const o = {}
  for (const k of INDICATOR_DETAIL_KEYS) {
    const raw = localStorage.getItem(`chartIndDetail_${k}`)
    o[k] = raw === null ? true : raw === 'true'
  }
  return o
}

const indicatorDetailExpanded = reactive(loadIndicatorDetailExpanded())
const indDetailTitleCollapse = '\u6536\u5408\u53c3\u6578'
const indDetailTitleExpand = '\u5c55\u958b\u53c3\u6578'

function toggleIndicatorDetail(key) {
  if (!INDICATOR_DETAIL_KEYS.includes(key)) return
  indicatorDetailExpanded[key] = !indicatorDetailExpanded[key]
  try {
    localStorage.setItem(`chartIndDetail_${key}`, String(indicatorDetailExpanded[key]))
  } catch (_) {}
}

/** 2×2／四分割格內：預設收合均線圖例，主圖較寬、較不擋 K 線 */
watch(
  () => props.multiTileMode,
  (v) => {
    if (v) maLegendCollapsed.value = true
  },
  { immediate: true }
)
function toggleMaLegendCollapsed() {
  maLegendCollapsed.value = !maLegendCollapsed.value
  localStorage.setItem('chartMALegendCollapsed', maLegendCollapsed.value.toString())
  try {
    const isFs = isFullscreen.value
    if (isFs || isMobileViewport()) {
      // 展開 MA 圖例 -> 顯示 MA、關閉多空線
      if (!maLegendCollapsed.value) {
        maHmaExclusiveSyncing = true
        showMA1.value = true
        showMA2.value = true
        showMA3.value = true
        showMA4.value = true
        showMA5.value = true
        saveMAVisibility()
        if (showHMA.value) {
          showHMA.value = false
        }
        maHmaExclusiveSyncing = false
      }
      // 收合 MA 圖例 -> 顯示多空線、關閉所有 MA
      if (maLegendCollapsed.value) {
        maHmaExclusiveSyncing = true
        showMA1.value = false
        showMA2.value = false
        showMA3.value = false
        showMA4.value = false
        showMA5.value = false
        saveMAVisibility()
        showHMA.value = true
        maHmaExclusiveSyncing = false
      }
    }
  } catch (_) {}
  if (chartData.value && chartData.value.length > 0 && chartInstance) {
    renderChart()
  }
}

function enableAllMA() {
  showMA1.value = true
  showMA2.value = true
  showMA3.value = true
  showMA4.value = true
  showMA5.value = true
  saveMAVisibility()
}

function disableAllMA() {
  showMA1.value = false
  showMA2.value = false
  showMA3.value = false
  showMA4.value = false
  showMA5.value = false
  saveMAVisibility()
}

// Toggle fullscreen
function syncFullscreenHost(el, active) {
  const host = el?.parentElement
  if (!host || host === document.body) return
  if (active) {
    if (fullscreenHostState.el !== host) {
      fullscreenHostState.el = host
      fullscreenHostState.style = host.getAttribute('style') || ''
    }
    host.style.position = 'fixed'
    host.style.top = '0'
    host.style.left = '0'
    host.style.right = '0'
    host.style.bottom = '0'
    host.style.width = '100vw'
    host.style.height = '100vh'
    host.style.minWidth = '100vw'
    host.style.minHeight = '100vh'
    host.style.display = 'flex'
    host.style.flexDirection = 'column'
    host.style.overflow = 'hidden'
    host.style.boxSizing = 'border-box'
    host.style.pointerEvents = 'auto'
    host.style.zIndex = '9998'
    host.style.background = '#0b1220'
    return
  }
  if (fullscreenHostState.el === host) {
    if (fullscreenHostState.style) {
      host.setAttribute('style', fullscreenHostState.style)
    } else {
      host.removeAttribute('style')
    }
    fullscreenHostState.el = null
    fullscreenHostState.style = ''
  }
}

function applyCssFullscreen(el) {
  syncFullscreenHost(el, true)
  el.style.position = 'fixed'
  el.style.top = '0'
  el.style.left = '0'
  el.style.right = '0'
  el.style.bottom = '0'
  el.style.width = '100vw'
  el.style.height = '100vh'
  el.style.minWidth = '100vw'
  el.style.minHeight = '100vh'
  el.style.maxWidth = '100vw'
  el.style.maxHeight = '100vh'
  el.style.display = 'flex'
  el.style.flexDirection = 'column'
  el.style.overflow = 'hidden'
  el.style.boxSizing = 'border-box'
  el.style.zIndex = '9999'
  el.style.background = '#0b1220'
  nextTick(() => {
    try { syncFullscreenChartViewportLayout('apply-css-fullscreen') } catch (_) {}
    try { window.dispatchEvent(new Event('resize')) } catch (_) {}
  })
}

function clearCssFullscreen(el) {
  syncFullscreenHost(el, false)
  resetRenderRetryState()
  clearForcedFullscreenChartLayout()
  el.style.position = ''
  el.style.top = ''
  el.style.left = ''
  el.style.right = ''
  el.style.bottom = ''
  el.style.width = ''
  el.style.height = ''
  el.style.minWidth = ''
  el.style.minHeight = ''
  el.style.maxWidth = ''
  el.style.maxHeight = ''
  el.style.display = ''
  el.style.flexDirection = ''
  el.style.overflow = ''
  el.style.boxSizing = ''
  el.style.zIndex = ''
  el.style.background = ''
}

function toggleFullscreen() {
  const el = rootEl.value
  if (!el) {
    console.error('[StockChart fullscreen] toggleFullscreen aborted: rootEl is missing', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
    })
    return
  }
  console.log('[StockChart fullscreen] toggleFullscreen called', {
    symbol: props.symbol,
    isFullscreen: isFullscreen.value,
    chartDataLength: chartData.value.length,
    rootClientWidth: el.clientWidth,
    rootClientHeight: el.clientHeight,
    rootRect: (() => {
      try {
        const rect = el.getBoundingClientRect()
        return { top: rect.top, left: rect.left, width: rect.width, height: rect.height }
      } catch (_) {
        return null
      }
    })(),
    hostStyle: (() => {
      try {
        return el.parentElement?.getAttribute?.('style') || ''
      } catch (_) {
        return ''
      }
    })(),
  })
  if (!isFullscreen.value) {
    // 1. Apply CSS fullscreen FIRST — synchronous, always works regardless of element position
    isFullscreen.value = true
    nativeFullscreenActive.value = false
    applyCssFullscreen(el)
    // 2. Trigger chart resize immediately so chart fills the new dimensions
    nextTick(() => {
      try { syncFullscreenChartViewportLayout('toggle-fullscreen-enter') } catch (_) {}
      try { safeResize() } catch (_) {}
      if (!loading.value && chartData.value.length > 0) {
        try { renderChart() } catch (_) {}
      }
      try { window.dispatchEvent(new Event('resize')) } catch (_) {}
      console.log('[StockChart fullscreen] after CSS fullscreen resize tick', {
        symbol: props.symbol,
        isFullscreen: isFullscreen.value,
        rootClientWidth: el.clientWidth,
        rootClientHeight: el.clientHeight,
      })
    })
    // 3. Attempt native fullscreen as a best-effort enhancement (hides browser chrome).
    //    Failure is intentionally ignored — CSS fullscreen is already active.
    try {
      const req = el.requestFullscreen || el.webkitRequestFullscreen || el.msRequestFullscreen
      if (req) {
        const promise = req.call(el)
        if (promise && typeof promise.then === 'function') {
          promise.catch((e) => {
            console.warn('[fullscreen] native API rejected (CSS fullscreen already active):', e?.message || e)
          })
        }
      } else {
        console.warn('[StockChart fullscreen] native requestFullscreen API unavailable', {
          symbol: props.symbol,
        })
      }
    } catch (e) {
      console.warn('[fullscreen] native API threw (CSS fullscreen already active):', e?.message || e)
    }
  } else {
    // Exit: clear state and CSS first, then exit native if it was active
    isFullscreen.value = false
    nativeFullscreenActive.value = false
    controlPanelOpen.value = false
    persistControlPanelOpenState()
    aiModalOpen.value = false
    clearCssFullscreen(el)
    try {
      const hasNativeFullscreen = !!(document.fullscreenElement || document.webkitFullscreenElement || document.msFullscreenElement)
      const canExitNativeFullscreen = document.visibilityState === 'visible' && document.hasFocus()
      if (hasNativeFullscreen && canExitNativeFullscreen) {
        if (document.exitFullscreen) document.exitFullscreen()
        else if (document.webkitExitFullscreen) document.webkitExitFullscreen()
        else if (document.msExitFullscreen) document.msExitFullscreen()
      }
    } catch (_) {}
    nextTick(() => { try { window.dispatchEvent(new Event('resize')) } catch (_) {} })
  }
}

async function enterFullscreen() {
  console.log('[StockChart fullscreen] enterFullscreen called', {
    symbol: props.symbol,
    isFullscreen: isFullscreen.value,
    chartDataLength: chartData.value.length,
  })
  if (isFullscreen.value) return
  await nextTick()
  for (let attempt = 1; attempt <= 6; attempt += 1) {
    const ready = !!rootEl.value && !!String(props.symbol || '').trim()
    console.log('[StockChart fullscreen] enterFullscreen readiness check', {
      attempt,
      ready,
      symbol: props.symbol,
      hasRootEl: !!rootEl.value,
      chartDataLength: chartData.value.length,
    })
    if (ready) {
      toggleFullscreen()
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 50))
    await nextTick()
  }
  console.error('[StockChart fullscreen] enterFullscreen aborted: chart component not ready in time', {
    symbol: props.symbol,
    hasRootEl: !!rootEl.value,
    chartDataLength: chartData.value.length,
  })
}

function onMultiTileDblClickEnterFullscreen(e) {
  if (!props.multiTileMode) return
  if (isFullscreen.value) return
  const t = e?.target
  if (t && typeof t.closest === 'function') {
    if (t.closest('.chart-drawing-context-menu, .chart-drawing-style-toolbar, .chart-drawing-hint, button, a, input, textarea, select, [contenteditable="true"]')) {
      return
    }
  }
  e?.preventDefault?.()
  void enterFullscreen()
}

async function exportChartImage() {
  try {
    const inst = ensureChartInstance()
    if (!inst || isChartDisposed(inst)) return

    const pixelRatio = 2
    const backgroundColor = '#0b1220'

    const dataUrl = inst.getDataURL({
      type: 'png',
      pixelRatio,
      backgroundColor,
    })
    if (!dataUrl || typeof dataUrl !== 'string') return

    const img = new Image()
    img.decoding = 'async'
    img.src = dataUrl
    await new Promise((resolve, reject) => {
      img.onload = () => resolve(true)
      img.onerror = (e) => reject(e)
    })

    const canvas = document.createElement('canvas')
    canvas.width = img.naturalWidth || img.width
    canvas.height = img.naturalHeight || img.height
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    // Background + chart
    ctx.fillStyle = backgroundColor
    ctx.fillRect(0, 0, canvas.width, canvas.height)
    ctx.drawImage(img, 0, 0)

    // Overlay title (HTML header is not part of ECharts canvas)
    try {
      const titleText = String(stockTitle?.value || props.symbol || '').trim()
      if (titleText) {
        const padTop = Math.round(10 * pixelRatio)
        const fontSize = Math.round(20 * pixelRatio)
        ctx.save()
        ctx.font = `700 ${fontSize}px system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', 'PingFang TC', 'Microsoft JhengHei', sans-serif`
        ctx.textAlign = 'center'
        ctx.textBaseline = 'top'
        ctx.shadowColor = 'rgba(0,0,0,0.6)'
        ctx.shadowBlur = Math.round(6 * pixelRatio)
        ctx.fillStyle = 'rgba(226, 232, 240, 0.95)'
        ctx.fillText(titleText, canvas.width / 2, padTop)
        ctx.restore()
      }
    } catch (_) {}

    // Watermark (embedded in PNG)
    try {
      const wmText = 'QuantGems®'
      const wmSize = Math.round(Math.min(canvas.width, canvas.height) * 0.10)
      ctx.save()
      ctx.translate(canvas.width / 2, canvas.height / 2)
      ctx.rotate(-Math.PI / 10)
      ctx.font = `800 ${wmSize}px system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', 'PingFang TC', 'Microsoft JhengHei', sans-serif`
      ctx.textAlign = 'center'
      ctx.textBaseline = 'middle'
      ctx.shadowColor = 'rgba(0,0,0,0.55)'
      ctx.shadowBlur = Math.round(10 * pixelRatio)
      ctx.fillStyle = 'rgba(226, 232, 240, 0.06)'
      ctx.fillText(wmText, 0, 0)
      ctx.restore()
    } catch (_) {}

    const outUrl = canvas.toDataURL('image/png')

    const safeSymbol = String(props.symbol || 'chart').toUpperCase().replace(/[^A-Z0-9._-]+/g, '_')
    const safePeriod = String(props.period || '').replace(/[^A-Z0-9._-]+/gi, '_')
    const fileName = `${safeSymbol}${safePeriod ? `_${safePeriod}` : ''}.png`

    const a = document.createElement('a')
    a.href = outUrl
    a.download = fileName
    document.body.appendChild(a)
    a.click()
    a.remove()
  } catch (e) {
    console.warn('exportChartImage error', e)
  }
}

async function requestFullscreenForPanel() {
  const el = rootEl.value || document.querySelector('.stock-chart')
  if (!el) return false
  try {
    const req = el.requestFullscreen || el.webkitRequestFullscreen || el.msRequestFullscreen
    if (!req) return false
    const ret = req.call(el)
    if (ret && typeof ret.then === 'function') {
      await ret
    }
    return true
  } catch (_) {
    return false
  }
}

async function exitFullscreenForPanel() {
  try {
    const exit = document.exitFullscreen || document.webkitExitFullscreen || document.msExitFullscreen
    if (!exit) return false
    const ret = exit.call(document)
    if (ret && typeof ret.then === 'function') {
      await ret
    }
    return true
  } catch (_) {
    return false
  }
}

defineExpose({ enterFullscreen })

function handleFullscreenKeydown(event) {
  if (!event) return
  const key = event.key
  const code = event.code
  const isEnterKey = key === 'Enter' || key === 'Return' || code === 'Enter' || code === 'NumpadEnter'
  const isLeft = key === 'ArrowLeft'
  const isRight = key === 'ArrowRight'
  if (!(isEnterKey || isLeft || isRight)) return
  const target = event.target
  if (target) {
    const tag = target.tagName
    const isSearchInput = target.classList?.contains('fullscreen-search-input')
    const isVisibleCountInput = target.closest?.('.visible-count-input') != null
    const isTextAreaOrSelect = ['TEXTAREA', 'SELECT'].includes(tag)
    const hasSearchValue = isSearchInput && typeof target.value === 'string' && target.value.trim().length > 0
    if (target.isContentEditable || isTextAreaOrSelect || hasSearchValue || isVisibleCountInput || target.closest?.('[data-stop-enter-carousel]')) {
      return
    }
  }
  if (!props.carouselEnabled) return
  if (isEnterKey) {
    event.preventDefault()
    emit('carousel-toggle')
  } else if (isLeft) {
    event.preventDefault()
    emit('carousel-prev')
  } else if (isRight) {
    event.preventDefault()
    emit('carousel-next')
  }
}

let fullscreenKeyListenerActive = false

/** Last visualViewport size to detect virtual keyboard (height shrinks, width stable). */
let lastVisualViewportSize = { w: 0, h: 0 }
let viewportResizeDebounceTimer = null
/** 送出手機股票查詢後，暫停鍵盤收合引發的重排／重繪。 */
let suppressSearchLayoutUntil = 0

function isFocusInsideChartFormControl() {
  try {
    const ae = document.activeElement
    if (!ae || !rootEl.value) return false
    if (!rootEl.value.contains(ae)) return false
    const tag = String(ae.tagName || '').toUpperCase()
    return ['INPUT', 'TEXTAREA', 'SELECT'].includes(tag)
  } catch (_) {
    return false
  }
}

/**
 * Mobile: avoid chartInstance.resize + renderChart while the OS keyboard is up or speech is
 * actively capturing. Do not treat focus on mic / tab buttons inside .fullscreen-search-box as
 * suppressing layout — that leaves stale heights in voice mode (subplots not filling the screen).
 * INPUT/textarea inside the chart is already covered by isFocusInsideChartFormControl().
 */
function shouldSuppressChartLayoutForSearchUi() {
  try {
    if (typeof window === 'undefined') return false
    if (!(isMobileMaHmaExclusiveMode() || isMobileViewport())) return false
    if (symbolSwitchingInProgress) return true
    if (Date.now() < suppressSearchLayoutUntil) return true
    if (!props.fullscreenSearchEnabled) {
      return isFocusInsideChartFormControl()
    }
    return (
      !!fsSpeechListening.value ||
      isFocusInsideChartFormControl()
    )
  } catch (_) {
    return false
  }
}

function isLikelyVirtualKeyboardResize(nextW, nextH) {
  const prev = lastVisualViewportSize
  if (!(prev.w > 0 && prev.h > 0)) return false
  if (Math.abs(nextW - prev.w) >= 2) return false
  // 鍵盤開啟與收合都只改高度；兩個方向都必須抑制重繪。
  if (Math.abs(nextH - prev.h) < 36) return false
  return (
    isFocusInsideChartFormControl() ||
    !!fsSpeechListening.value ||
    Date.now() < suppressSearchLayoutUntil
  )
}

function handleViewportChange() {
  const vv = window.visualViewport
  const vw = Number(vv?.width ?? window.innerWidth)
  const vh = Number(vv?.height ?? window.innerHeight)
  const prev = lastVisualViewportSize
  const widthChanged = prev.w > 0 && Math.abs(vw - prev.w) >= 2
  const skipHeavyResize =
    isLikelyVirtualKeyboardResize(vw, vh) || (!widthChanged && shouldSuppressChartLayoutForSearchUi())
  lastVisualViewportSize = { w: vw, h: vh }

  try { updateIsMobileUi() } catch (_) {}
  try { updateFullscreenKeyListener() } catch (_) {}
  // 必須在 syncFullscreenChartViewportLayout 前返回；該函式會改畫布尺寸，
  // 即使稍後不 render，仍會造成手機鍵盤收合時閃一下。
  if (skipHeavyResize) {
    console.warn('[StockChart debug] handleViewportChange suppressed', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      viewport: { width: vw, height: vh },
      previousViewport: prev,
      widthChanged,
    })
    return
  }
  const layoutReady = (() => {
    try {
      return syncFullscreenChartViewportLayout('viewport-change')
    } catch (_) {
      return false
    }
  })()

  if (!chartInstance) {
    console.warn('[StockChart debug] handleViewportChange skipped: no chartInstance', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      viewport: { width: vw, height: vh },
      layoutReady,
    })
    if (layoutReady && !loading.value && chartData.value.length > 0) {
      requestAnimationFrame(() => {
        if (!loading.value && chartData.value.length > 0) {
          try { renderChart() } catch (_) {}
        }
      })
    }
    return
  }
  console.log('[StockChart debug] handleViewportChange scheduling resize', {
    symbol: props.symbol,
    isFullscreen: isFullscreen.value,
    chartDataLength: chartData.value.length,
    viewport: { width: vw, height: vh },
    previousViewport: prev,
    widthChanged,
  })

  if (viewportResizeDebounceTimer != null) {
    try { clearTimeout(viewportResizeDebounceTimer) } catch (_) {}
    viewportResizeDebounceTimer = null
  }
  viewportResizeDebounceTimer = setTimeout(() => {
    viewportResizeDebounceTimer = null
    try {
      handleWindowResize()
    } catch (_) {}
  }, 80)
}

function updateFullscreenKeyListener() {
  const shouldListen = isFullscreen.value && props.carouselEnabled && showFullscreenCarouselControls.value
  if (shouldListen && !fullscreenKeyListenerActive) {
    document.addEventListener('keydown', handleFullscreenKeydown, true)
    fullscreenKeyListenerActive = true
  } else if (!shouldListen && fullscreenKeyListenerActive) {
    document.removeEventListener('keydown', handleFullscreenKeydown, true)
    fullscreenKeyListenerActive = false
  }
}

// Save KD parameters to localStorage
function saveKDParams() {
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartKDPeriod${suffix}`, kdParams.value.period.toString())
  localStorage.setItem(`chartKDK${suffix}`, kdParams.value.k.toString())
  localStorage.setItem(`chartKDD${suffix}`, kdParams.value.d.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

// Save MACD parameters to localStorage
function saveMACDParams() {
  const suffix = getCurrentTfSuffix()
  const difWidth = Number(macdLineWidths.value.dif)
  const signalWidth = Number(macdLineWidths.value.macd)
  const histHeight = Number(macdHistHeight.value)
  const histBarWidth = Number(macdOscStyle.value.barWidth)
  const histOpacityUp = Number(macdOscStyle.value.opacityUp)
  const histOpacityDown = Number(macdOscStyle.value.opacityDown)
  const allowedDisplays = ['dif', 'macd', 'osc']
  const displayList = Array.isArray(macdDisplayMode.value)
    ? macdDisplayMode.value.filter(v => allowedDisplays.includes(v))
    : parseMacdDisplaySelection(macdDisplayMode.value)
  macdDisplayMode.value = displayList.length ? Array.from(new Set(displayList)) : ['dif', 'macd', 'osc']
  macdLineWidths.value.dif = Number.isFinite(difWidth) ? Math.max(1, Math.min(8, difWidth)) : 2
  macdLineWidths.value.macd = Number.isFinite(signalWidth) ? Math.max(1, Math.min(8, signalWidth)) : 2
  macdHistHeight.value = Number.isFinite(histHeight) ? Math.max(0.2, Math.min(10, histHeight)) : 0.7
  macdOscStyle.value.barWidth = Number.isFinite(histBarWidth) ? Math.max(10, Math.min(100, histBarWidth)) : 60
  macdOscStyle.value.opacityUp = Number.isFinite(histOpacityUp) ? Math.max(0.1, Math.min(1, histOpacityUp)) : 1
  macdOscStyle.value.opacityDown = Number.isFinite(histOpacityDown) ? Math.max(0.1, Math.min(1, histOpacityDown)) : 1
  macdOscStyle.value.colorUp = /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(String(macdOscStyle.value.colorUp || '').trim()) ? macdOscStyle.value.colorUp : '#ff5050'
  macdOscStyle.value.colorDown = /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(String(macdOscStyle.value.colorDown || '').trim()) ? macdOscStyle.value.colorDown : '#10b981'
  localStorage.setItem(`chartMACDFast${suffix}`, macdParams.value.fast.toString())
  localStorage.setItem(`chartMACDSlow${suffix}`, macdParams.value.slow.toString())
  localStorage.setItem(`chartMACDSignal${suffix}`, macdParams.value.signal.toString())
  localStorage.setItem(`chartMACDDisplay${suffix}`, macdDisplayMode.value.join(','))
  localStorage.setItem(`chartMACDDifWidth${suffix}`, macdLineWidths.value.dif.toString())
  localStorage.setItem(`chartMACDSignalWidth${suffix}`, macdLineWidths.value.macd.toString())
  localStorage.setItem(`chartMACDHistHeight${suffix}`, macdHistHeight.value.toString())
  localStorage.setItem(`chartMACDHistBarWidth${suffix}`, macdOscStyle.value.barWidth.toString())
  localStorage.setItem(`chartMACDHistColorUp${suffix}`, macdOscStyle.value.colorUp)
  localStorage.setItem(`chartMACDHistColorDown${suffix}`, macdOscStyle.value.colorDown)
  localStorage.setItem(`chartMACDHistOpacityUp${suffix}`, macdOscStyle.value.opacityUp.toString())
  localStorage.setItem(`chartMACDHistOpacityDown${suffix}`, macdOscStyle.value.opacityDown.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

function resetKDParams() {
  const suffix = getCurrentTfSuffix()
  const keys = [
    'chartKDPeriod',
    'chartKDK',
    'chartKDD',
    'chartShowKLine',
    'chartShowDLine',
    'chartKDBold',
    'chartKDMidline',
  ]
  keys.forEach((baseKey) => {
    try {
      localStorage.removeItem(`${baseKey}${suffix}`)
    } catch (_) {}
  })

  kdParams.value.period = 9
  kdParams.value.k = 3
  kdParams.value.d = 3
  showKLine.value = true
  showDLine.value = true
  kdBold.value = false
  kdMidline.value = false

  saveKDParams()
}

function resetMACDParams() {
  const suffix = getCurrentTfSuffix()
  const keys = [
    'chartMACDFast',
    'chartMACDSlow',
    'chartMACDSignal',
    'chartMACDDisplay',
    'chartMACDDifWidth',
    'chartMACDSignalWidth',
    'chartMACDHistHeight',
    'chartMACDHistBarWidth',
    'chartMACDHistColorUp',
    'chartMACDHistColorDown',
    'chartMACDHistOpacityUp',
    'chartMACDHistOpacityDown',
  ]
  keys.forEach((baseKey) => {
    try {
      localStorage.removeItem(`${baseKey}${suffix}`)
    } catch (_) {}
  })

  macdParams.value.fast = 12
  macdParams.value.slow = 26
  macdParams.value.signal = 9
  macdDisplayMode.value = ['dif', 'macd', 'osc']
  macdLineWidths.value.dif = 2
  macdLineWidths.value.macd = 2
  macdHistHeight.value = 0.7
  macdOscStyle.value.barWidth = 60
  macdOscStyle.value.colorUp = '#ff5050'
  macdOscStyle.value.colorDown = '#10b981'
  macdOscStyle.value.opacityUp = 1
  macdOscStyle.value.opacityDown = 1

  saveMACDParams()
}

function saveM4MomentumParams() {
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartM4Fast${suffix}`, m4MomentumParams.value.fast.toString())
  localStorage.setItem(`chartM4Slow${suffix}`, m4MomentumParams.value.slow.toString())
  localStorage.setItem(`chartM4MinChange${suffix}`, String(m4MomentumParams.value.minChange ?? 0))
  localStorage.setItem(`chartM4AutoSqueeze${suffix}`, String(m4MomentumParams.value.autoSqueeze))
  localStorage.setItem(`chartM4SqueezeTh${suffix}`, String(m4MomentumParams.value.squeezeTh ?? 0))
  localStorage.setItem(`chartM4AutoTrend${suffix}`, String(m4MomentumParams.value.autoTrend))
  localStorage.setItem(`chartM4TrendTh${suffix}`, String(m4MomentumParams.value.trendTh ?? 0))

  if (typeof m4MomentumParams.value.colorUp === 'string') localStorage.setItem('chartM4ColorUp', m4MomentumParams.value.colorUp)
  if (typeof m4MomentumParams.value.colorDown === 'string') localStorage.setItem('chartM4ColorDown', m4MomentumParams.value.colorDown)
  if (typeof m4MomentumParams.value.colorRange === 'string') localStorage.setItem('chartM4ColorRange', m4MomentumParams.value.colorRange)
  if (typeof m4MomentumParams.value.colorSqueeze === 'string') localStorage.setItem('chartM4ColorSqueeze', m4MomentumParams.value.colorSqueeze)
  const op = Number(m4MomentumParams.value.bandOpacity)
  if (Number.isFinite(op)) localStorage.setItem('chartM4BandOpacity', String(op))

  // keep legacy keys (no longer shown) for backward compatibility
  localStorage.setItem(`chartM4Signal${suffix}`, m4MomentumParams.value.signal.toString())
  localStorage.setItem('chartM4UseZeroFilter', m4MomentumParams.value.useZeroFilter.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

function saveCCIParams() {
  let p = Math.floor(Number(cciParams.value.period))
  if (!Number.isFinite(p) || p < 2) p = 2
  if (p > 300) p = 300
  cciParams.value.period = p

  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartCCIPeriod${suffix}`, cciParams.value.period.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

// Save RSI parameters to localStorage
function saveRSIParams() {
  let p = Math.floor(Number(rsiParams.value.period))
  if (!Number.isFinite(p) || p < 2) p = 2
  if (p > 100) p = 100
  rsiParams.value.period = p

  let ob = Math.floor(Number(rsiParams.value.overbought))
  if (!Number.isFinite(ob) || ob < 50) ob = 50
  if (ob > 100) ob = 100
  rsiParams.value.overbought = ob

  let os = Math.floor(Number(rsiParams.value.oversold))
  if (!Number.isFinite(os) || os < 0) os = 0
  if (os > 50) os = 50
  rsiParams.value.oversold = os

  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartRSIPeriod${suffix}`, rsiParams.value.period.toString())
  localStorage.setItem('chartRSIOverbought', rsiParams.value.overbought.toString())
  localStorage.setItem('chartRSIOversold', rsiParams.value.oversold.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

function resetRSIParams() {
  const suffix = getCurrentTfSuffix()
  try { localStorage.removeItem(`chartRSIPeriod${suffix}`) } catch (_) {}
  try { localStorage.removeItem('chartRSIOverbought') } catch (_) {}
  try { localStorage.removeItem('chartRSIOversold') } catch (_) {}

  rsiParams.value.period = 14
  rsiParams.value.overbought = 70
  rsiParams.value.oversold = 30
  saveRSIParams()
}

// Save Bollinger Bands parameters to localStorage
function saveBBParams() {
  let p = Math.floor(Number(bbParams.value.period))
  if (!Number.isFinite(p) || p < 2) p = 2
  if (p > 300) p = 300
  bbParams.value.period = p

  let mult = Number(bbParams.value.mult)
  if (!Number.isFinite(mult) || mult <= 0) mult = 2
  if (mult > 10) mult = 10
  bbParams.value.mult = Math.round(mult * 10) / 10

  const normalizeColor = (v, fallback) => {
    const s = String(v || '').trim()
    return /^#[0-9a-fA-F]{6}$/.test(s) ? s : fallback
  }
  bbParams.value.colorUpper = normalizeColor(bbParams.value.colorUpper, BB_DEFAULT_COLORS.colorUpper)
  bbParams.value.colorMid = normalizeColor(bbParams.value.colorMid, BB_DEFAULT_COLORS.colorMid)
  bbParams.value.colorLower = normalizeColor(bbParams.value.colorLower, BB_DEFAULT_COLORS.colorLower)

  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartBBPeriod${suffix}`, bbParams.value.period.toString())
  localStorage.setItem(`chartBBMult${suffix}`, bbParams.value.mult.toString())
  localStorage.setItem(`chartBBColorUpper${suffix}`, bbParams.value.colorUpper)
  localStorage.setItem(`chartBBColorMid${suffix}`, bbParams.value.colorMid)
  localStorage.setItem(`chartBBColorLower${suffix}`, bbParams.value.colorLower)
  scheduleSyncChartSettingsToServer()
  renderChart()
}

function resetBBParams() {
  const suffix = getCurrentTfSuffix()
  try { localStorage.removeItem(`chartBBPeriod${suffix}`) } catch (_) {}
  try { localStorage.removeItem(`chartBBMult${suffix}`) } catch (_) {}
  try { localStorage.removeItem(`chartBBColorUpper${suffix}`) } catch (_) {}
  try { localStorage.removeItem(`chartBBColorMid${suffix}`) } catch (_) {}
  try { localStorage.removeItem(`chartBBColorLower${suffix}`) } catch (_) {}

  bbParams.value.period = 20
  bbParams.value.mult = 2
  bbParams.value.colorUpper = BB_DEFAULT_COLORS.colorUpper
  bbParams.value.colorMid = BB_DEFAULT_COLORS.colorMid
  bbParams.value.colorLower = BB_DEFAULT_COLORS.colorLower
  saveBBParams()
}

function saveExpertParams() {
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartExpertLength${suffix}`, expertParams.value.length.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

function saveVPVRParams() {
  const suffix = getCurrentTfSuffix()

  const bins = Math.floor(Number(vpvrParams.value.bins) || 0)
  if (Number.isFinite(bins) && bins > 0) {
    localStorage.setItem(`chartVPVRBins${suffix}`, String(bins))
  } else {
    localStorage.removeItem(`chartVPVRBins${suffix}`)
  }

  const maxWidthPct = Number(vpvrParams.value.maxWidthPct)
  const minWidthPct = Number(vpvrParams.value.minWidthPct)
  if (Number.isFinite(maxWidthPct)) localStorage.setItem(`chartVPVRMaxWidthPct${suffix}`, String(maxWidthPct))
  if (Number.isFinite(minWidthPct)) localStorage.setItem(`chartVPVRMinWidthPct${suffix}`, String(minWidthPct))

  if (typeof vpvrParams.value.fillColor === 'string') localStorage.setItem(`chartVPVRFillColor${suffix}`, vpvrParams.value.fillColor)
  const fillAlpha = Number(vpvrParams.value.fillAlpha)
  if (Number.isFinite(fillAlpha)) localStorage.setItem(`chartVPVRFillAlpha${suffix}`, String(fillAlpha))

  if (typeof vpvrParams.value.strokeColor === 'string') localStorage.setItem(`chartVPVRStrokeColor${suffix}`, vpvrParams.value.strokeColor)
  const strokeAlpha = Number(vpvrParams.value.strokeAlpha)
  if (Number.isFinite(strokeAlpha)) localStorage.setItem(`chartVPVRStrokeAlpha${suffix}`, String(strokeAlpha))

  scheduleSyncChartSettingsToServer()
  renderChart()
}

// Save MA parameters to localStorage
function saveMAParams() {
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartMA1${suffix}`, maParams.value.ma1.toString())
  localStorage.setItem(`chartMA2${suffix}`, maParams.value.ma2.toString())
  localStorage.setItem(`chartMA3${suffix}`, maParams.value.ma3.toString())
  localStorage.setItem(`chartMA4${suffix}`, maParams.value.ma4.toString())
  localStorage.setItem(`chartMA5${suffix}`, maParams.value.ma5.toString())
  scheduleSyncChartSettingsToServer()
  renderChart()
}

// Save MA visibility to localStorage
function saveMAVisibility() {
  localStorage.setItem('chartShowMA1', showMA1.value.toString())
  localStorage.setItem('chartShowMA2', showMA2.value.toString())
  localStorage.setItem('chartShowMA3', showMA3.value.toString())
  localStorage.setItem('chartShowMA4', showMA4.value.toString())
  localStorage.setItem('chartShowMA5', showMA5.value.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}

// Save Tower (寶塔線) parameters to localStorage
function saveTowerParams() {
  let buf = Number(towerParams.value.buffer)
  if (!Number.isFinite(buf) || buf < 0) buf = 0
  if (buf > 20) buf = 20 // 上限 20% 避免輸入錯誤
  towerParams.value.buffer = buf
  // 儲存時以比例形式存入（0.5% -> 0.005），並依週期區分
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartTowerBuffer${suffix}`, (buf / 100).toString())
  renderChart()
}

// Save diagonal S/R parameters to localStorage
function saveDiagSrParams() {
  const params = diagSrParams
  params.lookback = Math.max(1, Math.min(20, Number(params.lookback) || 5))
  params.maxSegments = Math.max(1, Math.min(8, Math.floor(Number(params.maxSegments) || 3)))
  params.windowSize = Math.max(20, Math.min(300, Math.floor(Number(params.windowSize) || 120)))
  params.minSpan = Math.max(1, Math.min(50, Math.floor(Number(params.minSpan) || 5)))

  const suffix = getCurrentTfSuffix()

  localStorage.setItem(`diagSrLookback${suffix}`, params.lookback.toString())
  localStorage.setItem(`diagSrMaxSegments${suffix}`, params.maxSegments.toString())
  localStorage.setItem(`diagSrWindowSize${suffix}`, params.windowSize.toString())
  localStorage.setItem(`diagSrMinSpan${suffix}`, params.minSpan.toString())

  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}

function resetDiagSrParams() {
  Object.assign(diagSrParams, DEFAULT_DIAG_SR)
  saveDiagSrParams()
}

// Save HMA parameters to localStorage
function saveHMAParams() {
  let p = Math.floor(Number(hmaParams.value.period))
  if (!Number.isFinite(p) || p < 2) p = 2
  if (p > 200) p = 200
  hmaParams.value.period = p
  localStorage.setItem('chartHMAPeriod', String(hmaParams.value.period))
  renderChart()
}

function saveHMAIndParams() {
  let p = Math.floor(Number(hmaIndParams.value.period))
  if (!Number.isFinite(p) || p < 2) p = 2
  if (p > 200) p = 200
  hmaIndParams.value.period = p

  let pad = Number(hmaIndParams.value.paddingPct)
  if (!Number.isFinite(pad)) pad = 3
  if (pad < 0) pad = 0
  if (pad > 20) pad = 20
  // 保留 2 位小數方便微調
  hmaIndParams.value.paddingPct = Math.round(pad * 100) / 100

  const suffix = getCurrentTfSuffix()

  localStorage.setItem(`chartHMAIndPeriod${suffix}`, String(hmaIndParams.value.period))
  localStorage.setItem(`chartHMAIndUpColor${suffix}`, String(hmaIndParams.value.upColor))
  localStorage.setItem(`chartHMAIndDownColor${suffix}`, String(hmaIndParams.value.downColor))
  localStorage.setItem(`chartHMAIndPaddingPct${suffix}`, String(hmaIndParams.value.paddingPct))
  localStorage.setItem(`chartHMAIndZeroVisible${suffix}`, String(hmaIndParams.value.zeroVisible))
  renderChart()
}

function resetHMAIndParams() {
  const suffix = getCurrentTfSuffix()
  const keys = [
    'chartHMAIndPeriod',
    'chartHMAIndUpColor',
    'chartHMAIndDownColor',
    'chartHMAIndPaddingPct',
    'chartHMAIndZeroVisible'
  ]
  for (const k of keys) {
    try { localStorage.removeItem(`${k}${suffix}`) } catch (_) {}
  }
  Object.assign(hmaIndParams.value, {
    period: 24,
    upColor: '#ef4444',
    downColor: '#22c55e',
    paddingPct: 3,
    zeroVisible: false
  })
  saveHMAIndParams()
}

let maHmaExclusiveSyncing = false
/** Mobile / narrow width: main-chart HMA and MA legend are mutually exclusive (slightly wider than isMobileViewport for small tablets). */
function isMobileMaHmaExclusiveMode() {
  try {
    const iw = Number(window.innerWidth) || 0
    const vw = Number(window.visualViewport?.width)
    const w = Number.isFinite(vw) && vw > 0 ? Math.min(iw, vw) : iw
    return w > 0 && w <= 768
  } catch (_) {
    return false
  }
}

// HMA 顯示開關：同步到 localStorage 並即時重繪
watch(showHMA, (val) => {
  if (val && !canUseProTech.value) {
    showHMA.value = false
    localStorage.setItem('chartShowHMA', 'false')
    alertUpgrade('Pro')
    return
  }

  localStorage.setItem('chartShowHMA', val.toString())
  scheduleSyncChartSettingsToServer()
  if (!maHmaExclusiveSyncing && val && isMobileMaHmaExclusiveMode()) {
    maHmaExclusiveSyncing = true
    maCollapsedByHma = true
    maLegendCollapsed.value = true
    showMA1.value = false
    showMA2.value = false
    showMA3.value = false
    showMA4.value = false
    showMA5.value = false
    saveMAVisibility()
    maHmaExclusiveSyncing = false
  }
  if (!val && maCollapsedByHma) {
    maCollapsedByHma = false
    maHmaExclusiveSyncing = true
    maLegendCollapsed.value = false
    showMA1.value = true
    showMA2.value = true
    showMA3.value = true
    showMA4.value = true
    showMA5.value = true
    saveMAVisibility()
    maHmaExclusiveSyncing = false
  }

  if (chartData.value && chartData.value.length > 0 && chartInstance) {
    renderChart()
  }
}, { immediate: true })

watch([showMA1, showMA2, showMA3, showMA4, showMA5], () => {
  saveMAVisibility()
  if (!maHmaExclusiveSyncing && isMobileMaHmaExclusiveMode()) {
    const hasAnyMA = !!(showMA1.value || showMA2.value || showMA3.value || showMA4.value || showMA5.value)
    if (hasAnyMA && showHMA.value) {
      maHmaExclusiveSyncing = true
      showHMA.value = false
      maHmaExclusiveSyncing = false
    }
  }
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
})

watch(showHMAInd, (val) => {
  if (val && !canUseProTech.value) {
    showHMAInd.value = false
    localStorage.setItem('chartShowHMAInd', 'false')
    alertUpgrade('Pro')
    return
  }
  localStorage.setItem('chartShowHMAInd', val.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
})

watch(showGoldenWave, (val) => {
  if (val && !canUseProTech.value) {
    showGoldenWave.value = false
    localStorage.setItem('chartShowGoldenWave', 'false')
    alertUpgrade('Pro')
    return
  }
  localStorage.setItem('chartShowGoldenWave', val.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}, { immediate: true })

watch(showCCI, (val) => {
  if (val && !canUseProTech.value) {
    showCCI.value = false
    localStorage.setItem('chartShowCCI', 'false')
    alertUpgrade('Pro')
    return
  }
  localStorage.setItem('chartShowCCI', val.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}, { immediate: true })

watch(showVPVR, (val) => {
  if (val && !canUseProTech.value) {
    showVPVR.value = false
    localStorage.setItem('chartShowVPVR', 'false')
    alertUpgrade('Pro')
    return
  }
  localStorage.setItem('chartShowVPVR', val.toString())
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
})

watch(showFib, (val) => {
  if (val && !canUseProTech.value) {
    showFib.value = false
    localStorage.setItem('chartShowFib', 'false')
    alertUpgrade('Pro')
    return
  }
  localStorage.setItem('chartShowFib', val.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
}, { immediate: true })

watch(showDiagSR, (val) => {
  if (val && !canUseProTech.value) {
    showDiagSR.value = false
    localStorage.setItem('chartShowDiagSR', 'false')
    alertUpgrade('Pro')
    return
  }
  localStorage.setItem('chartShowDiagSR', val.toString())
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
})

// KD 顯示相關開關：K 線 / D 線 / 50 中線 / 粗細
watch([showKLine, showDLine, kdMidline, kdBold], () => {
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartShowKLine${suffix}`, showKLine.value.toString())
  localStorage.setItem(`chartShowDLine${suffix}`, showDLine.value.toString())
  localStorage.setItem(`chartKDMidline${suffix}`, kdMidline.value.toString())
  localStorage.setItem(`chartKDBold${suffix}`, kdBold.value.toString())
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
})

const DEFAULT_PERIOD = '1D'
const periodOptions = [
  { key: 'day', period: '1D', label: '日K' },
  { key: 'week', period: '1W', label: '週K' },
  { key: 'month', period: '1M', label: '月K' }
]

const periodKeyMap = new Map(periodOptions.map(option => [option.key, option]))
const periodValueMap = new Map(periodOptions.map(option => [option.period, option]))

function resolveInitialKey(initialPeriod) {
  if (periodValueMap.has(initialPeriod)) {
    return periodValueMap.get(initialPeriod).key
  }
  return periodOptions.find(option => option.period === DEFAULT_PERIOD)?.key ?? 'day'
}

const selectedPeriodKey = ref(resolveInitialKey(props.period))

const olderLoading = ref(false)
const oldestLoadedEnd = ref(null)

function getCurrentTfSuffix() {
  const key = selectedPeriodKey.value || resolveInitialKey(props.period) || 'day'
  return `_${key}`
}

function loadParamsForCurrentPeriod() {
  const suffix = getCurrentTfSuffix()
  const getWithFallback = (baseKey, fallback) => {
    const tfKey = `${baseKey}${suffix}`
    const tfVal = localStorage.getItem(tfKey)
    if (tfVal != null) return tfVal
    const globalVal = localStorage.getItem(baseKey)
    return globalVal != null ? globalVal : fallback
  }

  kdParams.value.period = Number(getWithFallback('chartKDPeriod', '9'))
  kdParams.value.k = Number(getWithFallback('chartKDK', '3'))
  kdParams.value.d = Number(getWithFallback('chartKDD', '3'))
  showKLine.value = getWithFallback('chartShowKLine', 'true') !== 'false'
  showDLine.value = getWithFallback('chartShowDLine', 'true') !== 'false'
  kdMidline.value = getWithFallback('chartKDMidline', 'false') === 'true'
  kdBold.value = getWithFallback('chartKDBold', 'false') === 'true'
  macdParams.value.fast = parseInt(getWithFallback('chartMACDFast', '12'))
  macdParams.value.slow = parseInt(getWithFallback('chartMACDSlow', '26'))
  macdParams.value.signal = parseInt(getWithFallback('chartMACDSignal', '9'))
  macdDisplayMode.value = parseMacdDisplaySelection(getWithFallback('chartMACDDisplay', 'all'))
  macdLineWidths.value.dif = parseFloat(getWithFallback('chartMACDDifWidth', '2'))
  macdLineWidths.value.macd = parseFloat(getWithFallback('chartMACDSignalWidth', '2'))
  macdHistHeight.value = parseFloat(getWithFallback('chartMACDHistHeight', '0.7'))
  macdOscStyle.value.barWidth = parseFloat(getWithFallback('chartMACDHistBarWidth', '60'))
  macdOscStyle.value.colorUp = getWithFallback('chartMACDHistColorUp', '#ff5050')
  macdOscStyle.value.colorDown = getWithFallback('chartMACDHistColorDown', '#10b981')
  macdOscStyle.value.opacityUp = parseFloat(getWithFallback('chartMACDHistOpacityUp', '1'))
  macdOscStyle.value.opacityDown = parseFloat(getWithFallback('chartMACDHistOpacityDown', '1'))
  cciParams.value.period = parseInt(getWithFallback('chartCCIPeriod', String(cciParams.value.period || 100)))

  rsiParams.value.period = parseInt(getWithFallback('chartRSIPeriod', String(rsiParams.value.period || 14)))
  rsiParams.value.overbought = parseInt(getWithFallback('chartRSIOverbought', String(rsiParams.value.overbought || 70)))
  rsiParams.value.oversold = parseInt(getWithFallback('chartRSIOversold', String(rsiParams.value.oversold || 30)))

  bbParams.value.period = parseInt(getWithFallback('chartBBPeriod', String(bbParams.value.period || 20)))
  bbParams.value.mult = parseFloat(getWithFallback('chartBBMult', String(bbParams.value.mult || 2)))
  bbParams.value.colorUpper = getWithFallback('chartBBColorUpper', bbParams.value.colorUpper || BB_DEFAULT_COLORS.colorUpper)
  bbParams.value.colorMid = getWithFallback('chartBBColorMid', bbParams.value.colorMid || BB_DEFAULT_COLORS.colorMid)
  bbParams.value.colorLower = getWithFallback('chartBBColorLower', bbParams.value.colorLower || BB_DEFAULT_COLORS.colorLower)

  maParams.value.ma1 = parseInt(getWithFallback('chartMA1', '5'))
  maParams.value.ma2 = parseInt(getWithFallback('chartMA2', '10'))
  maParams.value.ma3 = parseInt(getWithFallback('chartMA3', '20'))
  maParams.value.ma4 = parseInt(getWithFallback('chartMA4', '30'))
  maParams.value.ma5 = parseInt(getWithFallback('chartMA5', '60'))

  // Tower buffer 以比例儲存，讀取時轉回百分比
  const bufRatioStr = getWithFallback('chartTowerBuffer', '0.01')
  const bufRatio = parseFloat(bufRatioStr)
  towerParams.value.buffer = Number.isFinite(bufRatio) && bufRatio >= 0 ? bufRatio * 100 : 1

  vpvrParams.value.bins = parseInt(getWithFallback('chartVPVRBins', '0'))
  vpvrParams.value.maxWidthPct = parseFloat(getWithFallback('chartVPVRMaxWidthPct', '0.62'))
  vpvrParams.value.minWidthPct = parseFloat(getWithFallback('chartVPVRMinWidthPct', '0.02'))
  vpvrParams.value.fillColor = getWithFallback('chartVPVRFillColor', '#2563eb')
  vpvrParams.value.fillAlpha = parseFloat(getWithFallback('chartVPVRFillAlpha', '0.30'))
  vpvrParams.value.strokeColor = getWithFallback('chartVPVRStrokeColor', '#60a5fa')
  vpvrParams.value.strokeAlpha = parseFloat(getWithFallback('chartVPVRStrokeAlpha', '0.50'))

  const gwVersionKey = `chartGWVersion${suffix}`
  const gwVersion = localStorage.getItem(gwVersionKey)
  const EXPECTED_GW_VERSION = 'ptr_a_v1'
  if (gwVersion !== EXPECTED_GW_VERSION) {
    const keysToReset = [
      'chartGWFastMa',
      'chartGWSlowMa',
      'chartGWFastMa2',
      'chartGWSlowMa2',
      'chartGWMultiMa',
      'chartGWWaveMa2',
      'chartGWWaveMa3',
      'chartGWBoxPeriod',
      'chartGWShowDifLine',
      'chartGWShowMa2Line',
      'chartGWDifLineColor',
      'chartGWMa2LineColor',
      'chartGWDifLineWidth',
      'chartGWMa2LineWidth',
      'chartGWBarUpColor',
      'chartGWBarStopUpColor',
      'chartGWBarDownColor',
      'chartGWBarStopDownColor',
      'chartGWBarScale'
    ]
    for (const k of keysToReset) {
      try { localStorage.removeItem(`${k}${suffix}`) } catch (_) {}
      try { localStorage.removeItem(k) } catch (_) {}
    }
    try { localStorage.setItem(gwVersionKey, EXPECTED_GW_VERSION) } catch (_) {}
  }

  goldenWaveParams.value.fastMa = parseInt(getWithFallback('chartGWFastMa', '30'))
  goldenWaveParams.value.slowMa = parseInt(getWithFallback('chartGWSlowMa', '100'))
  goldenWaveParams.value.fastMa2 = parseInt(getWithFallback('chartGWFastMa2', '130'))
  goldenWaveParams.value.slowMa2 = parseInt(getWithFallback('chartGWSlowMa2', '140'))
  goldenWaveParams.value.multiMa = parseInt(getWithFallback('chartGWMultiMa', '18'))
  goldenWaveParams.value.waveMa2 = parseInt(getWithFallback('chartGWWaveMa2', '18'))
  goldenWaveParams.value.waveMa3 = parseInt(getWithFallback('chartGWWaveMa3', '18'))
  goldenWaveParams.value.boxPeriod = parseInt(getWithFallback('chartGWBoxPeriod', '78'))
  goldenWaveParams.value.showDifLine = getWithFallback('chartGWShowDifLine', 'true') !== 'false'
  goldenWaveParams.value.showMa2Line = getWithFallback('chartGWShowMa2Line', 'false') === 'true'
  goldenWaveParams.value.difLineColor = getWithFallback('chartGWDifLineColor', '#1e3a8a')
  goldenWaveParams.value.ma2LineColor = getWithFallback('chartGWMa2LineColor', '#ef4444')
  goldenWaveParams.value.difLineWidth = parseFloat(getWithFallback('chartGWDifLineWidth', '3'))
  goldenWaveParams.value.ma2LineWidth = parseFloat(getWithFallback('chartGWMa2LineWidth', '3'))
  goldenWaveParams.value.barUpColor = getWithFallback('chartGWBarUpColor', '#dc2626')
  goldenWaveParams.value.barStopUpColor = getWithFallback('chartGWBarStopUpColor', '#22c55e')
  goldenWaveParams.value.barDownColor = getWithFallback('chartGWBarDownColor', '#1144B0')
  goldenWaveParams.value.barStopDownColor = getWithFallback('chartGWBarStopDownColor', '#06b6d4')
  goldenWaveParams.value.barScale = parseFloat(getWithFallback('chartGWBarScale', '1'))

  diagSrParams.lookback = Number(getWithFallback('diagSrLookback', String(DEFAULT_DIAG_SR.lookback)))
  diagSrParams.maxSegments = Number(getWithFallback('diagSrMaxSegments', String(DEFAULT_DIAG_SR.maxSegments)))
  diagSrParams.windowSize = Number(getWithFallback('diagSrWindowSize', String(DEFAULT_DIAG_SR.windowSize)))
  diagSrParams.minSpan = Number(getWithFallback('diagSrMinSpan', String(DEFAULT_DIAG_SR.minSpan)))

  hmaIndParams.value.period = parseInt(getWithFallback('chartHMAIndPeriod', '24'), 10)
  hmaIndParams.value.upColor = getWithFallback('chartHMAIndUpColor', '#ef4444')
  hmaIndParams.value.downColor = getWithFallback('chartHMAIndDownColor', '#22c55e')
  hmaIndParams.value.paddingPct = parseFloat(getWithFallback('chartHMAIndPaddingPct', '3'))
  hmaIndParams.value.zeroVisible = getWithFallback('chartHMAIndZeroVisible', 'false') === 'true'

  if (hmaIndParams.value.upColor === '#3b82f6' && hmaIndParams.value.downColor === '#ef4444') {
    hmaIndParams.value.upColor = '#ef4444'
    hmaIndParams.value.downColor = '#22c55e'
    localStorage.setItem(`chartHMAIndUpColor${suffix}`, hmaIndParams.value.upColor)
    localStorage.setItem(`chartHMAIndDownColor${suffix}`, hmaIndParams.value.downColor)
  }
}

// 初始載入當前週期的參數
loadParamsForCurrentPeriod()

watch(() => props.period, (newPeriod) => {
  const resolvedKey = resolveInitialKey(newPeriod)
  if (selectedPeriodKey.value !== resolvedKey) {
    selectedPeriodKey.value = resolvedKey
    // selectedPeriodKey watcher 統一載入，避免同一週期變更重複請求／重繪。
  }
})

// 當使用者在控制面板切換日K/週K/月K等時，selectedPeriodKey 會改變
// 這裡確保每次切換週期都重新載入該週期的指標參數與資料
watch(selectedPeriodKey, () => {
  if (suppressSelectedPeriodAutoLoad) return
  loadParamsForCurrentPeriod()
  loadChartData()
})

watch(drawingMode, () => {
  if (!loading.value && chartData.value.length > 0) {
    renderChart()
  }
})

function onQuantgemsDataUpdated() {
  chartDataCache.clear()
  loadChartData()
}

async function loadChartData() {
  const symbol = String(props.symbol || '').trim()
  if (!symbol) return

  const requestId = ++chartDataRequestId
  resetRenderRetryState()
  clearChartError()
  loading.value = true
  const option = periodKeyMap.get(selectedPeriodKey.value)
  const periodValue = option?.period ?? DEFAULT_PERIOD
  const cacheKey = `${symbol.toUpperCase()}|${periodValue}`
  try {
    const cachedRows = chartDataCache.get(cacheKey)
    const rawRows = cachedRows || await fetchStockPriceHistory(symbol, periodValue)
    if (requestId !== chartDataRequestId || symbol !== String(props.symbol || '').trim()) return
    if (!cachedRows) {
      chartDataCache.set(cacheKey, Array.isArray(rawRows) ? rawRows : [])
      if (chartDataCache.size > CHART_DATA_CACHE_LIMIT) {
        chartDataCache.delete(chartDataCache.keys().next().value)
      }
    }
    rawChartData.value = Array.isArray(rawRows) ? rawRows : []
    applyChartDataFromRaw()
    oldestLoadedEnd.value = rawChartData.value?.[0]?.time ? toIsoDateOnly(rawChartData.value[0].time) : null
    if (chartData.value.length === 0) {
      setChartError('查無可顯示的歷史資料', `${symbol} 在目前週期沒有 K 線資料`)
    }
  } catch (error) {
    if (requestId !== chartDataRequestId) return
    console.error('StockChart: Error loading data', error)
    setChartError(describeChartLoadError(error), error)
  } finally {
    if (requestId !== chartDataRequestId) return
    loading.value = false
  }
  // loading=false 會由下方 data watcher 統一繪圖；此處不再重畫第二次。
}

function uniqMergePrepend(oldArr, newArr) {
  const oldList = Array.isArray(oldArr) ? oldArr : []
  const newList = Array.isArray(newArr) ? newArr : []
  const seen = new Set(oldList.map((r) => String(r?.time || '')))
  const prepend = []
  for (const row of newList) {
    const k = String(row?.time || '')
    if (!k || seen.has(k)) continue
    prepend.push(row)
    seen.add(k)
  }
  prepend.sort((a, b) => String(a?.time || '').localeCompare(String(b?.time || '')))
  return prepend.concat(oldList)
}

async function maybeLoadOlder({ startValue, endValue } = {}) {
  try {
    if (olderLoading.value || loading.value) return
    if (!props.symbol) return
    const option = periodKeyMap.get(selectedPeriodKey.value)
    const periodValue = option?.period ?? DEFAULT_PERIOD

    const len = chartData.value.length
    if (!len) return

    const sv = typeof startValue === 'number' ? startValue : 0
    const ev = typeof endValue === 'number' ? endValue : (len - 1)
    const leftEdgeHit = sv <= Math.max(2, Math.floor(len * 0.02))
    if (!leftEdgeHit) return

    const currentOldest = chartData.value?.[0]?.time ? toIsoDateOnly(chartData.value[0].time) : null
    if (!currentOldest) return

    if (oldestLoadedEnd.value && currentOldest === oldestLoadedEnd.value) {
      // already attempted with same oldest boundary
    }

    // Fetch older chunk ending at the day before current oldest
    const d = new Date(`${currentOldest}T00:00:00Z`)
    if (Number.isNaN(d.getTime())) return
    d.setUTCDate(d.getUTCDate() - 1)
    const end = toIsoDateOnly(d)
    if (!end) return
    if (end === oldestLoadedEnd.value) return

    olderLoading.value = true
    oldestLoadedEnd.value = end

    const older = await fetchStockPriceHistory(props.symbol, periodValue, { end })
    if (!Array.isArray(older) || older.length === 0) return

    const beforeLen = rawChartData.value.length
    const merged = uniqMergePrepend(rawChartData.value, older)
    const added = merged.length - beforeLen
    if (added <= 0) {
      return
    }
    rawChartData.value = merged
    applyChartDataFromRaw()

    // Keep current window stable by shifting indices to the right by added
    await nextTick()
    if (!chartInstance) return
    const indexes = dataZoomIndexes.length ? dataZoomIndexes : [0]
    for (const idx of indexes) {
      try {
        chartInstance.dispatchAction({
          type: 'dataZoom',
          dataZoomIndex: idx,
          startValue: sv + added,
          endValue: ev + added,
        })
      } catch (_) {}
    }
  } catch (e) {
    console.warn('maybeLoadOlder error', e)
  } finally {
    olderLoading.value = false
  }
}

function renderChart() {
  let subplotLayoutBasisH = 0
  if (!chartContainer.value || !chartMountEl.value || chartData.value.length === 0) {
    console.warn('[StockChart debug] renderChart skipped: missing container or empty data', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      hasChartContainer: !!chartContainer.value,
      chartDataLength: chartData.value.length,
      loading: loading.value,
    })
    visibleKCount.value = chartData.value.length
    desiredKCount.value = chartData.value.length
    return
  }

  if (loading.value) {
    console.warn('[StockChart debug] renderChart skipped: still loading', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
    })
    return
  }

  try { syncFullscreenChartViewportLayout('renderChart') } catch (_) {}
  const containerRect = chartContainer.value.getBoundingClientRect?.()
  console.log('[StockChart debug] renderChart start', {
    symbol: props.symbol,
    isFullscreen: isFullscreen.value,
    chartDataLength: chartData.value.length,
    containerClientWidth: chartContainer.value.clientWidth,
    containerClientHeight: chartContainer.value.clientHeight,
    containerRect: containerRect ? { width: containerRect.width, height: containerRect.height, top: containerRect.top, left: containerRect.left } : null,
  })

  chartInstance = ensureChartInstance()
  if (!chartInstance) {
    console.warn('[StockChart debug] renderChart: ensureChartInstance returned null; scheduling retry', {
      symbol: props.symbol,
      isFullscreen: isFullscreen.value,
      chartDataLength: chartData.value.length,
      renderRetryCount,
    })
    scheduleRenderRetry()
    return
  }
  resetRenderRetryState()
  
  // Prepare data for ECharts
  const dates = chartData.value.map(d => d.time)
  // Identify index symbols for style tweaks
  const sym = String(props.symbol || '').toUpperCase()
  const isIndex = sym.startsWith('^') || sym === 'TWII'

  // Build OHLC according to selected mode
  const buildHeikinAshi = (rows) => {
    const ha = []
    for (let i = 0; i < rows.length; i++) {
      const { open, high, low, close } = rows[i]
      const haClose = (open + high + low + close) / 4
      const prev = ha[i - 1]
      const prevHaOpen = prev ? prev[0] : open
      const prevHaClose = prev ? prev[1] : close
      const haOpen = (prevHaOpen + prevHaClose) / 2
      const haHigh = Math.max(high, haOpen, haClose)
      const haLow = Math.min(low, haOpen, haClose)
      // Keep order [open, close, low, high]
      ha.push([Number(haOpen.toFixed(4)), Number(haClose.toFixed(4)), Number(haLow.toFixed(4)), Number(haHigh.toFixed(4))])
    }
    return ha
  }

  // Optionally smooth inputs before computing HA
  function buildHeikinAshiWithSmoothing(rows, n = 1) {
    const N = Math.max(1, Math.floor(n || 1))
    if (N === 1) return buildHeikinAshi(rows)
    const o = [], h = [], l = [], c = []
    for (let i = 0; i < rows.length; i++) {
      o.push(rows[i].open)
      h.push(rows[i].high)
      l.push(rows[i].low)
      c.push(rows[i].close)
    }
    function sma(arr, p) {
      const out = new Array(arr.length).fill(undefined)
      let sum = 0
      for (let i = 0; i < arr.length; i++) {
        sum += arr[i]
        if (i >= p) sum -= arr[i - p]
        if (i >= p - 1) out[i] = sum / p
      }
      return out
    }
    const so = sma(o, N), sh = sma(h, N), sl = sma(l, N), sc = sma(c, N)
    const smoothed = rows.map((r, i) => ({
      open: so[i] ?? r.open,
      high: sh[i] ?? r.high,
      low: sl[i] ?? r.low,
      close: sc[i] ?? r.close,
    }))
    return buildHeikinAshi(smoothed)
  }

  // ECharts candlestick expects: [open, close, low, high]
  const standardOhlc = chartData.value.map(d => [d.open, d.close, d.low, d.high])
  const ohlc = chartMode.value === 'heikin' ? buildHeikinAshi(chartData.value) : standardOhlc
  let towerSeries = null

  if (showHA.value) {
    try {
      towerSeries = calculateTowerSeries(chartData.value)
    } catch (err) {
      console.error('Failed to calculate Tower series:', err)
      towerSeries = null
    }
  }
  // Keep a copy of raw OHLC for volume color decision (do not depend on Heikin mode)
  const rawOhlc = standardOhlc
  // Color palette：一般股票共用一組配色，指數略為加亮，讓加權K棒更明顯（不改變寬度與密度）
  const baseUpCandleColor = '#f87171'
  const baseDownCandleColor = '#4ade80'
  const upCandleColor = isIndex ? '#ff4d6a' : baseUpCandleColor
  const downCandleColor = isIndex ? '#16c47f' : baseDownCandleColor
  const upCandleBorder = isIndex ? '#ff5c75' : '#ef4444'
  const downCandleBorder = isIndex ? '#16a36b' : '#22c55e'
  const upCandleGlow = 'rgba(248, 113, 113, 0.32)'
  const downCandleGlow = 'rgba(74, 222, 128, 0.30)'
  const narrowMobileForKline = (() => {
    try {
      return isMobileViewport()
    } catch (_) {
      return false
    }
  })()
  // Timeframe-aware widths: 稍微收窄 K 棒寬度，讓主圖更清爽；指數與個股共用同一套寬度設定（僅桌機）。
  // 手機（isMobileViewport）略加寬棒實、提高日K 像素下限並略收類別間距，觸控小螢幕較易辨識。
  const tf = selectedPeriodKey.value
  const isWeek = tf === 'week'
  const isMonth = tf === 'month'
  // 原本日K 60% / 週K 48% / 月K 36%，統一調細一階（桌機預設）
  let candleBarWidth = isMonth ? '30%' : (isWeek ? '42%' : '52%')
  let candleMinW = isMonth ? 2 : (isWeek ? 3 : 2)
  let candleMaxW = isMonth ? 12 : (isWeek ? 16 : 12)
  let candleCategoryGap = isMonth ? '42%' : (isWeek ? '30%' : '20%')
  if (narrowMobileForKline) {
    candleBarWidth = isMonth ? '34%' : (isWeek ? '46%' : '58%')
    candleMinW = isMonth ? 2 : (isWeek ? 3 : 2)
    candleMaxW = isMonth ? 14 : (isWeek ? 18 : 14)
    candleCategoryGap = isMonth ? '38%' : (isWeek ? '24%' : '14%')
  }
  const rawVolumes = chartData.value.map(d => Number(d?.volume || 0))
  const volumes = isIndex
    ? rawVolumes
    : rawVolumes.map(v => Math.round(v / 1000))
  const volBarWidth = isMonth ? '36%' : (isWeek ? '48%' : '60%')
  const volCategoryGap = isMonth ? '40%' : (isWeek ? '30%' : '20%')

  const vpvrBinsDefault = isIndex
    ? (isMonth ? 18 : (isWeek ? 22 : 26))
    : (isMonth ? 14 : (isWeek ? 18 : 22))
  const vpvrBins = (() => {
    const b = Number(vpvrParams.value?.bins)
    if (Number.isFinite(b) && b > 0) return Math.max(6, Math.min(120, Math.floor(b)))
    return vpvrBinsDefault
  })()

  const hexToRgb = (hex) => {
    const s = String(hex || '').trim().replace(/^#/, '')
    if (!/^[0-9a-fA-F]{6}$/.test(s)) return null
    const r = parseInt(s.slice(0, 2), 16)
    const g = parseInt(s.slice(2, 4), 16)
    const b = parseInt(s.slice(4, 6), 16)
    return { r, g, b }
  }

  const rgbaFromHex = (hex, alpha, fallback) => {
    const rgb = hexToRgb(hex)
    const a = Number(alpha)
    const aa = Number.isFinite(a) ? Math.max(0, Math.min(1, a)) : undefined
    if (rgb && aa !== undefined) return `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${aa})`
    return fallback
  }

  const computeVPVR = (startIdx, endIdx, bins = 28) => {
    const n = Array.isArray(rawOhlc) ? rawOhlc.length : 0
    if (!n) return []
    const s = Math.max(0, Math.min(n - 1, Math.floor(startIdx ?? 0)))
    const e = Math.max(0, Math.min(n - 1, Math.floor(endIdx ?? (n - 1))))
    const loI = Math.min(s, e)
    const hiI = Math.max(s, e)
    const anchorX = dates[loI] ?? dates[0] ?? ''
    let minP = Infinity
    let maxP = -Infinity
    for (let i = loI; i <= hiI; i++) {
      const h = Number(rawOhlc[i]?.[3])
      const l = Number(rawOhlc[i]?.[2])
      if (Number.isFinite(h) && h > maxP) maxP = h
      if (Number.isFinite(l) && l < minP) minP = l
    }
    if (!Number.isFinite(minP) || !Number.isFinite(maxP) || maxP <= minP) return []
    const B = Math.max(8, Math.min(120, Math.floor(bins || 28)))
    const size = (maxP - minP) / B
    if (!Number.isFinite(size) || size <= 0) return []
    const sums = new Array(B).fill(0)
    for (let i = loI; i <= hiI; i++) {
      const c = Number(rawOhlc[i]?.[1])
      const v = Number(rawVolumes[i] ?? 0)
      if (!Number.isFinite(c) || !Number.isFinite(v)) continue
      const idx = Math.max(0, Math.min(B - 1, Math.floor((c - minP) / size)))
      sums[idx] += v
    }
    let maxV = 0
    for (let i = 0; i < B; i++) {
      const v = sums[i]
      if (Number.isFinite(v) && v > maxV) maxV = v
    }
    if (!Number.isFinite(maxV) || maxV <= 0) return []
    const out = []
    for (let i = 0; i < B; i++) {
      const low = minP + i * size
      const high = low + size
      const vol = sums[i]
      const norm = vol / maxV
      out.push({ value: [anchorX, low, high, norm, vol] })
    }
    return out
  }

  const vpvrInitialStart = Math.max(0, dates.length - (isMonth ? 48 : (isWeek ? 80 : 120)))
  const vpvrInitialEnd = Math.max(0, dates.length - 1)
  const vpvrData = showVPVR.value ? computeVPVR(vpvrInitialStart, vpvrInitialEnd, vpvrBins) : null
  aiVisibleStartIdx.value = vpvrInitialStart
  aiVisibleEndIdx.value = vpvrInitialEnd
  
  let fibLevels = null
  if (showFib.value && Array.isArray(chartData.value) && chartData.value.length > 0) {
    let globalHigh = -Infinity
    let globalLow = Infinity
    for (const row of chartData.value) {
      const h = Number(row?.high ?? row?.[3])
      const l = Number(row?.low ?? row?.[2])
      if (Number.isFinite(h) && h > globalHigh) globalHigh = h
      if (Number.isFinite(l) && l < globalLow) globalLow = l
    }
    if (Number.isFinite(globalHigh) && Number.isFinite(globalLow) && globalHigh > globalLow) {
      const range = globalHigh - globalLow
      const ratios = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]
      fibLevels = ratios.map(r => ({
        ratio: r,
        value: globalHigh - range * r
      }))
    }
  }
  
  // Calculate MA lines (based on close price index=1)
  function calculateMA(data, dayCount) {
    const result = []
    for (let i = 0; i < data.length; i++) {
      if (i < dayCount - 1) {
        result.push('-')
        continue
      }
      let sum = 0
      for (let j = 0; j < dayCount; j++) {
        sum += data[i - j][1] // close price
      }
      result.push((sum / dayCount).toFixed(2))
    }
    return result
  }
  
  const ma1 = showMainK.value ? calculateMA(ohlc, maParams.value.ma1) : []
  const ma2 = showMainK.value ? calculateMA(ohlc, maParams.value.ma2) : []
  const ma3 = showMainK.value ? calculateMA(ohlc, maParams.value.ma3) : []
  const ma4 = showMainK.value ? calculateMA(ohlc, maParams.value.ma4) : []
  const ma5 = showMainK.value ? calculateMA(ohlc, maParams.value.ma5) : []
  // Simple slope direction for MA legend arrows: 1 = up, -1 = down, 0 = flat/undefined
  function getMaDirection(series) {
    if (!Array.isArray(series) || series.length < 2) return 0
    // Extract last two valid numeric values
    const vals = []
    for (let i = series.length - 1; i >= 0 && vals.length < 2; i--) {
      const v = series[i]
      if (v === '-' || v === null || v === undefined) continue
      const n = Number(v)
      if (!Number.isFinite(n)) continue
      vals.push(n)
    }
    if (vals.length < 2) return 0
    const last = vals[0]
    const prev = vals[1]
    const diff = last - prev
    const threshold = Math.max(0.0001, Math.abs(last) * 0.0005) // about 0.05%
    if (diff > threshold) return 1
    if (diff < -threshold) return -1
    return 0
  }

  const maDirMap = {
    [`MA${maParams.value.ma1}`]: getMaDirection(ma1),
    [`MA${maParams.value.ma2}`]: getMaDirection(ma2),
    [`MA${maParams.value.ma3}`]: getMaDirection(ma3),
    [`MA${maParams.value.ma4}`]: getMaDirection(ma4),
    [`MA${maParams.value.ma5}`]: getMaDirection(ma5),
  }

  // Last numeric value for each MA, used for legend display
  function getMaLastValue(series) {
    if (!Array.isArray(series) || !series.length) return null
    for (let i = series.length - 1; i >= 0; i--) {
      const v = series[i]
      if (v === '-' || v === null || v === undefined) continue
      const n = Number(v)
      if (!Number.isFinite(n)) continue
      return n
    }
    return null
  }

  const maLastMap = {
    [`MA${maParams.value.ma1}`]: getMaLastValue(ma1),
    [`MA${maParams.value.ma2}`]: getMaLastValue(ma2),
    [`MA${maParams.value.ma3}`]: getMaLastValue(ma3),
    [`MA${maParams.value.ma4}`]: getMaLastValue(ma4),
    [`MA${maParams.value.ma5}`]: getMaLastValue(ma5),
  }
  
  // Update MA legend state (value & direction) based on a specific index
  function getMaValueAt(series, idx) {
    if (!Array.isArray(series) || typeof idx !== 'number') return null
    if (idx < 0 || idx >= series.length) return null
    const v = series[idx]
    if (v === '-' || v === null || v === undefined) return null
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  }

  function getMaDirectionAt(series, idx) {
    const curr = getMaValueAt(series, idx)
    if (curr == null) return 0
    let prev = null
    for (let j = idx - 1; j >= 0; j--) {
      prev = getMaValueAt(series, j)
      if (prev != null) break
    }
    if (prev == null) return 0
    const currNum = Number(curr)
    const prevNum = Number(prev)
    const diff = currNum - prevNum
    const threshold = Math.max(0.0001, Math.abs(currNum) * 0.0005)
    if (diff > threshold) return 1
    if (diff < -threshold) return -1
    return 0
  }

  function updateMaLegendStateAt(idx) {
    const i = typeof idx === 'number' ? idx : (ohlc.length - 1)
    if (i < 0 || i >= ohlc.length) return

    const name1 = `MA${maParams.value.ma1}`
    const name2 = `MA${maParams.value.ma2}`
    const name3 = `MA${maParams.value.ma3}`
    const name4 = `MA${maParams.value.ma4}`
    const name5 = `MA${maParams.value.ma5}`

    const v1 = getMaValueAt(ma1, i)
    const v2 = getMaValueAt(ma2, i)
    const v3 = getMaValueAt(ma3, i)
    const v4 = getMaValueAt(ma4, i)
    const v5 = getMaValueAt(ma5, i)

    maLastMap[name1] = v1
    maLastMap[name2] = v2
    maLastMap[name3] = v3
    maLastMap[name4] = v4
    maLastMap[name5] = v5

    maDirMap[name1] = getMaDirectionAt(ma1, i)
    maDirMap[name2] = getMaDirectionAt(ma2, i)
    maDirMap[name3] = getMaDirectionAt(ma3, i)
    maDirMap[name4] = getMaDirectionAt(ma4, i)
    maDirMap[name5] = getMaDirectionAt(ma5, i)
  }
  // Calculate HMA indicator (based on close price index=1)
  function calculateWMA(arr, period) {
    const out = new Array(arr.length).fill(undefined)
    if (period <= 1) {
      for (let i = 0; i < arr.length; i++) out[i] = arr[i]
      return out
    }
    const weightSum = period * (period + 1) / 2
    for (let i = period - 1; i < arr.length; i++) {
      let acc = 0
      let valid = true
      for (let j = 0; j < period; j++) {
        const v = arr[i - j]
        if (typeof v !== 'number' || !isFinite(v)) { valid = false; break }
        acc += v * (period - j)
      }
      out[i] = valid ? acc / weightSum : undefined
    }
    return out
  }
  function calculateHMA(data, period) {
    const closes = data.map(d => d[1])
    const n = Math.max(2, Math.floor(period))
    const n2 = Math.max(1, Math.floor(n / 2))
    const ns = Math.max(1, Math.floor(Math.sqrt(n)))
    const wmaN = calculateWMA(closes, n)
    const wmaN2 = calculateWMA(closes, n2)
    const diff = closes.map((_, i) => {
      const a = wmaN2[i]
      const b = wmaN[i]
      return (a !== undefined && b !== undefined) ? (2 * a - b) : undefined
    })
    const hmaRaw = calculateWMA(diff, ns)
    const base = hmaRaw.map(v => v === undefined ? null : Number(v.toFixed(2)))

    // 依照斜率拆成「上漲段」與「非上漲段」兩個序列；轉折點同時放進兩段，避免分段線斷裂。
    const up = new Array(base.length).fill(null)
    const flatDown = new Array(base.length).fill(null)
    for (let i = 0; i < base.length; i++) {
      const v = base[i]
      if (typeof v !== 'number' || !Number.isFinite(v)) continue
      const prev = i > 0 ? base[i - 1] : null
      const rising = (typeof prev === 'number' && Number.isFinite(prev)) ? v > prev : false
      if (rising) {
        if (i > 0 && typeof prev === 'number' && Number.isFinite(prev)) up[i - 1] = prev
        up[i] = v
      } else {
        if (i > 0 && typeof prev === 'number' && Number.isFinite(prev)) flatDown[i - 1] = prev
        flatDown[i] = v
      }
    }

    return { base, up, flatDown }
  }
  function calculateTowerSeries(rows) {
    const n = Array.isArray(rows) ? rows.length : 0
    const red = new Array(n).fill(null)
    const green = new Array(n).fill(null)
    if (!n) return { red, green }

    const closes = rows.map(r => Number(r.close))
    const isFiniteNumber = (v) => typeof v === 'number' && Number.isFinite(v)
    if (!isFiniteNumber(closes[0])) return { red, green }

    let dir = 0 // 1: up (red), -1: down (green)
    let prevClose = closes[0]
    let swingHigh = prevClose
    let swingLow = prevClose
    // 需要比前一段高點/低點多突破一定比例才翻色，避免過於敏感
    // towerParams.buffer 以「百分比」儲存（例如 0.5、1、2），計算時轉成倍率
    const rawPercent = Number(towerParams.value.buffer)
    const pct = Number.isFinite(rawPercent) && rawPercent >= 0 ? rawPercent : 1
    const flipBufferRatio = pct / 100

    const assign = (target, idx, value) => {
      if (idx >= 0 && idx < target.length && isFiniteNumber(value)) {
        target[idx] = Number(Number(value).toFixed(2))
      }
    }

    for (let i = 1; i < n; i++) {
      const close = closes[i]
      if (!isFiniteNumber(close)) continue

      if (dir === 0) {
        dir = close >= prevClose ? 1 : -1
        swingHigh = Math.max(prevClose, close)
        swingLow = Math.min(prevClose, close)
        if (dir > 0) {
          assign(red, i - 1, prevClose)
          assign(red, i, close)
        } else {
          assign(green, i - 1, prevClose)
          assign(green, i, close)
        }
        prevClose = close
        continue
      }

      if (dir > 0) {
        if (close >= swingHigh) {
          swingHigh = close
          assign(red, i, close)
        } else if (close > swingLow) {
          // 回檔但尚未跌破前一段低點，維持多頭紅線
          swingLow = Math.min(swingLow, close)
          assign(red, i, close)
        } else {
          // 價格跌破前一段低點，僅當跌破幅度超過 flipBufferRatio 才翻為綠線
          const flipThreshold = swingLow * (1 - flipBufferRatio)
          if (close <= flipThreshold) {
            dir = -1
            swingLow = close
            swingHigh = close
            assign(green, i - 1, prevClose)
            assign(green, i, close)
          } else {
            // 跌破一點點但未達門檻，仍視為多頭段末端的回檔
            swingLow = Math.min(swingLow, close)
            assign(red, i, close)
          }
        }
      } else {
        if (close <= swingLow) {
          swingLow = close
          assign(green, i, close)
        } else if (close < swingHigh) {
          // 反彈但尚未突破前一段高點，維持空頭綠線
          swingHigh = Math.max(swingHigh, close)
          assign(green, i, close)
        } else {
          // 價格突破前一段高點，僅當突破幅度超過 flipBufferRatio 才翻為紅線
          const flipThreshold = swingHigh * (1 + flipBufferRatio)
          if (close >= flipThreshold) {
            dir = 1
            swingHigh = close
            swingLow = close
            assign(red, i - 1, prevClose)
            assign(red, i, close)
          } else {
            // 小幅突破但未達門檻，仍視為空頭段中的反彈
            swingHigh = Math.max(swingHigh, close)
            assign(green, i, close)
          }
        }
      }

      prevClose = close
    }

    if (dir >= 0 && red[0] == null) assign(red, 0, closes[0])
    if (dir <= 0 && green[0] == null) assign(green, 0, closes[0])

    return { red, green }
  }

  const showReversalLines = showReversal.value && (showReversalUp.value || showReversalDown.value)
  const hma = (showHMA.value || showReversalLines) ? calculateHMA(ohlc, hmaParams.value.period) : null
  function computeHmaTrendDirFromBase(baseArr) {
    if (!Array.isArray(baseArr) || baseArr.length < 2) return 0
    let i = baseArr.length - 1
    while (i >= 0) {
      const cur = baseArr[i]
      if (cur != null && Number.isFinite(Number(cur))) break
      i--
    }
    if (i <= 0) return 0
    const v = Number(baseArr[i])
    for (let j = i - 1; j >= 0; j--) {
      const x = baseArr[j]
      if (x != null && Number.isFinite(Number(x))) {
        const prev = Number(x)
        if (v > prev) return 1
        if (v < prev) return -1
        return 0
      }
    }
    return 0
  }
  const hmaTrendDirForColor =
    showHMA.value && hma && Array.isArray(hma.base) ? computeHmaTrendDirFromBase(hma.base) : 0
  const hmaIndData = showHMAInd.value ? calculateHMA(ohlc, hmaIndParams.value.period) : null
  const reversalData = showReversalLines ? (function calculateReversalLine(rows, boxSize = 0.5) {
    const n = Array.isArray(rows) ? rows.length : 0
    const red = new Array(n).fill(null)   // 上漲段（紅）
    const green = new Array(n).fill(null) // 下跌段（綠）
    if (!n) return { red, green }

    const closes = rows.map(r => Number(r.close))
    const quantize = (v) => (typeof v === 'number' && isFinite(v))
      ? Math.round(v / boxSize) * boxSize
      : null

    let base = quantize(closes[0])
    if (base == null) return { red, green }

    let dir = 0 // 1: up, -1: down
    let color = null // 'up' | 'down'
    let swingHigh = base
    let swingLow = base
    let bullThreshold = base
    let bearThreshold = base
    let started = false

    for (let i = 1; i < n; i++) {
      const currClose = closes[i]
      if (!Number.isFinite(currClose)) continue

      const c = quantize(currClose)
      if (c == null) continue

      if (!started) {
        // 尚未有明確方向，僅當價位脫離初始格價時才啟動
        if (c === base) {
          // 價格與初始格相同：暫不畫線
          continue
        }
        dir = c > base ? 1 : -1
        color = dir > 0 ? 'up' : 'down'
        if (dir > 0) {
          red[i - 1] = base
          red[i] = c
        } else {
          green[i - 1] = base
          green[i] = c
        }
        swingHigh = Math.max(base, c)
        swingLow = Math.min(base, c)
        bullThreshold = swingHigh
        bearThreshold = swingLow
        base = c
        started = true
        continue
      }

      if (dir > 0) {
        // 多頭段
        if (c >= base) {
          base = c
          if (c > swingHigh) {
            swingHigh = c
            bullThreshold = swingHigh
          }
        } else {
          base = c
          if (c < swingLow) {
            swingLow = c
            bearThreshold = swingLow
          }
          if (c <= bearThreshold) {
            // 翻為空頭段
            dir = -1
            color = 'down'
          }
        }
      } else if (dir < 0) {
        // 空頭段
        if (c <= base) {
          base = c
          if (c < swingLow) {
            swingLow = c
            bearThreshold = swingLow
          }
        } else {
          base = c
          if (c > swingHigh) {
            swingHigh = c
            bullThreshold = swingHigh
          }
          if (c >= bullThreshold) {
            // 翻為多頭段
            dir = 1
            color = 'up'
          }
        }
      }

      if (color === 'up') {
        red[i] = base
      } else if (color === 'down') {
        green[i] = base
      }
    }

    return { red, green }
  })(chartData.value, 0.5) : null

  const reversalColored = (showReversalLines && reversalData) ? (function buildReversalColored(data, hmaData) {
    const n = Math.max(data.red.length, data.green.length)
    const up = new Array(n).fill(null)
    const down = new Array(n).fill(null)
    const hasHMA = showHMA.value && hmaData && Array.isArray(hmaData.base)
    for (let i = 0; i < n; i++) {
      const rRed = data.red[i]
      const rGreen = data.green[i]
      let v = null
      if (typeof rRed === 'number' && Number.isFinite(rRed)) {
        v = rRed
      } else if (typeof rGreen === 'number' && Number.isFinite(rGreen)) {
        v = rGreen
      }
      if (v == null) continue
      if (hasHMA) {
        const hv = hmaData.base[i]
        if (typeof hv === 'number' && Number.isFinite(hv)) {
          if (v >= hv) {
            up[i] = v
          } else {
            down[i] = v
          }
          continue
        }
      }
      if (typeof rRed === 'number' && Number.isFinite(rRed)) {
        up[i] = rRed
      }
      if (typeof rGreen === 'number' && Number.isFinite(rGreen)) {
        down[i] = rGreen
      }
    }
    return { up, down }
  })(reversalData, hma) : null

  // 轉折線多空訊號：
  // 綠 -> 紅：紅色向上三角形（轉強）
  // 紅 -> 綠：綠色向下三角形（轉弱）
  const reversalSignals = null

  function calculateCCI(rows, period = 5) {
    const n = Math.max(1, Math.floor(period || 5))
    if (!Array.isArray(rows) || !rows.length) return []
    const len = rows.length
    const tp = new Array(len)
    for (let i = 0; i < len; i++) {
      const r = rows[i]
      const h = Number(r?.high ?? r?.[3])
      const l = Number(r?.low ?? r?.[2])
      const c = Number(r?.close ?? r?.[1])
      if (!Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(c)) {
        tp[i] = null
      } else {
        tp[i] = (h + l + c) / 3
      }
    }
    const result = []
    for (let i = 0; i < len; i++) {
      const curTp = tp[i]
      if (i < n - 1 || curTp == null) {
        result.push('-')
        continue
      }
      let sumTp = 0
      let valid = true
      for (let j = 0; j < n; j++) {
        const v = tp[i - j]
        if (v == null) {
          valid = false
          break
        }
        sumTp += v
      }
      if (!valid) {
        result.push('-')
        continue
      }
      const ma = sumTp / n
      let sumDev = 0
      for (let j = 0; j < n; j++) {
        const v = tp[i - j]
        sumDev += Math.abs(v - ma)
      }
      const md = sumDev / n
      if (!Number.isFinite(md) || md === 0) {
        result.push('-')
        continue
      }
      const cci = (curTp - ma) / (0.015 * md)
      result.push(Number(cci.toFixed(2)))
    }
    return result
  }

  // RSI（Wilder 平滑法，與後端 computeRsiSnapshot 相同演算法）
  function calculateRSI(rows, period = 14) {
    const n = Math.max(2, Math.floor(period || 14))
    if (!Array.isArray(rows) || !rows.length) return []
    const len = rows.length
    const closes = new Array(len)
    for (let i = 0; i < len; i++) {
      const r = rows[i]
      const c = Number(r?.close ?? r?.[1])
      closes[i] = Number.isFinite(c) ? c : null
    }
    const result = new Array(len).fill('-')
    if (len < n + 1 || closes.some((c) => c == null)) return result

    const rsiAt = (avgG, avgL) => {
      if (avgL === 0) return avgG === 0 ? 50 : 100
      if (avgG === 0) return 0
      const rs = avgG / avgL
      return Number((100 - 100 / (1 + rs)).toFixed(2))
    }

    let avgGain = 0
    let avgLoss = 0
    for (let i = 1; i <= n; i++) {
      const ch = closes[i] - closes[i - 1]
      if (ch >= 0) avgGain += ch
      else avgLoss -= ch
    }
    avgGain /= n
    avgLoss /= n
    result[n] = rsiAt(avgGain, avgLoss)

    for (let i = n + 1; i < len; i++) {
      const ch = closes[i] - closes[i - 1]
      const gain = ch > 0 ? ch : 0
      const loss = ch < 0 ? -ch : 0
      avgGain = (avgGain * (n - 1) + gain) / n
      avgLoss = (avgLoss * (n - 1) + loss) / n
      result[i] = rsiAt(avgGain, avgLoss)
    }
    return result
  }

  // 布林通道：中軌=SMA，帶寬=群體標準差（除以 n，與後端 computeBbSnapshot 相同）
  function calculateBollinger(rows, period = 20, mult = 2) {
    const n = Math.max(2, Math.floor(period || 20))
    const kRaw = Number(mult)
    const k = Number.isFinite(kRaw) && kRaw > 0 ? kRaw : 2
    const len = Array.isArray(rows) ? rows.length : 0
    const mid = new Array(len).fill('-')
    const upper = new Array(len).fill('-')
    const lower = new Array(len).fill('-')
    const bandWidth = new Array(len).fill('-')
    if (!len) return { mid, upper, lower, bandWidth }

    const closes = new Array(len)
    for (let i = 0; i < len; i++) {
      const r = rows[i]
      const c = Number(r?.close ?? r?.[1])
      closes[i] = Number.isFinite(c) ? c : null
    }

    for (let i = n - 1; i < len; i++) {
      let sum = 0
      let valid = true
      for (let j = 0; j < n; j++) {
        const c = closes[i - j]
        if (c == null) { valid = false; break }
        sum += c
      }
      if (!valid) continue
      const mean = sum / n
      let variance = 0
      for (let j = 0; j < n; j++) {
        variance += (closes[i - j] - mean) ** 2
      }
      variance /= n
      const std = Math.sqrt(variance)
      const up = mean + k * std
      const lo = mean - k * std
      mid[i] = Number(mean.toFixed(4))
      upper[i] = Number(up.toFixed(4))
      lower[i] = Number(lo.toFixed(4))
      bandWidth[i] = Number((up - lo).toFixed(4))
    }
    return { mid, upper, lower, bandWidth }
  }

  // Calculate KD indicator
  function calculateKD(data, n = 9, m1 = 3, m2 = 3) {
    const k = []
    const d = []
    const rsv = []
    
    for (let i = 0; i < data.length; i++) {
      if (i < n - 1) {
        rsv.push('-')
        k.push('-')
        d.push('-')
        continue
      }
      
      // Find lowest low and highest high in the period
      let lowestLow = Infinity
      let highestHigh = -Infinity
      for (let j = 0; j < n; j++) {
        const candle = data[i - j]
        lowestLow = Math.min(lowestLow, candle[2])  // low
        highestHigh = Math.max(highestHigh, candle[3])  // high
      }
      
      // Calculate RSV
      const close = data[i][1]
      const rsvValue = highestHigh === lowestLow ? 0 : ((close - lowestLow) / (highestHigh - lowestLow)) * 100
      rsv.push(rsvValue)
      
      // Calculate K (smoothed RSV)
      const prevK = k.length > 0 && k[k.length - 1] !== '-' ? k[k.length - 1] : 50
      const kValue = (prevK * (m1 - 1) + rsvValue) / m1
      k.push(kValue)
      
      // Calculate D (smoothed K)
      const prevD = d.length > 0 && d[d.length - 1] !== '-' ? d[d.length - 1] : 50
      const dValue = (prevD * (m2 - 1) + kValue) / m2
      d.push(dValue)
    }
    
    return { k, d }
  }
  
  // Calculate MACD indicator
  function calculateMACD(data, fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
    const closes = data.map(d => d[1])  // close price
    
    // Calculate EMA
    function calculateEMA(prices, period) {
      const ema = []
      const multiplier = 2 / (period + 1)
      
      for (let i = 0; i < prices.length; i++) {
        if (i < period - 1) {
          ema.push('-')
          continue
        }
        
        if (i === period - 1) {
          // First EMA is SMA
          let sum = 0
          for (let j = 0; j < period; j++) {
            sum += prices[i - j]
          }
          ema.push(sum / period)
        } else {
          // EMA = (Close - EMA(previous day)) × multiplier + EMA(previous day)
          const prevEMA = ema[ema.length - 1]
          ema.push((prices[i] - prevEMA) * multiplier + prevEMA)
        }
      }
      
      return ema
    }
    
    const fastEMA = calculateEMA(closes, fastPeriod)
    const slowEMA = calculateEMA(closes, slowPeriod)
    
    // Calculate MACD line
    const macdLine = []
    for (let i = 0; i < closes.length; i++) {
      if (fastEMA[i] === '-' || slowEMA[i] === '-') {
        macdLine.push('-')
      } else {
        macdLine.push(fastEMA[i] - slowEMA[i])
      }
    }
    
    // Calculate Signal line (EMA of MACD)
    const signal = []
    const validMacd = []
    let firstValidIndex = 0
    
    for (let i = 0; i < macdLine.length; i++) {
      if (macdLine[i] !== '-') {
        validMacd.push(macdLine[i])
        if (validMacd.length === 1) firstValidIndex = i
        
        if (validMacd.length < signalPeriod) {
          signal.push('-')
        } else if (validMacd.length === signalPeriod) {
          const sum = validMacd.reduce((a, b) => a + b, 0)
          signal.push(sum / signalPeriod)
        } else {
          const multiplier = 2 / (signalPeriod + 1)
          const prevSignal = signal[signal.length - 1]
          signal.push((macdLine[i] - prevSignal) * multiplier + prevSignal)
        }
      } else {
        signal.push('-')
      }
    }
    
    // Calculate histogram
    const histogram = []
    for (let i = 0; i < macdLine.length; i++) {
      if (macdLine[i] === '-' || signal[i] === '-') {
        histogram.push('-')
      } else {
        histogram.push(macdLine[i] - signal[i])
      }
    }
    
  
  return { macd: macdLine, signal, histogram }
  }

  // Calculate Expert Indicator (行家指標)
  // Q = (2*Close + High + Low) / 4
  // A1 = Lowest(Low, Length), A2 = Highest(High, Length)
  // A0 = EMA((Q - A1) / (A2 - A1) * 100, 13)
  // B0 = EMA(0.667 * A0[prev] + 0.333 * A0, 3)
  // Value1 = A0 - B0
  function calculateExpert(data, length = 13) {
    const n = data.length
    const a0 = new Array(n).fill('-')
    const b0 = new Array(n).fill('-')
    const value1 = new Array(n).fill('-')
    const crossOver = new Array(n).fill(false)
    const crossUnder = new Array(n).fill(false)

    if (n < length) return { a0, b0, value1, crossOver, crossUnder }

    // Step 1: Calculate Q, A1 (lowest low), A2 (highest high)
    const q = []
    const a1 = []
    const a2 = []
    for (let i = 0; i < n; i++) {
      const close = data[i][1]
      const low = data[i][2]
      const high = data[i][3]
      q.push((2 * close + high + low) / 4)

      if (i < length - 1) {
        a1.push('-')
        a2.push('-')
      } else {
        let lowestLow = Infinity
        let highestHigh = -Infinity
        for (let j = 0; j < length; j++) {
          lowestLow = Math.min(lowestLow, data[i - j][2])
          highestHigh = Math.max(highestHigh, data[i - j][3])
        }
        a1.push(lowestLow)
        a2.push(highestHigh)
      }
    }

    // Step 2: Calculate raw RSV-like value: (Q - A1) / (A2 - A1) * 100
    const rawRsv = []
    for (let i = 0; i < n; i++) {
      if (a1[i] === '-' || a2[i] === '-') {
        rawRsv.push('-')
      } else {
        const range = a2[i] - a1[i]
        if (range === 0) {
          rawRsv.push(50)
        } else {
          rawRsv.push(((q[i] - a1[i]) / range) * 100)
        }
      }
    }

    // Step 3: Calculate A0 = EMA(rawRsv, 13)
    const emaPeriod1 = 13
    const k1 = 2 / (emaPeriod1 + 1)
    let emaA0 = null
    for (let i = 0; i < n; i++) {
      if (rawRsv[i] === '-') {
        a0[i] = '-'
        continue
      }
      if (emaA0 === null) {
        emaA0 = rawRsv[i]
      } else {
        emaA0 = rawRsv[i] * k1 + emaA0 * (1 - k1)
      }
      a0[i] = emaA0
    }

    // Step 4: Calculate B0 = EMA(0.667 * A0[prev] + 0.333 * A0, 3)
    const emaPeriod2 = 3
    const k2 = 2 / (emaPeriod2 + 1)
    let emaB0 = null
    for (let i = 0; i < n; i++) {
      if (a0[i] === '-') {
        b0[i] = '-'
        continue
      }
      const prevA0 = i > 0 && a0[i - 1] !== '-' ? a0[i - 1] : a0[i]
      const smoothedInput = 0.667 * prevA0 + 0.333 * a0[i]
      if (emaB0 === null) {
        emaB0 = smoothedInput
      } else {
        emaB0 = smoothedInput * k2 + emaB0 * (1 - k2)
      }
      b0[i] = emaB0
    }

    // Step 5: Calculate Value1 = A0 - B0 and detect crossovers
    for (let i = 0; i < n; i++) {
      if (a0[i] === '-' || b0[i] === '-') {
        value1[i] = '-'
        continue
      }
      value1[i] = a0[i] - b0[i]

      // Detect crossover/crossunder
      if (i > 0 && a0[i - 1] !== '-' && b0[i - 1] !== '-') {
        const prevDiff = a0[i - 1] - b0[i - 1]
        const currDiff = a0[i] - b0[i]
        if (prevDiff <= 0 && currDiff > 0) {
          crossOver[i] = true
        }
        if (prevDiff >= 0 && currDiff < 0) {
          crossUnder[i] = true
        }
      }
    }

    return { a0, b0, value1, crossOver, crossUnder }
  }

  // 黃金波段指標計算函數
  // DIF = MACD(Close, 快速均線, 慢速均線)
  // DIFSlow = MACD(Close, 快速均線二, 慢速均線二)
  // DIFMa2 = EMA(DIF, 多空均線)
  // DIFDev2 = StdDev(DIF, 多空均線)
  // DIFMa1, DIFMa3, DIFSub, DIFCross, DIFDir
  function calculateGoldenWave(data, params) {
    const { fastMa, slowMa, fastMa2, slowMa2, multiMa, waveMa2, waveMa3, boxPeriod } = params
    const n = data.length
    
    // 輔助函數：計算 EMA
    function calcEMA(arr, period) {
      const result = new Array(n).fill('-')
      const k = 2 / (period + 1)
      let ema = null
      for (let i = 0; i < n; i++) {
        const val = Number(arr[i])
        if (!Number.isFinite(val)) continue
        if (ema === null) {
          ema = val
        } else {
          ema = val * k + ema * (1 - k)
        }
        result[i] = ema
      }
      return result
    }

    // 輔助函數：計算標準差
    function calcStdDev(arr, period) {
      const result = new Array(n).fill('-')
      for (let i = period - 1; i < n; i++) {
        let sum = 0, count = 0
        for (let j = i - period + 1; j <= i; j++) {
          const v = Number(arr[j])
          if (Number.isFinite(v)) {
            sum += v
            count++
          }
        }
        if (count < period) continue
        const mean = sum / count
        let variance = 0
        for (let j = i - period + 1; j <= i; j++) {
          const v = Number(arr[j])
          if (Number.isFinite(v)) {
            variance += Math.pow(v - mean, 2)
          }
        }
        result[i] = Math.sqrt(variance / count)
      }
      return result
    }

    // 輔助函數：計算 Highest
    function calcHighest(arr, period) {
      const result = new Array(n).fill('-')
      for (let i = period - 1; i < n; i++) {
        let max = -Infinity
        for (let j = i - period + 1; j <= i; j++) {
          const v = Number(arr[j])
          if (Number.isFinite(v) && v > max) {
            max = v
          }
        }
        result[i] = max === -Infinity ? '-' : max
      }
      return result
    }

    // 輔助函數：計算 Lowest
    function calcLowest(arr, period) {
      const result = new Array(n).fill('-')
      for (let i = period - 1; i < n; i++) {
        let min = Infinity
        for (let j = i - period + 1; j <= i; j++) {
          const v = Number(arr[j])
          if (Number.isFinite(v) && v < min) {
            min = v
          }
        }
        result[i] = min === Infinity ? '-' : min
      }
      return result
    }

    // 取得收盤價 (ohlc 格式: [open, close, low, high])
    const closes = data.map(d => {
      const val = Array.isArray(d) ? d[1] : d.close
      return Number(val)
    })

    // 計算 DIF (MACD 的 DIF 線) = EMA(close, fast) - EMA(close, slow)
    const emaFast = calcEMA(closes, fastMa)
    const emaSlow = calcEMA(closes, slowMa)
    const dif = new Array(n).fill('-')
    for (let i = 0; i < n; i++) {
      const f = Number(emaFast[i])
      const s = Number(emaSlow[i])
      if (Number.isFinite(f) && Number.isFinite(s)) {
        dif[i] = f - s
      }
    }

    // 計算 DIFSlow = EMA(close, fast2) - EMA(close, slow2)
    const emaFast2 = calcEMA(closes, fastMa2)
    const emaSlow2 = calcEMA(closes, slowMa2)
    const difSlow = new Array(n).fill('-')
    for (let i = 0; i < n; i++) {
      const f2 = Number(emaFast2[i])
      const s2 = Number(emaSlow2[i])
      if (Number.isFinite(f2) && Number.isFinite(s2)) {
        difSlow[i] = f2 - s2
      }
    }

    // DIFMa = EMA(DIF, multiMa) - 波段均線一
    const difMa = calcEMA(dif, multiMa)

    // DIFDev = StdDev(DIFMa, multiMa)
    const difDev = calcStdDev(difMa, multiMa)

    // UpperDIF / LowerDIF
    const upperDif = new Array(n).fill('-')
    const lowerDif = new Array(n).fill('-')
    for (let i = 0; i < n; i++) {
      const m = Number(difMa[i])
      const dev = Number(difDev[i])
      if (!Number.isFinite(m) || !Number.isFinite(dev)) continue
      upperDif[i] = m + dev * 2
      lowerDif[i] = m - dev * 2
    }

    // 波段均線二 / 三
    const difMa2 = calcEMA(dif, waveMa2)
    const difMa3 = calcEMA(dif, waveMa3)

    // DIFSub = Abs(DIF - DIFSlow)
    const difSub = new Array(n).fill('-')
    for (let i = 0; i < n; i++) {
      const d = Number(dif[i])
      const ds = Number(difSlow[i])
      if (Number.isFinite(d) && Number.isFinite(ds)) {
        difSub[i] = Math.abs(d - ds)
      }
    }

    const uppestDif = calcHighest(dif, boxPeriod)
    const lowestDif = calcLowest(dif, boxPeriod)

    return {
      emaFast,
      emaSlow,
      dif,
      difSlow,
      difMa,
      difDev,
      upperDif,
      lowerDif,
      difSub,
      difMa2,
      difMa3,
      uppestDif,
      lowestDif,
    }
  }
  
  const cciData = showCCI.value ? calculateCCI(chartData.value, cciParams.value.period) : null
  const cciSegments = cciData && Array.isArray(cciData) ? (() => {
    const below = []
    const mid = []
    const above = []
    for (let i = 0; i < cciData.length; i++) {
      const raw = cciData[i]
      const num = Number(raw)
      if (raw === '-' || raw === null || raw === undefined || !Number.isFinite(num)) {
        below.push('-')
        mid.push('-')
        above.push('-')
        continue
      }
      if (num <= 0) {
        below.push(num)
        mid.push('-')
        above.push('-')
      } else if (num < 100) {
        below.push('-')
        mid.push(num)
        above.push('-')
      } else {
        below.push('-')
        mid.push('-')
        above.push(num)
      }
    }
    return { below, mid, above }
  })() : null
  const rsiData = showRSI.value ? calculateRSI(chartData.value, rsiParams.value.period) : null
  const bbData = showBB.value ? calculateBollinger(chartData.value, bbParams.value.period, bbParams.value.mult) : null
  const kdData = showKD.value ? calculateKD(ohlc, kdParams.value.period, kdParams.value.k, kdParams.value.d) : null
  const macdData = showMACD.value ? calculateMACD(ohlc, macdParams.value.fast, macdParams.value.slow, macdParams.value.signal) : null
  const m4MomentumData = showM4Momentum.value
    ? calculateMACD(ohlc, m4MomentumParams.value.fast, m4MomentumParams.value.slow, m4MomentumParams.value.signal)
    : null
  const expertData = showExpert.value ? calculateExpert(ohlc, expertParams.value.length) : null
  let goldenWaveData = null
  if (showGoldenWave.value) {
    try {
      goldenWaveData = calculateGoldenWave(ohlc, goldenWaveParams.value)
    } catch (e) {
      console.error('[GoldenWave] calculate failed:', e)
      goldenWaveData = null
    }
  }
  // 純視覺用倍率：放大 MACD 柱狀圖高度，不改變數據計算與文字顯示
  // 改為自適應倍率，避免柱體過高把 DIF / MACD 線擠在一起。
  const macdLineAbsMax = macdData
    ? [...macdData.macd, ...macdData.signal]
        .filter(v => v !== '-' && v !== null && v !== undefined)
        .map(v => Math.abs(Number(v)))
        .filter(v => Number.isFinite(v))
        .reduce((max, v) => Math.max(max, v), 0)
    : 0
  const macdHistAbsMax = macdData
    ? macdData.histogram
        .filter(v => v !== '-' && v !== null && v !== undefined)
        .map(v => Math.abs(Number(v)))
        .filter(v => Number.isFinite(v))
        .reduce((max, v) => Math.max(max, v), 0)
    : 0
  const macdHistVisualScale = (() => {
    if (!(macdHistAbsMax > 0)) return 1
    if (!(macdLineAbsMax > 0)) return Math.max(0.2, Math.min(10, Number(macdHistHeight.value) || 0.7))
    const targetRatio = Math.max(0.2, Math.min(10, Number(macdHistHeight.value) || 0.7))
    const adaptive = (macdLineAbsMax * targetRatio) / macdHistAbsMax
    return Math.max(1, Math.min(10, adaptive))
  })()
  const m4HistVisualScale = 6

  const poc = (() => {
    const arr = Array.isArray(vpvrData) ? vpvrData : null
    if (!arr || !arr.length) return null
    let best = null
    for (const row of arr) {
      const vol = Number(row?.value?.[4])
      if (!Number.isFinite(vol)) continue
      if (!best || vol > best.vol) {
        best = {
          low: Number(row?.value?.[1]),
          high: Number(row?.value?.[2]),
          vol,
        }
      }
    }
    if (!best || !Number.isFinite(best.low) || !Number.isFinite(best.high)) return null
    return best
  })()

  const idxLast = Math.max(0, ohlc.length - 1)
  const closeLast = Number(ohlc[idxLast]?.[1])
  const prevClose = idxLast > 0 ? Number(ohlc[idxLast - 1]?.[1]) : NaN
  const changePct = (Number.isFinite(closeLast) && Number.isFinite(prevClose) && prevClose !== 0)
    ? ((closeLast - prevClose) / prevClose) * 100
    : null

  const kdLastK = kdData && Array.isArray(kdData.k) ? Number(kdData.k[idxLast]) : null
  const kdLastD = kdData && Array.isArray(kdData.d) ? Number(kdData.d[idxLast]) : null
  const macdLast = macdData && Array.isArray(macdData.macd) ? Number(macdData.macd[idxLast]) : null
  const macdSignalLast = macdData && Array.isArray(macdData.signal) ? Number(macdData.signal[idxLast]) : null
  const macdHistLast = macdData && Array.isArray(macdData.histogram) ? Number(macdData.histogram[idxLast]) : null
  const hmaLast = hma && Array.isArray(hma.base) ? Number(hma.base[idxLast]) : null
  const hmaPrev = hma && Array.isArray(hma.base) && idxLast > 0 ? Number(hma.base[idxLast - 1]) : null
  const hmaDir = (Number.isFinite(hmaLast) && Number.isFinite(hmaPrev)) ? (hmaLast > hmaPrev ? 1 : (hmaLast < hmaPrev ? -1 : 0)) : null

  aiSnapshot.value = {
    asOf: dates[idxLast] ?? null,
    visibleRange: {
      startIdx: aiVisibleStartIdx.value,
      endIdx: aiVisibleEndIdx.value,
      startDate: dates[aiVisibleStartIdx.value] ?? null,
      endDate: dates[aiVisibleEndIdx.value] ?? null,
    },
    price: {
      close: Number.isFinite(closeLast) ? closeLast : null,
      changePct: Number.isFinite(changePct) ? Number(changePct.toFixed(2)) : null,
    },
    ma: {
      values: { ...maLastMap },
      dirs: { ...maDirMap },
      periods: { ...maParams.value },
    },
    kd: {
      enabled: Boolean(showKD.value),
      period: kdParams.value?.period ?? null,
      k: Number.isFinite(kdLastK) ? kdLastK : null,
      d: Number.isFinite(kdLastD) ? kdLastD : null,
    },
    macd: {
      enabled: Boolean(showMACD.value),
      fast: macdParams.value?.fast ?? null,
      slow: macdParams.value?.slow ?? null,
      signal: macdParams.value?.signal ?? null,
      macd: Number.isFinite(macdLast) ? macdLast : null,
      signalLine: Number.isFinite(macdSignalLast) ? macdSignalLast : null,
      histogram: Number.isFinite(macdHistLast) ? macdHistLast : null,
    },
    bullBearLine: {
      label: '多空線',
      enabled: Boolean(showHMA.value),
      period: hmaParams.value?.period ?? null,
      value: Number.isFinite(hmaLast) ? hmaLast : null,
      dir: hmaDir,
    },
    vpvr: {
      enabled: Boolean(showVPVR.value),
      bins: vpvrBins,
      poc,
    },
  }
  const macdHistogramVisual = macdData
    ? macdData.histogram.map(v => {
        if (v === '-' || v === null || v === undefined) return v
        const num = Number(v)
        return Number.isFinite(num) ? num * macdHistVisualScale : v
      })
    : null

  // 4 狀態：上漲(紅)、下跌(黑)、盤整(綠)、收斂(淺藍)
  const m4RegimeData = (() => {
    if (!ohlc || !ohlc.length) return null

    const len = ohlc.length
    const maPeriodRaw = Number(m4MomentumParams.value?.slow)
    const maPeriod = Math.max(2, Math.floor(Number.isFinite(maPeriodRaw) ? maPeriodRaw : 20))
    const emaPeriodRaw = Number(m4MomentumParams.value?.fast)
    const emaPeriod = Math.max(1, Math.floor(Number.isFinite(emaPeriodRaw) ? emaPeriodRaw : 8))
    const expandThRaw = Number(m4MomentumParams.value?.minChange)
    const expandTh = (() => {
      const v = Number.isFinite(expandThRaw) ? expandThRaw : 0
      // minChange 以「百分比點」解讀：例如 0.02 表示 |dist| 變化超過 0.02% 就視為拉大
      // 若使用者設 0（或太小），給一個更合理的預設，避免永遠只剩綠色
      if (!(v > 0)) return 0.02
      return Math.max(0.005, v)
    })()

    const ma = calculateMA(ohlc, maPeriod)
    const dist = new Array(len).fill(null)
    for (let i = 0; i < len; i++) {
      const maRaw = ma[i]
      if (maRaw === '-' || maRaw === null || maRaw === undefined) continue
      const maNum = Number(maRaw)
      const close = Number(ohlc[i]?.[1])
      if (!Number.isFinite(maNum) || maNum === 0 || !Number.isFinite(close)) continue
      dist[i] = ((close - maNum) / maNum) * 100
    }

    const k = 2 / (emaPeriod + 1)
    const distEma = new Array(len).fill(null)
    let ema = null
    for (let i = 0; i < len; i++) {
      const d = dist[i]
      if (!Number.isFinite(d)) {
        distEma[i] = ema
        continue
      }
      ema = ema === null ? d : d * k + ema * (1 - k)
      distEma[i] = ema
    }

    const medianAbsDist = (() => {
      const arr = distEma
        .filter(v => Number.isFinite(v))
        .map(v => Math.abs(Number(v)))
        .sort((a, b) => a - b)
      if (!arr.length) return 0
      const mid = Math.floor(arr.length / 2)
      return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2
    })()
    const squeezeThAuto = Math.max(0.03, medianAbsDist ? medianAbsDist * 0.6 : 0.2)
    const trendThAuto = Math.max(squeezeThAuto * 2.2, medianAbsDist ? medianAbsDist * 1.4 : 0.6)
    const squeezeTh = m4MomentumParams.value?.autoSqueeze === false
      ? Math.max(0.01, Number(m4MomentumParams.value?.squeezeTh) || squeezeThAuto)
      : squeezeThAuto
    const trendTh = m4MomentumParams.value?.autoTrend === false
      ? Math.max(squeezeTh * 1.05, Number(m4MomentumParams.value?.trendTh) || trendThAuto)
      : trendThAuto

    const colorRange = (m4MomentumParams.value?.colorRange && String(m4MomentumParams.value.colorRange)) || '#1d4ed8'
    const colorSqueeze = (m4MomentumParams.value?.colorSqueeze && String(m4MomentumParams.value.colorSqueeze)) || '#38bdf8'
    const colorUp = (m4MomentumParams.value?.colorUp && String(m4MomentumParams.value.colorUp)) || '#ef4444'
    const colorDown = (m4MomentumParams.value?.colorDown && String(m4MomentumParams.value.colorDown)) || '#22c55e'

    const out = []
    let lastColor = colorRange
    let lastFiniteD = 0
    for (let i = 0; i < len; i++) {
      const d = distEma[i]
      if (!Number.isFinite(d)) {
        // 均線尚未形成或缺值：沿用上一個顏色，避免合併區段後出現透明洞
        out.push({ value: lastFiniteD, itemStyle: { color: lastColor } })
        continue
      }
      lastFiniteD = d
      const absD = Math.abs(d)
      const prev = i > 0 ? distEma[i - 1] : null
      const prevAbs = Number.isFinite(prev) ? Math.abs(prev) : absD
      const expanding = (absD - prevAbs) > expandTh

      let color = colorRange
      if (absD < squeezeTh) {
        color = colorSqueeze
      } else if ((expanding || absD > trendTh) && d > 0) {
        color = colorUp
      } else if ((expanding || absD > trendTh) && d < 0) {
        color = colorDown
      }
      lastColor = color
      out.push({ value: Number(d.toFixed(3)), itemStyle: { color } })
    }
    return out
  })()

  const m4LineValues = m4RegimeData
    ? m4RegimeData.map(v => (v && typeof v === 'object' && 'value' in v) ? Number(v.value) : Number(v))
    : null

  const m4LineByState = (() => {
    if (!m4RegimeData || !m4RegimeData.length || !m4LineValues) return null
    const colorRange = (m4MomentumParams.value?.colorRange && String(m4MomentumParams.value.colorRange)) || '#1d4ed8'
    const colorSqueeze = (m4MomentumParams.value?.colorSqueeze && String(m4MomentumParams.value.colorSqueeze)) || '#38bdf8'
    const colorUp = (m4MomentumParams.value?.colorUp && String(m4MomentumParams.value.colorUp)) || '#ef4444'
    const colorDown = (m4MomentumParams.value?.colorDown && String(m4MomentumParams.value.colorDown)) || '#22c55e'

    const up = new Array(m4LineValues.length).fill('-')
    const down = new Array(m4LineValues.length).fill('-')
    const range = new Array(m4LineValues.length).fill('-')
    const squeeze = new Array(m4LineValues.length).fill('-')
    const stateAt = (c) => {
      if (c === colorUp) return 'up'
      if (c === colorDown) return 'down'
      if (c === colorSqueeze) return 'squeeze'
      return 'range'
    }
    const put = (state, idx, value) => {
      if (state === 'up') up[idx] = value
      else if (state === 'down') down[idx] = value
      else if (state === 'squeeze') squeeze[idx] = value
      else range[idx] = value
    }

    let prevState = null
    for (let i = 0; i < m4LineValues.length; i++) {
      const v = m4LineValues[i]
      if (!Number.isFinite(v)) continue
      const c = m4RegimeData[i]?.itemStyle?.color
      const state = stateAt(c)
      put(state, i, v)

      // Seamless join between colors: duplicate boundary points
      if (i > 0 && prevState && prevState !== state) {
        const pv = m4LineValues[i - 1]
        if (Number.isFinite(pv)) {
          put(state, i - 1, pv)
          put(prevState, i, v)
        }
      }
      prevState = state
    }
    return { up, down, range, squeeze, colorUp, colorDown, colorRange, colorSqueeze }
  })()

  const m4RegimeSegments = (() => {
    if (!m4RegimeData || !m4RegimeData.length) return null
    const segs = []
    let start = 0
    let currentColor = null
    for (let i = 0; i < m4RegimeData.length; i++) {
      const row = m4RegimeData[i]
      const c = (row && typeof row === 'object' && row.itemStyle && row.itemStyle.color)
        ? row.itemStyle.color
        : '#22c55e'
      if (i === 0) {
        start = 0
        currentColor = c
        continue
      }
      if (c !== currentColor) {
        segs.push([start, i - 1, currentColor])
        start = i
        currentColor = c
      }
    }
    if (currentColor != null) {
      segs.push([start, m4RegimeData.length - 1, currentColor])
    }
    return segs
  })()
  
  // Debug: Log MACD data availability
  if (macdData && showMACD.value) {
    const validMacdCount = macdData.macd.filter(v => v !== '-').length
    const validSignalCount = macdData.signal.filter(v => v !== '-').length
    const totalDataPoints = ohlc.length
    const requiredPoints = macdParams.value.slow + macdParams.value.signal
    if (totalDataPoints < requiredPoints) {
      console.warn(`⚠️ MACD 參數過大：需要至少 ${requiredPoints} 個數據點，但只有 ${totalDataPoints} 個`)
    }
  }
  
  // Unified MACD scale for all periods (day/week/month): symmetric around 0
  let macdScale = 1
  if (macdData) {
    const visualHistAbsMax = macdHistAbsMax * Math.max(1, Number(macdHistVisualScale) || 1)
    const lineDrivenScale = macdLineAbsMax > 0 ? macdLineAbsMax * 1.9 : 0
    const histDrivenScale = visualHistAbsMax > 0 ? visualHistAbsMax * 1.18 : 0
    const protectedHistScale = macdLineAbsMax > 0
      ? macdLineAbsMax * Math.max(1.6, Math.min(4.2, (Number(macdHistHeight.value) || 0.7) * 1.18 + 0.95))
      : histDrivenScale
    const absMax = Math.max(1, lineDrivenScale, Math.min(histDrivenScale, protectedHistScale))
    macdScale = absMax
  }

  // Pixel-based layout to prevent subplot overlap
  const container = chartContainer.value
  const containerWidth = container?.clientWidth || 0
  const gapPx = 18
  const legendBandPx = 22
  // 主圖即使關閉 K 線，也可能仍需要顯示主圖疊加物（多空線/VPVR/黃金切割/斜線支撐壓力等）
  // 若主圖 grid 不存在，後續子圖的 gridIndex 會變成 0，導致 xAxisIndex/yAxisIndex 指到主圖價格軸而壓扁柱狀圖。
  const showMainArea = showMainK.value || showReversalLines || showHMA.value || showVPVR.value || showFib.value || showDiagSR.value
  // 手機版：主圖頂保留帶 mobileMaLegendBandPx；圖例預設貼上緣(top:34)，與查價橫條對調時改貼近 grid 上緣
  const isMobileLayout =
    (typeof window !== 'undefined' && window.innerWidth < 640) ||
    (Number.isFinite(containerWidth) && containerWidth > 0 && containerWidth < 640)
  const maLegendRowShown =
    showMainK.value &&
    !showReversal.value &&
    !maLegendCollapsed.value &&
    !(isMobileMaHmaExclusiveMode() && showHMA.value)
  const mtCompact = props.multiTileMode
  const swapMobileMaLegendAndPinnedTip =
    isMobileLayout && maLegendRowShown && !mtCompact
  /** 僅開啟布林時才改手機垂直堆疊；無布林時均線維持原位置 */
  const mobileBbStackLayout = !!(swapMobileMaLegendAndPinnedTip && showBB.value)
  const mobileMaLegendBandPx = (isMobileLayout && maLegendRowShown)
    ? (mobileBbStackLayout ? 36 : 40)
    : 0
  // 多格內主圖頂邊：ECharts 均線圖例在 y≈34 起，主圖 grid 須在圖例下方，否則 K 線與圖例重疊
  const mtMaLegendBandPx = (mtCompact && maLegendRowShown) ? 24 : 0
  const mobileDockBandPx = mobileBbStackLayout
    ? (PINNED_LOOKUP_ASIDE_DOCK_HEIGHT_PX + 2)
    : 0
  const mobileBbLegendGapPx = mobileBbStackLayout ? 1 : 0
  const bbLegendBandPx = showBB.value
    ? (mtCompact ? 14 : (mobileBbStackLayout ? 12 : 18))
    : 0

  const stripGapForMaMobile = Math.max(2, gapPx - 18)
  const mobileMaPinnedDockLiftPx = (swapMobileMaLegendAndPinnedTip && !mobileBbStackLayout)
    ? PINNED_LOOKUP_ASIDE_DOCK_HEIGHT_PX + 2
    : 0

  let topPadPx
  let topMain
  let mobileMaLegendGraphicTopPx
  let mobileDockTopPx
  let bbLegendTopPx

  if (mobileBbStackLayout) {
    // 有布林：均線置頂 → 查價 → 布林（貼近）→ K 線
    topPadPx = 6
    mobileMaLegendGraphicTopPx = Math.max(2, Math.round(topPadPx))
    mobileDockTopPx = Math.max(4, Math.round(mobileMaLegendGraphicTopPx + mobileMaLegendBandPx + 2))
    bbLegendTopPx = Math.max(4, Math.round(mobileDockTopPx + PINNED_LOOKUP_ASIDE_DOCK_HEIGHT_PX + mobileBbLegendGapPx))
    topMain = topPadPx + mobileMaLegendBandPx + mobileDockBandPx + mobileBbLegendGapPx + bbLegendBandPx
  } else {
    // 無布林（或非手機堆疊）：維持原先均線／topMain 計算
    topPadPx = mtCompact ? 32 : 58
    topMain = topPadPx + (mtCompact ? mtMaLegendBandPx : mobileMaLegendBandPx) + bbLegendBandPx
    mobileMaLegendGraphicTopPx = swapMobileMaLegendAndPinnedTip
      ? Math.max(
        6,
        Math.round(topMain - mobileMaLegendBandPx - stripGapForMaMobile - mobileMaPinnedDockLiftPx)
      )
      : 0
    const maStripRowStepPx = Math.round(9 + 10)
    mobileDockTopPx = swapMobileMaLegendAndPinnedTip
      ? Math.max(4, Math.round(mobileMaLegendGraphicTopPx + 2 * maStripRowStepPx + 10))
      : 0
    bbLegendTopPx = showBB.value
      ? Math.max(4, topMain - bbLegendBandPx + 2)
      : 0
  }
  try {
    qgBbLegendLayout.active = !!(showBB.value && isMobileLayout && !mtCompact)
    qgBbLegendLayout.bandPx = qgBbLegendLayout.active ? bbLegendBandPx : 0
    qgBbLegendLayout.topPx = bbLegendTopPx
    qgBbLegendLayout.left = resolveBbLegendLeft(swapMobileMaLegendAndPinnedTip, containerWidth)
    qgBbLegendLayout.mobileSide = 'left'
  } catch (_) {}
  try {
    qgMaLegendStripLayout.active = !!(swapMobileMaLegendAndPinnedTip && maLegendRowShown)
    const yPad = 14
    qgMaLegendStripLayout.yMin = swapMobileMaLegendAndPinnedTip
      ? Math.max(0, Math.round(mobileMaLegendGraphicTopPx - 12))
      : 0
    qgMaLegendStripLayout.yMax = swapMobileMaLegendAndPinnedTip
      ? Math.round((mobileBbStackLayout ? (mobileMaLegendGraphicTopPx + mobileMaLegendBandPx) : topMain) + yPad)
      : 0
    qgMaLegendStripLayout.legendGraphicTop = swapMobileMaLegendAndPinnedTip
      ? Math.max(0, Math.round(mobileMaLegendGraphicTopPx))
      : 0
    qgMaLegendStripLayout.dockTopPx = swapMobileMaLegendAndPinnedTip
      ? Math.max(0, Math.round(mobileDockTopPx))
      : 0
  } catch (_) {}
  // 查價浮層：有布林堆疊時對齊 dock；無布林維持原上緣安全距離
  const pinnedTooltipLegendFloorPx = mobileBbStackLayout
    ? Math.max(4, mobileDockTopPx)
    : swapMobileMaLegendAndPinnedTip
      ? 8
      : !maLegendRowShown
        ? 8
        : mtCompact
          ? Math.round(4 + (containerWidth > 0 && containerWidth < 400 ? 46 : 40) + 10)
          : isMobileLayout
            ? Math.round(34 + 56 + 12)
            : 8
  const subIds = []
  if (showHA.value) subIds.push('ha')
  if (showKD.value) subIds.push('kd')
  if (showRSI.value) subIds.push('rsi')
  if (showCCI.value) subIds.push('cci')
  if (showMACD.value) subIds.push('macd')
  if (showHMAInd.value) subIds.push('hmaInd')
  if (showGoldenWave.value) subIds.push('goldenWave')
  if (showVolume.value) subIds.push('volume')
  const ids = []
  if (showMainArea) ids.push('main')
  ids.push(...subIds)
  const subCount = subIds.length
  const hasSubplot = subCount > 0
  // Any sub-chart: squeeze into real canvas H; do not inflate --chart-container-* (avoids clipping past viewport)
  const squeezeManySubs = hasSubplot
  const bottomPadding = hasSubplot ? Math.min(64, 12 + subCount * 12) : 8

  const minMainPx = mtCompact ? 150 : 220
  const macdMinSubPx = containerWidth >= 1800 ? 190 : (containerWidth >= 1500 ? 170 : (containerWidth >= 1280 ? 145 : 115))
  const defaultMinSubPx = containerWidth >= 1500 ? 102 : 90
  const minsPx = ids.map((id) => {
    if (id === 'main') return minMainPx
    if (id === 'macd') return macdMinSubPx
    return defaultMinSubPx
  })
  const gapsBetweenPanels = ids.length > 1 ? (ids.length - 1) * gapPx : 0
  const legendOverhead = hasSubplot ? subCount * legendBandPx : 0
  const overhead = gapsBetweenPanels + legendOverhead
  const sumMinPanels = minsPx.reduce((a, b) => a + Number(b || 0), 0)
  const minRequiredH = topMain + bottomPadding + overhead + sumMinPanels

  let H = (chartInstance && typeof chartInstance.getHeight === 'function')
    ? chartInstance.getHeight()
    : (container?.clientHeight || 600)
  if (!H || H <= 0) {
    H = container?.clientHeight || 600
  }

  /**
   * Mobile fullscreen + collapsed toolbar: skip vp-fit; outer flex fills viewport (avoids gap under voice/collapsed UI).
   * Expanded toolbar needs vp-fit so ECharts height matches visible area and subplots are not clipped.
   */

  const skipMobileFullscreenVpFit =
    isFullscreen.value &&
    mobileFsToolbarCollapsed.value &&
    useMobileKlineDropdown.value &&
    typeof window !== 'undefined' &&
    window.matchMedia &&
    window.matchMedia('(max-width: 768px)').matches

  try {
    if (container) {
      try {
        if (container.dataset.qgFitVp === '1') {
          container.style.removeProperty('min-height')
          container.style.removeProperty('height')
          container.removeAttribute('data-qg-fit-vp')
        }
      } catch (_) {}
      if (props.multiTileMode && rootEl.value) {
        const host = rootEl.value
        const hr = host.getBoundingClientRect()
        const hdr = host.querySelector?.('.chart-header')
        const headerH = hdr ? hdr.getBoundingClientRect().height : 0
        const parentH = container?.parentElement?.getBoundingClientRect?.()?.height || 0
        const hostH = Math.max(hr.height, parentH, host.clientHeight || 0, 0)
        const hAvail = Math.max(100, Math.floor((hostH > 30 ? hostH : parentH) - headerH - 6))
        const hPx = `${hAvail}px`
        container.style.setProperty('--chart-container-height', hPx, 'important')
        container.style.setProperty('--chart-container-min-height', hPx, 'important')
      } else if (squeezeManySubs) {
        container.style.removeProperty('--chart-container-height')
        container.style.removeProperty('--chart-container-min-height')
      } else {
        const targetH = Math.max(H, minRequiredH)
        const hPx = `${Math.round(targetH)}px`
        container.style.setProperty('--chart-container-height', hPx, 'important')
        container.style.setProperty('--chart-container-min-height', hPx, 'important')
      }
      if (chartInstance && typeof chartInstance.resize === 'function') {
        chartInstance.resize()
      }
      const hAfter = (chartInstance && typeof chartInstance.getHeight === 'function' && chartInstance.getHeight() > 0)
        ? chartInstance.getHeight()
        : (container.clientHeight || 0)
      if (hAfter > 0) {
        H = hAfter
      }
      if (
        isMobileLayout &&
        hasSubplot &&
        !skipMobileFullscreenVpFit &&
        chartInstance &&
        typeof chartInstance.resize === 'function'
      ) {
        const visRaw = getChartVisibleHeightPx(container)
        const visH = visRaw > 0 ? Math.max(0, visRaw - 8) : 0
        if (visH > 100 && visH + 4 < H) {
          const hFit = Math.round(visH)
          container.setAttribute('data-qg-fit-vp', '1')
          container.style.setProperty('min-height', '0', 'important')
          container.style.setProperty('height', `${hFit}px`, 'important')
          chartInstance.resize()
          const h2 = chartInstance.getHeight() > 0 ? chartInstance.getHeight() : hFit
          if (h2 > 0) H = h2
        }
      }
    }
  } catch (_) {}

  subplotLayoutBasisH = H
  const usableH = Math.max(0, H - topMain - bottomPadding)
  const availableForGrids = Math.max(0, usableH - overhead)

  const weights01 = ensurePanelWeights(ids)
  const tightVertical = hasSubplot || (H + 2 < minRequiredH)
  const minsForLayout = tightVertical
    ? minsPx.map((m, idx) => {
        const id = ids[idx]
        const cap = id === 'main' ? 160 : (id === 'macd' ? 72 : 56)
        return Math.min(Number(m) || 0, cap)
      })
    : minsPx
  const heightsPx = computeHeightsFromWeights(ids, weights01, availableForGrids, minsForLayout)

  const grids = []
  let haLegendTopPx, kdLegendTopPx, rsiLegendTopPx, cciLegendTopPx, macdLegendTopPx, hmaIndLegendTopPx, goldenWaveLegendTopPx
  const splitterTopsPx = []
  /**
   * 黃金切割標籤包含「比例＋價格」。手機用百分比留白時實際像素不足，
   * ECharts 會把 markLine 的尾端標籤裁掉，因此窄圖表改保留固定像素安全區。
   */
  const chartGridRight = showFib.value
    ? (isMobileLayout ? 96 : '14%')
    : '8%'

  let nextTop = topMain
  const mainHeightPx = showMainArea ? (heightsPx[0] || 0) : 0
  if (showMainArea) {
    grids.push({ left: '5%', right: chartGridRight, top: topMain, height: mainHeightPx, containLabel: false, borderWidth: 1, borderColor: 'rgba(100, 200, 255, 0.2)' })
    nextTop = topMain + mainHeightPx
    if (hasSubplot) splitterTopsPx.push(nextTop + gapPx / 2)
    nextTop = nextTop + (hasSubplot ? gapPx : 0)
  }

  const panelIndexById = {}
  ids.forEach((id, i) => { panelIndexById[id] = i })

  function addSubGrid(id, setLegendTopFn) {
    const legendTopPx = nextTop
    if (typeof setLegendTopFn === 'function') setLegendTopFn(legendTopPx)
    const topPx = legendTopPx + legendBandPx
    const heightPx = heightsPx[panelIndexById[id]] ?? minsPx[panelIndexById[id]] ?? defaultMinSubPx
    grids.push({ left: '5%', right: chartGridRight, top: topPx, height: heightPx, containLabel: false, borderWidth: 1, borderColor: 'rgba(100, 200, 255, 0.2)', backgroundColor: 'rgba(15,23,42,0.35)' })
    const isLast = ids[ids.length - 1] === id
    if (!isLast) splitterTopsPx.push(topPx + heightPx + gapPx / 2)
    nextTop = topPx + heightPx + (isLast ? 0 : gapPx)
    return grids.length - 1
  }

  const idxMain = 0
  const idxHA = showHA.value ? addSubGrid('ha', (v) => { haLegendTopPx = v }) : undefined
  const idxKD = showKD.value ? addSubGrid('kd', (v) => { kdLegendTopPx = v }) : undefined
  const idxRSI = showRSI.value ? addSubGrid('rsi', (v) => { rsiLegendTopPx = v }) : undefined
  const idxCCI = showCCI.value ? addSubGrid('cci', (v) => { cciLegendTopPx = v }) : undefined
  const idxMACD = showMACD.value ? addSubGrid('macd', (v) => { macdLegendTopPx = v }) : undefined
  const idxHMAInd = showHMAInd.value ? addSubGrid('hmaInd', (v) => { hmaIndLegendTopPx = v }) : undefined
  const idxGoldenWave = showGoldenWave.value ? addSubGrid('goldenWave', (v) => { goldenWaveLegendTopPx = v }) : undefined
  const idxVol = showVolume.value ? addSubGrid('volume', undefined) : undefined

  const limitPx = H - bottomPadding
  if (grids.length > 0) {
    const minPlotH = 14
    for (let guard = 0; guard < 12; guard++) {
      let layoutBottomPx = 0
      for (const g of grids) {
        const t = Number(g.top)
        const h = Number(g.height)
        if (Number.isFinite(t) && Number.isFinite(h)) layoutBottomPx = Math.max(layoutBottomPx, t + h)
      }
      if (layoutBottomPx <= limitPx + 0.5) break
      let over = layoutBottomPx - limitPx
      for (let gi = grids.length - 1; gi >= 0 && over > 0.25; gi--) {
        const g = grids[gi]
        const ch = Number(g.height)
        if (!Number.isFinite(ch)) continue
        const room = Math.max(0, ch - minPlotH)
        const take = Math.min(over, room)
        if (take > 0) {
          g.height = ch - take
          over -= take
        }
      }
    }
  }

  try {
    const el = chartContainer.value
    if (el) {
      el.style.setProperty('--chart-grid-right-pct', typeof chartGridRight === 'number' ? `${chartGridRight}px` : chartGridRight)
      if (showMainArea && grids.length > 0) {
        const g0 = grids[0]
        const gt = Number(g0.top)
        const gh = Number(g0.height)
        if (Number.isFinite(H) && Number.isFinite(gt) && Number.isFinite(gh)) {
          const padInsideMain = isMobileLayout ? 22 : 12
          const bottomPx = Math.max(12, H - gt - gh + padInsideMain)
          el.style.setProperty('--qg-crosshair-nav-bottom', `${bottomPx}px`)
        }
      } else {
        el.style.removeProperty('--qg-crosshair-nav-bottom')
      }
    }
  } catch (_) {}

  const haLegendTop = showHA.value ? haLegendTopPx : undefined
  const kdLegendTop = showKD.value ? kdLegendTopPx : undefined
  const rsiLegendTop = showRSI.value ? rsiLegendTopPx : undefined
  const cciLegendTop = showCCI.value ? cciLegendTopPx : undefined
  const macdLegendTop = showMACD.value ? macdLegendTopPx : undefined
  const m4LegendTop = showM4Momentum.value ? m4LegendTopPx : undefined
  const expertLegendTop = showExpert.value ? expertLegendTopPx : undefined
  const hmaIndLegendTop = showHMAInd.value ? hmaIndLegendTopPx : undefined
  const goldenWaveLegendTop = showGoldenWave.value ? goldenWaveLegendTopPx : undefined

  panelLayout.value = { ids, heightsPx, splitterTopsPx, availableForGrids, minsPx }

  // Unified grid indexes to avoid off-by-one mistakes
  const idxRev = undefined

  // 有主圖 K 線時 Fib 已由 candlestick.markLine 繪製；其餘情境用 line 系列補上，避免重疊或漏畫
  let fibSeries = []
  const fibMarkLineOnCandle = showMainK.value && !showReversal.value
  if (!fibMarkLineOnCandle && showFib.value && fibLevels && Array.isArray(fibLevels) && fibLevels.length) {
    const len = dates.length
    fibSeries = fibLevels.map((level) => {
      const ratio = level.ratio
      const pct = (typeof ratio === 'number' && Number.isFinite(ratio))
        ? (ratio * 100).toFixed(1).replace(/\.0$/, '') + '%'
        : ''
      return {
        name: pct ? `Fib ${pct}` : 'Fib',
        type: 'line',
        xAxisIndex: idxMain,
        yAxisIndex: idxMain,
        data: new Array(len).fill(level.value),
        showSymbol: false,
        smooth: false,
        silent: true,
        lineStyle: {
          color: '#facc15',
          width: 1,
          type: 'dashed'
        },
        tooltip: { show: false },
        z: 6
      }
    })
  }

  // Diagonal support/resistance trendlines based on recent swing highs/lows
  // A 方案：將最近區間內的相鄰波峰/波谷幾乎全部連起來，形成多段斜線壓力/支撐
  let diagSrSeries = []
  if (showDiagSR.value && ohlc.length >= 10) {
    const len = dates.length
    const n = ohlc.length
    const windowSize = Math.min(Math.max(20, diagSrParams.windowSize || 120), n)
    const start = Math.max(0, n - windowSize)
    const swings = detectDiagSwings(chartData.value, start, Math.max(1, diagSrParams.lookback || 5))

    const buildSegments = (points, namePrefix, color) => {
      const segments = []
      if (!points || points.length < 2) return segments

      const maxSegments = Math.max(1, Math.floor(diagSrParams.maxSegments || 3))
      for (let i = 0; i < points.length - 1 && segments.length < maxSegments; i++) {
        const p1 = points[i]
        const p2 = points[i + 1]
        if (!p1 || !p2) continue
        if (p2.index <= p1.index) continue
        if (p2.index < start) continue
        // 忽略間隔太短的微小波動，避免畫出過多極短斜線
        const span = p2.index - p1.index
        if (span < Math.max(1, diagSrParams.minSpan || 5)) continue

        const data = new Array(len).fill('-')
        const slope = (p2.value - p1.value) / span
        const from = Math.max(p1.index, start)
        const to = len - 1 // 從第一個波峰/波谷一路延伸到目前最新 K 線
        for (let k = from; k <= to; k++) {
          const y = p1.value + slope * (k - p1.index)
          if (Number.isFinite(y)) data[k] = Number(y.toFixed(2))
        }

        segments.push({
          name: `${namePrefix}${segments.length + 1}`,
          type: 'line',
          xAxisIndex: idxMain,
          yAxisIndex: idxMain,
          data,
          showSymbol: false,
          smooth: false,
          connectNulls: true,
          symbol: 'none',
          lineStyle: {
            width: 1.2,
            color,
            type: 'solid'
          },
          silent: true,
          tooltip: { show: false },
          z: 7
        })
      }

      return segments
    }

    const supportSegments = buildSegments(swings.lows, '斜線支撐', '#22c55e')
    const resistanceSegments = buildSegments(swings.highs, '斜線壓力', '#f97316')

    diagSrSeries = [
      ...supportSegments,
      ...resistanceSegments
    ]
  }

  function detectDiagSwings(rows, startIndex = 0, lookback = 3) {
    const highs = []
    const lows = []
    if (!Array.isArray(rows) || !rows.length) return { highs, lows }
    const n = rows.length
    const lb = Math.max(1, Math.floor(lookback))
    const from = Math.max(startIndex + lb, lb)
    const to = n - lb - 1
    if (from >= to) return { highs, lows }

    for (let i = from; i <= to; i++) {
      const row = rows[i]
      const h = Number(row?.high ?? row?.[3])
      const l = Number(row?.low ?? row?.[2])
      if (!Number.isFinite(h) || !Number.isFinite(l)) continue

      let isHigh = true
      let isLow = true
      for (let j = 1; j <= lb; j++) {
        const prev = rows[i - j]
        const next = rows[i + j]
        const ph = Number(prev?.high ?? prev?.[3])
        const nh = Number(next?.high ?? next?.[3])
        const pl = Number(prev?.low ?? prev?.[2])
        const nl = Number(next?.low ?? next?.[2])
        if (Number.isFinite(ph) && h <= ph) isHigh = false
        if (Number.isFinite(nh) && h <= nh) isHigh = false
        if (Number.isFinite(pl) && l >= pl) isLow = false
        if (Number.isFinite(nl) && l >= nl) isLow = false
        if (!isHigh && !isLow) break
      }
      if (isHigh) highs.push({ index: i, value: h })
      if (isLow) lows.push({ index: i, value: l })
    }
    return { highs, lows }
  }

  // === MA 扣抵標記（三角形） ===
  const maOffsetSeries = []
  if (showMainK.value || showReversal.value) {
    const len = ohlc.length
    // 取主圖整體高低，將扣抵三角形貼近底邊顯示
    const lows = ohlc.map(d => Number(d?.[2])).filter(v => Number.isFinite(v))
    const highs = ohlc.map(d => Number(d?.[3])).filter(v => Number.isFinite(v))
    const yMin = lows.length ? Math.min(...lows) : 0
    const yMax = highs.length ? Math.max(...highs) : 1
    const bottomY = yMin + (yMax - yMin) * 0.005 // 距底約 0.5% 圖高
    const buildOffsetPoint = (period) => {
      const p = Number(period)
      if (!Number.isFinite(p) || p <= 1 || len <= p) return null
      // 扣抵索引：len - N（從最新往回數 N 根）
      const offsetIdx = len - p
      if (offsetIdx < 0 || offsetIdx >= len) return null
      const candle = ohlc[offsetIdx]
      if (!candle || !Array.isArray(candle) || candle.length < 2) return null
      // 使用類別軸的值（日期字串）做 x，y 固定貼近底部
      return { x: dates[offsetIdx], y: bottomY }
    }

    const pushOffset = (enabled, period, color, label) => {
      if (!enabled) return
      const pt = buildOffsetPoint(period)
      if (!pt) return
      maOffsetSeries.push({
        name: `${label}-offset`,
        type: 'scatter',
        xAxisIndex: idxMain,
        yAxisIndex: idxMain,
        data: [[pt.x, pt.y]],
        symbol: 'triangle',
        symbolRotate: 180,
        symbolSize: 13,
        symbolOffset: [0, 12],
        z: 100,
        zlevel: 3,
        itemStyle: { color },
        clip: true,
        tooltip: { show: false },
        legendHoverLink: false,
        silent: true
      })
    }

    if (len > 0) {
      pushOffset(showMA1.value, maParams.value.ma1, '#3b82f6', `MA${maParams.value.ma1}`)
      pushOffset(showMA2.value, maParams.value.ma2, '#a855f7', `MA${maParams.value.ma2}`)
      pushOffset(showMA3.value, maParams.value.ma3, '#f59e0b', `MA${maParams.value.ma3}`)
      pushOffset(showMA4.value, maParams.value.ma4, '#10b981', `MA${maParams.value.ma4}`)
      pushOffset(showMA5.value, maParams.value.ma5, '#f97316', `MA${maParams.value.ma5}`)
    }
  }

  // Build hard separators using graphic masks between subplots
  const separators = []
  const chartBg = '#0b1220' // close to tailwind slate-900/950
  function addSeparator(yPx, hPx) {
    separators.push({
      type: 'rect',
      silent: true,
      z: 20,
      left: '5%',
      right: chartGridRight,
      top: yPx,
      shape: { width: '100%', height: hPx },
      style: { fill: chartBg }
    })
  }
  
  // Add warning text if MACD parameters are too large
  if (showMACD.value && macdData) {
    const validMacdCount = macdData.macd.filter(v => v !== '-').length
    const totalDataPoints = ohlc.length
    const requiredPoints = macdParams.value.slow + macdParams.value.signal
    const dataPercentage = totalDataPoints > 0 ? (validMacdCount / totalDataPoints * 100).toFixed(0) : 0
    
    if (totalDataPoints < requiredPoints || dataPercentage < 50) {
      const macdGrid = grids[idxMACD]
      separators.push({
        type: 'text',
        z: 100,
        left: 'center',
        top: macdGrid.top + macdGrid.height / 2 - 20,
        style: {
          text: `⚠️ MACD 參數過大\n需要 ${requiredPoints} 個數據點，目前只有 ${totalDataPoints} 個\n僅 ${dataPercentage}% 的數據可顯示`,
          font: 'bold 14px sans-serif',
          fill: 'rgba(245, 158, 11, 0.9)',
          textAlign: 'center',
          textVerticalAlign: 'middle'
        }
      })
    }
  }
  // After main -> before first sub-plot (KD or HMA)
  if (showKD.value) {
    const firstTop = kdLegendTopPx
    addSeparator(firstTop - Math.floor((gapPx - 2) / 2), gapPx)
  }
  // Between KD and MACD
  if (showKD.value && showMACD.value) {
    const lastKD = grids[idxKD]
    addSeparator(lastKD.top + lastKD.height - Math.floor((gapPx - 2) / 2), gapPx)
  }
  // Between KD and MACD
  if (showKD.value && showMACD.value) {
    const kdG = grids[idxKD]
    addSeparator(kdG.top + kdG.height - Math.floor((gapPx - 2) / 2), gapPx)
  }
  // Between MACD and Volume
  if (showMACD.value && showVolume.value) {
    const macdG = grids[idxMACD]
    addSeparator(macdG.top + macdG.height - Math.floor((gapPx - 2) / 2), gapPx)
  }
  // Between CCI and Volume when MACD is hidden
  if (showCCI.value && showVolume.value && !showMACD.value && grids[idxCCI]) {
    const cciG = grids[idxCCI]
    addSeparator(cciG.top + cciG.height - Math.floor((gapPx - 2) / 2), gapPx)
  }

  // Helper for building legends with optional values at an index
  const fmt = (v, digits = 2) => (v === '-' || v === undefined || v === null || !isFinite(Number(v))) ? '--' : Number(v).toFixed(digits)
  /** 手機／窄螢幕：多空線圖例改為與均線同一套「彩色名稱 + 漲跌箭頭 + 白字數值」，主圖改單色細線 */
  const hmaMobileMaLegend = (() => {
    try {
      if (typeof window === 'undefined') return false
      const iw = Number(window.innerWidth) || 0
      const vw = Number(window.visualViewport?.width)
      const w = Number.isFinite(vw) && vw > 0 ? Math.min(iw, vw) : iw
      return w > 0 && w <= 768
    } catch (_) {
      return false
    }
  })()
  const hmaMobileMaLineColor =
    hmaTrendDirForColor > 0 ? '#ef4444'
      : (hmaTrendDirForColor < 0 ? '#38bdf8' : '#eab308')
  function buildLegendsAt(idxForValues){
    const legends = []
    if (showMainK.value) {
      // 動態轉折(小不點）模式下，不顯示主圖 MA 圖例與數值（showReversal 時 MA series 也不會被加入）
      if (showReversal.value) {
        return legends
      }
      // 當圖例被收合時，不顯示 MA legend，但線條仍保留，由 legend selected 狀態控制可見性
      if (maLegendCollapsed.value || (isMobileMaHmaExclusiveMode() && showHMA.value)) {
        return legends
      }
      const n1 = `MA${maParams.value.ma1}`
      const n2 = `MA${maParams.value.ma2}`
      const n3 = `MA${maParams.value.ma3}`
      const n4 = `MA${maParams.value.ma4}`
      const n5 = `MA${maParams.value.ma5}`
      const isMobile = typeof window !== 'undefined' && window.innerWidth < 640
      const maStrip = swapMobileMaLegendAndPinnedTip
      const maFs = mtCompact ? 10 : (maStrip ? 11 : (isMobile ? 13 : 14))
      const items = [
        { name: n1, textStyle: { color: '#3b82f6', fontSize: maFs, fontWeight: 700 } },
        { name: n2, textStyle: { color: '#a855f7', fontSize: maFs, fontWeight: 700 } },
        { name: n3, textStyle: { color: '#f59e0b', fontSize: maFs, fontWeight: 700 } },
        { name: n4, textStyle: { color: '#10b981', fontSize: maFs, fontWeight: 700 } },
        { name: n5, textStyle: { color: '#f97316', fontSize: maFs, fontWeight: 700 } },
      ]
      if (items.length) {
        const maLegendFormatter = (name) => {
          const upArrow = '▲'
          const downArrow = '▼'
          const dir = maDirMap[name]
          const val = maLastMap[name]
          const arrow = dir > 0 ? `{maArrowUp|${upArrow}}` : (dir < 0 ? `{maArrowDown|${downArrow}}` : '')
          const digits = maStrip ? 1 : 2
          const valStr = val != null && Number.isFinite(val) ? val.toFixed(digits) : ''
          const valToken = valStr ? ` {maVal|${valStr}}` : ''
          if (arrow) {
            return `${name} ${arrow}${valToken}`
          }
          return `${name}${valToken}`
        }

        if (maStrip) {
          const baseTop = Math.max(6, mobileMaLegendGraphicTopPx)
          const rowStepPx = Math.round(maFs + 10)
          const mid = Math.ceil(items.length / 2)
          const row1Items = items.slice(0, mid)
          const row2Items = items.slice(mid)
          const maRichTextStyle = {
            fontSize: maFs,
            fontWeight: 650,
            rich: {
              maArrowUp: {
                color: '#ef4444',
                fontWeight: 800,
                fontSize: maFs,
              },
              maArrowDown: {
                color: '#22c55e',
                fontWeight: 800,
                fontSize: maFs,
              },
              maVal: {
                color: '#ffffff',
                fontWeight: 650,
                fontSize: maFs,
              }
            }
          }
          const maSelectedAll = {
            [n1]: !!showMA1.value,
            [n2]: !!showMA2.value,
            [n3]: !!showMA3.value,
            [n4]: !!showMA4.value,
            [n5]: !!showMA5.value,
          }
          legends.push({
            id: 'legend-ma-main-r1',
            z: 50,
            zlevel: 2,
            show: true,
            data: row1Items,
            top: baseTop,
            orient: 'horizontal',
            type: 'plain',
            left: '3%',
            right: '3%',
            itemGap: 6,
            itemWidth: 11,
            itemHeight: 5,
            padding: [2, 4, 2, 4],
            borderRadius: 4,
            backgroundColor: 'transparent',
            icon: 'line',
            selectedMode: 'multiple',
            inactiveColor: 'rgba(148, 163, 184, 0.7)',
            textStyle: maRichTextStyle,
            selected: maSelectedAll,
            formatter: maLegendFormatter,
          })
          if (row2Items.length) {
            legends.push({
              id: 'legend-ma-main-r2',
              z: 50,
              zlevel: 2,
              show: true,
              data: row2Items,
              top: baseTop + rowStepPx,
              orient: 'horizontal',
              type: 'plain',
              left: '3%',
              right: '3%',
              itemGap: 6,
              itemWidth: 11,
              itemHeight: 5,
              padding: [2, 4, 2, 4],
              borderRadius: 4,
              backgroundColor: 'transparent',
              icon: 'line',
              selectedMode: 'multiple',
              inactiveColor: 'rgba(148, 163, 184, 0.7)',
              textStyle: maRichTextStyle,
              selected: maSelectedAll,
              formatter: maLegendFormatter,
            })
          }
        } else {
          legends.push({
            id: 'legend-ma-main',
            z: 50,
            zlevel: 2,
            show: true,
            data: items,
            top: mtCompact ? 4 : 34,
            orient: 'horizontal',
            ...(mtCompact
              ? {
                  type: 'plain',
                  left: '4%',
                  right: '4%',
                  itemGap: 3,
                  itemWidth: 12,
                  itemHeight: 6,
                  padding: [2, 3],
                  borderRadius: 6,
                }
              : {
                  type: 'plain',
                  left: '4%',
                  right: '4%',
                  itemGap: isMobile ? 8 : 14,
                  itemWidth: isMobile ? 16 : 24,
                  itemHeight: isMobile ? 8 : 12,
                  padding: isMobile ? [6, 10] : [8, 16],
                  borderRadius: 6,
                }),
            backgroundColor: 'transparent',
            icon: 'line',
            selectedMode: 'multiple',
            inactiveColor: 'rgba(148, 163, 184, 0.7)',
            textStyle: {
              fontSize: maFs,
              fontWeight: 700,
              rich: {
                maArrowUp: {
                  color: '#ef4444',
                  fontWeight: 800
                },
                maArrowDown: {
                  color: '#22c55e',
                  fontWeight: 800
                },
                maVal: {
                  color: '#ffffff',
                  fontWeight: 700
                }
              }
            },
            selected: {
              [n1]: !!showMA1.value,
              [n2]: !!showMA2.value,
              [n3]: !!showMA3.value,
              [n4]: !!showMA4.value,
              [n5]: !!showMA5.value,
            },
            formatter: maLegendFormatter,
          })
        }
      }
    }
    // 其餘指標（HA/KD/MACD/HMA）改以 graphic 文字顯示，避免多 legend 互相影響
    return legends
  }

  // Expose renderChart-scoped helpers to hover updater
  buildLegendTextUpdatesAtFn = buildLegendTextUpdatesAt
  updateMaLegendStateAtFn = updateMaLegendStateAt
  buildLegendsAtFn = buildLegendsAt

  // Build always-visible text overlays near each sub-legend (robust fallback)
  const baseFontSize = 12
  const hmaFontSize = 18
  const kdRich = {
    kdLabel: { color: 'rgba(226,232,240,0.85)', fill: 'rgba(226,232,240,0.85)', fontWeight: 500, fontSize: baseFontSize },
    kdK: { color: '#f59e0b', fill: '#f59e0b', fontWeight: 600, fontSize: baseFontSize },
    kdD: { color: '#3b82f6', fill: '#3b82f6', fontWeight: 600, fontSize: baseFontSize },
    kdArrowUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 700, fontSize: baseFontSize },
    kdArrowDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 700, fontSize: baseFontSize },
  }
  const cciRich = {
    cciLabel: { color: '#22d3ee', fill: '#22d3ee', fontWeight: 700, fontSize: baseFontSize },
    // 一般 / 向下時的數值（綠色）
    cciVal: { color: '#22c55e', fill: '#22c55e', fontWeight: 600, fontSize: baseFontSize },
    // 向上時的數值（紅色）
    cciValUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 600, fontSize: baseFontSize },
    cciValDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 600, fontSize: baseFontSize },
    cciArrowUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 700, fontSize: baseFontSize },
    cciArrowDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 700, fontSize: baseFontSize },
  }
  const rsiRich = {
    rsiLabel: { color: '#c084fc', fill: '#c084fc', fontWeight: 700, fontSize: baseFontSize },
    rsiVal: { color: 'rgba(226,232,240,0.9)', fill: 'rgba(226,232,240,0.9)', fontWeight: 600, fontSize: baseFontSize },
    rsiValOverbought: { color: '#ef4444', fill: '#ef4444', fontWeight: 600, fontSize: baseFontSize },
    rsiValOversold: { color: '#22c55e', fill: '#22c55e', fontWeight: 600, fontSize: baseFontSize },
    rsiArrowUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 700, fontSize: baseFontSize },
    rsiArrowDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 700, fontSize: baseFontSize },
  }
  const hmaRichMaMobileFs = 11
  const hmaRich = {
    hmaLabel: { fill: 'rgba(226,232,240,0.9)', fontWeight: 600, fontSize: hmaFontSize },
    hmaArrowUp: { fill: '#ef4444', fontWeight: 800, fontSize: hmaFontSize },
    hmaArrowDown: { fill: '#38bdf8', fontWeight: 800, fontSize: hmaFontSize },
    hmaVal: { fill: '#e5e7eb', fontWeight: 700, fontSize: hmaFontSize },
    hmaValUp: { fill: '#ef4444', fontWeight: 700, fontSize: hmaFontSize },
    hmaValDown: { fill: '#38bdf8', fontWeight: 700, fontSize: hmaFontSize },
    ...(hmaMobileMaLegend
      ? {
          hmaLbl: { fill: hmaMobileMaLineColor, fontWeight: 700, fontSize: hmaRichMaMobileFs },
          hmaArrUp: { fill: '#ef4444', fontWeight: 800, fontSize: hmaRichMaMobileFs },
          hmaArrDn: { fill: '#38bdf8', fontWeight: 800, fontSize: hmaRichMaMobileFs },
          hmaV: { fill: '#ffffff', fontWeight: 650, fontSize: hmaRichMaMobileFs },
        }
      : {}),
  }
  const macdRich = {
    macdLabel: { color: 'rgba(226,232,240,0.85)', fill: 'rgba(226,232,240,0.85)', fontWeight: 500, fontSize: baseFontSize },
    macdVal: { color: '#3b82f6', fill: '#3b82f6', fontWeight: 600, fontSize: baseFontSize },
    macdSignal: { color: '#f59e0b', fill: '#f59e0b', fontWeight: 600, fontSize: baseFontSize },
    macdHist: { color: '#fb7185', fill: '#fb7185', fontWeight: 600, fontSize: baseFontSize },
    macdArrowUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 700, fontSize: baseFontSize },
    macdArrowDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 700, fontSize: baseFontSize },
  }
  const volRich = {
    volLabel: { color: 'rgba(226,232,240,0.85)', fill: 'rgba(226,232,240,0.85)', fontWeight: 500, fontSize: baseFontSize },
    volUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 600, fontSize: baseFontSize },
    volDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 600, fontSize: baseFontSize },
  }

  const m4Rich = {
    m4Label: { color: 'rgba(226,232,240,0.85)', fill: 'rgba(226,232,240,0.85)', fontWeight: 500, fontSize: baseFontSize },
    m4Val: { color: '#3b82f6', fill: '#3b82f6', fontWeight: 600, fontSize: baseFontSize },
    m4ValUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 600, fontSize: baseFontSize },
    m4ValDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 600, fontSize: baseFontSize },
    m4ArrowUp: { color: '#ef4444', fill: '#ef4444', fontWeight: 700, fontSize: baseFontSize },
    m4ArrowDown: { color: '#22c55e', fill: '#22c55e', fontWeight: 700, fontSize: baseFontSize },
  }

  // Generic helper: determine short-term slope direction for a numeric series at index
  function getLineDirectionAt(series, idx) {
    if (!Array.isArray(series) || !series.length) return 0
    const isInvalid = (v) => v === '-' || v === null || v === undefined || !Number.isFinite(Number(v))

    const len = series.length
    let i = typeof idx === 'number' ? idx : (len - 1)
    if (i < 0 || i >= len) i = len - 1

    // locate current value (fallback向左找最近一個有效值)
    let curr = series[i]
    while (i >= 0 && isInvalid(curr)) {
      i -= 1
      curr = series[i]
    }
    if (i <= 0 || isInvalid(curr)) return 0

    // locate previous valid value
    let j = i - 1
    let prev = null
    while (j >= 0) {
      const v = series[j]
      if (!isInvalid(v)) {
        prev = v
        break
      }
      j -= 1
    }
    if (prev === null) return 0

    const currNum = Number(curr)
    const prevNum = Number(prev)
    const diff = currNum - prevNum
    const threshold = Math.max(0.0001, Math.abs(currNum) * 0.002) // 約 0.2% 變化才算明顯
    if (diff > threshold) return 1
    if (diff < -threshold) return -1
    return 0
  }

  function kdLegendText(idx) {
    const kVal = (idx != null && kdData && kdData.k[idx] !== undefined) ? fmt(kdData.k[idx], 2) : '--'
    const dVal = (idx != null && kdData && kdData.d[idx] !== undefined) ? fmt(kdData.d[idx], 2) : '--'
    const dirK = kdData ? getLineDirectionAt(kdData.k, idx) : 0
    const dirD = kdData ? getLineDirectionAt(kdData.d, idx) : 0
    const arrowK = dirK > 0 ? '{kdArrowUp|▲}' : (dirK < 0 ? '{kdArrowDown|▼}' : '')
    const arrowD = dirD > 0 ? '{kdArrowUp|▲}' : (dirD < 0 ? '{kdArrowDown|▼}' : '')
    // 僅顯示 K / D 數值與箭頭，箭頭擺在數值右側並與數值留出一點間距
    const kPart = `{kdK|K ${kVal}}` + (arrowK ? `  ${arrowK}` : '')
    const dPart = `{kdD|D ${dVal}}` + (arrowD ? `  ${arrowD}` : '')
    const parts = []
    if (showKLine.value) parts.push(kPart)
    if (showDLine.value) parts.push(dPart)
    if (!parts.length) {
      return ''
    }
    return parts.join('   ')
  }

  function cciLegendText(idx) {
    if (!cciData || !Array.isArray(cciData) || !cciData.length) {
      return '{cciLabel|ORC}  {cciVal|--}'
    }
    let i = typeof idx === 'number' ? idx : (cciData.length - 1)
    if (i < 0 || i >= cciData.length) i = cciData.length - 1
    let v = cciData[i]
    // 往左找最近一個有效值
    for (let j = i; (v === '-' || v == null || !Number.isFinite(Number(v))) && j >= 0; j--) {
      v = cciData[j]
      i = j
    }
    if (v === '-' || v == null || !Number.isFinite(Number(v))) {
      return '{cciLabel|ORC}  {cciVal|--}'
    }
    const valNum = Number(v)
    const valText = fmt(valNum, 0)
    const dir = getLineDirectionAt(cciData, i)
    const arrow = dir > 0 ? '{cciArrowUp|▲}' : (dir < 0 ? '{cciArrowDown|▼}' : '')
    let valKey = 'cciVal'
    if (dir > 0) valKey = 'cciValUp'
    else if (dir < 0) valKey = 'cciValDown'
    const valToken = `{${valKey}|${valText}}`
    const withArrow = arrow ? `${valToken}  ${arrow}` : valToken
    return `{cciLabel|ORC}  ${withArrow}`
  }

  function rsiLegendText(idx) {
    const label = `RSI(${rsiParams.value.period})`
    if (!rsiData || !Array.isArray(rsiData) || !rsiData.length) {
      return `{rsiLabel|${label}}  {rsiVal|--}`
    }
    let i = typeof idx === 'number' ? idx : (rsiData.length - 1)
    if (i < 0 || i >= rsiData.length) i = rsiData.length - 1
    let v = rsiData[i]
    // 往左找最近一個有效值
    for (let j = i; (v === '-' || v == null || !Number.isFinite(Number(v))) && j >= 0; j--) {
      v = rsiData[j]
      i = j
    }
    if (v === '-' || v == null || !Number.isFinite(Number(v))) {
      return `{rsiLabel|${label}}  {rsiVal|--}`
    }
    const valNum = Number(v)
    const valText = fmt(valNum, 1)
    const dir = getLineDirectionAt(rsiData, i)
    const arrow = dir > 0 ? '{rsiArrowUp|▲}' : (dir < 0 ? '{rsiArrowDown|▼}' : '')
    let valKey = 'rsiVal'
    if (valNum >= rsiParams.value.overbought) valKey = 'rsiValOverbought'
    else if (valNum <= rsiParams.value.oversold) valKey = 'rsiValOversold'
    const valToken = `{${valKey}|${valText}}`
    const withArrow = arrow ? `${valToken}  ${arrow}` : valToken
    return `{rsiLabel|${label}}  ${withArrow}`
  }

  function bbLegendText(idx) {
    const label = `布林(${bbParams.value.period},${bbParams.value.mult})`
    if (!bbData || !Array.isArray(bbData.mid) || !bbData.mid.length) {
      return `{bbLabel|${label}}  {bbUpperMark|━}{bbUpper|上 --}  {bbMidMark|━}{bbMid|中 --}  {bbLowerMark|━}{bbLower|下 --}`
    }
    let i = typeof idx === 'number' ? idx : (bbData.mid.length - 1)
    if (i < 0 || i >= bbData.mid.length) i = bbData.mid.length - 1
    // 往左找最近一個有效值（三條軌道以相同索引為準）
    for (let j = i; j >= 0; j--) {
      const m = bbData.mid[j]
      if (m !== '-' && m != null && Number.isFinite(Number(m))) { i = j; break }
      if (j === 0) i = -1
    }
    if (i < 0) {
      return `{bbLabel|${label}}  {bbUpperMark|━}{bbUpper|上 --}  {bbMidMark|━}{bbMid|中 --}  {bbLowerMark|━}{bbLower|下 --}`
    }
    const upperText = Number.isFinite(Number(bbData.upper[i])) ? fmt(Number(bbData.upper[i]), 2) : '--'
    const midText = Number.isFinite(Number(bbData.mid[i])) ? fmt(Number(bbData.mid[i]), 2) : '--'
    const lowerText = Number.isFinite(Number(bbData.lower[i])) ? fmt(Number(bbData.lower[i]), 2) : '--'
    return `{bbLabel|${label}}  {bbUpperMark|━}{bbUpper|上 ${upperText}}  {bbMidMark|━}{bbMid|中 ${midText}}  {bbLowerMark|━}{bbLower|下 ${lowerText}}`
  }

  function hmaLegendText(idx) {
    if (!hma || !Array.isArray(hma.base)) {
      if (hmaMobileMaLegend) return `{hmaLbl|多空線} {hmaV|--}`
      return `{hmaLabel|多空線}  {hmaVal|--}`
    }

    const arr = hma.base
    let i = (typeof idx === 'number') ? idx : -1

    // 若沒有提供索引或索引超出範圍，就使用最後一個有效值
    if (i < 0 || i >= arr.length) {
      for (let j = arr.length - 1; j >= 0; j--) {
        const v = arr[j]
        if (typeof v === 'number' && Number.isFinite(v)) {
          i = j
          break
        }
      }
    }

    const v = (i >= 0 && i < arr.length) ? arr[i] : null
    if (v == null || !Number.isFinite(v)) {
      if (hmaMobileMaLegend) return `{hmaLbl|多空線} {hmaV|--}`
      return `{hmaLabel|多空線}  {hmaVal|--}`
    }

    // 以 HMA 斜率判斷方向：當前值相對前一筆
    let dir = 0
    if (i > 0) {
      const prev = arr[i - 1]
      if (typeof prev === 'number' && Number.isFinite(prev)) {
        if (v > prev) dir = 1
        else if (v < prev) dir = -1
      }
    }

    const hmaDigits = (hmaMobileMaLegend && swapMobileMaLegendAndPinnedTip) ? 1 : 2
    const hVal = fmt(v, hmaDigits)
    if (hmaMobileMaLegend) {
      const arrow =
        dir > 0 ? `{hmaArrUp|▲}` : (dir < 0 ? `{hmaArrDn|▼}` : '')
      return arrow
        ? `{hmaLbl|多空線} ${arrow} {hmaV|${hVal}}`
        : `{hmaLbl|多空線} {hmaV|${hVal}}`
    }

    let arrowToken = ''
    let valueToken = `{hmaVal|${hVal}}`
    if (dir > 0) {
      arrowToken = `{hmaArrowUp|▲}`
      valueToken = `{hmaValUp|${hVal}}`
    } else if (dir < 0) {
      arrowToken = `{hmaArrowDown|▼}`
      valueToken = `{hmaValDown|${hVal}}`
    }
    const valWithArrow = arrowToken ? `${arrowToken} ${valueToken}` : valueToken
    return `{hmaLabel|多空線}  ${valWithArrow}`
  }

  function hmaIndLegendText(idx) {
    if (!hmaIndData || !Array.isArray(hmaIndData.base)) {
      return '{hmaIndLabel|多空趨勢線}  {hmaIndVal|--}'
    }

    const arr = hmaIndData.base
    let i = (typeof idx === 'number') ? idx : -1
    if (i < 0 || i >= arr.length) {
      for (let j = arr.length - 1; j >= 0; j--) {
        const v = arr[j]
        if (typeof v === 'number' && Number.isFinite(v)) {
          i = j
          break
        }
      }
    }

    const v = (i >= 0 && i < arr.length) ? arr[i] : null
    if (v == null || !Number.isFinite(v)) {
      return '{hmaIndLabel|多空趨勢線}  {hmaIndVal|--}'
    }

    const dir = getLineDirectionAt(arr, i)
    const hVal = fmt(v, 2)
    if (dir > 0) {
      return `{hmaIndLabelUp|多空趨勢線}  {hmaIndArrowUp|▲} {hmaIndValUp|${hVal}}`
    }
    if (dir < 0) {
      return `{hmaIndLabelDown|多空趨勢線}  {hmaIndArrowDown|▼} {hmaIndValDown|${hVal}}`
    }
    return `{hmaIndLabel|多空趨勢線}  {hmaIndVal|${hVal}}`
  }

  function hmaIndLegendRich() {
    const upColor = hmaIndParams.value?.upColor || '#ef4444'
    const downColor = hmaIndParams.value?.downColor || '#22c55e'
    const labelColor = '#fbbf24'
    return {
      hmaIndLabel: { color: labelColor, fill: labelColor, fontSize: 11, fontWeight: 'bold' },
      hmaIndLabelUp: { color: labelColor, fill: labelColor, fontSize: 11, fontWeight: 'bold' },
      hmaIndLabelDown: { color: labelColor, fill: labelColor, fontSize: 11, fontWeight: 'bold' },
      hmaIndArrowUp: { color: upColor, fill: upColor, fontSize: 11, fontWeight: 'bold' },
      hmaIndArrowDown: { color: downColor, fill: downColor, fontSize: 11, fontWeight: 'bold' },
      hmaIndVal: { color: 'rgba(226,232,240,0.85)', fill: 'rgba(226,232,240,0.85)', fontSize: 11, fontWeight: 'bold' },
      hmaIndValUp: { color: upColor, fill: upColor, fontSize: 11, fontWeight: 'bold' },
      hmaIndValDown: { color: downColor, fill: downColor, fontSize: 11, fontWeight: 'bold' },
    }
  }

  function macdLegendText(idx) {
    const macdV = (idx != null && macdData && macdData.macd[idx] !== undefined) ? fmt(macdData.macd[idx], 2) : '--'
    const sigV  = (idx != null && macdData && macdData.signal[idx] !== undefined) ? fmt(macdData.signal[idx], 2) : '--'
    const histV = (idx != null && macdData && macdData.histogram[idx] !== undefined) ? fmt(macdData.histogram[idx], 2) : '--'
    const dirDif  = macdData ? getLineDirectionAt(macdData.macd, idx) : 0
    const dirMacd = macdData ? getLineDirectionAt(macdData.signal, idx) : 0
    const dirOsc  = macdData ? getLineDirectionAt(macdData.histogram, idx) : 0

    const arrowDif  = dirDif  > 0 ? '{macdArrowUp|▲}' : (dirDif  < 0 ? '{macdArrowDown|▼}' : '')
    const arrowMacd = dirMacd > 0 ? '{macdArrowUp|▲}' : (dirMacd < 0 ? '{macdArrowDown|▼}' : '')
    const arrowOsc  = dirOsc  > 0 ? '{macdArrowUp|▲}' : (dirOsc  < 0 ? '{macdArrowDown|▼}' : '')

    // DIF / MACD / OSC 數值與箭頭，箭頭擺在數值右側並與數值留出一點間距
    const difPart  = `{macdVal|DIF ${macdV}}`    + (arrowDif  ? `  ${arrowDif}`  : '')
    const macdPart = `{macdSignal|MACD ${sigV}}` + (arrowMacd ? `  ${arrowMacd}` : '')
    const oscPart  = `{macdHist|OSC ${histV}}`   + (arrowOsc  ? `  ${arrowOsc}`  : '')

    const parts = []
    if (hasMacdDisplay('dif')) parts.push(difPart)
    if (hasMacdDisplay('macd')) parts.push(macdPart)
    if (hasMacdDisplay('osc')) parts.push(oscPart)
    return parts.length ? parts.join('   ') : `${difPart}   ${macdPart}   ${oscPart}`
  }

  function m4LegendText(idx) {
    if (!m4LineValues || !Array.isArray(m4LineValues) || !m4LineValues.length) {
      return '{m4Label|四狀態動能}  {m4Val|--}'
    }
    let i = typeof idx === 'number' ? idx : (m4LineValues.length - 1)
    if (i < 0 || i >= m4LineValues.length) i = m4LineValues.length - 1
    let v = m4LineValues[i]
    for (let j = i; (v === '-' || v == null || !Number.isFinite(Number(v))) && j >= 0; j--) {
      v = m4LineValues[j]
      i = j
    }
    if (v === '-' || v == null || !Number.isFinite(Number(v))) {
      return '{m4Label|四狀態動能}  {m4Val|--}'
    }
    const valNum = Number(v)
    const valText = fmt(valNum, 2)
    const dir = getLineDirectionAt(m4LineValues, i)
    const arrow = dir > 0 ? '{m4ArrowUp|▲}' : (dir < 0 ? '{m4ArrowDown|▼}' : '')
    let valKey = 'm4Val'
    if (dir > 0) valKey = 'm4ValUp'
    else if (dir < 0) valKey = 'm4ValDown'
    const valToken = `{${valKey}|${valText}}`
    const withArrow = arrow ? `${valToken}  ${arrow}` : valToken
    return `{m4Label|四狀態動能}  ${withArrow}`
  }

  function expertLegendText(idx) {
    if (!expertData || !Array.isArray(expertData.a0) || !expertData.a0.length) {
      return '{expertLabel|行家指標}  {expertA0|A0: --}  {expertB0|B0: --}  {expertVal|力度: --}'
    }
    let i = typeof idx === 'number' ? idx : (expertData.a0.length - 1)
    if (i < 0 || i >= expertData.a0.length) i = expertData.a0.length - 1
    const a0Val = expertData.a0[i]
    const b0Val = expertData.b0[i]
    const val1 = expertData.value1[i]
    if (a0Val === '-' || b0Val === '-' || val1 === '-') {
      return '{expertLabel|行家指標}  {expertA0|A0: --}  {expertB0|B0: --}  {expertVal|力度: --}'
    }
    const a0Text = fmt(a0Val, 1)
    const b0Text = fmt(b0Val, 1)
    const val1Text = fmt(val1, 2)
    const val1Key = val1 > 0 ? 'expertValUp' : (val1 < 0 ? 'expertValDown' : 'expertVal')
    return `{expertLabel|行家指標}  {expertA0|A0: ${a0Text}}  {expertB0|B0: ${b0Text}}  {${val1Key}|力度: ${val1Text}}`
  }

  function goldenWaveLegendText(idx) {
    if (!goldenWaveData) {
      return '{gwLabel|動態轉折(小不點）}  {gwDif|DIF: --}'
    }
    let i = typeof idx === 'number' ? idx : (goldenWaveData.dif.length - 1)
    if (i < 0 || i >= goldenWaveData.dif.length) i = goldenWaveData.dif.length - 1
    const difVal = goldenWaveData.dif[i]
    const difNum = (difVal !== '-' && difVal != null) ? Number(difVal) : NaN
    const difText = Number.isFinite(difNum) ? fmt(difNum, 2) : '--'
    const dirDif = getLineDirectionAt(goldenWaveData.dif, i)
    if (dirDif > 0) {
      return `{gwLabelUp|動態轉折(小不點）}  {gwArrowUp|▲} {gwDifUp|DIF: ${difText}}`
    }
    if (dirDif < 0) {
      return `{gwLabelDown|動態轉折(小不點）}  {gwArrowDown|▼} {gwDifDown|DIF: ${difText}}`
    }
    return `{gwLabel|動態轉折(小不點）}  {gwDif|DIF: ${difText}}`
  }

  function goldenWaveLegendRich() {
    const upColor = '#ef4444'
    const downColor = '#22c55e'
    const labelColor = '#a78bfa'
    const difColor = goldenWaveParams.value?.difLineColor || 'rgba(226,232,240,0.85)'
    return {
      gwLabel: { color: labelColor, fill: labelColor, fontSize: 11, fontWeight: 'bold' },
      gwLabelUp: { color: labelColor, fill: labelColor, fontSize: 11, fontWeight: 'bold' },
      gwLabelDown: { color: labelColor, fill: labelColor, fontSize: 11, fontWeight: 'bold' },
      gwArrowUp: { color: upColor, fill: upColor, fontSize: 11, fontWeight: 'bold' },
      gwArrowDown: { color: downColor, fill: downColor, fontSize: 11, fontWeight: 'bold' },
      gwDif: { color: difColor, fill: difColor, fontSize: 11, fontWeight: 'bold' },
      gwDifUp: { color: upColor, fill: upColor, fontSize: 11, fontWeight: 'bold' },
      gwDifDown: { color: downColor, fill: downColor, fontSize: 11, fontWeight: 'bold' },
      gwMa2: { color: goldenWaveParams.value?.ma2LineColor || '#ef4444', fill: goldenWaveParams.value?.ma2LineColor || '#ef4444', fontSize: 11, fontWeight: 'bold' },
      gwCross: { color: 'rgba(226,232,240,0.85)', fill: 'rgba(226,232,240,0.85)', fontSize: 11 },
      gwCrossUp: { color: upColor, fill: upColor, fontSize: 11, fontWeight: 'bold' },
      gwCrossDown: { color: downColor, fill: downColor, fontSize: 11, fontWeight: 'bold' },
    }
  }

  function volumeLegendText(idx) {
    if (!Array.isArray(rawVolumes) || !rawVolumes.length) {
      const label = isIndex ? '成交金額' : '成交量'
      return `{volLabel|${label}: --}`
    }
    let i = (typeof idx === 'number') ? idx : (rawVolumes.length - 1)
    if (i < 0 || i >= rawVolumes.length) {
      i = rawVolumes.length - 1
    }
    const v = rawVolumes[i]
    const num = Number(v)
    if (!Number.isFinite(num)) {
      const label = isIndex ? '成交金額' : '成交量'
      return `{volLabel|${label}: --}`
    }

    // 決定目前這一根成交量柱是紅(上漲)還是綠(下跌)，沿用主圖/成交量的顏色邏輯
    let isUp = true
    if (i < rawOhlc.length) {
      if (isIndex) {
        if (i === 0) {
          isUp = true
        } else {
          // 指數：比較前一根與本根收盤價
          isUp = rawOhlc[i][1] >= rawOhlc[i-1][1]
        }
      } else {
        // 個股：比較本根開盤與收盤
        isUp = rawOhlc[i][1] >= rawOhlc[i][0]
      }
    }

    const valueText = (() => {
      if (isIndex) {
        const yi = num / 1e3
        if (yi >= 1e4) return `${(yi / 1e4).toFixed(1)} 兆`
        return `${yi.toFixed(1)} 億`
      }
      const lots = Math.round(num / 1000)
      return `${lots.toLocaleString()} 張`
    })()

    const richKey = isUp ? 'volUp' : 'volDown'
    const label = isIndex ? '成交金額' : '成交量'
    return `{volLabel|${label}: }{${richKey}|${valueText}}`
  }

  function resolveHmaLegendOverlayLayout() {
    const mobileFs = isFullscreen.value && (typeof window !== 'undefined' && window.innerWidth <= 768)
    const mobileChart = typeof window !== 'undefined' && (mobileFs || isMobileViewport())
    // Larger top = lower on chart; shrink for mobile so 多空線 sits closer to the top edge.
    let top = 34
    if (mobileFs) top = 38
    else if (mobileChart) top = 30
    let right = '6%'
    if (showDiagSR.value) {
      right = mobileChart ? '30%' : '16%'
      top = Math.max(16, top - 6)
    }
    if (mobileChart && showPinnedDockedLookupAside.value) {
      const dockTop = Number.parseFloat(chartContainer.value?.style?.getPropertyValue('--qg-pinned-aside-top') || '')
      const avoidTop = Number.isFinite(dockTop)
        ? dockTop + PINNED_LOOKUP_ASIDE_DOCK_HEIGHT_PX + 16
        : 64
      top = Math.max(top, avoidTop)
    }
    return { top, right, mobileChart }
  }

  function buildLegendTextsAt(idxForValues){
    const texts = []
    // BB text（布林通道，主圖疊加；左緣對齊均線圖例文字）
    if (showBB.value) {
      const bbFs = baseFontSize
      const bbLeft = qgBbLegendLayout.left || resolveBbLegendLeft(swapMobileMaLegendAndPinnedTip, containerWidth)
      const bbGraphic = {
        type: 'text',
        id: 'legend-text-bb',
        z: 90,
        zlevel: 3,
        left: bbLeft,
        top: bbLegendTopPx,
        silent: true,
        style: {
          text: bbLegendText(idxForValues),
          rich: getBbRich(bbFs),
          fill: 'rgba(226,232,240,0.85)',
          font: `500 ${bbFs}px sans-serif`,
          textAlign: 'left',
        }
      }
      texts.push(bbGraphic)
    }
    // HA text (static label)
    if (showHA.value) {
      texts.push({
        type: 'text',
        id: 'legend-text-ha',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof haLegendTop === 'number' ? haLegendTop : 0) + 2,
        silent: true,
        style: {
          text: '寶塔線',
          fill: 'rgba(226,232,240,0.9)',
          font: '600 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // CCI text
    if (showCCI.value) {
      texts.push({
        type: 'text',
        id: 'legend-text-cci',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof cciLegendTop === 'number' ? cciLegendTop : 0) + 2,
        silent: true,
        style: {
          text: cciLegendText(idxForValues),
          rich: cciRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // KD text
    if (showKD.value) {
      texts.push({
        type: 'text',
        id: 'legend-text-kd',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof kdLegendTop === 'number' ? kdLegendTop : 0) + 2,
        silent: true,
        style: {
          text: kdLegendText(idxForValues),
          rich: kdRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // RSI text
    if (showRSI.value) {
      texts.push({
        type: 'text',
        id: 'legend-text-rsi',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof rsiLegendTop === 'number' ? rsiLegendTop : 0) + 2,
        silent: true,
        style: {
          text: rsiLegendText(idxForValues),
          rich: rsiRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // HMA text（多空線）：固定畫在主圖區右上角，不依賴多空趨勢線副圖開關／位置
    if (showHMA.value) {
      const hmaLayout = resolveHmaLegendOverlayLayout()
      texts.push({
        type: 'text',
        id: 'legend-text-hma',
        z: 90,
        zlevel: 3,
        right: hmaLayout.right,
        top: hmaLayout.top,
        silent: true,
        style: {
          text: hmaLegendText(idxForValues),
          rich: hmaRich,
          fill: 'rgba(226,232,240,0.85)',
          font: (hmaLayout.mobileChart || hmaMobileMaLegend) ? '500 11px sans-serif' : '500 14px sans-serif',
          textAlign: 'right',
        }
      })
    }
    // MACD text
    if (showMACD.value) {
      texts.push({
        type: 'text',
        id: 'legend-text-macd',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof macdLegendTop === 'number' ? macdLegendTop : 0) + 2,
        silent: true,
        style: {
          text: macdLegendText(idxForValues),
          rich: macdRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // M4 text
    if (showM4Momentum.value) {
      texts.push({
        type: 'text',
        id: 'legend-text-m4',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof m4LegendTop === 'number' ? m4LegendTop : 0) + 2,
        silent: true,
        style: {
          text: m4LegendText(idxForValues),
          rich: m4Rich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // Expert (行家指標) text
    if (showExpert.value) {
      const expertRich = {
        expertLabel: { color: 'rgba(226,232,240,0.7)', fontSize: 11 },
        expertA0: { color: '#ef4444', fontSize: 11, fontWeight: 'bold' },
        expertB0: { color: '#22c55e', fontSize: 11, fontWeight: 'bold' },
        expertVal: { color: 'rgba(226,232,240,0.85)', fontSize: 11 },
        expertValUp: { color: '#ef4444', fontSize: 11, fontWeight: 'bold' },
        expertValDown: { color: '#22c55e', fontSize: 11, fontWeight: 'bold' }
      }
      texts.push({
        type: 'text',
        id: 'legend-text-expert',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof expertLegendTop === 'number' ? expertLegendTop : 0) + 2,
        silent: true,
        style: {
          text: expertLegendText(idxForValues),
          rich: expertRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    if (showHMAInd.value) {
      const hmaIndRich = hmaIndLegendRich()
      texts.push({
        type: 'text',
        id: 'legend-text-hmaind',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof hmaIndLegendTop === 'number' ? hmaIndLegendTop : 0) + 2,
        silent: true,
        style: {
          text: hmaIndLegendText(idxForValues),
          rich: hmaIndRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // GoldenWave (黃金波段) text
    if (showGoldenWave.value) {
      const gwRich = goldenWaveLegendRich()
      texts.push({
        type: 'text',
        id: 'legend-text-goldenwave',
        z: 90,
        zlevel: 3,
        left: '12%',
        top: (typeof goldenWaveLegendTop === 'number' ? goldenWaveLegendTop : 0) + 2,
        silent: true,
        style: {
          text: goldenWaveLegendText(idxForValues),
          rich: gwRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    // Volume label：在成交量子圖左上固定顯示「成交量」
    if (showVolume.value && typeof idxVol === 'number' && grids[idxVol]) {
      const volGrid = grids[idxVol]
      texts.push({
        type: 'text',
        id: 'legend-text-vol',
        z: 90,
        zlevel: 3,
        left: '12%',
        // 與成交量柱狀圖保持適中距離：比 -18 再略微往下貼近一些
        top: volGrid.top - 14,
        silent: true,
        style: {
          text: volumeLegendText(idxForValues),
          rich: volRich,
          fill: 'rgba(226,232,240,0.85)',
          font: '500 12px sans-serif',
          textAlign: 'left',
        }
      })
    }
    return texts
  }

  function buildGraphicAt(idxForValues) {
    return [...separators, ...buildLegendTextsAt(idxForValues), ...buildUserDrawingGraphics(chartInstance, dates)]
  }

  // Minimal updates for text overlays: only change style.text by element id
  function buildLegendTextUpdatesAt(idxForValues) {
    const updates = []
    if (showBB.value) {
      updates.push({
        id: 'legend-text-bb',
        left: qgBbLegendLayout.left || (qgMaLegendStripLayout.active ? resolveBbLegendLeft(true, chartContainer.value?.clientWidth) : '12%'),
        top: qgBbLegendLayout.topPx || undefined,
        style: {
          text: bbLegendText(idxForValues),
          rich: getBbRich(baseFontSize),
          textAlign: 'left',
        },
      })
    }
    if (showHA.value) {
      updates.push({ id: 'legend-text-ha', style: { text: '寶塔線' } })
    }
    if (showKD.value) {
      updates.push({ id: 'legend-text-kd', style: { text: kdLegendText(idxForValues), rich: kdRich } })
    }
    if (showRSI.value) {
      updates.push({ id: 'legend-text-rsi', style: { text: rsiLegendText(idxForValues), rich: rsiRich } })
    }
    if (showCCI.value) {
      updates.push({ id: 'legend-text-cci', style: { text: cciLegendText(idxForValues), rich: cciRich } })
    }
    if (showHMA.value) {
      const hmaLayout = resolveHmaLegendOverlayLayout()
      updates.push({
        id: 'legend-text-hma',
        right: hmaLayout.right,
        top: hmaLayout.top,
        style: { text: hmaLegendText(idxForValues), rich: hmaRich }
      })
    }
    if (showMACD.value) {
      updates.push({ id: 'legend-text-macd', style: { text: macdLegendText(idxForValues), rich: macdRich } })
    }
    if (showM4Momentum.value) {
      updates.push({ id: 'legend-text-m4', style: { text: m4LegendText(idxForValues), rich: m4Rich } })
    }
    if (showExpert.value) {
      const expertRich = {
        expertLabel: { color: 'rgba(226,232,240,0.7)', fontSize: 11 },
        expertA0: { color: '#ef4444', fontSize: 11, fontWeight: 'bold' },
        expertB0: { color: '#22c55e', fontSize: 11, fontWeight: 'bold' },
        expertVal: { color: 'rgba(226,232,240,0.85)', fontSize: 11 },
        expertValUp: { color: '#ef4444', fontSize: 11, fontWeight: 'bold' },
        expertValDown: { color: '#22c55e', fontSize: 11, fontWeight: 'bold' }
      }
      updates.push({ id: 'legend-text-expert', style: { text: expertLegendText(idxForValues), rich: expertRich } })
    }
    if (showHMAInd.value) {
      const hmaIndRich = hmaIndLegendRich()
      updates.push({ id: 'legend-text-hmaind', style: { text: hmaIndLegendText(idxForValues), rich: hmaIndRich } })
    }
    if (showGoldenWave.value) {
      const gwRich = goldenWaveLegendRich()
      updates.push({ id: 'legend-text-goldenwave', style: { text: goldenWaveLegendText(idxForValues), rich: gwRich } })
    }
    if (showVolume.value) {
      updates.push({ id: 'legend-text-vol', style: { text: volumeLegendText(idxForValues) } })
    }
    return updates
  }

  const lastIdxForLegend = Math.max(0, dates.length - 1)
  const pinnedMobileTip = usePinnedMobileTooltip()

  /** 主圖價格軸刻度：以目前 dataZoom 視窗內 OHLC（與該區間內均線）為準；啟用黃金切割時再與 Fib 價位合併（見 resolveMainYExtentForScale） */
  function computeVisibleMainYBoundsForAxis() {
    const n = Array.isArray(ohlc) ? ohlc.length : 0
    if (!n) return null
    let lo = Math.min(aiVisibleStartIdx.value, aiVisibleEndIdx.value)
    let hi = Math.max(aiVisibleStartIdx.value, aiVisibleEndIdx.value)
    lo = Math.max(0, Math.min(n - 1, Math.floor(lo)))
    hi = Math.max(0, Math.min(n - 1, Math.floor(hi)))
    const i0 = Math.min(lo, hi)
    const i1 = Math.max(lo, hi)
    let minP = Infinity
    let maxP = -Infinity
    for (let i = i0; i <= i1; i++) {
      const bar = ohlc[i]
      if (!bar || !Array.isArray(bar) || bar.length < 4) continue
      const low = Number(bar[2])
      const high = Number(bar[3])
      if (Number.isFinite(low) && low < minP) minP = low
      if (Number.isFinite(high) && high > maxP) maxP = high
    }
    function extendMa(series, enabled) {
      if (!enabled || !Array.isArray(series)) return
      for (let i = i0; i <= i1; i++) {
        const v = series[i]
        if (v === '-' || v === null || v === undefined) continue
        const num = Number(v)
        if (!Number.isFinite(num)) continue
        if (num < minP) minP = num
        if (num > maxP) maxP = num
      }
    }
    if (showMainK.value && !showReversal.value) {
      extendMa(ma1, showMA1.value)
      extendMa(ma2, showMA2.value)
      extendMa(ma3, showMA3.value)
      extendMa(ma4, showMA4.value)
      extendMa(ma5, showMA5.value)
    }
    if (!Number.isFinite(minP) || !Number.isFinite(maxP) || maxP <= minP) return null
    return { minP, maxP }
  }

  function resolveMainYExtentForScale(extentFromECharts) {
    let eff
    const vis = computeVisibleMainYBoundsForAxis()
    if (
      vis &&
      Number.isFinite(vis.minP) &&
      Number.isFinite(vis.maxP) &&
      vis.maxP > vis.minP
    ) {
      eff = { min: vis.minP, max: vis.maxP }
    } else {
      eff = {
        min: extentFromECharts.min,
        max: extentFromECharts.max,
      }
    }
    // Fib 依「全部載入資料」高低計算；僅依視窗縮 Y 軸時，較低分割價會落在座標外而消失
    if (showFib.value && Array.isArray(fibLevels) && fibLevels.length) {
      for (const lvl of fibLevels) {
        const v = Number(lvl?.value)
        if (!Number.isFinite(v)) continue
        eff.min = Math.min(eff.min, v)
        eff.max = Math.max(eff.max, v)
      }
    }
    return eff
  }

  const pinTipChartW =
    chartInstance && typeof chartInstance.getWidth === 'function'
      ? chartInstance.getWidth()
      : containerWidth || 0
  const pinnedMobileTipSideGutter = 6
  const pinnedMobileTipBarWidthPx =
    pinnedMobileTip ? Math.min(190, Math.max(176, Math.round(pinTipChartW * 0.3))) : 0
  /** 手機堆疊：有布林時 tip 對齊 dock；無布林維持均線下方原位 */
  const pinnedMobileTipTopPx =
    pinnedMobileTip && swapMobileMaLegendAndPinnedTip && maLegendRowShown
      ? Math.max(4, mobileDockTopPx || 24)
      : 24
  const crosshairLookupOn = !drawingMode.value && klineCrosshairLookupEnabled.value
  const option = {
    backgroundColor: 'transparent',
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    graphic: buildGraphicAt(lastIdxForLegend),
    legend: buildLegendsAt(lastIdxForLegend),
    tooltip: {
      show: !drawingMode.value && !shouldSuppressEchartsTooltipForMobileCrosshairDock(),
      alwaysShowContent:
        pinnedMobileTip &&
        !shouldSuppressEchartsTooltipForMobileCrosshairDock() &&
        !shouldShowPinnedLookupAsideLayout() &&
        !mobileTooltipDismissed.value &&
        mobilePinnedHoverIdx.value != null &&
        crosshairLookupOn,
      trigger: 'axis',
      confine: pinnedMobileTip,
      axisPointer: {
        type: 'cross',
        show: crosshairLookupOn
      },
      backgroundColor: pinnedMobileTip ? 'transparent' : 'rgba(15, 23, 42, 0.95)',
      borderColor: pinnedMobileTip ? 'transparent' : 'rgba(100, 200, 255, 0.5)',
      borderWidth: pinnedMobileTip ? 0 : 1,
      textStyle: pinnedMobileTip
        ? { color: 'rgba(226, 232, 240, 0.92)', fontSize: 11 }
        : { color: '#fff' },
      padding: pinnedMobileTip ? [4, 8, 4, 8] : [10, 14, 10, 14],
      extraCssText: pinnedMobileTip
        ? `border-radius: 0; box-shadow: none; background: transparent; border: 0; backdrop-filter: none; pointer-events: none; box-sizing: border-box; width: ${pinnedMobileTipBarWidthPx}px; max-width: none;`
        : 'border-radius: 10px; box-shadow: 0 12px 30px rgba(15,23,42,0.9); backdrop-filter: blur(10px); pointer-events: none;',
      position: function (pos, params, dom, rect, size) {
        if (pinnedMobileTip) {
          const viewWidth = Number(size?.viewSize?.[0]) || 0
          let gridLeftPx = Math.round(viewWidth * 0.05)
          try {
            const gridRect = chartInstance?.getModel?.()?.getComponent?.('grid', 0)?.coordinateSystem?.getRect?.()
            if (Number.isFinite(Number(gridRect?.x))) {
              gridLeftPx = Math.round(Number(gridRect.x))
            }
          } catch (_) {}
          // 查價卡左緣貼齊主圖 grid 的 Y 軸位置。
          const yAxisRightPx = gridLeftPx
          const x = Math.max(
            pinnedMobileTipSideGutter,
            Math.min(yAxisRightPx, viewWidth - pinnedMobileTipBarWidthPx - pinnedMobileTipSideGutter)
          )
          return [x, pinnedMobileTipTopPx]
        }
        return null
      },
      formatter: function (params) {
        const anyForIndex = params.find(p => p?.dataIndex != null)
        const hoverIdx = anyForIndex?.dataIndex
        if (typeof hoverIdx === 'number') {
          try { scheduleHoverOverlayUpdate(hoverIdx) } catch (_) {}
        }
        if (pinnedMobileTip && shouldShowPinnedLookupAsideLayout()) {
          return ''
        }

        const axisValue = params && params.length ? params[0].axisValue : null
        const candleParam = params.find(p => p.seriesType === 'candlestick')
        let o = null
        if (candleParam) {
          const dataArray = candleParam.value || candleParam.data
          if (dataArray && Array.isArray(dataArray)) {
            if (dataArray.length >= 5) {
              o = {
                open: Number(dataArray[1]),
                close: Number(dataArray[2]),
                low: Number(dataArray[3]),
                high: Number(dataArray[4]),
              }
            } else if (dataArray.length >= 4) {
              o = {
                open: Number(dataArray[0]),
                close: Number(dataArray[1]),
                low: Number(dataArray[2]),
                high: Number(dataArray[3]),
              }
            }
          }
        }
        if (!o && typeof hoverIdx === 'number' && chartData.value[hoverIdx]) {
          const row = chartData.value[hoverIdx]
          const open = Number(row?.open)
          const high = Number(row?.high)
          const low = Number(row?.low)
          const close = Number(row?.close)
          if ([open, high, low, close].every((n) => Number.isFinite(n))) {
            o = { open, high, low, close }
          }
        }

        let volLine = ''
        if (typeof hoverIdx === 'number' && Array.isArray(rawVolumes)) {
          const raw = rawVolumes[hoverIdx]
          const num = Number(raw)
          if (Number.isFinite(num)) {
            if (isIndex) {
              const yi = num / 1e3
              const valueText = (yi >= 1e4)
                ? `${(yi / 1e4).toFixed(1)} 兆`
                : `${yi.toFixed(1)} 億`
              volLine = `金額 ${valueText}`
            } else {
              const lots = Math.round(num / 1000)
              volLine = `量 ${lots.toLocaleString()} 張`
            }
          }
        }

        // 手機／全螢幕：放在股票名稱左邊的小卡，兩列呈現。
        if (pinnedMobileTip) {
          let pctNum = null
          let pctStr = ''
          if (
            o &&
            Number.isFinite(o.close) &&
            typeof hoverIdx === 'number' &&
            hoverIdx > 0 &&
            chartData.value[hoverIdx - 1]
          ) {
            const prevClose = Number(chartData.value[hoverIdx - 1].close)
            if (Number.isFinite(prevClose) && prevClose !== 0) {
              pctNum = ((o.close - prevClose) / prevClose) * 100
              pctStr = `${pctNum >= 0 ? '+' : ''}${pctNum.toFixed(2)}%`
            }
          }
          const pctCol = pctNum == null || !Number.isFinite(pctNum)
            ? 'rgba(226,232,240,0.8)'
            : (pctNum >= 0 ? '#f87171' : '#4ade80')
          const oc = o && Number.isFinite(o.open) ? o.open.toFixed(2) : '--'
          const cc = o && Number.isFinite(o.close) ? o.close.toFixed(2) : '--'
          const hc = o && Number.isFinite(o.high) ? o.high.toFixed(2) : '--'
          const lc = o && Number.isFinite(o.low) ? o.low.toFixed(2) : '--'

          const dot = '<span style="display:inline-block;margin:0 0.12em;color:rgba(148,163,184,0.35);font-weight:400;font-size:9px;flex-shrink:0">·</span>'

          let html = '<div style="position:relative;box-sizing:border-box;width:100%;min-height:34px;padding:2px 2px;display:flex;flex-direction:column;justify-content:center;line-height:1.35;text-align:left;pointer-events:none">'
          html += '<div style="min-width:0;display:flex;flex-flow:row nowrap;align-items:baseline;white-space:nowrap;overflow:visible;font-size:10px;font-weight:600;color:rgba(248,250,252,0.93);letter-spacing:0.01em;font-variant-numeric:tabular-nums;pointer-events:none">'

          if (axisValue != null) {
            html += `<span style="color:rgba(226,232,240,0.85);font-weight:600;flex-shrink:0">${axisValue}</span>`
          }
          if (axisValue != null) html += dot

          html += `<span style="flex-shrink:0">收 <span style="color:rgba(248,250,252,0.98)">${cc}</span>`
          if (pctStr) {
            html += `<span style="color:${pctCol};font-weight:700;margin-left:0.28em">${pctStr}</span>`
          }
          html += '</span>'
          html += '</div>'

          html += '<div style="min-width:0;display:flex;flex-flow:row nowrap;align-items:baseline;white-space:nowrap;overflow:visible;font-size:10px;font-weight:500;color:rgba(148,163,184,0.92);letter-spacing:0.01em;font-variant-numeric:tabular-nums;pointer-events:none">'
          if (o && [o.open, o.high, o.low].every((n) => Number.isFinite(n))) {
            html += `<span style="flex-shrink:0">開 ${oc}</span>${dot}<span style="flex-shrink:0">高 ${hc}</span>${dot}<span style="flex-shrink:0">低 ${lc}</span>`
          }
          if (volLine) {
            if (o && [o.open, o.high, o.low].every((n) => Number.isFinite(n))) html += dot
            html += `<span style="font-weight:500;color:rgba(148,163,184,0.88);flex-shrink:0">${volLine}</span>`
          }

          html += '</div></div>'
          return html
        }

        // 桌面：維持多行 OHLC／量
        let result = ''
        if (params && params.length) {
          if (axisValue != null) {
            result += `${axisValue}<br/>`
          }
        }

        if (o && [o.open, o.close, o.low, o.high].every((n) => Number.isFinite(n))) {
          result += `開盤: ${o.open.toFixed(2)}<br/>`
          result += `最高: ${o.high.toFixed(2)}<br/>`
          result += `最低: ${o.low.toFixed(2)}<br/>`
          result += `收盤: ${o.close.toFixed(2)}<br/>`
        }

        if (typeof hoverIdx === 'number' && Array.isArray(rawVolumes)) {
          const raw = rawVolumes[hoverIdx]
          const num = Number(raw)
          if (Number.isFinite(num)) {
            if (isIndex) {
              const yi = num / 1e3
              const valueText = (yi >= 1e4)
                ? `${(yi / 1e4).toFixed(1)} 兆`
                : `${yi.toFixed(1)} 億`
              result += `成交金額: ${valueText}<br/>`
            } else {
              const lots = Math.round(num / 1000)
              result += `成交量: ${lots.toLocaleString()} 張<br/>`
            }
          }
        }

        return result
      }
    },
    axisPointer: {
      show: crosshairLookupOn,
      link: [{ xAxisIndex: 'all' }]
    },
    grid: grids,
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: (isWeek || isMonth) ? true : narrowMobileForKline,
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: { show: false },
        splitLine: { show: false },
        min: 'dataMin',
        max: 'dataMax'
      },
      ...(showHA.value ? [{
        type: 'category',
        gridIndex: idxHA,
        data: dates,
        boundaryGap: true,
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          margin: 12,
          hideOverlap: true,
          show: false
        },
        splitLine: { show: false }
      }] : []),
      ...(showKD.value ? [{
        type: 'category',
        gridIndex: idxKD,
        data: dates,
        boundaryGap: false,
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          margin: 12,
          hideOverlap: true,
          show: false
        },
        splitLine: { show: false }
      }] : []),
      ...(showRSI.value ? [{
        type: 'category',
        gridIndex: idxRSI,
        data: dates,
        boundaryGap: false,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          margin: 12,
          hideOverlap: true,
          show: false
        },
        splitLine: { show: false }
      }] : []),
      ...(showCCI.value ? [{
        type: 'category',
        gridIndex: idxCCI,
        data: dates,
        boundaryGap: false,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          margin: 12,
          hideOverlap: true,
          show: false
        },
        splitLine: { show: false }
      }] : []),
      ...(showMACD.value ? [{
        type: 'category',
        gridIndex: idxMACD,
        data: dates,
        boundaryGap: false,
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          margin: 12,
          hideOverlap: true,
          show: false
        },
        splitLine: { show: false }
      }] : []),
      ...(showM4Momentum.value ? [{
        type: 'category',
        gridIndex: idxM4,
        data: dates,
        boundaryGap: false,
        axisLine: {
          lineStyle: { color: 'rgba(255, 255, 255, 0.3)' }
        },
        axisLabel: {
          show: false
        },
        axisTick: { show: false },
        splitLine: { show: false }
      }] : []),
      ...(showExpert.value ? [{
        type: 'category',
        gridIndex: idxExpert,
        data: dates,
        boundaryGap: false,
        axisLine: {
          lineStyle: { color: 'rgba(255, 255, 255, 0.3)' }
        },
        axisLabel: {
          show: false
        },
        axisTick: { show: false },
        splitLine: { show: false }
      }] : []),
      ...(showHMAInd.value ? [{
        type: 'category',
        gridIndex: idxHMAInd,
        data: dates,
        boundaryGap: false,
        axisLine: {
          lineStyle: { color: 'rgba(255, 255, 255, 0.3)' }
        },
        axisLabel: {
          show: false
        },
        axisTick: { show: false },
        splitLine: { show: false }
      }] : []),
      ...(showGoldenWave.value ? [{
        type: 'category',
        gridIndex: idxGoldenWave,
        data: dates,
        boundaryGap: false,
        axisLine: {
          lineStyle: { color: 'rgba(255, 255, 255, 0.3)' }
        },
        axisLabel: {
          show: false
        },
        axisTick: { show: false },
        splitLine: { show: false }
      }] : []),
      ...(showVolume.value ? [{
        type: 'category',
        gridIndex: idxVol,
        data: dates,
        boundaryGap: false,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          margin: 12,
          hideOverlap: true,
          show: true
        },
        splitLine: { show: false }
      }] : [])
    ],
    yAxis: [
      {
        scale: true,
        // 主圖價格軸：依目前可見資料自動收窄上下界
        // 指數(^TWII/TWII)使用較小 padding 以放大細微波動
        min: function (value) {
          const eff = resolveMainYExtentForScale(value)
          const range = eff.max - eff.min
          if (!Number.isFinite(range) || range <= 0) return eff.min
          let ratioLow
          if (showFib.value) {
            // 開啟黃金切割：較鬆的底部留白，與「含 Fib 全距離」的 Y 軸刻度搭配（維持目前 Fib 模式體感）
            ratioLow = isIndex ? 0.015 : 0.05
            if (isMobileLayout) {
              ratioLow += isIndex ? 0.022 : 0.06
            }
          } else {
            // 未開 Fib：縮小底部預留，拉高 K 線主圖可用高度
            ratioLow = isIndex ? 0.012 : 0.035
            if (isMobileLayout) {
              ratioLow += isIndex ? 0.012 : 0.04
            }
          }
          const padding = range * ratioLow
          return eff.min - padding
        },
        max: function (value) {
          const eff = resolveMainYExtentForScale(value)
          const range = eff.max - eff.min
          if (!Number.isFinite(range) || range <= 0) return eff.max
          const ratioHigh = isIndex ? 0.015 : 0.05
          const padding = range * ratioHigh
          return eff.max + padding
        },
        splitArea: {
          show: false
        },
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          width: 60,
          overflow: 'truncate',
          align: 'right',
          formatter: function (value) {
            return Number(value).toFixed(0)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      },
      ...(showHA.value ? [{
        scale: true,
        gridIndex: idxHA,
        splitNumber: 2,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(1)
          }
        },
        splitLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.1)' }
        }
      }] : []),
      // Reversal 線移到主圖疊加，不再使用獨立 yAxis
      ...(showKD.value ? [{
        scale: true,
        gridIndex: idxKD,
        min: 0,
        max: 100,
        splitNumber: 2,
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(0)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showRSI.value ? [{
        scale: true,
        gridIndex: idxRSI,
        min: 0,
        max: 100,
        splitNumber: 2,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(0)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showCCI.value ? [{
        scale: true,
        gridIndex: idxCCI,
        splitNumber: 2,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(0)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showMACD.value ? [{
        scale: true,
        gridIndex: idxMACD,
        splitNumber: 2,
        min: -macdScale,
        max: macdScale,
        boundaryGap: ['10%', '10%'],
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(2)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showM4Momentum.value ? [{
        scale: true,
        gridIndex: idxM4,
        splitNumber: 2,
        min: -m4Scale,
        max: m4Scale,
        boundaryGap: ['10%', '10%'],
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { show: false },
        splitLine: { show: false }
      }] : []),
      ...(showExpert.value ? [{
        scale: true,
        gridIndex: idxExpert,
        splitNumber: 2,
        min: 0,
        max: 100,
        boundaryGap: ['5%', '5%'],
        axisLine: { 
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' } 
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 40,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(0)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showHMAInd.value ? [{
        scale: true,
        gridIndex: idxHMAInd,
        splitNumber: 2,
        min: function (value) {
          const range = value.max - value.min
          if (!Number.isFinite(range) || range <= 0) return value.min
          const pct = Number(hmaIndParams.value?.paddingPct)
          const ratio = (Number.isFinite(pct) ? pct : 3) / 100
          const padding = range * ratio
          return value.min - padding
        },
        max: function (value) {
          const range = value.max - value.min
          if (!Number.isFinite(range) || range <= 0) return value.max
          const pct = Number(hmaIndParams.value?.paddingPct)
          const ratio = (Number.isFinite(pct) ? pct : 3) / 100
          const padding = range * ratio
          return value.max + padding
        },
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(2)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showGoldenWave.value ? [{
        scale: true,
        gridIndex: idxGoldenWave,
        splitNumber: 2,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 10,
          width: 60,
          overflow: 'truncate',
          formatter: function (value) {
            return Number(value).toFixed(2)
          }
        },
        splitLine: {
          lineStyle: {
            color: 'rgba(100, 200, 255, 0.1)'
          }
        }
      }] : []),
      ...(showVolume.value ? [{
        scale: true,
        gridIndex: idxVol,
        splitNumber: 2,
        min: 0,
        axisLine: {
          lineStyle: { color: 'rgba(100, 200, 255, 0.3)' }
        },
        axisLabel: {
          color: 'rgba(226, 232, 240, 0.6)',
          fontSize: 11,
          width: 60,
          overflow: 'truncate',
          align: 'right',
          formatter: function (value) {
            if (isIndex) {
              const v = Number(value)
              if (!Number.isFinite(v)) return String(value)
              if (v >= 1e8) return (v / 1e8).toFixed(1) + '\u5104'
              if (v >= 1e4) return (v / 1e4).toFixed(1) + '\u842c'
              return v.toLocaleString()
            } else {
              const num = Number(value)
              if (!Number.isFinite(num)) return String(value)
              // 個股：Y 軸直接顯示張數（不附加單位文字）
              return num.toLocaleString()
            }
          }
        },
        splitLine: { show: false }
      }] : [])
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: 'all',
        // 預設顯示根數：日120 / 週80 / 月48
        startValue: Math.max(0, dates.length - (isMonth ? 48 : (isWeek ? 80 : 120))),
        endValue: Math.max(0, dates.length - 1),
        filterMode: 'filter',
        zoomOnMouseWheel: true,
        moveOnMouseMove: true,
        moveOnMouseWheel: true
      },
      {
        type: 'slider',
        show: false,
        xAxisIndex: 'all',
        startValue: Math.max(0, dates.length - (isMonth ? 48 : (isWeek ? 80 : 120))),
        endValue: Math.max(0, dates.length - 1),
        filterMode: 'filter',
        bottom: 6,
        height: 18,
        backgroundColor: 'rgba(15, 23, 42, 0.5)',
        fillerColor: 'rgba(59, 130, 246, 0.2)',
        borderColor: 'rgba(100, 200, 255, 0.2)',
        handleStyle: { color: 'rgba(59, 130, 246, 0.8)' },
        textStyle: { color: 'rgba(226, 232, 240, 0.6)', fontSize: 10 }
      }
    ],
    series: [
      // 主K線僅在 showMainK 且未單獨查看轉折線時顯示
      ...(showMainK.value && !showReversal.value ? [{
        name: chartMode.value === 'heikin' ? '神奇K線' : 'K線',
        type: 'candlestick',
        data: ohlc,
        barWidth: candleBarWidth,
        barCategoryGap: candleCategoryGap,
        barMinWidth: candleMinW,
        barMaxWidth: candleMaxW,
        // 提升主K線層級，避免被均線與其他指標線蓋住
        z: 5,
        itemStyle: {
          color: upCandleColor,
          color0: downCandleColor,
          borderColor: upCandleBorder,
          borderColor0: downCandleBorder,
          opacity: 1,
          borderWidth: narrowMobileForKline ? 1.2 : (isIndex ? 1.8 : 1.1),
          shadowBlur: narrowMobileForKline ? 0 : (isIndex ? 10 : 5),
          shadowColor: narrowMobileForKline
            ? 'transparent'
            : (isIndex ? 'rgba(255,255,255,0.18)' : 'rgba(15,23,42,0.55)'),
          shadowOffsetY: narrowMobileForKline ? 0 : 1,
        },
        emphasis: { disabled: true },
        markLine: showFib.value && fibLevels && fibLevels.length ? {
          symbol: 'none',
          silent: true,
          lineStyle: {
            color: '#facc15',
            width: 1,
            type: 'dashed'
          },
          label: {
            show: true,
            position: 'end',
            color: '#facc15',
            fontSize: 11,
            formatter: function (param) {
              const d = param && param.data ? param.data : {}
              const ratio = typeof d.ratio === 'number' ? d.ratio : null
              const valRaw = d.value != null ? d.value : param.value
              const valNum = Number(valRaw)
              if (!Number.isFinite(valNum)) return ''
              const priceStr = valNum.toFixed(2)
              if (ratio == null || !Number.isFinite(ratio)) return priceStr
              const pct = (ratio * 100).toFixed(1).replace(/\.0$/, '') + '%'
              return pct + '  ' + priceStr
            }
          },
          data: fibLevels.map(function (level) {
            return {
              yAxis: level.value,
              value: level.value,
              ratio: level.ratio
            }
          })
        } : undefined
      }] : []),
      ...fibSeries,
      ...diagSrSeries,
      // Reversal 線：上漲段（紅）、下跌段（綠），皆畫在主圖座標，可由控制面板獨立開關
      ...(showReversalLines && reversalData ? [
        ...(showReversalUp.value ? [{
          name: '轉折線-上漲',
          type: 'line',
          step: 'end',
          smooth: false,
          xAxisIndex: idxMain,
          yAxisIndex: idxMain,
          // 若有計算與 HMA 相對應的顏色資料，優先使用；否則退回原本 red 資料
          data: reversalColored && Array.isArray(reversalColored.up) ? reversalColored.up : reversalData.red,
          showSymbol: false,
          connectNulls: false,
          lineStyle: {
            width: 2,
            color: '#ef4444'
          }
        }] : []),
        ...(showReversalDown.value ? [{
          name: '轉折線-下跌',
          type: 'line',
          step: 'end',
          smooth: false,
          xAxisIndex: idxMain,
          yAxisIndex: idxMain,
          // 若有計算與 HMA 相對應的顏色資料，優先使用；否則退回原本 green 資料
          data: reversalColored && Array.isArray(reversalColored.down) ? reversalColored.down : reversalData.green,
          showSymbol: false,
          connectNulls: false,
          lineStyle: {
            width: 2,
            color: '#22c55e'
          }
        }] : [])
      ] : []),
      // 轉折線多空訊號（三角形）
      ...(showReversal.value && reversalSignals ? [] : []),
      ...(showHA.value && towerSeries ? [
        {
          name: '寶塔線-紅',
          type: 'line',
          step: 'end',
          smooth: false,
          xAxisIndex: idxHA,
          yAxisIndex: idxHA,
          data: towerSeries.red,
          showSymbol: false,
          connectNulls: false,
          lineStyle: {
            width: 2,
            color: '#ef4444'
          }
        },
        {
          name: '寶塔線-綠',
          type: 'line',
          step: 'end',
          smooth: false,
          xAxisIndex: idxHA,
          yAxisIndex: idxHA,
          data: towerSeries.green,
          showSymbol: false,
          connectNulls: false,
          lineStyle: {
            width: 2,
            color: '#22c55e'
          }
        }
      ] : []),
      // MA 線：當主K線顯示且未單看轉折線時，永遠建立五條 MA 系列；可見性交由 legend 選取與面板勾選控制
      ...(showMainK.value && !showReversal.value ? [
        {
          name: `MA${maParams.value.ma1}`,
          type: 'line',
          data: showMA1.value ? ma1 : (Array.isArray(ma1) ? ma1.map(() => '-') : []),
          smooth: true,
          lineStyle: { width: isIndex ? 1.0 : 1.5, color: '#3b82f6' },
          showSymbol: false
        },
        {
          name: `MA${maParams.value.ma2}`,
          type: 'line',
          data: showMA2.value ? ma2 : (Array.isArray(ma2) ? ma2.map(() => '-') : []),
          smooth: true,
          lineStyle: { width: isIndex ? 1.0 : 1.5, color: '#a855f7' },
          showSymbol: false
        },
        {
          name: `MA${maParams.value.ma3}`,
          type: 'line',
          data: showMA3.value ? ma3 : (Array.isArray(ma3) ? ma3.map(() => '-') : []),
          smooth: true,
          lineStyle: { width: isIndex ? 1.0 : 1.5, color: '#f59e0b' },
          showSymbol: false
        },
        {
          name: `MA${maParams.value.ma4}`,
          type: 'line',
          data: showMA4.value ? ma4 : (Array.isArray(ma4) ? ma4.map(() => '-') : []),
          smooth: true,
          lineStyle: { width: isIndex ? 1.0 : 1.5, color: '#10b981' },
          showSymbol: false
        },
        {
          name: `MA${maParams.value.ma5}`,
          type: 'line',
          data: showMA5.value ? ma5 : (Array.isArray(ma5) ? ma5.map(() => '-') : []),
          smooth: true,
          lineStyle: { width: isIndex ? 1.0 : 1.5, color: '#f97316' },
          showSymbol: false
        }
      ] : []),
      // MA 扣抵三角形：不加入 legend，只在主圖下方顯示當前各均線扣抵位置
      ...maOffsetSeries,
      // 布林通道（主圖疊加）：上／中／下軌可各自設定顏色，並以堆疊技巧在上下軌間鋪一層淡色底色
      ...(showBB.value && bbData ? [
        {
          id: 'series-bb-band-base',
          name: '__bb_band_base__',
          type: 'line',
          data: bbData.lower,
          stack: 'bb-band',
          symbol: 'none',
          silent: true,
          lineStyle: { opacity: 0 },
          areaStyle: { opacity: 0 },
          tooltip: { show: false },
          z: 1
        },
        {
          id: 'series-bb-band-fill',
          name: '__bb_band_fill__',
          type: 'line',
          data: bbData.bandWidth,
          stack: 'bb-band',
          symbol: 'none',
          silent: true,
          lineStyle: { opacity: 0 },
          areaStyle: { color: hexToRgba(bbParams.value.colorMid || BB_DEFAULT_COLORS.colorMid, 0.10) },
          tooltip: { show: false },
          z: 1
        },
        {
          name: `BB上軌(${bbParams.value.period},${bbParams.value.mult})`,
          type: 'line',
          data: bbData.upper,
          smooth: true,
          clip: true,
          showSymbol: false,
          itemStyle: { color: bbParams.value.colorUpper || BB_DEFAULT_COLORS.colorUpper },
          lineStyle: { width: 1.5, color: bbParams.value.colorUpper || BB_DEFAULT_COLORS.colorUpper, type: 'solid' },
          tooltip: { show: false },
          z: 3
        },
        {
          name: `BB中軌(${bbParams.value.period},${bbParams.value.mult})`,
          type: 'line',
          data: bbData.mid,
          smooth: true,
          clip: true,
          showSymbol: false,
          itemStyle: { color: bbParams.value.colorMid || BB_DEFAULT_COLORS.colorMid },
          lineStyle: { width: 1.5, color: bbParams.value.colorMid || BB_DEFAULT_COLORS.colorMid, type: 'solid' },
          tooltip: { show: false },
          z: 3
        },
        {
          name: `BB下軌(${bbParams.value.period},${bbParams.value.mult})`,
          type: 'line',
          data: bbData.lower,
          smooth: true,
          clip: true,
          showSymbol: false,
          itemStyle: { color: bbParams.value.colorLower || BB_DEFAULT_COLORS.colorLower },
          lineStyle: { width: 1.5, color: bbParams.value.colorLower || BB_DEFAULT_COLORS.colorLower, type: 'solid' },
          tooltip: { show: false },
          z: 3
        }
      ] : []),
      // 多空線：上彎紅、下彎天藍；窄螢幕加粗以便辨識。
      ...(showHMA.value && hma ? [
        {
          name: `多空線(${hmaParams.value.period})`,
          type: 'line',
          xAxisIndex: idxMain,
          yAxisIndex: idxMain,
          data: hma.flatDown,
          smooth: true,
          clip: true,
          lineStyle: {
            width: hmaMobileMaLegend ? (isIndex ? 1.8 : 2.2) : (isIndex ? 3 : 4),
            color: '#38bdf8',
          },
          showSymbol: false,
          z: 4,
        },
        {
          name: `多空線(${hmaParams.value.period})`,
          type: 'line',
          xAxisIndex: idxMain,
          yAxisIndex: idxMain,
          data: hma.up,
          smooth: true,
          clip: true,
          lineStyle: {
            width: hmaMobileMaLegend ? (isIndex ? 1.8 : 2.2) : (isIndex ? 3 : 4),
            color: '#ef4444',
          },
          showSymbol: false,
          z: 5,
        },
      ] : []),
      ...(showHMAInd.value && hmaIndData && typeof idxHMAInd === 'number' ? [
        {
          id: 'series-hmaind-up',
          name: `多空趨勢線(${hmaIndParams.value.period})`,
          type: 'scatter',
          xAxisIndex: idxHMAInd,
          yAxisIndex: idxHMAInd,
          data: hmaIndData.up,
          clip: true,
          symbol: 'circle',
          symbolSize: ((typeof window !== 'undefined' && window.innerWidth <= 768)
            ? (isIndex ? 7 : 8)
            : (isIndex ? 5 : 6)),
          itemStyle: {
            color: hmaIndParams.value.upColor
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-hmaind-down',
          name: `多空趨勢線(${hmaIndParams.value.period})`,
          type: 'scatter',
          xAxisIndex: idxHMAInd,
          yAxisIndex: idxHMAInd,
          data: hmaIndData.flatDown,
          clip: true,
          symbol: 'circle',
          symbolSize: ((typeof window !== 'undefined' && window.innerWidth <= 768)
            ? (isIndex ? 7 : 8)
            : (isIndex ? 5 : 6)),
          itemStyle: {
            color: hmaIndParams.value.downColor
          },
          markLine: hmaIndParams.value.zeroVisible ? {
            symbol: 'none',
            silent: true,
            lineStyle: { color: 'rgba(226,232,240,0.35)', width: 1, type: 'solid' },
            label: { show: false },
            data: [{ yAxis: 0 }]
          } : undefined,
          emphasis: { disabled: true },
          z: 3
        }
      ] : []),
      // KD 指標：固定使用線圖模式（移除點陣模式切換），並可獨立顯示 K / D 線且可加粗
      ...(showKD.value && kdData ? [
        ...(showKLine.value ? [{
          name: `KD(${kdParams.value.period},${kdParams.value.k},${kdParams.value.d}): K`,
          type: 'line',
          xAxisIndex: idxKD,
          yAxisIndex: idxKD,
          data: kdData.k,
          smooth: true,
          clip: true,
          lineStyle: {
            width: kdBold.value ? 3 : 2,
            color: '#f59e0b'
          },
          showSymbol: false,
          markLine: kdMidline.value ? {
            symbol: 'none',
            silent: true,
            z: 10,
            lineStyle: { color: '#ef4444', width: 3, type: 'solid' },
            label: {
              show: true,
              position: 'end',
              formatter: () => '50',
              color: '#ef4444',
              fontSize: 13,
              fontWeight: '800',
              backgroundColor: 'transparent',
              padding: 0,
              borderRadius: 0
            },
            data: [{ yAxis: 50 }]
          } : undefined
        }] : []),
        ...(showDLine.value ? [{
          name: `KD(${kdParams.value.period},${kdParams.value.k},${kdParams.value.d}): D`,
          type: 'line',
          xAxisIndex: idxKD,
          yAxisIndex: idxKD,
          data: kdData.d,
          smooth: true,
          clip: true,
          lineStyle: {
            width: kdBold.value ? 3 : 2,
            color: '#3b82f6'
          },
          showSymbol: false,
          markLine: !showKLine.value && kdMidline.value ? {
            symbol: 'none',
            silent: true,
            z: 10,
            lineStyle: { color: '#ef4444', width: 3, type: 'solid' },
            label: {
              show: true,
              position: 'end',
              formatter: () => '50',
              color: '#ef4444',
              fontSize: 13,
              fontWeight: '800',
              backgroundColor: 'transparent',
              padding: 0,
              borderRadius: 0
            },
            data: [{ yAxis: 50 }]
          } : undefined
        }] : [])
      ] : []),
      ...(showRSI.value && rsiData ? [{
        name: `RSI(${rsiParams.value.period})`,
        type: 'line',
        xAxisIndex: idxRSI,
        yAxisIndex: idxRSI,
        data: rsiData,
        smooth: true,
        clip: true,
        showSymbol: false,
        lineStyle: {
          width: 2,
          color: '#c084fc'
        },
        markLine: {
          symbol: 'none',
          silent: true,
          z: 10,
          animation: false,
          lineStyle: { type: 'dashed', width: 1 },
          label: {
            show: true,
            position: 'end',
            fontSize: 11,
            fontWeight: '700'
          },
          data: [
            {
              yAxis: rsiParams.value.overbought,
              lineStyle: { color: '#ef4444' },
              label: { color: '#ef4444', formatter: () => String(rsiParams.value.overbought) }
            },
            {
              yAxis: rsiParams.value.oversold,
              lineStyle: { color: '#22c55e' },
              label: { color: '#22c55e', formatter: () => String(rsiParams.value.oversold) }
            }
          ]
        }
      }] : []),
      ...(showCCI.value && cciData ? [{
        name: 'ORC',
        type: 'bar',
        xAxisIndex: idxCCI,
        yAxisIndex: idxCCI,
        data: cciData,
        clip: true,
        barWidth: `${Math.min(100, Math.max(10, Number(orcStyle.barWidthPct) || 60))}%`,
        itemStyle: {
          color: function (params) {
            const v = Number(params.value)
            const neg = orcStyle.colorNeg || '#22c55e'
            const mid = orcStyle.colorMid || '#f59e0b'
            const pos = orcStyle.colorPos || '#ef4444'
            if (!Number.isFinite(v)) return neg
            if (v <= 0) return neg
            if (v < 100) return mid
            return pos
          }
        }
      }] : []),
      ...(showMACD.value && macdData ? [
        ...(hasMacdDisplay('dif') ? [{
          name: `MACD(${macdParams.value.fast},${macdParams.value.slow},${macdParams.value.signal}): MACD`,
          type: 'line',
          xAxisIndex: idxMACD,
          yAxisIndex: idxMACD,
          data: macdData.macd,
          smooth: true,
          clip: true,
          lineStyle: {
            width: macdLineWidths.value.dif,
            color: '#3b82f6'
          },
          showSymbol: false
        }] : []),
        ...(hasMacdDisplay('macd') ? [{
          name: `MACD(${macdParams.value.fast},${macdParams.value.slow},${macdParams.value.signal}): Signal`,
          type: 'line',
          xAxisIndex: idxMACD,
          yAxisIndex: idxMACD,
          data: macdData.signal,
          smooth: true,
          clip: true,
          lineStyle: {
            width: macdLineWidths.value.macd,
            color: '#f59e0b'
          },
          showSymbol: false
        }] : []),
        ...(hasMacdDisplay('osc') ? [{
          name: `MACD(${macdParams.value.fast},${macdParams.value.slow},${macdParams.value.signal}): Histogram`,
          type: 'bar',
          xAxisIndex: idxMACD,
          yAxisIndex: idxMACD,
          // 僅在視覺上放大柱狀圖高度：計算與文字仍使用原始 histogram
          data: macdHistogramVisual || macdData.histogram,
          clip: true,
          barWidth: `${Math.min(100, Math.max(10, Number(macdOscStyle.value.barWidth) || 60))}%`,
          itemStyle: {
            color: function(params) {
              return params.value > 0
                ? hexToRgba(macdOscStyle.value.colorUp, macdOscStyle.value.opacityUp)
                : hexToRgba(macdOscStyle.value.colorDown, macdOscStyle.value.opacityDown)
            },
            borderColor: 'transparent',
            borderWidth: 0,
            shadowBlur: 8,
            shadowColor: function(params) {
              return params.value > 0
                ? hexToRgba(macdOscStyle.value.colorUp, Math.min(0.7, Math.max(0.15, macdOscStyle.value.opacityUp * 0.45)))
                : hexToRgba(macdOscStyle.value.colorDown, Math.min(0.7, Math.max(0.15, macdOscStyle.value.opacityDown * 0.45)))
            }
          }
        }] : [])
      ] : []),
      ...(showM4Momentum.value && m4LineByState ? [
        {
          id: 'series-m4-zero',
          name: `四狀態動能(${m4MomentumParams.value.fast},${m4MomentumParams.value.slow},${m4MomentumParams.value.signal})`,
          type: 'line',
          xAxisIndex: idxM4,
          yAxisIndex: idxM4,
          data: dates.map(() => 0),
          smooth: false,
          clip: true,
          silent: true,
          lineStyle: { width: 1, color: 'rgba(226, 232, 240, 0.18)' },
          showSymbol: false,
          z: 1
        },
        {
          id: 'series-m4-range',
          name: `四狀態動能(${m4MomentumParams.value.fast},${m4MomentumParams.value.slow},${m4MomentumParams.value.signal})`,
          type: 'line',
          xAxisIndex: idxM4,
          yAxisIndex: idxM4,
          data: m4LineByState.range,
          smooth: true,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 3,
            color: m4LineByState.colorRange,
            shadowBlur: 10,
            shadowColor: rgbaFromHex(m4LineByState.colorRange, 0.55, 'rgba(59, 130, 246, 0.35)')
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-m4-squeeze',
          name: `四狀態動能(${m4MomentumParams.value.fast},${m4MomentumParams.value.slow},${m4MomentumParams.value.signal})`,
          type: 'line',
          xAxisIndex: idxM4,
          yAxisIndex: idxM4,
          data: m4LineByState.squeeze,
          smooth: true,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 3,
            color: m4LineByState.colorSqueeze,
            shadowBlur: 10,
            shadowColor: rgbaFromHex(m4LineByState.colorSqueeze, 0.55, 'rgba(56, 189, 248, 0.35)')
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-m4-up',
          name: `四狀態動能(${m4MomentumParams.value.fast},${m4MomentumParams.value.slow},${m4MomentumParams.value.signal})`,
          type: 'line',
          xAxisIndex: idxM4,
          yAxisIndex: idxM4,
          data: m4LineByState.up,
          smooth: true,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 3,
            color: m4LineByState.colorUp,
            shadowBlur: 10,
            shadowColor: rgbaFromHex(m4LineByState.colorUp, 0.55, 'rgba(239, 68, 68, 0.35)')
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-m4-down',
          name: `四狀態動能(${m4MomentumParams.value.fast},${m4MomentumParams.value.slow},${m4MomentumParams.value.signal})`,
          type: 'line',
          xAxisIndex: idxM4,
          yAxisIndex: idxM4,
          data: m4LineByState.down,
          smooth: true,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 3,
            color: m4LineByState.colorDown,
            shadowBlur: 10,
            shadowColor: rgbaFromHex(m4LineByState.colorDown, 0.55, 'rgba(34, 197, 94, 0.35)')
          },
          emphasis: { disabled: true },
          z: 3
        }
      ] : []),
      ...(showExpert.value && expertData ? [
        // A0 線 (紅色虛線)
        {
          id: 'series-expert-a0',
          name: `A0(${expertParams.value.length})`,
          type: 'line',
          xAxisIndex: idxExpert,
          yAxisIndex: idxExpert,
          data: expertData.a0,
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 1.5,
            color: '#dc2626',
            type: 'dashed'
          },
          emphasis: { disabled: true },
          z: 4
        },
        // B0 線 (綠色虛線)
        {
          id: 'series-expert-b0',
          name: `B0(${expertParams.value.length})`,
          type: 'line',
          xAxisIndex: idxExpert,
          yAxisIndex: idxExpert,
          data: expertData.b0,
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 1.5,
            color: '#16a34a',
            type: 'dashed'
          },
          emphasis: { disabled: true },
          z: 4
        },
        // 柱狀體：填在 A0 與 B0 之間（有長短）
        {
          id: 'series-expert-interval-bars',
          name: `行家指標(${expertParams.value.length})`,
          type: 'custom',
          xAxisIndex: idxExpert,
          yAxisIndex: idxExpert,
          clip: true,
          renderItem: function (params, api) {
            const x = api.value(0)
            const y0 = api.value(1)
            const y1 = api.value(2)
            const v = api.value(3)
            if (!Number.isFinite(x) || !Number.isFinite(y0) || !Number.isFinite(y1)) return null

            const p0 = api.coord([x, y0])
            const p1 = api.coord([x, y1])
            const barW = api.size([1, 0])[0] * 0.85
            const left = p0[0] - barW / 2
            const top = Math.min(p0[1], p1[1])
            const height = Math.max(1, Math.abs(p0[1] - p1[1]))

            const isBull = Number(v) > 0
            const fill = isBull ? '#ff0000' : '#00ff00'
            const stroke = isBull ? '#ff4d4d' : '#4dff4d'

            return {
              type: 'rect',
              shape: {
                x: left,
                y: top,
                width: barW,
                height,
              },
              style: api.style({ fill, stroke, lineWidth: 0.6, opacity: 1 }),
              silent: true,
            }
          },
          data: expertData.value1.map((raw, i) => {
            const v = Number(raw)
            const a0Val = Number(expertData.a0[i])
            const b0Val = Number(expertData.b0[i])
            if (!Number.isFinite(v) || !Number.isFinite(a0Val) || !Number.isFinite(b0Val)) {
              return [i, NaN, NaN, NaN]
            }
            const low = Math.min(a0Val, b0Val)
            const high = Math.max(a0Val, b0Val)
            return [i, low, high, v]
          }),
          z: 1
        },
        // 垂直訊號線 (使用 scatter + markLine)
        {
          id: 'series-expert-signals',
          name: '策略標記',
          type: 'scatter',
          xAxisIndex: idxExpert,
          yAxisIndex: idxExpert,
          data: [],
          symbolSize: 0,
          silent: true,
          markLine: {
            silent: true,
            symbol: 'none',
            data: (() => {
              const lines = []
              const vals = expertData.value1 || []
              for (let i = 1; i < vals.length; i++) {
                const prev = vals[i - 1]
                const curr = vals[i]
                if (!Number.isFinite(prev) || !Number.isFinite(curr)) continue
                if ((prev > 0 && curr < 0)) {
                  lines.push({
                    xAxis: dates[i],
                    lineStyle: { color: '#2563eb', width: 1, type: 'solid' },
                    label: { show: false }
                  })
                } else if ((prev < 0 && curr > 0)) {
                  lines.push({
                    xAxis: dates[i],
                    lineStyle: { color: '#dc2626', width: 1, type: 'solid' },
                    label: { show: false }
                  })
                }
              }
              return lines
            })()
          },
          z: 2
        }
      ] : []),
      ...(showGoldenWave.value && goldenWaveData && typeof idxGoldenWave === 'number' ? [
        // DIF 柱狀圖 (以 0 為基準)
        {
          id: 'series-gw-dif-bar',
          name: 'DIF',
          type: 'custom',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          clip: true,
          renderItem: function (params, api) {
            const x = api.value(0)
            const difVal = api.value(1)
            const difSubVal = api.value(2)
            const difSubPrev3 = api.value(3)
            if (!Number.isFinite(x) || !Number.isFinite(difVal)) return null

            const scale = Number(goldenWaveParams.value?.barScale)
            const barScale = Number.isFinite(scale) && scale > 0 ? scale : 1

            const base = 0
            const p0 = api.coord([x, base])
            const p1 = api.coord([x, difVal * barScale])
            const barW = api.size([1, 0])[0] * 0.5
            const left = p0[0] - barW / 2
            const top = Math.min(p0[1], p1[1])
            const height = Math.max(2, Math.abs(p0[1] - p1[1]))

            const isBull = difVal >= 0
            const subNow = Number(difSubVal)
            const subPrev3 = Number(difSubPrev3)
            const hasPrev3 = Number.isFinite(subPrev3)
            const isStrengthening = hasPrev3 ? (subNow > subPrev3) : true
            let fill
            if (isBull) {
              fill = isStrengthening ? goldenWaveParams.value.barUpColor : goldenWaveParams.value.barStopUpColor
            } else {
              fill = isStrengthening ? goldenWaveParams.value.barDownColor : goldenWaveParams.value.barStopDownColor
            }

            return {
              type: 'rect',
              shape: { x: left, y: top, width: barW, height },
              style: api.style({ fill, stroke: 'rgba(0,0,0,0)', lineWidth: 0 }),
              silent: true,
            }
          },
          data: goldenWaveData.dif.map((_, i) => {
            const d = Number(goldenWaveData.dif?.[i])
            const sub = Number(goldenWaveData.difSub?.[i])
            const subPrev3 = (i >= 3) ? Number(goldenWaveData.difSub?.[i - 3]) : NaN
            if (!Number.isFinite(d)) return [i, NaN, NaN, NaN]
            return [i, d, sub, subPrev3]
          }),
          z: 1
        },
        // DIF 線
        ...(goldenWaveParams.value.showDifLine ? [{
          id: 'series-gw-dif',
          name: 'DIF',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.dif.map(v => v === '-' ? null : Number(v)),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: goldenWaveParams.value.difLineWidth,
            color: goldenWaveParams.value.difLineColor
          },
          emphasis: { disabled: true },
          z: 3
        }] : []),
        // 波段均線 (EMA of DIF)
        ...(goldenWaveParams.value.showMa2Line ? [{
          id: 'series-gw-difma-up-pos',
          name: 'DIFMa',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.difMa.map((v, i) => {
            const cur = Number(v)
            const prev = i > 0 ? Number(goldenWaveData.difMa?.[i - 1]) : NaN
            if (!Number.isFinite(cur) || !Number.isFinite(prev)) return null
            return (cur > 0 && cur >= prev) ? cur : null
          }),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: goldenWaveParams.value.ma2LineWidth,
            color: '#dc2626'
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-gw-difma-down-pos',
          name: 'DIFMa',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.difMa.map((v, i) => {
            const cur = Number(v)
            const prev = i > 0 ? Number(goldenWaveData.difMa?.[i - 1]) : NaN
            if (!Number.isFinite(cur) || !Number.isFinite(prev)) return null
            return (cur > 0 && cur < prev) ? cur : null
          }),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: goldenWaveParams.value.ma2LineWidth,
            color: '#22c55e'
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-gw-difma-up-neg',
          name: 'DIFMa',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.difMa.map((v, i) => {
            const cur = Number(v)
            const prev = i > 0 ? Number(goldenWaveData.difMa?.[i - 1]) : NaN
            if (!Number.isFinite(cur) || !Number.isFinite(prev)) return null
            return (cur <= 0 && cur > prev) ? cur : null
          }),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: goldenWaveParams.value.ma2LineWidth,
            color: '#06b6d4'
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-gw-difma-down-neg',
          name: 'DIFMa',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.difMa.map((v, i) => {
            const cur = Number(v)
            const prev = i > 0 ? Number(goldenWaveData.difMa?.[i - 1]) : NaN
            if (!Number.isFinite(cur) || !Number.isFinite(prev)) return null
            return (cur <= 0 && cur <= prev) ? cur : null
          }),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: goldenWaveParams.value.ma2LineWidth,
            color: '#111827'
          },
          emphasis: { disabled: true },
          z: 3
        },
        {
          id: 'series-gw-upper',
          name: 'UpperDIF',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.upperDif.map(v => v === '-' ? null : Number(v)),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 2,
            color: '#dc2626',
            type: 'dashed'
          },
          emphasis: { disabled: true },
          z: 2
        },
        {
          id: 'series-gw-lower',
          name: 'LowerDIF',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.lowerDif.map(v => v === '-' ? null : Number(v)),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 2,
            color: '#06b6d4',
            type: 'dashed'
          },
          emphasis: { disabled: true },
          z: 2
        },
        {
          id: 'series-gw-difma2',
          name: 'DIFMa2',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.difMa2.map(v => v === '-' ? null : Number(v)),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 2,
            color: '#111827'
          },
          emphasis: { disabled: true },
          z: 2
        },
        {
          id: 'series-gw-difma3',
          name: 'DIFMa3',
          type: 'line',
          xAxisIndex: idxGoldenWave,
          yAxisIndex: idxGoldenWave,
          data: goldenWaveData.difMa3.map(v => v === '-' ? null : Number(v)),
          smooth: false,
          clip: true,
          showSymbol: false,
          lineStyle: {
            width: 2,
            color: '#2563eb'
          },
          emphasis: { disabled: true },
          z: 2
        }
        ] : [])
      ] : []),
      ...(showVolume.value ? [{
        name: isIndex ? '成交金額' : '成交量',
        type: 'bar',
        xAxisIndex: idxVol,
        yAxisIndex: idxVol,
        data: volumes,
        clip: true,
        barWidth: volBarWidth,
        barCategoryGap: volCategoryGap,
        itemStyle: {
          color: function(params) {
            const i = params.dataIndex
            if (i < rawOhlc.length) {
              // 指數：依「前收 vs 今收」；個股：依「開收」
              if (isIndex) {
                if (i === 0) return upCandleColor
                return rawOhlc[i][1] >= rawOhlc[i-1][1] ? upCandleColor : downCandleColor
              }
              return rawOhlc[i][1] >= rawOhlc[i][0] ? upCandleColor : '#60a5fa'
            }
            return isIndex ? upCandleColor : '#60a5fa'
          }
        }
      }] : []),
      ...(showVPVR.value && vpvrData ? [{
        id: 'series-vpvr',
        name: 'VPVR',
        type: 'custom',
        xAxisIndex: idxMain,
        yAxisIndex: idxMain,
        silent: true,
        clip: true,
        z: 8,
        data: vpvrData,
        renderItem: function (params, api) {
          const coordSys = params.coordSys
          if (!coordSys) return null
          const anchorX = api.value(0)
          const low = api.value(1)
          const high = api.value(2)
          const norm = api.value(3)
          const left = coordSys.x
          const maxPctRaw = Number(vpvrParams.value?.maxWidthPct)
          const minPctRaw = Number(vpvrParams.value?.minWidthPct)
          const maxPct = Number.isFinite(maxPctRaw) ? Math.max(0.1, Math.min(0.95, maxPctRaw)) : 0.62
          const minPct = Number.isFinite(minPctRaw) ? Math.max(0, Math.min(0.2, minPctRaw)) : 0.02
          const maxW = coordSys.width * maxPct
          const minW = coordSys.width * minPct
          const nn = Math.max(0, Math.min(1, Number(norm)))
          const scaled = Math.pow(nn, 0.55)
          const w = Math.max(0, Math.min(maxW, Math.max(minW, maxW * scaled)))
          const y1 = api.coord([anchorX, high])[1]
          const y2 = api.coord([anchorX, low])[1]
          if (!Number.isFinite(y1) || !Number.isFinite(y2)) return null
          const y = Math.min(y1, y2)
          const h = Math.max(3, Math.abs(y2 - y1))
          return {
            type: 'rect',
            shape: {
              x: left + 1,
              y,
              width: w,
              height: h,
              r: 1.5
            },
            style: api.style({
              fill: rgbaFromHex(vpvrParams.value?.fillColor, vpvrParams.value?.fillAlpha, 'rgba(37, 99, 235, 0.30)'),
              stroke: rgbaFromHex(vpvrParams.value?.strokeColor, vpvrParams.value?.strokeAlpha, 'rgba(96, 165, 250, 0.50)'),
              lineWidth: 1.1,
              shadowBlur: 6,
              shadowColor: 'rgba(37, 99, 235, 0.22)',
              shadowOffsetX: 0,
              shadowOffsetY: 0
            })
          }
        }
      }] : []),
    ]
  }

  try {
    const previousDesired = Math.max(1, Math.floor(Number(desiredKPref.value) || 0))
    const inst = ensureChartInstance()
    if (!inst) return
    try {
      setChartOptionWithoutBlank(inst, option)
    } catch (err) {
      // If instance internal state is corrupted (common with rapid resize/dispose), re-init once and retry.
      try { inst.dispose?.() } catch (_) {}
      chartInstance = null
      const retryInst = ensureChartInstance()
      if (!retryInst) throw err
      setChartOptionWithoutBlank(retryInst, option)
    }
    clearChartError()
    syncChartInteractionBindings()
    updateVisibleKCountFromOption()
    if (dataZoomListener) {
      inst.off('dataZoom', dataZoomListener)
    }
    dataZoomListener = function (params) {
      try {
        if (mobileChartFingerDown && inst && !isChartDisposed(inst)) {
          suppressMobileTooltipDuringChartPanRef.value = true
          inst.dispatchAction({ type: 'hideTip' })
        }
      } catch (_) {}
      const len = chartData.value.length
      try {
        let sv, ev
        if (Array.isArray(params?.batch) && params.batch.length > 0) {
          const b = params.batch[0]
          sv = b.startValue; ev = b.endValue
          if (sv === undefined || ev === undefined) {
            const st = b.start ?? 0
            const en = b.end ?? 100
            sv = Math.round((st / 100) * Math.max(0, len - 1))
            ev = Math.round((en / 100) * Math.max(0, len - 1))
          }
        } else {
          sv = params?.startValue; ev = params?.endValue
          if (sv === undefined || ev === undefined) {
            const st = params?.start ?? 0
            const en = params?.end ?? 100
            sv = Math.round((st / 100) * Math.max(0, len - 1))
            ev = Math.round((en / 100) * Math.max(0, len - 1))
          }
        }
        const newCount = computeVisibleCount(sv, ev, len)
        visibleKCount.value = newCount
        try {
          if (typeof sv === 'number' && typeof ev === 'number') {
            maybeLoadOlder({ startValue: sv, endValue: ev })
          }
        } catch (_) {}
        if (typeof sv === 'number' && typeof ev === 'number') {
          aiVisibleStartIdx.value = Math.max(0, Math.min(len - 1, Math.floor(sv)))
          aiVisibleEndIdx.value = Math.max(0, Math.min(len - 1, Math.floor(ev)))
          if (aiSnapshot.value && aiSnapshot.value.visibleRange) {
            aiSnapshot.value.visibleRange.startIdx = aiVisibleStartIdx.value
            aiSnapshot.value.visibleRange.endIdx = aiVisibleEndIdx.value
            aiSnapshot.value.visibleRange.startDate = dates[aiVisibleStartIdx.value] ?? null
            aiSnapshot.value.visibleRange.endDate = dates[aiVisibleEndIdx.value] ?? null
          }
        }
        if (newCount > 0) {
          desiredKCount.value = newCount
          // 只有在顯示的根數小於可用資料長度時，才視為使用者調整偏好，更新全域預設值
          if (newCount < len) {
            desiredKPref.value = newCount
            localStorage.setItem('chartDesiredKCount', newCount.toString())
          }
        }

        if (showVPVR.value && chartInstance && typeof sv === 'number' && typeof ev === 'number') {
          try {
            const vpvrNext = computeVPVR(sv, ev, vpvrBins)
            scheduleVpvrUpdate(vpvrNext)
            if (aiSnapshot.value && aiSnapshot.value.vpvr && Array.isArray(vpvrNext) && vpvrNext.length) {
              let best = null
              for (const row of vpvrNext) {
                const vol = Number(row?.value?.[4])
                if (!Number.isFinite(vol)) continue
                if (!best || vol > best.vol) {
                  best = {
                    low: Number(row?.value?.[1]),
                    high: Number(row?.value?.[2]),
                    vol,
                  }
                }
              }
              if (best && Number.isFinite(best.low) && Number.isFinite(best.high)) {
                aiSnapshot.value.vpvr.poc = best
              }
            }
          } catch (_) {}
        }
      } catch (_) {
        visibleKCount.value = len
        desiredKCount.value = len
      }
    }
    inst.on('dataZoom', dataZoomListener)

    // Cursor move: keep legends/overlays in sync with hovered index
    if (chartInstance._legendHoverListener) {
      try { chartInstance.off('updateAxisPointer', chartInstance._legendHoverListener) } catch (_) {}
      chartInstance._legendHoverListener = null
    }
    chartInstance._legendHoverListener = function (event) {
      try {
        if (suppressMobileTooltipDuringChartPanRef.value) return
        let idx = undefined
        if (event && Array.isArray(event.seriesData) && event.seriesData.length) {
          const sd = event.seriesData.find(s => typeof s.dataIndex === 'number') || event.seriesData[0]
          idx = sd && typeof sd.dataIndex === 'number' ? sd.dataIndex : undefined
        }
        if (idx === undefined && event && Array.isArray(event.axesInfo)) {
          // value could be number (index) or category label (string)
          const ax = event.axesInfo.find(a => a && (typeof a.value === 'number' || typeof a.value === 'string'))
          if (ax) {
            if (typeof ax.value === 'number') {
              idx = ax.value
            } else if (typeof ax.value === 'string') {
              const i = dates.indexOf(ax.value)
              idx = i >= 0 ? i : undefined
            }
          }
        }
        if (typeof idx === 'number' && chartInstance) {
          scheduleHoverOverlayUpdate(idx)
        }
      } catch (_) {}
    }
    chartInstance.on('updateAxisPointer', chartInstance._legendHoverListener)
    if (chartInstance._legendSelectListener) {
      try { chartInstance.off('legendselectchanged', chartInstance._legendSelectListener) } catch (_) {}
      chartInstance._legendSelectListener = null
    }
    chartInstance._legendSelectListener = function (evt) {
      try {
        const sel = evt && evt.selected ? evt.selected : {}
        const n1 = `MA${maParams.value.ma1}`
        const n2 = `MA${maParams.value.ma2}`
        const n3 = `MA${maParams.value.ma3}`
        const n4 = `MA${maParams.value.ma4}`
        const n5 = `MA${maParams.value.ma5}`
        if (Object.prototype.hasOwnProperty.call(sel, n1)) showMA1.value = !!sel[n1]
        if (Object.prototype.hasOwnProperty.call(sel, n2)) showMA2.value = !!sel[n2]
        if (Object.prototype.hasOwnProperty.call(sel, n3)) showMA3.value = !!sel[n3]
        if (Object.prototype.hasOwnProperty.call(sel, n4)) showMA4.value = !!sel[n4]
        if (Object.prototype.hasOwnProperty.call(sel, n5)) showMA5.value = !!sel[n5]
        // Persist only; do not re-render to avoid legend rebuild
        localStorage.setItem('chartShowMA1', showMA1.value.toString())
        localStorage.setItem('chartShowMA2', showMA2.value.toString())
        localStorage.setItem('chartShowMA3', showMA3.value.toString())
        localStorage.setItem('chartShowMA4', showMA4.value.toString())
        localStorage.setItem('chartShowMA5', showMA5.value.toString())
      } catch (_) {}
    }
    inst.on('legendselectchanged', inst._legendSelectListener)
    if (chartData.value.length > 0) {
      const len = chartData.value.length
      const desired = Math.max(1, Math.min(len, previousDesired || len))
      desiredKCount.value = desired
      const endIdx = len - 1
      const startIdx = Math.max(0, endIdx - (desired - 1))
      const indexes = dataZoomIndexes.length ? dataZoomIndexes : [0]
      for (const idx of indexes) {
        try {
          inst.dispatchAction({
            type: 'dataZoom',
            dataZoomIndex: idx,
            startValue: startIdx,
            endValue: endIdx
          })
        } catch (_) {}
      }
      if (usePinnedMobileTooltip() && mobilePinnedHoverIdx.value != null && !shouldShowPinnedLookupAsideLayout()) {
        requestAnimationFrame(() => {
          try { applyPinnedMobileTooltip(inst, mobilePinnedHoverIdx.value) } catch (_) {}
        })
      }
    }

    if (hasSubplot && subplotLayoutBasisH > 0 && subplotRelayoutDepth < 2) {
      const basis = subplotLayoutBasisH
      requestAnimationFrame(() => {
        try {
          const ci = chartInstance
          if (!ci || typeof ci.getHeight !== 'function' || typeof ci.resize !== 'function') return
          ci.resize()
          const hNow = ci.getHeight() || 0
          if (hNow > 0 && basis - hNow > 3) {
            subplotRelayoutDepth += 1
            renderChart()
            subplotRelayoutDepth -= 1
          }
        } catch (_) {}
      })
    }

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        try { syncPinnedLookupAsideDockPosition() } catch (_) {}
        try { clampCrosshairLookupNavPositionToBounds() } catch (_) {}
      })
    })

  } catch (e) {
    console.error('Failed to set chart option:', e)
    setChartError('圖表繪製失敗', e)
  }
}

function changePeriod(key) {
  if (!periodKeyMap.has(key)) return
  const option = periodKeyMap.get(key)
  const periodValue = option?.period ?? DEFAULT_PERIOD
  selectedPeriodKey.value = key
  emit('update:period', periodValue)
  // selectedPeriodKey watcher 統一載入，避免同一操作請求／重繪兩次。
}

// Watch for data changes and render chart
watch(() => [chartData.value.length, loading.value], async ([newLen, isLoading]) => {
  if (!newLen) mobilePinnedHoverIdx.value = null
  if (!newLen) mobileTooltipDismissed.value = false
  if (newLen > 0 && !isLoading) {
    await nextTick()
    try {
      renderChart()
    } catch (e) {
      console.error('[StockChart debug] data watcher renderChart error', e)
      setChartError('圖表繪製失敗', e)
    }
  }
})

watch(
  () => [crosshairLookupNavCollapsed.value, isFullscreen.value, props.multiTileMode],
  () => {
    nextTick(() => {
      try { clampCrosshairLookupNavPositionToBounds() } catch (_) {}
    })
  }
)

// Watch for indicator toggles
watch([showMainK, showVolume, showKD, showMACD, showRSI, showCCI, showBB, showGoldenWave, showVPVR, showHA, showReversal], () => {
  localStorage.setItem('chartShowMainK', showMainK.value.toString())
  localStorage.setItem('chartShowVolume', showVolume.value.toString())
  localStorage.setItem('chartShowKD', showKD.value.toString())
  localStorage.setItem('chartShowMACD', showMACD.value.toString())
  localStorage.setItem('chartShowRSI', showRSI.value.toString())
  localStorage.setItem('chartShowCCI', showCCI.value.toString())
  localStorage.setItem('chartShowBB', showBB.value.toString())
  localStorage.setItem('chartShowGoldenWave', showGoldenWave.value.toString())
  localStorage.setItem('chartShowVPVR', showVPVR.value.toString())
  localStorage.setItem('chartShowHA', showHA.value.toString())
  localStorage.setItem('chartShowReversal', showReversal.value.toString())
  scheduleSyncChartSettingsToServer()
  if (chartData.value && chartData.value.length > 0) {
    renderChart()
  }
})

// Persist KD midline and re-render on change
watch(() => kdMidline.value, (val) => {
  const suffix = getCurrentTfSuffix()
  localStorage.setItem(`chartKDMidline${suffix}`, val.toString())
  if (chartData.value.length > 0) {
    renderChart()
  }
})

watch(() => [isFullscreen.value, props.carouselEnabled, props.carouselLength, props.symbol, showFullscreenCarouselControls.value], () => {
  updateFullscreenKeyListener()
}, { immediate: true })

watch(() => props.carouselPlaying, () => {
  // trigger re-render of controls if needed
})

watch(
  () => [props.multiTileMode, props.fsHostQuadCell],
  () => {
    nextTick(() => {
      try {
        renderChart()
      } catch (_) {}
    })
  }
)

onMounted(async () => {
  await nextTick()
  updateIsMobileUi()
  try {
    if (useMobileKlineDropdown.value) {
      controlPanelOpen.value = false
      persistControlPanelOpenState()
    }
  } catch (_) {}

  // Load chart settings from server (if logged in) before first render
  try {
    if (hasAuthToken() && !chartSettingsLoadedOnce) {
      await syncChartSettingsFromServer()
    }
  } catch (_) {}

  // After local/server hydrate: keep MA legend collapsed when HMA is on (narrow screens).
  try {
    if (isMobileMaHmaExclusiveMode() && showHMA.value) {
      maHmaExclusiveSyncing = true
      maCollapsedByHma = true
      maLegendCollapsed.value = true
      showMA1.value = false
      showMA2.value = false
      showMA3.value = false
      showMA4.value = false
      showMA5.value = false
      saveMAVisibility()
      maHmaExclusiveSyncing = false
    }
  } catch (_) {}

  loadChartData()
  window.addEventListener('quantgems:data-updated', onQuantgemsDataUpdated)
  
  // Handle window resize
  window.addEventListener('resize', handleWindowResize)
  window.addEventListener('orientationchange', handleViewportChange)
  window.addEventListener('pointerdown', handleGlobalPointerDown, true)
  if (window.visualViewport) {
    try {
      lastVisualViewportSize = {
        w: Number(window.visualViewport.width),
        h: Number(window.visualViewport.height),
      }
    } catch (_) {
      lastVisualViewportSize = { w: 0, h: 0 }
    }
    window.visualViewport.addEventListener('resize', handleViewportChange)
  }
  
  // Handle Esc key to close panel
  window.addEventListener('keydown', handleEscKey)
  
  // Listen for native fullscreen changes.
  // When native fullscreen exits (e.g. user presses ESC), we must also clear the CSS
  // inline styles that were applied by applyCssFullscreen(), otherwise the chart
  // stays visually fullscreen even after the native session ends.
  function onNativeFullscreenChange(isNativeActive) {
    if (isNativeActive) {
      // Native fullscreen entered (CSS is already applied; just confirm state)
      nativeFullscreenActive.value = true
      isFullscreen.value = true
    } else if (nativeFullscreenActive.value && isFullscreen.value) {
      // Native fullscreen exited while chart was in fullscreen mode
      nativeFullscreenActive.value = false
      isFullscreen.value = false
      controlPanelOpen.value = false
      persistControlPanelOpenState()
      aiModalOpen.value = false
      const el = rootEl.value
      if (el) clearCssFullscreen(el)
    } else {
      nativeFullscreenActive.value = false
    }
    updateFullscreenKeyListener()
    setTimeout(() => {
      try { syncFullscreenChartViewportLayout('native-fullscreen-change') } catch (_) {}
      ensureChartInstance()
      safeResize()
      if (!loading.value && chartData.value.length > 0) {
        renderChart()
      }
    }, 100)
  }
  document.addEventListener('fullscreenchange', () => {
    onNativeFullscreenChange(!!document.fullscreenElement)
  })
  document.addEventListener('webkitfullscreenchange', () => {
    onNativeFullscreenChange(!!document.webkitFullscreenElement)
  })
  document.addEventListener('msfullscreenchange', () => {
    onNativeFullscreenChange(!!document.msFullscreenElement)
  })
})

onUnmounted(() => {
  try {
    fsToolsMenuOpen.value = false
  } catch (_) {}
  if (fsToolsMenuDocListenerTid != null) {
    try {
      clearTimeout(fsToolsMenuDocListenerTid)
    } catch (_) {}
    fsToolsMenuDocListenerTid = null
  }
  document.removeEventListener('pointerdown', onFsToolsMenuDocPointerDown, true)
  if (klineModeMenuDocTid != null) {
    try {
      clearTimeout(klineModeMenuDocTid)
    } catch (_) {}
    klineModeMenuDocTid = null
  }
  document.removeEventListener('pointerdown', onKlineModeMenuDocPointerDown, true)
  if (fsToolsMenuLayoutListenersBound) {
    try {
      window.removeEventListener('resize', onFsToolsMenuWindowRelayout)
      window.removeEventListener('scroll', onFsToolsMenuWindowRelayout, true)
    } catch (_) {}
    try {
      window.visualViewport?.removeEventListener?.('resize', onFsToolsMenuWindowRelayout)
    } catch (_) {}
    fsToolsMenuLayoutListenersBound = false
  }
  if (chartSettingsSyncTimer) {
    try { clearTimeout(chartSettingsSyncTimer) } catch (_) {}
    chartSettingsSyncTimer = null
    try {
      if (hasAuthToken()) {
        const payload = collectChartSettingsFromLocalStorage()
        saveUserChartSettings(payload).catch(() => {})
      }
    } catch (_) {}
  }
  window.removeEventListener('pointerdown', handleGlobalPointerDown, true)
  window.removeEventListener('quantgems:data-updated', onQuantgemsDataUpdated)
  window.removeEventListener('pointermove', onCrosshairLookupNavPointerMove, true)
  window.removeEventListener('pointerup', onCrosshairLookupNavPointerUp, true)
  window.removeEventListener('pointercancel', onCrosshairLookupNavPointerUp, true)
  try {
    if (typeof window !== 'undefined' && typeof window.removeEventListener === 'function') {
      window.removeEventListener('quantgems:before-logout', beforeLogoutHandler)
    }
  } catch (_) {}
  if (chartContextMenuListener) {
    try {
      const dom = chartInstance?.getDom?.() || chartContainer.value
      dom?.removeEventListener?.('contextmenu', chartContextMenuListener)
    } catch (_) {}
    chartContextMenuListener = null
  }
  if (chartZrClickListener) {
    try { chartInstance?.getZr?.()?.off?.('click', chartZrClickListener) } catch (_) {}
    chartZrClickListener = null
  }
  if (chartZrMouseDownListener) {
    try { chartInstance?.getZr?.()?.off?.('mousedown', chartZrMouseDownListener) } catch (_) {}
    chartZrMouseDownListener = null
  }
  if (chartZrMouseMoveListener) {
    try { chartInstance?.getZr?.()?.off?.('mousemove', chartZrMouseMoveListener) } catch (_) {}
    chartZrMouseMoveListener = null
  }
  if (chartZrMouseUpListener) {
    try { chartInstance?.getZr?.()?.off?.('mouseup', chartZrMouseUpListener) } catch (_) {}
    chartZrMouseUpListener = null
  }
  if (chartTouchStartListener) {
    try { chartInstance?.getDom?.()?.removeEventListener?.('touchstart', chartTouchStartListener) } catch (_) {}
    chartTouchStartListener = null
  }
  if (chartTouchMoveListener) {
    try { chartInstance?.getDom?.()?.removeEventListener?.('touchmove', chartTouchMoveListener) } catch (_) {}
    chartTouchMoveListener = null
  }
  if (chartTouchEndListener) {
    try { chartInstance?.getDom?.()?.removeEventListener?.('touchend', chartTouchEndListener) } catch (_) {}
    chartTouchEndListener = null
  }
  if (chartTouchCancelListener) {
    try { chartInstance?.getDom?.()?.removeEventListener?.('touchcancel', chartTouchCancelListener) } catch (_) {}
    chartTouchCancelListener = null
  }
  resetDrawingLongPressState()
  if (chartInstance) {
    chartInstance.dispose()
    chartInstance = null
  }
  if (fullscreenKeyListenerActive) {
    document.removeEventListener('keydown', handleFullscreenKeydown, true)
    fullscreenKeyListenerActive = false
  }
  window.removeEventListener('resize', handleWindowResize)
  window.removeEventListener('orientationchange', handleViewportChange)
  if (window.visualViewport) {
    window.visualViewport.removeEventListener('resize', handleViewportChange)
  }
  if (viewportResizeDebounceTimer != null) {
    try { clearTimeout(viewportResizeDebounceTimer) } catch (_) {}
    viewportResizeDebounceTimer = null
  }
  window.removeEventListener('keydown', handleEscKey)
})
</script>

<template>
  <div
    class="stock-chart"
    :class="{
      'is-fullscreen': isFullscreen,
      'plan-free': isFreeTech,
      'ai-modal-open': aiModalOpen,
      'mobile-fs-toolbar-collapsed': showMobileFsToolbarCollapseUi && mobileFsToolbarCollapsed,
      'stock-chart--multi-tile': multiTileMode && !isFullscreen,
    }"
    ref="rootEl"
  >
    <div
      class="chart-header"
      :class="{
        'fullscreen-active': isFullscreen,
        'mobile-fs-toolbar-collapsed': showMobileFsToolbarCollapseUi && mobileFsToolbarCollapsed,
        'chart-header--multi-tile': multiTileMode && !isFullscreen,
      }"
    >
      <template v-if="!props.multiTileMode || isFullscreen">
      <button
        v-if="showMobileFsToolbarCollapseUi && mobileFsToolbarCollapsed"
        type="button"
        class="mobile-fs-toolbar-expand"
        aria-expanded="false"
        @click="mobileFsToolbarCollapsed = false"
      >
        <i class="fas fa-chevron-down" aria-hidden="true"></i>
        <span>展開</span>
      </button>
      <!-- 時間週期控制區 -->
      <div
        ref="frequencyControlsRef"
        v-show="!showMobileFsToolbarCollapseUi || !mobileFsToolbarCollapsed"
        class="frequency-controls"
        :class="{ 'fullscreen-active': isFullscreen }"
      >
        <!-- 手機版：顯示根數與週期按鈕包裝在一起 -->
        <div class="mobile-period-row">
          <div class="visible-count-stepper">
            <button class="stepper-btn" type="button" @click="decrementKCount" title="減少根數">－</button>
            <input
              class="stepper-value"
              type="number"
              v-model.number="desiredKCount"
              min="20"
              :max="chartData.length"
              inputmode="numeric"
              @keyup.enter.prevent="applyDesiredKCount"
              @blur="applyDesiredKCount"
            />
            <button class="stepper-btn" type="button" @click="incrementKCount" title="增加根數">＋</button>
          </div>
          <div class="period-chips-scroll">
            <button
              v-for="option in periodOptions"
              :key="option.key"
              class="period-chip"
              :class="{ active: selectedPeriodKey === option.key }"
              @click="changePeriod(option.key)"
            >
              {{ option.label }}
            </button>
          </div>
        </div>
        
        <!-- 手機全螢幕：K 線切換 | 股票查詢 | 圖示同一列；桌機用 display:contents 維持原 flex 子項 -->
        <div class="mobile-toolbar-secondary">
        <div class="kline-search-cluster">
        <!-- K線模式：桌機三按鈕（原始／神奇／轉折）；手機／窄螢幕（isMobileUi）圖示下拉 -->
        <div class="kline-mode-toggle">
          <template v-if="!isMobileUi">
            <button
              type="button"
              class="mode-btn"
              :class="{ active: !showReversal && chartMode === 'standard' }"
              :aria-pressed="!showReversal && chartMode === 'standard'"
              @click="setStandardMode"
            >
              <i class="fas fa-chart-bar" aria-hidden="true"></i>
              <span>原始K線</span>
            </button>
            <button
              type="button"
              class="mode-btn"
              :class="{ active: !showReversal && chartMode === 'heikin' }"
              :aria-pressed="!showReversal && chartMode === 'heikin'"
              @click="setHeikinMode"
            >
              <i class="fas fa-magic" aria-hidden="true"></i>
              <span>神奇K線</span>
            </button>
            <button
              type="button"
              class="mode-btn"
              :class="{ active: showReversal }"
              :aria-pressed="showReversal"
              @click="toggleReversalFromToolbar"
            >
              <i class="fas fa-wave-square" aria-hidden="true"></i>
              <span>階梯線</span>
            </button>
          </template>
          <template v-else>
          <div class="kline-mode-select-shell">
            <div
              class="kline-mode-icon-dropdown"
              :class="{ 'is-open': klineModeMenuOpen }"
            >
              <button
                type="button"
                class="kline-mode-icon-dropdown__trigger"
                aria-haspopup="listbox"
                :aria-expanded="klineModeMenuOpen ? 'true' : 'false'"
                aria-label="Chart mode"
                @click.stop="toggleKlineModeMenu"
              >
                <i
                  class="kline-mode-icon-dropdown__trigger-icon"
                  :class="klineModeTriggerIconClass"
                  aria-hidden="true"
                ></i>
                <i class="fas fa-chevron-down kline-mode-icon-dropdown__caret" aria-hidden="true"></i>
              </button>
              <ul
                v-show="klineModeMenuOpen"
                class="kline-mode-icon-dropdown__menu"
                role="listbox"
                aria-label="Chart mode"
              >
                <li role="presentation">
                  <button
                    type="button"
                    role="option"
                    class="kline-mode-icon-dropdown__option"
                    :class="{ 'is-active': klineToolbarMode === 'standard' }"
                    :aria-selected="klineToolbarMode === 'standard'"
                    aria-label="OHLC candles"
                    @click="pickKlineToolbarMode('standard')"
                  >
                    <i class="fas fa-chart-bar" aria-hidden="true"></i>
                  </button>
                </li>
                <li role="presentation">
                  <button
                    type="button"
                    role="option"
                    class="kline-mode-icon-dropdown__option"
                    :class="{ 'is-active': klineToolbarMode === 'heikin' }"
                    :aria-selected="klineToolbarMode === 'heikin'"
                    aria-label="Heikin Ashi"
                    @click="pickKlineToolbarMode('heikin')"
                  >
                    <i class="fas fa-magic" aria-hidden="true"></i>
                  </button>
                </li>
                <li role="presentation">
                  <button
                    type="button"
                    role="option"
                    class="kline-mode-icon-dropdown__option"
                    :class="{ 'is-active': klineToolbarMode === 'reversal' }"
                    :aria-selected="klineToolbarMode === 'reversal'"
                    aria-label="Step line"
                    @click="pickKlineToolbarMode('reversal')"
                  >
                    <i class="fas fa-wave-square" aria-hidden="true"></i>
                  </button>
                </li>
              </ul>
            </div>
          </div>
          </template>
          <button
            v-if="showKlineCrosshairToggle"
            type="button"
            class="mode-btn"
            :class="{ active: klineCrosshairLookupEnabled }"
            :title="klineCrosshairLookupEnabled ? '結束查價：滑動不顯示十字線' : '啟動查價：滑動顯示十字線與價格'"
            :aria-pressed="klineCrosshairLookupEnabled"
            @click="toggleKlineCrosshairLookup"
          >
            <i class="fas fa-crosshairs" aria-hidden="true"></i>
            <span>{{ klineCrosshairLookupEnabled ? '結束查價' : '查價線' }}</span>
          </button>
        </div>
        
        <div
          v-if="props.fullscreenSearchEnabled && (isFullscreen || useMobileKlineDropdown)"
          v-show="showFullscreenSearchBox"
          class="fullscreen-search-box fullscreen-search-box--inline"
          :class="{ 'fullscreen-search-box--mobile-panel': showMobileToolbarSearchTrigger && mobileToolbarSearchOpen }"
          data-stop-enter-carousel
        >
          <div
            v-if="showMobileToolbarSearchTrigger && mobileToolbarSearchOpen"
            class="fullscreen-search-panel-head"
          >
            <div class="fullscreen-search-panel-copy">
              <div class="fullscreen-search-panel-title">股票查詢</div>
              <div class="fullscreen-search-panel-hint">可輸入代號或名稱，例如 2330 / 台積電</div>
            </div>
            <button
              type="button"
              class="fullscreen-search-panel-close"
              aria-label="關閉股票查詢"
              @pointerdown="prepareMobileToolbarSearchUiTransition"
              @click="closeMobileToolbarSearch"
            >
              <i class="fas fa-times"></i>
            </button>
          </div>
          <div class="fullscreen-search-query-row">
            <!-- 麥克風：點一下直接開始/停止錄音，不切換 DOM 結構（避免 chart resize 閃屏） -->
            <button
              v-if="fsSpeechSupported"
              type="button"
              class="fullscreen-search-mic-toggle"
              :class="{ 'is-active': fsSpeechListening, 'is-listening': fsSpeechListening }"
              :title="fsSpeechListening ? '停止語音' : '語音輸入'"
              :aria-label="fsSpeechListening ? '停止語音' : '語音輸入'"
              @click="toggleFullscreenSpeechSearch"
            >
              <i class="fas fa-microphone"></i>
            </button>
            <!-- 輸入框：永遠顯示，聽取中時 placeholder 提示，避免 DOM 切換造成閃屏 -->
            <div class="search-input" :class="{ 'is-listening': fsSpeechListening }">
              <input
                v-model="fullscreenSearchSymbol"
                type="text"
                maxlength="12"
                class="fullscreen-search-input"
                :placeholder="fsSpeechListening ? '聽取中…' : (useMobileKlineDropdown ? '股票代號' : '股票代號或名稱')"
                autocomplete="off"
                autocorrect="off"
                autocapitalize="none"
                spellcheck="false"
                enterkeyhint="search"
                ref="fullscreenSearchInput"
                @blur="onFullscreenSearchInputBlur"
                @keyup.enter.prevent="handleFullscreenSearch"
              />
            </div>
            <button
              type="button"
              class="fullscreen-search-btn"
              @click="handleFullscreenSearch"
            >
              <i class="fas fa-search"></i>
            </button>
          </div>
          <div
            v-if="fsSpeechSupported && fsSpeechError"
            class="fullscreen-search-speech-err"
          >
            {{ fsSpeechError }}
          </div>
        </div>
        </div>
        
        <!-- 全螢幕模式下顯示的按鈕與輪播控制 -->
        <div class="fullscreen-actions">
          <div class="fullscreen-actions-buttons">
            <div
              v-if="isFullscreen && !useMobileKlineDropdown"
              ref="fsToolsDropdownRef"
              class="fs-tools-dropdown"
            >
              <button
                type="button"
                class="action-icon-btn fs-tools-dropdown__trigger"
                :class="{ 'is-open': fsToolsMenuOpen }"
                :aria-expanded="fsToolsMenuOpen"
                aria-haspopup="true"
                title="全螢幕工具"
                @click.stop="toggleFsToolsMenu"
              >
                <i class="fas fa-ellipsis-v" aria-hidden="true"></i>
              </button>
            </div>
            <Teleport to="body" :disabled="isFullscreen">
              <ul
                v-if="fsToolsMenuOpen"
                ref="fsToolsMenuPanelRef"
                class="fs-tools-menu fs-tools-menu--teleport"
                role="menu"
                :style="fsToolsMenuFixedStyle"
                @click.stop
              >
                <li v-if="!props.multiTileMode && quadLayoutAvailable" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    @click="emit('requestFsQuad'); closeFsToolsMenu()"
                  >
                    <i class="fas fa-th-large" aria-hidden="true"></i>
                    <span>全螢幕四分割</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none" class="fs-tools-menu__divider"></li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    :class="{ 'is-active': drawingMode === 'trend' }"
                    @click="toggleDrawingMode('trend')"
                  >
                    <i class="fas fa-chart-line" aria-hidden="true"></i>
                    <span>趨勢線</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    :class="{ 'is-active': drawingMode === 'horizontal' }"
                    @click="toggleDrawingMode('horizontal')"
                  >
                    <i class="fas fa-minus" aria-hidden="true"></i>
                    <span>水平線</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    :class="{ 'is-active': drawingMode === 'vertical' }"
                    @click="toggleDrawingMode('vertical')"
                  >
                    <i class="fas fa-grip-lines-vertical" aria-hidden="true"></i>
                    <span>垂直線</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    :class="{ 'is-active': drawingMode === 'rect' }"
                    @click="toggleDrawingMode('rect')"
                  >
                    <i class="far fa-square" aria-hidden="true"></i>
                    <span>矩形框</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    :class="{ 'is-active': drawingMode === 'channel' }"
                    @click="toggleDrawingMode('channel')"
                  >
                    <i class="fas fa-equals" aria-hidden="true"></i>
                    <span>平行通道</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled() && drawingMode" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    @click="stopDrawingModeFromToolbar"
                  >
                    <i class="fas fa-ban" aria-hidden="true"></i>
                    <span>結束畫線模式</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled() && selectedDrawing" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item fs-tools-menu__item--danger"
                    @click="deleteDrawingById(selectedDrawing.id); closeFsToolsMenu()"
                  >
                    <i class="fas fa-trash" aria-hidden="true"></i>
                    <span>刪除此畫線</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled() && chartDrawings.length" role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item fs-tools-menu__item--danger"
                    @click="clearAllDrawings(); closeFsToolsMenu()"
                  >
                    <i class="fas fa-trash-alt" aria-hidden="true"></i>
                    <span>清除全部畫線</span>
                  </button>
                </li>
                <li v-if="isDesktopFullscreenDrawingEnabled()" role="none" class="fs-tools-menu__divider"></li>
                <li role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    @click="void exportChartImage(); closeFsToolsMenu()"
                  >
                    <i class="fas fa-download" aria-hidden="true"></i>
                    <span>匯出圖片</span>
                  </button>
                </li>
                <li role="none">
                  <button
                    type="button"
                    role="menuitem"
                    class="fs-tools-menu__item"
                    @click="toggleFullscreen(); closeFsToolsMenu()"
                  >
                    <i class="fas fa-compress" aria-hidden="true"></i>
                    <span>退出全螢幕</span>
                  </button>
                </li>
              </ul>
            </Teleport>

            <button
              v-if="isFullscreen && warrantFsChips.length"
              type="button"
              class="action-icon-btn"
              :class="{ active: warrantInfoOpen }"
              @click="toggleWarrantInfoPanel"
              title="權證基本資料"
              aria-label="權證基本資料"
            >
              <i class="fas fa-id-card"></i>
            </button>
            <button 
              v-show="!controlPanelOpen" 
              class="action-icon-btn" 
              @click="toggleControlPanel"
              title="圖表控制"
            >
              <i class="fas fa-cog"></i>
            </button>
          </div>
          <div
            v-if="showFullscreenCarouselControls"
            class="fullscreen-carousel-controls"
          >
            <span class="carousel-indicator">
              輪播 {{ Math.min(props.carouselIndex + 1, props.carouselLength) }} / {{ props.carouselLength }}
            </span>
            <div class="carousel-buttons">
              <button
                class="carousel-btn"
                :disabled="!canNavigateCarousel"
                @click.stop="emit('carousel-prev')"
                title="上一檔"
              >
                <i class="fas fa-chevron-left"></i>
              </button>
              <button
                class="carousel-btn carousel-btn--toggle"
                :class="{ 'carousel-btn--active': props.carouselPlaying }"
                :disabled="props.carouselLength <= 1"
                @click.stop="emit('carousel-toggle')"
                title="播放 / 暫停輪播"
              >
                <i :class="props.carouselPlaying ? 'fas fa-pause' : 'fas fa-play'"></i>
              </button>
              <button
                class="carousel-btn"
                :disabled="!canNavigateCarousel"
                @click.stop="emit('carousel-next')"
                title="下一檔"
              >
                <i class="fas fa-chevron-right"></i>
              </button>
            </div>
          </div>
        </div>
        </div>
        <button
          v-if="showMobileFsToolbarCollapseUi"
          type="button"
          class="mobile-fs-toolbar-collapse-btn"
          aria-expanded="true"
          @click="mobileFsToolbarCollapsed = true"
        >
          <i class="fas fa-chevron-up" aria-hidden="true"></i>
          <span>收合</span>
        </button>
      </div>

      <div class="header-row" :class="{ 'fullscreen-active': isFullscreen }">
        <div class="header-left"></div>
        <!-- 右側控制按鈕 -->
        <div class="header-actions">
          <button 
            class="action-icon-btn" 
            @click="toggleFullscreen"
            :title="isFullscreen ? '退出全螢幕' : '全螢幕'"
          >
            <i :class="isFullscreen ? 'fas fa-compress' : 'fas fa-expand'"></i>
          </button>

          <button
            v-if="isFullscreen"
            class="action-icon-btn"
            @click="exportChartImage"
            title="匯出圖片 / 下載圖表"
          >
            <i class="fas fa-download"></i>
          </button>
          
          <button
            v-if="isFullscreen && warrantFsChips.length"
            type="button"
            class="action-icon-btn"
            :class="{ active: warrantInfoOpen }"
            @click="toggleWarrantInfoPanel"
            title="權證基本資料"
            aria-label="權證基本資料"
          >
            <i class="fas fa-id-card"></i>
          </button>
          <button 
            v-show="!controlPanelOpen" 
            class="action-icon-btn" 
            @click="toggleControlPanel"
            title="圖表控制"
          >
            <i class="fas fa-cog"></i>
          </button>
        </div>
      </div>
      </template>
      <template v-else>
        <div class="fs-mt-bar" role="toolbar" aria-label="分割視窗走勢">
          <div class="fs-mt-top">
            <span class="fs-mt-title" :title="stockTitle || '—'">
              {{ stockTitle || '—' }}
            </span>
            <div class="fs-mt-periods" role="tablist" aria-label="K 線週期">
              <button
                v-for="option in periodOptions"
                :key="`mtp-${option.key}`"
                type="button"
                class="fs-mt-chip"
                :class="{ active: selectedPeriodKey === option.key }"
                @click="changePeriod(option.key)"
              >
                {{ option.label }}
              </button>
            </div>
            <button type="button" class="fs-mt-ico" title="圖表控制" @click="toggleControlPanel">
              <i class="fas fa-cog"></i>
            </button>
          </div>
          <div
            v-if="props.fsHostQuadCell && props.fullscreenSearchEnabled"
            class="fs-mt-search"
          >
            <input
              v-model="fullscreenSearchSymbol"
              type="text"
              maxlength="12"
              class="fs-mt-input"
              placeholder="代號／名稱"
              autocomplete="off"
              autocapitalize="none"
              @keyup.enter.prevent="handleFullscreenSearch"
            />
            <button type="button" class="fs-mt-ico" title="查詢" @click="handleFullscreenSearch">
              <i class="fas fa-search"></i>
            </button>
          </div>
        </div>
      </template>

    </div>

    <!-- 權證基本資料：全螢幕內垂直面板（需在 fullscreen 元素內） -->
    <transition name="panel-fade">
      <aside
        v-if="isFullscreen && warrantInfoOpen && warrantFsChips.length"
        class="warrant-fs-panel"
        @click.stop
      >
        <div class="warrant-fs-panel__head">
          <div class="warrant-fs-panel__titles">
            <span class="warrant-fs-panel__eyebrow">權證基本資料</span>
            <strong class="warrant-fs-panel__title">{{ warrantFsTitle || '—' }}</strong>
          </div>
          <button
            type="button"
            class="warrant-fs-panel__close"
            title="關閉"
            aria-label="關閉權證基本資料"
            @click="closeWarrantInfoPanel"
          >
            <i class="fas fa-times"></i>
          </button>
        </div>
        <dl class="warrant-fs-panel__list">
          <div
            v-for="chip in warrantFsChips"
            :key="chip.label"
            class="warrant-fs-panel__row"
          >
            <dt>{{ chip.label }}</dt>
            <dd>{{ chip.value }}</dd>
          </div>
        </dl>
      </aside>
    </transition>

    <div
      class="chart-wrapper"
      :title="multiTileMode && !isFullscreen ? '雙擊圖表區可全螢幕' : undefined"
      @dblclick="onMultiTileDblClickEnterFullscreen"
    >
      <div ref="chartContainer" class="chart-container">
        <div ref="chartMountEl" class="chart-container-echarts"></div>
        <div
          v-if="showPinnedDockedLookupAside"
          class="mobile-pinned-lookup-aside mobile-pinned-lookup-aside--chart-dock"
          aria-live="polite"
        >
          <div class="mobile-pinned-lookup-aside__body">
            <div class="mobile-pinned-lookup-aside__cell mobile-pinned-lookup-aside__cell--close-only">
              <span class="mobile-pinned-lookup-aside__label">收盤價</span>
              <span class="mobile-pinned-lookup-aside__price">{{ pinnedLookupAsideDisplay.close }}</span>
              <template v-if="pinnedLookupAsideDisplay.pctStr">
                <span class="mobile-pinned-lookup-aside__label">漲跌幅</span>
                <span
                  class="mobile-pinned-lookup-aside__pct"
                  :class="pinnedLookupAsideDisplay.pctUp ? 'is-up' : 'is-down'"
                >{{ pinnedLookupAsideDisplay.pctStr }}</span>
              </template>
              <template v-if="pinnedLookupAsideDisplay.deltaStr">
                <span class="mobile-pinned-lookup-aside__label">漲跌</span>
                <span
                  class="mobile-pinned-lookup-aside__delta"
                  :class="pinnedLookupAsideDisplay.pctUp ? 'is-up' : 'is-down'"
                >{{ pinnedLookupAsideDisplay.deltaStr }}</span>
              </template>
            </div>
          </div>
        </div>
        <div
          v-if="klineCrosshairLookupEnabled && !drawingMode && chartData.length && !loading && showMainPlotForCrosshairNav"
          ref="crosshairLookupNavEl"
          class="crosshair-lookup-nav"
          :class="{ 'is-collapsed': crosshairLookupNavCollapsed, 'is-dragging': crosshairLookupNavDrag.active }"
          :style="crosshairLookupNavStyle"
          role="toolbar"
          aria-label="查價線移動"
          @click.capture="onCrosshairLookupNavClickCapture"
          @pointerdown.stop="onCrosshairLookupNavPointerDown"
        >
          <button
            v-show="!crosshairLookupNavCollapsed"
            type="button"
            class="crosshair-lookup-nav__btn"
            title="上一根（較早）"
            :disabled="!crosshairLookupCanStep(-1)"
            aria-label="上一根較早日期"
            @click="stepCrosshairLookup(-1)"
          >
            <i class="fas fa-chevron-left" aria-hidden="true"></i>
          </button>
          <button
            v-show="!crosshairLookupNavCollapsed"
            type="button"
            class="crosshair-lookup-nav__btn"
            title="下一根（較新）"
            :disabled="!crosshairLookupCanStep(1)"
            aria-label="下一根較新日期"
            @click="stepCrosshairLookup(1)"
          >
            <i class="fas fa-chevron-right" aria-hidden="true"></i>
          </button>
          <button
            v-show="!crosshairLookupNavCollapsed"
            type="button"
            class="crosshair-lookup-nav__btn crosshair-lookup-nav__btn--latest"
            title="跳到最新一根 K 線"
            :disabled="!crosshairLookupCanJumpLatest()"
            aria-label="跳到最新一根"
            @click="jumpCrosshairLookupLatest"
          >
            <i class="fas fa-angles-right" aria-hidden="true"></i>
          </button>
          <button
            v-show="!crosshairLookupNavCollapsed"
            type="button"
            class="crosshair-lookup-nav__btn"
            title="放大 K 線"
            aria-label="放大 K 線"
            @click="zoomPinnedLookupAsideIn"
          >
            <i class="fas fa-plus" aria-hidden="true"></i>
          </button>
          <button
            v-show="!crosshairLookupNavCollapsed"
            type="button"
            class="crosshair-lookup-nav__btn"
            title="縮小 K 線"
            aria-label="縮小 K 線"
            @click="zoomPinnedLookupAsideOut"
          >
            <i class="fas fa-minus" aria-hidden="true"></i>
          </button>
          <button
            type="button"
            class="crosshair-lookup-nav__btn crosshair-lookup-nav__btn--close"
            :title="crosshairLookupNavCollapsed ? '展開查價工具' : '收合查價工具'"
            :aria-label="crosshairLookupNavCollapsed ? '展開查價工具' : '收合查價工具'"
            @click="toggleCrosshairLookupNavCollapsed"
          >
            <i :class="crosshairLookupNavCollapsed ? 'fas fa-ellipsis-h' : 'fas fa-chevron-down'" aria-hidden="true"></i>
          </button>
        </div>
      </div>
      <div
        v-if="isAnyFullscreenDrawingEnabled() && drawingMenuVisible"
        class="chart-drawing-context-menu"
        :style="{ left: `${drawingMenuPosition.x}px`, top: `${drawingMenuPosition.y}px` }"
      >
        <button type="button" class="chart-drawing-context-menu__item" @click="startTrendLineDrawing">趨勢線</button>
        <button type="button" class="chart-drawing-context-menu__item" @click="startHorizontalLineDrawing">水平線</button>
        <button type="button" class="chart-drawing-context-menu__item" @click="startVerticalLineDrawing">垂直線</button>
        <button type="button" class="chart-drawing-context-menu__item" @click="startRectDrawing">矩形框</button>
        <button type="button" class="chart-drawing-context-menu__item" @click="startChannelDrawing">平行通道</button>
        <button v-if="drawingMenuTargetId != null" type="button" class="chart-drawing-context-menu__item chart-drawing-context-menu__item--danger" @click="deleteDrawingById(drawingMenuTargetId)">刪除此畫線</button>
        <button type="button" class="chart-drawing-context-menu__item" @click="clearAllDrawings">清除全部畫線</button>
      </div>
      <div
        v-if="isAnyFullscreenDrawingEnabled() && drawingMode"
        class="chart-drawing-hint"
      >
        {{ buildDrawingInstructionText() }}
      </div>
      <div
        v-if="isDesktopFullscreenDrawingEnabled() && selectedDrawing"
        class="chart-drawing-style-toolbar"
      >
        <div class="chart-drawing-style-toolbar__group">
          <button
            v-for="color in drawingStylePalette"
            :key="color"
            type="button"
            class="chart-drawing-color-swatch"
            :class="{ 'is-active': selectedDrawing?.style?.color === color }"
            :style="{ background: color }"
            @click="updateSelectedDrawingStyle({ color })"
          ></button>
        </div>
        <div class="chart-drawing-style-toolbar__group">
          <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.width || 2) === 2 }" @click="updateSelectedDrawingStyle({ width: 2 })">細</button>
          <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.width || 2) === 3 }" @click="updateSelectedDrawingStyle({ width: 3 })">中</button>
          <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.width || 2) === 5 }" @click="updateSelectedDrawingStyle({ width: 5 })">粗</button>
        </div>
        <div class="chart-drawing-style-toolbar__group">
          <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.dash || 'solid') === 'solid' }" @click="updateSelectedDrawingStyle({ dash: 'solid' })">實線</button>
          <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': selectedDrawing?.style?.dash === 'dashed' }" @click="updateSelectedDrawingStyle({ dash: 'dashed' })">虛線</button>
        </div>
      </div>
      <div
        v-if="isMobileFullscreenDrawingEnabled() && selectedDrawing"
        class="chart-drawing-style-sheet"
      >
        <div class="chart-drawing-style-sheet__handle"></div>
        <div class="chart-drawing-style-sheet__title-row">
          <span class="chart-drawing-style-sheet__title">畫線設定</span>
          <button type="button" class="chart-drawing-style-sheet__close" @click="selectDrawing(null)">完成</button>
        </div>
        <button
          type="button"
          class="chart-drawing-style-sheet__move"
          :class="{ 'is-active': isMobileMoveArmed(selectedDrawing?.id) }"
          @click="toggleMobileMoveMode(selectedDrawing?.id)"
        >
          {{ isMobileMoveArmed(selectedDrawing?.id) ? '移動中：拖曳圖上的畫線或框框' : '移動畫線 / 框框' }}
        </button>
        <div class="chart-drawing-style-sheet__section">
          <span class="chart-drawing-style-sheet__label">顏色</span>
          <div class="chart-drawing-style-toolbar__group chart-drawing-style-toolbar__group--sheet">
            <button
              v-for="color in drawingStylePalette"
              :key="`sheet-${color}`"
              type="button"
              class="chart-drawing-color-swatch"
              :class="{ 'is-active': selectedDrawing?.style?.color === color }"
              :style="{ background: color }"
              @click="updateSelectedDrawingStyle({ color })"
            ></button>
          </div>
        </div>
        <div class="chart-drawing-style-sheet__section">
          <span class="chart-drawing-style-sheet__label">粗細</span>
          <div class="chart-drawing-style-toolbar__group chart-drawing-style-toolbar__group--sheet">
            <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.width || 2) === 2 }" @click="updateSelectedDrawingStyle({ width: 2 })">細</button>
            <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.width || 2) === 3 }" @click="updateSelectedDrawingStyle({ width: 3 })">中</button>
            <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.width || 2) === 5 }" @click="updateSelectedDrawingStyle({ width: 5 })">粗</button>
          </div>
        </div>
        <div class="chart-drawing-style-sheet__section">
          <span class="chart-drawing-style-sheet__label">線型</span>
          <div class="chart-drawing-style-toolbar__group chart-drawing-style-toolbar__group--sheet">
            <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': (selectedDrawing?.style?.dash || 'solid') === 'solid' }" @click="updateSelectedDrawingStyle({ dash: 'solid' })">實線</button>
            <button type="button" class="chart-drawing-style-chip" :class="{ 'is-active': selectedDrawing?.style?.dash === 'dashed' }" @click="updateSelectedDrawingStyle({ dash: 'dashed' })">虛線</button>
          </div>
        </div>
        <button type="button" class="chart-drawing-style-sheet__delete" @click="deleteDrawingById(selectedDrawing?.id)">刪除此畫線</button>
      </div>
      <div v-show="loading && chartData.length === 0" class="chart-loading chart-loading--overlay">
        <i class="fas fa-spinner fa-spin"></i>
        <span>載入中...</span>
      </div>
      <div
        v-show="!loading && chartError"
        class="chart-empty chart-empty--overlay chart-error-overlay"
        role="alert"
        aria-live="assertive"
      >
        <i class="fas fa-triangle-exclamation"></i>
        <p>{{ chartError }}</p>
        <span v-if="chartErrorDetail" class="chart-error-detail">{{ chartErrorDetail }}</span>
        <button type="button" class="chart-error-retry" @click="retryChartLoad">
          <i class="fas fa-rotate-right"></i>
          重新載入
        </button>
      </div>
      <div v-show="!loading && !chartError && chartData.length === 0" class="chart-empty chart-empty--overlay">
        <i class="fas fa-chart-line"></i>
        <p>暫無圖表數據</p>
        <span>此股票在目前週期沒有可顯示的歷史資料</span>
      </div>
      <div
        v-for="(top, i) in (panelLayout?.splitterTopsPx || [])"
        :key="`split-${i}`"
        v-show="isFullscreen"
        class="chart-splitter"
        :class="{ 'is-dragging': splitDragging && activeSplitterIndex === i }"
        :style="{ top: `${top}px` }"
        @pointerdown="(e) => onSplitterPointerDown(i, e)"
        @pointermove="onSplitterPointerMove"
        @pointerup="onSplitterPointerUp"
        @pointercancel="onSplitterPointerUp"
      ></div>
      <div
        v-if="!isFullscreen && !multiTileMode"
        class="legend-overlay-row legend-overlay-row--name"
        :class="{ 'legend-overlay-row--mobile-pinned-inline': liftMobileStockTitleAboveMa }"
      >
        <div class="legend-title-text">
          <i class="fas fa-chart-candlestick"></i>
          <span>{{ stockTitle }}</span>
          <button
            type="button"
            class="legend-toggle-btn legend-toggle-btn--title-side"
            aria-label="切換多空線與均線"
            @click.stop="toggleMaLegendCollapsed"
          >
            <i :class="maLegendCollapsed ? 'fas fa-angle-down' : 'fas fa-angle-up'"></i>
          </button>
        </div>
      </div>
      <div
        v-if="isFullscreen"
        class="legend-overlay-row fullscreen-active"
        :class="{ 'legend-overlay-row--mobile-pinned-inline': liftMobileStockTitleAboveMa }"
      >
        <div
          class="legend-title-text legend-title-text--fullscreen"
          :class="{ 'legend-title-text--fullscreen-inline': liftMobileStockTitleAboveMa }"
        >
          <i class="fas fa-chart-candlestick"></i>
          <span>{{ stockTitle }}</span>
          <button
            type="button"
            class="legend-toggle-btn legend-toggle-btn--title-side legend-toggle-btn--fs-inline"
            aria-label="切換多空線與均線"
            @click.stop="toggleMaLegendCollapsed"
          >
            <i :class="maLegendCollapsed ? 'fas fa-angle-down' : 'fas fa-angle-up'"></i>
          </button>
        </div>
      </div>
      <div
        v-if="controlPanelOpen && multiTileMode && !isFullscreen"
        class="panel-backdrop panel-backdrop--cell"
        @click="toggleControlPanel"
      ></div>
      <!-- Floating Control Panel -->
      <transition name="panel-fade">
        <div
          v-show="controlPanelOpen"
          ref="controlPanelEl"
          class="floating-panel"
          :class="[controlPanelClass, { 'floating-panel--in-tile': useMultiTileEmbeddedPanel }]"
          :style="controlPanelStyle"
          @pointerdown="onControlPanelPointerDown"
          @pointermove="onControlPanelPointerMove"
          @pointerup="onControlPanelPointerUp"
          @pointercancel="onControlPanelPointerUp"
          @click.stop
        >
          <div class="sheet__header">
            <div class="panel-drag-area">
              <div class="panel-drag-handle"></div>
            </div>
            <div class="panel-header">
              <span class="panel-title">圖表控制</span>
              <div class="panel-header-actions">
                <button
                  class="panel-expand"
                  type="button"
                  @pointerdown.stop
                  @pointerup.stop
                  @click="toggleControlPanelExpanded"
                >
                  <i :class="sheetStage === 'full' ? 'fas fa-angle-down' : 'fas fa-angle-up'"></i>
                </button>
                <button class="panel-close" @pointerdown.stop @pointerup.stop @click="toggleControlPanel">
                  <i class="fas fa-times"></i>
                </button>
              </div>
            </div>
          </div>

          <div class="sheet__content">

          <div class="ai-analysis-row">
            <button class="ai-analysis-btn" type="button" @click="runAiTechnicalAnalysis">AI 技術結構解讀</button>
          </div>
          
          <div class="panel-section">
            <div class="section-header" @click="togglePanelSection('ma')">
              <label class="section-label">均線設定</label>
              <div class="ma-quick-actions" @click.stop>
                <button type="button" class="ma-quick-btn ma-quick-btn--on" @click="enableAllMA" title="全部開啟">
                  <i class="fas fa-eye"></i><span>全開</span>
                </button>
                <button type="button" class="ma-quick-btn ma-quick-btn--off" @click="disableAllMA" title="全部關閉">
                  <i class="fas fa-eye-slash"></i><span>全關</span>
                </button>
              </div>
              <i class="fas fa-chevron-down section-toggle" :class="{ 'rotated': !panelSections.ma }"></i>
            </div>
            <transition name="collapse">
              <div class="parameter-controls" v-show="panelSections.ma">
              <div class="param-group">
                <label class="ma-toggle-label">
                  <input
                    type="checkbox"
                    v-model="showMA1"
                    @change="saveMAVisibility"
                  />
                  <span>MA1:</span>
                </label>
                <input 
                  type="number" 
                  v-model.number="maParams.ma1" 
                  min="1" 
                  max="200" 
                  @change="saveMAParams"
                  class="param-input"
                />
              </div>
              <div class="param-group">
                <label class="ma-toggle-label">
                  <input
                    type="checkbox"
                    v-model="showMA2"
                    @change="saveMAVisibility"
                  />
                  <span>MA2:</span>
                </label>
                <input 
                  type="number" 
                  v-model.number="maParams.ma2" 
                  min="1" 
                  max="200" 
                  @change="saveMAParams"
                  class="param-input"
                />
              </div>
              <div class="param-group">
                <label class="ma-toggle-label">
                  <input
                    type="checkbox"
                    v-model="showMA3"
                    @change="saveMAVisibility"
                  />
                  <span>MA3:</span>
                </label>
                <input 
                  type="number" 
                  v-model.number="maParams.ma3" 
                  min="1" 
                  max="200" 
                  @change="saveMAParams"
                  class="param-input"
                />
              </div>
              <div class="param-group">
                <label class="ma-toggle-label">
                  <input
                    type="checkbox"
                    v-model="showMA4"
                    @change="saveMAVisibility"
                  />
                  <span>MA4:</span>
                </label>
                <input 
                  type="number" 
                  v-model.number="maParams.ma4" 
                  min="1" 
                  max="200" 
                  @change="saveMAParams"
                  class="param-input"
                />
              </div>
              <div class="param-group">
                <label class="ma-toggle-label">
                  <input
                    type="checkbox"
                    v-model="showMA5"
                    @change="saveMAVisibility"
                  />
                  <span>MA5:</span>
                </label>
                <input 
                  type="number" 
                  v-model.number="maParams.ma5" 
                  min="1" 
                  max="200" 
                  @change="saveMAParams"
                  class="param-input"
                />
              </div>
              <div class="param-group param-group--hma">
                <label class="hma-toggle-label" style="display: flex; align-items: center; gap: 6px;" @click.stop="handleTechGate('Pro', canUseProTech, $event)">
                  <input 
                    type="checkbox" 
                    v-model="showHMA"
                    class="param-checkbox-hma"
                    :disabled="!canUseProTech"
                  />
                  <span>多空線:</span>
                </label>
                <!-- 固定周期，不顯示輸入框，避免空白框 -->
                <input 
                  type="number" 
                  v-model.number="hmaParams.period" 
                  min="2" 
                  max="200" 
                  @change="saveHMAParams"
                  class="param-input"
                  :disabled="!canUseProTech"
                />
              </div>
              </div>
            </transition>
          </div>
          
          <div class="panel-section">
            <div class="section-header" @click="togglePanelSection('indicators')">
              <label class="section-label">技術指標</label>
              <i class="fas fa-chevron-down section-toggle" :class="{ 'rotated': !panelSections.indicators }"></i>
            </div>
            <transition name="collapse">
              <div v-show="panelSections.indicators">
            <div class="indicator-toggles">
              <label class="indicator-toggle">
                <input
                  type="checkbox"
                  v-model="showMainK"
                  class="toggle-checkbox"
                />
                <span class="toggle-label">
                  <i class="fas fa-chart-candlestick"></i>
                  <span>主K線</span>
                </span>
              </label>


              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main" @click.stop="handleTechGate('Pro', canUseHeikinLadder, $event)">
                    <input
                      type="checkbox"
                      :checked="showReversal"
                      :disabled="!canUseHeikinLadder"
                      @change="toggleReversalFromPanel"
                      class="toggle-checkbox"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-chart-line"></i>
                      <span>階梯線</span>
                    </span>
                  </label>
                  <button
                    v-show="showReversal"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.reversal"
                    :title="indicatorDetailExpanded.reversal ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('reversal')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.reversal ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showReversal && indicatorDetailExpanded.reversal" class="parameter-controls">
                <div class="param-group">
                  <label>顯示上漲(紅):</label>
                  <input type="checkbox" v-model="showReversalUp" class="param-checkbox" @change="saveReversalVisibility" />
                </div>
                <div class="param-group">
                  <label>顯示下跌(綠):</label>
                  <input type="checkbox" v-model="showReversalDown" class="param-checkbox" @change="saveReversalVisibility" />
                </div>
              </div>
              </div>

              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main">
                    <input 
                      type="checkbox" 
                      v-model="showKD"
                      class="toggle-checkbox"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-chart-line"></i>
                      <span>KD指標</span>
                    </span>
                  </label>
                  <button
                    v-show="showKD"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.kd"
                    :title="indicatorDetailExpanded.kd ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('kd')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.kd ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showKD && indicatorDetailExpanded.kd" class="parameter-controls">
                <div class="param-group">
                  <label>週期:</label>
                  <input 
                    type="number" 
                    v-model.number="kdParams.period" 
                    min="5" 
                    max="50" 
                    @change="saveKDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>K值:</label>
                  <input 
                    type="number" 
                    v-model.number="kdParams.k" 
                    min="1" 
                    max="10" 
                    @change="saveKDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>D值:</label>
                  <input 
                    type="number" 
                    v-model.number="kdParams.d" 
                    min="1" 
                    max="10" 
                    @change="saveKDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>顯示 K 線:</label>
                  <input type="checkbox" v-model="showKLine" class="param-checkbox" />
                </div>
                <div class="param-group">
                  <label>顯示 D 線:</label>
                  <input type="checkbox" v-model="showDLine" class="param-checkbox" />
                </div>
                <div class="param-group">
                  <label>加粗 KD 線:</label>
                  <input type="checkbox" v-model="kdBold" class="param-checkbox" />
                </div>
                <div class="param-group">
                  <label>顯示50中線:</label>
                  <input type="checkbox" v-model="kdMidline" class="param-checkbox" @change="() => {}" />
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetKDParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>
              
              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main">
                    <input 
                      type="checkbox" 
                      v-model="showMACD"
                      class="toggle-checkbox"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-chart-area"></i>
                      <span>MACD指標</span>
                    </span>
                  </label>
                  <button
                    v-show="showMACD"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.macd"
                    :title="indicatorDetailExpanded.macd ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('macd')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.macd ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showMACD && indicatorDetailExpanded.macd" class="parameter-controls">
                <div class="param-group">
                  <label>顯示 DIF:</label>
                  <input
                    type="checkbox"
                    class="param-checkbox"
                    :checked="hasMacdDisplay('dif')"
                    @change="toggleMacdDisplay('dif')"
                  />
                </div>
                <div class="param-group">
                  <label>顯示 MACD:</label>
                  <input
                    type="checkbox"
                    class="param-checkbox"
                    :checked="hasMacdDisplay('macd')"
                    @change="toggleMacdDisplay('macd')"
                  />
                </div>
                <div class="param-group">
                  <label>顯示 OSC:</label>
                  <input
                    type="checkbox"
                    class="param-checkbox"
                    :checked="hasMacdDisplay('osc')"
                    @change="toggleMacdDisplay('osc')"
                  />
                </div>
                <div class="param-group">
                  <label>DIF線寬:</label>
                  <input 
                    type="number"
                    v-model.number="macdLineWidths.dif"
                    min="1"
                    max="8"
                    step="0.5"
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>MACD線寬:</label>
                  <input 
                    type="number"
                    v-model.number="macdLineWidths.macd"
                    min="1"
                    max="8"
                    step="0.5"
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>OSC長短:</label>
                  <input 
                    type="number"
                    v-model.number="macdHistHeight"
                    min="0.2"
                    max="10"
                    step="0.1"
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>OSC柱寬:</label>
                  <input 
                    type="number"
                    v-model.number="macdOscStyle.barWidth"
                    min="10"
                    max="100"
                    step="5"
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>紅柱顏色:</label>
                  <input type="color" v-model="macdOscStyle.colorUp" @change="saveMACDParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>綠柱顏色:</label>
                  <input type="color" v-model="macdOscStyle.colorDown" @change="saveMACDParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>紅柱透明:</label>
                  <input 
                    type="number"
                    v-model.number="macdOscStyle.opacityUp"
                    min="0.1"
                    max="1"
                    step="0.1"
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>綠柱透明:</label>
                  <input 
                    type="number"
                    v-model.number="macdOscStyle.opacityDown"
                    min="0.1"
                    max="1"
                    step="0.1"
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>快線:</label>
                  <input 
                    type="number" 
                    v-model.number="macdParams.fast" 
                    min="5" 
                    max="200" 
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>慢線:</label>
                  <input 
                    type="number" 
                    v-model.number="macdParams.slow" 
                    min="10" 
                    max="300" 
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>信號:</label>
                  <input 
                    type="number" 
                    v-model.number="macdParams.signal" 
                    min="3" 
                    max="50" 
                    @change="saveMACDParams"
                    class="param-input"
                  />
                </div>
                <div v-if="chartData.length > 0 && (macdParams.slow + macdParams.signal) > chartData.length" class="param-warning">
                  <i class="fas fa-exclamation-triangle"></i>
                  <span>參數過大！需要至少 {{ macdParams.slow + macdParams.signal }} 個數據點，但只有 {{ chartData.length }} 個</span>
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetMACDParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>

              <label class="indicator-toggle">
                <input 
                  type="checkbox" 
                  v-model="showVolume"
                  class="toggle-checkbox"
                />
                <span class="toggle-label">
                  <i class="fas fa-chart-bar"></i>
                  <span>成交量</span>
                </span>
              </label>
              


              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main" @click.stop="handleTechGate('Pro', canUseProTech, $event)">
                    <input
                      type="checkbox"
                      v-model="showHMAInd"
                      class="toggle-checkbox"
                      :disabled="!canUseProTech"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-chart-area"></i>
                      <span>多空趨勢線</span>
                    </span>
                  </label>
                  <button
                    v-show="showHMAInd"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.hmaInd"
                    :title="indicatorDetailExpanded.hmaInd ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('hmaInd')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.hmaInd ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showHMAInd && indicatorDetailExpanded.hmaInd" class="parameter-controls">
                <div class="param-group">
                  <label>週期:</label>
                  <input
                    type="number"
                    v-model.number="hmaIndParams.period"
                    min="2"
                    max="200"
                    @change="saveHMAIndParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>Padding(%):</label>
                  <input
                    type="number"
                    v-model.number="hmaIndParams.paddingPct"
                    min="0"
                    max="20"
                    step="0.1"
                    @change="saveHMAIndParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>上漲顏色:</label>
                  <input type="color" v-model="hmaIndParams.upColor" @change="saveHMAIndParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>下跌顏色:</label>
                  <input type="color" v-model="hmaIndParams.downColor" @change="saveHMAIndParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>0軸:</label>
                  <input type="checkbox" v-model="hmaIndParams.zeroVisible" @change="saveHMAIndParams" />
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetHMAIndParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>

              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main" @click.stop="handleTechGate('Pro', canUseProTech, $event)">
                    <input 
                      type="checkbox" 
                      v-model="showGoldenWave"
                      class="toggle-checkbox"
                      :disabled="!canUseProTech"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-wave-square"></i>
                      <span>動態轉折(小不點）</span>
                    </span>
                  </label>
                  <button
                    v-show="showGoldenWave"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.goldenWave"
                    :title="indicatorDetailExpanded.goldenWave ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('goldenWave')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.goldenWave ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showGoldenWave && indicatorDetailExpanded.goldenWave" class="parameter-controls">
                <div class="param-group">
                  <label>快速:</label>
                  <input 
                    type="number" 
                    v-model.number="goldenWaveParams.fastMa" 
                    min="5" 
                    max="100" 
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>慢速:</label>
                  <input 
                    type="number" 
                    v-model.number="goldenWaveParams.slowMa" 
                    min="5" 
                    max="100" 
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div v-if="false">
                <div class="param-group">
                  <label>多空:</label>
                  <input 
                    type="number" 
                    v-model.number="goldenWaveParams.multiMa" 
                    min="5" 
                    max="100" 
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>波段二:</label>
                  <input
                    type="number"
                    v-model.number="goldenWaveParams.waveMa2"
                    min="2"
                    max="200"
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>波段三:</label>
                  <input
                    type="number"
                    v-model.number="goldenWaveParams.waveMa3"
                    min="2"
                    max="200"
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>箱體:</label>
                  <input 
                    type="number" 
                    v-model.number="goldenWaveParams.boxPeriod" 
                    min="10" 
                    max="200" 
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>柱狀倍率:</label>
                  <input
                    type="number"
                    v-model.number="goldenWaveParams.barScale"
                    min="0.5"
                    max="100"
                    step="0.5"
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>上漲顏色:</label>
                  <input type="color" v-model="goldenWaveParams.barUpColor" @change="saveGoldenWaveParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>止漲顏色:</label>
                  <input type="color" v-model="goldenWaveParams.barStopUpColor" @change="saveGoldenWaveParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>下跌顏色:</label>
                  <input type="color" v-model="goldenWaveParams.barDownColor" @change="saveGoldenWaveParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>止跌顏色:</label>
                  <input type="color" v-model="goldenWaveParams.barStopDownColor" @change="saveGoldenWaveParams" class="param-input" />
                </div>
                </div>
                <div class="param-group">
                  <label>快線:</label>
                  <input type="checkbox" v-model="goldenWaveParams.showDifLine" @change="saveGoldenWaveParams" />
                </div>
                <div class="param-group" v-show="goldenWaveParams.showDifLine">
                  <label>快線顏色:</label>
                  <input type="color" v-model="goldenWaveParams.difLineColor" @change="saveGoldenWaveParams" class="param-input" />
                </div>
                <div class="param-group" v-show="goldenWaveParams.showDifLine">
                  <label>快線粗細:</label>
                  <input
                    type="number"
                    v-model.number="goldenWaveParams.difLineWidth"
                    min="0.5"
                    max="10"
                    step="0.5"
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>慢線:</label>
                  <input type="checkbox" v-model="goldenWaveParams.showMa2Line" @change="saveGoldenWaveParams" />
                </div>
                <div class="param-group" v-show="goldenWaveParams.showMa2Line">
                  <label>慢線顏色:</label>
                  <input type="color" v-model="goldenWaveParams.ma2LineColor" @change="saveGoldenWaveParams" class="param-input" />
                </div>
                <div class="param-group" v-show="goldenWaveParams.showMa2Line">
                  <label>慢線粗細:</label>
                  <input
                    type="number"
                    v-model.number="goldenWaveParams.ma2LineWidth"
                    min="0.5"
                    max="10"
                    step="0.5"
                    @change="saveGoldenWaveParams"
                    class="param-input"
                  />
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetGoldenWaveParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>

              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main" @click.stop="handleTechGate('Pro', canUseProTech, $event)">
                    <input 
                      type="checkbox" 
                      v-model="showCCI"
                      class="toggle-checkbox"
                      :disabled="!canUseProTech"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-sliders-h"></i>
                      <span>ORC指標</span>
                    </span>
                  </label>
                  <button
                    v-show="showCCI"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.cci"
                    :title="indicatorDetailExpanded.cci ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('cci')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.cci ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showCCI && indicatorDetailExpanded.cci" class="parameter-controls">
                <div class="param-group">
                  <label>週期:</label>
                  <input
                    type="number"
                    v-model.number="cciParams.period"
                    min="2"
                    max="300"
                    step="1"
                    @change="saveCCIParams"
                    class="param-input"
                    :disabled="!canUseProTech"
                  />
                </div>
                <div class="param-group">
                  <label>柱狀寬度%:</label>
                  <input
                    type="number"
                    v-model.number="orcStyle.barWidthPct"
                    min="10"
                    max="100"
                    step="1"
                    @change="saveOrcStyle"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>0以下顏色:</label>
                  <input type="color" v-model="orcStyle.colorNeg" @change="saveOrcStyle" class="param-input" />
                </div>
                <div class="param-group">
                  <label>0~100顏色:</label>
                  <input type="color" v-model="orcStyle.colorMid" @change="saveOrcStyle" class="param-input" />
                </div>
                <div class="param-group">
                  <label>100以上顏色:</label>
                  <input type="color" v-model="orcStyle.colorPos" @change="saveOrcStyle" class="param-input" />
                </div>
              </div>
              </div>

              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main">
                    <input
                      type="checkbox"
                      v-model="showRSI"
                      class="toggle-checkbox"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-chart-line"></i>
                      <span>RSI指標</span>
                    </span>
                  </label>
                  <button
                    v-show="showRSI"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.rsi"
                    :title="indicatorDetailExpanded.rsi ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('rsi')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.rsi ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showRSI && indicatorDetailExpanded.rsi" class="parameter-controls">
                <div class="param-group">
                  <label>週期:</label>
                  <input
                    type="number"
                    v-model.number="rsiParams.period"
                    min="2"
                    max="100"
                    step="1"
                    @change="saveRSIParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>超買:</label>
                  <input
                    type="number"
                    v-model.number="rsiParams.overbought"
                    min="50"
                    max="100"
                    step="1"
                    @change="saveRSIParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>超賣:</label>
                  <input
                    type="number"
                    v-model.number="rsiParams.oversold"
                    min="0"
                    max="50"
                    step="1"
                    @change="saveRSIParams"
                    class="param-input"
                  />
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetRSIParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>

              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main">
                    <input
                      type="checkbox"
                      v-model="showBB"
                      class="toggle-checkbox"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-chart-area"></i>
                      <span>布林通道</span>
                    </span>
                  </label>
                  <button
                    v-show="showBB"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.bb"
                    :title="indicatorDetailExpanded.bb ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('bb')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.bb ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showBB && indicatorDetailExpanded.bb" class="parameter-controls">
                <div class="param-group">
                  <label>週期:</label>
                  <input
                    type="number"
                    v-model.number="bbParams.period"
                    min="2"
                    max="300"
                    step="1"
                    @change="saveBBParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>標準差倍數:</label>
                  <input
                    type="number"
                    v-model.number="bbParams.mult"
                    min="0.5"
                    max="10"
                    step="0.1"
                    @change="saveBBParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>上軌顏色:</label>
                  <input type="color" v-model="bbParams.colorUpper" @input="saveBBParams" @change="saveBBParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>中軌顏色:</label>
                  <input type="color" v-model="bbParams.colorMid" @input="saveBBParams" @change="saveBBParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>下軌顏色:</label>
                  <input type="color" v-model="bbParams.colorLower" @input="saveBBParams" @change="saveBBParams" class="param-input" />
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetBBParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>

              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main" @click.stop="handleTechGate('Pro', canUsePrimeTech, $event)">
                    <input 
                      type="checkbox" 
                      v-model="showVPVR"
                      class="toggle-checkbox"
                      :disabled="!canUsePrimeTech"
                    />
                    <span class="toggle-label">
                      <i class="fas fa-align-left"></i>
                      <span>價量累積(分價量)</span>
                    </span>
                  </label>
                  <button
                    v-show="showVPVR"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.vpvr"
                    :title="indicatorDetailExpanded.vpvr ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('vpvr')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.vpvr ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showVPVR && indicatorDetailExpanded.vpvr" class="parameter-controls">
                <div class="param-group">
                  <label>分桶數:</label>
                  <input
                    type="number"
                    v-model.number="vpvrParams.bins"
                    min="6"
                    max="120"
                    @change="saveVPVRParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>最大寬度%:</label>
                  <input
                    type="number"
                    v-model.number="vpvrParams.maxWidthPct"
                    min="0.1"
                    max="0.95"
                    step="0.01"
                    @change="saveVPVRParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>最小寬度%:</label>
                  <input
                    type="number"
                    v-model.number="vpvrParams.minWidthPct"
                    min="0"
                    max="0.2"
                    step="0.01"
                    @change="saveVPVRParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>填色:</label>
                  <input type="color" v-model="vpvrParams.fillColor" @change="saveVPVRParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>填色透明:</label>
                  <input
                    type="range"
                    v-model.number="vpvrParams.fillAlpha"
                    min="0"
                    max="1"
                    step="0.01"
                    @change="saveVPVRParams"
                    class="param-input"
                  />
                </div>
                <div class="param-group">
                  <label>描邊:</label>
                  <input type="color" v-model="vpvrParams.strokeColor" @change="saveVPVRParams" class="param-input" />
                </div>
                <div class="param-group">
                  <label>描邊透明:</label>
                  <input
                    type="range"
                    v-model.number="vpvrParams.strokeAlpha"
                    min="0"
                    max="1"
                    step="0.01"
                    @change="saveVPVRParams"
                    class="param-input"
                  />
                </div>
              </div>
              </div>

              <label class="indicator-toggle" @click.stop="handleTechGate('Pro', canUsePrimeTech, $event)">
                <input 
                  type="checkbox" 
                  v-model="showFib"
                  class="toggle-checkbox"
                  :disabled="!canUsePrimeTech"
                />
                <span class="toggle-label">
                  <i class="fas fa-ruler-horizontal"></i>
                  <span>黃金切割率</span>
                </span>
                <button
                  class="help-icon"
                  type="button"
                  @click.stop.prevent="toggleHelp('fib')"
                  title="說明"
                >
                  <i class="fas fa-info-circle"></i>
                </button>
              </label>
              <div v-show="helpOpenKey === 'fib'" class="ind-help-popover">
                <div class="help-title">黃金切割率</div>
                <div class="help-body">
                  <p>定義：以目前載入的全部歷史走勢最高價與最低價為基準，繪出 0%～100% 之間的黃金分割價位水平線。</p>
                  <ul>
                    <li>比例：0%、23.6%、38.2%、50%、61.8%、78.6%、100%。</li>
                    <li>用法：觀察價格在重要比例附近的支撐／壓力與反轉機會。</li>
                    <li>說明：比例固定，以整段歷史高低點計算，不隨縮放區間改變。</li>
                  </ul>
                </div>
              </div>
              
              <div class="indicator-block">
                <div class="indicator-block__row">
                  <label class="indicator-toggle indicator-toggle--block-main" @click.stop="handleTechGate('Pro', canUsePrimeTech, $event)">
                    <input 
                      type="checkbox" 
                      v-model="showDiagSR"
                      class="toggle-checkbox"
                      :disabled="!canUsePrimeTech"
                    />
                    <span class="toggle-label">
                  <i class="fas fa-draw-polygon"></i>
                  <span>斜線壓力／支撐</span>
                </span>
                    <button
                  class="help-icon"
                  type="button"
                  @click.stop.prevent="toggleHelp('diagSR')"
                  title="說明"
                >
                  <i class="fas fa-info-circle"></i>
                </button>
                  </label>
                  <button
                    v-show="showDiagSR"
                    type="button"
                    class="indicator-detail-collapse-btn"
                    :aria-expanded="indicatorDetailExpanded.diagSR"
                    :title="indicatorDetailExpanded.diagSR ? indDetailTitleCollapse : indDetailTitleExpand"
                    @click.stop="toggleIndicatorDetail('diagSR')"
                  >
                    <i class="fas" :class="indicatorDetailExpanded.diagSR ? 'fa-chevron-up' : 'fa-chevron-down'"></i>
                  </button>
                </div>
              <div v-show="showDiagSR && indicatorDetailExpanded.diagSR" class="param-grid param-grid--diag-sr">
                <div class="param-row">
                  <label title="只使用最近 N 根 K 線來找轉折並畫線；數字愈大愈偏長期結構">最近 K 數（計算範圍）</label>
                  <input
                    type="number"
                    min="20"
                    max="300"
                    v-model.number="diagSrParams.windowSize"
                    @change="saveDiagSrParams"
                    class="param-input"
                    title="20～300"
                  />
                </div>
                <div class="param-row">
                  <label title="判定波峰／波谷時，向左與向右各比對幾根 K；數字愈大轉折愈少、斜線愈少">轉折比對（左右各幾根）</label>
                  <input
                    type="number"
                    min="1"
                    max="20"
                    v-model.number="diagSrParams.lookback"
                    @change="saveDiagSrParams"
                    class="param-input"
                    title="1～20"
                  />
                </div>
                <div class="param-row">
                  <label title="兩個轉折在時間軸上至少相隔幾根 K 才連成一段斜線，可過濾過短的碎線">成線最短間隔（根）</label>
                  <input
                    type="number"
                    min="1"
                    max="50"
                    v-model.number="diagSrParams.minSpan"
                    @change="saveDiagSrParams"
                    class="param-input"
                    title="1～50"
                  />
                </div>
                <div class="param-row">
                  <label title="壓力線與支撐線各自最多畫幾條；兩邊分開計算">壓力／支撐：各顯示幾條</label>
                  <input
                    type="number"
                    min="1"
                    max="8"
                    v-model.number="diagSrParams.maxSegments"
                    @change="saveDiagSrParams"
                    class="param-input"
                    title="每側 1～8 條"
                  />
                </div>
                <div class="param-row param-row--full">
                  <button type="button" class="param-reset-btn" @click="resetDiagSrParams">
                    <i class="fas fa-undo"></i>
                    <span>恢復預設</span>
                  </button>
                </div>
              </div>
              </div>
              <div v-show="helpOpenKey === 'diagSR'" class="ind-help-popover">
                <div class="help-title">斜線壓力／支撐</div>
                <div class="help-body">
                  <p>定義：在「最近 K 數（計算範圍）」內，以左右對稱區間辨識波峰／波谷，將相鄰高點或低點連成斜線並延伸至最新 K 線，用來觀察斜率型通道。</p>
                  <ul>
                    <li>壓力線連結相鄰高點，支撐線連結相鄰低點；新高／新低出現後會依目前「最近 K 數」設定重算。</li>
                  </ul>
                  <p><strong>參數對照（與面板由上到下次序相同）</strong>（日線約 20 根 ≈ 一個月交易日）</p>
                  <ul>
                    <li><strong>最近 K 數（計算範圍）</strong>：計算用的歷史長度；愈大愈偏長期。</li>
                    <li><strong>轉折比對（左右各幾根）</strong>：左右各比對幾根 K 才視為轉折；數字愈大線愈少。</li>
                    <li><strong>成線最短間隔（根）</strong>：兩轉折至少相隔幾根 K 才連線；愈大愈能濾掉碎線。</li>
                    <li><strong>壓力／支撐：各顯示幾條</strong>：壓力、支撐<strong>各自</strong>最多幾條斜線。</li>
                  </ul>
                  <p><strong>短期（約 1～2 個月）</strong></p>
                  <ul>
                    <li>建議：最近 K 數 <strong>40～80</strong>、轉折比對 <strong>3～4</strong>、成線間隔 <strong>5～10</strong>、各側 <strong>3～5</strong> 條。</li>
                    <li>用途：貼近最近整理與小通道；線條變動較快，可拉長「最近 K 數」與中期設定對照。</li>
                  </ul>
                  <p><strong>中期（約 3～6 個月）</strong></p>
                  <ul>
                    <li>建議：最近 K 數 <strong>100～180</strong>、轉折比對 <strong>5～6</strong>、成線間隔 <strong>8～18</strong>、各側 <strong>3～4</strong> 條。</li>
                    <li>用途：與預設（120／5／5／3）相近；線太碎可優先<strong>加大成線間隔</strong>或略加大轉折比對。</li>
                  </ul>
                  <p><strong>長期（約半年～一年以上）</strong></p>
                  <ul>
                    <li>建議：最近 K 數 <strong>200～300</strong>（勿超過圖表已載入根數）、轉折比對 <strong>6～12</strong>、成線間隔 <strong>15～35</strong>、各側 <strong>2～3</strong> 條。</li>
                    <li>用途：大級距上下緣；線少、每條意義較重。</li>
                  </ul>
                  <p><strong>週 K</strong>：同一數字代表更長日曆時間；轉折比對可比日線略降 1～2，以免轉折過少。</p>
                  <p style="margin-top:8px;opacity:0.85">以上為指標操作說明，非投資建議。</p>
                </div>
              </div>
              
            </div>
              </div>
            </transition>
          </div>

          </div>
        </div>
      </transition>
      
      <!-- 全螢幕單圖專用 panel-backdrop；多格用 chart-wrapper 內的 panel-backdrop--cell -->
      <div v-if="controlPanelOpen && isFullscreen" class="panel-backdrop" @click="toggleControlPanel"></div>

      <div v-if="upgradeModalOpen && isFullscreen" class="upgrade-modal" role="dialog" aria-modal="true" @click.self="closeUpgradeModal">
        <div class="upgrade-modal__panel">
          <div class="upgrade-modal__header">
            <div class="upgrade-modal__head-left">
              <div class="upgrade-modal__icon" aria-hidden="true">
                <i class="fas fa-crown"></i>
              </div>
              <div>
                <div class="upgrade-modal__title">{{ upgradeModalTitle || '需要升級方案' }}</div>
                <div class="upgrade-modal__subtitle">功能未開放</div>
              </div>
            </div>
            <button class="upgrade-modal__close" type="button" aria-label="close" @click="closeUpgradeModal">
              <i class="fas fa-times"></i>
            </button>
          </div>
          <div class="upgrade-modal__body">
            <p class="upgrade-modal__lead">{{ upgradeModalMessage }}</p>
          </div>
          <div class="upgrade-modal__actions">
            <button class="upgrade-modal__btn upgrade-modal__btn--ghost" type="button" @click="closeUpgradeModal">我知道了</button>
            <button class="upgrade-modal__btn upgrade-modal__btn--primary" type="button" @click="goToPricing">立即升級</button>
          </div>
        </div>
      </div>

      <div v-if="aiModalOpen" class="ai-modal" @click.self="aiModalOpen = false">
        <div class="ai-modal__panel">
          <div class="ai-modal__header">
            <span class="ai-modal__title">自動生成的數據解讀內容（非投資建議）</span>
            <button class="ai-modal__close" type="button" @click="aiModalOpen = false"><i class="fas fa-times"></i></button>
          </div>
          <div class="ai-modal__body">
            <div v-if="aiLoading" class="ai-modal__loading">生成中...</div>
            <div v-else-if="aiError" class="ai-modal__error">{{ aiError }}</div>
            <div v-else class="ai-modal__content">
              <div class="ai-modal__disclaimer">本內容為依據可見數據生成之解讀內容，僅供研究參考，不構成任何買賣建議或報酬保證。</div>
              <pre class="ai-modal__text">{{ aiText }}</pre>
              <div v-if="aiUsage" class="ai-modal__usage">
                <span class="ai-modal__usage-label">Tokens</span>
                <span class="ai-modal__usage-item">prompt: {{ aiUsage.prompt_tokens ?? '—' }}</span>
                <span class="ai-modal__usage-item">completion: {{ aiUsage.completion_tokens ?? '—' }}</span>
                <span class="ai-modal__usage-item">total: {{ aiUsage.total_tokens ?? '—' }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    
  </div>
</template>

<style scoped>
.stock-chart {
  display: flex;
  flex-direction: column;
  gap: 0;
  --chart-container-height: 640px;
  --chart-container-min-height: 640px;
  /* Remove any outer frame/glow on the container */
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
}

/* App 全螢幕四分割：格內填滿 */
.stock-chart--multi-tile {
  min-height: 0 !important;
  height: 100% !important;
  max-height: 100%;
  display: flex !important;
  flex-direction: column !important;
  overflow: hidden;
  position: relative;
  isolation: isolate;
}
.stock-chart--multi-tile .chart-header--multi-tile {
  flex: 0 0 auto;
  margin: 0;
  border-radius: 10px 10px 0 0;
  border-bottom: 1px solid rgba(100, 200, 255, 0.12);
  background: linear-gradient(140deg, rgba(15, 23, 42, 0.92), rgba(30, 64, 175, 0.4));
  padding: 0;
  box-shadow: none;
}
.stock-chart--multi-tile .chart-wrapper {
  flex: 1 1 0;
  min-height: 0;
  margin-top: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.fs-mt-bar {
  width: 100%;
  padding: 4px 6px 6px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.fs-mt-top {
  display: flex;
  align-items: center;
  gap: 4px;
  flex-wrap: wrap;
  min-width: 0;
}
.fs-mt-title {
  flex: 0 1 auto;
  min-width: 0;
  max-width: 48%;
  font-size: 0.78rem;
  font-weight: 700;
  color: #e2e8f0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.fs-mt-date {
  margin-left: 6px;
  font-size: 0.72rem;
  font-weight: 500;
  color: rgba(226, 232, 240, 0.78);
}
.fs-mt-periods {
  display: flex;
  flex: 1 1 auto;
  flex-wrap: wrap;
  gap: 2px;
  min-width: 0;
  justify-content: center;
}
.fs-mt-chip {
  border: 1px solid rgba(100, 200, 255, 0.2);
  background: rgba(15, 23, 42, 0.45);
  color: #cbd5e1;
  border-radius: 6px;
  padding: 1px 6px;
  font-size: 0.65rem;
  font-weight: 600;
  cursor: pointer;
  line-height: 1.2;
}
.fs-mt-chip.active {
  background: rgba(59, 130, 246, 0.4);
  border-color: rgba(96, 165, 250, 0.55);
  color: #fff;
}
.fs-mt-ico {
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(100, 200, 255, 0.22);
  background: rgba(15, 23, 42, 0.55);
  border-radius: 8px;
  color: #cbd5e1;
  cursor: pointer;
  font-size: 0.8rem;
  padding: 0;
  flex: 0 0 auto;
}
.fs-mt-search {
  display: flex;
  align-items: center;
  gap: 4px;
  width: 100%;
  min-width: 0;
}
.fs-mt-input {
  flex: 1 1 auto;
  min-width: 0;
  border-radius: 8px;
  border: 1px solid rgba(100, 200, 255, 0.25);
  background: rgba(15, 23, 42, 0.65);
  color: #ffffff;
  caret-color: #ffffff;
  padding: 4px 8px;
  font-size: 0.75rem;
  font-weight: 600;
}

.fs-mt-input::placeholder {
  color: rgba(255, 255, 255, 0.88);
  opacity: 1;
}

/* CSS-first fullscreen: applied immediately by applyCssFullscreen().
   Override the transparent background so the overlay is opaque. */
.warrant-fs-panel {
  position: absolute;
  top: 4.5rem;
  right: 0.75rem;
  z-index: 40;
  width: min(300px, calc(100vw - 1.5rem));
  max-height: min(70vh, 520px);
  overflow: auto;
  border-radius: 12px;
  border: 1px solid rgba(0, 212, 255, 0.35);
  background: rgba(8, 14, 22, 0.96);
  box-shadow: 0 12px 36px rgba(0, 0, 0, 0.45);
  backdrop-filter: blur(10px);
  padding: 0.7rem 0.8rem 0.85rem;
}
.warrant-fs-panel__head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.55rem;
  margin-bottom: 0.65rem;
  padding-bottom: 0.55rem;
  border-bottom: 1px solid rgba(148, 183, 205, 0.16);
}
.warrant-fs-panel__titles {
  min-width: 0;
  display: grid;
  gap: 0.15rem;
}
.warrant-fs-panel__eyebrow {
  font-size: 0.72rem;
  color: #7dd3fc;
  font-weight: 600;
}
.warrant-fs-panel__title {
  font-size: 0.92rem;
  font-weight: 700;
  color: #e8f7ff;
  line-height: 1.35;
  word-break: break-word;
}
.warrant-fs-panel__close {
  width: 30px;
  height: 30px;
  flex-shrink: 0;
  border: 1px solid rgba(148, 183, 205, 0.22);
  border-radius: 8px;
  background: transparent;
  color: #c2cce0;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}
.warrant-fs-panel__close:hover {
  border-color: rgba(0, 212, 255, 0.45);
  color: #e8f7ff;
}
.warrant-fs-panel__list {
  margin: 0;
  display: grid;
  gap: 0.35rem;
}
.warrant-fs-panel__row {
  display: grid;
  grid-template-columns: 5.2rem 1fr;
  gap: 0.45rem;
  align-items: baseline;
  padding: 0.28rem 0.35rem;
  border-radius: 8px;
  background: rgba(2, 8, 14, 0.4);
}
.warrant-fs-panel__row dt {
  margin: 0;
  font-size: 0.75rem;
  color: #9bb0c0;
}
.warrant-fs-panel__row dd {
  margin: 0;
  font-size: 0.86rem;
  font-weight: 650;
  color: #eef5f8;
  font-variant-numeric: tabular-nums;
  text-align: right;
  word-break: break-word;
}
.action-icon-btn.active {
  border-color: rgba(0, 212, 255, 0.65);
  color: #38bdf8;
  background: rgba(0, 212, 255, 0.12);
}

.stock-chart.is-fullscreen {
  background: #0b1220 !important;
  position: relative;
}


.chart-header {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 0;
  z-index: 2600;
  background: linear-gradient(140deg, rgba(15, 23, 42, 0.85), rgba(30, 64, 175, 0.65));
  padding: 0;
  margin: 0;
  border-bottom: none;
  box-shadow: none;
  border-radius: 16px;
  border: 1px solid rgba(59, 130, 246, 0.15);
}

.chart-header.fullscreen-active {
  background: rgba(0,0,0,0.9);
  border: 1px solid rgba(59,130,246,0.1);
}

.header-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0;
  padding: 8px 16px;
  background: transparent;
  border-radius: 12px;
  border: none;
  box-shadow: none !important;
}

.header-row.fullscreen-active {
  background: transparent;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 18px;
  flex: 1;
  min-width: 0;
}

.chart-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 1.2rem;
  font-weight: 600;
  color: #fff;
  padding-left: 16px;
  white-space: nowrap;
}

.chart-title i {
  color: rgba(100, 200, 255, 0.8);
  font-size: 1.3rem;
}

.header-actions {
  display: flex;
  gap: 10px;
  align-items: center;
}

.action-icon-btn {
  width: 40px;
  height: 40px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(100, 200, 255, 0.1);
  border: 1px solid rgba(100, 200, 255, 0.2);
  border-radius: 10px;
  color: rgba(226, 232, 240, 0.8);
  cursor: pointer;
  transition: all 0.3s ease;
  font-size: 1rem;
}

.action-icon-btn:hover {
  background: rgba(100, 200, 255, 0.2);
  border-color: rgba(100, 200, 255, 0.4);
  color: #64c8ff;
  transform: translateY(-2px);
}

.action-icon-btn:active {
  transform: translateY(0);
}

.chart-header {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 0;
  background: transparent;
  padding: 0;
  margin: 0 0 16px 0;
  border-bottom: none;
  box-shadow: none;
}

/* Frequency Controls */
.frequency-controls {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  justify-content: flex-start;
  padding: 0 4px;
  background: transparent;
  border-radius: 12px;
  border: none;
  margin-bottom: 4px;
}

.frequency-controls.fullscreen-active {
  background: rgba(0,0,0,0.85);
  border: 1px solid rgba(59,130,246,0.08);
}

/* 週期列：與 K 線模式鈕分開為獨立 flex 群組；整組 z-index 高於 .kline-mode-toggle，月K 才不會被原始K線蓋住 */
.mobile-period-row {
  position: relative;
  z-index: 5;
  display: flex;
  flex-direction: row;
  align-items: center;
  gap: 8px;
  flex: 0 1 auto;
  min-width: 0;
  max-width: 100%;
}

/* 桌機／一般全螢幕：K 線／搜尋／圖示仍直接參與 .frequency-controls 排版 */
.mobile-toolbar-secondary {
  display: contents;
}

/* 原始K／神奇K／階梯線 與 股票查詢 緊鄰；預設打平以相容手機全螢幕 grid／:not(:fullscreen) 排版 */
.kline-search-cluster {
  display: contents;
}

@media (min-width: 769px) {
  .stock-chart .kline-search-cluster {
    display: flex;
    align-items: center;
    gap: 6px;
    flex: 0 1 auto;
    min-width: 0;
    margin-left: auto;
  }
}

.period-chips-scroll {
  display: flex;
  flex-direction: row;
  align-items: center;
  flex-wrap: nowrap;
  gap: 8px;
  min-width: 0;
  flex: 0 1 auto;
}

/* Stepper: 顯示根數 [-] 25 [+] */
.visible-count-stepper {
  display: flex;
  align-items: center;
  gap: 0;
  border: 1px solid rgba(100, 200, 255, 0.25);
  border-radius: 22px;
  background: rgba(15, 23, 42, 0.5);
  overflow: hidden;
  flex-shrink: 0;
}

.stepper-btn {
  width: 32px;
  height: 36px;
  border: none;
  background: transparent;
  color: rgba(226, 232, 240, 0.7);
  font-size: 1rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background 0.15s ease, color 0.15s ease;
  flex-shrink: 0;
}

.stepper-btn:hover {
  background: rgba(100, 200, 255, 0.15);
  color: #fff;
}

.stepper-btn:active {
  background: rgba(100, 200, 255, 0.25);
}

.stepper-value {
  width: 48px;
  min-width: 36px;
  text-align: center;
  font-size: 0.85rem;
  font-weight: 600;
  color: rgba(226, 232, 240, 0.9);
  font-variant-numeric: tabular-nums;
  border: none;
  border-left: 1px solid rgba(100, 200, 255, 0.15);
  border-right: 1px solid rgba(100, 200, 255, 0.15);
  background: transparent;
  padding: 0 4px;
  height: 36px;
  line-height: 36px;
  outline: none;
  -moz-appearance: textfield;
}

.stepper-value::-webkit-outer-spin-button,
.stepper-value::-webkit-inner-spin-button {
  -webkit-appearance: none;
  margin: 0;
}

.stepper-value:focus {
  background: rgba(59, 130, 246, 0.1);
  color: #fff;
}

/* Inline mic toggle button (replaces the old tab row) */
.fullscreen-search-mic-toggle {
  flex-shrink: 0;
  width: 36px;
  height: 36px;
  border-radius: 10px;
  border: 1px solid rgba(100, 200, 255, 0.25);
  background: rgba(15, 23, 42, 0.55);
  color: rgba(226, 232, 240, 0.65);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.85rem;
  transition: all 0.18s ease;
}

.fullscreen-search-mic-toggle:hover {
  background: rgba(100, 200, 255, 0.12);
  border-color: rgba(100, 200, 255, 0.4);
  color: rgba(226, 232, 240, 0.9);
}

.fullscreen-search-mic-toggle.is-active {
  border-color: rgba(248, 113, 113, 0.7);
  background: rgba(248, 113, 113, 0.12);
  color: #fca5a5;
}

.fullscreen-search-mic-toggle.is-active.is-listening {
  box-shadow: 0 0 8px rgba(248, 113, 113, 0.4);
  animation: mic-pulse 1.2s ease-in-out infinite;
}

@keyframes mic-pulse {
  0%, 100% { box-shadow: 0 0 6px rgba(248, 113, 113, 0.35); }
  50% { box-shadow: 0 0 14px rgba(248, 113, 113, 0.65); }
}

/* Inline voice button (shown when in voice mode, replaces large standalone mic) */
.fullscreen-search-mic--inline {
  flex: 1 1 auto;
  height: 36px;
  border-radius: 10px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  font-size: 0.82rem;
  color: rgba(226, 232, 240, 0.75);
  border: 1px dashed rgba(100, 200, 255, 0.3);
  background: rgba(15, 23, 42, 0.4);
}

.fullscreen-search-mic--inline.is-listening {
  border-color: rgba(248, 113, 113, 0.7);
  color: #fca5a5;
  background: rgba(248, 113, 113, 0.08);
  animation: mic-pulse 1.2s ease-in-out infinite;
}

.mic-status-label {
  font-size: 0.8rem;
}

.period-chip {
  position: relative;
  z-index: 6;
  flex-shrink: 0;
  padding: 10px 20px;
  border: 1px solid rgba(100, 200, 255, 0.25);
  border-radius: 24px;
  background: rgba(15, 23, 42, 0.5);
  color: rgba(226, 232, 240, 0.7);
  font-size: 0.9rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  text-align: center;
  white-space: nowrap;
  min-width: 70px;
}

.period-chip:hover {
  background: rgba(100, 200, 255, 0.15);
  border-color: rgba(100, 200, 255, 0.4);
  color: #fff;
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(100, 200, 255, 0.2);
}

.period-chip.active {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.9), rgba(147, 51, 234, 0.9));
  border-color: rgba(59, 130, 246, 0.6);
  color: #fff;
  font-weight: 600;
  box-shadow: 0 4px 16px rgba(59, 130, 246, 0.3);
  transform: translateY(-2px);
}

/* K線模式下拉 + 選用工具鈕（查價線） */
.kline-mode-toggle {
  position: relative;
  z-index: 0;
  display: flex;
  flex-wrap: nowrap;
  gap: 8px;
  align-items: center;
  flex-shrink: 0;
  margin-left: 0;
  margin-right: 0;
  padding: 4px;
  background: rgba(15, 23, 42, 0.5);
  border-radius: 24px;
  border: 1px solid rgba(100, 200, 255, 0.2);
}

.kline-mode-select-shell {
  display: flex;
  flex: 0 0 auto;
  width: auto;
  max-width: none;
  align-items: stretch;
}

.kline-mode-sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

.kline-mode-icon-dropdown {
  position: relative;
  width: auto;
  min-width: 0;
}

.kline-mode-icon-dropdown__trigger {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  width: auto;
  min-width: 0;
  max-width: none;
  padding: 6px 9px;
  font-size: 0.85rem;
  font-weight: 600;
  color: rgba(226, 232, 240, 0.92);
  background-color: rgba(15, 23, 42, 0.85);
  border: 1px solid rgba(100, 200, 255, 0.3);
  border-radius: 12px;
  cursor: pointer;
  box-sizing: border-box;
}

.kline-mode-icon-dropdown.is-open > .kline-mode-icon-dropdown__trigger {
  border-color: rgba(59, 130, 246, 0.42);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.16);
}

.kline-mode-icon-dropdown__trigger-icon {
  font-size: 1.05rem;
  line-height: 1;
}

.kline-mode-icon-dropdown__caret {
  font-size: 0.5rem !important;
  opacity: 0.85;
  transition: transform 0.2s ease;
}

.kline-mode-icon-dropdown.is-open .kline-mode-icon-dropdown__caret {
  transform: rotate(180deg);
}

.kline-mode-icon-dropdown__menu {
  position: absolute;
  top: calc(100% + 6px);
  left: 0;
  z-index: 48;
  display: flex;
  flex-direction: row;
  flex-wrap: nowrap;
  gap: 8px;
  padding: 8px;
  margin: 0;
  list-style: none;
  background: rgba(12, 18, 34, 0.96);
  border: 1px solid rgba(96, 165, 250, 0.22);
  border-radius: 14px;
  box-shadow: 0 12px 28px rgba(0, 0, 0, 0.36);
}

.kline-mode-icon-dropdown__option {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 44px;
  padding: 0;
  border-radius: 12px;
  border: 1px solid rgba(96, 165, 250, 0.2);
  background: rgba(15, 23, 42, 0.78);
  color: rgba(226, 232, 240, 0.85);
  cursor: pointer;
  box-sizing: border-box;
}

.kline-mode-icon-dropdown__option i {
  font-size: 1rem;
  pointer-events: none;
}

.kline-mode-icon-dropdown__option:hover {
  border-color: rgba(96, 165, 250, 0.35);
  color: rgba(248, 250, 252, 0.95);
}

.kline-mode-icon-dropdown__option.is-active {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.88), rgba(147, 51, 234, 0.82));
  border-color: rgba(59, 130, 246, 0.45);
  color: #fff;
}

.kline-mode-icon-dropdown__trigger:focus {
  outline: none;
  border-color: rgba(59, 130, 246, 0.55);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.22);
}

/* Fullscreen-specific overrides for control bar */
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header.fullscreen-active,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .header-row,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls.fullscreen-active,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
}

.mode-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  border: 1px solid transparent;
  border-radius: 20px;
  background: transparent;
  color: rgba(226, 232, 240, 0.6);
  font-size: 0.85rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  white-space: nowrap;
}

.mode-btn i {
  font-size: 0.9rem;
}

.mode-btn:hover {
  color: rgba(226, 232, 240, 0.9);
  background: rgba(100, 200, 255, 0.1);
  border-color: rgba(100, 200, 255, 0.2);
}

.mode-btn.active {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.8), rgba(147, 51, 234, 0.8));
  border-color: rgba(59, 130, 246, 0.5);
  color: #fff;
  font-weight: 600;
  box-shadow: 0 2px 8px rgba(59, 130, 246, 0.3);
}

/* Fullscreen actions container */
.fullscreen-actions {
  display: none;
  flex-direction: row;
  gap: 10px;
  align-items: center;
  margin-left: 12px;
}

.fullscreen-actions-buttons {
  display: flex;
  gap: 10px;
}

/* 全螢幕三合一：單一按鈕展開選單（首項：全螢幕四分割） */
.fs-tools-dropdown {
  position: relative;
  display: inline-flex;
  align-items: center;
  z-index: 8;
  overflow: visible;
}

.fs-tools-dropdown__trigger {
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
}

.fs-tools-dropdown__trigger.is-open {
  background: rgba(100, 200, 255, 0.22) !important;
  border-color: rgba(100, 200, 255, 0.5) !important;
  color: #93c5fd !important;
}

.fs-tools-menu {
  position: absolute;
  right: 0;
  top: calc(100% + 6px);
  min-width: 220px;
  margin: 0;
  padding: 6px;
  list-style: none;
  border-radius: 12px;
  background: rgba(15, 23, 42, 0.98);
  border: 1px solid rgba(100, 200, 255, 0.28);
  box-shadow: 0 16px 40px rgba(0, 0, 0, 0.55);
  z-index: 4005;
}

/* 掛在 body 時改為明確直向欄、固定定位由 :style 提供 */
.fs-tools-menu.fs-tools-menu--teleport {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 2px;
  box-sizing: border-box;
}

.fs-tools-menu__item {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border: none;
  border-radius: 8px;
  background: transparent;
  color: #e2e8f0;
  font-size: 0.9rem;
  font-weight: 600;
  cursor: pointer;
  text-align: left;
  transition: background 0.15s ease;
}

.fs-tools-menu__item i {
  width: 1.1rem;
  text-align: center;
  color: rgba(147, 197, 253, 0.95);
}

.fs-tools-menu__item:hover,
.fs-tools-menu__item:focus-visible {
  background: rgba(59, 130, 246, 0.22);
  outline: none;
}

.fs-tools-menu__item.is-active {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.26), rgba(147, 51, 234, 0.22));
  color: #fff;
}

.fs-tools-menu__item.is-active i {
  color: #bfdbfe;
}

.fs-tools-menu__item--danger {
  color: #fecaca;
}

.fs-tools-menu__item--danger i {
  color: #fca5a5;
}

.fs-tools-menu__divider {
  height: 1px;
  margin: 4px 2px;
  list-style: none;
  background: rgba(100, 200, 255, 0.16);
}

.fullscreen-carousel-controls {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 6px 12px;
  border-radius: 16px;
  background: rgba(5, 12, 30, 0.55);
  border: 1px solid rgba(37, 99, 235, 0.28);
}

.fullscreen-carousel-controls--inline {
  margin-left: 12px;
}

.fullscreen-carousel-controls .carousel-indicator {
  font-size: 0.9rem;
  font-weight: 600;
  color: rgba(226, 232, 240, 0.95);
}

.fullscreen-carousel-controls .carousel-buttons {
  display: flex;
  align-items: center;
  gap: 8px;
}

.fullscreen-carousel-controls .carousel-btn {
  width: 42px;
  height: 42px;
  border-radius: 14px;
  background: rgba(8, 20, 44, 0.72);
  border: 1px solid rgba(59, 130, 246, 0.35);
  color: rgba(226, 232, 240, 0.86);
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.18s ease;
}

.fullscreen-carousel-controls .carousel-btn:hover:not(:disabled) {
  background: rgba(30, 64, 175, 0.65);
  border-color: rgba(147, 197, 253, 0.55);
  color: #f8fbff;
  transform: translateY(-1px);
}

.fullscreen-carousel-controls .carousel-btn:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}

.fullscreen-carousel-controls .carousel-btn--toggle {
  position: relative;
  width: 42px;
  height: 42px;
  border-radius: 14px;
  font-size: 16px;
  background: rgba(4, 108, 151, 0.85);
  border: 1px solid rgba(59, 209, 255, 0.55);
  box-shadow: inset 0 0 0 1px rgba(7, 16, 34, 0.65);
}

.fullscreen-carousel-controls .carousel-btn--toggle::before {
  content: '';
  position: absolute;
  inset: 4px;
  border-radius: 11px;
  background: rgba(7, 18, 40, 0.76);
  border: 1px solid rgba(147, 197, 253, 0.2);
  transition: all 0.25s ease;
}

.fullscreen-carousel-controls .carousel-btn--toggle::after {
  content: '';
  position: absolute;
  inset: 8px;
  border-radius: 9px;
  background: rgba(15, 118, 190, 0.4);
  opacity: 0;
  transition: opacity 0.2s ease;
}

.fullscreen-carousel-controls .carousel-btn--active {
  background: rgba(4, 131, 176, 0.95);
  border-color: rgba(59, 209, 255, 0.7);
  color: #83f0ff;
}

.fullscreen-carousel-controls .carousel-btn--active::before {
  background: rgba(6, 44, 74, 0.82);
  border-color: rgba(56, 189, 248, 0.45);
}

.fullscreen-carousel-controls .carousel-btn--active::after {
  opacity: 1;
}

/* Positioning context for legend overlay */
.chart-wrapper {
  position: relative;
  margin-top: -18px;
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  min-height: 0;
  width: 100%;
}

.chart-splitter {
  position: absolute;
  left: 5%;
  right: var(--chart-grid-right-pct, 8%);
  height: 14px;
  margin-top: -7px;
  z-index: 1190;
  cursor: row-resize;
  pointer-events: auto;
  touch-action: none;
}

.chart-splitter::before {
  content: '';
  position: absolute;
  left: 50%;
  top: 50%;
  width: 64px;
  height: 2px;
  transform: translate(-50%, -50%);
  display: none;
}

.chart-splitter.is-dragging::before {
  display: none;
}

/* 查價線：快捷鈕疊在主圖第一格 grid（K 線區）右下；bottom 由 renderChart 寫入 --qg-crosshair-nav-bottom */
.crosshair-lookup-nav {
  position: absolute;
  right: calc(8% + 6px);
  bottom: calc(var(--qg-crosshair-nav-bottom, 48px) + env(safe-area-inset-bottom, 0px));
  z-index: 75;
  display: flex;
  flex-direction: row;
  align-items: center;
  gap: 4px;
  pointer-events: auto;
  padding: 4px;
  border-radius: 14px;
  background: linear-gradient(135deg, rgba(9, 16, 32, 0.36), rgba(30, 41, 59, 0.18));
  border: 1px solid rgba(148, 163, 184, 0.12);
  box-shadow: 0 12px 28px rgba(2, 6, 23, 0.24);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  cursor: grab;
  touch-action: none;
}

.crosshair-lookup-nav.is-collapsed {
  gap: 0;
}

.crosshair-lookup-nav.is-dragging {
  cursor: grabbing;
  user-select: none;
}

.crosshair-lookup-nav__btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  position: relative;
  overflow: hidden;
  width: 30px;
  height: 30px;
  padding: 0;
  border-radius: 10px;
  border: 1px solid rgba(147, 197, 253, 0.22);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.16), rgba(255, 255, 255, 0.03)),
    linear-gradient(135deg, rgba(30, 41, 59, 0.92), rgba(15, 23, 42, 0.86));
  color: rgba(239, 246, 255, 0.96);
  font-size: 0.64rem;
  cursor: pointer;
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.2),
    0 6px 16px rgba(15, 23, 42, 0.28);
  transition: transform 0.18s ease, background 0.18s ease, border-color 0.18s ease, color 0.18s ease, box-shadow 0.18s ease;
}

.crosshair-lookup-nav__btn::after {
  content: '';
  position: absolute;
  inset: 1px;
  border-radius: inherit;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.1), rgba(255, 255, 255, 0));
  opacity: 0.9;
  pointer-events: none;
}

.crosshair-lookup-nav__btn:hover:not(:disabled) {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.2), rgba(255, 255, 255, 0.05)),
    linear-gradient(135deg, rgba(59, 130, 246, 0.42), rgba(99, 102, 241, 0.34));
  border-color: rgba(191, 219, 254, 0.55);
  color: #fff;
  transform: translateY(-1.5px) scale(1.03);
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.24),
    0 10px 24px rgba(37, 99, 235, 0.22);
}

.crosshair-lookup-nav__btn:active:not(:disabled) {
  transform: translateY(0) scale(0.98);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.14), rgba(255, 255, 255, 0.04)),
    linear-gradient(135deg, rgba(37, 99, 235, 0.48), rgba(79, 70, 229, 0.4));
}

.crosshair-lookup-nav__btn:disabled {
  opacity: 0.34;
  cursor: not-allowed;
  box-shadow: none;
}

.crosshair-lookup-nav__btn--latest {
  width: 34px;
}

.crosshair-lookup-nav__btn--close {
  color: rgba(255, 228, 230, 0.98);
  border-color: rgba(251, 113, 133, 0.28);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.14), rgba(255, 255, 255, 0.03)),
    linear-gradient(135deg, rgba(159, 18, 57, 0.5), rgba(244, 63, 94, 0.28));
}

.crosshair-lookup-nav__btn--close:hover:not(:disabled) {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.18), rgba(255, 255, 255, 0.04)),
    linear-gradient(135deg, rgba(244, 63, 94, 0.52), rgba(251, 113, 133, 0.34));
  border-color: rgba(253, 164, 175, 0.58);
  color: #fff1f2;
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.22),
    0 10px 24px rgba(244, 63, 94, 0.18);
}

/* 手機／窄螢幕（≤768）：查價鈕適中尺寸，不佔過多圖表區 */
@media (max-width: 768px) {
  .stock-chart .crosshair-lookup-nav {
    gap: 4px;
    right: calc(6px + env(safe-area-inset-right, 0px));
    padding: 4px;
    margin: -4px -4px -4px 0;
    border-radius: 12px;
    background: linear-gradient(135deg, rgba(9, 16, 32, 0.4), rgba(30, 41, 59, 0.22));
    backdrop-filter: blur(9px);
  }

  .stock-chart .crosshair-lookup-nav__btn {
    box-sizing: border-box;
    width: 29px;
    height: 29px;
    min-width: 29px;
    min-height: 29px;
    padding: 0;
    border-radius: 9px;
    font-size: 0.62rem;
    box-shadow:
      inset 0 1px 0 rgba(255, 255, 255, 0.18),
      0 4px 10px rgba(8, 15, 35, 0.24);
  }

  .stock-chart .crosshair-lookup-nav__btn--latest {
    width: 32px;
    min-width: 32px;
  }

  .stock-chart .crosshair-lookup-nav__btn i {
    font-size: 10px;
    line-height: 1;
  }
}

/* Overlay row inside chart for title + carousel next to MA legend */
.chart-container {
  position: relative;
  flex: 1 1 auto;
  min-height: var(--chart-container-min-height);
  height: var(--chart-container-height);
  width: 100%;
}

.chart-container-echarts {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}

.chart-drawing-context-menu {
  position: absolute;
  min-width: 148px;
  padding: 6px;
  border-radius: 12px;
  background: rgba(15, 23, 42, 0.96);
  border: 1px solid rgba(96, 165, 250, 0.32);
  box-shadow: 0 18px 40px rgba(2, 6, 23, 0.45);
  z-index: 1400;
}

.chart-drawing-context-menu__item {
  display: flex;
  width: 100%;
  align-items: center;
  justify-content: flex-start;
  padding: 10px 12px;
  border: none;
  border-radius: 10px;
  background: transparent;
  color: rgba(226, 232, 240, 0.92);
  font-size: 0.9rem;
  cursor: pointer;
}

.chart-drawing-context-menu__item:hover {
  background: rgba(30, 41, 59, 0.92);
}

.chart-drawing-context-menu__item--danger {
  color: #fca5a5;
}

.chart-drawing-context-menu__item--danger:hover {
  background: rgba(127, 29, 29, 0.32);
}

.chart-drawing-hint {
  position: absolute;
  left: 50%;
  bottom: 18px;
  transform: translateX(-50%);
  padding: 8px 14px;
  border-radius: 999px;
  background: rgba(15, 23, 42, 0.88);
  border: 1px solid rgba(96, 165, 250, 0.24);
  color: rgba(226, 232, 240, 0.92);
  font-size: 0.84rem;
  box-shadow: 0 14px 30px rgba(2, 6, 23, 0.35);
  z-index: 1300;
  pointer-events: none;
}

.chart-drawing-style-toolbar {
  position: absolute;
  left: 50%;
  top: 16px;
  transform: translateX(-50%);
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 10px 12px;
  width: min(calc(100% - 24px), 520px);
  max-width: calc(100% - 24px);
  border-radius: 18px;
  background: rgba(15, 23, 42, 0.94);
  border: 1px solid rgba(96, 165, 250, 0.24);
  box-shadow: 0 16px 34px rgba(2, 6, 23, 0.36);
  z-index: 1390;
  box-sizing: border-box;
}

.chart-drawing-style-toolbar__group {
  display: inline-flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-width: 0;
}

.chart-drawing-color-swatch {
  width: 18px;
  height: 18px;
  border-radius: 999px;
  border: 2px solid transparent;
  cursor: pointer;
  box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.16);
}

.chart-drawing-color-swatch.is-active {
  border-color: #e2e8f0;
  transform: scale(1.08);
}

.chart-drawing-style-chip {
  padding: 6px 10px;
  border: 1px solid rgba(148, 163, 184, 0.2);
  border-radius: 999px;
  background: rgba(30, 41, 59, 0.76);
  color: rgba(226, 232, 240, 0.9);
  font-size: 0.78rem;
  cursor: pointer;
}

.chart-drawing-style-chip.is-active {
  background: rgba(59, 130, 246, 0.22);
  border-color: rgba(96, 165, 250, 0.46);
  color: #eff6ff;
}

.chart-drawing-style-sheet {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 10px 16px calc(16px + env(safe-area-inset-bottom, 0px));
  background: rgba(15, 23, 42, 0.97);
  border-top-left-radius: 22px;
  border-top-right-radius: 22px;
  border-top: 1px solid rgba(96, 165, 250, 0.22);
  box-shadow: 0 -12px 34px rgba(2, 6, 23, 0.42);
  z-index: 1395;
}

.chart-drawing-style-sheet__handle {
  width: 42px;
  height: 4px;
  margin: 0 auto;
  border-radius: 999px;
  background: rgba(148, 163, 184, 0.5);
}

.chart-drawing-style-sheet__title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.chart-drawing-style-sheet__title {
  font-size: 0.96rem;
  font-weight: 700;
  color: #f8fafc;
}

.chart-drawing-style-sheet__close {
  padding: 6px 10px;
  border: 1px solid rgba(96, 165, 250, 0.28);
  border-radius: 999px;
  background: rgba(30, 41, 59, 0.84);
  color: rgba(226, 232, 240, 0.94);
  font-size: 0.76rem;
  cursor: pointer;
}

.chart-drawing-style-sheet__move {
  width: 100%;
  padding: 12px 14px;
  border: 1px solid rgba(96, 165, 250, 0.3);
  border-radius: 14px;
  background: rgba(30, 64, 175, 0.2);
  color: #dbeafe;
  font-size: 0.84rem;
  font-weight: 700;
  cursor: pointer;
}

.chart-drawing-style-sheet__move.is-active {
  background: rgba(37, 99, 235, 0.34);
  border-color: rgba(147, 197, 253, 0.6);
  color: #eff6ff;
  box-shadow: inset 0 0 0 1px rgba(191, 219, 254, 0.18);
}

.chart-drawing-style-sheet__section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.chart-drawing-style-sheet__label {
  font-size: 0.76rem;
  font-weight: 600;
  color: rgba(191, 219, 254, 0.88);
}

.chart-drawing-style-toolbar__group--sheet {
  justify-content: flex-start;
}

.chart-drawing-style-sheet__delete {
  width: 100%;
  padding: 12px 14px;
  border: 1px solid rgba(239, 68, 68, 0.24);
  border-radius: 14px;
  background: rgba(127, 29, 29, 0.22);
  color: #fca5a5;
  font-size: 0.86rem;
  font-weight: 700;
  cursor: pointer;
}

@media (max-width: 768px) {
  .chart-drawing-style-toolbar {
    display: none;
  }

  .chart-drawing-style-toolbar__group {
    width: 100%;
  }

  .chart-drawing-color-swatch {
    width: 16px;
    height: 16px;
  }

  .chart-drawing-style-chip {
    padding: 5px 9px;
    font-size: 0.74rem;
  }
}

.chart-watermark {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: flex-start;
  padding-left: 8%;
  pointer-events: none;
  z-index: 1;
}

.chart-watermark__inner {
  transform: none;
}

.chart-watermark__text {
  font-size: clamp(2rem, 5vw, 3.5rem);
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: rgba(148, 163, 184, 0.05);
  text-shadow:
    0 0 24px rgba(15, 23, 42, 0.9),
    0 0 40px rgba(15, 23, 42, 0.95);
  white-space: nowrap;
}

@media (max-width: 768px) {
  .chart-watermark__text {
    font-size: clamp(1.6rem, 9vw, 2.4rem);
    letter-spacing: 0.12em;
  }
}


.legend-overlay-row {
  position: absolute;
  top: -4px;
  display: flex;
  align-items: center;
  gap: 10px;
  z-index: 1000;
  pointer-events: none;
}

.legend-overlay-row--name {
  left: 55%;
  transform: translateX(-50%);
  top: -40px;
}

.legend-overlay-row.legend-overlay-row--mobile-pinned-inline {
  left: 0;
  right: 0;
  transform: none;
  width: 100%;
  max-width: 100%;
  top: -40px;
  padding: 4px 10px 0;
  box-sizing: border-box;
  display: flex;
  justify-content: flex-end;
  align-items: center;
  gap: 8px;
  min-height: 28px;
}

.legend-overlay-row.fullscreen-active.legend-overlay-row--mobile-pinned-inline {
  top: -8px;
  padding: 6px 12px 2px 8px;
  justify-content: flex-end;
  min-height: 32px;
}

.legend-overlay-row--mobile-pinned-inline .legend-title-text {
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: 5;
  max-width: min(82vw, calc(100% - 72px));
  min-width: 0;
  justify-content: center;
  font-size: 0.96rem;
  padding: 4px 6px;
}

.legend-overlay-row--mobile-pinned-inline .legend-title-text > span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.legend-overlay-row--mobile-pinned-inline .legend-toggle-btn {
  position: relative;
  z-index: 4;
  flex-shrink: 0;
  order: -1;
  margin-left: 0;
  margin-right: auto;
  align-self: center;
  right: auto;
  top: auto;
  transform: none;
}

.legend-title-text--fullscreen.legend-title-text--fullscreen-inline {
  position: relative;
  left: auto;
  top: auto;
  transform: none;
  font-size: 0.98rem;
}

.legend-overlay-row--mobile-pinned-inline .legend-title-text.legend-title-text--fullscreen.legend-title-text--fullscreen-inline {
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
}

.mobile-pinned-lookup-aside {
  position: relative;
  box-sizing: border-box;
  width: auto;
  max-width: min(94vw, 340px);
  height: auto;
  min-height: 26px;
  flex-shrink: 0;
  padding: 2px 6px;
  border: none;
  border-radius: 0;
  background: transparent;
  backdrop-filter: none;
  box-shadow: none;
  pointer-events: auto;
  display: flex;
  align-items: center;
  overflow: visible;
  touch-action: manipulation;
}

.mobile-pinned-lookup-aside.mobile-pinned-lookup-aside--chart-dock {
  position: absolute;
  left: var(--qg-pinned-aside-left, 50%);
  transform: var(--qg-pinned-aside-dock-transform, translateX(-50%));
  top: var(--qg-pinned-aside-top, 52px);
  z-index: 28;
  margin: 0;
  /* 左 padding 歸零，讓「收盤價」與均線／布林文字左緣對齊 */
  padding-left: 0;
  padding-right: 6px;
}

.mobile-pinned-lookup-aside__body {
  flex: 1;
  width: 100%;
  min-width: 0;
  display: flex;
  flex-direction: row;
  flex-wrap: nowrap;
  align-items: center;
  justify-content: center;
  gap: 8px;
  overflow: hidden;
}

.mobile-pinned-lookup-aside.mobile-pinned-lookup-aside--chart-dock .mobile-pinned-lookup-aside__body {
  justify-content: flex-start;
}

.mobile-pinned-lookup-aside__actions {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-left: auto;
  padding-left: 10px;
  flex-shrink: 0;
}

.mobile-pinned-lookup-aside__actionBtn {
  width: 28px;
  height: 28px;
  border: 1px solid rgba(96, 165, 250, 0.34);
  border-radius: 9px;
  background: rgba(15, 23, 42, 0.82);
  color: rgba(226, 232, 240, 0.92);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06);
  transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease, transform 0.15s ease;
}

.mobile-pinned-lookup-aside__actionBtn:hover {
  background: rgba(30, 64, 175, 0.42);
  border-color: rgba(147, 197, 253, 0.56);
  color: #ffffff;
  transform: translateY(-1px);
}

.mobile-pinned-lookup-aside__actionBtn:active {
  background: rgba(37, 99, 235, 0.5);
  transform: translateY(0);
}

.mobile-pinned-lookup-aside__actionBtn i {
  font-size: 11px;
  line-height: 1;
}

.mobile-pinned-lookup-aside__actionBtn--close {
  color: rgba(248, 113, 113, 0.95);
  border-color: rgba(248, 113, 113, 0.32);
}

.mobile-pinned-lookup-aside__actionBtn--close:hover {
  background: rgba(127, 29, 29, 0.42);
  border-color: rgba(252, 165, 165, 0.52);
  color: #fecaca;
}

.mobile-pinned-lookup-aside__cell {
  font-variant-numeric: tabular-nums;
  letter-spacing: 0.02em;
  line-height: 1.2;
  flex: 0 0 auto;
  overflow: visible;
  white-space: nowrap;
}

.mobile-pinned-lookup-aside__cell--close-only {
  display: inline-flex;
  align-items: baseline;
  gap: 8px;
  font-weight: 700;
  font-size: 13px;
  color: rgba(248, 250, 252, 0.96);
  text-shadow:
    0 0 10px rgba(15, 23, 42, 0.95),
    0 1px 2px rgba(0, 0, 0, 0.85);
}

.mobile-pinned-lookup-aside__cell--close-only .mobile-pinned-lookup-aside__price {
  font-size: 14px;
  font-weight: 800;
}

.mobile-pinned-lookup-aside__cell--close-only .mobile-pinned-lookup-aside__pct,
.mobile-pinned-lookup-aside__cell--close-only .mobile-pinned-lookup-aside__delta {
  margin-left: 0;
  font-size: 12px;
}

.mobile-pinned-lookup-aside__sep {
  margin: 0 0.08em;
  color: rgba(148, 163, 184, 0.35);
  font-weight: 400;
}

.mobile-pinned-lookup-aside__label {
  margin-right: 0.12em;
  color: rgba(226, 232, 240, 0.82);
  font-weight: 600;
  font-size: 11px;
  flex-shrink: 0;
}

.mobile-pinned-lookup-aside__cell--close-only .mobile-pinned-lookup-aside__label {
  margin-right: 0.06em;
}

.mobile-pinned-lookup-aside__price {
  color: rgba(248, 250, 252, 0.98);
}

.mobile-pinned-lookup-aside__pct,
.mobile-pinned-lookup-aside__delta {
  margin-left: 0.18em;
  font-weight: 700;
}

.mobile-pinned-lookup-aside__pct.is-up,
.mobile-pinned-lookup-aside__delta.is-up {
  color: #f87171;
}

.mobile-pinned-lookup-aside__pct.is-down,
.mobile-pinned-lookup-aside__delta.is-down {
  color: #4ade80;
}

.legend-title-text {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  font-weight: 600;
  font-size: 1.05rem;
  color: rgba(226, 232, 240, 0.9);
  background: transparent;
  border: none;
  border-radius: 0;
  letter-spacing: 0.04em;
  pointer-events: none;
}

.legend-title-text i {
  display: none;
}

.legend-latest-date {
  font-size: 0.82em;
  font-weight: 500;
  color: rgba(226, 232, 240, 0.78);
  letter-spacing: 0.02em;
  white-space: nowrap;
  margin-right: 2px;
}

.legend-title-text--fullscreen {
  background: transparent;
  border-color: transparent;
  font-size: 1.25rem;
  position: absolute;
  left: 50%;
  top: -4px;
  transform: translateX(-50%);
}

.legend-overlay-row.fullscreen-active {
  left: 0;
  right: 0;
  top: 0;
  padding: 0 12px;
  justify-content: center;
  box-sizing: border-box;
  width: 100%;
}

.legend-overlay-row.fullscreen-active:not(.legend-overlay-row--mobile-pinned-inline) {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 36px;
}

.legend-overlay-row.fullscreen-active:not(.legend-overlay-row--mobile-pinned-inline) .legend-title-text--fullscreen {
  position: static;
  left: auto;
  top: auto;
  transform: none;
  gap: 6px;
}

.legend-carousel--fullscreen {
  margin-left: auto;
  pointer-events: auto;
}

.legend-title-block {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  color: #e2e8f0;
  font-weight: 700;
  font-size: 1.35rem;
  letter-spacing: 0.03em;
  text-shadow: 0 1px 2px rgba(0,0,0,0.4);
  background: rgba(0,0,0,0.25);
  border-radius: 10px;
  pointer-events: auto;
}

.legend-title-block i { color: rgba(100,200,255,0.9); font-size: 1.5rem; }

.legend-carousel {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  background: rgba(7,14,28,0.6);
  border: 1px solid rgba(59,130,246,0.35);
  border-radius: 12px;
  pointer-events: auto;
}

.legend-carousel .carousel-indicator { color: rgba(226,232,240,0.95); font-weight: 600; }
.legend-carousel button {
  width: 32px;
  height: 32px;
  border-radius: 10px;
  border: 1px solid rgba(59,130,246,0.35);
  background: rgba(8,20,44,0.75);
  color: rgba(226,232,240,0.92);
  display: flex; align-items: center; justify-content: center;
  cursor: pointer; transition: all .18s ease;
}
.legend-carousel button:hover { transform: translateY(-1px); background: rgba(30,64,175,0.72); color: #fff; }
.legend-carousel .play-toggle { background: linear-gradient(135deg, rgba(14,165,233,0.9), rgba(59,130,246,0.9)); border-color: rgba(147,197,253,0.45); }

/* Remove outer glow for controls */
.fullscreen-carousel-controls,
.fullscreen-carousel-controls .carousel-btn,
.fullscreen-btn,
.chart-control-btn,
.mode-btn,
.mode-btn.active,
.fullscreen-search-btn {
  box-shadow: none !important;
}

.fullscreen-btn:hover,
.chart-control-btn:hover,
.fullscreen-search-btn:hover {
  box-shadow: none !important;
}

.fullscreen-carousel-controls .carousel-btn i {
  position: relative;
  z-index: 1;
}

.legend-toggle-btn {
  margin-left: 8px;
  padding: 4px 8px;
  border-radius: 999px;
  border: 1px solid rgba(148, 163, 184, 0.35);
  background: rgba(15, 23, 42, 0.8);
  color: rgba(226, 232, 240, 0.8);
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-size: 0.8rem;
  pointer-events: auto;
}

.legend-toggle-btn:hover {
  border-color: rgba(148, 163, 184, 0.6);
  background: rgba(30, 64, 175, 0.7);
  color: #e5f0ff;
}

.legend-toggle-btn--title-side {
  flex: 0 0 28px !important;
  width: 28px !important;
  height: 28px !important;
  min-width: 28px !important;
  min-height: 28px !important;
  max-width: 28px !important;
  max-height: 28px !important;
  margin-left: 4px;
  padding: 0 !important;
  border-radius: 50% !important;
  box-sizing: border-box;
  align-self: center;
  font-size: 0.72rem;
  letter-spacing: 0;
  line-height: 1;
}

.legend-toggle-btn--fs-inline {
  margin-left: 2px !important;
  pointer-events: auto;
}

.legend-toggle-btn--title-side i {
  display: inline-block;
  line-height: 1;
}

.legend-overlay-row--mobile-pinned-inline .legend-toggle-btn--title-side {
  order: initial;
  margin-left: 4px;
  margin-right: 0;
  align-self: center;
}

/* Fullscreen search box */
.fullscreen-search-box {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  border-radius: 999px;
  background: rgba(15, 23, 42, 0.55);
  border: 1px solid rgba(100, 200, 255, 0.2);
}

.fullscreen-search-box--inline {
  margin-left: auto;
  margin-right: 16px;
}

.fullscreen-search-mode-toggle {
  flex-basis: 100%;
  display: flex;
  gap: 4px;
  padding: 3px;
  border-radius: 999px;
  background: rgba(15, 23, 42, 0.75);
  border: 1px solid rgba(100, 200, 255, 0.18);
  box-sizing: border-box;
}

.fullscreen-search-mode-btn {
  flex: 1;
  margin: 0;
  padding: 6px 12px;
  border: none;
  border-radius: 999px;
  background: transparent;
  color: rgba(226, 232, 240, 0.65);
  font-size: 0.78rem;
  font-weight: 600;
  font-family: inherit;
  cursor: pointer;
  transition: background 0.15s ease, color 0.15s ease;
}

.fullscreen-search-mode-btn:hover {
  color: rgba(226, 232, 240, 0.95);
}

.fullscreen-search-mode-btn.active {
  background: rgba(59, 130, 246, 0.5);
  color: #fff;
}

.fullscreen-search-box .search-icon {
  font-size: 0.9rem;
  font-weight: 600;
  color: rgba(226, 232, 240, 0.95);
  letter-spacing: 1px;
}

.fullscreen-search-box .search-input {
  display: flex;
  gap: 8px;
  align-items: center;
}

.fullscreen-search-query-row {
  display: flex;
  flex-direction: row;
  align-items: stretch;
  gap: 8px;
  width: 100%;
  min-width: 0;
  flex: 1 1 auto;
}

.fullscreen-search-query-row .search-input {
  flex: 1 1 auto;
  min-width: 0;
}

.fullscreen-search-query-row .fullscreen-search-btn {
  flex: 0 0 auto;
  align-self: stretch;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.fullscreen-search-box .search-input input,
.fullscreen-search-box .search-input input.fullscreen-search-input {
  width: 120px;
  padding: 4px 8px;
  border-radius: 12px;
  border: 1px solid rgba(59, 130, 246, 0.3);
  background: rgba(15, 23, 42, 0.6);
  color: #ffffff;
  caret-color: #ffffff;
  font-size: 0.85rem;
  font-weight: 600;
  text-align: center;
}

.fullscreen-search-box .search-input input::placeholder {
  color: rgba(255, 255, 255, 0.88);
  opacity: 1;
  font-weight: 500;
}

.fullscreen-search-box .search-input input:focus {
  outline: none;
  border-color: rgba(59, 130, 246, 0.5);
  background: rgba(15, 23, 42, 0.85);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
  color: #ffffff;
}

.fullscreen-search-box .search-input input:focus::placeholder {
  color: rgba(255, 255, 255, 0.65);
}

.fullscreen-search-box .search-input.is-listening input {
  border-color: rgba(248, 113, 113, 0.7);
  background: rgba(15, 23, 42, 0.75);
  color: #fca5a5;
  caret-color: #fca5a5;
  animation: mic-pulse-border 1.2s ease-in-out infinite;
}

.fullscreen-search-box .search-input.is-listening input::placeholder {
  color: rgba(252, 165, 165, 0.85);
}

@keyframes mic-pulse-border {
  0%, 100% { box-shadow: 0 0 0 2px rgba(248, 113, 113, 0.2); }
  50% { box-shadow: 0 0 0 3px rgba(248, 113, 113, 0.45); }
}

.fullscreen-search-btn {
  padding: 8px 20px;
  border-radius: 24px;
  border: 1px solid rgba(100, 200, 255, 0.25);
  background: rgba(15, 23, 42, 0.5);
  color: rgba(226, 232, 240, 0.85);
  font-size: 0.9rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s ease;
}

.fullscreen-search-btn:hover {
  background: rgba(100, 200, 255, 0.12);
  border-color: rgba(100, 200, 255, 0.35);
  color: #fff;
  box-shadow: 0 3px 10px rgba(100, 200, 255, 0.18);
}

.fullscreen-search-btn:active {
  transform: translateY(1px);
}

.fullscreen-search-mic {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: 1px solid rgba(100, 200, 255, 0.35);
  background: rgba(15, 23, 42, 0.65);
  color: rgba(226, 232, 240, 0.9);
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  transition: border-color 0.15s ease, background 0.15s ease, box-shadow 0.15s ease;
}

.fullscreen-search-mic:hover {
  border-color: rgba(56, 189, 248, 0.55);
  background: rgba(30, 58, 138, 0.45);
}

.fullscreen-search-mic.is-listening {
  border-color: rgba(248, 113, 113, 0.85);
  color: #fecaca;
  box-shadow: 0 0 10px rgba(248, 113, 113, 0.35);
}

.fullscreen-search-speech-err {
  flex-basis: 100%;
  font-size: 0.75rem;
  color: #fecaca;
  margin: -2px 0 0;
  padding-left: 4px;
}

.fullscreen-search-speech-hint {
  flex-basis: 100%;
  font-size: 0.75rem;
  color: rgba(125, 211, 252, 0.95);
  margin: -2px 0 0;
  padding-left: 4px;
}

/* Fullscreen Button */
.fullscreen-btn {
  width: 40px;
  height: 40px;
  color: #e5e7eb;
}

.param-reset-btn {
  width: 100%;
  background: rgba(15, 23, 42, 0.55);
  border: 1px solid rgba(148, 163, 184, 0.22);
  color: rgba(226, 232, 240, 0.9);
  border-radius: 10px;
  padding: 9px 0;
  cursor: pointer;
  transition: background 0.18s ease, border-color 0.18s ease, transform 0.08s ease, box-shadow 0.18s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  font-weight: 600;
  font-size: 0.78rem;
  letter-spacing: 0.02em;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
}

.param-reset-btn:hover {
  background: rgba(15, 23, 42, 0.7);
  border-color: rgba(148, 163, 184, 0.35);
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.05),
    0 6px 18px rgba(0, 0, 0, 0.25);
}

.param-reset-btn:active {
  background: rgba(15, 23, 42, 0.78);
  border-color: rgba(148, 163, 184, 0.45);
  transform: translateY(1px);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
}

/* Chart Control Button */
.chart-control-btn {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background: rgba(100, 200, 255, 0.1);
  border: 1px solid rgba(100, 200, 255, 0.2);
  color: rgba(100, 200, 255, 0.8);
  cursor: pointer;
  transition: all 0.2s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  flex-shrink: 0;
}

.chart-control-btn:hover {
  background: rgba(100, 200, 255, 0.15);
  border-color: rgba(100, 200, 255, 0.4);
  color: #fff;
  transform: scale(1.05);
  box-shadow: 0 4px 12px rgba(100, 200, 255, 0.2);
}

/* Fullscreen mode styles */
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) {
  background: rgba(15, 23, 42, 1);
  padding: 0;
  margin: 0;
  box-sizing: border-box;
  /*
   * Use svh (small viewport) for the shell — dvh shrinks when the virtual keyboard opens,
   * which with flex + min-height:0 collapsed the chart to ~0 and caused a black area.
   */
  height: 100vh;
  min-height: 100vh;
  height: 100svh;
  min-height: 100svh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  --chart-container-height: calc(100svh - 140px);
  --chart-container-min-height: max(220px, calc(100svh - 140px));
}

@media (orientation: landscape) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header {
    padding: 12px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .header-row {
    margin-bottom: 10px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-title {
    font-size: 1.2rem !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
    flex-wrap: wrap;
    gap: 8px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
    flex-wrap: wrap;
    justify-content: center;
    gap: 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
    max-width: 100%;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
  }
}

 .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-wrapper {
   flex: 1 1 auto;
   min-height: max(200px, 32svh);
   height: auto;
   margin-top: 0 !important;
   overflow: hidden;
 }

 .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-container {
   flex: 1 1 auto;
   height: 100% !important;
   min-height: max(200px, var(--chart-container-min-height)) !important;
   width: 100% !important;
 }

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header {
  position: relative !important;
  width: 100% !important;
  height: auto !important;
  padding: 20px !important;
  margin: 0 !important;
  top: 0 !important;
  left: 0 !important;
  right: 0 !important;
  z-index: 3000 !important;
  pointer-events: auto;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .header-row {
  margin-bottom: 20px !important;
  justify-content: center !important;
  border: none !important;
  box-shadow: none !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-title {
  text-align: center !important;
  justify-content: center !important;
  padding-left: 0 !important;
  font-size: 1.5rem !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .header-left {
  flex: 0 1 auto !important;
  justify-content: center !important;
  margin: 0 auto !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .header-actions {
  display: none !important;
}

/* Strip borders/glows from inline carousel controls on fullscreen */
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
  border: none !important;
  box-shadow: none !important;
  background: rgba(5, 12, 30, 0.45) !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn {
  border: none !important;
  box-shadow: none !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn--toggle {
  border: none !important;
  box-shadow: none !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn--toggle::before,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn--toggle::after {
  border: none !important;
  box-shadow: none !important;
  background: transparent !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
  justify-content: flex-start !important;
  align-items: center !important;
  flex-wrap: nowrap !important;
  overflow-x: auto !important;
  /* 需可見，否則 .fs-tools-menu（absolute 向下展開）會被裁切，看起來像點了沒反應 */
  overflow-y: visible !important;
  -webkit-overflow-scrolling: touch;
  gap: 12px !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls::-webkit-scrollbar {
  display: none;
}

.stock-chart.is-fullscreen .frequency-controls,
.stock-chart:-webkit-full-screen .frequency-controls {
  justify-content: flex-start !important;
  align-items: center !important;
  flex-wrap: nowrap !important;
  overflow-x: auto !important;
  overflow-y: visible !important;
  -webkit-overflow-scrolling: touch;
}

.stock-chart.is-fullscreen .frequency-controls::-webkit-scrollbar,
.stock-chart:-webkit-full-screen .frequency-controls::-webkit-scrollbar {
  display: none;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-period-row,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chips-scroll,
.stock-chart.is-fullscreen .mobile-period-row,
.stock-chart.is-fullscreen .period-chips-scroll,
.stock-chart:-webkit-full-screen .mobile-period-row,
.stock-chart:-webkit-full-screen .period-chips-scroll {
  display: flex !important;
  flex-direction: row !important;
  flex-wrap: nowrap !important;
  gap: 8px;
  align-items: center;
  overflow-x: auto;
  overflow-y: hidden;
  -webkit-overflow-scrolling: touch;
  min-width: 0;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-period-row::-webkit-scrollbar,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chips-scroll::-webkit-scrollbar,
.stock-chart.is-fullscreen .mobile-period-row::-webkit-scrollbar,
.stock-chart.is-fullscreen .period-chips-scroll::-webkit-scrollbar,
.stock-chart:-webkit-full-screen .mobile-period-row::-webkit-scrollbar,
.stock-chart:-webkit-full-screen .period-chips-scroll::-webkit-scrollbar {
  display: none;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-stepper,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chip,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions,
.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls,
.stock-chart.is-fullscreen .visible-count-stepper,
.stock-chart.is-fullscreen .period-chip,
.stock-chart.is-fullscreen .kline-mode-toggle,
.stock-chart.is-fullscreen .fullscreen-actions,
.stock-chart.is-fullscreen .fullscreen-carousel-controls,
.stock-chart:-webkit-full-screen .visible-count-stepper,
.stock-chart:-webkit-full-screen .period-chip,
.stock-chart:-webkit-full-screen .kline-mode-toggle,
.stock-chart:-webkit-full-screen .fullscreen-actions,
.stock-chart:-webkit-full-screen .fullscreen-carousel-controls {
  flex-shrink: 0;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
  margin-left: 0 !important;
  margin-right: 0 !important;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
  display: flex !important;
  position: relative;
  z-index: 3100;
  pointer-events: auto;
  flex-wrap: nowrap;
  max-width: 100%;
  overflow-x: auto;
  /* 下拉已 Teleport 至 body；仍設 visible 避免子層觸控被捲動容器吃掉 */
  overflow-y: visible !important;
  -webkit-overflow-scrolling: touch;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions::-webkit-scrollbar {
  display: none;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
  position: relative;
  z-index: 3200;
  pointer-events: auto;
}

.stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .action-icon-btn {
  display: flex !important;
  width: 44px !important;
  height: 44px !important;
}

/* Fallback: some environments/browsers don't reliably apply :fullscreen styles */
.stock-chart.is-fullscreen .header-actions,
.stock-chart:-webkit-full-screen .header-actions {
  display: none !important;
}

.stock-chart.is-fullscreen .fullscreen-actions,
.stock-chart:-webkit-full-screen .fullscreen-actions {
  display: flex !important;
  position: relative;
  z-index: 3100;
  pointer-events: auto;
}

.stock-chart.is-fullscreen .fullscreen-carousel-controls,
.stock-chart:-webkit-full-screen .fullscreen-carousel-controls {
  position: relative;
  z-index: 3200;
  pointer-events: auto;
}

.stock-chart.is-fullscreen .action-icon-btn,
.stock-chart:-webkit-full-screen .action-icon-btn {
  display: flex !important;
  width: 44px !important;
  height: 44px !important;
}

/* 平板寬度全螢幕：右側「輪播」列在上、「縮小／下載／圖控」列在下，縮窄橫向寬度並避免與股票查詢重疊 */
@media (min-width: 769px) and (max-width: 1366px) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
    flex-direction: column !important;
    align-items: flex-end !important;
    align-self: flex-start !important;
    justify-content: flex-start !important;
    flex-wrap: nowrap !important;
    overflow-x: visible !important;
    overflow-y: visible !important;
    gap: 6px !important;
    margin-left: 8px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
    order: 1 !important;
    position: relative !important;
    inset: auto !important;
    left: auto !important;
    bottom: auto !important;
    transform: none !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions-buttons {
    order: 2 !important;
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    gap: 8px !important;
  }
}

/* Floating Gear Button */
.gear-button {
  position: absolute;
  top: 12px;
  right: 12px;
  width: 48px;
  height: 48px;
  border-radius: 50%;
  border: none;
  background: linear-gradient(135deg, rgba(15, 23, 42, 0.95), rgba(16, 12, 48, 0.95));
  color: rgba(100, 200, 255, 0.9);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 1.3rem;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: none;
  z-index: 100 !important;
  pointer-events: auto;
}

.gear-button:hover {
  background: linear-gradient(135deg, rgba(100, 200, 255, 0.15), rgba(100, 150, 255, 0.2));
  color: rgba(100, 200, 255, 1);
  transform: scale(1.08);
}

.gear-button.active {
  background: linear-gradient(135deg, rgba(100, 200, 255, 0.25), rgba(100, 150, 255, 0.3));
  color: #fff;
}

/* Removed spinning animation */

/* Floating Control Panel */
.floating-panel {
  position: fixed;
  top: 50%;
  right: 30px;
  transform: translateY(-50%);
  width: 320px;
  max-height: 75vh;
  overflow-y: auto;
  background: linear-gradient(145deg, rgba(15, 23, 42, 0.98), rgba(16, 12, 48, 0.98));
  backdrop-filter: blur(24px);
  border: 1px solid rgba(100, 200, 255, 0.25);
  border-radius: 20px;
  box-shadow: 
    0 20px 50px rgba(0, 0, 0, 0.6),
    0 0 80px rgba(100, 200, 255, 0.15),
    inset 0 1px 0 rgba(255, 255, 255, 0.1);
  z-index: 3000;
  padding: 24px;
  animation: panelSlideIn 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
}

/* 多格內：靠右窄欄，左側仍看得到 K 線，避免整格被面板蓋滿 */
.floating-panel.floating-panel--in-tile {
  position: absolute !important;
  top: 4px;
  right: 4px;
  bottom: 4px;
  left: auto !important;
  /* 小格不強制 200px 以上，避免比視窗寬 */
  width: min(100%, 232px, max(38%, 128px)) !important;
  max-width: min(100%, 232px) !important;
  max-height: calc(100% - 8px) !important;
  height: auto;
  min-height: 0;
  transform: none !important;
  z-index: 2600;
  background: rgba(8, 12, 28, 0.97) !important;
  background-image: none !important;
  backdrop-filter: none !important;
  -webkit-backdrop-filter: none !important;
  box-shadow: -6px 0 24px rgba(0, 0, 0, 0.45);
  animation: none;
  padding: 6px 8px 10px;
  border-radius: 12px;
  border: 1px solid rgba(100, 200, 255, 0.32);
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
  box-sizing: border-box;
}

.floating-panel--in-tile .sheet__header {
  padding: 0 0 2px 0;
}
.floating-panel--in-tile .panel-drag-area {
  display: none;
}
.floating-panel--in-tile .panel-header {
  margin-bottom: 8px;
  padding-bottom: 6px;
}
.floating-panel--in-tile .panel-title {
  font-size: 0.9rem;
}
.floating-panel--in-tile .panel-title::before {
  font-size: 0.85rem;
}
.floating-panel--in-tile .ai-analysis-btn {
  padding: 8px 10px;
  font-size: 0.78rem;
  border-radius: 10px;
}
.floating-panel--in-tile .panel-section {
  margin-bottom: 6px;
}
.floating-panel--in-tile .section-header {
  padding: 4px 0;
  min-height: 0;
}
.floating-panel--in-tile .section-label {
  font-size: 0.8rem;
}
.floating-panel--in-tile .param-group,
.floating-panel--in-tile .indicator-toggle {
  font-size: 0.78rem;
}
.floating-panel--in-tile .param-input {
  max-width: 3.2rem;
  padding: 2px 4px;
  font-size: 0.75rem;
}
.floating-panel--in-tile .sheet__content {
  padding: 0 0 4px 0;
}
.floating-panel--in-tile .ma-quick-actions {
  gap: 2px;
}
.floating-panel--in-tile .ma-quick-btn {
  padding: 2px 4px;
  font-size: 0.7rem;
}

@keyframes panelSlideIn {
  from {
    opacity: 0;
    transform: translateY(-50%) translateX(20px) scale(0.95);
  }
  to {
    opacity: 1;
    transform: translateY(-50%) translateX(0) scale(1);
  }
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid rgba(100, 200, 255, 0.15);
  position: relative;
}

.panel-header-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.panel-expand {
  display: none;
  width: 32px;
  height: 32px;
  border-radius: 999px;
  border: 1px solid rgba(100, 200, 255, 0.22);
  background: rgba(15, 23, 42, 0.35);
  color: rgba(226, 232, 240, 0.85);
}

.panel-expand:active {
  transform: scale(0.98);
}

.panel-header::after {
  content: '';
  position: absolute;
  bottom: -1px;
  left: 0;
  width: 60px;
  height: 2px;
  background: linear-gradient(90deg, rgba(100, 200, 255, 0.8), transparent);
  border-radius: 1px;
}

.panel-title {
  font-size: 1.1rem;
  font-weight: 700;
  color: #fff;
  display: flex;
  align-items: center;
  gap: 10px;
  letter-spacing: 0.02em;
}

.panel-title::before {
  content: '⚙️';
  font-size: 1.2rem;
}

.panel-close {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: 1px solid rgba(100, 200, 255, 0.15);
  background: rgba(15, 23, 42, 0.8);
  color: rgba(226, 232, 240, 0.6);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  font-size: 0.9rem;
}

.panel-close:hover {
  background: rgba(239, 68, 68, 0.15);
  border-color: rgba(239, 68, 68, 0.4);
  color: #f87171;
  transform: scale(1.1) rotate(90deg);
  box-shadow: 0 4px 12px rgba(239, 68, 68, 0.2);
}

.panel-section {
  margin-bottom: 28px;
  padding: 16px;
  border-radius: 12px;
  background: rgba(15, 23, 42, 0.3);
  border: 1px solid rgba(100, 200, 255, 0.08);
  transition: all 0.3s ease;
}

.panel-section:hover {
  background: rgba(15, 23, 42, 0.5);
  border-color: rgba(100, 200, 255, 0.15);
}

.panel-section:last-child {
  margin-bottom: 0;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  cursor: pointer;
  margin-bottom: 16px;
  padding: 8px 0;
  border-radius: 6px;
  transition: all 0.2s ease;
}

.section-header:hover {
  background: rgba(100, 200, 255, 0.05);
}

.section-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  font-weight: 700;
  color: rgba(226, 232, 240, 0.8);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  position: relative;
  margin-bottom: 0;
}

.section-label::after {
  content: '';
  width: 40px;
  height: 1px;
  background: linear-gradient(90deg, rgba(100, 200, 255, 0.3), transparent);
  margin-left: 8px;
}

.section-toggle {
  font-size: 0.8rem;
  color: rgba(226, 232, 240, 0.6);
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.section-toggle.rotated {
  transform: rotate(-90deg);
}

.section-toggle:hover {
  color: rgba(100, 200, 255, 0.8);
}

/* Mode Toggle Switch */
.mode-toggle-switch {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
}

.toggle-btn {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  padding: 16px 12px;
  border: 1px solid rgba(100, 200, 255, 0.2);
  border-radius: 12px;
  background: rgba(15, 23, 42, 0.4);
  color: rgba(226, 232, 240, 0.7);
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  position: relative;
  overflow: hidden;
}

.toggle-btn::before {
  content: '';
  position: absolute;
  top: 0;
  left: -100%;
  width: 100%;
  height: 100%;
  background: linear-gradient(90deg, transparent, rgba(100, 200, 255, 0.1), transparent);
  transition: left 0.5s ease;
}

.toggle-btn i {
  font-size: 1.2rem;
}

.toggle-btn span {
  font-size: 0.75rem;
  font-weight: 500;
}

.toggle-btn:hover {
  background: rgba(100, 200, 255, 0.12);
  border-color: rgba(100, 200, 255, 0.4);
  color: #fff;
  transform: translateY(-3px);
  box-shadow: 0 8px 20px rgba(100, 200, 255, 0.15);
}

.toggle-btn:hover::before {
  left: 100%;
}

.toggle-btn.active {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.3), rgba(147, 51, 234, 0.3));
  border-color: rgba(59, 130, 246, 0.6);
  color: #fff;
  box-shadow: 
    0 0 20px rgba(59, 130, 246, 0.4),
    0 4px 15px rgba(59, 130, 246, 0.2),
    inset 0 1px 3px rgba(255, 255, 255, 0.15);
  transform: translateY(-2px);
}

/* Period Chips */
.param-row {
  display: grid;
  grid-template-columns: 1fr 80px;
  align-items: center;
  gap: 8px;
}

.param-row--full {
  grid-column: 1 / -1;
  grid-template-columns: 1fr;
}

.param-input {
  width: 100%;
  background: rgba(255, 255, 255, 0.04);
  border-radius: 8px;
  background: rgba(15, 23, 42, 0.5);
  color: rgba(226, 232, 240, 0.7);
  font-size: 0.8rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  text-align: center;
}

.chip:hover {
  background: rgba(100, 200, 255, 0.15);
  border-color: rgba(100, 200, 255, 0.4);
  color: #fff;
  transform: translateY(-1px);
}

.chip.active {
  background: linear-gradient(135deg, rgba(100, 200, 255, 0.3), rgba(100, 150, 255, 0.35));
  border-color: rgba(100, 200, 255, 0.6);
  color: #fff;
  box-shadow: 0 0 12px rgba(100, 200, 255, 0.3);
  font-weight: 600;
}

/* Indicator Toggles */
.indicator-toggles {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.indicator-block {
  display: flex;
  flex-direction: column;
  gap: 0;
}

.indicator-block__row {
  display: flex;
  flex-direction: row;
  align-items: stretch;
  gap: 8px;
}

.indicator-toggle--block-main {
  flex: 1 1 0;
  min-width: 0;
}

.indicator-detail-collapse-btn {
  flex: 0 0 auto;
  width: 44px;
  min-height: 52px;
  align-self: stretch;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(100, 200, 255, 0.35);
  border-radius: 10px;
  background: rgba(15, 23, 42, 0.55);
  color: rgba(148, 163, 184, 0.95);
  cursor: pointer;
  transition: background 0.2s ease, border-color 0.2s ease, color 0.2s ease;
}

.indicator-detail-collapse-btn:hover {
  background: rgba(100, 200, 255, 0.14);
  border-color: rgba(100, 200, 255, 0.5);
  color: #e2e8f0;
}

.indicator-toggle {
  display: flex;
  align-items: center;
  padding: 16px;
  border: 1px solid rgba(100, 200, 255, 0.2);
  border-radius: 12px;
  background: rgba(15, 23, 42, 0.4);
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  position: relative;
  overflow: hidden;
}

.indicator-toggle::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  width: 4px;
  height: 100%;
  background: linear-gradient(180deg, rgba(100, 200, 255, 0.6), rgba(147, 51, 234, 0.6));
  transform: scaleY(0);
  transition: transform 0.3s ease;
}

.indicator-toggle:hover {
  background: rgba(100, 200, 255, 0.12);
  border-color: rgba(100, 200, 255, 0.4);
  transform: translateX(4px);
}

.indicator-toggle:hover::before {
  transform: scaleY(1);
}

.toggle-checkbox {
  width: 20px;
  height: 20px;
  margin-right: 16px;
  cursor: pointer;
  accent-color: rgba(59, 130, 246, 0.8);
  border-radius: 4px;
  transition: all 0.2s ease;
}

.toggle-checkbox:checked {
  accent-color: rgba(59, 130, 246, 1);
  transform: scale(1.1);
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: 12px;
  color: rgba(226, 232, 240, 0.8);
  font-size: 0.95rem;
  font-weight: 600;
  flex: 1;
  transition: all 0.3s ease;
}

.toggle-label i {
  color: rgba(100, 200, 255, 0.7);
  font-size: 1.1rem;
  transition: all 0.3s ease;
}

.indicator-plan-tag {
  margin-left: 2px;
  padding: 1px 6px;
  border-radius: 999px;
  border: 1px solid rgba(251, 191, 36, 0.45);
  background: rgba(251, 191, 36, 0.14);
  color: #fbbf24;
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.02em;
  line-height: 1.35;
}

.indicator-toggle:has(.toggle-checkbox:checked) {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.25), rgba(147, 51, 234, 0.25));
  border-color: rgba(59, 130, 246, 0.5);
  box-shadow: 0 0 15px rgba(59, 130, 246, 0.2);
}

.indicator-toggle:has(.toggle-checkbox:checked)::before {
  transform: scaleY(1);
}

.indicator-toggle:has(.toggle-checkbox:checked) .toggle-label {
  color: #fff;
  font-weight: 700;
}

.indicator-toggle:has(.toggle-checkbox:checked) .toggle-label i {
  color: rgba(59, 130, 246, 0.9);
  transform: scale(1.1);
}

/* Parameter Controls */
.parameter-controls {
  margin-top: 12px;
  padding: 12px;
  background: rgba(15, 23, 42, 0.6);
  border-radius: 8px;
  border: 1px solid rgba(100, 200, 255, 0.15);
}

.param-group {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}

.param-group:last-child {
  margin-bottom: 0;
}

.param-group label {
  font-size: 0.85rem;
  color: rgba(226, 232, 240, 0.7);
  font-weight: 500;
  min-width: 40px;
}

.param-input {
  width: 60px;
  padding: 4px 8px;
  border: 1px solid rgba(100, 200, 255, 0.25);
  border-radius: 4px;
  background: rgba(15, 23, 42, 0.8);
  color: rgba(226, 232, 240, 0.9);
  font-size: 0.85rem;
  text-align: center;
  transition: all 0.2s ease;
}

.param-input:focus {
  outline: none;
  border-color: rgba(59, 130, 246, 0.6);
  background: rgba(15, 23, 42, 0.9);
  box-shadow: 0 0 8px rgba(59, 130, 246, 0.2);
}

.param-input:hover {
  border-color: rgba(100, 200, 255, 0.4);
}

 .param-input--hidden-value {
   color: transparent;
   -webkit-text-fill-color: transparent;
   text-shadow: none;
   caret-color: transparent;
 }

.param-checkbox {
  width: 18px;
  height: 18px;
  accent-color: rgba(59, 130, 246, 0.9);
  cursor: pointer;
}

/* Parameter Warning */
.param-warning {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin-top: 12px;
  padding: 10px 12px;
  background: rgba(245, 158, 11, 0.1);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 6px;
  color: rgba(251, 191, 36, 0.95);
  font-size: 0.8rem;
  line-height: 1.4;
}

.param-warning i {
  color: rgba(245, 158, 11, 0.9);
  font-size: 0.9rem;
  margin-top: 2px;
  flex-shrink: 0;
}

.param-warning span {
  flex: 1;
}

/* Panel Backdrop */
.panel-backdrop {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.15);
  z-index: 2500;
  cursor: pointer;
}

/* 多格內：僅蓋本格 chart-wrapper；濃度降低，便於仍辨識左側 K 線 */
.panel-backdrop--cell {
  position: absolute;
  inset: 0;
  z-index: 2480;
  background: linear-gradient(
    90deg,
    rgba(2, 6, 23, 0.22) 0%,
    rgba(2, 6, 23, 0.22) min(50%, calc(100% - 240px)),
    rgba(2, 6, 23, 0.42) 100%
  );
  cursor: pointer;
  pointer-events: auto;
  border-radius: 0 0 12px 12px;
}

.ai-analysis-row {
  padding: 10px 6px 12px 6px;
}

.ai-analysis-btn {
  width: 100%;
  padding: 12px 14px;
  border-radius: 12px;
  border: 1px solid rgba(100, 200, 255, 0.25);
  background: rgba(15, 23, 42, 0.6);
  color: rgba(226, 232, 240, 0.92);
  font-weight: 700;
  cursor: pointer;
}

.ai-analysis-btn:hover {
  border-color: rgba(59, 130, 246, 0.6);
  background: rgba(30, 64, 175, 0.5);
  color: #fff;
}

:global(.ai-modal),
.ai-modal {
  position: fixed !important;
  inset: 0 !important;
  background: rgba(0, 0, 0, 0.68);
  z-index: 12000 !important;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
  pointer-events: auto;
}

:global(.ai-modal__panel),
.ai-modal__panel {
  width: min(760px, 96vw);
  max-height: min(80vh, 720px);
  overflow: hidden;
  border-radius: 16px;
  border: 1px solid rgba(100, 200, 255, 0.22);
  background: rgba(15, 23, 42, 0.98);
  box-shadow: 0 24px 80px rgba(0, 0, 0, 0.55);
}

:global(.ai-modal__header),
.ai-modal__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 14px;
  border-bottom: 1px solid rgba(100, 200, 255, 0.15);
}

:global(.ai-modal__title),
.ai-modal__title {
  color: rgba(226, 232, 240, 0.95);
  font-weight: 800;
}

:global(.ai-modal__close),
.ai-modal__close {
  border: none;
  background: transparent;
  color: rgba(226, 232, 240, 0.75);
  cursor: pointer;
  padding: 6px;
}

:global(.ai-modal__body),
.ai-modal__body {
  padding: 14px;
  overflow: auto;
  max-height: calc(min(80vh, 720px) - 52px);
}

:global(.ai-modal__loading),
.ai-modal__loading {
  color: rgba(226, 232, 240, 0.85);
  font-weight: 700;
}

:global(.ai-modal__error),
.ai-modal__error {
  color: rgba(248, 113, 113, 0.95);
  font-weight: 700;
  white-space: pre-wrap;
}

:global(.ai-modal__text),
.ai-modal__text {
  color: rgba(226, 232, 240, 0.9);
  white-space: pre-wrap;
  font-size: 0.95rem;
  line-height: 1.6;
  margin: 0;
}

:global(.ai-modal__disclaimer),
.ai-modal__disclaimer {
  color: rgba(148, 163, 184, 0.9);
  font-size: 0.85rem;
  line-height: 1.5;
  margin: 0 0 10px 0;
}

:global(.ai-modal__content),
.ai-modal__content {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

:global(.ai-modal__usage),
.ai-modal__usage {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 8px;
}

:global(.ai-modal__usage-label),
.ai-modal__usage-label {
  color: rgba(148, 163, 184, 0.9);
  font-size: 0.78rem;
  font-weight: 700;
}

:global(.ai-modal__usage-item),
.ai-modal__usage-item {
  color: rgba(226, 232, 240, 0.82);
  font-size: 0.78rem;
}

.stock-chart.ai-modal-open .chart-header,
.stock-chart.ai-modal-open .floating-panel,
.stock-chart.ai-modal-open .panel-backdrop,
.stock-chart.ai-modal-open .fullscreen-carousel-controls,
.stock-chart.ai-modal-open .mobile-fs-toolbar-collapse-btn {
  display: none !important;
}

.plan-free .parameter-controls,
.plan-free .param-grid {
  opacity: 0.55;
}

.plan-free .parameter-controls input,
.plan-free .parameter-controls select,
.plan-free .parameter-controls textarea,
.plan-free .param-grid input,
.plan-free .param-grid select,
.plan-free .param-grid textarea {
  pointer-events: none;
}

.upgrade-modal {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.55);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9999;
  padding: 16px;
}

.upgrade-modal__panel {
  width: min(560px, 92vw);
  overflow: hidden;
  border-radius: 16px;
  background: rgba(15, 23, 42, 0.92);
  border: 1px solid rgba(148, 163, 184, 0.18);
  box-shadow: 0 18px 50px rgba(0, 0, 0, 0.45);
}

.upgrade-modal__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 14px;
  border-bottom: 1px solid rgba(100, 200, 255, 0.15);
}

.upgrade-modal__head-left {
  display: flex;
  gap: 12px;
  align-items: center;
}

.upgrade-modal__icon {
  width: 38px;
  height: 38px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 12px;
  background: rgba(234, 179, 8, 0.16);
  border: 1px solid rgba(234, 179, 8, 0.28);
  color: rgba(234, 179, 8, 0.95);
}

.upgrade-modal__title {
  color: rgba(226, 232, 240, 0.95);
  font-weight: 900;
}

.upgrade-modal__subtitle {
  color: rgba(226, 232, 240, 0.7);
  font-size: 0.85rem;
  margin-top: 2px;
}

.upgrade-modal__close {
  border: none;
  background: transparent;
  color: rgba(226, 232, 240, 0.75);
  cursor: pointer;
  padding: 6px;
}

.upgrade-modal__body {
  padding: 14px;
}

.upgrade-modal__lead {
  color: rgba(226, 232, 240, 0.9);
  white-space: pre-wrap;
  margin: 0;
  line-height: 1.6;
}

.upgrade-modal__actions {
  display: flex;
  gap: 10px;
  justify-content: flex-end;
  padding: 0 14px 14px;
}

.upgrade-modal__btn {
  border: 1px solid rgba(148, 163, 184, 0.22);
  background: rgba(15, 23, 42, 0.55);
  color: rgba(226, 232, 240, 0.92);
  padding: 10px 12px;
  border-radius: 12px;
  cursor: pointer;
  font-weight: 800;
}

.upgrade-modal__btn--primary {
  background: linear-gradient(180deg, rgba(59, 130, 246, 0.95), rgba(37, 99, 235, 0.95));
  border-color: rgba(59, 130, 246, 0.55);
}

/* Panel Fade Animation */
.panel-fade-enter-active,
.panel-fade-leave-active {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

/* Collapse Animation */
.collapse-enter-active,
.collapse-leave-active {
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  overflow: hidden;
}

.collapse-enter-from,
.collapse-leave-to {
  max-height: 0;
  opacity: 0;
  transform: translateY(-10px);
}

.collapse-enter-to,
.collapse-leave-from {
  max-height: 500px;
  opacity: 1;
  transform: translateY(0);
}

.ma-quick-actions {
  display: flex;
  align-items: center;
  gap: 5px;
  margin-left: auto;
  margin-right: 8px;
}

.ma-quick-btn {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 3px 9px;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.03em;
  cursor: pointer;
  border: 1px solid transparent;
  transition: background 0.16s, border-color 0.16s, opacity 0.16s;
  white-space: nowrap;
  line-height: 1.5;
}

.ma-quick-btn i {
  font-size: 0.68rem;
}

.ma-quick-btn--on {
  background: rgba(56, 189, 248, 0.12);
  border-color: rgba(56, 189, 248, 0.32);
  color: rgba(125, 211, 252, 0.95);
}

.ma-quick-btn--on:hover {
  background: rgba(56, 189, 248, 0.22);
  border-color: rgba(125, 211, 252, 0.55);
}

.ma-quick-btn--off {
  background: rgba(148, 163, 184, 0.08);
  border-color: rgba(148, 163, 184, 0.22);
  color: rgba(148, 163, 184, 0.75);
}

.ma-quick-btn--off:hover {
  background: rgba(148, 163, 184, 0.15);
  border-color: rgba(148, 163, 184, 0.38);
  color: rgba(226, 232, 240, 0.85);
}

.panel-fade-enter-from {
  opacity: 0;
}

.panel-fade-leave-to {
  opacity: 0;
}

.chart-loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
}

.chart-loading--overlay {
  position: absolute;
  inset: 0;
  z-index: 3;
  min-height: var(--chart-container-min-height);
  background: linear-gradient(180deg, rgba(2, 6, 23, 0.08), rgba(2, 6, 23, 0.24));
  pointer-events: none;
}

.chart-loading i {
  font-size: 2rem;
  color: rgba(100, 200, 255, 0.6);
}

.chart-loading span {
  font-size: 0.95rem;
  color: rgba(226, 232, 240, 0.6);
}

.chart-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  background: rgba(15, 23, 42, 0.4);
  border: 2px dashed rgba(100, 200, 255, 0.2);
  border-radius: 12px;
}

.chart-empty--overlay {
  position: absolute;
  inset: 0;
  z-index: 2;
  min-height: var(--chart-container-min-height);
}

.chart-empty i {
  font-size: 3rem;
  color: rgba(100, 200, 255, 0.4);
}

.chart-empty p {
  font-size: 1.1rem;
  font-weight: 600;
  color: rgba(226, 232, 240, 0.7);
  margin: 0;
}

.chart-error-overlay {
  z-index: 4;
  padding: 24px;
  text-align: center;
  background: rgba(8, 15, 30, 0.94);
  border-color: rgba(248, 113, 113, 0.38);
}

.chart-error-overlay > i {
  color: #fb7185;
}

.chart-error-detail {
  max-width: min(520px, 90%);
  color: rgba(203, 213, 225, 0.78);
  font-size: 0.85rem;
  line-height: 1.5;
  overflow-wrap: anywhere;
}

.chart-error-retry {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 40px;
  padding: 9px 16px;
  border: 1px solid rgba(96, 165, 250, 0.48);
  border-radius: 10px;
  color: #e2e8f0;
  background: rgba(37, 99, 235, 0.28);
  font: inherit;
  cursor: pointer;
}

.chart-error-retry:hover {
  border-color: rgba(147, 197, 253, 0.75);
  background: rgba(37, 99, 235, 0.42);
}

.chart-error-retry i {
  font-size: 0.9rem;
}

/* Fix sticky header z-index over chart - Ensure chart stays below header */
.chart-container {
  width: 100% !important;
  height: var(--chart-container-height) !important;
  min-height: var(--chart-container-min-height) !important;
  position: relative !important;
  z-index: 1 !important;
  overflow: visible !important;
  background: transparent !important;
  border: none !important;
  border-radius: 0 !important;
  padding: 0 !important;
  display: block !important;
  isolation: isolate !important;
  visibility: visible !important;
}

.chart-container-echarts {
  position: absolute !important;
  inset: 0 !important;
  width: 100% !important;
  height: 100% !important;
  z-index: 1 !important;
}

/* 確保 ECharts canvas 在工具列下方 */
.chart-container canvas,
.chart-container-echarts canvas {
  position: relative !important;
  z-index: 1 !important;
}

/* Fix sticky header z-index over chart */
.chart-wrapper {
  position: relative;
  z-index: 1;
  /* Remove any outer frame/glow */
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
}

.chart-info {
  display: flex;
  justify-content: center;
  background: rgba(15, 23, 42, 0.4);
  border-radius: 8px;
}

.info-item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  color: rgba(226, 232, 240, 0.6);
}

.info-item i {
  color: rgba(100, 200, 255, 0.5);
}

@media (max-width: 640px) {
  /* 股票代號上移，避免與貼頂的均線圖例重疊 */
  .legend-overlay-row--name {
    top: -44px;
  }

  .legend-overlay-row.legend-overlay-row--mobile-pinned-inline {
    top: -44px;
  }

  .legend-overlay-row.fullscreen-active.legend-overlay-row--mobile-pinned-inline {
    top: -10px;
  }

  .chart-header {
    flex-direction: column;
    align-items: stretch;
  }
  
  .chart-container {
    height: var(--chart-container-height) !important;
    min-height: max(400px, var(--chart-container-min-height)) !important;
  }
  
  /* Adjust floating panel for mobile */
  .floating-panel {
    position: fixed;
    top: auto;
    bottom: 0;
    left: 0;
    right: 0;
    width: 100%;
    max-height: none;
    border-radius: 20px 20px 0 0;
    padding: 0;
    -webkit-overflow-scrolling: touch;
    overscroll-behavior: contain;
    z-index: 3000;
    touch-action: none;
  }

  .floating-panel.is-sheet {
    display: flex;
    flex-direction: column;
    overflow: hidden;
    padding-bottom: env(safe-area-inset-bottom, 0px);
  }

  .floating-panel.is-collapsed .sheet__content {
    display: none;
  }

  .sheet__header {
    position: sticky;
    top: 0;
    z-index: 10;
    padding: 12px 20px 10px;
    background: inherit;
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
  }

  .sheet__content {
    flex: 1;
    overflow-y: auto;
    -webkit-overflow-scrolling: touch;
    overscroll-behavior: contain;
    padding: 0 20px 16px;
  }

  .floating-panel .panel-drag-area {
    display: flex;
    justify-content: center;
    align-items: center;
    height: 16px;
    margin: -6px 0 10px;
    cursor: grab;
    touch-action: none;
  }

  .floating-panel .panel-drag-handle {
    width: 56px;
    height: 5px;
    border-radius: 999px;
    background: rgba(226, 232, 240, 0.25);
    border: 1px solid rgba(100, 200, 255, 0.18);
  }

  .floating-panel .panel-header {
    touch-action: pan-y;
  }

  .panel-expand {
    display: inline-flex;
    align-items: center;
    justify-content: center;
  }
  
  .gear-button {
    width: 40px;
    height: 40px;
    font-size: 1rem;
  }
  
  .period-chips {
    grid-template-columns: repeat(2, 1fr);
  }
}

/* 手機（非全螢幕）內嵌圖表：兩列結構（週期列 + K線/查詢列），避免 display:contents 造成 chip 與控制項亂換行 */
@media (max-width: 768px) {
  .stock-chart:not(.is-fullscreen) .chart-header {
    margin-bottom: 8px;
  }

  .stock-chart:not(.is-fullscreen) .frequency-controls {
    display: flex !important;
    flex-direction: column !important;
    flex-wrap: nowrap !important;
    align-items: stretch !important;
    gap: 8px;
    padding: 8px 10px 10px;
    margin-bottom: 4px;
    background: rgba(12, 18, 34, 0.88);
    border: 1px solid rgba(59, 130, 246, 0.14);
    border-radius: 14px;
    box-shadow: 0 8px 28px rgba(0, 0, 0, 0.22);
  }

  .stock-chart:not(.is-fullscreen) .mobile-period-row {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    align-items: center;
    gap: 8px;
    width: 100%;
    min-width: 0;
    padding-bottom: 6px;
    border-bottom: 1px solid rgba(100, 200, 255, 0.1);
  }

  .stock-chart:not(.is-fullscreen) .period-chips-scroll {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    flex: 1 1 auto;
    min-width: 0;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    gap: 4px;
    scrollbar-width: none;
    -ms-overflow-style: none;
    padding-right: max(8px, env(safe-area-inset-right, 0px));
  }

  .stock-chart:not(.is-fullscreen) .period-chips-scroll::-webkit-scrollbar {
    display: none;
  }

  .stock-chart:not(.is-fullscreen) .mobile-toolbar-secondary {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: wrap !important;
    align-items: center !important;
    justify-content: flex-start;
    align-content: flex-start;
    gap: 8px;
    row-gap: 8px;
    width: 100%;
    min-width: 0;
    overflow-x: visible;
  }

  /* kline, search, actions in DOM; flex order: kline + icons row, then full-width search */
  .stock-chart:not(.is-fullscreen) .mobile-toolbar-secondary .kline-mode-toggle {
    order: 1;
    flex: 0 0 auto;
    width: auto;
    min-width: 0;
    max-width: none;
  }

  .stock-chart:not(.is-fullscreen) .mobile-toolbar-secondary .fullscreen-actions {
    order: 2;
    flex: 0 0 auto;
    margin-left: auto;
    align-self: center;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-box {
    order: 3;
    flex: 1 1 100%;
    width: 100%;
    min-width: 100%;
    max-width: 100%;
    margin: 0 !important;
    padding: 0 !important;
    gap: 0 !important;
    border: none !important;
    background: none !important;
    border-radius: 0 !important;
    box-sizing: border-box !important;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-query-row {
    gap: 8px;
    width: 100%;
    align-items: stretch;
  }

  .stock-chart:not(.is-fullscreen) .kline-mode-icon-dropdown__trigger {
    height: 40px;
    width: auto;
    min-width: 40px;
    padding: 0 9px;
    gap: 5px;
    font-size: 0.88rem;
    border-radius: 12px;
    box-sizing: border-box;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-box .search-input {
    flex: 1 1 auto;
    min-width: 0;
    width: auto;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-box .search-input input {
    width: 100%;
    min-width: 0;
    height: 40px;
    padding: 0 12px;
    font-size: 0.88rem;
    font-weight: 600;
    color: #ffffff;
    caret-color: #ffffff;
    text-align: left;
    box-sizing: border-box;
    border-radius: 10px;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-box .search-input input::placeholder {
    color: rgba(255, 255, 255, 0.88);
    opacity: 1;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-btn {
    flex-shrink: 0;
    height: 40px;
    min-width: 62px;
    padding: 0 12px;
    font-size: 0.84rem;
    border-radius: 10px;
  }

  .stock-chart:not(.is-fullscreen) .visible-count-stepper {
    border-radius: 20px;
    height: 40px;
    flex-shrink: 0;
  }

  .stock-chart:not(.is-fullscreen) .stepper-btn {
    width: 30px;
    height: 38px;
    font-size: 1rem;
  }

  .stock-chart:not(.is-fullscreen) .stepper-value {
    width: 44px;
    min-width: 36px;
    font-size: 0.82rem;
    height: 38px;
    line-height: 38px;
  }

  .stock-chart:not(.is-fullscreen) .fullscreen-search-mic-toggle {
    width: 40px;
    height: 40px;
    border-radius: 10px;
    font-size: 0.88rem;
    flex-shrink: 0;
  }

  .stock-chart:not(.is-fullscreen) .period-chip {
    flex: 0 0 auto;
    padding: 0 14px;
    height: 40px;
    line-height: 40px;
    font-size: 0.82rem;
    min-width: 52px;
    text-align: center;
    border-radius: 12px;
    box-sizing: border-box;
  }

  .stock-chart:not(.is-fullscreen) .period-chip.active {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.9), rgba(147, 51, 234, 0.9));
    border-color: rgba(59, 130, 246, 0.62);
    color: #fff;
    font-weight: 600;
    box-shadow: 0 4px 16px rgba(59, 130, 246, 0.28);
    transform: none;
  }

  .stock-chart:not(.is-fullscreen) .kline-mode-toggle {
    padding: 3px;
    gap: 4px;
    border-radius: 18px;
    display: inline-flex;
    width: auto;
    max-width: 100%;
  }

  .stock-chart:not(.is-fullscreen) .kline-mode-toggle .mode-btn {
    flex: 0 0 auto;
    min-width: 44px;
    justify-content: center;
    padding: 6px 8px;
    border-radius: 12px;
    font-size: 0.76rem;
  }

  .stock-chart:not(.is-fullscreen) .kline-mode-toggle .mode-btn span {
    display: inline;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .stock-chart:not(.is-fullscreen) .mode-btn {
    padding: 6px 12px;
    font-size: 0.78rem;
  }

  @media (max-width: 480px) {
    .stock-chart:not(.is-fullscreen) .period-chip {
      font-size: 0.7rem;
      height: 36px;
      line-height: 36px;
      padding: 0 10px;
      min-width: 44px;
    }
    .stock-chart:not(.is-fullscreen) .visible-count-stepper {
      height: 36px;
    }
    .stock-chart:not(.is-fullscreen) .stepper-btn {
      height: 34px;
    }
    .stock-chart:not(.is-fullscreen) .stepper-value {
      height: 34px;
      line-height: 34px;
    }
  }
}

@media (min-width: 641px) {
  .floating-panel {
    top: 50% !important;
    bottom: auto !important;
    left: auto !important;
    right: 30px !important;
    transform: translateY(-50%) !important;
    max-height: 75vh !important;
    overflow-y: auto !important;
    touch-action: auto !important;
  }
}

/* ========================================
   手機版全螢幕專用樣式
   Mobile Fullscreen Specific Styles
   ======================================== */
@media (max-width: 768px) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) {
    display: flex;
    flex-direction: column;
    padding: 0 !important;
    /* 工具列縮矮後，扣減量同步降低，把垂直空間還給圖表 */
    /* 查詢與 K 線、圖示同一列，工具列總高度再降 */
    --chart-container-height: calc(100svh - 122px);
    --chart-container-min-height: max(200px, calc(100svh - 122px));
  }

  .stock-chart.mobile-fs-toolbar-collapsed:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) {
    --chart-container-height: calc(100svh - 52px - env(safe-area-inset-top, 0px));
    --chart-container-min-height: max(200px, calc(100svh - 52px - env(safe-area-inset-top, 0px)));
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-fs-toolbar-expand,
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-fs-toolbar-collapse-btn {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 10px 14px;
    margin: 0;
    border-radius: 12px;
    border: 1px solid rgba(59, 130, 246, 0.28);
    background: rgba(12, 18, 34, 0.94);
    color: rgba(226, 232, 240, 0.95);
    font-size: 0.82rem;
    font-weight: 600;
    cursor: pointer;
    flex-shrink: 0;
    box-sizing: border-box;
    -webkit-tap-highlight-color: transparent;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-fs-toolbar-expand:active,
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-fs-toolbar-collapse-btn:active {
    background: rgba(30, 64, 175, 0.45);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-fs-toolbar-collapse-btn {
    margin-top: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header {
    padding: 6px 8px !important;
    gap: 6px;
    flex-shrink: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .header-row {
    display: none !important;
  }

  /* 手機版工具列：垂直堆疊佈局（緊湊） */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
    flex-direction: column !important;
    align-items: stretch !important;
    gap: 10px !important;
    padding: 10px !important;
    background: linear-gradient(180deg, rgba(12, 18, 34, 0.94), rgba(10, 16, 30, 0.9));
    border: 1px solid rgba(96, 165, 250, 0.14);
    border-radius: 18px;
    box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28);
  }

  /* 手機版週期行：水平排列，可滾動 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-period-row {
    display: flex !important;
    flex-direction: row !important;
    align-items: center;
    gap: 8px;
    width: 100%;
    order: 1;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(96, 165, 250, 0.12);
  }

  /* 顯示根數：緊湊版 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-stepper {
    flex-shrink: 0;
    border-radius: 16px;
    height: 34px;
    border-color: rgba(96, 165, 250, 0.2);
    background: rgba(15, 23, 42, 0.72);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .stepper-btn {
    width: 28px;
    height: 32px;
    font-size: 0.82rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .stepper-value {
    min-width: 40px;
    font-size: 0.8rem;
    line-height: 32px;
    height: 32px;
  }

  /* keep old visible-count-input entry for backward compat */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input {
    display: none !important;  /* hidden; stepper replaces it */
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input input {
    width: 46px;
    padding: 2px 4px;
    font-size: 0.72rem;
  }

  /* 週期按鈕滾動容器（右側留白避免最後一顆 chip 貼邊／被誤認裁切） */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chips-scroll {
    display: flex !important;
    flex-direction: row !important;
    gap: 6px;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    -ms-overflow-style: none;
    flex: 1;
    min-width: 0;
    padding: 0;
    padding-right: max(10px, env(safe-area-inset-right, 0px));
    scroll-padding-inline-end: max(10px, env(safe-area-inset-right, 0px));
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chips-scroll::-webkit-scrollbar {
    display: none;
  }

  /* 週期按鈕：日K/週K/月K 緊湊 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chip {
    padding: 0 16px;
    font-size: 0.8rem;
    min-width: auto;
    min-height: 42px;
    flex-shrink: 0;
    border-radius: 14px;
    box-sizing: border-box;
    background: rgba(15, 23, 42, 0.62);
    border-color: rgba(96, 165, 250, 0.16);
    color: rgba(226, 232, 240, 0.72);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chip.active {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.9), rgba(147, 51, 234, 0.9));
    border-color: rgba(59, 130, 246, 0.62);
    color: #fff;
    font-weight: 600;
    transform: none;
    box-shadow: 0 4px 14px rgba(59, 130, 246, 0.24);
  }

  /* 第二區：日週月列下方 — K 線模式下拉 | 股票查詢 | 圖表控制 同一列（不靠內層裁切捲動） */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-toolbar-secondary {
    display: flex !important;
    flex-direction: row;
    flex-wrap: nowrap;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;
    order: 3;
    min-width: 0;
    overflow-x: visible;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
    order: 1;
    flex: 0 0 auto;
    min-width: 0;
    width: auto;
    max-width: none;
    margin: 0 !important;
    padding: 0;
    justify-content: flex-start;
    flex-wrap: nowrap;
    gap: 6px;
    border-radius: 0;
    overflow: visible;
    background: transparent;
    border: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn {
    flex: 0 0 auto;
    width: 44px;
    min-width: 44px;
    height: 44px;
    min-height: 44px;
    justify-content: center;
    align-items: center;
    padding: 0;
    gap: 0;
    font-size: 0.8rem;
    border-radius: 14px;
    background: rgba(15, 23, 42, 0.68);
    border-color: rgba(96, 165, 250, 0.18);
    color: rgba(226, 232, 240, 0.86);
    box-sizing: border-box;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn span {
    display: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn i {
    font-size: 0.9rem;
    flex-shrink: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-select-shell {
    flex: 0 0 auto;
    width: auto;
    min-width: 0;
    max-width: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-icon-dropdown__trigger {
    width: auto;
    min-width: 44px;
    height: 44px;
    min-height: 44px;
    padding: 0 9px;
    gap: 6px;
    font-size: 0.8rem;
    font-weight: 600;
    color: rgba(226, 232, 240, 0.92);
    border-radius: 14px;
    box-sizing: border-box;
    background-color: rgba(15, 23, 42, 0.68);
    border: 1px solid rgba(96, 165, 250, 0.22);
    cursor: pointer;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-icon-dropdown__trigger:focus {
    outline: none;
    border-color: rgba(59, 130, 246, 0.55);
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.22);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-query-row {
    gap: 6px;
    padding: 6px;
    border-radius: 14px;
    background: rgba(15, 23, 42, 0.58);
    border: 1px solid rgba(96, 165, 250, 0.14);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box .search-input {
    flex: 1 1 auto;
    min-width: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box .search-input input {
    width: 72px;
    min-width: 0;
    flex: 1 1 72px;
    max-width: 100%;
    min-height: 40px;
    padding: 0 10px;
    font-size: 0.88rem;
    font-weight: 600;
    color: #ffffff;
    caret-color: #ffffff;
    text-align: left;
    box-sizing: border-box;
    border-radius: 12px;
    background: rgba(15, 23, 42, 0.78);
    border-color: rgba(96, 165, 250, 0.22);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box .search-input input::placeholder {
    color: rgba(255, 255, 255, 0.88);
    opacity: 1;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-btn {
    flex-shrink: 0;
    min-height: 40px;
    min-width: 62px;
    padding: 0 12px;
    font-size: 0.84rem;
    border-radius: 12px;
    align-self: stretch;
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.92), rgba(147, 51, 234, 0.88));
    border-color: rgba(96, 165, 250, 0.3);
    color: #fff;
    box-shadow: 0 6px 18px rgba(59, 130, 246, 0.22);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
    order: 2;
    position: static !important;
    top: auto;
    right: auto;
    flex: 0 0 auto;
    flex-shrink: 0;
    flex-direction: row !important;
    align-items: center;
    gap: 6px !important;
    margin: 0 !important;
    z-index: 1;
    padding: 0;
    border-radius: 0;
    background: transparent;
    border: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box {
    order: 2;
    flex: 0 1 230px;
    min-width: 0;
    max-width: 230px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions-buttons {
    flex-direction: row;
    align-items: center;
    gap: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .action-icon-btn {
    width: 44px !important;
    height: 44px !important;
    min-width: 44px;
    min-height: 44px;
    font-size: 0.9rem;
    border-radius: 14px;
    background: rgba(15, 23, 42, 0.68);
    border-color: rgba(96, 165, 250, 0.18);
    color: rgba(226, 232, 240, 0.9);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-mic-toggle {
    width: 40px;
    height: 40px;
    border-radius: 12px;
    background: rgba(15, 23, 42, 0.78);
    border-color: rgba(96, 165, 250, 0.2);
    color: rgba(226, 232, 240, 0.78);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-speech-err {
    padding: 0 4px;
    font-size: 0.78rem;
    color: rgba(252, 165, 165, 0.92);
  }

  /* 輪播控制：底部固定 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
    position: fixed !important;
    bottom: 10px;
    left: 50%;
    transform: translateX(-50%);
    flex-direction: row;
    padding: 6px 14px;
    background: rgba(5, 12, 30, 0.9) !important;
    border-radius: 20px;
    z-index: 3500;
    gap: 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-indicator {
    font-size: 0.75rem;
    white-space: nowrap;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn {
    width: 34px;
    height: 34px;
    border-radius: 10px;
  }

  /* 圖表標題：縮小並置中 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .legend-title-text--fullscreen {
    font-size: 0.95rem;
    top: 0;
    padding: 4px 10px;
  }

  /* ========================================
     手機版控制面板：精緻化設計
     Mobile Control Panel: Refined Design
     ======================================== */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel {
    position: fixed !important;
    top: auto !important;
    bottom: 0 !important;
    left: 0 !important;
    right: 0 !important;
    width: 100% !important;
    max-height: 70vh !important;
    border-radius: 24px 24px 0 0 !important;
    transform: translateY(0) !important;
    animation: mobilePanelSlideUp 0.4s cubic-bezier(0.32, 0.72, 0, 1) !important;
    background: linear-gradient(180deg, rgba(15, 20, 35, 0.98) 0%, rgba(10, 15, 28, 0.99) 100%) !important;
    border: none !important;
    border-top: 1px solid rgba(100, 150, 255, 0.15) !important;
    box-shadow: 
      0 -8px 32px rgba(0, 0, 0, 0.5),
      0 -2px 16px rgba(59, 130, 246, 0.08),
      inset 0 1px 0 rgba(255, 255, 255, 0.05) !important;
    padding: 0 !important;
    overflow: hidden !important;
  }

  @keyframes mobilePanelSlideUp {
    from {
      opacity: 0;
      transform: translateY(100%);
    }
    to {
      opacity: 1;
      transform: translateY(0);
    }
  }

  /* 面板頂部拖曳指示條 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel::before {
    content: '';
    position: absolute;
    top: 8px;
    left: 50%;
    transform: translateX(-50%);
    width: 36px;
    height: 4px;
    background: rgba(148, 163, 184, 0.4);
    border-radius: 2px;
  }

  /* 面板標題區 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-header {
    position: sticky;
    top: 0;
    background: linear-gradient(180deg, rgba(15, 20, 35, 0.99) 0%, rgba(15, 20, 35, 0.95) 100%);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    z-index: 10;
    margin: 0 !important;
    padding: 20px 20px 14px 20px !important;
    border-bottom: 1px solid rgba(100, 150, 255, 0.08);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-header::after {
    display: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-title {
    font-size: 1rem;
    font-weight: 600;
    color: rgba(226, 232, 240, 0.95);
    letter-spacing: 0.02em;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-title::before {
    content: '⚙️';
    font-size: 1.1rem;
    margin-right: 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-close {
    width: 28px;
    height: 28px;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.06);
    border: 1px solid rgba(255, 255, 255, 0.1);
    color: rgba(226, 232, 240, 0.7);
    font-size: 0.85rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-close:hover {
    background: rgba(239, 68, 68, 0.15);
    border-color: rgba(239, 68, 68, 0.3);
    color: #f87171;
  }

  /* AI 分析按鈕 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ai-analysis-row {
    padding: 12px 16px 8px 16px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ai-analysis-btn {
    width: 100%;
    padding: 14px 16px;
    border-radius: 14px;
    border: 1px solid rgba(59, 130, 246, 0.25);
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.12) 0%, rgba(147, 51, 234, 0.08) 100%);
    color: rgba(226, 232, 240, 0.95);
    font-weight: 600;
    font-size: 0.92rem;
    letter-spacing: 0.02em;
    transition: all 0.2s ease;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ai-analysis-btn:hover,
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ai-analysis-btn:active {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(147, 51, 234, 0.15) 100%);
    border-color: rgba(59, 130, 246, 0.4);
    transform: scale(0.99);
  }

  /* 面板內容滾動區 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel > *:not(.panel-header):not(.ai-analysis-row) {
    padding-left: 16px;
    padding-right: 16px;
  }

  /* 區塊設定 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-section {
    margin: 0 16px 12px 16px !important;
    padding: 0 !important;
    background: rgba(255, 255, 255, 0.02) !important;
    border: 1px solid rgba(100, 150, 255, 0.08) !important;
    border-radius: 16px !important;
    overflow: hidden;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-section:hover {
    background: rgba(255, 255, 255, 0.03) !important;
    border-color: rgba(100, 150, 255, 0.12) !important;
  }

  /* 區塊標題 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-header {
    padding: 14px 16px;
    margin: 0;
    background: transparent;
    border-radius: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-label {
    font-size: 0.88rem;
    font-weight: 600;
    color: rgba(226, 232, 240, 0.85);
    text-transform: none;
    letter-spacing: 0.01em;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-label::after {
    display: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-toggle {
    font-size: 0.75rem;
    color: rgba(148, 163, 184, 0.6);
    transition: transform 0.3s ease, color 0.2s ease;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-toggle.rotated {
    transform: rotate(-90deg);
  }

  /* 參數控制區 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .parameter-controls {
    margin: 0;
    padding: 12px 16px 16px 16px;
    background: rgba(0, 0, 0, 0.15);
    border-radius: 0;
    border: none;
    border-top: 1px solid rgba(100, 150, 255, 0.06);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-group {
    margin-bottom: 10px;
    padding: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-group:last-child {
    margin-bottom: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-group label {
    font-size: 0.82rem;
    color: rgba(226, 232, 240, 0.7);
    font-weight: 500;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-input {
    width: 60px;
    padding: 6px 8px;
    font-size: 0.82rem;
    border-radius: 8px;
    border: 1px solid rgba(100, 150, 255, 0.2);
    background: rgba(15, 23, 42, 0.6);
    color: rgba(226, 232, 240, 0.9);
    text-align: center;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-input:focus {
    border-color: rgba(59, 130, 246, 0.5);
    background: rgba(15, 23, 42, 0.8);
    outline: none;
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
  }

  /* 指標切換項目 - 精緻卡片風格 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggles {
    display: flex;
    flex-direction: column;
    gap: 0;
    padding: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle {
    padding: 14px 16px;
    margin: 0;
    border: none;
    border-radius: 0;
    background: transparent;
    border-bottom: 1px solid rgba(100, 150, 255, 0.06);
    transition: background 0.15s ease;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle:last-child {
    border-bottom: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle::before {
    display: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle:hover {
    background: rgba(255, 255, 255, 0.03);
    transform: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle:active {
    background: rgba(255, 255, 255, 0.05);
  }

  /* 勾選框樣式 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .toggle-checkbox {
    width: 22px;
    height: 22px;
    margin-right: 14px;
    border-radius: 6px;
    accent-color: #3b82f6;
    cursor: pointer;
  }

  /* 指標標籤 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .toggle-label {
    display: flex;
    align-items: center;
    gap: 10px;
    color: rgba(226, 232, 240, 0.9);
    font-size: 0.9rem;
    font-weight: 500;
    flex: 1;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .toggle-label i {
    width: 20px;
    text-align: center;
    color: rgba(148, 163, 184, 0.7);
    font-size: 0.95rem;
  }

  /* 選中狀態 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle:has(.toggle-checkbox:checked) {
    background: linear-gradient(90deg, rgba(59, 130, 246, 0.12) 0%, rgba(59, 130, 246, 0.04) 100%);
    border-color: rgba(59, 130, 246, 0.15);
    box-shadow: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle:has(.toggle-checkbox:checked) .toggle-label {
    color: rgba(226, 232, 240, 0.98);
    font-weight: 600;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle:has(.toggle-checkbox:checked) .toggle-label i {
    color: #3b82f6;
  }

  /* 說明按鈕 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .help-icon {
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(148, 163, 184, 0.2);
    color: rgba(148, 163, 184, 0.6);
    font-size: 0.75rem;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-left: auto;
    flex-shrink: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .help-icon:hover {
    background: rgba(59, 130, 246, 0.1);
    border-color: rgba(59, 130, 246, 0.3);
    color: #60a5fa;
  }

  /* 說明彈窗 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ind-help-popover {
    margin: 0 16px 12px 16px;
    padding: 14px 16px;
    background: rgba(30, 41, 59, 0.95);
    border: 1px solid rgba(100, 150, 255, 0.15);
    border-radius: 12px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ind-help-popover .help-title {
    font-size: 0.88rem;
    font-weight: 600;
    color: rgba(226, 232, 240, 0.95);
    margin-bottom: 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ind-help-popover .help-body {
    font-size: 0.8rem;
    color: rgba(226, 232, 240, 0.7);
    line-height: 1.5;
  }

  /* MA 設定區域 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ma-toggle-label {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 0.82rem;
    color: rgba(226, 232, 240, 0.8);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ma-toggle-label input[type="checkbox"] {
    width: 16px;
    height: 16px;
    accent-color: #3b82f6;
  }

  /* 參數網格 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    padding: 12px 16px;
  }

  /* 斜線壓力／支撐：單欄排列、長標籤用 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-grid.param-grid--diag-sr {
    grid-template-columns: 1fr;
    gap: 12px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-row {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-row label {
    font-size: 0.75rem;
    color: rgba(148, 163, 184, 0.8);
    font-weight: 500;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-reset-btn:hover {
    background: rgba(255, 255, 255, 0.08);
    border-color: rgba(148, 163, 184, 0.25);
  }

  :global(.ai-modal__panel),
  .ai-modal__panel {
    width: 95vw;
    max-height: 75vh;
  }

  /* 圖表區域：卡滿 header 以下空間（避免副圖下方大塊留白） */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-wrapper {
    flex: 1 1 auto;
    min-height: max(200px, 32svh);
    margin-top: 0 !important;
    display: flex;
    flex-direction: column;
    height: auto;
    overflow: hidden;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-container {
    flex: 1 1 auto;
    min-height: max(200px, var(--chart-container-min-height)) !important;
    width: 100% !important;
    max-height: none !important;
    height: 100% !important;
    box-sizing: border-box;
  }

  /* 分隔線拖曳區域加大 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-splitter {
    height: 20px;
    margin-top: -10px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-splitter::before {
    display: none;
  }
}

/* 手機版橫向模式特殊處理 - 用 max-height 判斷橫放手機 */
@media (max-height: 500px) and (orientation: landscape) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) {
    --chart-container-height: calc(100svh - 60px);
    --chart-container-min-height: max(160px, calc(100svh - 60px));
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header {
    padding: 4px 8px !important;
  }

  /* 橫向模式：工具列改為單行水平滾動 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
    flex-direction: row !important;
    flex-wrap: nowrap !important;
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
    padding: 4px 8px !important;
    gap: 8px !important;
    align-items: center !important;
  }

  /* 橫向：還原為扁平 flex 子項，避免包一層導致無法單行捲動 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-toolbar-secondary {
    display: contents !important;
  }

  /* 橫向模式：週期行恢復為 contents */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-period-row {
    display: contents !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chips-scroll {
    display: contents !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input {
    order: 0;
    flex-shrink: 0;
    padding: 4px 8px;
    font-size: 0.72rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input input {
    width: 45px;
    padding: 2px 4px;
    font-size: 0.72rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chip {
    order: 0;
    padding: 4px 8px;
    font-size: 0.68rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
    order: 0;
    width: auto;
    margin: 0 !important;
    padding: 2px;
    gap: 2px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-select-shell {
    flex: 0 0 auto;
    width: auto;
    max-width: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-icon-dropdown__trigger {
    width: auto;
    min-width: 36px;
    min-height: 34px;
    height: auto;
    padding: 4px 8px;
    font-size: 0.72rem;
    border-radius: 12px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn {
    padding: 4px 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box {
    order: 0;
    width: auto;
    margin-left: auto !important;
    padding: 4px 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box .search-input input {
    width: 70px;
    font-size: 0.72rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-btn {
    padding: 4px 10px;
    font-size: 0.72rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
    position: relative !important;
    top: auto;
    right: auto;
    flex-direction: row !important;
    order: 0;
    gap: 4px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .action-icon-btn {
    width: 30px !important;
    height: 30px !important;
    font-size: 0.8rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
    position: relative !important;
    bottom: auto;
    left: auto;
    transform: none;
    padding: 4px 10px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn {
    width: 28px;
    height: 28px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel {
    max-height: 80vh !important;
  }

  /* 橫向模式：圖表標題更小 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .legend-title-text--fullscreen {
    font-size: 0.85rem;
    padding: 2px 8px;
  }
}

/* 超小螢幕（<480px）進一步優化 */
@media (max-width: 480px) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) {
    --chart-container-height: calc(100svh - 114px);
    --chart-container-min-height: max(200px, calc(100svh - 114px));
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
    padding: 2px 4px !important;
    gap: 3px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-period-row {
    gap: 4px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input {
    padding: 3px 6px;
    font-size: 0.68rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input input {
    width: 42px;
    font-size: 0.68rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chip {
    padding: 3px 6px;
    font-size: 0.65rem;
    min-width: auto;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
    padding: 1px;
    gap: 2px;
    max-width: none;
    overflow: visible;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-icon-dropdown__trigger {
    font-size: 0.68rem;
    width: auto;
    padding: 3px 6px;
    min-height: 30px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn {
    padding: 4px 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn i {
    font-size: 0.82rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box {
    min-width: clamp(96px, 30vw, 200px);
    padding: 3px 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box .search-input input {
    width: 100%;
    min-width: 5rem;
    font-size: 0.68rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-btn {
    padding: 3px 6px;
    font-size: 0.68rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .action-icon-btn {
    width: 34px !important;
    height: 34px !important;
    min-width: 34px;
    min-height: 34px;
    font-size: 0.8rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls {
    padding: 5px 12px;
    gap: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-indicator {
    font-size: 0.7rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-btn {
    width: 32px;
    height: 32px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .legend-title-text--fullscreen {
    font-size: 0.85rem;
    padding: 3px 8px;
  }

  /* 超小螢幕控制面板精緻化 */
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel {
    max-height: 65vh !important;
    border-radius: 20px 20px 0 0 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel::before {
    width: 32px;
    height: 3px;
    top: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-header {
    padding: 16px 16px 12px 16px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-title {
    font-size: 0.92rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-title::before {
    font-size: 1rem;
    margin-right: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-close {
    width: 26px;
    height: 26px;
    font-size: 0.8rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ai-analysis-row {
    padding: 10px 14px 6px 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ai-analysis-btn {
    padding: 12px 14px;
    border-radius: 12px;
    font-size: 0.85rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .panel-section {
    margin: 0 14px 10px 14px !important;
    border-radius: 14px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-header {
    padding: 12px 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .section-label {
    font-size: 0.82rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .indicator-toggle {
    padding: 12px 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .toggle-checkbox {
    width: 20px;
    height: 20px;
    margin-right: 12px;
    border-radius: 5px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .toggle-label {
    font-size: 0.85rem;
    gap: 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .toggle-label i {
    width: 18px;
    font-size: 0.9rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .help-icon {
    width: 22px;
    height: 22px;
    font-size: 0.7rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .parameter-controls {
    padding: 10px 14px 14px 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-group label {
    font-size: 0.78rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-input {
    width: 55px;
    padding: 5px 6px;
    font-size: 0.78rem;
    border-radius: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-checkbox {
    width: 16px;
    height: 16px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-grid {
    gap: 8px;
    padding: 10px 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-row label {
    font-size: 0.72rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-reset-btn {
    padding: 8px;
    font-size: 0.75rem;
    border-radius: 8px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ma-toggle-label {
    font-size: 0.78rem;
    gap: 5px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .ma-toggle-label input[type="checkbox"] {
    width: 14px;
    height: 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-group {
    margin-bottom: 6px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-group label {
    font-size: 0.75rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .floating-panel .param-input {
    width: 50px;
    padding: 2px 4px;
    font-size: 0.75rem;
  }

  :global(.ai-modal__panel),
  .ai-modal__panel {
    width: 98vw;
    max-height: 70vh;
    border-radius: 12px;
  }

  :global(.ai-modal__header),
  .ai-modal__header {
    padding: 10px 12px;
  }

  :global(.ai-modal__title),
  .ai-modal__title {
    font-size: 0.9rem;
  }

  :global(.ai-modal__body),
  .ai-modal__body {
    padding: 10px 12px;
  }

  :global(.ai-modal__text),
  .ai-modal__text {
    font-size: 0.85rem;
    line-height: 1.5;
  }
}

/* 超超小螢幕（<380px）極限優化 */
@media (max-width: 380px) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input span {
    display: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .visible-count-input input {
    width: 55px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .period-chip {
    padding: 4px 6px;
    font-size: 0.65rem;
    min-width: 32px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box {
    min-width: clamp(88px, 28vw, 180px);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box .search-input input {
    min-width: 4.75rem;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-carousel-controls .carousel-indicator {
    font-size: 0.65rem;
  }
}

@media (max-width: 768px) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header {
    overflow: visible !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
    position: relative;
    overflow: visible !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-toolbar-secondary {
    order: 2 !important;
    display: grid !important;
    grid-template-columns: auto minmax(0, 1fr) auto;
    align-items: center !important;
    column-gap: 10px !important;
    row-gap: 8px !important;
    position: relative;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-search-cluster {
    display: contents !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
    grid-column: 1;
    grid-row: 1;
    justify-self: start;
    align-self: center;
    width: auto;
    max-width: none;
    margin: 0 !important;
    padding: 0 !important;
    gap: 8px !important;
    background: transparent;
    border: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn {
    width: 44px !important;
    min-width: 44px !important;
    height: 44px !important;
    min-height: 44px !important;
    padding: 0 !important;
    border-radius: 14px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-select-shell {
    flex: 0 0 auto;
    width: auto;
    min-width: 0;
    max-width: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-icon-dropdown__trigger {
    width: auto;
    min-width: 44px;
    height: 44px !important;
    min-height: 44px !important;
    padding: 0 9px !important;
    gap: 6px;
    font-size: 0.78rem !important;
    font-weight: 600;
    border-radius: 14px !important;
    box-sizing: border-box !important;
    color: rgba(226, 232, 240, 0.92) !important;
    background-color: rgba(15, 23, 42, 0.68) !important;
    border: 1px solid rgba(96, 165, 250, 0.22) !important;
    cursor: pointer;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline {
    order: 0 !important;
    grid-column: 2;
    grid-row: 1;
    justify-self: stretch;
    align-self: center;
    flex: 1 1 auto;
    width: 100% !important;
    max-width: 360px !important;
    min-width: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
    border: none !important;
    border-radius: 0 !important;
    background: transparent !important;
    box-shadow: none !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-query-row {
    display: grid !important;
    grid-template-columns: 38px minmax(0, 1fr) 22px;
    align-items: center !important;
    gap: 3px !important;
    width: 100% !important;
    padding: 5px !important;
    border-radius: 16px !important;
    background: rgba(15, 23, 42, 0.58) !important;
    border: 1px solid rgba(96, 165, 250, 0.14) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03) !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-mic-toggle {
    grid-column: 1;
    width: 38px !important;
    height: 38px !important;
    min-width: 38px !important;
    min-height: 38px !important;
    border-radius: 12px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .search-input {
    grid-column: 2;
    min-width: 0 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .search-input input {
    width: 100% !important;
    min-height: 38px !important;
    min-width: 0 !important;
    padding: 0 8px !important;
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    color: #ffffff !important;
    caret-color: #ffffff !important;
    border-radius: 12px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .search-input input::placeholder {
    color: rgba(255, 255, 255, 0.88) !important;
    opacity: 1 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-btn {
    grid-column: 3;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    min-width: 22px !important;
    max-width: 22px !important;
    width: 22px !important;
    min-height: 38px !important;
    padding: 0 !important;
    font-size: 0.8rem !important;
    background: transparent !important;
    border: none !important;
    border-radius: 0 !important;
    color: rgba(226, 232, 240, 0.82) !important;
    box-shadow: none !important;
    justify-self: end;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-btn:hover {
    background: transparent !important;
    border: none !important;
    color: #fff !important;
    box-shadow: none !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-toolbar-search-trigger {
    order: 0;
    grid-column: 1;
    grid-row: 1;
    justify-self: center;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 44px;
    height: 44px;
    min-width: 44px;
    min-height: 44px;
    border-radius: 14px;
    border: 1px solid rgba(96, 165, 250, 0.18);
    background: rgba(15, 23, 42, 0.68);
    color: rgba(226, 232, 240, 0.86);
    cursor: pointer;
    box-sizing: border-box;
    transition: all 0.2s ease;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-toolbar-search-trigger.is-active {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.92), rgba(147, 51, 234, 0.88));
    border-color: rgba(96, 165, 250, 0.3);
    color: #fff;
    box-shadow: 0 6px 18px rgba(59, 130, 246, 0.22);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
    order: 3 !important;
    grid-column: 3;
    grid-row: 1;
    justify-self: end;
    margin-left: 0 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions-buttons {
    gap: 6px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .action-icon-btn {
    width: 44px !important;
    height: 44px !important;
    min-width: 44px !important;
    min-height: 44px !important;
    font-size: 0.82rem !important;
    border-radius: 14px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel {
    position: absolute !important;
    top: calc(100% + 8px);
    left: 0;
    right: 0;
    z-index: 24;
    order: 4 !important;
    grid-column: auto;
    grid-row: auto;
    width: 100% !important;
    min-width: 0 !important;
    max-width: 100% !important;
    margin: 0 !important;
    padding: 12px !important;
    gap: 10px !important;
    border-radius: 18px !important;
    background: linear-gradient(180deg, rgba(12, 18, 34, 0.96), rgba(10, 16, 30, 0.92)) !important;
    border: 1px solid rgba(96, 165, 250, 0.16) !important;
    box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28) !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-panel-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    width: 100%;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-panel-copy {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-panel-title {
    font-size: 0.95rem;
    font-weight: 700;
    color: #fff;
    line-height: 1.2;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-panel-hint {
    font-size: 0.78rem;
    line-height: 1.45;
    color: rgba(191, 219, 254, 0.88);
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-panel-close {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 36px;
    height: 36px;
    min-width: 36px;
    min-height: 36px;
    border-radius: 12px;
    border: 1px solid rgba(96, 165, 250, 0.16);
    background: rgba(15, 23, 42, 0.64);
    color: rgba(226, 232, 240, 0.86);
    cursor: pointer;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-query-row {
    width: 100%;
    gap: 8px !important;
    padding: 0 !important;
    border-radius: 0 !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .search-input {
    display: flex;
    flex: 1 1 auto !important;
    min-width: 0;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .search-input input {
    width: 100% !important;
    min-width: 0 !important;
    flex: 1 1 auto !important;
    max-width: 100% !important;
    min-height: 44px !important;
    padding: 0 14px !important;
    font-size: 0.92rem !important;
    font-weight: 600 !important;
    color: #ffffff !important;
    caret-color: #ffffff !important;
    text-align: left !important;
    border-radius: 14px !important;
    background: rgba(15, 23, 42, 0.78) !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .search-input input::placeholder {
    color: rgba(255, 255, 255, 0.88) !important;
    opacity: 1 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-mic-toggle {
    width: 44px;
    height: 44px;
    min-width: 44px;
    min-height: 44px;
    border-radius: 14px;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-btn {
    min-width: 74px !important;
    min-height: 44px !important;
    padding: 0 16px !important;
    font-size: 0.92rem !important;
    border-radius: 14px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel .fullscreen-search-speech-err {
    width: 100%;
    padding: 0;
    font-size: 0.78rem;
  }
}

@media (max-height: 500px) and (orientation: landscape) {
  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .chart-header {
    overflow: visible !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .frequency-controls {
    overflow: visible !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mobile-toolbar-secondary {
    display: grid !important;
    grid-template-columns: auto minmax(0, 1fr) auto;
    align-items: center !important;
    column-gap: 10px !important;
    row-gap: 8px !important;
    position: relative;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-search-cluster {
    display: contents !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-toggle {
    grid-column: 1;
    grid-row: 1;
    justify-self: start;
    align-self: center;
    margin: 0 !important;
    padding: 0 !important;
    gap: 8px !important;
    flex-wrap: nowrap;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .mode-btn {
    width: 44px !important;
    min-width: 44px !important;
    height: 44px !important;
    min-height: 44px !important;
    padding: 0 !important;
    border-radius: 14px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-select-shell {
    flex: 0 0 auto;
    width: auto;
    min-width: 0;
    max-width: none;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .kline-mode-icon-dropdown__trigger {
    width: auto;
    min-width: 44px;
    height: 44px !important;
    min-height: 44px !important;
    padding: 0 9px !important;
    gap: 6px;
    font-size: 0.78rem !important;
    font-weight: 600;
    border-radius: 14px !important;
    box-sizing: border-box !important;
    color: rgba(226, 232, 240, 0.92) !important;
    background-color: rgba(15, 23, 42, 0.68) !important;
    border: 1px solid rgba(96, 165, 250, 0.22) !important;
    cursor: pointer;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline {
    grid-column: 2;
    grid-row: 1;
    justify-self: center;
    align-self: center;
    flex: 1 1 auto;
    width: 100% !important;
    max-width: 360px !important;
    min-width: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
    border: none !important;
    border-radius: 0 !important;
    background: transparent !important;
    box-shadow: none !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-query-row {
    display: grid !important;
    grid-template-columns: 38px minmax(0, 1fr) 22px;
    align-items: center !important;
    gap: 3px !important;
    width: 100% !important;
    padding: 5px !important;
    border-radius: 16px !important;
    background: rgba(15, 23, 42, 0.58) !important;
    border: 1px solid rgba(96, 165, 250, 0.14) !important;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03) !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-mic-toggle {
    grid-column: 1;
    width: 38px !important;
    height: 38px !important;
    min-width: 38px !important;
    min-height: 38px !important;
    border-radius: 12px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .search-input {
    grid-column: 2;
    min-width: 0 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .search-input input {
    width: 100% !important;
    min-width: 0 !important;
    min-height: 38px !important;
    padding: 0 8px !important;
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    color: #ffffff !important;
    caret-color: #ffffff !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .search-input input::placeholder {
    color: rgba(255, 255, 255, 0.88) !important;
    opacity: 1 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--inline .fullscreen-search-btn {
    grid-column: 3;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    width: 22px !important;
    min-width: 22px !important;
    height: 38px !important;
    min-height: 38px !important;
    padding: 0 !important;
    font-size: 0.8rem !important;
    background: transparent !important;
    border: none !important;
    border-radius: 0 !important;
    color: rgba(226, 232, 240, 0.82) !important;
    box-shadow: none !important;
    justify-self: end;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions {
    order: 3 !important;
    grid-column: 3;
    grid-row: 1;
    justify-self: end;
    margin-left: 0 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-actions-buttons {
    display: flex !important;
    align-items: center !important;
    gap: 0 !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .action-icon-btn {
    width: 44px !important;
    height: 44px !important;
    min-width: 44px !important;
    min-height: 44px !important;
    border-radius: 14px !important;
  }

  .stock-chart:is(:fullscreen, .is-fullscreen, :-webkit-full-screen) .fullscreen-search-box--mobile-panel {
    position: absolute !important;
    top: calc(100% + 8px);
    left: 0;
    right: 0;
    z-index: 24;
    order: 4 !important;
    grid-column: auto;
    grid-row: auto;
    width: 100% !important;
    min-width: 0 !important;
    max-width: 100% !important;
    margin: 0 !important;
  }
}
</style>
