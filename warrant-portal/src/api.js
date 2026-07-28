import axios from 'axios'

const inferDefaultBase = () => {
  if (import.meta.env.VITE_API_BASE) return import.meta.env.VITE_API_BASE
  return '/api'
}

const api = axios.create({
  baseURL: inferDefaultBase(),
  timeout: 120000,
})

function unwrap(resp, fallback = '請求失敗') {
  if (!resp.data?.success) throw new Error(resp.data?.error || fallback)
  return resp.data
}

export async function fetchPortalStats() {
  return unwrap(await api.get('/warrants/portal/stats'), '讀取統計失敗')
}

export async function fetchMasterSearch(params = {}) {
  return unwrap(await api.get('/warrants/portal/master', { params }), '主檔查詢失敗')
}

export async function fetchMasterDetail(code) {
  return unwrap(await api.get(`/warrants/portal/master/${encodeURIComponent(code)}`), '主檔詳情失敗')
}

export async function fetchDates(limit = 120, market = 'both') {
  const data = unwrap(await api.get('/warrants/dates', { params: { limit, market } }), '載入日期失敗')
  return data.dates || []
}

export async function fetchRankings({ date, metric = 'turnover', market = 'both', limit = 50 } = {}) {
  return unwrap(
    await api.get('/warrants/rankings', { params: { date, metric, market, limit } }),
    '排行榜查詢失敗',
  )
}

export async function fetchTimeseries({ code, limitDays = 90 } = {}) {
  return unwrap(
    await api.get('/warrants/timeseries', { params: { code, limitDays } }),
    '時間序列查詢失敗',
  )
}

export async function importLatestWarrants() {
  try {
    return unwrap(await api.post('/warrants/import-latest'), '匯入失敗')
  } catch (err) {
    const status = err?.response?.status
    const data = err?.response?.data
    if (status === 409 && data?.inProgress) return data
    throw err
  }
}
