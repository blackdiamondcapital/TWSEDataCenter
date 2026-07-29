import { ref, computed } from 'vue'

/** Same storage keys as www.quantgems.com */
const TOKEN_KEY = 'quantgem_auth_token'
const USER_KEY = 'quantgem_user'

function authApiBase() {
  const backend = String(import.meta.env.VITE_BACKEND_URL || '')
    .trim()
    .replace(/\/$/, '')
  if (/^https?:\/\//i.test(backend)) return `${backend}/api`
  const explicit = String(import.meta.env.VITE_AUTH_API_BASE || '')
    .trim()
    .replace(/\/$/, '')
  if (explicit) return explicit
  return '/api'
}

function readStoredUser() {
  try {
    const raw = localStorage.getItem(USER_KEY)
    if (!raw) return null
    return JSON.parse(raw)
  } catch {
    try {
      localStorage.removeItem(USER_KEY)
    } catch {}
    return null
  }
}

const token = ref(localStorage.getItem(TOKEN_KEY) || null)
const user = ref(token.value ? readStoredUser() : null)
const loading = ref(false)
const error = ref(null)

if (!token.value) {
  try {
    user.value = null
    localStorage.removeItem(USER_KEY)
  } catch {}
}

const isAuthenticated = computed(() => !!token.value)
const displayName = computed(() => {
  const u = user.value
  if (!u) return ''
  return u.username || u.full_name || u.email || '會員'
})
const planLabel = computed(() => {
  const p = String(user.value?.plan || 'free').toLowerCase()
  if (p === 'admin') return 'Admin'
  if (p === 'prime' || p === 'enterprise') return 'Prime'
  if (p === 'pro') return 'Pro'
  return 'Free'
})

/** 與主站一致：role === 'admin' 才是管理員 */
const isAdmin = computed(() => {
  const u = user.value
  if (!u || typeof u !== 'object') return false
  return String(u.role ?? '').trim().toLowerCase() === 'admin'
})

function setToken(newToken) {
  token.value = newToken || null
  if (newToken) localStorage.setItem(TOKEN_KEY, newToken)
  else localStorage.removeItem(TOKEN_KEY)
}

function setUser(newUser) {
  user.value = newUser || null
  if (newUser) localStorage.setItem(USER_KEY, JSON.stringify(newUser))
  else localStorage.removeItem(USER_KEY)
}

function getAuthHeaders() {
  if (!token.value) return {}
  return { Authorization: `Bearer ${token.value}` }
}

async function fetchCurrentUser() {
  if (!token.value) return { success: false, error: '未登入' }
  loading.value = true
  error.value = null
  try {
    const response = await fetch(`${authApiBase()}/auth/me`, {
      headers: getAuthHeaders(),
    })
    const data = await response.json().catch(() => ({}))
    if (!response.ok) {
      if (response.status === 401) {
        setToken(null)
        setUser(null)
      }
      throw new Error(data.message || '獲取用戶資訊失敗')
    }
    setUser(data.data?.user || null)
    return { success: true, data: data.data?.user }
  } catch (err) {
    error.value = err.message
    return { success: false, error: err.message }
  } finally {
    loading.value = false
  }
}

async function logout() {
  try {
    if (token.value) {
      await fetch(`${authApiBase()}/auth/logout`, {
        method: 'POST',
        headers: {
          ...getAuthHeaders(),
          'Content-Type': 'application/json',
        },
      }).catch(() => {})
    }
  } finally {
    setUser(null)
    setToken(null)
  }
}

function consumeOAuthCallbackFromUrl() {
  if (typeof window === 'undefined') return { token: null, error: null }
  const url = new URL(window.location.href)
  let oauthToken = url.searchParams.get('token')
  let oauthError = url.searchParams.get('error')
  let provider = url.searchParams.get('provider')

  // Non-product handoff may put token in hash
  if (!oauthToken && url.hash) {
    const hash = new URLSearchParams(url.hash.replace(/^#/, ''))
    oauthToken = hash.get('token') || oauthToken
    oauthError = hash.get('error') || oauthError
    provider = hash.get('provider') || provider
  }

  if (oauthToken || oauthError) {
    url.searchParams.delete('token')
    url.searchParams.delete('error')
    url.searchParams.delete('provider')
    url.hash = ''
    window.history.replaceState({}, '', `${url.pathname}${url.search}`)
  }

  return { token: oauthToken, error: oauthError, provider }
}

export function useAuth() {
  return {
    token,
    user,
    loading,
    error,
    isAuthenticated,
    displayName,
    planLabel,
    isAdmin,
    setToken,
    setUser,
    getAuthHeaders,
    fetchCurrentUser,
    logout,
    consumeOAuthCallbackFromUrl,
  }
}
