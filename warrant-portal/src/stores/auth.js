import { useAuth as useWarrantAuth } from '../lib/auth.js'

/**
 * 相容主站 StockChartECharts 的 useAuth。
 * 傳入真實登入使用者，供 planAccess 判定 Pro／Lite 指標權限。
 */
export function useAuth() {
  const auth = useWarrantAuth()
  return {
    user: auth.user,
    token: auth.token,
    isAuthenticated: auth.isAuthenticated,
    loading: auth.loading,
    error: auth.error,
    logout: auth.logout,
    fetchCurrentUser: auth.fetchCurrentUser,
  }
}
