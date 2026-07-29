import { computed } from 'vue'
import { useAuth as useWarrantAuth } from '../lib/auth.js'

/**
 * 相容主站 StockChartECharts 的 useAuth。
 * 權證站圖表功能已全面解鎖；此處僅提供 user／token 狀態。
 */
export function useAuth() {
  const auth = useWarrantAuth()
  const user = computed(() => {
    const u = auth.user?.value
    if (!u) {
      // 未登入也視為可使用完整圖表（1A）
      return { plan: 'pro', subscription_status: 'active' }
    }
    return { ...u, plan: u.plan || 'pro' }
  })
  return {
    user,
    token: auth.token,
    isAuthenticated: auth.isAuthenticated,
    loading: auth.loading,
    error: auth.error,
    logout: auth.logout,
    fetchCurrentUser: auth.fetchCurrentUser,
  }
}
