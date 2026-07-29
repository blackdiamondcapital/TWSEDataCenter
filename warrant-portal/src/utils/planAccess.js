/** 權證雷達：整包比照主站圖表，不套 Lite／Pro 付費牆 */

export const TRIAL_DAYS = 14

export const LITE_FREE_PRO_UPGRADE_MESSAGE =
  '此功能需訂閱 Pro 版本才能使用，請前往方案頁升級。'

export function normalizePlanKey(plan) {
  const key = String(plan ?? 'free').trim().toLowerCase()
  if (key === 'admin') return 'admin'
  if (key === 'enterprise' || key === 'prime') return 'prime'
  if (key === 'pro') return 'pro'
  if (key === 'lite_free') return 'lite_free'
  return 'free'
}

export function resolveUserAccess(user) {
  void user
  return {
    tier: 'pro',
    effectivePlan: 'pro',
    isLiteFree: false,
    isTrialActive: false,
    trialExpired: false,
    trialEndsAt: null,
    trialDaysLeft: null,
  }
}

export function getEffectivePlanKey() {
  return 'pro'
}

/** 權證站解鎖神奇 K／階梯線 */
export function canUseMagicKAndLadder() {
  return true
}

/** 權證站解鎖 Pro 圖表功能 */
export function canUseProChartFeatures() {
  return true
}
