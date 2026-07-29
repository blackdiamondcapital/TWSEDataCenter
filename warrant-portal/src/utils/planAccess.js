/** 新用戶 Pro 試用天數（方案 B） */
export const TRIAL_DAYS = 14

export function normalizePlanKey(plan) {
  const key = String(plan ?? 'free').trim().toLowerCase()
  if (key === 'admin') return 'admin'
  if (key === 'enterprise' || key === 'prime') return 'prime'
  if (key === 'pro') return 'pro'
  if (key === 'lite_free') return 'lite_free'
  return 'free'
}

export function parseTrialEndDate(value) {
  if (!value) return null
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? null : d
}

export function resolveUserAccess(user) {
  if (!user || typeof user !== 'object') {
    return {
      tier: 'guest',
      effectivePlan: 'free',
      isLiteFree: false,
      isTrialActive: false,
      trialExpired: false,
      trialEndsAt: null,
      trialDaysLeft: null,
    }
  }

  const rawPlan = normalizePlanKey(user.plan)
  const sub = String(user.subscription_status ?? '').trim().toLowerCase()

  if (rawPlan === 'admin' || rawPlan === 'prime' || rawPlan === 'pro') {
    return {
      tier: rawPlan,
      effectivePlan: rawPlan,
      isLiteFree: false,
      isTrialActive: false,
      trialExpired: false,
      trialEndsAt: null,
      trialDaysLeft: null,
    }
  }

  if (rawPlan === 'free' && sub === 'active') {
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

  const trialEnd = parseTrialEndDate(user.trial_end_date)
  const now = new Date()

  if (trialEnd) {
    if (trialEnd > now) {
      const msLeft = trialEnd.getTime() - now.getTime()
      const daysLeft = Math.max(0, Math.ceil(msLeft / (24 * 60 * 60 * 1000)))
      return {
        tier: 'trial_pro',
        effectivePlan: 'pro',
        isLiteFree: false,
        isTrialActive: true,
        trialExpired: false,
        trialEndsAt: trialEnd,
        trialDaysLeft: daysLeft,
      }
    }

    return {
      tier: 'lite_free',
      effectivePlan: 'lite_free',
      isLiteFree: true,
      isTrialActive: false,
      trialExpired: true,
      trialEndsAt: trialEnd,
      trialDaysLeft: 0,
    }
  }

  return {
    tier: 'free',
    effectivePlan: 'free',
    isLiteFree: false,
    isTrialActive: false,
    trialExpired: false,
    trialEndsAt: null,
    trialDaysLeft: null,
  }
}

/** 與 auth store 一致：試用中 / 付費訂閱視同 Pro */
export function getEffectivePlanKey(user) {
  return resolveUserAccess(user).effectivePlan
}

/** 舊 Free 用戶登入後可選試用 Pro 或繼續免費版 */
export function isEligibleForLegacyTrialOffer(user) {
  if (!user || typeof user !== 'object') return false

  const rawPlan = normalizePlanKey(user.plan)
  const sub = String(user.subscription_status ?? '').trim().toLowerCase()

  if (rawPlan === 'admin' || rawPlan === 'prime' || rawPlan === 'pro') return false
  if (rawPlan === 'free' && sub === 'active') return false
  if (parseTrialEndDate(user.trial_end_date)) return false
  if (user.trial_used === true || user.trial_used === 't' || user.trial_used === 1) return false

  const access = resolveUserAccess(user)
  return access.tier === 'free' && access.effectivePlan === 'free'
}

/** 用量／排行：lite_free 視同 free 上限 */
export function getQuotaPlanKey(user) {
  const key = getEffectivePlanKey(user)
  return key === 'lite_free' ? 'free' : key
}

/** 精簡 Free：報酬排行／自選股輪播與自選股上限 */
export const LITE_FREE_CAROUSEL_LIMIT = 30
export const LITE_FREE_WATCHLIST_MAX = 30

export const LITE_FREE_PRO_UPGRADE_MESSAGE =
  '此功能需訂閱 Pro 版本才能使用，請前往方案頁升級。'

export const SCREENER_PLAN_REQUIRED_MESSAGE =
  '選股策略需訂閱 Pro 版本才能使用，請前往方案頁升級。'

/** Pro 試用中／付費 Pro／Prime／Admin 才可使用選股 */
export function canUseScreener(user) {
  const plan = getEffectivePlanKey(user)
  return plan === 'pro' || plan === 'prime' || plan === 'admin'
}

/** Pro 試用中／付費 Pro／Prime／Admin 才可使用神奇 K 線與階梯線 */
export function canUseMagicKAndLadder(user) {
  const plan = getEffectivePlanKey(user)
  return plan === 'pro' || plan === 'prime' || plan === 'admin'
}

export const LITE_FREE_ALLOWED_VIEWS = new Set([
  'overview',
  'ranking',
  'watchlist',
  'screener',
  'industryRotation',
  'aiResearch',
  'pricing',
  'profile',
  'blog',
  'productIntro',
  'feedback',
  'faq',
])

export function isLiteFreeViewAllowed(view) {
  return LITE_FREE_ALLOWED_VIEWS.has(String(view || '').trim())
}

export function getOptimizeComboLimitForPlan(planKey) {
  const plan = normalizePlanKey(planKey)
  if (plan === 'prime' || plan === 'admin') return 240
  if (plan === 'pro') return 20
  return 10
}

export function getOptimizeComboLimitForUser(user) {
  return getOptimizeComboLimitForPlan(getEffectivePlanKey(user))
}

export function canUsePrimeFeatures(user) {
  const plan = getEffectivePlanKey(user)
  return plan === 'prime' || plan === 'admin'
}

export function canUseIndustryRotation(user) {
  const plan = getEffectivePlanKey(user)
  return plan === 'pro' || plan === 'prime' || plan === 'admin'
}

export function canUseFullMemberFeatures(user) {
  const access = resolveUserAccess(user)
  return !access.isLiteFree
}

/** 所有 Free（含 legacy／lite_free）不可使用 Pro 技術指標 */
export function canUseProChartFeatures(user) {
  const plan = getEffectivePlanKey(user)
  return plan === 'pro' || plan === 'prime' || plan === 'admin'
}

export function getWatchlistLimitsForUser(user) {
  const access = resolveUserAccess(user)
  if (access.isLiteFree) {
    return { groupCount: 1, maxPerGroup: LITE_FREE_WATCHLIST_MAX }
  }
  const plan = getEffectivePlanKey(user)
  if (plan === 'admin' || plan === 'prime') return { groupCount: 10, maxPerGroup: 100 }
  if (plan === 'pro') return { groupCount: 5, maxPerGroup: 100 }
  return { groupCount: 5, maxPerGroup: 20 }
}

export function getCarouselLimitForUser(user, context = 'ranking') {
  const access = resolveUserAccess(user)
  const ctx = String(context || 'ranking').toLowerCase()
  const plan = getEffectivePlanKey(user)
  if (ctx === 'screener' && (plan === 'free' || plan === 'lite_free')) {
    return 0
  }
  if (access.isLiteFree) {
    return LITE_FREE_CAROUSEL_LIMIT
  }
  if (plan === 'admin' || plan === 'prime') {
    return ctx === 'watchlist' ? 1000 : 300
  }
  if (plan === 'pro') return 200
  return 30
}
