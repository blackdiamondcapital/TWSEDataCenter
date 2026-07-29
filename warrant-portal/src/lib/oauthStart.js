/**
 * Same Google OAuth start URL as www.quantgems.com
 * (Passport redirect on QuantGems Node backend / Render).
 */
export function buildOAuthStartUrl(provider = 'google') {
  const key = String(provider || 'google').toLowerCase() === 'facebook' ? 'facebook' : 'google'
  const backend = String(import.meta.env.VITE_BACKEND_URL || '')
    .trim()
    .replace(/\/$/, '')
  const siteOrigin =
    typeof window !== 'undefined' && window.location?.origin
      ? String(window.location.origin).replace(/\/$/, '')
      : String(import.meta.env.VITE_SITE_URL || 'https://warrant.quantgems.com')
          .trim()
          .replace(/\/$/, '')

  const base = /^https?:\/\//i.test(backend) ? backend : siteOrigin || ''
  const url = new URL(`${base}/api/auth/${key}`)
  if (siteOrigin) url.searchParams.set('redirect', siteOrigin)
  return url.toString()
}
