async function request(path, options = {}) {
  const res = await fetch(path, {
    headers: { Accept: 'application/json', ...(options.headers || {}) },
    ...options,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(text || `HTTP ${res.status}`)
  }
  return res.json()
}

export const api = {
  health: () => request('/api/health'),
  meta: () => request('/api/meta'),
  stats: () => request('/api/stats'),
  search: (q) => request(`/api/symbols/search?q=${encodeURIComponent(q)}&limit=16`),
  overview: (code) => request(`/api/reports/${encodeURIComponent(code)}/overview`),
  statement: (code, kind, limit = 8) =>
    request(`/api/reports/${encodeURIComponent(code)}/${kind}?limit=${limit}`),
}
