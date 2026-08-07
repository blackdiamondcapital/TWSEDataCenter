import { ref, computed, onUnmounted } from 'vue'

/** Strip spoken command prefixes before name/code extraction. */
export function preprocessSpeechTranscriptForStock(text) {
  let t = String(text || '').trim()
  t = t.replace(/^(?:\u8acb|\u5e6e\u6211|\u6211\u8981|\u6211\u60f3)+[\uff0c,\u3002.\s\u3000]*/u, '')
  t = t.replace(/^(?:\u67e5\u8a62|\u67e5\u4e00\u4e0b|\u67e5|\u641c\u5c0b|\u627e)+[\uff0c,\u3002.\s\u3000]*/giu, '')
  return t.trim()
}

/** Map spoken Mandarin digit chars to Arabic (e.g. 二三三零 -> 2330). */
const CN_DIGIT = {
  '\u96f6': '0',
  '\u3007': '0',
  '\u4e00': '1',
  '\u58f9': '1',
  '\u4e8c': '2',
  '\u8cb3': '2',
  '\u4e24': '2',
  '\u5169': '2',
  '\u4e09': '3',
  '\u53c3': '3',
  '\u56db': '4',
  '\u4e94': '5',
  '\u516d': '6',
  '\u4e03': '7',
  '\u516b': '8',
  '\u4e5d': '9',
}

/**
 * Extract 4-5 digit Taiwan stock/ETF codes from speech text; keep full string for name lookup.
 * @param {string} text
 * @returns {{ codes: string[], cleanText: string }}
 */
export function extractStockCodesFromSpeech(text) {
  const raw = String(text || '').trim()
  const codes = []
  const seen = new Set()
  const add = (sym) => {
    const u = String(sym || '').toUpperCase().replace(/\.(TW|TWO)$/i, '')
    if (!/^\d{4,5}$/.test(u)) return
    if (seen.has(u)) return
    seen.add(u)
    codes.push(u)
  }

  if (!raw) return { codes: [], cleanText: '' }

  for (const m of raw.matchAll(/\d{4,5}/g)) {
    add(m[0])
  }

  const compact = raw.replace(/\s/g, '')
  let buf = ''
  const flushCn = () => {
    if (buf.length >= 4) add(buf)
    buf = ''
  }
  for (const ch of compact) {
    if (CN_DIGIT[ch] !== undefined) buf += CN_DIGIT[ch]
    else flushCn()
  }
  flushCn()

  return { codes, cleanText: raw }
}

export function useWatchlistSpeechRecognition() {
  const listening = ref(false)
  const errorMessage = ref('')
  const supported = computed(() => {
    if (typeof window === 'undefined') return false
    return !!(window.SpeechRecognition || window.webkitSpeechRecognition)
  })

  let recognition = null
  let wantListening = false
  let continuousMode = false
  let restartTimer = null

  function clearRestartTimer() {
    if (restartTimer) {
      clearTimeout(restartTimer)
      restartTimer = null
    }
  }

  function getInstance() {
    if (recognition) return recognition
    const Ctor = typeof window !== 'undefined' ? window.SpeechRecognition || window.webkitSpeechRecognition : null
    if (!Ctor) return null
    const r = new Ctor()
    r.lang = 'zh-TW'
    r.continuous = false
    r.interimResults = false
    r.maxAlternatives = 1
    recognition = r
    return r
  }

  function scheduleRestart() {
    clearRestartTimer()
    if (!wantListening || !recognition) return
    restartTimer = setTimeout(() => {
      restartTimer = null
      if (!wantListening || !recognition) return
      try {
        listening.value = true
        recognition.start()
      } catch (_) {
        scheduleRestart()
      }
    }, 120)
  }

  function start(onFinal, options = {}) {
    errorMessage.value = ''
    continuousMode = !!options.continuous
    wantListening = true
    clearRestartTimer()

    const r = getInstance()
    if (!r) {
      wantListening = false
      errorMessage.value = '\u6b64\u700f\u89bd\u5668\u4e0d\u652f\u63f4\u8a9e\u97f3\u8fa8\u8b58\uff0c\u5efa\u8b70\u4f7f\u7528 Chrome \u6216 Edge'
      return
    }

    r.continuous = continuousMode
    r.onresult = (ev) => {
      const item = ev.results[ev.results.length - 1]
      const text = (item && item[0] && item[0].transcript) ? String(item[0].transcript).trim() : ''
      if (text && typeof onFinal === 'function') onFinal(text)
    }

    r.onerror = (ev) => {
      const code = ev.error || ''
      if (code === 'aborted') return
      if (continuousMode && (code === 'no-speech' || code === 'audio-capture')) {
        return
      }
      wantListening = false
      listening.value = false
      if (code === 'not-allowed' || code === 'service-not-allowed') {
        errorMessage.value = '\u7121\u6cd5\u4f7f\u7528\u9ea5\u514b\u98a8\uff0c\u8acb\u65bc\u700f\u89bd\u5668\u8a2d\u5b9a\u4e2d\u5141\u8a31\u6b0a\u9650'
      } else if (code === 'no-speech') {
        errorMessage.value = '\u672a\u5075\u6e2c\u5230\u8a9e\u97f3\uff0c\u8acb\u518d\u8a66\u4e26\u9760\u8fd1\u9ea5\u514b\u98a8'
      } else {
        errorMessage.value = '\u8a9e\u97f3\u8fa8\u8b58\u5931\u6557\uff0c\u8acb\u6539\u7528\u9375\u76e4\u8f38\u5165'
      }
    }

    r.onend = () => {
      if (wantListening && continuousMode) {
        scheduleRestart()
        return
      }
      listening.value = false
    }

    try {
      listening.value = true
      r.start()
    } catch (_) {
      if (continuousMode && wantListening) {
        scheduleRestart()
        return
      }
      wantListening = false
      listening.value = false
      errorMessage.value = '\u7121\u6cd5\u555f\u52d5\u8a9e\u97f3\u8fa8\u8b58'
    }
  }

  function stop() {
    wantListening = false
    continuousMode = false
    clearRestartTimer()
    if (!recognition) {
      listening.value = false
      return
    }
    try {
      recognition.stop()
    } catch (_) {}
    listening.value = false
  }

  onUnmounted(() => {
    wantListening = false
    clearRestartTimer()
    if (!recognition) return
    try {
      recognition.abort()
    } catch (_) {}
    recognition = null
  })

  return {
    listening,
    errorMessage,
    supported,
    start,
    stop,
  }
}
