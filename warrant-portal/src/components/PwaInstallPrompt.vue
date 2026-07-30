<template>
  <Transition name="pwa-slide">
    <div
      v-if="needRefresh"
      class="pwa-install-banner pwa-update-banner"
      role="status"
      aria-live="polite"
    >
      <div class="pwa-install-icon">
        <img src="/pwa-icon-192.png" alt="權證雷達" width="40" height="40" />
      </div>
      <div class="pwa-install-text">
        <div class="pwa-install-title">發現新版本</div>
        <div class="pwa-install-desc">
          {{ updating ? '正在更新…' : autoReloadSec > 0 ? `${autoReloadSec} 秒後自動重整` : '請更新以載入最新功能' }}
        </div>
      </div>
      <div class="pwa-install-actions">
        <button class="pwa-btn-install" type="button" :disabled="updating" @click="applyUpdate">
          {{ updating ? '更新中' : '立即更新' }}
        </button>
      </div>
    </div>
  </Transition>

  <Transition name="pwa-slide">
    <div v-if="showPrompt && !needRefresh" class="pwa-install-banner">
      <div class="pwa-install-icon">
        <img src="/pwa-icon-192.png" alt="權證雷達" width="40" height="40" />
      </div>
      <div class="pwa-install-text">
        <div class="pwa-install-title">安裝權證雷達</div>
        <div class="pwa-install-desc">加入主畫面，快速開啟 QuantGems 權證工具</div>
      </div>
      <div class="pwa-install-actions">
        <button class="pwa-btn-install" type="button" @click="install">安裝</button>
        <button class="pwa-btn-dismiss" type="button" aria-label="關閉" @click="dismiss">✕</button>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted } from 'vue'
import { useRegisterSW } from 'virtual:pwa-register/vue'

const showPrompt = ref(false)
const updating = ref(false)
const autoReloadSec = ref(0)
let deferredPrompt = null
let swRegistration = null
let updatePollTimer = null
let autoReloadTimer = null
let autoReloadCountdown = null
let lastUpdateCheckAt = 0

const AUTO_RELOAD_DELAY_SEC = 5
const UPDATE_POLL_MS = 60 * 1000
const UPDATE_CHECK_MIN_GAP_MS = 15 * 1000
const DISMISS_KEY = 'warrant-pwa-install-dismissed'

const { needRefresh, updateServiceWorker } = useRegisterSW({
  immediate: true,
  onRegisteredSW(_swUrl, registration) {
    swRegistration = registration || null
    startUpdatePolling()
    checkForSwUpdate(true)
  },
  onRegisterError(error) {
    console.warn('[PWA] SW registration error', error)
  },
})

function checkForSwUpdate(force = false) {
  if (!swRegistration?.update) return
  const now = Date.now()
  if (!force && now - lastUpdateCheckAt < UPDATE_CHECK_MIN_GAP_MS) return
  lastUpdateCheckAt = now
  try {
    swRegistration.update()
  } catch {
    /* ignore */
  }
}

function startUpdatePolling() {
  stopUpdatePolling()
  if (!swRegistration?.update) return
  updatePollTimer = window.setInterval(() => {
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return
    checkForSwUpdate(true)
  }, UPDATE_POLL_MS)
}

function stopUpdatePolling() {
  if (updatePollTimer != null) {
    clearInterval(updatePollTimer)
    updatePollTimer = null
  }
}

function clearAutoReload() {
  if (autoReloadTimer != null) {
    clearTimeout(autoReloadTimer)
    autoReloadTimer = null
  }
  if (autoReloadCountdown != null) {
    clearInterval(autoReloadCountdown)
    autoReloadCountdown = null
  }
  autoReloadSec.value = 0
}

async function applyUpdate() {
  if (updating.value) return
  updating.value = true
  clearAutoReload()
  try {
    await updateServiceWorker(true)
  } catch (e) {
    console.warn('[PWA] updateServiceWorker failed, fallback reload', e)
    window.location.reload()
  }
}

function scheduleAutoReload() {
  clearAutoReload()
  autoReloadSec.value = AUTO_RELOAD_DELAY_SEC
  autoReloadCountdown = window.setInterval(() => {
    autoReloadSec.value = Math.max(0, autoReloadSec.value - 1)
  }, 1000)
  autoReloadTimer = window.setTimeout(() => {
    applyUpdate()
  }, AUTO_RELOAD_DELAY_SEC * 1000)
}

watch(needRefresh, (v) => {
  if (v) scheduleAutoReload()
  else clearAutoReload()
})

function handleVisibilityChange() {
  if (document.visibilityState !== 'visible') return
  checkForSwUpdate()
}

function handleWindowFocus() {
  checkForSwUpdate()
}

function handlePageShow(event) {
  if (event?.persisted) checkForSwUpdate(true)
  else checkForSwUpdate()
}

function handleControllerChange() {
  if (updating.value) return
  updating.value = true
  try {
    window.location.reload()
  } catch {
    /* ignore */
  }
}

function handleBeforeInstallPrompt(e) {
  e.preventDefault()
  deferredPrompt = e
  const dismissed = localStorage.getItem(DISMISS_KEY)
  if (!dismissed) {
    setTimeout(() => {
      showPrompt.value = true
    }, 3000)
  }
}

async function install() {
  if (!deferredPrompt) return
  deferredPrompt.prompt()
  const { outcome } = await deferredPrompt.userChoice
  if (outcome === 'accepted') {
    showPrompt.value = false
    localStorage.setItem(DISMISS_KEY, '1')
  }
  deferredPrompt = null
}

function dismiss() {
  showPrompt.value = false
  localStorage.setItem(DISMISS_KEY, '1')
}

onMounted(() => {
  window.addEventListener('beforeinstallprompt', handleBeforeInstallPrompt)
  document.addEventListener('visibilitychange', handleVisibilityChange)
  window.addEventListener('focus', handleWindowFocus)
  window.addEventListener('pageshow', handlePageShow)
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.addEventListener('controllerchange', handleControllerChange)
  }
  if (needRefresh.value) scheduleAutoReload()
})

onUnmounted(() => {
  window.removeEventListener('beforeinstallprompt', handleBeforeInstallPrompt)
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  window.removeEventListener('focus', handleWindowFocus)
  window.removeEventListener('pageshow', handlePageShow)
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.removeEventListener('controllerchange', handleControllerChange)
  }
  stopUpdatePolling()
  clearAutoReload()
})
</script>

<style scoped>
.pwa-install-banner {
  position: fixed;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 10060;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 16px;
  border-radius: 14px;
  background: rgba(8, 14, 22, 0.96);
  border: 1px solid rgba(0, 212, 255, 0.28);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5);
  min-width: 300px;
  max-width: 420px;
  width: calc(100vw - 40px);
  backdrop-filter: blur(10px);
}

.pwa-update-banner {
  border-color: rgba(14, 165, 233, 0.55);
  box-shadow: 0 8px 32px rgba(14, 165, 233, 0.22);
}

.pwa-install-icon img {
  border-radius: 10px;
  flex-shrink: 0;
}

.pwa-install-text {
  flex: 1;
  min-width: 0;
}

.pwa-install-title {
  font-weight: 700;
  font-size: 14px;
  color: #f1f5f9;
}

.pwa-install-desc {
  font-size: 12px;
  color: #94a3b8;
  margin-top: 2px;
}

.pwa-install-actions {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-shrink: 0;
}

.pwa-btn-install {
  background: linear-gradient(135deg, #0ea5e9, #0284c7);
  color: #fff;
  border: none;
  border-radius: 8px;
  padding: 7px 16px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
}

.pwa-btn-install:hover:not(:disabled) {
  opacity: 0.9;
}

.pwa-btn-install:disabled {
  opacity: 0.7;
  cursor: wait;
}

.pwa-btn-dismiss {
  background: transparent;
  color: #64748b;
  border: none;
  font-size: 16px;
  cursor: pointer;
  padding: 4px 6px;
  border-radius: 6px;
  line-height: 1;
}

.pwa-btn-dismiss:hover {
  color: #94a3b8;
}

.pwa-slide-enter-active,
.pwa-slide-leave-active {
  transition: all 0.35s cubic-bezier(0.4, 0, 0.2, 1);
}

.pwa-slide-enter-from,
.pwa-slide-leave-to {
  opacity: 0;
  transform: translateX(-50%) translateY(20px);
}
</style>
