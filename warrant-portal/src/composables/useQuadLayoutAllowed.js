import { ref, onMounted, onUnmounted } from 'vue'

/** 與圖表工具列、全螢幕手機佈局一致：此寬度以下不顯示四宮格 */
export const QUAD_DISABLE_MAX_PX = 768

/**
 * 是否允許顯示／進入全螢幕四分割與內嵌四宮格（寬度 > 768 為 true）
 * @returns {boolean}
 */
export function isQuadLayoutAvailable() {
  if (typeof window === 'undefined') return true
  try {
    return !window.matchMedia(`(max-width: ${QUAD_DISABLE_MAX_PX}px)`).matches
  } catch {
    return true
  }
}

/**
 * 響應式：是否允許四宮格（用於 v-if 隱藏按鈕）
 * @returns {{ quadLayoutAvailable: import('vue').Ref<boolean> }}
 */
export function useQuadLayoutAvailable() {
  const quadLayoutAvailable = ref(isQuadLayoutAvailable())
  function update() {
    quadLayoutAvailable.value = isQuadLayoutAvailable()
  }
  let mql = null
  onMounted(() => {
    update()
    mql = window.matchMedia(`(max-width: ${QUAD_DISABLE_MAX_PX}px)`)
    mql.addEventListener('change', update)
    window.addEventListener('resize', update)
  })
  onUnmounted(() => {
    try {
      mql?.removeEventListener('change', update)
    } catch {
      // ignore
    }
    window.removeEventListener('resize', update)
  })
  return { quadLayoutAvailable }
}
