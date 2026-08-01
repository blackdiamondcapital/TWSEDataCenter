<script setup>
import { onMounted, ref } from 'vue'
import { api } from './api'
import LandingHero from './components/LandingHero.vue'
import EngineWorkspace from './components/EngineWorkspace.vue'

const view = ref('landing')
const stats = ref(null)
const ready = ref(false)
const seedCode = ref('2330')

onMounted(async () => {
  try {
    const [s] = await Promise.all([api.stats(), api.health()])
    stats.value = s
    ready.value = true
  } catch {
    // API 未啟動時仍可進前端，稍後載入會顯示錯誤
    ready.value = true
  }
})

function enter(demo = true) {
  seedCode.value = demo ? '2330' : '2330'
  view.value = 'engine'
}
</script>

<template>
  <LandingHero
    v-if="view === 'landing'"
    :stats="stats"
    :ready="ready"
    @enter="enter(true)"
  />
  <EngineWorkspace
    v-else
    :seed-code="seedCode"
    @back="view = 'landing'"
  />
</template>
