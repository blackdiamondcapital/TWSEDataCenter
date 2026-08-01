<script setup>
import { ref, watch } from 'vue'
import { api } from '../api'

const emit = defineEmits(['select'])
const props = defineProps({
  initial: { type: String, default: '' },
})

const q = ref(props.initial)
const items = ref([])
const open = ref(false)
const loading = ref(false)
let timer = null

watch(
  () => props.initial,
  (v) => {
    if (v && v !== q.value) q.value = v
  },
)

function onInput() {
  clearTimeout(timer)
  const value = q.value.trim()
  if (!value) {
    items.value = []
    open.value = false
    return
  }
  timer = setTimeout(async () => {
    loading.value = true
    try {
      const data = await api.search(value)
      items.value = data.items || []
      open.value = items.value.length > 0
    } catch {
      items.value = []
      open.value = false
    } finally {
      loading.value = false
    }
  }, 180)
}

function pick(item) {
  q.value = `${item.code} ${item.label || ''}`.trim()
  open.value = false
  emit('select', item)
}

function onEnter() {
  const code = q.value.trim().split(/\s+/)[0]
  if (!code) return
  if (items.value[0]) {
    pick(items.value[0])
    return
  }
  emit('select', { code, label: code, symbol: `${code}.TW` })
}
</script>

<template>
  <div class="search" @keydown.escape="open = false">
    <label class="label muted" for="symbol-q">搜尋股票</label>
    <div class="row">
      <input
        id="symbol-q"
        v-model="q"
        type="search"
        placeholder="代號或名稱，例如 2330 / 台積電"
        autocomplete="off"
        @input="onInput"
        @focus="open = items.length > 0"
        @keydown.enter.prevent="onEnter"
      />
      <button class="cta" type="button" @click="onEnter">載入</button>
    </div>
    <ul v-if="open" class="menu" role="listbox">
      <li
        v-for="item in items"
        :key="item.symbol || item.code"
        role="option"
        @mousedown.prevent="pick(item)"
      >
        <span class="code mono">{{ item.code }}</span>
        <span class="name">{{ item.label }}</span>
        <span class="market muted">{{ item.market || '' }}</span>
      </li>
    </ul>
    <p v-if="loading" class="hint muted">搜尋中…</p>
  </div>
</template>

<style scoped>
.search {
  position: relative;
}

.label {
  display: block;
  font-size: 0.75rem;
  letter-spacing: 0.12em;
  margin-bottom: 0.45rem;
}

.row {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 0.55rem;
}

.menu {
  position: absolute;
  z-index: 20;
  left: 0;
  right: 0;
  top: calc(100% + 4px);
  margin: 0;
  padding: 0.35rem 0;
  list-style: none;
  background: #10151f;
  border: 1px solid var(--line-strong);
  max-height: 280px;
  overflow: auto;
}

.menu li {
  display: grid;
  grid-template-columns: 4.5rem 1fr auto;
  gap: 0.75rem;
  padding: 0.65rem 0.9rem;
  cursor: pointer;
}

.menu li:hover {
  background: var(--aqua-dim);
}

.code {
  color: var(--aqua);
  font-size: 0.9rem;
}

.name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.market {
  font-size: 0.75rem;
}

.hint {
  margin: 0.4rem 0 0;
  font-size: 0.78rem;
}
</style>
