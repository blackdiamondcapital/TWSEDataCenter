<script setup>
import { computed, onBeforeUnmount, onMounted } from 'vue'

const props = defineProps({
  stats: { type: Object, default: null },
  ready: { type: Boolean, default: false },
})
defineEmits(['enter'])

const periodText = computed(() => {
  const p = String(props.stats?.latestPeriod || '')
  if (p.length >= 6 && /^\d{6}/.test(p)) {
    return `${p.slice(0, 4)} Q${Number(p.slice(4, 6))}`
  }
  return p || '—'
})

let sectionObserver = null

onMounted(() => {
  const sections = document.querySelectorAll('.reveal-section')

  if (!('IntersectionObserver' in window)) {
    sections.forEach((section) => section.classList.add('is-visible'))
    return
  }

  sectionObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return
        entry.target.classList.add('is-visible')
        sectionObserver?.unobserve(entry.target)
      })
    },
    {
      threshold: 0.14,
      rootMargin: '0px 0px -8% 0px',
    },
  )

  sections.forEach((section) => sectionObserver.observe(section))
})

onBeforeUnmount(() => {
  sectionObserver?.disconnect()
  sectionObserver = null
})
</script>

<template>
  <main class="landing">
  <section class="hero">
    <div class="hero-atmosphere" aria-hidden="true">
      <div class="glow g1" />
      <div class="glow g2" />
      <div class="grid-fade" />
    </div>

    <header class="topbar anim-rise">
      <div class="brand-lockup">
        <span class="mark">QUANTGEMS</span>
        <span class="divider" />
        <span class="product muted">STATEMENT ENGINE</span>
      </div>
      <nav class="topnav" aria-label="頁面導覽">
        <a href="#capabilities">核心能力</a>
        <a href="#workflow">解讀流程</a>
        <button type="button" :disabled="!ready" @click="$emit('enter')">開啟工作台</button>
      </nav>
    </header>

    <div class="hero-stage">
      <div class="copy anim-rise">
        <p class="eyebrow mono">TAIWAN EQUITY INTELLIGENCE / 01</p>
        <p class="brand display">QUANTGEMS</p>
        <h1 class="headline display anim-rise anim-rise-delay-1">財報引擎</h1>
        <p class="lead anim-rise anim-rise-delay-2">
          不只是看數字，而是看見企業如何運轉。把季報切成可讀的切面——損益、資產負債、現金流，一次對齊。
        </p>
        <div class="hero-points anim-rise anim-rise-delay-2">
          <span>多期趨勢</span>
          <span>三表對照</span>
          <span>關鍵比率</span>
        </div>
        <div class="actions anim-rise anim-rise-delay-3">
          <button class="cta" type="button" :disabled="!ready" @click="$emit('enter')">
            進入引擎
          </button>
          <button class="ghost" type="button" :disabled="!ready" @click="$emit('enter')">
            以台積電示範
          </button>
        </div>
      </div>

      <aside class="visual anim-rise anim-rise-delay-2" aria-hidden="true">
        <div class="facet-panel">
          <div class="facet-ring" />
          <div class="facet-inner">
            <span class="facet-label">STATEMENT CUT</span>
            <ul class="ledger">
              <li><span>營收</span><i /><b>Revenue</b></li>
              <li><span>毛利</span><i /><b>Gross</b></li>
              <li class="on"><span>營業利益</span><i /><b>Operating</b></li>
              <li><span>淨利</span><i /><b>Net</b></li>
              <li><span>現金流</span><i /><b>Cash</b></li>
            </ul>
            <div class="facet-foot">
              <span>IFRS · 季報</span>
              <span class="mono">三表對齊</span>
            </div>
          </div>
        </div>
      </aside>
    </div>

    <footer class="hero-foot muted anim-rise anim-rise-delay-3" v-if="stats">
      <span>{{ Number(stats.companies || 0).toLocaleString('zh-TW') }} 家公司</span>
      <span class="dot">·</span>
      <span>{{ Number(stats.income || 0).toLocaleString('zh-TW') }} 筆損益</span>
      <span class="dot">·</span>
      <span>最新 {{ periodText }}</span>
      <span class="foot-note">資料來源 · 公開資訊觀測站</span>
    </footer>
  </section>

  <section id="capabilities" class="content-section capabilities reveal-section">
    <div class="section-heading">
      <div>
        <p class="section-index mono">01 / READ THE BUSINESS</p>
        <h2 class="display">一份財報，三個觀察角度</h2>
      </div>
      <p>將分散的會計科目重新排列成投資人真正需要的營運脈絡。</p>
    </div>

    <div class="statement-cards">
      <article class="statement-card income-card">
        <div class="card-top">
          <span class="card-no mono">01</span>
          <span class="card-code mono">INCOME</span>
        </div>
        <h3>獲利的品質</h3>
        <p>從營收到淨利，追蹤毛利率、營益率與 EPS，辨認成長來自本業還是業外。</p>
        <ul>
          <li>營收與獲利趨勢</li>
          <li>核心費用結構</li>
          <li>每股盈餘變化</li>
        </ul>
      </article>

      <article class="statement-card balance-card">
        <div class="card-top">
          <span class="card-no mono">02</span>
          <span class="card-code mono">BALANCE</span>
        </div>
        <h3>體質的韌性</h3>
        <p>檢視資產配置、負債壓力與股東權益，理解企業承受景氣循環的能力。</p>
        <ul>
          <li>資產與負債結構</li>
          <li>流動性與償債力</li>
          <li>權益累積速度</li>
        </ul>
      </article>

      <article class="statement-card cash-card">
        <div class="card-top">
          <span class="card-no mono">03</span>
          <span class="card-code mono">CASH FLOW</span>
        </div>
        <h3>現金的方向</h3>
        <p>對照營業、投資與籌資現金流，判讀帳面獲利是否真正轉化為現金。</p>
        <ul>
          <li>營業現金含金量</li>
          <li>資本支出強度</li>
          <li>股利與融資動向</li>
        </ul>
      </article>
    </div>
  </section>

  <section id="workflow" class="content-section workflow reveal-section">
    <div class="workflow-copy">
      <p class="section-index mono">02 / FROM DATA TO SIGNAL</p>
      <h2 class="display">把複雜季報，縮成清楚的判讀順序</h2>
      <p class="workflow-lead">
        先看趨勢，再查結構，最後驗證現金。QuantGems 讓每一次財報研究都有一致的起點。
      </p>
      <button class="cta" type="button" :disabled="!ready" @click="$emit('enter')">
        用 2330 開始分析
      </button>
    </div>
    <ol class="steps">
      <li>
        <div class="step-head">
          <span class="step-no mono">01</span>
          <span class="step-mark display"><i>搜</i></span>
        </div>
        <div><b>搜尋公司</b><p>輸入股票代號或名稱，快速定位上市櫃公司。</p></div>
      </li>
      <li>
        <div class="step-head">
          <span class="step-no mono">02</span>
          <span class="step-mark display"><i>表</i></span>
        </div>
        <div><b>切換報表</b><p>損益、資產負債、現金流在同一工作區查看。</p></div>
      </li>
      <li>
        <div class="step-head">
          <span class="step-no mono">03</span>
          <span class="step-mark display"><i>比</i></span>
        </div>
        <div><b>比較多期</b><p>用趨勢與增減幅度，辨認結構性變化。</p></div>
      </li>
      <li>
        <div class="step-head">
          <span class="step-no mono">04</span>
          <span class="step-mark display"><i>觀</i></span>
        </div>
        <div><b>形成觀點</b><p>回到獲利、體質與現金，完成基本面判讀。</p></div>
      </li>
    </ol>
  </section>

  <section class="content-section data-section reveal-section">
    <div class="data-copy">
      <p class="section-index mono">03 / COVERAGE</p>
      <h2 class="display">不是範例資料，<br>是持續更新的台股財報庫。</h2>
    </div>
    <div class="data-metrics">
      <div><strong class="display">{{ Number(stats?.companies || 0).toLocaleString('zh-TW') }}</strong><span>涵蓋公司</span></div>
      <div><strong class="display">{{ Number(stats?.income || 0).toLocaleString('zh-TW') }}</strong><span>損益紀錄</span></div>
      <div><strong class="display">3</strong><span>核心財務報表</span></div>
      <div><strong class="display">{{ periodText }}</strong><span>最新資料期別</span></div>
    </div>
  </section>

  <section class="final-cta reveal-section">
    <p class="mono">QUANTGEMS / STATEMENT ENGINE</p>
    <h2 class="display">數字很多，真正重要的是脈絡。</h2>
    <button class="cta" type="button" :disabled="!ready" @click="$emit('enter')">進入財報引擎</button>
  </section>
  </main>
</template>

<style scoped>
.hero {
  position: relative;
  min-height: 100vh;
  min-height: 100dvh;
  display: grid;
  grid-template-rows: auto 1fr auto;
  padding: clamp(1.25rem, 3.5vw, 2.75rem) clamp(1.25rem, 5vw, 4rem);
  overflow: hidden;
}

.hero-atmosphere {
  position: absolute;
  inset: 0;
  pointer-events: none;
  z-index: 0;
}

.glow {
  position: absolute;
  border-radius: 50%;
  filter: blur(8px);
}

.g1 {
  width: 55vw;
  height: 55vw;
  max-width: 680px;
  max-height: 680px;
  right: -8%;
  top: -10%;
  background: radial-gradient(circle, rgba(45, 212, 191, 0.16), transparent 68%);
}

.g2 {
  width: 40vw;
  height: 40vw;
  max-width: 420px;
  max-height: 420px;
  left: -10%;
  bottom: 5%;
  background: radial-gradient(circle, rgba(212, 165, 116, 0.12), transparent 70%);
}

.grid-fade {
  position: absolute;
  inset: 0;
  background-image:
    linear-gradient(rgba(232, 228, 220, 0.035) 1px, transparent 1px),
    linear-gradient(90deg, rgba(232, 228, 220, 0.035) 1px, transparent 1px);
  background-size: 72px 72px;
  mask-image: radial-gradient(ellipse 70% 60% at 70% 45%, #000 20%, transparent 75%);
  -webkit-mask-image: radial-gradient(ellipse 70% 60% at 70% 45%, #000 20%, transparent 75%);
}

.topbar,
.hero-stage,
.hero-foot {
  position: relative;
  z-index: 1;
}

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  font-size: 0.78rem;
  letter-spacing: 0.18em;
}

.brand-lockup {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.topnav {
  display: flex;
  align-items: center;
  gap: 1.4rem;
}

.topnav a {
  color: var(--muted);
  text-decoration: none;
  font-size: 0.76rem;
  letter-spacing: 0.08em;
  transition: color 0.2s;
}

.topnav a:hover { color: var(--paper); }

.topnav button {
  padding: 0.48rem 0.8rem;
  font-size: 0.74rem;
}

.mark {
  font-family: var(--display);
  font-weight: 800;
  color: var(--aqua);
}

.divider {
  width: 28px;
  height: 1px;
  background: var(--line);
}

.product {
  letter-spacing: 0.22em;
  font-size: 0.72rem;
}

.hero-stage {
  display: grid;
  align-items: center;
  gap: clamp(2rem, 5vw, 4rem);
  width: min(1120px, 100%);
  margin: 0 auto;
  padding: clamp(1.5rem, 4vh, 3rem) 0;
}

@media (min-width: 900px) {
  .hero-stage {
    grid-template-columns: minmax(0, 1.05fr) minmax(280px, 0.85fr);
  }
}

.copy {
  min-width: 0;
}

.eyebrow {
  margin: 0 0 1rem;
  color: var(--aqua);
  font-size: 0.7rem;
  letter-spacing: 0.14em;
}

.brand {
  margin: 0;
  font-size: clamp(2.6rem, 6.5vw, 5.2rem);
  font-weight: 800;
  letter-spacing: -0.03em;
  line-height: 0.95;
  white-space: nowrap;
  background: linear-gradient(115deg, var(--paper) 15%, var(--aqua) 90%);
  -webkit-background-clip: text;
  background-clip: text;
  color: transparent;
}

.headline {
  margin: 0.7rem 0 0;
  font-size: clamp(1.7rem, 3.4vw, 2.6rem);
  font-weight: 700;
  letter-spacing: 0.1em;
  color: var(--brass);
}

.lead {
  margin: 1.25rem 0 0;
  max-width: 32ch;
  font-size: clamp(1rem, 1.5vw, 1.12rem);
  color: var(--paper-dim);
  line-height: 1.7;
}

.actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.75rem;
  margin-top: 2rem;
}

.hero-points {
  display: flex;
  flex-wrap: wrap;
  gap: 0.65rem;
  margin-top: 1.25rem;
}

.hero-points span {
  padding: 0.3rem 0.55rem;
  border-left: 1px solid var(--aqua);
  background: rgba(45, 212, 191, 0.05);
  color: var(--paper-dim);
  font-size: 0.75rem;
  letter-spacing: 0.08em;
}

.visual {
  display: none;
  justify-content: center;
}

@media (min-width: 900px) {
  .visual {
    display: flex;
  }
}

.facet-panel {
  position: relative;
  width: min(100%, 380px);
  aspect-ratio: 1 / 1.05;
  display: grid;
  place-items: center;
}

.facet-ring {
  position: absolute;
  inset: 0;
  clip-path: polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%);
  background:
    linear-gradient(160deg, rgba(45, 212, 191, 0.14), transparent 55%),
    linear-gradient(320deg, rgba(212, 165, 116, 0.1), transparent 48%),
    rgba(10, 12, 18, 0.55);
  box-shadow: inset 0 0 0 1px rgba(45, 212, 191, 0.28);
  animation: facetPulse 8s ease-in-out infinite;
}

.facet-inner {
  position: relative;
  width: 72%;
  padding: 0.2rem 0;
}

.facet-label {
  display: block;
  font-family: var(--mono);
  font-size: 0.68rem;
  letter-spacing: 0.18em;
  color: var(--aqua);
  margin-bottom: 1rem;
}

.ledger {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 0.7rem;
}

.ledger li {
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: center;
  gap: 0.65rem;
  font-size: 0.92rem;
  color: var(--paper-dim);
}

.ledger li i {
  height: 1px;
  background: linear-gradient(90deg, rgba(232, 228, 220, 0.28), transparent);
}

.ledger li b {
  font-family: var(--mono);
  font-weight: 500;
  font-size: 0.72rem;
  color: var(--muted);
}

.ledger li.on {
  color: var(--paper);
}

.ledger li.on span {
  color: var(--brass);
  font-weight: 600;
}

.ledger li.on b {
  color: var(--aqua);
}

.facet-foot {
  display: flex;
  justify-content: space-between;
  margin-top: 1.25rem;
  padding-top: 0.85rem;
  border-top: 1px solid var(--line);
  font-size: 0.72rem;
  color: var(--muted);
  letter-spacing: 0.06em;
}

.hero-foot {
  display: flex;
  flex-wrap: wrap;
  gap: 0.45rem;
  font-family: var(--mono);
  font-size: 0.78rem;
  letter-spacing: 0.02em;
  border-top: 1px solid var(--line);
  padding-top: 1rem;
  width: min(1120px, 100%);
  margin: 0 auto;
}

.dot { opacity: 0.45; }

.foot-note {
  margin-left: auto;
}

.content-section {
  position: relative;
  width: min(1120px, calc(100% - 2.5rem));
  margin: 0 auto;
  padding: clamp(5rem, 9vw, 8rem) 0;
}

.reveal-section {
  opacity: 0;
  transform: translateY(72px);
  filter: blur(5px);
  transition:
    opacity 0.85s cubic-bezier(0.22, 1, 0.36, 1),
    transform 0.85s cubic-bezier(0.22, 1, 0.36, 1),
    filter 0.75s ease;
  will-change: opacity, transform, filter;
}

.reveal-section.is-visible {
  opacity: 1;
  transform: translateY(0);
  filter: blur(0);
}

.reveal-section .statement-card,
.reveal-section .steps li,
.reveal-section .data-metrics > div {
  opacity: 0;
  transform: translateY(28px);
  transition:
    opacity 0.6s ease,
    transform 0.7s cubic-bezier(0.22, 1, 0.36, 1),
    border-color 0.25s ease,
    background 0.25s ease;
}

.reveal-section.is-visible .statement-card,
.reveal-section.is-visible .steps li,
.reveal-section.is-visible .data-metrics > div {
  opacity: 1;
  transform: translateY(0);
}

.reveal-section.is-visible .statement-card:nth-child(2),
.reveal-section.is-visible .steps li:nth-child(2),
.reveal-section.is-visible .data-metrics > div:nth-child(2) {
  transition-delay: 0.1s;
}

.reveal-section.is-visible .statement-card:nth-child(3),
.reveal-section.is-visible .steps li:nth-child(3),
.reveal-section.is-visible .data-metrics > div:nth-child(3) {
  transition-delay: 0.2s;
}

.reveal-section.is-visible .steps li:nth-child(4),
.reveal-section.is-visible .data-metrics > div:nth-child(4) {
  transition-delay: 0.3s;
}

.section-heading {
  display: grid;
  grid-template-columns: minmax(0, 1.3fr) minmax(260px, 0.7fr);
  align-items: end;
  gap: 3rem;
  margin-bottom: 3rem;
}

.section-index {
  margin: 0 0 1rem;
  color: var(--aqua);
  font-size: 0.7rem;
  letter-spacing: 0.16em;
}

.section-heading h2,
.workflow h2,
.data-copy h2,
.final-cta h2 {
  margin: 0;
  font-size: clamp(2rem, 4vw, 3.6rem);
  line-height: 1.12;
  letter-spacing: -0.03em;
}

.section-heading > p {
  margin: 0;
  color: var(--paper-dim);
  line-height: 1.8;
}

.capabilities {
  border-bottom: 1px solid var(--line);
}

.statement-cards {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.statement-card {
  position: relative;
  min-height: 390px;
  padding: 2rem;
  border-right: 1px solid var(--line);
  overflow: hidden;
}

.statement-card:last-child { border-right: 0; }

.statement-card::after {
  content: '';
  position: absolute;
  width: 180px;
  height: 180px;
  right: -75px;
  bottom: -75px;
  transform: rotate(45deg);
  border: 1px solid rgba(45, 212, 191, 0.15);
}

.balance-card::after { border-color: rgba(212, 165, 116, 0.22); }

.card-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  color: var(--muted);
  font-size: 0.88rem;
  letter-spacing: 0.12em;
}

.card-no {
  color: var(--aqua);
  font-size: 1rem;
  font-weight: 600;
}

.card-code {
  font-size: 0.82rem;
  font-weight: 500;
}

.balance-card .card-no { color: var(--brass); }

.statement-card h3 {
  margin: 4.5rem 0 1rem;
  font-family: var(--display);
  font-size: 1.65rem;
}

.statement-card > p {
  margin: 0;
  min-height: 5.4em;
  color: var(--paper-dim);
  line-height: 1.8;
}

.statement-card ul {
  list-style: none;
  margin: 1.8rem 0 0;
  padding: 1.2rem 0 0;
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: 0.82rem;
}

.statement-card li {
  margin-top: 0.45rem;
}

.statement-card li::before {
  content: '—';
  margin-right: 0.55rem;
  color: var(--aqua);
}

.workflow {
  display: grid;
  grid-template-columns: minmax(0, 0.9fr) minmax(380px, 1.1fr);
  gap: clamp(3rem, 8vw, 7rem);
  align-items: center;
}

.workflow-lead {
  margin: 1.5rem 0 2rem;
  max-width: 38ch;
  color: var(--paper-dim);
  line-height: 1.8;
}

.steps {
  position: relative;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.9rem;
  margin: 0;
  padding: 0;
  list-style: none;
}

.steps li {
  position: relative;
  min-height: 210px;
  padding: 1.25rem;
  border: 1px solid var(--line);
  background:
    linear-gradient(145deg, rgba(45, 212, 191, 0.08), transparent 55%),
    rgba(12, 15, 22, 0.72);
  overflow: hidden;
  transition:
    opacity 0.6s ease,
    transform 0.25s ease,
    border-color 0.25s ease,
    background 0.25s ease;
}

.steps li:nth-child(2),
.steps li:nth-child(3) {
  background:
    linear-gradient(145deg, rgba(212, 165, 116, 0.09), transparent 58%),
    rgba(12, 15, 22, 0.72);
}

.steps li:nth-child(2),
.steps li:nth-child(4) {
  transform: translateY(1.25rem);
}

.workflow.reveal-section.is-visible .steps li:nth-child(2),
.workflow.reveal-section.is-visible .steps li:nth-child(4) {
  transform: translateY(1.25rem);
}

.steps li:hover {
  transform: translateY(-4px);
  border-color: rgba(45, 212, 191, 0.4);
  background:
    linear-gradient(145deg, rgba(45, 212, 191, 0.14), transparent 60%),
    rgba(14, 18, 26, 0.92);
}

.steps li:nth-child(2):hover,
.steps li:nth-child(4):hover {
  transform: translateY(calc(1.25rem - 4px));
}

.steps li::after {
  content: '';
  position: absolute;
  right: -34px;
  bottom: -45px;
  width: 105px;
  height: 105px;
  border: 1px solid rgba(45, 212, 191, 0.15);
  transform: rotate(45deg);
}

.steps li:nth-child(2)::after,
.steps li:nth-child(3)::after {
  border-color: rgba(212, 165, 116, 0.2);
}

.step-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 1.5rem;
}

.step-no {
  color: var(--aqua);
  font-size: 1.35rem;
  font-weight: 600;
  line-height: 1;
  letter-spacing: 0.04em;
}

.steps li:nth-child(2) .step-no,
.steps li:nth-child(3) .step-no {
  color: var(--brass);
}

.step-mark {
  display: grid;
  place-items: center;
  width: 2.6rem;
  height: 2.6rem;
  border: 1px solid rgba(45, 212, 191, 0.3);
  color: var(--aqua);
  font-size: 1rem;
  transform: rotate(45deg);
}

.step-mark i {
  font-style: normal;
  transform: rotate(-45deg);
}

.steps li:nth-child(2) .step-mark,
.steps li:nth-child(3) .step-mark {
  color: var(--brass);
  border-color: rgba(212, 165, 116, 0.35);
}

.steps b {
  font-family: var(--display);
  font-size: 1.15rem;
  letter-spacing: 0.03em;
  font-weight: 600;
}

.steps p {
  margin: 0.55rem 0 0;
  color: var(--muted);
  font-size: 0.86rem;
  line-height: 1.7;
}

.data-section {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 4rem;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.data-metrics {
  display: grid;
  grid-template-columns: 1fr 1fr;
  border-top: 1px solid var(--line);
  border-left: 1px solid var(--line);
}

.data-metrics > div {
  display: flex;
  flex-direction: column;
  justify-content: center;
  min-height: 150px;
  padding: 1.25rem;
  border-right: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.data-metrics strong {
  color: var(--aqua);
  font-size: clamp(1.6rem, 3vw, 2.5rem);
}

.data-metrics span {
  margin-top: 0.4rem;
  color: var(--muted);
  font-size: 0.78rem;
  letter-spacing: 0.08em;
}

.final-cta {
  position: relative;
  width: min(1120px, calc(100% - 2.5rem));
  margin: 0 auto;
  padding: clamp(6rem, 12vw, 10rem) 1rem;
  text-align: center;
  overflow: hidden;
}

.final-cta::before {
  content: '';
  position: absolute;
  width: 520px;
  height: 520px;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%) rotate(45deg);
  border: 1px solid rgba(45, 212, 191, 0.12);
  pointer-events: none;
}

.final-cta p {
  color: var(--aqua);
  font-size: 0.7rem;
  letter-spacing: 0.16em;
}

.final-cta h2 {
  position: relative;
  margin: 1.2rem auto 2rem;
  max-width: 17ch;
}

@media (max-width: 899px) {
  .topnav a { display: none; }

  .brand {
    white-space: normal;
    font-size: clamp(2.4rem, 12vw, 3.6rem);
  }

  .lead {
    max-width: 36ch;
  }

  .section-heading,
  .workflow,
  .data-section {
    grid-template-columns: 1fr;
  }

  .statement-cards {
    grid-template-columns: 1fr;
  }

  .statement-card {
    min-height: auto;
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .statement-card:last-child { border-bottom: 0; }
  .statement-card h3 { margin-top: 2.5rem; }
  .statement-card > p { min-height: auto; }

  .steps li:nth-child(2),
  .steps li:nth-child(4) {
    transform: none;
  }

  .steps li:nth-child(2):hover,
  .steps li:nth-child(4):hover {
    transform: translateY(-4px);
  }
}

@media (max-width: 560px) {
  .product,
  .divider,
  .topnav {
    display: none;
  }

  .foot-note { width: 100%; margin: 0.3rem 0 0; }
  .content-section { width: min(100% - 2rem, 1120px); }
  .data-metrics { grid-template-columns: 1fr; }
  .steps { grid-template-columns: 1fr; }
}

@media (prefers-reduced-motion: reduce) {
  .reveal-section,
  .reveal-section .statement-card,
  .reveal-section .steps li,
  .reveal-section .data-metrics > div {
    opacity: 1;
    transform: none;
    filter: none;
    transition: none;
  }
}
</style>
