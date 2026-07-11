(() => {
    const hardcodedBase = 'http://localhost:5003';
    const qs = (typeof window !== 'undefined' && window.location && window.location.search)
        ? new URLSearchParams(window.location.search)
        : null;
    const fromQuery = qs ? (qs.get('api_base') || qs.get('apiBase') || qs.get('api')) : null;
    const fromStorage = (() => {
        try {
            return window && window.localStorage ? window.localStorage.getItem('API_BASE_URL') : null;
        } catch (_) {
            return null;
        }
    })();
    const fromWindow = (typeof window !== 'undefined') ? (window.API_BASE_URL || window.__API_BASE_URL) : null;

    const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
        ? window.location.origin
        : '';
    const defaultBase = (origin && origin !== 'file://' && origin !== 'null') ? origin : hardcodedBase;

    const configuredBase = String(fromQuery || fromStorage || fromWindow || defaultBase).trim().replace(/\/+$/, '');

    if (typeof window !== 'undefined') {
        window.__API_BASE_URL = configuredBase;
    }

    const rewriteUrl = (u) => {
        try {
            const s = String(u);
            if (s.startsWith(hardcodedBase)) {
                return configuredBase + s.slice(hardcodedBase.length);
            }
            return s;
        } catch (_) {
            return u;
        }
    };

    const apiUrl = (path) => {
        const p = String(path || '');
        return `${configuredBase}${p.startsWith('/') ? '' : '/'}${p}`;
    };

    if (typeof window !== 'undefined') {
        window.apiUrl = apiUrl;
        window.rewriteUrl = rewriteUrl;
    }

    if (typeof window !== 'undefined' && typeof window.fetch === 'function') {
        const originalFetch = window.fetch.bind(window);
        window.fetch = (input, init) => {
            try {
                if (typeof input === 'string') {
                    return originalFetch(rewriteUrl(input), init);
                }
                if (input && typeof Request !== 'undefined' && input instanceof Request) {
                    const newUrl = rewriteUrl(input.url);
                    if (newUrl !== input.url) {
                        const req = new Request(newUrl, input);
                        return originalFetch(req, init);
                    }
                }
            } catch (_) {}
            return originalFetch(input, init);
        };
    }

    if (typeof window !== 'undefined' && typeof window.EventSource === 'function') {
        const OriginalEventSource = window.EventSource;
        function WrappedEventSource(url, config) {
            return new OriginalEventSource(rewriteUrl(url), config);
        }
        WrappedEventSource.prototype = OriginalEventSource.prototype;
        window.EventSource = WrappedEventSource;
    }
})();

// Global helpers for the rest of this script
const apiUrl = (typeof window !== 'undefined' && typeof window.apiUrl === 'function')
    ? window.apiUrl
    : (p) => p;
const rewriteUrl = (typeof window !== 'undefined' && typeof window.rewriteUrl === 'function')
    ? window.rewriteUrl
    : (u) => u;

// Taiwan Stock Data Update System - JavaScript
class TaiwanStockApp {
    constructor() {
        this.dbConfig = {
            host: 'localhost',
            port: '5432',
            user: 'postgres',
            password: '',
            dbname: 'postgres'
        };
        
        this.dbTarget = 'remote';
        this.isUpdating = false;
        // Summary and logging state
        this.summary = { total: 0, processed: 0, success: 0, failed: 0 };
        this.timerStart = null;
        this.timerInterval = null;
        this.autoScrollLog = true;
        this.currentLogFilter = 'all';
        this.bwibbuLoading = false;
        // T86 狀態
        this.t86Data = [];
        this.t86DailyStats = [];
        this.t86LogPanel = null;
        this.t86LogAutoScroll = true;
        this._t86LogInitialized = false;
        this._t86ProgressTimer = null;
        this._t86ProgressStart = 0;
        this._t86LastProgressLog = 0;
        // Margin 狀態
        this.marginData = [];
        this.marginDailyStats = [];
        this.marginLogPanel = null;
        this.marginLogAutoScroll = true;
        this._marginLogInitialized = false;
        this._marginProgressTimer = null;
        this._marginProgressStart = 0;
        this._marginLastProgressLog = 0;
        // Revenue 狀態
        this.revenueData = [];
        this.revenueLogPanel = null;
        this.revenueLogAutoScroll = true;
        this._revenueLogInitialized = false;
        // Income statement 狀態
        this.incomeData = [];
        this.incomeLogPanel = null;
        this.incomeLogAutoScroll = true;
        this._incomeLogInitialized = false;
        this._incomeProgressTimer = null;
        // Balance sheet 狀態
        this.balanceData = [];
        this.balanceLogPanel = null;
        this.balanceLogAutoScroll = true;
        this._balanceLogInitialized = false;
        this._balanceProgressTimer = null;
        // Cash-flow statement 狀態
        this.cashflowData = [];
        this.cashflowLogPanel = null;
        this._cashflowProgressTimer = null;
        // Financial ratios 狀態
        this.ratiosData = [];
        this.ratiosLogPanel = null;
        this.ratiosLogAutoScroll = true;
        this._ratiosLogInitialized = false;
        this._ratiosProgressTimer = null;
        this._ratiosLastProgressPct = null;
        // Symbols UI state
        this.symbolsList = [];

        // Database sync UI state
        this._lastSyncTables = [];
        this._lastSyncTablesSource = 'local';
        this.init();
    }

    setupDatabaseTargetToggle() {
        const radios = document.querySelectorAll('input[name="dbTarget"]');
        if (!radios.length) return;

        const syncRadios = () => {
            radios.forEach((radio) => {
                radio.checked = radio.value === this.dbTarget;
            });
        };

        syncRadios();

        if (!window.__CLOUD_DEPLOYMENT) {
            document.querySelectorAll('#dbTargetToggle, .module-db-toggle').forEach((element) => {
                element.style.display = '';
            });
        }

        radios.forEach((radio) => {
            radio.addEventListener('change', async (event) => {
                if (!event.target.checked) return;
                this.dbTarget = event.target.value === 'local' ? 'local' : 'remote';
                syncRadios();
                this.addLogMessage(`🔁 切換資料庫目標為 ${this.dbTarget === 'local' ? '本地 PostgreSQL' : 'Neon（雲端）'}`, 'info');
                await this.checkDatabaseConnection();
                try {
                    await this.loadQueryTables();
                } catch (e) {
                }
            });
        });
    }

    get useLocalDb() {
        return this.dbTarget === 'local';
    }

    // ===== 異常檢核與修復 =====
    getAnomalyParams() {
        const symbol = document.getElementById('anomalySymbol')?.value?.trim();
        const start = document.getElementById('anomalyStartDate')?.value;
        const end = document.getElementById('anomalyEndDate')?.value;
        const thEl = document.getElementById('anomalyThreshold');
        let threshold = parseFloat(thEl?.value || '0.2');
        if (isNaN(threshold) || threshold <= 0) threshold = 0.2;
        return { symbol: symbol || null, start, end, threshold };
    }

    async detectAnomalies() {
        try {
            const { symbol, start, end, threshold } = this.getAnomalyParams();
            if (!start || !end) {
                this.addLogMessage('請填寫開始與結束日期再執行檢測', 'warning');
                return;
            }
            const qs = new URLSearchParams();
            if (symbol) qs.set('symbol', symbol);
            qs.set('start', start);
            qs.set('end', end);
            qs.set('threshold', String(threshold));

            this.addLogMessage(`🔎 檢測異常：symbol=${symbol || 'ALL'}, 範圍=${start}~${end}, 閾值=${threshold}`, 'info');
            if (this.useLocalDb) qs.set('use_local_db', 'true');
            const res = await fetch(`http://localhost:5003/api/anomalies/detect?${qs.toString()}`);
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.error || `HTTP ${res.status}`);
            }
            this.addLogMessage(`✅ 檢測完成，發現異常 ${data.count} 筆（threshold=${data.threshold}）`, 'success');
            // 簡要列出前 10 筆
            const preview = (data.data || []).slice(0, 10);
            if (preview.length > 0) {
                this.addLogMessage(`前${preview.length}筆：`,'info');
                preview.forEach((r, idx) => {
                    this.addLogMessage(`${idx+1}. ${r.symbol} ${r.date} prev=${r.prev_close} close=${r.close} change=${(r.pct_change*100).toFixed(2)}%`, 'info');
                });
                if (data.count > preview.length) {
                    this.addLogMessage(`... 其餘 ${data.count - preview.length} 筆省略`, 'info');
                }
            } else {
                this.addLogMessage('未檢出異常。', 'info');
            }
        } catch (err) {
            this.addLogMessage(`檢測異常失敗：${err.message}`, 'error');
        }
    }
    
    // 透過後端 API 匯入 ^OTC 日K（TPEX OpenAPI）至 tw_stock_prices
    async importOtcFromTpexApi() {
        try {
            let start = null;
            let end = null;
            if (typeof this.getUpdateConfig === 'function') {
                try {
                    const cfg = this.getUpdateConfig();
                    if (cfg && cfg.valid && cfg.startDate && cfg.endDate) {
                        start = cfg.startDate;
                        end = cfg.endDate;
                    }
                } catch (_) {}
            }

            if (!start || !end) {
                const today = new Date();
                const todayStr = this.formatDate(today);
                start = null;
                end = todayStr;
            }

            const payload = { fetch_market_index: true, only_market_index: true };
            if (start) payload.start_date = start;
            if (end) payload.end_date = end;
            payload.symbols = []; // 僅同步指數
            payload.update_prices = true;
            payload.use_batch_mode = true;
            payload.force_full_refresh = false;
            payload.index_symbols = ['^OTC'];
            payload.respect_requested_range = true;
            if (this.useLocalDb) payload.use_local_db = true;

            const targetLabel = this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端';
            const rangeLabel = start && end ? `${start} ~ ${end}` : `預設起始 ~ ${payload.end_date || 'today'}`;
            this.addLogMessage(`📈 開始匯入櫃買指數 (^OTC)：範圍 ${rangeLabel}，目標：${targetLabel}`, 'info');

            const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
                ? window.location.origin
                : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

            // 後端 /api/update 會在 fetch_market_index=true 時同步 ^TWII 與 ^OTC，symbols 留空只跑指數
            const resp = await fetch(`${base}/api/update`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            // 取出指數同步摘要
            const indexSummary = (data.index_sync_summary || data.index_sync || []).filter(s => s && s.mode === 'index');
            const otcs = indexSummary.find(s => s.symbol === '^OTC');
            if (otcs) {
                this.addLogMessage(`✅ 櫃買指數匯入完成：寫入 ${otcs.prices_updated ?? 0} 筆`, 'success');
            } else {
                this.addLogMessage('ℹ️ 櫃買指數同步完成（無寫入或已最新）', 'info');
            }

            // 計算報酬率
            try {
                const computePayload = { symbol: '^OTC', fill_missing: true };
                if (start) computePayload.start = start;
                if (end) computePayload.end = end;
                if (this.useLocalDb) computePayload.use_local_db = true;

                this.addLogMessage('🧮 開始計算 ^OTC 報酬率並寫入 tw_stock_returns...', 'info');

                const retResp = await fetch(`${base}/api/returns/compute`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(computePayload)
                });
                const retData = await retResp.json();
                if (!retResp.ok || !retData.success) {
                    throw new Error(retData.error || `HTTP ${retResp.status}`);
                }

                const totalWritten = retData.total_written ?? 0;
                this.addLogMessage(`✅ ^OTC 報酬率計算完成：寫入 ${totalWritten} 筆`, 'success');
            } catch (retErr) {
                this.addLogMessage(`⚠️ ^OTC 報酬率計算失敗：${retErr.message}`, 'warning');
            }
        } catch (err) {
            this.addLogMessage(`❌ 匯入櫃買指數失敗：${err.message}`, 'error');
        }
    }

    async fetchBalanceMultiPeriod() {
        try {
            const fromStr = document.getElementById('balanceYearFrom')?.value || '';
            const toStr = document.getElementById('balanceYearTo')?.value || '';
            const baseYearStr = document.getElementById('balanceYear')?.value || '';

            let fromYear = parseInt(fromStr || baseYearStr || '', 10);
            let toYear = parseInt(toStr || baseYearStr || '', 10);

            if (!Number.isFinite(fromYear) || fromYear < 2000) {
                this.addBalanceLog('請輸入正確的多期起始年度（例如 2020），或至少填寫上方單一期別年度。', 'warning');
                return;
            }
            if (!Number.isFinite(toYear) || toYear < 2000) {
                toYear = fromYear;
            }
            if (fromYear > toYear) {
                const tmp = fromYear;
                fromYear = toYear;
                toYear = tmp;
            }

            const codeFromEl = document.getElementById('balanceCodeFrom');
            const codeToEl = document.getElementById('balanceCodeTo');
            const codeFrom = codeFromEl ? String(codeFromEl.value || '').trim() : '';
            const codeTo = codeToEl ? String(codeToEl.value || '').trim() : '';

            const batchSizeStr = document.getElementById('balanceBatchSize')?.value || '';
            const restMinutesStr = document.getElementById('balanceBatchRestMinutes')?.value || '';
            const retryMaxStr = document.getElementById('balanceRetryMax')?.value || '';
            const retryWaitMinutesStr = document.getElementById('balanceRetryWaitMinutes')?.value || '';
            const batchSize = parseInt(batchSizeStr, 10);
            const restMinutes = parseFloat(restMinutesStr);
            const retryMax = parseInt(retryMaxStr, 10);
            const retryWaitMinutes = parseFloat(retryWaitMinutesStr);
            const hasBatch = Number.isFinite(batchSize) && batchSize > 0;
            const hasRest = Number.isFinite(restMinutes) && restMinutes > 0;
            const hasRetryMax = Number.isFinite(retryMax) && retryMax >= 0;
            const hasRetryWait = Number.isFinite(retryWaitMinutes) && retryWaitMinutes > 0;

            const selectedSeasons = [];
            for (let s = 1; s <= 4; s += 1) {
                const cb = document.getElementById(`balanceMultiSeason${s}`);
                if (!cb || cb.checked) selectedSeasons.push(s);
            }
            if (!selectedSeasons.length) {
                this.addBalanceLog('請至少勾選一個季別', 'warning');
                return;
            }

            const tasks = [];
            for (let y = fromYear; y <= toYear; y += 1) {
                for (const s of selectedSeasons) {
                    tasks.push({ year: y, season: s });
                }
            }
            if (!tasks.length) {
                this.addBalanceLog('沒有可執行的期別，請檢查年度與季別設定。', 'warning');
                return;
            }

            const autoImportEl = document.getElementById('balanceAutoImportCheckbox');
            const writeToDb = !!(autoImportEl && autoImportEl.checked);

            this.clearBalanceLog(true);
            this.addBalanceLog(
                `開始多期別資產負債表抓取：年度 ${fromYear} ~ ${toYear}，季別 ${selectedSeasons.join('、')}（共 ${tasks.length} 期）`,
                'info',
            );
            if (codeFrom || codeTo) {
                this.addBalanceLog(
                    `多期別僅限股票代號範圍：${codeFrom || '最小'} ~ ${codeTo || '最大'}`,
                    'info',
                );
            }
            if (hasBatch && hasRest) {
                this.addBalanceLog(
                    `多期別節流設定：每抓取 ${batchSize} 檔休息 ${restMinutes} 分鐘後繼續。`,
                    'info',
                );
            }
            if (hasRetryMax) {
                this.addBalanceLog(
                    `封鎖自動續抓設定：最多暫停/重試 ${retryMax} 次（每次 ${hasRetryWait ? retryWaitMinutes : 5} 分鐘）。`,
                    'info',
                );
            }
            if (writeToDb) {
                this.addBalanceLog(
                    `多期別資產負債表將在伺服器端同步寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}）。`,
                    'info',
                );
            }

            this.stopBalanceProgressTimer();
            this.updateBalanceProgress(5, '準備開始多期別抓取…');

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/balance-sheet`;

            let allRows = [];
            const allCodes = new Set();

            const overallStartedAt = (typeof performance !== 'undefined' && performance.now)
                ? performance.now()
                : Date.now();

            for (let i = 0; i < tasks.length; i += 1) {
                const { year, season } = tasks[i];
                const periodLabel = `${year}${String(season).padStart(2, '0')}`;

                this.addBalanceLog(`▶️ (${i + 1}/${tasks.length}) 開始抓取期別 ${periodLabel} 全市場`, 'info');

                this.stopBalanceProgressTimer();
                this.updateBalanceProgress(5, `準備抓取期別 ${periodLabel}…`);

                const params = new URLSearchParams({ year: String(year), season: String(season) });
                if (codeFrom) params.append('code_from', codeFrom);
                if (codeTo) params.append('code_to', codeTo);
                if (hasBatch) params.append('pause_every', String(batchSize));
                if (hasRest) params.append('pause_minutes', String(restMinutes));
                params.append('retry_on_block', '1');
                params.append('retry_wait_minutes', String(hasRetryWait ? retryWaitMinutes : 5));
                if (hasRetryMax) params.append('retry_max', String(retryMax));
                if (writeToDb) {
                    params.append('write_to_db', '1');
                    params.append('use_local_db', this.useLocalDb ? 'true' : 'false');
                }

                this._balanceProgressTimer = window.setInterval(async () => {
                    try {
                        const res = await fetch(`${base}/api/balance-sheet/status`);
                        if (!res.ok) return;
                        const json = await res.json();
                        if (!json || !json.success || !json.status) return;
                        const st = json.status;
                        const total = Number(st.total || 0);
                        const processed = Number(st.processed || 0);

                        let pct = 10;
                        if (total > 0 && processed >= 0) {
                            pct = Math.max(10, Math.min(99, Math.round((processed / total) * 100)));
                        }

                        let msg = '';
                        if (st.running) {
                            if (st.paused) {
                                const resumeAt = st.resumeAt || '';
                                const blocks = Number(st.block_count || 0);
                                msg = `伺服器暫停中（${periodLabel}，已觸發防護${Number.isFinite(blocks) && blocks > 0 ? ` ${blocks} 次` : ''}），預計 ${resumeAt || '稍後'} 續抓…`;
                            } else if (total > 0 && processed > 0) {
                                msg = `伺服器處理中（${periodLabel}）：第 ${processed}/${total} 檔（${st.current_code || ''}）`;
                            } else {
                                msg = `伺服器處理中（${periodLabel}），等待進度資料…`;
                            }
                        } else {
                            msg = `伺服器已回應（${periodLabel}），前端正在整理資料…`;
                        }

                        this.updateBalanceProgress(pct, msg);
                    } catch (err) {
                        console.warn('balance-sheet status poll error (multi)', err);
                    }
                }, 5000);

                const startedAt = (typeof performance !== 'undefined' && performance.now)
                    ? performance.now()
                    : Date.now();
                const resp = await fetch(`${requestUrl}?${params.toString()}`);
                if (!resp.ok) {
                    let msg = `HTTP ${resp.status}`;
                    try {
                        const raw = await resp.text();
                        if (raw) {
                            try {
                                const j = JSON.parse(raw);
                                if (j && j.error) msg = j.error;
                                else msg = raw;
                            } catch (_) {
                                msg = raw;
                            }
                        }
                    } catch (_) {}
                    throw new Error(msg);
                }
                const data = await resp.json();

                const finishedAt = (typeof performance !== 'undefined' && performance.now)
                    ? performance.now()
                    : Date.now();
                const elapsedMs = Math.max(0, finishedAt - startedAt);
                const elapsedSec = (elapsedMs / 1000).toFixed(2);

                this.stopBalanceProgressTimer();

                if (!Array.isArray(data) || !data.length) {
                    this.addBalanceLog(`⚠️ 期別 ${periodLabel} 無資料（可能尚未公告）`, 'warning');
                    continue;
                }

                data.forEach((row) => {
                    if (row && row['股票代號']) allCodes.add(row['股票代號']);
                });
                allRows = allRows.concat(data);

                this.addBalanceLog(
                    `✅ 期別 ${periodLabel} 完成，新增 ${this.formatInteger(data.length)} 筆（耗時 ${elapsedSec} 秒）`,
                    'success',
                );
            }

            const overallFinishedAt = (typeof performance !== 'undefined' && performance.now)
                ? performance.now()
                : Date.now();
            const overallElapsedMs = Math.max(0, overallFinishedAt - overallStartedAt);
            const overallElapsedSec = (overallElapsedMs / 1000).toFixed(2);

            if (!allRows.length) {
                this.balanceData = [];
                this.renderBalanceResultsTable();
                this.addBalanceLog('多期別抓取未取得任何資料。', 'warning');
                this.updateBalanceProgress(0, '多期別抓取未取得資料');
                return;
            }

            this.balanceData = allRows;
            const periodSummaryLabel = `${fromYear}-${toYear} 多期（季別：${selectedSeasons.join('、')}）`;
            this.updateBalanceSummary(allRows, fromYear, selectedSeasons[0]);
            const badge = document.getElementById('balanceSummaryBadge');
            if (badge) badge.textContent = periodSummaryLabel;
            this.renderBalanceResultsTable();

            const totalRows = allRows.length;
            this.addLogMessage(
                `✅ 多期別資產負債表抓取完成，共 ${this.formatInteger(totalRows)} 筆，涵蓋 ${this.formatInteger(allCodes.size)} 檔股票（總耗時 ${overallElapsedSec} 秒）`,
                'success',
            );
            this.addBalanceLog(
                `多期別資產負債表抓取完成：共 ${this.formatInteger(totalRows)} 筆，${this.formatInteger(allCodes.size)} 檔股票（總耗時 ${overallElapsedSec} 秒）`,
                'success',
            );
            this.updateBalanceProgress(100, `多期別完成：共 ${this.formatInteger(totalRows)} 筆，${this.formatInteger(allCodes.size)} 檔股票`);
        } catch (err) {
            console.error('[Balance] multi-period fetch error', err);
            this.addLogMessage(`❌ 多期別資產負債表抓取失敗：${err.message}`, 'error');
            this.addBalanceLog(`多期別資產負債表抓取失敗：${err.message}`, 'error');
            this.stopBalanceProgressTimer();
            this.updateBalanceProgress(0, '多期別抓取失敗，請查看下方日誌訊息');
        }
    }

    async importBalanceToDb() {
        try {
            const rows = Array.isArray(this.balanceData) ? this.balanceData : [];
            if (!rows.length) {
                this.addBalanceLog('目前沒有可寫入資料庫的資產負債表資料，請先執行抓取。', 'warning');
                return;
            }

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const isBackendOrigin = typeof origin === 'string' && /:\s*5003\b/.test(origin.replace(/\s+/g, ''));
            const base = isBackendOrigin ? origin : 'http://localhost:5003';
            const url = `${base}/api/balance-sheet/import`;

            this.addBalanceLog(
                `開始將目前資產負債表資料寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}），共 ${this.formatInteger(rows.length)} 筆`,
                'info',
            );
            this.addBalanceLog(`POST ${url}`, 'info');

            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const resp = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rows, use_local_db: this.useLocalDb }),
            });
            const data = await resp.json().catch(() => ({}));

            if (!resp.ok || !data || data.success === false) {
                const msg = (data && data.error) ? data.error : `HTTP ${resp.status}`;
                throw new Error(msg);
            }

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);

            const inserted = Number(data.inserted || 0);
            this.addLogMessage(
                `✅ 資產負債表寫入資料庫完成，成功寫入 ${this.formatInteger(inserted)} 筆（耗時 ${elapsedSec} 秒）`,
                'success',
            );
            this.addBalanceLog(
                `資產負債表寫入資料庫完成：成功寫入 ${this.formatInteger(inserted)} 筆（耗時 ${elapsedSec} 秒）`,
                'success',
            );
        } catch (err) {
            console.error('[Balance] import DB error', err);
            this.addLogMessage(`❌ 資產負債表寫入資料庫失敗：${err.message}`, 'error');
            this.addBalanceLog(`資產負債表寫入資料庫失敗：${err.message}`, 'error');
        }
    }

    async exportAnomalies() {
        try {
            const { symbol, start, end, threshold } = this.getAnomalyParams();
            if (!start || !end) {
                this.addLogMessage('請先選擇開始與結束日期再匯出', 'warning');
                return;
            }
            const qs = new URLSearchParams();
            if (symbol) qs.set('symbol', symbol);
            qs.set('start', start);
            qs.set('end', end);
            qs.set('threshold', String(threshold));
            if (this.useLocalDb) qs.set('use_local_db', 'true');
            const url = apiUrl(`/api/anomalies/export?${qs.toString()}`);
            this.addLogMessage(`📤 匯出異常清單: ${url}`, 'info');
            
            // 以 fetch 取得 Blob，避免被瀏覽器阻擋或另開頁問題
            const res = await fetch(url, { method: 'GET' });
            if (!res.ok) {
                let msg = `HTTP ${res.status}`;
                try { const j = await res.json(); if (j && j.error) msg = j.error; } catch (_) {}
                throw new Error(msg);
            }
            const blob = await res.blob();
            const cd = res.headers.get('Content-Disposition') || '';
            let filename = 'anomalies.csv';
            const m = cd.match(/filename="?([^";]+)"?/i);
            if (m && m[1]) filename = m[1];
            const objUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = objUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(objUrl);
        } catch (err) {
            this.addLogMessage(`匯出異常清單失敗：${err.message}`, 'error');
        }
    }

    async fixAnomalies() {
        try {
            const { symbol, start, end, threshold } = this.getAnomalyParams();
            if (!start || !end) {
                this.addLogMessage('請填寫開始與結束日期再執行修復', 'warning');
                return;
            }
            const deleteThenRefetch = !!document.getElementById('refetchOnlyToggle')?.checked;
            const refetchOnly = !deleteThenRefetch;
            const ok = window.confirm(deleteThenRefetch
                ? `將對 ${symbol || '全部股票'} 在 ${start}~${end} 期間進行：偵測異常→備份異常日期→刪除異常日期→補抓異常日期，threshold=${threshold}。是否繼續？`
                : `將對 ${symbol || '全部股票'} 在 ${start}~${end} 期間進行：重抓寫回（不刪除舊資料、不備份；以 upsert 覆蓋同日資料），threshold=${threshold}。是否繼續？`
            );
            if (!ok) return;

            if (deleteThenRefetch) {
                if (!symbol) {
                    this.addLogMessage(`🧹 開始刪除後重抓（異常股票批次）：只處理異常日期（非整段重抓）。範圍=${start}~${end}，threshold=${threshold}`, 'info');
                    const endpoint = rewriteUrl('http://localhost:5003/api/prices/refetch_range_by_anomalies');
                    this.addLogMessage(`📡 呼叫：${endpoint}`, 'info');
                    const res = await fetch(endpoint, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ start, end, threshold, use_local_db: this.useLocalDb })
                    });
                    const data = await res.json();
                    if (!res.ok || !data.success) {
                        throw new Error(data.error || `HTTP ${res.status}`);
                    }
                    this.addLogMessage(`✅ 批次完成：處理 ${Array.isArray(data.symbols) ? data.symbols.length : 0} 檔`, 'success');
                    this.addLogMessage(`🗑️ 總刪除：${data.deleted || 0} 筆`, 'success');
                    this.addLogMessage(`⬇️ 總補抓：fetched=${data.fetched || 0}`, 'success');
                    this.addLogMessage(`💾 總寫入：inserted=${data.inserted || 0}`, 'success');
                    if (Array.isArray(data.details) && data.details.length) {
                        data.details.slice(0, 10).forEach((d, i) => {
                            const datesCnt = Array.isArray(d.anomaly_dates) ? d.anomaly_dates.length : 0;
                            this.addLogMessage(`${i + 1}. ${d.symbol} 異常日=${datesCnt} 刪除=${d.deleted || 0} 補抓=${d.fetched || 0} 寫入=${d.inserted || 0}`, 'info');
                        });
                        if (data.details.length > 10) {
                            this.addLogMessage(`... 其餘 ${data.details.length - 10} 檔省略`, 'info');
                        }
                    }
                    return;
                }

                this.addLogMessage(`🧹 開始刪除後重抓：symbol=${symbol}, 範圍=${start}~${end}`, 'info');
                const endpoint = rewriteUrl('http://localhost:5003/api/prices/refetch_range');
                this.addLogMessage(`📡 呼叫：${endpoint}`, 'info');
                const res = await fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ symbol, start, end, use_local_db: this.useLocalDb })
                });
                const data = await res.json();
                if (!res.ok || !data.success) {
                    throw new Error(data.error || `HTTP ${res.status}`);
                }
                this.addLogMessage(`🗑️ 已刪除：${data.deleted || 0} 筆`, 'success');
                this.addLogMessage(`⬇️ 已重抓：fetched=${data.fetched || 0}`, 'success');
                this.addLogMessage(`💾 已寫入：inserted=${data.inserted || 0}`, 'success');
                return;
            }

            this.addLogMessage(`🧹 開始修復：symbol=${symbol || 'ALL'}, 範圍=${start}~${end}, 閾值=${threshold}${deleteThenRefetch ? '（刪除後重抓）' : '（僅重抓）'}`, 'info');
            const fixEndpoint = apiUrl('/api/anomalies/fix');
            this.addLogMessage(`📡 呼叫：${fixEndpoint}`, 'info');
            const res = await fetch(fixEndpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ symbol, start, end, threshold, ruleVersion: 'rules_v1_pct', refetchPaddingDays: 5, refetchOnly, use_local_db: this.useLocalDb })
            });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.error || `HTTP ${res.status}`);
            }
            this.addLogMessage(`✅ 修復完成：刪除 ${data.deleted || 0} 筆、重抓 ${data.refetched || 0} 筆。`, 'success');
            if (Array.isArray(data.details)) {
                data.details.slice(0, 5).forEach((d, i) => {
                    this.addLogMessage(`${i+1}. ${d.symbol} 受影響日期 ${d.dates.length} 筆，重抓 ${d.refetch_range.start}~${d.refetch_range.end}`, 'info');
                });
                if (data.details.length > 5) {
                    this.addLogMessage(`... 其餘 ${data.details.length - 5} 檔省略`, 'info');
                }
            }
        } catch (err) {
            this.addLogMessage(`修復異常失敗：${err.message}`, 'error');
        }
    }

    async fixAnomaliesStream() {
        try {
            const deleteThenRefetch = !!document.getElementById('refetchOnlyToggle')?.checked;
            if (deleteThenRefetch) {
                this.addLogMessage('ℹ️ 已勾選「刪除後重抓」：改用非串流模式執行（只刪除異常日期→補抓異常日期）', 'info');
                return this.fixAnomalies();
            }

            const { symbol, start, end, threshold } = this.getAnomalyParams();
            if (!start || !end) {
                this.addLogMessage('請填寫開始與結束日期再執行（串流）修復', 'warning');
                return;
            }
            // 串流端點僅做重抓（不刪除、不備份）
            const ok = window.confirm(`將對 ${symbol || '全部股票'} 在 ${start}~${end} 期間進行：僅重抓（不刪除，不備份），threshold=${threshold}。是否繼續？`);
            if (!ok) return;

            const qs = new URLSearchParams();
            if (symbol) qs.set('symbol', symbol);
            qs.set('start', start);
            qs.set('end', end);
            qs.set('threshold', String(threshold));
            // 可選：擴邊天數
            const pad = 5;
            qs.set('refetchPaddingDays', String(pad));

            if (this.useLocalDb) qs.set('use_local_db', 'true');
            const url = apiUrl(`/api/anomalies/fix_stream?${qs.toString()}`);
            this.addLogMessage(`🧹（串流）開始修復：symbol=${symbol || 'ALL'}, ${start}~${end}, threshold=${threshold}`, 'info');
            this.addLogMessage(`📡 呼叫：${url}`, 'info');

            // 進度狀態
            let processed = 0;
            let total = 0;
            let totalInserted = 0;

            const es = new EventSource(url);

            es.onmessage = (ev) => {
                try {
                    const msg = JSON.parse(ev.data);
                    if (msg.type === 'start') {
                        this.addLogMessage(`🚀 開始：${msg.start}~${msg.end}, threshold=${msg.threshold}`, 'info');
                    } else if (msg.type === 'symbol_start') {
                        processed += 1; // 粗略進度（以symbol為單位）
                        const { symbol, refetch_range } = msg;
                        this.addLogMessage(`▶️ ${symbol} 重抓範圍：${refetch_range.start}~${refetch_range.end}`, 'info');
                    } else if (msg.type === 'symbol_done') {
                        totalInserted += (msg.inserted || 0);
                        const pv = msg.preview || [];
                        this.addLogMessage(`✅ ${msg.symbol} 已匯入 ${msg.inserted || 0} 筆。預覽前 ${pv.length} 筆：`, 'success');
                        pv.forEach((r, i) => {
                            this.addLogMessage(`  ${i+1}. ${msg.symbol} ${r.date} O:${r.open} H:${r.high} L:${r.low} C:${r.close} V:${r.volume}`, 'info');
                        });
                    } else if (msg.type === 'done') {
                        total = msg.count || 0;
                        es.close();
                        this.addLogMessage(`🏁 完成：共偵測 ${total} 筆異常點，重抓匯入 ${msg.refetched || 0} 筆（累計）。`, 'success');
                        if (Array.isArray(msg.details)) {
                            const show = msg.details.slice(0, 5);
                            show.forEach((d, i) => {
                                this.addLogMessage(`${i+1}. ${d.symbol} 匯入 ${d.inserted} 筆，範圍 ${d.refetch_range.start}~${d.refetch_range.end}`, 'info');
                            });
                            if (msg.details.length > show.length) {
                                this.addLogMessage(`... 其餘 ${msg.details.length - show.length} 檔省略`, 'info');
                            }
                        }
                    } else if (msg.type === 'error') {
                        es.close();
                        this.addLogMessage(`❌ 串流錯誤：${msg.message}`, 'error');
                    }
                } catch (e) {
                    console.error('SSE parse error:', e, ev.data);
                }
            };

            es.onerror = (e) => {
                console.error('SSE error', e);
                this.addLogMessage('❌ 串流連線錯誤，已中斷', 'error');
                try { es.close(); } catch {}
            };
        } catch (err) {
            this.addLogMessage(`修復異常（串流）失敗：${err.message}`, 'error');
        }
    }

    // 簡單延遲
    sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

    // 自動化實驗：依多組參數自動執行、等待完成並導出日誌
    async runAutoExperiments() {
        if (this.isUpdating) {
            this.addLogMessage('目前有更新進行中，請稍後再開始自動實驗。', 'warning');
            return;
        }
        const autoBtn = document.getElementById('startAutoExperiments');
        if (autoBtn) autoBtn.disabled = true;

        try {
            // 定義參數組合（可依需求調整）
            const batchSizes = [10, 30, 50];
            const concurrencies = [10, 20, 40];
            const interBatchDelays = [300];

            // 若 UI 有當前其它設定（如股票數量/日期），保留不動，只調效能參數
            for (const b of batchSizes) {
                for (const c of concurrencies) {
                    for (const d of interBatchDelays) {
                        // 設置 UI 效能參數
                        const bs = document.getElementById('inputBatchSize');
                        const cc = document.getElementById('inputConcurrency');
                        const dd = document.getElementById('inputInterBatchDelay');
                        if (bs) bs.value = String(b);
                        if (cc) cc.value = String(c);
                        if (dd) dd.value = String(d);

                        // 方案A：每組開始前清空日誌，確保匯出只包含本組內容
                        this.clearLog();
                        this.addLogMessage(`[AUTO] Params B=${b} C=${c} D=${d}ms`, 'info');

                        // 紀錄開始
                        this.addLogMessage(`🧪 開始自動實驗：BatchSize=${b}, Concurrency=${c}, Delay=${d}ms`, 'info');

                        // 執行一次更新，等待完成（直接覆寫效能參數，避免讀到舊 UI 值）
                        const startTs = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                        await this.executeUpdate({
                            batchSize: b,
                            concurrency: c,
                            interBatchDelay: d
                        });
                        const endTs = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                        const elapsedMs = Math.round(endTs - startTs);

                        // 導出本次日誌（檔名含日期、參數、耗時）
                        const ts = new Date().toISOString().replace(/[:.]/g, '-');
                        const name = `app_log_${ts}_b${b}_c${c}_d${d}_t${elapsedMs}ms`;
                        this.exportLog(name);

                        // 每組之間小延遲，避免壓力尖峰
                        await this.sleep(1000);
                    }
                }
            }
            this.addLogMessage('✅ 自動實驗全部完成', 'success');
        } catch (err) {
            this.addLogMessage(`自動實驗發生錯誤：${err.message}`, 'error');
        } finally {
            if (autoBtn) autoBtn.disabled = false;
        }
    }

    // 將毫秒轉為可讀字串（例如 1小時 2分 3秒 或 2分 5秒）
    formatDuration(ms) {
        const totalSeconds = Math.max(0, Math.floor(ms / 1000));
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;
        const parts = [];
        if (hours > 0) parts.push(`${hours}小時`);
        if (minutes > 0) parts.push(`${minutes}分`);
        parts.push(`${seconds}秒`);
        return parts.join(' ');
    }

    // 受控並發執行器：以指定的並行數處理任務陣列
    async runWithConcurrency(items, limit, worker) {
        const results = [];
        let index = 0;
        const workers = new Array(Math.min(limit, items.length)).fill(0).map(async () => {
            while (true) {
                let currentIndex;
                // 取得下一個索引
                if (index >= items.length) break;
                currentIndex = index++;
                const item = items[currentIndex];
                try {
                    const res = await worker(item, currentIndex);
                    results[currentIndex] = { status: 'fulfilled', value: res };
                } catch (err) {
                    results[currentIndex] = { status: 'rejected', reason: err };
                }
            }
        });
        await Promise.all(workers);
        return results;
    }

    init() {
        this.setupEventListeners();
        this.setupStatsEventListeners(); // 設置統計功能事件監聽器
        this.setupDatabaseTargetToggle();
        this.setupBwibbuListeners();
        this.setupT86Listeners();
        this.setupMarginListeners();
        this.setupRevenueListeners();
        this.setupIncomeListeners();
        this.setupBalanceListeners();
        this.setupCashflowListeners();
        this.setupRatiosListeners();
        this.setupWarrantsListeners();
        this.initializeDates();
        this.initializeDisplayAreas();
        this.checkDatabaseConnection();
        this.addLogMessage('系統已啟動', 'info');
        
        // 延遲初始化默認選項，確保 DOM 完全載入
        setTimeout(() => {
            this.initializeDefaultOptions();
            this.loadStatistics(); // 載入統計數據
            this.loadBwibbuData();
        }, 100);

        // Init new UI behaviors
        this.initSummaryBar();
        this.initLogControls();
        this.startApiHealthPolling();

        // Symbols list UI
        this.setupSymbolsListUI();
    }

    setupSymbolsListUI() {
        const table = document.getElementById('symbolsTable');
        if (!table) return;
        const reloadBtn = document.getElementById('symbolsReloadBtn');
        const refreshBtn = document.getElementById('symbolsRefreshFromExchangesBtn');
        const etfRefreshBtn = document.getElementById('etfRefreshBtn');
        const searchInput = document.getElementById('symbolsSearchInput');

        if (reloadBtn) {
            reloadBtn.addEventListener('click', () => this.loadSymbolsList(true));
        }
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refreshSymbolsFromExchangesBoth());
        }
        if (etfRefreshBtn) {
            etfRefreshBtn.addEventListener('click', () => this.refreshEtfNamesBoth());
        }
        if (searchInput) {
            searchInput.addEventListener('input', () => this.renderSymbolsTable());
        }

        // Initial load
        this.loadSymbolsList(false);
    }

    setEtfRefreshStatus(text, visible) {
        const el = document.getElementById('etfRefreshStatus');
        if (!el) return;
        el.textContent = text || '';
        el.style.display = visible ? '' : 'none';
    }

    async refreshEtfNamesBoth() {
        const btn = document.getElementById('etfRefreshBtn');
        if (btn) btn.disabled = true;
        try {
            this.setEtfRefreshStatus('更新中：正在更新本機與雲端的 ETF 名稱，請稍候...', true);

            const resp = await fetch('http://localhost:5003/api/symbols/refresh_etf_names', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ target: 'both', table: 'tw_stock_symbols' })
            });
            const data = await resp.json();
            if (!resp.ok || !data) {
                throw new Error(`HTTP ${resp.status}`);
            }
            if (!data.success) {
                const results = Array.isArray(data.results) ? data.results : [];
                const msg = results.map(r => `${r.mode}: ${r.ok ? 'OK' : 'FAIL'}${r.stderr ? ` (${String(r.stderr).trim().slice(0, 200)})` : ''}`).join(' | ') || (data.error || '更新失敗');
                throw new Error(msg);
            }

            const results = Array.isArray(data.results) ? data.results : [];
            const summary = results.map(r => `${r.mode}: ${r.ok ? 'OK' : 'FAIL'}`).join(' | ');
            const details = results
                .map(r => {
                    const detail = (r && (r.stderr || r.stdout)) ? String(r.stderr || r.stdout).trim() : '';
                    const short = detail.length > 240 ? detail.slice(0, 240) + '...' : detail;
                    return `${r.mode}: ${r.ok ? 'OK' : 'FAIL'}${short ? ` (${short})` : ''}`;
                })
                .join(' | ');
            this.setEtfRefreshStatus(`更新完成：${details || summary}`, true);

            await this.loadSymbolsList(true);
        } catch (err) {
            this.setEtfRefreshStatus(`更新失敗：${err.message}`, true);
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    setSymbolsRefreshStatus(text, visible) {
        const el = document.getElementById('symbolsRefreshStatus');
        if (!el) return;
        el.textContent = text || '';
        el.style.display = visible ? '' : 'none';
    }

    async refreshSymbolsFromExchangesBoth() {
        const btn = document.getElementById('symbolsRefreshFromExchangesBtn');
        if (btn) btn.disabled = true;

        try {
            this.setSymbolsRefreshStatus('更新中：正在更新本機與雲端的股票名稱，請稍候...', true);

            const resp = await fetch('http://localhost:5003/api/symbols/refresh_from_exchanges', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ target: 'both', table: 'tw_stock_symbols' })
            });
            const data = await resp.json();
            if (!resp.ok || !data) {
                throw new Error(`HTTP ${resp.status}`);
            }
            if (!data.success) {
                const msg = data.error || (Array.isArray(data.results)
                    ? data.results.map(r => {
                        const detail = (r && (r.stderr || r.stdout)) ? String(r.stderr || r.stdout).trim() : 'failed';
                        const short = detail.length > 240 ? detail.slice(0, 240) + '...' : detail;
                        return `${r.mode}: ${short}`;
                    }).join(' | ')
                    : '更新失敗');
                throw new Error(msg);
            }

            const results = Array.isArray(data.results) ? data.results : [];
            const summary = results.map(r => `${r.mode}: ${r.ok ? 'OK' : 'FAIL'}`).join(' | ');
            const details = results
                .map(r => {
                    const detail = (r && (r.stderr || r.stdout)) ? String(r.stderr || r.stdout).trim() : '';
                    const short = detail.length > 240 ? detail.slice(0, 240) + '...' : detail;
                    return `${r.mode}: ${r.ok ? 'OK' : 'FAIL'}${short ? ` (${short})` : ''}`;
                })
                .join(' | ');
            this.setSymbolsRefreshStatus(`更新完成：${details || summary}`, true);

            // Reload current target list (depends on db toggle)
            await this.loadSymbolsList(true);
        } catch (err) {
            this.setSymbolsRefreshStatus(`更新失敗：${err.message}`, true);
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async loadSymbolsList(forceRefresh) {
        const emptyRow = document.getElementById('symbolsTableEmptyRow');
        if (emptyRow) {
            emptyRow.style.display = '';
            emptyRow.querySelector('h4') && (emptyRow.querySelector('h4').textContent = '載入中...');
            emptyRow.querySelector('p') && (emptyRow.querySelector('p').textContent = '正在抓取股票清單');
        }

        try {
            const qs = new URLSearchParams();
            if (this.useLocalDb) qs.set('use_local_db', 'true');
            if (forceRefresh) qs.set('refresh', 'true');

            const url = `http://localhost:5003/api/symbols${qs.toString() ? `?${qs.toString()}` : ''}`;
            const resp = await fetch(url);
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            this.symbolsList = Array.isArray(data.data) ? data.data : [];
            this.renderSymbolsTable();
        } catch (err) {
            this.symbolsList = [];
            this.renderSymbolsTable(err);
        }
    }

    renderSymbolsTable(error) {
        const table = document.getElementById('symbolsTable');
        const badge = document.getElementById('symbolsCountBadge');
        const searchInput = document.getElementById('symbolsSearchInput');

        if (!table) return;
        const tbody = table.querySelector('tbody');
        if (!tbody) return;

        const q = (searchInput && searchInput.value ? searchInput.value : '').trim().toLowerCase();

        let list = Array.isArray(this.symbolsList) ? this.symbolsList : [];
        if (q) {
            list = list.filter((it) => {
                const symbol = String(it.symbol || '').toLowerCase();
                const name = String(it.name || '').toLowerCase();
                const market = String(it.market || '').toLowerCase();
                const shortName = String(it.short_name || it.shortName || '').toLowerCase();
                const industry = String(it.industry || '').toLowerCase();
                return (
                    symbol.includes(q) ||
                    name.includes(q) ||
                    market.includes(q) ||
                    shortName.includes(q) ||
                    industry.includes(q)
                );
            });
        }

        if (badge) badge.textContent = `${list.length} 筆`;

        // Clear tbody
        tbody.innerHTML = '';

        if (error) {
            const tr = document.createElement('tr');
            tr.className = 'no-data-row';
            tr.innerHTML = `
                <td colspan="5" class="no-data-cell">
                    <div class="no-data-content">
                        <div class="no-data-icon"><i class="fas fa-triangle-exclamation"></i></div>
                        <div class="no-data-text">
                            <h4>載入失敗</h4>
                            <p>${this.escapeHtml(String(error.message || error))}</p>
                        </div>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
            return;
        }

        if (!list.length) {
            const tr = document.createElement('tr');
            tr.className = 'no-data-row';
            tr.innerHTML = `
                <td colspan="5" class="no-data-cell">
                    <div class="no-data-content">
                        <div class="no-data-icon"><i class="fas fa-search"></i></div>
                        <div class="no-data-text">
                            <h4>沒有符合的資料</h4>
                            <p>請調整搜尋條件或點擊重新載入</p>
                        </div>
                    </div>
                </td>
            `;
            tbody.appendChild(tr);
            return;
        }

        const frag = document.createDocumentFragment();
        list.slice(0, 2000).forEach((it) => {
            const symbol = it.symbol || '';
            const name = it.name || '';
            const market = it.market || '';
            const shortName = it.short_name || it.shortName || '';
            const industry = it.industry || '';

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><button type="button" class="btn btn-outline" data-symbol="${this.escapeHtml(symbol)}" style="padding:0.25rem 0.6rem; font-size:0.85rem;">${this.escapeHtml(symbol)}</button></td>
                <td>${this.escapeHtml(name)}</td>
                <td>${this.escapeHtml(market)}</td>
                <td>${this.escapeHtml(shortName)}</td>
                <td>${this.escapeHtml(industry)}</td>
            `;

            const btn = tr.querySelector('button[data-symbol]');
            if (btn) {
                btn.addEventListener('click', () => {
                    const input = document.getElementById('tickerInput');
                    if (!input) return;
                    input.value = String(symbol || '').trim();
                    input.focus();
                });
            }

            frag.appendChild(tr);
        });
        tbody.appendChild(frag);
    }

    escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    setupEventListeners() {
        console.log('🔧 設置事件監聽器...');
        
        // Modern Tab navigation
        const tabBtns = document.querySelectorAll('.modern-tab-btn');
        console.log(`找到 ${tabBtns.length} 個現代化標籤按鈕`);
        tabBtns.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const tab = btn.dataset.tab;
                console.log(`點擊標籤: ${tab}`);
                this.switchTab(tab);
            });
        });

        // 初始化新的 UI 切換功能
        this.initializeToggleOptions();
        this.initializeActionStatus();

        // Update functionality - 確保按鈕存在
        console.log('🔍 正在查找更新按鈕...');
        const executeBtn = document.getElementById('executeUpdate');
        const cancelBtn = document.getElementById('cancelUpdate');
        
        console.log('executeBtn:', executeBtn);
        console.log('cancelBtn:', cancelBtn);
        
        if (executeBtn) {
            console.log('✅ 找到執行按鈕，綁定事件');
            
            // 移除可能存在的舊事件監聽器
            executeBtn.replaceWith(executeBtn.cloneNode(true));
            const newExecuteBtn = document.getElementById('executeUpdate');
            
            newExecuteBtn.addEventListener('click', (e) => {
                e.preventDefault();
                console.log('🚀 執行按鈕被點擊');
                this.executeUpdate();
            });
            
            // 測試按鈕是否可點擊
            console.log('按鈕狀態 - disabled:', newExecuteBtn.disabled);
            console.log('按鈕樣式 - display:', window.getComputedStyle(newExecuteBtn).display);
            
        } else {
            console.error('❌ 未找到執行按鈕 #executeUpdate');
            console.log('所有按鈕元素:', document.querySelectorAll('button'));
        }
        
        if (cancelBtn) {
            console.log('✅ 找到取消按鈕，綁定事件');
            cancelBtn.addEventListener('click', () => {
                console.log('⏹️ 取消按鈕被點擊');
                this.cancelUpdate();
            });
        } else {
            console.error('❌ 未找到取消按鈕 #cancelUpdate');
        }

        // Query functionality - 安全綁定
        this.safeAddEventListener('executeQuery', () => this.executeQueryData());
        this.safeAddEventListener('exportQuery', () => this.exportQueryResults());
        this.safeAddEventListener('clearQuery', () => this.clearQueryResults());

        // Stats functionality
        this.safeAddEventListener('refreshStats', () => this.refreshDatabaseStats());

        // Settings functionality
        this.safeAddEventListener('testConnection', () => this.testDatabaseConnection());

        // Batch update functionality
        this.safeAddEventListener('updateAllListedBtn', () => this.updateAllListedStocks());
        this.safeAddEventListener('updateAllOtcBtn', () => this.updateAllOtcStocks());
        this.safeAddEventListener('saveSettings', () => this.saveSettings());

        // Log functionality
        this.safeAddEventListener('clearLog', () => this.clearLog());
        this.safeAddEventListener('exportLog', () => this.exportLog());

        // Database Sync functionality
        this.safeAddEventListener('btnCheckNeon', () => this.checkNeonConnection());
        this.safeAddEventListener('btnStartSync', () => this.startDatabaseSync());
        this.safeAddEventListener('btnDownloadFromNeon', () => this.startDatabaseDownload());
        this.safeAddEventListener('btnClearSyncLog', () => this.clearSyncLog());

        // Auto experiments
        this.safeAddEventListener('startAutoExperiments', () => this.runAutoExperiments());
        
        // Anomaly detection & fix
        this.safeAddEventListener('detectAnomaliesBtn', () => this.detectAnomalies());
        this.safeAddEventListener('exportAnomaliesBtn', () => this.exportAnomalies());
        // 勾選「刪除後重抓」時走非串流（整段刪除→重抓）；未勾選才走串流版本（僅重抓 upsert）
        this.safeAddEventListener('fixAnomaliesBtn', () => {
            const deleteThenRefetch = !!document.getElementById('refetchOnlyToggle')?.checked;
            if (deleteThenRefetch) return this.fixAnomalies();
            return this.fixAnomaliesStream();
        });
        // Returns compute
        this.safeAddEventListener('computeReturnsBtn', () => this.computeReturnsFromUI());
        // 匯入加權指數 (^TWII) 日K（yfinance）
        this.safeAddEventListener('importTwiiBtn', () => this.importTwiiFromYFinance());
        // 匯入櫃買指數 (^OTC) 日K（TPEX OpenAPI）
        this.safeAddEventListener('importOtcBtn', () => this.importOtcFromTpexApi());
        
        console.log('✅ 事件監聽器設置完成');
    }

    /** =========================
     *  權證資料 (Warrants)
     *  ========================= */
    setupWarrantsListeners() {
        const searchBtn = document.getElementById('warrantsSearchBtn');
        const dateSelect = document.getElementById('warrantsDateSelect');
        const keywordInput = document.getElementById('warrantsKeyword');
        const importBtn = document.getElementById('warrantsImportBtn');
        const marketSelect = document.getElementById('warrantsMarketSelect');
        const tpexMasterImportBtn = document.getElementById('warrantsTpexMasterImportBtn');
        const tpexDailyImportBtn = document.getElementById('warrantsTpexDailyImportBtn');

        if (searchBtn) {
            searchBtn.addEventListener('click', () => {
                this.fetchWarrants();
            });
        }
        if (keywordInput) {
            keywordInput.addEventListener('keyup', (ev) => {
                if (ev.key === 'Enter') {
                    this.fetchWarrants();
                }
            });
        }

        if (importBtn) {
            importBtn.addEventListener('click', () => {
                this.importLatestWarrants();
            });
        }

        if (marketSelect) {
            marketSelect.addEventListener('change', async () => {
                await this.loadWarrantsDates();
                await this.fetchWarrants();
            });
        }

        if (tpexMasterImportBtn) {
            tpexMasterImportBtn.addEventListener('click', () => {
                this.importTpexWarrantMaster();
            });
        }

        if (tpexDailyImportBtn) {
            tpexDailyImportBtn.addEventListener('click', () => {
                this.importTpexWarrantDaily();
            });
        }

        // 當首次切換到權證模式時載入日期
        this._warrantsInitialized = false;
    }

    async ensureWarrantsInitialized() {
        if (this._warrantsInitialized) return;
        await this.loadWarrantsDates();
        this._warrantsInitialized = true;
    }

    async loadWarrantsDates() {
        const statusEl = document.getElementById('warrantsStatus');
        const selectEl = document.getElementById('warrantsDateSelect');
        const marketEl = document.getElementById('warrantsMarketSelect');
        if (!selectEl) return;
        try {
            if (statusEl) statusEl.textContent = '載入可用日期中...';
            const params = new URLSearchParams();
            if (this.useLocalDb) params.set('use_local_db', 'true');
            params.set('limit', '120');
            const market = marketEl && marketEl.value ? marketEl.value : 'twse';
            params.set('market', market);
            const resp = await fetch(`http://localhost:5003/api/warrants/dates?${params.toString()}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }
            const dates = Array.isArray(data.dates) ? data.dates : [];
            selectEl.innerHTML = '';
            if (!dates.length) {
                const opt = document.createElement('option');
                opt.value = '';
                opt.textContent = '（尚無資料）';
                selectEl.appendChild(opt);
                if (statusEl) statusEl.textContent = `尚無 ${market.toUpperCase()} 權證資料，請先匯入。`;
                return;
            }
            dates.forEach(d => {
                const opt = document.createElement('option');
                opt.value = d;
                opt.textContent = d;
                selectEl.appendChild(opt);
            });
            selectEl.value = dates[0];
            if (statusEl) statusEl.textContent = `已載入 ${market.toUpperCase()} ${dates.length} 個日期，可選擇後查詢。`;
        } catch (err) {
            console.error('loadWarrantsDates error', err);
            if (statusEl) statusEl.textContent = `載入日期失敗：${err.message}`;
        }
    }

    async fetchWarrants() {
        const statusEl = document.getElementById('warrantsStatus');
        const selectEl = document.getElementById('warrantsDateSelect');
        const keywordEl = document.getElementById('warrantsKeyword');
        const tbody = document.getElementById('warrantsTableBody');
        const marketEl = document.getElementById('warrantsMarketSelect');
        if (!selectEl || !tbody) return;

        const date = selectEl.value;
        const keyword = (keywordEl && keywordEl.value ? keywordEl.value.trim() : '');
        const market = marketEl && marketEl.value ? marketEl.value : 'twse';

        try {
            if (statusEl) statusEl.textContent = '查詢中...';
            tbody.innerHTML = '<tr><td colspan="8" class="no-data-cell">查詢中...</td></tr>';

            const params = new URLSearchParams();
            if (date) params.set('date', date);
            if (keyword) params.set('keyword', keyword);
            params.set('market', market);
            params.set('page', '1');
            params.set('pageSize', '200');
            if (this.useLocalDb) params.set('use_local_db', 'true');

            const resp = await fetch(`http://localhost:5003/api/warrants?${params.toString()}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            const rows = Array.isArray(data.data) ? data.data : [];
            if (!rows.length) {
                tbody.innerHTML = `<tr class="no-data-row"><td colspan="8" class="no-data-cell">所選日期無資料</td></tr>`;
                if (statusEl) statusEl.textContent = '查無資料';
                return;
            }

            const fmtNum = (v) => {
                if (v === null || v === undefined) return '';
                const n = Number(v);
                return Number.isFinite(n) ? n.toLocaleString() : String(v);
            };

            tbody.innerHTML = rows.map(r => `
                <tr>
                    <td>${r.market || market.toUpperCase()}</td>
                    <td>${r.trade_date || ''}</td>
                    <td>${r.warrant_code || ''}</td>
                    <td>${r.warrant_name || ''}</td>
                    <td>${r.underlying_code || ''}</td>
                    <td>${r.underlying_name || ''}</td>
                    <td class="text-right">${fmtNum(r.trade_value ?? r.turnover)}</td>
                    <td class="text-right">${fmtNum(r.trade_volume ?? r.volume)}</td>
                </tr>
            `).join('');

            if (statusEl) {
                const marketLabel = market === 'both' ? '全市場' : market.toUpperCase();
                statusEl.textContent = `${marketLabel} 日期 ${data.date || date || ''}，共 ${data.total || rows.length} 筆（前 ${rows.length} 筆已顯示）`;
            }
        } catch (err) {
            console.error('fetchWarrants error', err);
            tbody.innerHTML = `<tr class="no-data-row"><td colspan="8" class="no-data-cell">查詢失敗：${err.message}</td></tr>`;
            if (statusEl) statusEl.textContent = `查詢失敗：${err.message}`;
        }
    }

    async pollWarrantImportStatus(type) {
        const statusEl = document.getElementById('warrantsStatus');
        try {
            if (type === 'twse') {
                const respStatus = await fetch('http://localhost:5003/api/warrants/import-status');
                const statusJson = await respStatus.json();
                if (!respStatus.ok || !statusJson.success) return;
                const s = statusJson.status || {};
                if (!s.running || !statusEl) return;
                statusEl.textContent = s.total
                    ? `TWSE 匯入中... 已處理 ${s.processed}/${s.total} 筆`
                    : `TWSE 匯入中... 已處理 ${s.processed} 筆`;
                return;
            }

            const respStatus = await fetch('http://localhost:5003/api/warrants/tpex/import-status');
            const statusJson = await respStatus.json();
            if (!respStatus.ok || !statusJson.success || !statusEl) return;
            const s = type === 'tpex-master' ? (statusJson.master || {}) : (statusJson.daily || {});
            if (!s.running) return;
            const label = type === 'tpex-master' ? 'TPEX 主檔' : 'TPEX 日行情';
            statusEl.textContent = s.total
                ? `${label}匯入中... 已處理 ${s.processed}/${s.total} 筆`
                : `${label}匯入中... 已處理 ${s.processed} 筆`;
        } catch (pollErr) {
            console.error('warrants import-status poll error', pollErr);
        }
    }

    clearWarrantsImportTimer() {
        if (this._warrantsImportTimer) {
            clearInterval(this._warrantsImportTimer);
            this._warrantsImportTimer = null;
        }
    }

    startWarrantsImportPolling(type) {
        this.clearWarrantsImportTimer();
        this._warrantsImportTimer = setInterval(() => {
            this.pollWarrantImportStatus(type);
        }, 1000);
    }

    async importLatestWarrants() {
        const statusEl = document.getElementById('warrantsStatus');
        try {
            if (statusEl) statusEl.textContent = '正在從 TWSE 抓取最新權證資料並匯入，請稍候...';
            this.startWarrantsImportPolling('twse');

            const params = new URLSearchParams();
            if (this.useLocalDb) params.set('use_local_db', 'true');

            const resp = await fetch(`http://localhost:5003/api/warrants/import-latest?${params.toString()}`, {
                method: 'POST',
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            const msg = data.message || '匯入完成';
            const tradeDate = data.tradeDate || '';
            if (statusEl) statusEl.textContent = `${msg}${tradeDate ? `（交易日期：${tradeDate}）` : ''}`;

            // 匯入成功後重新載入日期下拉
            await this.loadWarrantsDates();
        } catch (err) {
            console.error('importLatestWarrants error', err);
            if (statusEl) statusEl.textContent = `匯入失敗：${err.message}`;
        } finally {
            this.clearWarrantsImportTimer();
        }
    }

    async importTpexWarrantMaster() {
        const statusEl = document.getElementById('warrantsStatus');
        try {
            if (statusEl) statusEl.textContent = '正在匯入 TPEX 權證主檔，請稍候...';
            this.startWarrantsImportPolling('tpex-master');

            const params = new URLSearchParams();
            if (this.useLocalDb) params.set('use_local_db', 'true');

            const resp = await fetch(`http://localhost:5003/api/warrants/tpex/import-master?${params.toString()}`, {
                method: 'POST',
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }
            if (statusEl) statusEl.textContent = `${data.message || 'TPEX 主檔匯入完成'}（${data.importedCount || 0} 筆）`;
        } catch (err) {
            console.error('importTpexWarrantMaster error', err);
            if (statusEl) statusEl.textContent = `TPEX 主檔匯入失敗：${err.message}`;
        } finally {
            this.clearWarrantsImportTimer();
        }
    }

    async importTpexWarrantDaily() {
        const statusEl = document.getElementById('warrantsStatus');
        const marketEl = document.getElementById('warrantsMarketSelect');
        try {
            if (statusEl) statusEl.textContent = '正在匯入 TPEX 權證日行情，請稍候...';
            this.startWarrantsImportPolling('tpex-daily');

            const params = new URLSearchParams();
            if (this.useLocalDb) params.set('use_local_db', 'true');

            const resp = await fetch(`http://localhost:5003/api/warrants/tpex/import-daily?${params.toString()}`, {
                method: 'POST',
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }
            if (statusEl) {
                statusEl.textContent = `${data.message || 'TPEX 日行情匯入完成'}${data.tradeDate ? `（交易日期：${data.tradeDate}）` : ''}`;
            }
            if (marketEl) marketEl.value = 'tpex';
            await this.loadWarrantsDates();
        } catch (err) {
            console.error('importTpexWarrantDaily error', err);
            if (statusEl) statusEl.textContent = `TPEX 日行情匯入失敗：${err.message}`;
        } finally {
            this.clearWarrantsImportTimer();
        }
    }

    /** =========================
     *  市場指標 (BWIBBU)
     *  ========================= */

    setupBwibbuListeners() {
        const refreshBtn = document.getElementById('refreshBwibbuBtn');
        const reloadBtn = document.getElementById('reloadBwibbuBtn');

        if (refreshBtn) {
            refreshBtn.addEventListener('click', async () => {
                if (this.bwibbuLoading) return;
                this.bwibbuLoading = true;
                refreshBtn.disabled = true;
                try {
                    await this.refreshBwibbuData();
                } finally {
                    this.bwibbuLoading = false;
                    refreshBtn.disabled = false;
                }
            });
        }

        if (reloadBtn) {
            reloadBtn.addEventListener('click', () => {
                if (this.bwibbuLoading) return;
                this.loadBwibbuData();
            });
        }
    }

    async refreshBwibbuData() {
        try {
            this.addLogMessage('🔄 正在刷新市場指標 (BWIBBU) ...', 'info');
            const cfg = this.getUpdateConfig?.();
            let useRange = cfg && cfg.valid && cfg.startDate && cfg.endDate;
            let endpoint = 'http://localhost:5003/api/twse/bwibbu/refresh';
            const payload = {};
            if (useRange) {
                endpoint = 'http://localhost:5003/api/twse/bwibbu/refresh_range';
                payload.start = cfg.startDate;
                payload.end = cfg.endDate;
            } else {
                payload.force_refresh = true;
                payload.fetch_only = false;
            }
            if (this.useLocalDb) {
                payload.use_local_db = true;
            }

            const resp = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            const inserted = data.totalInserted ?? data.inserted ?? 0;
            const processed = data.daysProcessed ?? (data.details ? data.details.length : 1);
            this.addLogMessage(`✅ 市場指標已刷新：處理 ${processed} 天，寫入 ${inserted} 筆`, 'success');
            await this.loadBwibbuData();
        } catch (err) {
            this.addLogMessage(`❌ 刷新市場指標失敗：${err.message}`, 'error');
            throw err;
        }
    }

    async loadBwibbuData(limit = null) {
        try {
            this.bwibbuLoading = true;
            const qs = new URLSearchParams();
            if (this.useLocalDb) {
                qs.set('use_local_db', 'true');
            }
            const cfg = this.getUpdateConfig?.();
            if (cfg && cfg.valid) {
                qs.set('start', cfg.startDate);
                qs.set('end', cfg.endDate);
            }
            // 顯示全部：不帶 limit 參數

            const resp = await fetch(`http://localhost:5003/api/twse/bwibbu?${qs.toString()}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            this.renderBwibbuTable(data.data || []);
            const lastEl = document.getElementById('bwibbuLastDate');
            if (lastEl) {
                const dateText = data.latestDate || '尚未載入';
                const usedDate = data.usedDate || dateText;
                const cnt = typeof data.count === 'number' ? data.count : (data.data ? data.data.length : 0);
                lastEl.textContent = `${dateText}（採用 ${usedDate}，已匯入資料庫，共${cnt}筆）`;
            }

            this.addLogMessage(`📊 已載入市場指標 ${data.count || (data.data ? data.data.length : 0)} 筆（全部）`, 'info');
        } catch (err) {
            this.renderBwibbuTable([]);
            const lastEl = document.getElementById('bwibbuLastDate');
            if (lastEl) {
                lastEl.textContent = '載入失敗';
            }
            this.addLogMessage(`❌ 載入市場指標失敗：${err.message}`, 'error');
        } finally {
            this.bwibbuLoading = false;
        }
    }

    renderBwibbuTable(rows) {
        const tbody = document.getElementById('bwibbuTableBody');
        if (!tbody) return;

        if (!rows || rows.length === 0) {
            tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:#777;">尚未載入資料</td></tr>`;
            return;
        }

        const fmt = (value, digits = 2) => {
            if (value === null || value === undefined || value === '' || value === '-') {
                return '';
            }
            const num = Number(value);
            return Number.isFinite(num) ? num.toFixed(digits) : String(value);
        };

        tbody.innerHTML = rows.map(row => `
            <tr>
                <td>${row.Code}</td>
                <td>${row.Name || ''}</td>
                <td class="text-right">${fmt(row.PEratio)}</td>
                <td class="text-right">${fmt(row.DividendYield)}</td>
                <td class="text-right">${fmt(row.PBratio)}</td>
            </tr>
        `).join('');
    }

    /** =========================
     *  三大法人 (T86)
     *  ========================= */
    setupT86Listeners() {
        // 綁定按鈕（直接綁定，若初始化時元素尚未載入，改由事件委派補強）
        this.safeAddEventListener('t86FetchBtn', () => {
            console.log('[T86] direct click -> fetch');
            this.fetchT86Data();
        });
        this.safeAddEventListener('t86ExportBtn', () => {
            console.log('[T86] direct click -> export');
            this.exportT86Csv();
        });

        // 事件委派（保險機制）
        if (!this._t86Delegated) {
            document.addEventListener('click', (ev) => {
                const fetchBtn = ev.target.closest && ev.target.closest('#t86FetchBtn');
                const exportBtn = ev.target.closest && ev.target.closest('#t86ExportBtn');
                if (fetchBtn) {
                    console.log('[T86] delegated click -> fetch');
                    ev.preventDefault();
                    this.fetchT86Data();
                } else if (exportBtn) {
                    console.log('[T86] delegated click -> export');
                    ev.preventDefault();
                    this.exportT86Csv();
                }
            });
            this._t86Delegated = true;
        }

        // 市場選擇徽章
        const marketSelect = document.getElementById('t86MarketSelect');
        const badge = document.getElementById('t86ModeBadge');
        if (marketSelect && badge) {
            const updateBadge = () => {
                const mapping = { both: 'TWSE + TPEX', twse: '僅 TWSE', tpex: '僅 TPEX' };
                badge.textContent = mapping[marketSelect.value] || 'TWSE + TPEX';
            };
            marketSelect.addEventListener('change', updateBadge);
            updateBadge();
        }

        this.initializeT86LogPanel();
        this.safeAddEventListener('t86LogClearBtn', () => this.clearT86Log());
    }

    initializeT86LogPanel() {
        const panel = document.getElementById('t86LogPanel');
        if (panel) {
            this.t86LogPanel = panel;
            if (!this._t86LogInitialized) {
                this._t86LogInitialized = true;
                this.setT86LogEmptyState();
            }
        }

        const clearBtn = document.getElementById('t86LogClearBtn');
        if (clearBtn && !clearBtn.dataset.bound) {
            clearBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                this.clearT86Log();
            });
            clearBtn.dataset.bound = 'true';
        }
    }

    setT86LogEmptyState() {
        if (!this.t86LogPanel) return;
        this.t86LogPanel.innerHTML = '';
        const empty = document.createElement('div');
        empty.className = 't86-log-empty';
        empty.style.color = '#94a3b8';
        empty.textContent = '尚未開始抓取';
        this.t86LogPanel.appendChild(empty);
    }

    clearT86Log(silent = false) {
        this.initializeT86LogPanel();
        if (!this.t86LogPanel) return;
        this.setT86LogEmptyState();
        if (!silent) {
            this.addT86Log('日誌已清空', 'info');
        }
    }

    addT86Log(message, level = 'info') {
        this.initializeT86LogPanel();
        if (!this.t86LogPanel) return;

        const panel = this.t86LogPanel;
        const empty = panel.querySelector('.t86-log-empty');
        if (empty) empty.remove();

        const colors = {
            info: '#60a5fa',
            success: '#4ade80',
            warning: '#fbbf24',
            error: '#f87171'
        };
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌'
        };

        const entry = document.createElement('div');
        entry.className = `t86-log-entry t86-log-${level}`;
        entry.style.display = 'flex';
        entry.style.alignItems = 'flex-start';
        entry.style.gap = '0.5rem';
        entry.style.padding = '0.4rem 0.6rem';
        entry.style.marginBottom = '0.35rem';
        entry.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        entry.style.background = 'rgba(15,23,42,0.55)';
        entry.style.borderRadius = '6px';
        entry.style.boxShadow = 'inset 0 0 0 1px rgba(148,163,184,0.08)';

        const timeSpan = document.createElement('span');
        timeSpan.className = 't86-log-time';
        timeSpan.style.fontFamily = "'Roboto Mono','Courier New',monospace";
        timeSpan.style.fontSize = '0.75rem';
        timeSpan.style.color = '#cbd5f5';
        timeSpan.style.minWidth = '3.6rem';
        timeSpan.textContent = this.getCurrentTimeString();

        const iconSpan = document.createElement('span');
        iconSpan.className = 't86-log-icon';
        iconSpan.style.minWidth = '1.2rem';
        iconSpan.textContent = icons[level] || '•';

        const textSpan = document.createElement('span');
        textSpan.className = 't86-log-text';
        textSpan.style.flex = '1';
        textSpan.style.color = colors[level] || '#e2e8f0';
        textSpan.style.whiteSpace = 'pre-wrap';
        textSpan.textContent = message;

        entry.appendChild(timeSpan);
        entry.appendChild(iconSpan);
        entry.appendChild(textSpan);
        panel.appendChild(entry);

        if (this.t86LogAutoScroll) {
            panel.scrollTop = panel.scrollHeight;
        }
    }

    getCurrentTimeString() {
        const now = new Date();
        return now.toLocaleTimeString('zh-TW', { hour12: false });
    }

    logT86DailyStats(dailyStats) {
        if (!Array.isArray(dailyStats) || dailyStats.length === 0) {
            this.addT86Log('沒有每日統計資料（可能為非交易日或來源無資料）', 'warning');
            return;
        }

        const totals = dailyStats.reduce((acc, day) => {
            acc.twse += Number(day.twse_count || 0);
            acc.tpex += Number(day.tpex_count || 0);
            acc.total += Number(day.total_count || 0);
            return acc;
        }, { twse: 0, tpex: 0, total: 0 });

        this.addT86Log(
            `每日統計：處理 ${dailyStats.length} 天，TWSE ${this.formatInteger(totals.twse)} 筆、TPEX ${this.formatInteger(totals.tpex)} 筆、合計 ${this.formatInteger(totals.total)} 筆`,
            'info'
        );

        const previewLimit = dailyStats.length <= 7 ? dailyStats.length : 5;
        dailyStats.slice(0, previewLimit).forEach((day) => {
            this.addT86Log(
                ` - ${this.formatDate(day.date)}：TWSE ${this.formatInteger(day.twse_count)}、TPEX ${this.formatInteger(day.tpex_count)}、合計 ${this.formatInteger(day.total_count)}`,
                'info'
            );
        });

        if (dailyStats.length > previewLimit) {
            this.addT86Log(`... 其餘 ${dailyStats.length - previewLimit} 天省略`, 'info');
        }
    }

    calculateDateRangeDays(start, end) {
        if (!start || !end) return 0;
        try {
            const startDate = new Date(start);
            const endDate = new Date(end);
            if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
                return 0;
            }
            const diffMs = Math.abs(endDate.getTime() - startDate.getTime());
            return Math.floor(diffMs / 86400000) + 1;
        } catch (error) {
            return 0;
        }
    }

    getT86MarketLabel(market) {
        const key = (market || 'both').toLowerCase();
        if (key === 'twse') return '僅 TWSE';
        if (key === 'tpex') return '僅 TPEX';
        return 'TWSE + TPEX';
    }

    estimateT86DurationSeconds(totalDays, sleepSeconds, market) {
        if (!Number.isFinite(totalDays) || totalDays <= 0) return 0;
        const baseSleep = Number.isFinite(sleepSeconds) && sleepSeconds > 0 ? sleepSeconds : 0.6;
        const perMarketMultiplier = (market || '').toLowerCase() === 'both' ? 2 : 1;
        const estimated = totalDays * perMarketMultiplier * (baseSleep + 0.35);
        return Math.max(1, Math.round(estimated));
    }

    startT86ProgressMonitor(totalDays, sleepSeconds, marketLabel) {
        this.stopT86ProgressMonitor();

        const safeLabel = marketLabel || 'TWSE + TPEX';
        const rangeHint = totalDays > 0 ? `${this.formatInteger(totalDays)} 天` : '多日';
        const intervalSeconds = Math.max(5, Math.min(15, (Number(sleepSeconds) || 0.6) * 2));
        this.addT86Log(`伺服器正在處理 ${rangeHint} 的資料（${safeLabel}），進度更新約每 ${Math.round(intervalSeconds)} 秒提示一次`, 'info');

        this._t86ProgressStart = (typeof performance !== 'undefined' && performance.now)
            ? performance.now()
            : Date.now();
        this._t86LastProgressLog = 0;

        if (typeof window === 'undefined' || typeof window.setInterval !== 'function') {
            return;
        }

        this._t86ProgressTimer = window.setInterval(() => {
            const now = (typeof performance !== 'undefined' && performance.now)
                ? performance.now()
                : Date.now();
            const elapsedSec = Math.max(0, (now - this._t86ProgressStart) / 1000);
            if (elapsedSec - this._t86LastProgressLog < intervalSeconds - 0.5) {
                return;
            }
            this._t86LastProgressLog = elapsedSec;
            const minutes = Math.floor(elapsedSec / 60);
            const seconds = Math.round(elapsedSec % 60);
            const durationLabel = minutes > 0
                ? `${minutes} 分 ${seconds.toString().padStart(2, '0')} 秒`
                : `${seconds} 秒`;
            const scopeHint = totalDays > 0 ? `範圍 ${this.formatInteger(totalDays)} 天` : '範圍較大';
            this.addT86Log(`⌛ 已等待 ${durationLabel}，${scopeHint}（${safeLabel}），請稍候...`, 'info');
        }, intervalSeconds * 1000);
    }

    stopT86ProgressMonitor() {
        if (this._t86ProgressTimer) {
            clearInterval(this._t86ProgressTimer);
            this._t86ProgressTimer = null;
        }
        this._t86ProgressStart = 0;
        this._t86LastProgressLog = 0;
    }

    async fetchT86Data() {
        try {
            const start = document.getElementById('t86StartDate')?.value;
            const end = document.getElementById('t86EndDate')?.value;
            const market = document.getElementById('t86MarketSelect')?.value || 'both';
            const sleep = parseFloat(document.getElementById('t86SleepSeconds')?.value || '0.6');

            if (!start || !end) {
                this.addLogMessage('請先選擇開始與結束日期', 'warning');
                this.addT86Log('請先選擇開始與結束日期', 'warning');
                return;
            }

            console.log('[T86] fetchT86Data start', { start, end, market, sleep });
            this.addLogMessage(`📥 抓取 T86：${start}~${end} 市場=${market.toUpperCase()} 間隔=${sleep}s`, 'info');
            this.clearT86Log(true);
            this.addT86Log(`開始抓取：${start} ~ ${end}，市場=${market.toUpperCase()}，間隔=${sleep}s`, 'info');

            const totalDays = this.calculateDateRangeDays(start, end);
            const marketLabel = this.getT86MarketLabel(market);
            const estimatedSeconds = this.estimateT86DurationSeconds(totalDays, sleep, market);
            if (totalDays > 0) {
                const durationLabel = estimatedSeconds > 0
                    ? `${this.formatInteger(estimatedSeconds)} 秒 (約 ${Math.max(1, Math.round(estimatedSeconds / 60))} 分)`
                    : '少於 1 分鐘';
                this.addT86Log(`範圍內共有 ${this.formatInteger(totalDays)} 天資料（${marketLabel}），預估完成時間 ${durationLabel}`, 'info');
            } else {
                this.addT86Log(`無法推算日期範圍天數，仍嘗試抓取（${marketLabel}）`, 'warning');
            }
            this.startT86ProgressMonitor(totalDays, sleep, marketLabel);

            this.updateActionStatus?.('running', '抓取三大法人資料中...');

            const params = new URLSearchParams({ start, end, market, sleep: String(sleep) });
            if (this.useLocalDb) {
                params.set('use_local_db', 'true');
            }
            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/t86/fetch`;
            this.addT86Log(`向伺服器發送請求：${requestUrl}`, 'info');
            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

            const resp = await fetch(`${requestUrl}?${params.toString()}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) throw new Error(data.error || `HTTP ${resp.status}`);

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);

            this.addT86Log(`伺服器回應成功，開始處理資料（耗時 ${elapsedSec} 秒）`, 'success');

            this.t86Data = data.data || [];
            this.t86DailyStats = data.daily_stats || [];
            this.updateT86Summary(data.summary || {});
            this.renderT86DailyTable();
            this.renderT86ResultsTable();
            this.addLogMessage(`✅ 完成抓取，共 ${data.count} 筆`, 'success');
            this.addT86Log(`資料已整理完成：共 ${this.formatInteger(data.count)} 筆`, 'success');

            const summary = data.summary || {};
            const perMarket = summary.per_market || {};
            this.addT86Log(
                `摘要：TWSE ${this.formatInteger(perMarket.TWSE || 0)} 筆、TPEX ${this.formatInteger(perMarket.TPEX || 0)} 筆、總筆數 ${this.formatInteger(summary.total_records || 0)} 筆`,
                'info'
            );
            this.addT86Log(
                `處理天數：${summary.days_processed || 0} 天（${summary.start_date || '--'} ~ ${summary.end_date || '--'}）`,
                'info'
            );

            this.logT86DailyStats(this.t86DailyStats);

            if (data.persist_enabled) {
                const inserted = Number(data.persisted || 0);
                const targetLabel = this.useLocalDb ? '本地資料庫' : 'Neon 雲端';
                const level = inserted > 0 ? 'success' : 'warning';
                const msg = inserted > 0
                    ? `資料庫寫入完成：${this.formatInteger(inserted)} 筆（${targetLabel}）`
                    : `資料庫未寫入新資料（回傳 ${inserted} 筆），請檢查設定`;
                this.addT86Log(msg, level);
            } else {
                this.addT86Log('此次抓取僅預覽資料（未寫入資料庫）', 'warning');
            }

            this.addT86Log('渲染完成，面板資料已更新', 'success');
            this.updateActionStatus?.('ready', '三大法人資料已更新');
        } catch (err) {
            console.error('[T86] fetch error', err);
            this.addLogMessage(`❌ T86 抓取失敗：${err.message}`, 'error');
            this.addT86Log(`T86 抓取失敗：${err.message}`, 'error');
            this.updateActionStatus?.('error', '抓取失敗');
        } finally {
            this.stopT86ProgressMonitor();
        }
    }

    async exportT86Csv() {
        const start = document.getElementById('t86StartDate')?.value;
        const end = document.getElementById('t86EndDate')?.value;
        const market = document.getElementById('t86MarketSelect')?.value || 'both';
        const sleep = document.getElementById('t86SleepSeconds')?.value || '0.6';
        if (!start || !end) {
            this.addLogMessage('請先選擇開始與結束日期再匯出', 'warning');
            this.addT86Log('請先選擇開始與結束日期再匯出', 'warning');
            return;
        }
        const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
        const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
        const params = new URLSearchParams({ start, end, market, sleep });
        if (this.useLocalDb) {
            params.set('use_local_db', 'true');
        }
        const url = `${base}/api/t86/export?${params.toString()}`;
        this.addLogMessage(`📤 匯出 T86 CSV: ${url}`, 'info');
        this.addT86Log(`執行 CSV 匯出：${url}`, 'info');
        window.open(url, '_blank');
    }

    updateT86Summary(summary) {
        const perMarket = summary.per_market || {};
        this.setTextContent('t86StatTWSE', this.formatInteger(perMarket.TWSE || 0));
        this.setTextContent('t86StatTPEX', this.formatInteger(perMarket.TPEX || 0));
        this.setTextContent('t86StatTotal', this.formatInteger(summary.total_records || 0));
        this.setTextContent('t86StatDays', summary.days_processed || 0);
        const badge = document.getElementById('t86SummaryBadge');
        if (badge) badge.textContent = `${summary.start_date || '--'} ~ ${summary.end_date || '--'}`;
    }

    renderT86DailyTable() {
        const tbody = document.querySelector('#t86DailyTable tbody');
        if (!tbody) return;
        if (!this.t86DailyStats || this.t86DailyStats.length === 0) {
            tbody.innerHTML = '<tr class="no-data-row"><td colspan="4" class="no-data-cell">尚未執行</td></tr>';
            return;
        }
        tbody.innerHTML = this.t86DailyStats.map(s => `
            <tr>
                <td>${this.formatDate(s.date)}</td>
                <td class="number">${this.formatInteger(s.twse_count)}</td>
                <td class="number">${this.formatInteger(s.tpex_count)}</td>
                <td class="number">${this.formatInteger(s.total_count)}</td>
            </tr>
        `).join('');
    }

    renderT86ResultsTable() {
        const tbody = document.querySelector('#t86ResultsTable tbody');
        if (!tbody) return;
        if (!this.t86Data || this.t86Data.length === 0) {
            tbody.innerHTML = `
                <tr class="no-data-row">
                    <td colspan="10" class="no-data-cell">
                        <div class="no-data-content">
                            <div class="no-data-icon"><i class="fas fa-search"></i></div>
                            <div class="no-data-text">
                                <h4>尚無資料</h4>
                                <p>請先設定日期與市場後執行抓取</p>
                            </div>
                        </div>
                    </td>
                </tr>`;
            return;
        }
        const rows = this.t86Data.slice(0, 200).map(r => `
            <tr>
                <td>${this.formatDate(r.date)}</td>
                <td>${r.market || ''}</td>
                <td>${r.stock_no || ''}</td>
                <td>${r.stock_name || ''}</td>
                <td class="number">${this.formatInteger(r.foreign_buy)}</td>
                <td class="number">${this.formatInteger(r.foreign_sell)}</td>
                <td class="number">${this.formatInteger(r.foreign_net)}</td>
                <td class="number">${this.formatInteger(r.investment_trust_net)}</td>
                <td class="number">${this.formatInteger(r.dealer_total_net)}</td>
                <td class="number">${this.formatInteger(r.overall_net)}</td>
            </tr>
        `).join('');
        const extra = this.t86Data.length > 200 ? `<tr><td colspan="10" class="text-muted">僅顯示前 200 筆（共 ${this.formatInteger(this.t86Data.length)} 筆）</td></tr>` : '';
        tbody.innerHTML = rows + extra;
    }

    /** =========================
     *  融資融券 (Margin)
     *  ========================= */

    setupMarginListeners() {
        // 按鈕直接綁定
        this.safeAddEventListener('marginFetchBtn', () => {
            console.log('[Margin] direct click -> fetch');
            this.fetchMarginData();
        });
        this.safeAddEventListener('marginExportBtn', () => {
            console.log('[Margin] direct click -> export');
            this.exportMarginCsv();
        });

        // 市場選擇徽章
        const marketSelect = document.getElementById('marginMarketSelect');
        const badge = document.getElementById('marginModeBadge');
        if (marketSelect && badge) {
            const updateBadge = () => {
                const mapping = { both: 'TWSE + TPEX', twse: '僅 TWSE', tpex: '僅 TPEX' };
                badge.textContent = mapping[marketSelect.value] || 'TWSE + TPEX';
            };
            marketSelect.addEventListener('change', updateBadge);
            updateBadge();
        }

        this.initializeMarginLogPanel();
        this.safeAddEventListener('marginLogClearBtn', () => this.clearMarginLog());
    }

    initializeMarginLogPanel() {
        const panel = document.getElementById('marginLogPanel');
        if (panel) {
            this.marginLogPanel = panel;
            if (!this._marginLogInitialized) {
                this._marginLogInitialized = true;
                this.setMarginLogEmptyState();
            }
        }

        const clearBtn = document.getElementById('marginLogClearBtn');
        if (clearBtn && !clearBtn.dataset.bound) {
            clearBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                this.clearMarginLog();
            });
            clearBtn.dataset.bound = 'true';
        }
    }

    setMarginLogEmptyState() {
        if (!this.marginLogPanel) return;
        this.marginLogPanel.innerHTML = '';
        const empty = document.createElement('div');
        empty.className = 'margin-log-empty';
        empty.style.color = '#94a3b8';
        empty.textContent = '尚未開始抓取';
        this.marginLogPanel.appendChild(empty);
    }

    clearMarginLog(silent = false) {
        this.initializeMarginLogPanel();
        if (!this.marginLogPanel) return;
        this.setMarginLogEmptyState();
        if (!silent) {
            this.addMarginLog('日誌已清空', 'info');
        }
    }

    addMarginLog(message, level = 'info') {
        this.initializeMarginLogPanel();
        if (!this.marginLogPanel) return;

        const panel = this.marginLogPanel;
        const empty = panel.querySelector('.margin-log-empty');
        if (empty) empty.remove();

        const colors = {
            info: '#60a5fa',
            success: '#4ade80',
            warning: '#fbbf24',
            error: '#f87171'
        };
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌'
        };

        const entry = document.createElement('div');
        entry.className = `margin-log-entry margin-log-${level}`;
        entry.style.display = 'flex';
        entry.style.alignItems = 'flex-start';
        entry.style.gap = '0.5rem';
        entry.style.padding = '0.4rem 0.6rem';
        entry.style.marginBottom = '0.35rem';
        entry.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        entry.style.background = 'rgba(15,23,42,0.55)';
        entry.style.borderRadius = '6px';
        entry.style.boxShadow = 'inset 0 0 0 1px rgba(148,163,184,0.08)';

        const timeSpan = document.createElement('span');
        timeSpan.className = 'margin-log-time';
        timeSpan.style.fontFamily = "'Roboto Mono','Courier New',monospace";
        timeSpan.style.fontSize = '0.75rem';
        timeSpan.style.color = '#cbd5f5';
        timeSpan.style.minWidth = '3.6rem';
        timeSpan.textContent = this.getCurrentTimeString();

        const iconSpan = document.createElement('span');
        iconSpan.className = 'margin-log-icon';
        iconSpan.style.minWidth = '1.2rem';
        iconSpan.textContent = icons[level] || '•';

        const textSpan = document.createElement('span');
        textSpan.className = 'margin-log-text';
        textSpan.style.flex = '1';
        textSpan.style.color = colors[level] || '#e2e8f0';
        textSpan.style.whiteSpace = 'pre-wrap';
        textSpan.textContent = message;

        entry.appendChild(timeSpan);
        entry.appendChild(iconSpan);
        entry.appendChild(textSpan);
        panel.appendChild(entry);

        if (this.marginLogAutoScroll) {
            panel.scrollTop = panel.scrollHeight;
        }
    }

    logMarginDailyStats(dailyStats) {
        if (!Array.isArray(dailyStats) || dailyStats.length === 0) {
            this.addMarginLog('沒有每日統計資料（可能為非交易日或來源無資料）', 'warning');
            return;
        }

        const totals = dailyStats.reduce((acc, day) => {
            acc.twse += Number(day.twse_count || 0);
            acc.tpex += Number(day.tpex_count || 0);
            acc.total += Number(day.total_count || 0);
            return acc;
        }, { twse: 0, tpex: 0, total: 0 });

        this.addMarginLog(
            `每日統計：處理 ${dailyStats.length} 天，TWSE ${this.formatInteger(totals.twse)} 筆、TPEX ${this.formatInteger(totals.tpex)} 筆、合計 ${this.formatInteger(totals.total)} 筆`,
            'info'
        );

        const previewLimit = dailyStats.length <= 7 ? dailyStats.length : 5;
        dailyStats.slice(0, previewLimit).forEach((day) => {
            this.addMarginLog(
                ` - ${this.formatDate(day.date)}：TWSE ${this.formatInteger(day.twse_count)}、TPEX ${this.formatInteger(day.tpex_count)}、合計 ${this.formatInteger(day.total_count)}`,
                'info'
            );
        });

        if (dailyStats.length > previewLimit) {
            this.addMarginLog(`... 其餘 ${dailyStats.length - previewLimit} 天省略`, 'info');
        }
    }

    getMarginMarketLabel(market) {
        const key = (market || 'both').toLowerCase();
        if (key === 'twse') return '僅 TWSE';
        if (key === 'tpex') return '僅 TPEX';
        return 'TWSE + TPEX';
    }

    estimateMarginDurationSeconds(totalDays, sleepSeconds, market) {
        if (!Number.isFinite(totalDays) || totalDays <= 0) return 0;
        const baseSleep = Number.isFinite(sleepSeconds) && sleepSeconds > 0 ? sleepSeconds : 0.6;
        const perMarketMultiplier = (market || '').toLowerCase() === 'both' ? 2 : 1;
        const estimated = totalDays * perMarketMultiplier * (baseSleep + 0.35);
        return Math.max(1, Math.round(estimated));
    }

    startMarginProgressMonitor(totalDays, sleepSeconds, marketLabel) {
        this.stopMarginProgressMonitor();

        const safeLabel = marketLabel || 'TWSE + TPEX';
        const rangeHint = totalDays > 0 ? `${this.formatInteger(totalDays)} 天` : '多日';
        const intervalSeconds = Math.max(5, Math.min(15, (Number(sleepSeconds) || 0.6) * 2));
        this.addMarginLog(`伺服器正在處理 ${rangeHint} 的資料（${safeLabel}），進度更新約每 ${Math.round(intervalSeconds)} 秒提示一次`, 'info');

        this._marginProgressStart = (typeof performance !== 'undefined' && performance.now)
            ? performance.now()
            : Date.now();
        this._marginLastProgressLog = 0;

        if (typeof window === 'undefined' || typeof window.setInterval !== 'function') {
            return;
        }

        this._marginProgressTimer = window.setInterval(() => {
            const now = (typeof performance !== 'undefined' && performance.now)
                ? performance.now()
                : Date.now();
            const elapsedSec = Math.max(0, (now - this._marginProgressStart) / 1000);
            if (elapsedSec - this._marginLastProgressLog < intervalSeconds - 0.5) {
                return;
            }
            this._marginLastProgressLog = elapsedSec;
            const minutes = Math.floor(elapsedSec / 60);
            const seconds = Math.round(elapsedSec % 60);
            const durationLabel = minutes > 0
                ? `${minutes} 分 ${seconds.toString().padStart(2, '0')} 秒`
                : `${seconds} 秒`;
            const scopeHint = totalDays > 0 ? `範圍 ${this.formatInteger(totalDays)} 天` : '範圍較大';
            this.addMarginLog(`⌛ 已等待 ${durationLabel}，${scopeHint}（${safeLabel}），請稍候...`, 'info');
        }, intervalSeconds * 1000);
    }

    stopMarginProgressMonitor() {
        if (this._marginProgressTimer) {
            clearInterval(this._marginProgressTimer);
            this._marginProgressTimer = null;
        }
        this._marginProgressStart = 0;
        this._marginLastProgressLog = 0;
    }

    async fetchMarginData() {
        try {
            const start = document.getElementById('marginStartDate')?.value;
            const end = document.getElementById('marginEndDate')?.value;
            const market = document.getElementById('marginMarketSelect')?.value || 'both';
            const sleep = parseFloat(document.getElementById('marginSleepSeconds')?.value || '0.6');

            if (!start || !end) {
                this.addLogMessage('請先選擇開始與結束日期', 'warning');
                this.addMarginLog('請先選擇開始與結束日期', 'warning');
                return;
            }

            console.log('[Margin] fetchMarginData start', { start, end, market, sleep });
            this.addLogMessage(`📥 抓取融資融券：${start}~${end} 市場=${market.toUpperCase()} 間隔=${sleep}s`, 'info');
            this.clearMarginLog(true);
            this.addMarginLog(`開始抓取：${start} ~ ${end}，市場=${market.toUpperCase()}，間隔=${sleep}s`, 'info');

            const totalDays = this.calculateDateRangeDays(start, end);
            const marketLabel = this.getMarginMarketLabel(market);
            const estimatedSeconds = this.estimateMarginDurationSeconds(totalDays, sleep, market);
            if (totalDays > 0) {
                const durationLabel = estimatedSeconds > 0
                    ? `${this.formatInteger(estimatedSeconds)} 秒 (約 ${Math.max(1, Math.round(estimatedSeconds / 60))} 分)`
                    : '少於 1 分鐘';
                this.addMarginLog(`範圍內共有 ${this.formatInteger(totalDays)} 天資料（${marketLabel}），預估完成時間 ${durationLabel}`, 'info');
            } else {
                this.addMarginLog(`無法推算日期範圍天數，仍嘗試抓取（${marketLabel}）`, 'warning');
            }
            this.startMarginProgressMonitor(totalDays, sleep, marketLabel);

            const params = new URLSearchParams({ start, end, market, sleep: String(sleep) });
            if (this.useLocalDb) {
                params.set('use_local_db', 'true');
            }
            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/margin/fetch`;
            this.addMarginLog(`向伺服器發送請求：${requestUrl}`, 'info');
            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

            const resp = await fetch(`${requestUrl}?${params.toString()}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) throw new Error(data.error || `HTTP ${resp.status}`);

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);

            this.addMarginLog(`伺服器回應成功，開始處理資料（耗時 ${elapsedSec} 秒）`, 'success');

            this.marginData = data.data || [];
            this.marginDailyStats = data.daily_stats || [];
            this.updateMarginSummary(data.summary || {});
            this.renderMarginDailyTable();
            this.renderMarginResultsTable();
            this.addLogMessage(`✅ 完成融資融券抓取，共 ${data.count} 筆`, 'success');
            this.addMarginLog(`資料已整理完成：共 ${this.formatInteger(data.count)} 筆`, 'success');

            const summary = data.summary || {};
            const perMarket = summary.per_market || {};
            this.addMarginLog(
                `摘要：TWSE ${this.formatInteger(perMarket.TWSE || 0)} 筆、TPEX ${this.formatInteger(perMarket.TPEX || 0)} 筆、總筆數 ${this.formatInteger(summary.total_records || 0)} 筆`,
                'info'
            );
            this.addMarginLog(
                `處理天數：${summary.days_processed || 0} 天（${summary.start_date || '--'} ~ ${summary.end_date || '--'}）`,
                'info'
            );

            this.logMarginDailyStats(this.marginDailyStats);

            if (data.persist_enabled) {
                const inserted = Number(data.persisted || 0);
                const targetLabel = this.useLocalDb ? '本地資料庫' : 'Neon 雲端';
                const level = inserted > 0 ? 'success' : 'warning';
                const msg = inserted > 0
                    ? `資料庫寫入完成：${this.formatInteger(inserted)} 筆（${targetLabel}）`
                    : `資料庫未寫入新資料（回傳 ${inserted} 筆），請檢查設定`;
                this.addMarginLog(msg, level);
            } else {
                this.addMarginLog('此次抓取僅預覽資料（未寫入資料庫）', 'warning');
            }

            this.addMarginLog('渲染完成，面板資料已更新', 'success');
        } catch (err) {
            console.error('[Margin] fetch error', err);
            this.addLogMessage(`❌ 融資融券抓取失敗：${err.message}`, 'error');
            this.addMarginLog(`融資融券抓取失敗：${err.message}`, 'error');
        } finally {
            this.stopMarginProgressMonitor();
        }
    }

    async exportMarginCsv() {
        const start = document.getElementById('marginStartDate')?.value;
        const end = document.getElementById('marginEndDate')?.value;
        const market = document.getElementById('marginMarketSelect')?.value || 'both';
        const sleep = document.getElementById('marginSleepSeconds')?.value || '0.6';
        if (!start || !end) {
            this.addLogMessage('請先選擇開始與結束日期再匯出', 'warning');
            this.addMarginLog('請先選擇開始與結束日期再匯出', 'warning');
            return;
        }
        const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
        const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
        const params = new URLSearchParams({ start, end, market, sleep });
        if (this.useLocalDb) {
            params.set('use_local_db', 'true');
        }
        const url = `${base}/api/margin/export?${params.toString()}`;
        this.addLogMessage(`📤 匯出融資融券 CSV: ${url}`, 'info');
        this.addMarginLog(`執行 CSV 匯出：${url}`, 'info');
        window.open(url, '_blank');
    }

    updateMarginSummary(summary) {
        const perMarket = summary.per_market || {};
        this.setTextContent('marginStatTWSE', this.formatInteger(perMarket.TWSE || 0));
        this.setTextContent('marginStatTPEX', this.formatInteger(perMarket.TPEX || 0));
        this.setTextContent('marginStatTotal', this.formatInteger(summary.total_records || 0));
        this.setTextContent('marginStatDays', summary.days_processed || 0);
        const badge = document.getElementById('marginSummaryBadge');
        if (badge) badge.textContent = `${summary.start_date || '--'} ~ ${summary.end_date || '--'}`;
    }

    renderMarginDailyTable() {
        const tbody = document.querySelector('#marginDailyTable tbody');
        if (!tbody) return;
        if (!this.marginDailyStats || this.marginDailyStats.length === 0) {
            tbody.innerHTML = '<tr class="no-data-row"><td colspan="4" class="no-data-cell">尚未執行</td></tr>';
            return;
        }
        tbody.innerHTML = this.marginDailyStats.map(s => `
            <tr>
                <td>${this.formatDate(s.date)}</td>
                <td class="number">${this.formatInteger(s.twse_count)}</td>
                <td class="number">${this.formatInteger(s.tpex_count)}</td>
                <td class="number">${this.formatInteger(s.total_count)}</td>
            </tr>
        `).join('');
    }

    renderMarginResultsTable() {
        const tbody = document.querySelector('#marginResultsTable tbody');
        if (!tbody) return;
        if (!this.marginData || this.marginData.length === 0) {
            tbody.innerHTML = `
                <tr class="no-data-row">
                    <td colspan="15" class="no-data-cell">
                        <div class="no-data-content">
                            <div class="no-data-icon"><i class="fas fa-search"></i></div>
                            <div class="no-data-text">
                                <h4>尚無資料</h4>
                                <p>請先設定日期與市場後執行抓取</p>
                            </div>
                        </div>
                    </td>
                </tr>`;
            return;
        }
        const rows = this.marginData.slice(0, 200).map(r => `
            <tr>
                <td>${this.formatDate(r.date)}</td>
                <td>${r.market || ''}</td>
                <td>${r.stock_no || ''}</td>
                <td>${r.stock_name || ''}</td>
                <td class="number">${this.formatInteger(r.margin_prev_balance)}</td>
                <td class="number">${this.formatInteger(r.margin_buy)}</td>
                <td class="number">${this.formatInteger(r.margin_sell)}</td>
                <td class="number">${this.formatInteger(r.margin_repay)}</td>
                <td class="number">${this.formatInteger(r.margin_balance)}</td>
                <td class="number">${this.formatInteger(r.short_prev_balance)}</td>
                <td class="number">${this.formatInteger(r.short_sell)}</td>
                <td class="number">${this.formatInteger(r.short_buy)}</td>
                <td class="number">${this.formatInteger(r.short_repay)}</td>
                <td class="number">${this.formatInteger(r.short_balance)}</td>
                <td class="number">${this.formatInteger(r.offset_quantity)}</td>
            </tr>
        `).join('');
        const extra = this.marginData.length > 200 ? `<tr><td colspan="15" class="text-muted">僅顯示前 200 筆（共 ${this.formatInteger(this.marginData.length)} 筆）</td></tr>` : '';
        tbody.innerHTML = rows + extra;
    }

    /** =========================
     *  月營收 (Revenue)
     *  ========================= */

    setupRevenueListeners() {
        this.safeAddEventListener('revenueFetchBtn', () => {
            console.log('[Revenue] direct click -> fetch');
            this.fetchRevenueData();
        });
        this.safeAddEventListener('revenueExportBtn', () => {
            console.log('[Revenue] direct click -> export');
            this.exportRevenueCsv();
        });
        this.safeAddEventListener('revenueDownloadMopsBtn', () => {
            console.log('[Revenue] direct click -> download MOPS CSV');
            this.downloadMopsRevenueCsvFromUI();
        });

        const marketSelect = document.getElementById('revenueMarketSelect');
        const badge = document.getElementById('revenueModeBadge');
        if (marketSelect && badge) {
            const updateBadge = () => {
                const mapping = { both: 'TWSE + TPEX', twse: '僅 TWSE', tpex: '僅 TPEX' };
                badge.textContent = mapping[marketSelect.value] || 'TWSE + TPEX';
            };
            marketSelect.addEventListener('change', updateBadge);
            updateBadge();
        }

        this.initializeRevenueLogPanel();
        this.safeAddEventListener('revenueLogClearBtn', () => this.clearRevenueLog());
    }

    initializeRevenueLogPanel() {
        const panel = document.getElementById('revenueLogPanel');
        if (panel) {
            this.revenueLogPanel = panel;
            if (!this._revenueLogInitialized) {
                this._revenueLogInitialized = true;
                this.setRevenueLogEmptyState();
            }
        }

        const clearBtn = document.getElementById('revenueLogClearBtn');
        if (clearBtn && !clearBtn.dataset.bound) {
            clearBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                this.clearRevenueLog();
            });
            clearBtn.dataset.bound = 'true';
        }
    }

    setRevenueLogEmptyState() {
        if (!this.revenueLogPanel) return;
        this.revenueLogPanel.innerHTML = '';
        const empty = document.createElement('div');
        empty.className = 'revenue-log-empty';
        empty.style.color = '#94a3b8';
        empty.textContent = '尚未開始抓取';
        this.revenueLogPanel.appendChild(empty);
    }

    clearRevenueLog(silent = false) {
        this.initializeRevenueLogPanel();
        if (!this.revenueLogPanel) return;
        this.setRevenueLogEmptyState();
        if (!silent) {
            this.addRevenueLog('日誌已清空', 'info');
        }
    }

    addRevenueLog(message, level = 'info') {
        this.initializeRevenueLogPanel();
        if (!this.revenueLogPanel) return;

        const panel = this.revenueLogPanel;
        const empty = panel.querySelector('.revenue-log-empty');
        if (empty) empty.remove();

        const colors = {
            info: '#60a5fa',
            success: '#4ade80',
            warning: '#fbbf24',
            error: '#f87171'
        };
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌'
        };

        const entry = document.createElement('div');
        entry.className = `revenue-log-entry revenue-log-${level}`;
        entry.style.display = 'flex';
        entry.style.alignItems = 'flex-start';
        entry.style.gap = '0.5rem';
        entry.style.padding = '0.4rem 0.6rem';
        entry.style.marginBottom = '0.35rem';
        entry.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        entry.style.background = 'rgba(15,23,42,0.55)';
        entry.style.borderRadius = '6px';
        entry.style.boxShadow = 'inset 0 0 0 1px rgba(148,163,184,0.08)';

        const timeSpan = document.createElement('span');
        timeSpan.className = 'revenue-log-time';
        timeSpan.style.fontFamily = "'Roboto Mono','Courier New',monospace";
        timeSpan.style.fontSize = '0.75rem';
        timeSpan.style.color = '#cbd5f5';
        timeSpan.style.minWidth = '3.6rem';
        timeSpan.textContent = this.getCurrentTimeString();

        const iconSpan = document.createElement('span');
        iconSpan.className = 'revenue-log-icon';
        iconSpan.style.minWidth = '1.2rem';
        iconSpan.textContent = icons[level] || '•';

        const textSpan = document.createElement('span');
        textSpan.className = 'revenue-log-text';
        textSpan.style.flex = '1';
        textSpan.style.color = colors[level] || '#e2e8f0';
        textSpan.style.whiteSpace = 'pre-wrap';
        textSpan.textContent = message;

        entry.appendChild(timeSpan);
        entry.appendChild(iconSpan);
        entry.appendChild(textSpan);
        panel.appendChild(entry);

        if (this.revenueLogAutoScroll) {
            panel.scrollTop = panel.scrollHeight;
        }
    }

    getRevenueMarketLabel(market) {
        const key = (market || 'both').toLowerCase();
        if (key === 'twse') return '僅 TWSE';
        if (key === 'tpex') return '僅 TPEX';
        return 'TWSE + TPEX';
    }

    async downloadMopsRevenueCsvFromUI() {
        try {
            const market = document.getElementById('revenueMarketSelect')?.value || 'both';

            // 先用 UI 的開始/結束年月來推民國範圍（若沒填則用最近一年做預設）
            const startYm = document.getElementById('revenueStartYm')?.value;
            const endYm = document.getElementById('revenueEndYm')?.value;

            let startYearTw;
            let endYearTw;
            if (startYm && endYm) {
                const startAdYear = parseInt(String(startYm).split('-')[0] || '', 10);
                const endAdYear = parseInt(String(endYm).split('-')[0] || '', 10);
                if (!Number.isFinite(startAdYear) || !Number.isFinite(endAdYear) || startAdYear < 2000 || endAdYear < 2000) {
                    this.addRevenueLog('開始/結束年月格式不正確，無法推算民國年區間', 'warning');
                    return;
                }
                startYearTw = startAdYear - 1911;
                endYearTw = endAdYear - 1911;
            } else {
                const now = new Date();
                const adEnd = now.getFullYear();
                const adStart = adEnd - 1;
                startYearTw = adStart - 1911;
                endYearTw = adEnd - 1911;
            }

            this.clearRevenueLog(true);
            const marketLabel = this.getRevenueMarketLabel(market);
            this.addRevenueLog(`開始透過 Selenium 下載 MOPS 月營收 CSV 並匯入資料庫`, 'info');
            this.addRevenueLog(`民國年份區間：${startYearTw} ~ ${endYearTw}，市場=${marketLabel}，路徑預設 ~/Downloads/mops_csv`, 'info');

            const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
                ? window.location.origin
                : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

            const payload = {
                start_year_tw: startYearTw,
                end_year_tw: endYearTw,
                market,
                delay_between: 2.0,
                max_retries: 3,
                import_after: true,
                use_local_db: this.useLocalDb,
            };

            this.addRevenueLog(`向伺服器發送請求：${base}/api/revenue/download_mops_csv`, 'info');

            const resp = await fetch(`${base}/api/revenue/download_mops_csv`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });

            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.success) {
                const msg = data && data.error ? data.error : `HTTP ${resp.status}`;
                throw new Error(msg);
            }

            const dl = data.download_summary || {};
            const imp = data.import_summary || {};

            this.addRevenueLog(
                `下載完成：目錄=${dl.download_dir || '未知'}，市場=${(dl.markets || []).join(', ') || marketLabel}`,
                'success'
            );
            this.addRevenueLog(
                `下載任務：共 ${this.formatInteger(dl.total_tasks || 0)} 個，成功 ${this.formatInteger(dl.success_count || 0)}，失敗 ${this.formatInteger(dl.failed_count || 0)} 個`,
                'info'
            );
            if (Array.isArray(dl.failed_tasks) && dl.failed_tasks.length > 0) {
                const preview = dl.failed_tasks.slice(0, 5).map(t => `${t.market || ''} ${t.year_tw}/${t.month}`).join('，');
                this.addRevenueLog(`部分任務下載失敗（前 5 筆）：${preview}`, 'warning');
            }

            if (imp && typeof imp.inserted_rows === 'number') {
                const inserted = imp.inserted_rows || 0;
                const totalRows = imp.total_rows || 0;
                const files = imp.files || 0;
                const targetLabel = this.useLocalDb ? '本地資料庫' : 'Neon 雲端';
                this.addRevenueLog(`匯入完成：共讀取 ${this.formatInteger(totalRows)} 筆，實際寫入 ${this.formatInteger(inserted)} 筆，檔案數 ${files}，目標=${targetLabel}`, inserted > 0 ? 'success' : 'warning');
            } else {
                this.addRevenueLog('伺服器未回傳匯入摘要（可能僅下載未匯入）', 'warning');
            }

            this.addRevenueLog('MOPS CSV 下載 + 匯入流程已結束，可改用「抓取資料」查看資料庫內容', 'success');
        } catch (err) {
            console.error('[Revenue] download MOPS CSV error', err);
            this.addRevenueLog(`MOPS CSV 下載/匯入失敗：${err.message}`, 'error');
        }
    }

    async fetchRevenueData() {
        try {
            const startYm = document.getElementById('revenueStartYm')?.value;
            const endYm = document.getElementById('revenueEndYm')?.value;
            const market = document.getElementById('revenueMarketSelect')?.value || 'both';

            const hasStart = Boolean(startYm);
            const hasEnd = Boolean(endYm);
            const isRangeMode = hasStart || hasEnd;
            if (isRangeMode && !(hasStart && hasEnd)) {
                this.addRevenueLog('開始年月與結束年月需同時填寫或同時留空', 'warning');
                return;
            }

            const periodLabel = (hasStart && hasEnd)
                ? `${startYm} ~ ${endYm}`
                : '最新一個月';

            console.log('[Revenue] fetchRevenueData start', { startYm, endYm, market });
            this.addLogMessage(`📥 抓取月營收：${periodLabel} 市場=${market.toUpperCase()}`, 'info');
            this.clearRevenueLog(true);
            this.addRevenueLog(`開始抓取：${periodLabel}，市場=${market.toUpperCase()}`, 'info');

            const marketLabel = this.getRevenueMarketLabel(market);
            this.addRevenueLog(`目標市場：${marketLabel}，若未指定年月則抓取最新一期`, 'info');

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

            let requestUrl;
            let params;
            if (hasStart && hasEnd) {
                requestUrl = `${base}/api/revenue/fetch_range`;
                params = new URLSearchParams({ start: startYm, end: endYm, market, include_data: 'true' });
            } else {
                requestUrl = `${base}/api/revenue/fetch`;
                params = new URLSearchParams({ market });
            }

            if (this.useLocalDb) {
                params.set('use_local_db', 'true');
            }

            this.addRevenueLog(`向伺服器發送請求：${requestUrl}`, 'info');
            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

            const resp = await fetch(`${requestUrl}?${params.toString()}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) throw new Error(data.error || `HTTP ${resp.status}`);

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);

            this.addRevenueLog(`伺服器回應成功，開始處理資料（耗時 ${elapsedSec} 秒）`, 'success');

            this.revenueData = data.data || [];
            this.updateRevenueSummary(data.summary || {});
            this.renderRevenueResultsTable();
            this.addLogMessage(`✅ 完成月營收抓取，共 ${data.count} 筆`, 'success');
            this.addRevenueLog(`資料已整理完成：共 ${this.formatInteger(data.count)} 筆`, 'success');

            const summary = data.summary || {};
            const perMarket = summary.per_market || {};
            let actualYmLabel = null;
            const usedYear = summary.year;
            const usedMonth = summary.month;
            if (typeof usedYear === 'number' && typeof usedMonth === 'number') {
                actualYmLabel = `${usedYear}-${String(usedMonth).padStart(2, '0')}`;
            } else if (summary.roc_yyyymm) {
                const rocStr = String(summary.roc_yyyymm).trim();
                if (rocStr.length >= 4) {
                    const rocYearPart = rocStr.slice(0, -2);
                    const monthPart = rocStr.slice(-2);
                    const rocYear = parseInt(rocYearPart, 10);
                    const mVal = parseInt(monthPart, 10);
                    if (Number.isFinite(rocYear) && Number.isFinite(mVal) && mVal >= 1 && mVal <= 12) {
                        const adYear = rocYear + 1911;
                        actualYmLabel = `${adYear}-${String(mVal).padStart(2, '0')}`;
                    }
                }
            }
            if (actualYmLabel) {
                this.addRevenueLog(`實際抓取期別：${actualYmLabel}`, 'info');
            }

            const revenueTable = summary.revenue_table || summary.table || null;
            if (revenueTable) {
                this.addRevenueLog(`匯入資料表：${revenueTable}`, 'info');
            }

            const sourceUrls = summary.source_urls || {};
            const twseUrl = sourceUrls.TWSE || sourceUrls.twse || null;
            const tpexUrl = sourceUrls.TPEX || sourceUrls.tpex || null;
            const urlParts = [];
            if (twseUrl) urlParts.push(`TWSE ${twseUrl}`);
            if (tpexUrl) urlParts.push(`TPEX ${tpexUrl}`);
            if (urlParts.length > 0) {
                this.addRevenueLog(`來源網址：${urlParts.join(' | ')}`, 'info');
            }

            this.addRevenueLog(
                `摘要：TWSE ${this.formatInteger(perMarket.TWSE || 0)} 筆、TPEX ${this.formatInteger(perMarket.TPEX || 0)} 筆、總筆數 ${this.formatInteger(summary.total_records || 0)} 筆`,
                'info'
            );

            if (Array.isArray(data.monthly_stats) && data.monthly_stats.length > 0) {
                const ok = data.monthly_stats.filter(s => !s.error);
                const failed = data.monthly_stats.filter(s => s.error);
                this.addRevenueLog(
                    `月份統計：成功 ${this.formatInteger(ok.length)} 期、失敗 ${this.formatInteger(failed.length)} 期`,
                    failed.length > 0 ? 'warning' : 'info'
                );
            }

            if (data.persist_enabled) {
                const inserted = Number(data.persisted || 0);
                const targetLabel = this.useLocalDb ? '本地資料庫' : 'Neon 雲端';
                const level = inserted > 0 ? 'success' : 'warning';
                const msg = inserted > 0
                    ? `資料庫寫入完成：${this.formatInteger(inserted)} 筆（${targetLabel}）`
                    : `資料庫未寫入新資料（回傳 ${inserted} 筆），請檢查設定`;
                this.addRevenueLog(msg, level);
            } else {
                this.addRevenueLog('此次抓取僅預覽資料（未寫入資料庫）', 'warning');
            }

            this.addRevenueLog('渲染完成，面板資料已更新', 'success');
        } catch (err) {
            console.error('[Revenue] fetch error', err);
            this.addLogMessage(`❌ 月營收抓取失敗：${err.message}`, 'error');
            this.addRevenueLog(`月營收抓取失敗：${err.message}`, 'error');
        }
    }

    async exportRevenueCsv() {
        try {
            if (!Array.isArray(this.revenueData) || this.revenueData.length === 0) {
                this.addRevenueLog('目前沒有資料可匯出，請先執行抓取', 'warning');
                return;
            }

            const rows = this.revenueData;
            const header = [
                'revenue_month', 'market', 'stock_no', 'stock_name', 'industry', 'report_date',
                'month_revenue', 'last_month_revenue', 'last_year_month_revenue',
                'mom_change_pct', 'yoy_change_pct',
                'acc_revenue', 'last_year_acc_revenue', 'acc_change_pct', 'note',
            ];

            const escapeCsv = (v) => {
                const s = (v === null || v === undefined) ? '' : String(v);
                if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
                    return `"${s.replace(/"/g, '""')}"`;
                }
                return s;
            };

            const lines = [];
            lines.push(header.join(','));
            for (const r of rows) {
                const rec = {
                    revenue_month: r.revenue_month || r.revenueMonth || r.revenue_date || r.revenueMonthDate || r.revenue_month_date || '',
                    market: r.market || '',
                    stock_no: r.stock_no || '',
                    stock_name: r.stock_name || '',
                    industry: r.industry || '',
                    report_date: r.report_date || '',
                    month_revenue: r.month_revenue ?? '',
                    last_month_revenue: r.last_month_revenue ?? '',
                    last_year_month_revenue: r.last_year_month_revenue ?? '',
                    mom_change_pct: r.mom_change_pct ?? '',
                    yoy_change_pct: r.yoy_change_pct ?? '',
                    acc_revenue: r.acc_revenue ?? '',
                    last_year_acc_revenue: r.last_year_acc_revenue ?? '',
                    acc_change_pct: r.acc_change_pct ?? '',
                    note: r.note || '',
                };
                lines.push(header.map((k) => escapeCsv(rec[k])).join(','));
            }

            const startYm = document.getElementById('revenueStartYm')?.value;
            const endYm = document.getElementById('revenueEndYm')?.value;
            const nameLabel = (startYm && endYm) ? `${startYm}_to_${endYm}` : 'latest';
            const filename = `monthly_revenue_${nameLabel}.csv`;

            const csvText = lines.join('\n');
            const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);

            this.addRevenueLog(`CSV 匯出完成：${filename}（${this.formatInteger(rows.length)} 筆）`, 'success');
        } catch (err) {
            console.error('[Revenue] export csv error', err);
            this.addRevenueLog(`CSV 匯出失敗：${err.message}`, 'error');
        }
    }

    updateRevenueSummary(summary) {
        const perMarket = summary.per_market || {};
        this.setTextContent('revenueStatTWSE', this.formatInteger(perMarket.TWSE || 0));
        this.setTextContent('revenueStatTPEX', this.formatInteger(perMarket.TPEX || 0));
        this.setTextContent('revenueStatTotal', this.formatInteger(summary.total_records || 0));
        const year = summary.year;
        const month = summary.month;
        const period = summary.period
            || ((year && month) ? `${year}-${String(month).padStart(2, '0')}` : (summary.roc_yyyymm || '--'));
        this.setTextContent('revenueStatPeriod', period || '--');
        const badge = document.getElementById('revenueSummaryBadge');
        if (badge) badge.textContent = period || '尚未執行';
    }

    renderRevenueResultsTable() {
        const tbody = document.querySelector('#revenueResultsTable tbody');
        if (!tbody) return;
        if (!this.revenueData || this.revenueData.length === 0) {
            tbody.innerHTML = `
                <tr class="no-data-row">
                    <td colspan="13" class="no-data-cell">
                        <div class="no-data-content">
                            <div class="no-data-icon"><i class="fas fa-search"></i></div>
                            <div class="no-data-text">
                                <h4>尚無資料</h4>
                                <p>請先設定開始/結束年月與市場後執行抓取</p>
                            </div>
                        </div>
                    </td>
                </tr>`;
            return;
        }
        const rows = this.revenueData.slice(0, 200).map(r => `
            <tr>
                <td>${this.formatDate(r.revenue_month || r.revenueMonth || r.revenue_date || r.revenueMonthDate || r.revenue_month_date)}</td>
                <td>${r.market || ''}</td>
                <td>${r.stock_no || ''}</td>
                <td>${r.stock_name || ''}</td>
                <td>${r.industry || ''}</td>
                <td class="number">${this.formatInteger(r.month_revenue)}</td>
                <td class="number">${this.formatInteger(r.last_month_revenue)}</td>
                <td class="number">${this.formatInteger(r.last_year_month_revenue)}</td>
                <td class="number">${this.formatNumber(r.mom_change_pct, 2)}</td>
                <td class="number">${this.formatNumber(r.yoy_change_pct, 2)}</td>
                <td class="number">${this.formatInteger(r.acc_revenue)}</td>
                <td class="number">${this.formatInteger(r.last_year_acc_revenue)}</td>
                <td class="number">${this.formatNumber(r.acc_change_pct, 2)}</td>
            </tr>
        `).join('');
        const extra = this.revenueData.length > 200 ? `<tr><td colspan="13" class="text-muted">僅顯示前 200 筆（共 ${this.formatInteger(this.revenueData.length)} 筆）</td></tr>` : '';
        tbody.innerHTML = rows + extra;
    }

    /** =========================
     *  損益表 (Income Statement)
     *  ========================= */

    setupIncomeListeners() {
        this.safeAddEventListener('incomeFetchBtn', () => {
            console.log('[Income] direct click -> fetch');
            this.fetchIncomeData();
        });
        this.safeAddEventListener('incomeImportDbBtn', () => {
            console.log('[Income] direct click -> import DB');
            this.importIncomeToDb();
        });
        this.safeAddEventListener('incomeMultiFetchBtn', () => {
            console.log('[Income] direct click -> multi fetch');
            this.fetchIncomeMultiPeriod();
        });
        this.safeAddEventListener('incomeExportBtn', () => {
            console.log('[Income] direct click -> export');
            this.exportIncomeCsv();
        });
        this.safeAddEventListener('incomeSingleFetchBtn', () => {
            console.log('[Income] direct click -> fetch single');
            this.fetchIncomeSingleData();
        });

        this.initializeIncomeLogPanel();
        this.safeAddEventListener('incomeLogClearBtn', () => this.clearIncomeLog());
    }

    initializeIncomeLogPanel() {
        const panel = document.getElementById('incomeLogPanel');
        if (panel) {
            this.incomeLogPanel = panel;
            if (!this._incomeLogInitialized) {
                this._incomeLogInitialized = true;
                this.setIncomeLogEmptyState();
            }
        }

        const clearBtn = document.getElementById('incomeLogClearBtn');
        if (clearBtn && !clearBtn.dataset.bound) {
            clearBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                this.clearIncomeLog();
            });
            clearBtn.dataset.bound = 'true';
        }
    }

    setIncomeLogEmptyState() {
        if (!this.incomeLogPanel) return;
        this.incomeLogPanel.innerHTML = '';
        const empty = document.createElement('div');
        empty.className = 'income-log-empty';
        empty.style.color = '#94a3b8';
        empty.textContent = '尚未開始抓取';
        this.incomeLogPanel.appendChild(empty);
    }

    clearIncomeLog(silent = false) {
        this.initializeIncomeLogPanel();
        if (!this.incomeLogPanel) return;
        this.setIncomeLogEmptyState();
        if (!silent) {
            this.addIncomeLog('日誌已清空', 'info');
        }
    }

    addIncomeLog(message, level = 'info') {
        this.initializeIncomeLogPanel();
        if (!this.incomeLogPanel) return;

        const panel = this.incomeLogPanel;
        const empty = panel.querySelector('.income-log-empty');
        if (empty) empty.remove();

        const colors = {
            info: '#60a5fa',
            success: '#4ade80',
            warning: '#fbbf24',
            error: '#f87171',
        };
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌',
        };

        const entry = document.createElement('div');
        entry.className = `income-log-entry income-log-${level}`;
        entry.style.display = 'flex';
        entry.style.alignItems = 'flex-start';
        entry.style.gap = '0.5rem';
        entry.style.padding = '0.4rem 0.6rem';
        entry.style.marginBottom = '0.35rem';
        entry.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        entry.style.background = 'rgba(15,23,42,0.55)';
        entry.style.borderRadius = '6px';
        entry.style.boxShadow = 'inset 0 0 0 1px rgba(148,163,184,0.08)';

        const timeSpan = document.createElement('span');
        timeSpan.className = 'income-log-time';
        timeSpan.style.fontFamily = "'Roboto Mono','Courier New',monospace";
        timeSpan.style.fontSize = '0.75rem';
        timeSpan.style.color = '#cbd5f5';
        timeSpan.style.minWidth = '3.6rem';
        timeSpan.textContent = this.getCurrentTimeString();

        const iconSpan = document.createElement('span');
        iconSpan.className = 'income-log-icon';
        iconSpan.style.minWidth = '1.2rem';
        iconSpan.textContent = icons[level] || '•';

        const textSpan = document.createElement('span');
        textSpan.className = 'income-log-text';
        textSpan.style.flex = '1';
        textSpan.style.color = colors[level] || '#e2e8f0';
        textSpan.style.whiteSpace = 'pre-wrap';
        textSpan.textContent = message;

        entry.appendChild(timeSpan);
        entry.appendChild(iconSpan);
        entry.appendChild(textSpan);
        panel.appendChild(entry);

        if (this.incomeLogAutoScroll) {
            panel.scrollTop = panel.scrollHeight;
        }
    }

    updateIncomeProgress(percentage, message) {
        const fill = document.getElementById('incomeProgressFill');
        const text = document.getElementById('incomeProgressText');
        const status = document.getElementById('incomeProgressStatus');
        const pct = Math.max(0, Math.min(100, Math.round(Number(percentage) || 0)));
        if (fill) fill.style.width = `${pct}%`;
        if (text) text.textContent = `${pct}%`;
        if (status && typeof message === 'string' && message) status.textContent = message;
    }

    stopIncomeProgressTimer() {
        if (this._incomeProgressTimer) {
            clearInterval(this._incomeProgressTimer);
            this._incomeProgressTimer = null;
        }
    }

    async fetchIncomeData() {
        try {
            const yearStr = document.getElementById('incomeYear')?.value;
            const seasonStr = document.getElementById('incomeSeason')?.value || '1';

            const year = parseInt(yearStr || '', 10);
            const season = parseInt(seasonStr || '1', 10);

            if (!Number.isFinite(year) || year < 2000) {
                this.addIncomeLog('請輸入正確的西元年度（例如 2025）', 'warning');
                return;
            }
            if (![1, 2, 3, 4].includes(season)) {
                this.addIncomeLog('請選擇 1-4 季之一', 'warning');
                return;
            }

            const periodLabel = `${year}${String(season).padStart(2, '0')}`;

            const codeFromEl = document.getElementById('incomeCodeFrom');
            const codeToEl = document.getElementById('incomeCodeTo');
            const codeFrom = codeFromEl ? String(codeFromEl.value || '').trim() : '';
            const codeTo = codeToEl ? String(codeToEl.value || '').trim() : '';
            if (codeFrom || codeTo) {
                this.addIncomeLog(
                    `本次抓取僅限股票代號範圍：${codeFrom || '最小'} ~ ${codeTo || '最大'}`,
                    'info',
                );
            }

            const batchSizeStr = document.getElementById('incomeBatchSize')?.value || '';
            const restMinutesStr = document.getElementById('incomeBatchRestMinutes')?.value || '';
            const retryMaxStr = document.getElementById('incomeRetryMax')?.value || '';
            const retryWaitMinutesStr = document.getElementById('incomeRetryWaitMinutes')?.value || '';
            const batchSize = parseInt(batchSizeStr, 10);
            const restMinutes = parseFloat(restMinutesStr);
            const retryMax = parseInt(retryMaxStr, 10);
            const retryWaitMinutes = parseFloat(retryWaitMinutesStr);
            const hasBatch = Number.isFinite(batchSize) && batchSize > 0;
            const hasRest = Number.isFinite(restMinutes) && restMinutes > 0;
            const hasRetryMax = Number.isFinite(retryMax) && retryMax >= 0;
            const hasRetryWait = Number.isFinite(retryWaitMinutes) && retryWaitMinutes > 0;
            if (hasBatch && hasRest) {
                this.addIncomeLog(
                    `節流設定：每抓取 ${batchSize} 檔休息 ${restMinutes} 分鐘後繼續。`,
                    'info',
                );
            }
            if (hasRetryMax) {
                const waitLabel = hasRetryWait ? retryWaitMinutes : 5;
                this.addIncomeLog(`封鎖自動續抓設定：最多暫停/重試 ${retryMax} 次（每次 ${waitLabel} 分鐘）。`, 'info');
            }

            console.log('[Income] fetchIncomeData start', { year, season });
            this.addLogMessage(`📥 抓取損益表：${periodLabel} 全市場`, 'info');
            this.clearIncomeLog(true);
            this.addIncomeLog(`開始抓取損益表：年度 ${year}，季別 ${season}`, 'info');
            this.addIncomeLog('此操作會依序抓取所有上市櫃公司的 MOPS 損益表，執行時間可能長達數十分鐘以上，請耐心等待。', 'warning');

            const autoImportEl = document.getElementById('incomeAutoImportCheckbox');
            const writeToDb = !!(autoImportEl && autoImportEl.checked);
            if (writeToDb) {
                this.addIncomeLog(
                    `本次抓取將在伺服器端同步寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}）。`,
                    'info',
                );
            }

            // 初始化進度條
            this.stopIncomeProgressTimer();
            this.updateIncomeProgress(5, '準備開始抓取…');

            const params = new URLSearchParams({ year: String(year), season: String(season) });
            if (codeFrom) params.append('code_from', codeFrom);
            if (codeTo) params.append('code_to', codeTo);
            if (hasBatch) params.append('pause_every', String(batchSize));
            if (hasRest) params.append('pause_minutes', String(restMinutes));
            params.append('retry_on_block', '1');
            params.append('retry_wait_minutes', String(hasRetryWait ? retryWaitMinutes : 5));
            if (hasRetryMax) params.append('retry_max', String(retryMax));
            if (writeToDb) {
                params.append('write_to_db', '1');
                params.append('use_local_db', this.useLocalDb ? 'true' : 'false');
            }
            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/income-statement`;
            this.addIncomeLog(`向伺服器發送請求：${requestUrl}?${params.toString()}`, 'info');

            this.updateIncomeProgress(10, '已送出請求至伺服器，等待進度回報…');

            // 真的向後端查詢進度：每 5 秒輪詢 /api/income-statement/status
            this._incomeProgressTimer = window.setInterval(async () => {
                try {
                    const res = await fetch(`${base}/api/income-statement/status`);
                    if (!res.ok) return;
                    const json = await res.json();
                    if (!json || !json.success || !json.status) return;
                    const st = json.status;
                    const total = Number(st.total || 0);
                    const processed = Number(st.processed || 0);

                    let pct = 10;
                    if (total > 0 && processed >= 0) {
                        pct = Math.max(10, Math.min(99, Math.round((processed / total) * 100)));
                    }

                    let msg = '';
                    if (st.running) {
                        if (total > 0 && processed > 0) {
                            msg = `伺服器處理中：第 ${processed}/${total} 檔（${st.current_code || ''}）`;
                        } else {
                            msg = '伺服器處理中，等待進度資料…';
                        }
                    } else {
                        msg = '伺服器已回應，前端正在整理資料…';
                    }

                    this.updateIncomeProgress(pct, msg);
                } catch (err) {
                    console.warn('income-statement status poll error', err);
                }
            }, 5000);

            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

            const resp = await fetch(`${requestUrl}?${params.toString()}`);
            if (!resp.ok) {
                let msg = `HTTP ${resp.status}`;
                try {
                    const raw = await resp.text();
                    if (raw) {
                        try {
                            const j = JSON.parse(raw);
                            if (j && j.error) msg = j.error;
                            else msg = raw;
                        } catch (_) {
                            msg = raw;
                        }
                    }
                } catch (_) {}
                throw new Error(msg);
            }
            const data = await resp.json();
            if (!Array.isArray(data)) {
                throw new Error('伺服器回傳格式錯誤（預期為陣列）');
            }

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);
            this.addIncomeLog(`伺服器回應成功，開始處理資料（耗時 ${elapsedSec} 秒）`, 'success');

            this.stopIncomeProgressTimer();

            this.incomeData = data;
            this.updateIncomeSummary(data, year, season);
            this.renderIncomeResultsTable();

            const total = data.length;
            const unique = new Set();
            data.forEach((row) => {
                if (row && row['股票代號']) unique.add(row['股票代號']);
            });

            this.addLogMessage(`✅ 完成損益表抓取，共 ${total} 筆，涵蓋 ${unique.size} 檔股票`, 'success');
            this.addIncomeLog(`資料已整理完成：共 ${this.formatInteger(total)} 筆，${this.formatInteger(unique.size)} 檔股票`, 'success');
            this.updateIncomeProgress(100, `完成：共 ${this.formatInteger(total)} 筆，${this.formatInteger(unique.size)} 檔股票`);
        } catch (err) {
            console.error('[Income] fetch error', err);
            this.addLogMessage(`❌ 損益表抓取失敗：${err.message}`, 'error');
            this.addIncomeLog(`損益表抓取失敗：${err.message}`, 'error');
            this.stopIncomeProgressTimer();
            this.updateIncomeProgress(0, '抓取失敗，請查看下方日誌訊息');
        }
    }

    async fetchIncomeMultiPeriod() {
        try {
            const fromStr = document.getElementById('incomeYearFrom')?.value || '';
            const toStr = document.getElementById('incomeYearTo')?.value || '';
            const baseYearStr = document.getElementById('incomeYear')?.value || '';

            let fromYear = parseInt(fromStr || baseYearStr || '', 10);
            let toYear = parseInt(toStr || baseYearStr || '', 10);

            if (!Number.isFinite(fromYear) || fromYear < 2000) {
                this.addIncomeLog('請輸入正確的多期起始年度（例如 2020），或至少填寫上方單一期別年度。', 'warning');
                return;
            }
            if (!Number.isFinite(toYear) || toYear < 2000) {
                toYear = fromYear;
            }
            if (fromYear > toYear) {
                const tmp = fromYear;
                fromYear = toYear;
                toYear = tmp;
            }

            const codeFromEl = document.getElementById('incomeCodeFrom');
            const codeToEl = document.getElementById('incomeCodeTo');
            const codeFrom = codeFromEl ? String(codeFromEl.value || '').trim() : '';
            const codeTo = codeToEl ? String(codeToEl.value || '').trim() : '';

            const batchSizeStr = document.getElementById('incomeBatchSize')?.value || '';
            const restMinutesStr = document.getElementById('incomeBatchRestMinutes')?.value || '';
            const retryMaxStr = document.getElementById('incomeRetryMax')?.value || '';
            const retryWaitMinutesStr = document.getElementById('incomeRetryWaitMinutes')?.value || '';
            const batchSize = parseInt(batchSizeStr, 10);
            const restMinutes = parseFloat(restMinutesStr);
            const retryMax = parseInt(retryMaxStr, 10);
            const retryWaitMinutes = parseFloat(retryWaitMinutesStr);
            const hasBatch = Number.isFinite(batchSize) && batchSize > 0;
            const hasRest = Number.isFinite(restMinutes) && restMinutes > 0;
            const hasRetryMax = Number.isFinite(retryMax) && retryMax >= 0;
            const hasRetryWait = Number.isFinite(retryWaitMinutes) && retryWaitMinutes > 0;

            const selectedSeasons = [];
            for (let s = 1; s <= 4; s += 1) {
                const cb = document.getElementById(`incomeMultiSeason${s}`);
                if (!cb || cb.checked) selectedSeasons.push(s);
            }
            if (!selectedSeasons.length) {
                this.addIncomeLog('請至少勾選一個季別', 'warning');
                return;
            }

            const tasks = [];
            for (let y = fromYear; y <= toYear; y += 1) {
                for (const s of selectedSeasons) {
                    tasks.push({ year: y, season: s });
                }
            }
            if (!tasks.length) {
                this.addIncomeLog('沒有可執行的期別，請檢查年度與季別設定。', 'warning');
                return;
            }

            const autoImportEl = document.getElementById('incomeAutoImportCheckbox');
            const writeToDb = !!(autoImportEl && autoImportEl.checked);

            this.clearIncomeLog(true);
            this.addIncomeLog(
                `開始多期別損益表抓取：年度 ${fromYear} ~ ${toYear}，季別 ${selectedSeasons.join('、')}（共 ${tasks.length} 期）`,
                'info',
            );
            if (codeFrom || codeTo) {
                this.addIncomeLog(
                    `多期別僅限股票代號範圍：${codeFrom || '最小'} ~ ${codeTo || '最大'}`,
                    'info',
                );
            }
            if (hasBatch && hasRest) {
                this.addIncomeLog(
                    `多期別節流設定：每抓取 ${batchSize} 檔休息 ${restMinutes} 分鐘後繼續。`,
                    'info',
                );
            }
            if (hasRetryMax) {
                const waitLabel = hasRetryWait ? retryWaitMinutes : 5;
                this.addIncomeLog(`多期別封鎖自動續抓：最多暫停/重試 ${retryMax} 次（每次 ${waitLabel} 分鐘）。`, 'info');
            }
            if (writeToDb) {
                this.addIncomeLog(
                    `多期別損益表將在伺服器端同步寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}）。`,
                    'info',
                );
            }

            this.stopIncomeProgressTimer();
            this.updateIncomeProgress(5, '準備開始多期別抓取…');

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/income-statement`;

            let allRows = [];
            const allCodes = new Set();

            const overallStartedAt = (typeof performance !== 'undefined' && performance.now)
                ? performance.now()
                : Date.now();

            for (let i = 0; i < tasks.length; i += 1) {
                const { year, season } = tasks[i];
                const periodLabel = `${year}${String(season).padStart(2, '0')}`;

                this.addIncomeLog(`▶️ (${i + 1}/${tasks.length}) 開始抓取期別 ${periodLabel} 全市場`, 'info');

                this.stopIncomeProgressTimer();
                this.updateIncomeProgress(5, `準備抓取期別 ${periodLabel}…`);

                const params = new URLSearchParams({ year: String(year), season: String(season) });
                if (codeFrom) params.append('code_from', codeFrom);
                if (codeTo) params.append('code_to', codeTo);
                if (hasBatch) params.append('pause_every', String(batchSize));
                if (hasRest) params.append('pause_minutes', String(restMinutes));
                params.append('retry_on_block', '1');
                params.append('retry_wait_minutes', String(hasRetryWait ? retryWaitMinutes : 5));
                if (hasRetryMax) params.append('retry_max', String(retryMax));
                if (writeToDb) {
                    params.append('write_to_db', '1');
                    params.append('use_local_db', this.useLocalDb ? 'true' : 'false');
                }

                this._incomeProgressTimer = window.setInterval(async () => {
                    try {
                        const res = await fetch(`${base}/api/income-statement/status`);
                        if (!res.ok) return;
                        const json = await res.json();
                        if (!json || !json.success || !json.status) return;
                        const st = json.status;
                        const total = Number(st.total || 0);
                        const processed = Number(st.processed || 0);

                        let pct = 10;
                        if (total > 0 && processed >= 0) {
                            pct = Math.max(10, Math.min(99, Math.round((processed / total) * 100)));
                        }

                        let msg = '';
                        if (st.running) {
                            if (total > 0 && processed > 0) {
                                msg = `伺服器處理中（${periodLabel}）：第 ${processed}/${total} 檔（${st.current_code || ''}）`;
                            } else {
                                msg = `伺服器處理中（${periodLabel}），等待進度資料…`;
                            }
                        } else {
                            msg = `伺服器已回應（${periodLabel}），前端正在整理資料…`;
                        }

                        this.updateIncomeProgress(pct, msg);
                    } catch (err) {
                        console.warn('income-statement status poll error (multi)', err);
                    }
                }, 5000);

                const startedAt = (typeof performance !== 'undefined' && performance.now)
                    ? performance.now()
                    : Date.now();

                const resp = await fetch(`${requestUrl}?${params.toString()}`);
                if (!resp.ok) {
                    let msg = `HTTP ${resp.status}`;
                    try {
                        const j = await resp.json();
                        if (j && j.error) msg = j.error;
                    } catch (_) {}
                    throw new Error(msg);
                }
                const data = await resp.json();

                const finishedAt = (typeof performance !== 'undefined' && performance.now)
                    ? performance.now()
                    : Date.now();
                const elapsedMs = Math.max(0, finishedAt - startedAt);
                const elapsedSec = (elapsedMs / 1000).toFixed(2);

                this.stopIncomeProgressTimer();

                if (!Array.isArray(data) || !data.length) {
                    this.addIncomeLog(`⚠️ 期別 ${periodLabel} 無資料（可能尚未公告）`, 'warning');
                    continue;
                }

                data.forEach((row) => {
                    if (row && row['股票代號']) allCodes.add(row['股票代號']);
                });
                allRows = allRows.concat(data);

                this.addIncomeLog(
                    `✅ 期別 ${periodLabel} 完成，新增 ${this.formatInteger(data.length)} 筆（耗時 ${elapsedSec} 秒）`,
                    'success',
                );
            }

            const overallFinishedAt = (typeof performance !== 'undefined' && performance.now)
                ? performance.now()
                : Date.now();
            const overallElapsedMs = Math.max(0, overallFinishedAt - overallStartedAt);
            const overallElapsedSec = (overallElapsedMs / 1000).toFixed(2);

            if (!allRows.length) {
                this.incomeData = [];
                this.renderIncomeResultsTable();
                this.addIncomeLog('多期別抓取未取得任何資料。', 'warning');
                this.updateIncomeProgress(0, '多期別抓取未取得資料');
                return;
            }

            this.incomeData = allRows;
            const periodSummaryLabel = `${fromYear}-${toYear} 多期（季別：${selectedSeasons.join('、')}）`;
            this.updateIncomeSummary(allRows, fromYear, selectedSeasons[0], { periodLabel: periodSummaryLabel });
            this.renderIncomeResultsTable();

            const totalRows = allRows.length;
            this.addLogMessage(
                `✅ 多期別損益表抓取完成，共 ${this.formatInteger(totalRows)} 筆，涵蓋 ${this.formatInteger(allCodes.size)} 檔股票（總耗時 ${overallElapsedSec} 秒）`,
                'success',
            );
            this.addIncomeLog(
                `多期別損益表抓取完成：共 ${this.formatInteger(totalRows)} 筆，${this.formatInteger(allCodes.size)} 檔股票（總耗時 ${overallElapsedSec} 秒）`,
                'success',
            );
            this.updateIncomeProgress(100, `多期別完成：共 ${this.formatInteger(totalRows)} 筆，${this.formatInteger(allCodes.size)} 檔股票`);
        } catch (err) {
            console.error('[Income] multi-period fetch error', err);
            this.addLogMessage(`❌ 多期別損益表抓取失敗：${err.message}`, 'error');
            this.addIncomeLog(`多期別損益表抓取失敗：${err.message}`, 'error');
            this.stopIncomeProgressTimer();
            this.updateIncomeProgress(0, '多期別抓取失敗，請查看下方日誌訊息');
        }
    }

    async fetchIncomeSingleData() {
        try {
            const yearStr = document.getElementById('incomeYear')?.value;
            const seasonStr = document.getElementById('incomeSeason')?.value || '1';
            const codeRaw = document.getElementById('incomeSingleCode')?.value || '';
            const code = String(codeRaw).trim();

            const year = parseInt(yearStr || '', 10);
            const season = parseInt(seasonStr || '1', 10);

            if (!Number.isFinite(year) || year < 2000) {
                this.addIncomeLog('請輸入正確的西元年度（例如 2025）', 'warning');
                return;
            }
            if (![1, 2, 3, 4].includes(season)) {
                this.addIncomeLog('請選擇 1-4 季之一', 'warning');
                return;
            }
            if (!code) {
                this.addIncomeLog('請輸入要查詢的股票代號，例如 2330', 'warning');
                return;
            }

            const periodLabel = `${year}${String(season).padStart(2, '0')}`;

            console.log('[Income] fetchIncomeSingleData start', { year, season, code });
            this.clearIncomeLog(true);
            this.addIncomeLog(`開始抓取單一股票損益表：股票代號 ${code}，年度 ${year}，季別 ${season}`, 'info');

            this.stopIncomeProgressTimer();
            this.updateIncomeProgress(0, `抓取單一股票 ${code} 損益表中…`);

            const params = new URLSearchParams({ year: String(year), season: String(season), code });
            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/income-statement`;
            this.addIncomeLog(`向伺服器發送請求：${requestUrl}?${params.toString()}`, 'info');

            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const resp = await fetch(`${requestUrl}?${params.toString()}`);
            if (!resp.ok) {
                let msg = `HTTP ${resp.status}`;
                try {
                    const j = await resp.json();
                    if (j && j.error) msg = j.error;
                } catch (_) {}
                throw new Error(msg);
            }
            const data = await resp.json();

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);

            if (!Array.isArray(data) || data.length === 0) {
                this.incomeData = [];
                this.renderIncomeResultsTable();
                this.addIncomeLog(`找不到股票 ${code} 在期別 ${periodLabel} 的損益表資料（可能尚未公告或代號有誤）`, 'warning');
                this.updateIncomeProgress(0, `找不到股票 ${code} 的損益表資料`);
                return;
            }

            this.addIncomeLog(`伺服器回應成功，單一股票 ${code} 資料載入完成（耗時 ${elapsedSec} 秒）`, 'success');

            this.incomeData = data;
            this.updateIncomeSummary(data, year, season);
            this.renderIncomeResultsTable();

            const total = data.length;
            this.addLogMessage(`✅ 單一股票 ${code} 損益表抓取完成，共 ${total} 筆`, 'success');
            this.addIncomeLog(`單一股票 ${code} 損益表抓取完成，共 ${this.formatInteger(total)} 筆`, 'success');
            this.updateIncomeProgress(100, `單一股票 ${code} 完成，筆數 ${this.formatInteger(total)}`);
        } catch (err) {
            console.error('[Income] single fetch error', err);
            this.addLogMessage(`❌ 單一股票損益表抓取失敗：${err.message}`, 'error');
            this.addIncomeLog(`單一股票損益表抓取失敗：${err.message}`, 'error');
            this.stopIncomeProgressTimer();
            this.updateIncomeProgress(0, '單一股票抓取失敗，請查看下方日誌訊息');
        }
    }

    async importIncomeToDb() {
        try {
            const rows = Array.isArray(this.incomeData) ? this.incomeData : [];
            if (!rows.length) {
                this.addIncomeLog('目前沒有可寫入資料庫的損益表資料，請先執行抓取。', 'warning');
                return;
            }

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const isBackendOrigin = typeof origin === 'string' && /:\s*5003\b/.test(origin.replace(/\s+/g, ''));
            const base = isBackendOrigin ? origin : 'http://localhost:5003';
            const url = `${base}/api/income-statement/import`;

            this.addIncomeLog(
                `開始將目前損益表資料寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}），共 ${this.formatInteger(rows.length)} 筆`,
                'info',
            );
            this.addIncomeLog(`POST ${url}`, 'info');

            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

            const resp = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rows, use_local_db: this.useLocalDb }),
            });
            const data = await resp.json().catch(() => ({}));

            if (!resp.ok || !data || data.success === false) {
                const msg = (data && data.error) ? data.error : `HTTP ${resp.status}`;
                throw new Error(msg);
            }

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);

            const inserted = Number(data.inserted || 0);
            this.addLogMessage(
                `✅ 損益表寫入資料庫完成，成功寫入 ${this.formatInteger(inserted)} 筆（耗時 ${elapsedSec} 秒）`,
                'success',
            );
            this.addIncomeLog(
                `損益表寫入資料庫完成：成功寫入 ${this.formatInteger(inserted)} 筆（耗時 ${elapsedSec} 秒）`,
                'success',
            );
        } catch (err) {
            console.error('[Income] import DB error', err);
            this.addLogMessage(`❌ 損益表寫入資料庫失敗：${err.message}`, 'error');
            this.addIncomeLog(`損益表寫入資料庫失敗：${err.message}`, 'error');
        }
    }

    exportIncomeCsv() {
        try {
            const rows = Array.isArray(this.incomeData) ? this.incomeData : [];
            if (!rows.length) {
                this.addIncomeLog('目前沒有可匯出的損益表資料，請先執行抓取。', 'warning');
                return;
            }

            const escapeCSV = (value) => {
                if (value === null || value === undefined) return '';
                const str = String(value).replace(/"/g, '""');
                return /[",\r\n]/.test(str) ? `"${str}"` : str;
            };

            const first = rows[0] || {};
            const columns = Object.keys(first);
            const headerLine = columns.map(escapeCSV).join(',');
            const lines = [headerLine];
            rows.forEach((row) => {
                const line = columns.map((col) => escapeCSV(row[col])).join(',');
                lines.push(line);
            });

            const csvContent = '\ufeff' + lines.join('\r\n');
            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            const ts = new Date().toISOString().replace(/[:.]/g, '-');
            a.href = url;
            a.download = `income_statement_${ts}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            this.addIncomeLog('✅ 損益表資料已匯出為 CSV 檔案', 'success');
        } catch (err) {
            console.error('[Income] export CSV error', err);
            this.addIncomeLog(`損益表匯出失敗：${err.message}`, 'error');
        }
    }

    updateIncomeSummary(data, year, season, options = {}) {
        const rows = Array.isArray(data) ? data : [];
        const total = rows.length;
        const codes = new Set();
        let period = null;
        rows.forEach((row) => {
            if (row && row['股票代號']) codes.add(row['股票代號']);
            if (!period && row && row.period) period = row.period;
        });
        const { periodLabel } = options || {};
        if (!period) {
            if (periodLabel) {
                period = periodLabel;
            } else if (Number.isFinite(year) && [1, 2, 3, 4].includes(season)) {
                period = `${year}${String(season).padStart(2, '0')}`;
            } else {
                period = '--';
            }
        }

        this.setTextContent('incomeStatUnique', codes.size || '--');
        this.setTextContent('incomeStatTotal', total || '--');
        this.setTextContent('incomeStatPeriod', period || '--');
        const badge = document.getElementById('incomeSummaryBadge');
        if (badge) badge.textContent = period || '尚未執行';
    }

    renderIncomeResultsTable() {
        const tbody = document.querySelector('#incomeResultsTable tbody');
        if (!tbody) return;
        const rows = Array.isArray(this.incomeData) ? this.incomeData : [];
        if (!rows.length) {
            tbody.innerHTML = `
                <tr class="no-data-row">
                    <td colspan="6" class="no-data-cell">
                        <div class="no-data-content">
                            <div class="no-data-icon"><i class="fas fa-search"></i></div>
                            <div class="no-data-text">
                                <h4>尚無資料</h4>
                                <p>請先輸入年度與季別後執行抓取</p>
                            </div>
                        </div>
                    </td>
                </tr>`;
            return;
        }

        const fmtNum = (v, digits = 0) => {
            if (v === null || v === undefined || v === '') return '';
            const n = Number(v);
            if (!Number.isFinite(n)) return String(v);
            if (digits > 0) return n.toFixed(digits);
            return n.toLocaleString();
        };

        const sample = rows.slice(0, 200);
        const bodyHtml = sample
            .map((r) => `
                <tr>
                    <td>${r['股票代號'] || ''}</td>
                    <td>${r.period || ''}</td>
                    <td class="number">${fmtNum(r.Revenue)}</td>
                    <td class="number">${fmtNum(r.OperatingCosts)}</td>
                    <td class="number">${fmtNum(r.ProfitLossBeforeTax)}</td>
                    <td class="number">${fmtNum(r.BasicEarningsLossPerShareTotal, 2)}</td>
                </tr>`)
            .join('');
        const extraRow =
            rows.length > sample.length
                ? `<tr><td colspan="6" class="text-muted">僅顯示前 ${sample.length} 筆（共 ${this.formatInteger(rows.length)} 筆）</td></tr>`
                : '';
        tbody.innerHTML = bodyHtml + extraRow;
    }

    /** =========================
     *  資產負債表 (Balance Sheet)
     *  ========================= */

    setupBalanceListeners() {
        this.safeAddEventListener('balanceFetchBtn', () => this.fetchBalanceData());
        this.safeAddEventListener('balanceImportDbBtn', () => this.importBalanceToDb());
        this.safeAddEventListener('balanceMultiFetchBtn', () => this.fetchBalanceMultiPeriod());
        this.safeAddEventListener('balanceExportBtn', () => this.exportBalanceCsv());
        this.initializeBalanceLogPanel();
        this.safeAddEventListener('balanceLogClearBtn', () => this.clearBalanceLog());
    }

    /** =========================
     *  財務比率 (Financial Ratios)
     *  ========================= */

    setupRatiosListeners() {
        this.initializeRatiosLogPanel();
        this.safeAddEventListener('ratiosLogClearBtn', () => this.clearRatiosLog());
        this.safeAddEventListener('ratiosFetchBtn', () => this.fetchRatiosData(false));
        this.safeAddEventListener('ratiosWriteDbBtn', () => this.fetchRatiosData(true));
    }

    initializeRatiosLogPanel() {
        const panel = document.getElementById('ratiosLogPanel');
        if (panel) {
            this.ratiosLogPanel = panel;
            if (!this._ratiosLogInitialized) {
                this._ratiosLogInitialized = true;
                this.setRatiosLogEmptyState();
            }
        }

        const clearBtn = document.getElementById('ratiosLogClearBtn');
        if (clearBtn && !clearBtn.dataset.bound) {
            clearBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                this.clearRatiosLog();
            });
            clearBtn.dataset.bound = 'true';
        }
    }

    setRatiosLogEmptyState() {
        if (!this.ratiosLogPanel) return;
        this.ratiosLogPanel.innerHTML = '';
        const empty = document.createElement('div');
        empty.className = 'ratios-log-empty';
        empty.style.color = '#94a3b8';
        empty.textContent = '尚未開始計算';
        this.ratiosLogPanel.appendChild(empty);
    }

    clearRatiosLog(silent = false) {
        this.initializeRatiosLogPanel();
        if (!this.ratiosLogPanel) return;
        this.setRatiosLogEmptyState();
        if (!silent) {
            this.addRatiosLog('日誌已清空', 'info');
        }
    }

    addRatiosLog(message, level = 'info') {
        this.initializeRatiosLogPanel();
        if (!this.ratiosLogPanel) return;

        const panel = this.ratiosLogPanel;
        const empty = panel.querySelector('.ratios-log-empty');
        if (empty) empty.remove();

        const colors = {
            info: '#60a5fa',
            success: '#4ade80',
            warning: '#fbbf24',
            error: '#f87171',
        };
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌',
        };

        const entry = document.createElement('div');
        entry.className = `ratios-log-entry ratios-log-${level}`;
        entry.style.display = 'flex';
        entry.style.alignItems = 'flex-start';
        entry.style.gap = '0.5rem';
        entry.style.padding = '0.4rem 0.6rem';
        entry.style.marginBottom = '0.35rem';
        entry.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        entry.style.background = 'rgba(15,23,42,0.55)';
        entry.style.borderRadius = '6px';
        entry.style.boxShadow = 'inset 0 0 0 1px rgba(148,163,184,0.08)';

        const timeSpan = document.createElement('span');
        timeSpan.className = 'ratios-log-time';
        timeSpan.style.fontFamily = "'Roboto Mono','Courier New',monospace";
        timeSpan.style.fontSize = '0.75rem';
        timeSpan.style.color = '#cbd5f5';
        timeSpan.style.minWidth = '3.6rem';
        timeSpan.textContent = this.getCurrentTimeString();

        const iconSpan = document.createElement('span');
        iconSpan.className = 'ratios-log-icon';
        iconSpan.style.minWidth = '1.2rem';
        iconSpan.textContent = icons[level] || '•';

        const textSpan = document.createElement('span');
        textSpan.className = 'ratios-log-text';
        textSpan.style.flex = '1';
        textSpan.style.color = colors[level] || '#e2e8f0';
        textSpan.style.whiteSpace = 'pre-wrap';
        textSpan.textContent = message;

        entry.appendChild(timeSpan);
        entry.appendChild(iconSpan);
        entry.appendChild(textSpan);
        panel.appendChild(entry);

        if (this.ratiosLogAutoScroll) {
            panel.scrollTop = panel.scrollHeight;
        }
    }

    updateRatiosProgress(percentage, message) {
        const fill = document.getElementById('ratiosProgressFill');
        const text = document.getElementById('ratiosProgressText');
        const status = document.getElementById('ratiosProgressStatus');
        const pct = Math.max(0, Math.min(100, Math.round(Number(percentage) || 0)));
        if (fill) fill.style.width = `${pct}%`;
        if (text) text.textContent = `${pct}%`;
        if (status && typeof message === 'string' && message) status.textContent = message;
    }

    stopRatiosProgressTimer() {
        if (this._ratiosProgressTimer) {
            clearInterval(this._ratiosProgressTimer);
            this._ratiosProgressTimer = null;
        }
        this._ratiosLastProgressPct = null;
    }

    startRatiosProgressPolling(base) {
        this.stopRatiosProgressTimer();
        this.updateRatiosProgress(10, '已送出請求至伺服器，等待進度回報…');
        this._ratiosProgressTimer = window.setInterval(async () => {
            try {
                const res = await fetch(`${base}/api/financial-ratios/status${this.useLocalDb ? '?use_local_db=true' : ''}`);
                if (!res.ok) return;
                const json = await res.json();
                const st = json && json.status ? json.status : null;
                if (!st) return;
                const phase = st.phase || '';
                const totalRaw = st.total;
                const processedRaw = st.processed;
                const total = Number.isFinite(Number(totalRaw)) ? Number(totalRaw) : 0;
                const processed = Number.isFinite(Number(processedRaw)) ? Number(processedRaw) : 0;
                const running = !!st.running;
                const currentCode = st.current_code || st.currentCode || '';
                const inserted = st.db_inserted_rows ?? st.dbInsertedRows;
                const pct = total > 0 ? (processed / total) * 100 : 0;
                const pctRounded = Math.max(0, Math.min(100, Math.round(pct)));
                let statusText = '';
                if (phase === 'querying') {
                    statusText = '查詢中：正在從資料庫讀取損益表/資產負債表並進行 JOIN…';
                } else if (phase === 'computing') {
                    statusText =
                        total > 0
                            ? `計算中：${processed}/${total}（${pctRounded}%）${currentCode ? ` 目前：${currentCode}` : ''}`
                            : `計算中…${currentCode ? ` 目前：${currentCode}` : ''}`;
                } else if (phase === 'done') {
                    statusText = '完成';
                } else if (phase === 'error') {
                    statusText = `失敗${st.error ? `：${st.error}` : ''}`;
                } else {
                    statusText = total > 0
                        ? `處理中：${processed}/${total}（${pctRounded}%）${currentCode ? ` 目前：${currentCode}` : ''}`
                        : `處理中…${currentCode ? ` 目前：${currentCode}` : ''}`;
                }
                this.updateRatiosProgress(pctRounded, statusText);

                // 避免日誌刷太快：每 5% 記一次
                if (this._ratiosLastProgressPct === null || pctRounded - this._ratiosLastProgressPct >= 5) {
                    this._ratiosLastProgressPct = pctRounded;
                    this.addRatiosLog(statusText, 'info');
                    if (Number.isFinite(Number(inserted))) {
                        this.addRatiosLog(`DB 寫入：已寫入 ${this.formatInteger(Number(inserted))} 筆`, 'info');
                    }
                }

                if (!running) {
                    this.stopRatiosProgressTimer();
                }
            } catch (_) {
                // ignore polling errors
            }
        }, 1500);
    }

    async fetchRatiosData(writeToDb = false) {
        try {
            this.clearRatiosLog(true);
            const yearStr = document.getElementById('ratiosYear')?.value;
            const seasonStr = document.getElementById('ratiosSeason')?.value || '1';
            const year = parseInt(yearStr || '', 10);
            const season = parseInt(seasonStr || '1', 10);
            if (!Number.isFinite(year) || year < 2000) {
                this.addRatiosLog('請輸入正確的西元年度（例如 2025）', 'warning');
                this.showToast?.('請輸入正確的西元年度（例如 2025）', 'warning');
                return;
            }
            if (![1, 2, 3, 4].includes(season)) {
                this.addRatiosLog('請選擇 1-4 季之一', 'warning');
                this.showToast?.('請選擇 1-4 季之一', 'warning');
                return;
            }

            const codeFrom = String(document.getElementById('ratiosCodeFrom')?.value || '').trim();
            const codeTo = String(document.getElementById('ratiosCodeTo')?.value || '').trim();

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

            const params = new URLSearchParams();
            params.set('year', String(year));
            params.set('season', String(season));
            if (codeFrom) params.set('code_from', codeFrom);
            if (codeTo) params.set('code_to', codeTo);
            if (writeToDb) params.set('write_to_db', 'true');
            if (this.useLocalDb) params.set('use_local_db', 'true');

            const url = `${base}/api/financial-ratios?${params.toString()}`;

            this.addRatiosLog(`開始計算財務比率：年度 ${year} / 第 ${season} 季`, 'info');
            if (codeFrom || codeTo) {
                this.addRatiosLog(`股票代號範圍：${codeFrom || '最小'} ~ ${codeTo || '最大'}`, 'info');
            }
            if (writeToDb) {
                this.addRatiosLog(
                    `將在伺服器端同步寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}）`,
                    'info',
                );
            }
            this.addRatiosLog(`GET ${url}`, 'info');

            // 進度輪詢（需要後端提供 /api/financial-ratios/status）
            this.startRatiosProgressPolling(base);

            const badge = document.getElementById('ratiosSummaryBadge');
            if (badge) badge.textContent = '計算中…';

            const res = await fetch(url);
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(txt || `HTTP ${res.status}`);
            }
            const json = await res.json();
            const data = Array.isArray(json) ? json : (json.data || []);
            const meta = (json && json.meta) ? json.meta : {};

            this.ratiosData = data;
            this.renderRatiosResultsTable();
            this.updateRatiosSummary(meta);

            const rows = meta.rows ?? (Array.isArray(data) ? data.length : 0);
            const inserted = meta.inserted;
            if (writeToDb) {
                this.addRatiosLog(
                    `✅ 計算完成：共 ${this.formatInteger(rows)} 筆；寫入 ${Number.isFinite(Number(inserted)) ? this.formatInteger(inserted) : inserted ?? '--'} 筆`,
                    'success',
                );
            } else {
                this.addRatiosLog(`✅ 計算完成：共 ${this.formatInteger(rows)} 筆`, 'success');
            }

            if (writeToDb) {
                this.showToast?.('✅ 已計算並寫入資料庫', 'success');
            } else {
                this.showToast?.('✅ 財務比率計算完成', 'success');
            }

            this.stopRatiosProgressTimer();
            this.updateRatiosProgress(100, '完成');
        } catch (err) {
            console.error('[Ratios] fetch error', err);
            const badge = document.getElementById('ratiosSummaryBadge');
            if (badge) badge.textContent = '失敗';
            this.addRatiosLog(`財務比率計算失敗：${err.message}`, 'error');
            this.showToast?.(`財務比率計算失敗：${err.message}`, 'error');

            this.stopRatiosProgressTimer();
            this.updateRatiosProgress(0, '失敗');
        }
    }

    updateRatiosSummary(meta = {}) {
        const total = meta.rows ?? (Array.isArray(this.ratiosData) ? this.ratiosData.length : 0);
        const period = meta.period || '--';
        const inserted = meta.inserted ?? '--';
        const badge = document.getElementById('ratiosSummaryBadge');
        if (badge) badge.textContent = period || '尚未執行';
        this.setTextContent('ratiosStatTotal', total);
        this.setTextContent('ratiosStatPeriod', period);
        this.setTextContent('ratiosStatInserted', inserted);
    }

    renderRatiosResultsTable() {
        const tbody = document.querySelector('#ratiosResultsTable tbody');
        if (!tbody) return;
        const rows = Array.isArray(this.ratiosData) ? this.ratiosData : [];
        if (!rows.length) {
            tbody.innerHTML = `
                <tr class="no-data-row">
                    <td colspan="16" class="no-data-cell">
                        <div class="no-data-content">
                            <div class="no-data-icon"><i class="fas fa-search"></i></div>
                            <div class="no-data-text">
                                <h4>尚無資料</h4>
                                <p>請先輸入年度與季別後執行計算</p>
                            </div>
                        </div>
                    </td>
                </tr>`;
            return;
        }

        const fmtPct = (v) => {
            if (v === null || v === undefined || v === '') return '';
            const n = Number(v);
            if (!Number.isFinite(n)) return String(v);
            return (n * 100).toFixed(2) + '%';
        };
        const fmtInt = (v) => {
            if (v === null || v === undefined || v === '') return '';
            const n = Number(v);
            if (!Number.isFinite(n)) return String(v);
            try {
                return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
            } catch (_) {
                return String(Math.round(n));
            }
        };
        const fmtNum = (v) => {
            if (v === null || v === undefined || v === '') return '';
            const n = Number(v);
            if (!Number.isFinite(n)) return String(v);
            return n.toFixed(4);
        };

        const sample = rows.slice(0, 200);
        const bodyHtml = sample
            .map((r) => `
                <tr>
                    <td>${r.symbol || ''}</td>
                    <td>${r.period || ''}</td>
                    <td class="number">${fmtInt(r.revenue)}</td>
                    <td class="number">${fmtInt(r.gross_profit)}</td>
                    <td class="number">${fmtInt(r.op_profit)}</td>
                    <td class="number">${fmtInt(r.net_profit)}</td>
                    <td class="number">${fmtInt(r.assets)}</td>
                    <td class="number">${fmtInt(r.equity)}</td>
                    <td class="number">${fmtPct(r.gross_margin)}</td>
                    <td class="number">${fmtPct(r.op_margin)}</td>
                    <td class="number">${fmtPct(r.net_margin)}</td>
                    <td class="number">${fmtPct(r.roa)}</td>
                    <td class="number">${fmtPct(r.roe)}</td>
                    <td class="number">${fmtPct(r.debt_ratio)}</td>
                    <td class="number">${fmtNum(r.current_ratio)}</td>
                    <td class="number">${fmtNum(r.quick_ratio)}</td>
                </tr>`)
            .join('');

        const extraRow =
            rows.length > sample.length
                ? `<tr><td colspan="16" class="text-muted">僅顯示前 ${sample.length} 筆（共 ${this.formatInteger(rows.length)} 筆）</td></tr>`
                : '';
        tbody.innerHTML = bodyHtml + extraRow;
    }

    initializeBalanceLogPanel() {
        const panel = document.getElementById('balanceLogPanel');
        if (panel) {
            this.balanceLogPanel = panel;
            if (!this._balanceLogInitialized) {
                this._balanceLogInitialized = true;
                this.setBalanceLogEmptyState();
            }
        }

        const clearBtn = document.getElementById('balanceLogClearBtn');
        if (clearBtn && !clearBtn.dataset.bound) {
            clearBtn.addEventListener('click', (ev) => {
                ev.preventDefault();
                this.clearBalanceLog();
            });
            clearBtn.dataset.bound = 'true';
        }
    }

    setBalanceLogEmptyState() {
        if (!this.balanceLogPanel) return;
        this.balanceLogPanel.innerHTML = '';
        const empty = document.createElement('div');
        empty.className = 'balance-log-empty';
        empty.style.color = '#94a3b8';
        empty.textContent = '尚未開始抓取';
        this.balanceLogPanel.appendChild(empty);
    }

    clearBalanceLog(silent = false) {
        this.initializeBalanceLogPanel();
        if (!this.balanceLogPanel) return;
        this.setBalanceLogEmptyState();
        if (!silent) {
            this.addBalanceLog('日誌已清空', 'info');
        }
    }

    addBalanceLog(message, level = 'info') {
        this.initializeBalanceLogPanel();
        if (!this.balanceLogPanel) return;

        const panel = this.balanceLogPanel;
        const empty = panel.querySelector('.balance-log-empty');
        if (empty) empty.remove();

        const colors = {
            info: '#60a5fa',
            success: '#4ade80',
            warning: '#fbbf24',
            error: '#f87171',
        };
        const icons = {
            info: 'ℹ️',
            success: '✅',
            warning: '⚠️',
            error: '❌',
        };

        const entry = document.createElement('div');
        entry.style.display = 'flex';
        entry.style.alignItems = 'flex-start';
        entry.style.gap = '0.5rem';
        entry.style.padding = '0.4rem 0.6rem';
        entry.style.marginBottom = '0.35rem';
        entry.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        entry.style.background = 'rgba(15,23,42,0.55)';
        entry.style.borderRadius = '6px';
        entry.style.boxShadow = 'inset 0 0 0 1px rgba(148,163,184,0.08)';

        const timeSpan = document.createElement('span');
        timeSpan.style.fontFamily = "'Roboto Mono','Courier New',monospace";
        timeSpan.style.fontSize = '0.75rem';
        timeSpan.style.color = '#cbd5f5';
        timeSpan.style.minWidth = '3.6rem';
        timeSpan.textContent = this.getCurrentTimeString();

        const iconSpan = document.createElement('span');
        iconSpan.style.minWidth = '1.2rem';
        iconSpan.textContent = icons[level] || '•';

        const textSpan = document.createElement('span');
        textSpan.style.flex = '1';
        textSpan.style.color = colors[level] || '#e2e8f0';
        textSpan.style.whiteSpace = 'pre-wrap';
        textSpan.textContent = message;

        entry.appendChild(timeSpan);
        entry.appendChild(iconSpan);
        entry.appendChild(textSpan);
        panel.appendChild(entry);

        if (this.balanceLogAutoScroll) {
            panel.scrollTop = panel.scrollHeight;
        }
    }

    updateBalanceProgress(percentage, message) {
        const fill = document.getElementById('balanceProgressFill');
        const text = document.getElementById('balanceProgressText');
        const status = document.getElementById('balanceProgressStatus');
        const pct = Math.max(0, Math.min(100, Math.round(Number(percentage) || 0)));
        if (fill) fill.style.width = `${pct}%`;
        if (text) text.textContent = `${pct}%`;
        if (status && typeof message === 'string' && message) status.textContent = message;
    }

    stopBalanceProgressTimer() {
        if (this._balanceProgressTimer) {
            clearInterval(this._balanceProgressTimer);
            this._balanceProgressTimer = null;
        }
    }

    async fetchBalanceData() {
        try {
            const yearStr = document.getElementById('balanceYear')?.value;
            const seasonStr = document.getElementById('balanceSeason')?.value || '1';
            const year = parseInt(yearStr || '', 10);
            const season = parseInt(seasonStr || '1', 10);

            if (!Number.isFinite(year) || year < 2000) {
                this.addBalanceLog('請輸入正確的西元年度（例如 2025）', 'warning');
                return;
            }
            if (![1, 2, 3, 4].includes(season)) {
                this.addBalanceLog('請選擇 1-4 季之一', 'warning');
                return;
            }

            const periodLabel = `${year}${String(season).padStart(2, '0')}`;
            const codeFromEl = document.getElementById('balanceCodeFrom');
            const codeToEl = document.getElementById('balanceCodeTo');
            const codeFrom = codeFromEl ? String(codeFromEl.value || '').trim() : '';
            const codeTo = codeToEl ? String(codeToEl.value || '').trim() : '';
            if (codeFrom || codeTo) {
                this.addBalanceLog(`本次抓取僅限股票代號範圍：${codeFrom || '最小'} ~ ${codeTo || '最大'}`, 'info');
            }

            const batchSizeStr = document.getElementById('balanceBatchSize')?.value || '';
            const restMinutesStr = document.getElementById('balanceBatchRestMinutes')?.value || '';
            const retryMaxStr = document.getElementById('balanceRetryMax')?.value || '';
            const retryWaitMinutesStr = document.getElementById('balanceRetryWaitMinutes')?.value || '';
            const batchSize = parseInt(batchSizeStr, 10);
            const restMinutes = parseFloat(restMinutesStr);
            const retryMax = parseInt(retryMaxStr, 10);
            const retryWaitMinutes = parseFloat(retryWaitMinutesStr);
            const hasBatch = Number.isFinite(batchSize) && batchSize > 0;
            const hasRest = Number.isFinite(restMinutes) && restMinutes > 0;
            const hasRetryMax = Number.isFinite(retryMax) && retryMax >= 0;
            const hasRetryWait = Number.isFinite(retryWaitMinutes) && retryWaitMinutes > 0;
            if (hasBatch && hasRest) {
                this.addBalanceLog(`節流設定：每抓取 ${batchSize} 檔休息 ${restMinutes} 分鐘後繼續。`, 'info');
            }
            if (hasRetryMax) {
                this.addBalanceLog(`封鎖自動續抓設定：最多暫停/重試 ${retryMax} 次（每次 ${hasRetryWait ? retryWaitMinutes : 5} 分鐘）。`, 'info');
            }

            const autoImportEl = document.getElementById('balanceAutoImportCheckbox');
            const writeToDb = !!(autoImportEl && autoImportEl.checked);
            if (writeToDb) {
                this.addBalanceLog(
                    `本次抓取將在伺服器端同步寫入資料庫（目標：${this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端'}）。`,
                    'info',
                );
            }

            this.addLogMessage(`📥 抓取資產負債表：${periodLabel} 全市場`, 'info');
            this.clearBalanceLog(true);
            this.addBalanceLog(`開始抓取資產負債表：年度 ${year}，季別 ${season}`, 'info');
            this.addBalanceLog('此操作會依序抓取所有上市櫃公司的 MOPS 資產負債表，執行時間可能長達數十分鐘以上，請耐心等待。', 'warning');

            this.stopBalanceProgressTimer();
            this.updateBalanceProgress(5, '準備開始抓取…');

            const params = new URLSearchParams({ year: String(year), season: String(season) });
            if (codeFrom) params.append('code_from', codeFrom);
            if (codeTo) params.append('code_to', codeTo);
            if (hasBatch) params.append('pause_every', String(batchSize));
            if (hasRest) params.append('pause_minutes', String(restMinutes));
            params.append('retry_on_block', '1');
            params.append('retry_wait_minutes', String(hasRetryWait ? retryWaitMinutes : 5));
            if (hasRetryMax) params.append('retry_max', String(retryMax));
            if (writeToDb) {
                params.append('write_to_db', '1');
                params.append('use_local_db', this.useLocalDb ? 'true' : 'false');
            }

            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const requestUrl = `${base}/api/balance-sheet`;
            this.addBalanceLog(`向伺服器發送請求：${requestUrl}?${params.toString()}`, 'info');

            this.updateBalanceProgress(10, '已送出請求至伺服器，等待進度回報…');

            this._balanceProgressTimer = window.setInterval(async () => {
                try {
                    const res = await fetch(`${base}/api/balance-sheet/status`);
                    if (!res.ok) return;
                    const json = await res.json();
                    if (!json || !json.success || !json.status) return;
                    const st = json.status;
                    const total = Number(st.total || 0);
                    const processed = Number(st.processed || 0);

                    let pct = 10;
                    if (total > 0 && processed >= 0) {
                        pct = Math.max(10, Math.min(99, Math.round((processed / total) * 100)));
                    }

                    let msg = '';
                    if (st.running) {
                        if (st.paused) {
                            const resumeAt = st.resumeAt || '';
                            const blocks = Number(st.block_count || 0);
                            msg = `伺服器暫停中（已觸發防護${Number.isFinite(blocks) && blocks > 0 ? ` ${blocks} 次` : ''}），預計 ${resumeAt || '稍後'} 續抓…`;
                        } else if (total > 0 && processed > 0) {
                            msg = `伺服器處理中：第 ${processed}/${total} 檔（${st.current_code || ''}）`;
                        } else {
                            msg = '伺服器處理中，等待進度資料…';
                        }
                    } else {
                        msg = '伺服器已回應，前端正在整理資料…';
                    }

                    if (st && st.db_write_enabled) {
                        const inserted = Number(st.db_inserted_rows || 0);
                        if (Number.isFinite(inserted) && inserted > 0) {
                            msg += `｜DB 已寫入 ${this.formatInteger(inserted)} 筆`;
                        } else {
                            msg += '｜DB 寫入中…';
                        }
                    }

                    this.updateBalanceProgress(pct, msg);
                } catch (err) {
                    console.warn('balance-sheet status poll error', err);
                }
            }, 5000);

            const startedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const resp = await fetch(`${requestUrl}?${params.toString()}`);
            if (!resp.ok) {
                let msg = `HTTP ${resp.status}`;
                try {
                    const j = await resp.json();
                    if (j && j.error) msg = j.error;
                } catch (_) {}
                throw new Error(msg);
            }
            const data = await resp.json();
            if (!Array.isArray(data)) {
                throw new Error('伺服器回傳格式錯誤（預期為陣列）');
            }

            const finishedAt = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsedMs = Math.max(0, finishedAt - startedAt);
            const elapsedSec = (elapsedMs / 1000).toFixed(2);
            this.addBalanceLog(`伺服器回應成功，開始處理資料（耗時 ${elapsedSec} 秒）`, 'success');

            this.stopBalanceProgressTimer();

            this.balanceData = data;
            this.updateBalanceSummary(data, year, season);
            this.renderBalanceResultsTable();

            const total = data.length;
            const unique = new Set();
            data.forEach((row) => {
                if (row && row['股票代號']) unique.add(row['股票代號']);
            });

            this.addLogMessage(`✅ 完成資產負債表抓取，共 ${total} 筆，涵蓋 ${unique.size} 檔股票`, 'success');
            this.addBalanceLog(`資料已整理完成：共 ${this.formatInteger(total)} 筆，${this.formatInteger(unique.size)} 檔股票`, 'success');
            this.updateBalanceProgress(100, `完成：共 ${this.formatInteger(total)} 筆，${this.formatInteger(unique.size)} 檔股票`);
        } catch (err) {
            console.error('[Balance] fetch error', err);

            try {
                const origin = (window && window.location && window.location.origin)
                    ? window.location.origin
                    : '';
                const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
                const statusResp = await fetch(`${base}/api/balance-sheet/status`);
                if (statusResp.ok) {
                    const js = await statusResp.json();
                    if (js && js.success && js.status) {
                        const st = js.status || {};
                        const total = Number(st.total || 0);
                        const processed = Number(st.processed || 0);
                        const code = st.current_code || '';

                        if (total > 0 && processed > 0) {
                            this.addBalanceLog(
                                `最後進度：第 ${this.formatInteger(processed)} / ${this.formatInteger(total)} 檔（${code || '股票代號未知'}）`,
                                'warning',
                            );
                        } else if (code) {
                            this.addBalanceLog(`最後進度：正在處理股票 ${code}`, 'warning');
                        }
                    }
                }
            } catch (_) {}

            this.addLogMessage(`❌ 資產負債表抓取失敗：${err.message}`, 'error');
            this.addBalanceLog(`資產負債表抓取失敗：${err.message}`, 'error');
            this.stopBalanceProgressTimer();
            this.updateBalanceProgress(0, '抓取失敗，請查看下方日誌訊息');
        }
    }

    exportBalanceCsv() {
        try {
            const rows = Array.isArray(this.balanceData) ? this.balanceData : [];
            if (!rows.length) {
                this.addBalanceLog('目前沒有可匯出的資產負債表資料，請先執行抓取。', 'warning');
                return;
            }

            const escapeCSV = (value) => {
                if (value === null || value === undefined) return '';
                const str = String(value).replace(/"/g, '""');
                return /[",\r\n]/.test(str) ? `"${str}"` : str;
            };

            const first = rows[0] || {};
            const columns = Object.keys(first);
            const headerLine = columns.map(escapeCSV).join(',');
            const lines = [headerLine];
            rows.forEach((row) => {
                const line = columns.map((col) => escapeCSV(row[col])).join(',');
                lines.push(line);
            });

            const csvContent = '\ufeff' + lines.join('\r\n');
            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            const ts = new Date().toISOString().replace(/[:.]/g, '-');
            a.href = url;
            a.download = `balance_sheet_${ts}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            this.addBalanceLog('✅ 資產負債表資料已匯出為 CSV 檔案', 'success');
        } catch (err) {
            console.error('[Balance] export CSV error', err);
            this.addBalanceLog(`資產負債表匯出失敗：${err.message}`, 'error');
        }
    }

    updateBalanceSummary(data, year, season) {
        const rows = Array.isArray(data) ? data : [];
        const total = rows.length;
        const codes = new Set();
        let period = null;
        rows.forEach((row) => {
            if (row && row['股票代號']) codes.add(row['股票代號']);
            if (!period && row && row.period) period = row.period;
        });
        if (!period) {
            if (Number.isFinite(year) && [1, 2, 3, 4].includes(season)) {
                period = `${year}${String(season).padStart(2, '0')}`;
            } else {
                period = '--';
            }
        }

        this.setTextContent('balanceStatUnique', codes.size || '--');
        this.setTextContent('balanceStatTotal', total || '--');
        this.setTextContent('balanceStatPeriod', period || '--');
        const badge = document.getElementById('balanceSummaryBadge');
        if (badge) badge.textContent = period || '尚未執行';
    }

    renderBalanceResultsTable() {
        const tbody = document.querySelector('#balanceResultsTable tbody');
        if (!tbody) return;
        const rows = Array.isArray(this.balanceData) ? this.balanceData : [];
        if (!rows.length) {
            tbody.innerHTML = `
                <tr class="no-data-row">
                    <td colspan="6" class="no-data-cell">
                        <div class="no-data-content">
                            <div class="no-data-icon"><i class="fas fa-search"></i></div>
                            <div class="no-data-text">
                                <h4>尚無資料</h4>
                                <p>請先輸入年度與季別後執行抓取</p>
                            </div>
                        </div>
                    </td>
                </tr>`;
            return;
        }

        const fmtNum = (v, digits = 0) => {
            if (v === null || v === undefined || v === '') return '';
            const n = Number(v);
            if (!Number.isFinite(n)) return String(v);
            if (digits > 0) return n.toFixed(digits);
            return n.toLocaleString();
        };

        const sample = rows.slice(0, 200);
        const bodyHtml = sample
            .map((r) => `
                <tr>
                    <td>${r['股票代號'] || ''}</td>
                    <td>${r.period || ''}</td>
                    <td class="number">${fmtNum(r.CashAndCashEquivalents)}</td>
                    <td class="number">${fmtNum(r.CurrentAssets)}</td>
                    <td class="number">${fmtNum(r.Liabilities)}</td>
                    <td class="number">${fmtNum(r.EquityAndLiabilities)}</td>
                </tr>`)
            .join('');
        const extraRow =
            rows.length > sample.length
                ? `<tr><td colspan="6" class="text-muted">僅顯示前 ${sample.length} 筆（共 ${this.formatInteger(rows.length)} 筆）</td></tr>`
                : '';
        tbody.innerHTML = bodyHtml + extraRow;
    }

    /** =========================
     *  現金流量表 (Cash Flow)
     *  ========================= */
    setupCashflowListeners() {
        this.safeAddEventListener('cashflowFetchBtn', () => this.fetchCashflowData());
        this.safeAddEventListener('cashflowMultiFetchBtn', () => this.fetchCashflowMultiPeriod());
        this.safeAddEventListener('cashflowImportDbBtn', () => this.importCashflowToDb());
        this.safeAddEventListener('cashflowExportBtn', () => this.exportCashflowCsv());
        this.safeAddEventListener('cashflowLogClearBtn', () => this.clearCashflowLog());
        this.cashflowLogPanel = document.getElementById('cashflowLogPanel');
    }

    clearCashflowLog() {
        this.cashflowLogPanel = document.getElementById('cashflowLogPanel');
        if (this.cashflowLogPanel) this.cashflowLogPanel.textContent = '';
    }

    addCashflowLog(message, level = 'info') {
        this.cashflowLogPanel = document.getElementById('cashflowLogPanel');
        if (!this.cashflowLogPanel) return;
        if (this.cashflowLogPanel.textContent === '尚未開始抓取') {
            this.cashflowLogPanel.textContent = '';
        }
        const colors = {
            info: '#60a5fa', success: '#4ade80', warning: '#fbbf24', error: '#f87171',
        };
        const row = document.createElement('div');
        row.style.padding = '.35rem .5rem';
        row.style.marginBottom = '.25rem';
        row.style.borderLeft = `3px solid ${colors[level] || '#94a3b8'}`;
        row.style.background = 'rgba(15,23,42,.55)';
        row.style.borderRadius = '5px';
        row.textContent = `[${this.getCurrentTimeString()}] ${message}`;
        this.cashflowLogPanel.appendChild(row);
        this.cashflowLogPanel.scrollTop = this.cashflowLogPanel.scrollHeight;
    }

    updateCashflowProgress(percentage, message) {
        const pct = Math.max(0, Math.min(100, Math.round(Number(percentage) || 0)));
        const fill = document.getElementById('cashflowProgressFill');
        const text = document.getElementById('cashflowProgressText');
        const status = document.getElementById('cashflowProgressStatus');
        if (fill) fill.style.width = `${pct}%`;
        if (text) text.textContent = `${pct}%`;
        if (status && message) status.textContent = message;
    }

    stopCashflowProgressTimer() {
        if (this._cashflowProgressTimer) {
            clearInterval(this._cashflowProgressTimer);
            this._cashflowProgressTimer = null;
        }
    }

    cashflowBaseUrl() {
        const origin = window?.location?.origin || '';
        return origin && origin !== 'file://' ? origin : 'http://localhost:5003';
    }

    cashflowRequestParams(year, season) {
        const params = new URLSearchParams({ year: String(year), season: String(season) });
        const code = String(document.getElementById('cashflowSingleCode')?.value || '').trim();
        const codeFrom = String(document.getElementById('cashflowCodeFrom')?.value || '').trim();
        const codeTo = String(document.getElementById('cashflowCodeTo')?.value || '').trim();
        const batch = parseInt(document.getElementById('cashflowBatchSize')?.value || '', 10);
        const rest = parseFloat(document.getElementById('cashflowBatchRestMinutes')?.value || '');
        const retryMax = parseInt(document.getElementById('cashflowRetryMax')?.value || '1', 10);
        const retryWait = parseFloat(document.getElementById('cashflowRetryWaitMinutes')?.value || '5');
        if (code) params.set('code', code);
        if (!code && codeFrom) params.set('code_from', codeFrom);
        if (!code && codeTo) params.set('code_to', codeTo);
        if (!code && Number.isFinite(batch) && batch > 0) params.set('pause_every', String(batch));
        if (!code && Number.isFinite(rest) && rest > 0) params.set('pause_minutes', String(rest));
        if (!code) {
            params.set('retry_on_block', '1');
            params.set('retry_max', String(Number.isFinite(retryMax) ? Math.max(0, retryMax) : 1));
            params.set('retry_wait_minutes', String(Number.isFinite(retryWait) && retryWait > 0 ? retryWait : 5));
        }
        if (document.getElementById('cashflowAutoImportCheckbox')?.checked) {
            params.set('write_to_db', '1');
            params.set('use_local_db', this.useLocalDb ? 'true' : 'false');
        }
        return { params, isSingle: !!code };
    }

    formatCashflowProgressMessage(state) {
        if (!state) return '準備中…';
        if (state.paused) {
            return `MOPS 暫停中，預計 ${state.resumeAt || '稍後'} 續抓`;
        }
        const phase = String(state.phase || '');
        if (phase === 'connecting_db') {
            return '正在連線資料庫…';
        }
        if (phase === 'loading_codes' || (state.total == null && Number(state.processed || 0) === 0)) {
            return '正在載入股票清單…';
        }
        const total = Number(state.total || 0);
        const processed = Number(state.processed || 0);
        const code = state.current_code ? ` ${state.current_code}` : '';
        if (total > 0 && processed === 0) {
            return `即將開始抓取，共 ${this.formatInteger(total)} 檔…`;
        }
        let message = `處理中 ${processed}/${total || '?'}${code}`;
        if (state.db_write_enabled) {
            message += `｜DB 已寫入 ${this.formatInteger(state.db_inserted_rows || 0)} 筆`;
        }
        return message;
    }

    cashflowProgressPercent(state) {
        if (!state) return 3;
        const phase = String(state.phase || '');
        if (phase === 'connecting_db') return 3;
        if (phase === 'loading_codes') return 5;
        const total = Number(state.total || 0);
        const processed = Number(state.processed || 0);
        if (total <= 0) return 5;
        if (processed <= 0) return 8;
        return Math.max(8, Math.min(99, (processed / total) * 100));
    }

    startCashflowProgressPolling(base) {
        this.stopCashflowProgressTimer();
        const poll = async () => {
            try {
                const response = await fetch(`${base}/api/cash-flow-statement/status`);
                if (!response.ok) return;
                const payload = await response.json();
                const state = payload?.status;
                if (!state) return;
                const message = this.formatCashflowProgressMessage(state);
                const pct = this.cashflowProgressPercent(state);
                this.updateCashflowProgress(pct, message);
                if (!state.running) this.stopCashflowProgressTimer();
            } catch (_) {}
        };
        poll();
        this._cashflowProgressTimer = window.setInterval(poll, 3000);
    }

    async requestCashflowPeriod(year, season) {
        const { params, isSingle } = this.cashflowRequestParams(year, season);
        const base = this.cashflowBaseUrl();
        const url = `${base}/api/cash-flow-statement?${params.toString()}`;
        if (!isSingle) {
            this.startCashflowProgressPolling(base);
            this.updateCashflowProgress(3, '正在載入股票清單…');
        }
        this.addCashflowLog(`抓取 ${year} 年第 ${season} 季${isSingle ? '單一股票' : ''}`, 'info');
        const response = await fetch(url);
        if (!response.ok) {
            let message = `HTTP ${response.status}`;
            try {
                const payload = await response.json();
                message = payload.error || message;
            } catch (_) {}
            throw new Error(message);
        }
        const data = await response.json();
        if (!Array.isArray(data)) throw new Error('伺服器回傳格式錯誤');
        return data;
    }

    async fetchCashflowData() {
        this.clearCashflowLog();
        const year = parseInt(document.getElementById('cashflowYear')?.value || '', 10);
        const season = parseInt(document.getElementById('cashflowSeason')?.value || '1', 10);
        if (!Number.isFinite(year) || year < 2000 || ![1, 2, 3, 4].includes(season)) {
            this.addCashflowLog('請輸入正確年度並選擇第 1 至第 4 季', 'warning');
            return;
        }
        try {
            this.updateCashflowProgress(3, '正在啟動抓取…');
            const data = await this.requestCashflowPeriod(year, season);
            this.cashflowData = data;
            this.updateCashflowSummary(data, year, season);
            this.renderCashflowResultsTable();
            this.updateCashflowProgress(100, `完成：${this.formatInteger(data.length)} 筆`);
            this.addCashflowLog(`抓取完成，共 ${this.formatInteger(data.length)} 筆`, 'success');
        } catch (error) {
            this.updateCashflowProgress(0, '抓取失敗');
            this.addCashflowLog(`抓取失敗：${error.message}`, 'error');
        } finally {
            this.stopCashflowProgressTimer();
        }
    }

    async fetchCashflowMultiPeriod() {
        this.clearCashflowLog();
        const from = parseInt(document.getElementById('cashflowYearFrom')?.value || '', 10);
        const to = parseInt(document.getElementById('cashflowYearTo')?.value || '', 10);
        const seasons = [1, 2, 3, 4].filter(
            (season) => document.getElementById(`cashflowMultiSeason${season}`)?.checked,
        );
        if (!Number.isFinite(from) || !Number.isFinite(to) || from > to || !seasons.length) {
            this.addCashflowLog('請設定有效的多期起訖年度及至少一個季別', 'warning');
            return;
        }
        const tasks = [];
        for (let year = from; year <= to; year += 1) {
            seasons.forEach((season) => tasks.push({ year, season }));
        }
        const originalCode = document.getElementById('cashflowSingleCode');
        const savedCode = originalCode?.value || '';
        if (originalCode) originalCode.value = '';
        const merged = [];
        try {
            for (let index = 0; index < tasks.length; index += 1) {
                const task = tasks[index];
                this.updateCashflowProgress((index / tasks.length) * 100, `多期 ${index + 1}/${tasks.length}`);
                const rows = await this.requestCashflowPeriod(task.year, task.season);
                merged.push(...rows);
                this.addCashflowLog(`${task.year} Q${task.season}：${rows.length} 筆`, 'success');
            }
            this.cashflowData = merged;
            this.updateCashflowSummary(merged);
            this.renderCashflowResultsTable();
            this.updateCashflowProgress(100, `多期完成：${merged.length} 筆`);
        } catch (error) {
            this.addCashflowLog(`多期抓取中止：${error.message}`, 'error');
            this.updateCashflowProgress(0, '多期抓取失敗');
        } finally {
            if (originalCode) originalCode.value = savedCode;
            this.stopCashflowProgressTimer();
        }
    }

    async importCashflowToDb() {
        if (!Array.isArray(this.cashflowData) || !this.cashflowData.length) {
            this.addCashflowLog('目前沒有可寫入的資料', 'warning');
            return;
        }
        try {
            const response = await fetch(`${this.cashflowBaseUrl()}/api/cash-flow-statement/import`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rows: this.cashflowData, use_local_db: this.useLocalDb }),
            });
            const payload = await response.json();
            if (!response.ok || !payload.success) throw new Error(payload.error || `HTTP ${response.status}`);
            this.addCashflowLog(`已寫入資料庫 ${this.formatInteger(payload.inserted || 0)} 筆`, 'success');
        } catch (error) {
            this.addCashflowLog(`資料庫寫入失敗：${error.message}`, 'error');
        }
    }

    exportCashflowCsv() {
        const rows = Array.isArray(this.cashflowData) ? this.cashflowData : [];
        if (!rows.length) {
            this.addCashflowLog('目前沒有可匯出的資料', 'warning');
            return;
        }
        const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
        const escape = (value) => {
            if (value === null || value === undefined) return '';
            const text = String(value).replace(/"/g, '""');
            return /[",\r\n]/.test(text) ? `"${text}"` : text;
        };
        const csv = '\ufeff' + [
            columns.map(escape).join(','),
            ...rows.map((row) => columns.map((column) => escape(row[column])).join(',')),
        ].join('\r\n');
        const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }));
        const link = document.createElement('a');
        link.href = url;
        link.download = `cash_flow_statement_${new Date().toISOString().replace(/[:.]/g, '-')}.csv`;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        this.addCashflowLog('CSV 匯出完成', 'success');
    }

    updateCashflowSummary(data, year, season) {
        const rows = Array.isArray(data) ? data : [];
        const codes = new Set(rows.map((row) => row?.['股票代號']).filter(Boolean));
        const periods = [...new Set(rows.map((row) => row?.period).filter(Boolean))];
        const fallback = Number.isFinite(year) && season
            ? `${year}${String(season).padStart(2, '0')}` : '--';
        const periodText = periods.length > 1 ? `${periods[0]} ~ ${periods[periods.length - 1]}` : (periods[0] || fallback);
        this.setTextContent('cashflowStatUnique', codes.size || '--');
        this.setTextContent('cashflowStatTotal', rows.length || '--');
        this.setTextContent('cashflowStatPeriod', periodText);
        const badge = document.getElementById('cashflowSummaryBadge');
        if (badge) badge.textContent = periodText;
    }

    renderCashflowResultsTable() {
        const body = document.querySelector('#cashflowResultsTable tbody');
        if (!body) return;
        const rows = Array.isArray(this.cashflowData) ? this.cashflowData : [];
        if (!rows.length) {
            body.innerHTML = '<tr class="no-data-row"><td colspan="6" class="no-data-cell">尚無資料</td></tr>';
            return;
        }
        const fmt = (value) => {
            const number = Number(value);
            return value === null || value === undefined || value === ''
                ? '' : (Number.isFinite(number) ? number.toLocaleString() : String(value));
        };
        body.innerHTML = rows.slice(0, 200).map((row) => `
            <tr>
                <td>${row['股票代號'] || ''}</td><td>${row.period || ''}</td>
                <td class="number">${fmt(row.NetCashFlowsFromUsedInOperatingActivities)}</td>
                <td class="number">${fmt(row.NetCashFlowsFromUsedInInvestingActivities)}</td>
                <td class="number">${fmt(row.NetCashFlowsFromUsedInFinancingActivities)}</td>
                <td class="number">${fmt(row.NetIncreaseDecreaseInCashAndCashEquivalents)}</td>
            </tr>`).join('');
    }

    // 觸發後端計算報酬率
    async computeReturnsFromUI() {
        try {
            // 從既有的 UI 取得日期與股票範圍配置
            const cfg = this.getUpdateConfig();
            if (!cfg.valid) {
                this.addLogMessage(cfg.error || '請先設定有效的時間範圍', 'warning');
                return;
            }

            const symbolInput = document.getElementById('returnsSymbol')?.value?.trim();
            const customStart = document.getElementById('returnsStartDate')?.value;
            const customEnd = document.getElementById('returnsEndDate')?.value;
            const fillMissing = !!document.getElementById('returnsFillMissing')?.checked;
            const maxWorkersInput = document.getElementById('returnsMaxWorkers')?.value;
            const batchSizeInput = document.getElementById('returnsBatchSize')?.value;
            
            // 獲取資料庫選擇
            const dbTarget = document.querySelector('input[name="returnsDbTarget"]:checked')?.value || 'local';

            const rangeStart = customStart || cfg.startDate;
            const rangeEnd = customEnd || cfg.endDate;

            if (rangeStart && rangeEnd && rangeStart > rangeEnd) {
                this.addLogMessage('⚠️ 計算報酬率日期範圍錯誤：開始日期不可晚於結束日期', 'warning');
                return;
            }

            const payload = {
                start: rangeStart,
                end: rangeEnd,
                fillMissing: fillMissing,
            };

            // 并行/批次參數（可空，留給後端使用默認）
            const maxWorkersVal = parseInt(maxWorkersInput, 10);
            if (!Number.isNaN(maxWorkersVal) && maxWorkersVal > 0) {
                payload.max_workers = Math.min(32, Math.max(1, maxWorkersVal));
            }
            const batchSizeVal = parseInt(batchSizeInput, 10);
            if (!Number.isNaN(batchSizeVal) && batchSizeVal > 0) {
                payload.batch_size = Math.min(200, Math.max(1, batchSizeVal));
            }
            
            // 根據選擇設定資料庫參數
            if (dbTarget === 'local') {
                payload.use_local_db = true;
                payload.upload_to_neon = false;
            } else if (dbTarget === 'neon') {
                payload.use_local_db = false;
                payload.upload_to_neon = false;
            } else if (dbTarget === 'both') {
                payload.use_local_db = true;
                payload.upload_to_neon = true;
            }

            if (symbolInput) {
                payload.symbol = symbolInput;
            } else {
                // 沒填 symbol 時，根據 UI 決定 all/limit 行為
                payload.all = true;
                if (!cfg.updateAllStocks && typeof cfg.stockLimit === 'number' && cfg.stockLimit > 0) {
                    payload.limit = cfg.stockLimit;
                }
            }
            
            // 組建資料庫描述
            let dbDesc = '';
            if (dbTarget === 'local') {
                dbDesc = '⚡ 僅本地';
            } else if (dbTarget === 'neon') {
                dbDesc = '☁️ 僅Neon';
            } else if (dbTarget === 'both') {
                dbDesc = '🔄 同時兩邊';
            }

            this.addLogMessage(`🧮 開始計算報酬：${payload.symbol || `ALL${payload.limit ? ` (limit=${payload.limit})` : ''}`}，範圍 ${payload.start || '(未指定)'}~${payload.end || '(未指定)'}，fillMissing=${fillMissing}，資料庫: ${dbDesc}`, 'info');

            if (this.returnsEventSource) {
                try { this.returnsEventSource.close(); } catch (_) {}
            }

            const params = new URLSearchParams();
            Object.entries(payload).forEach(([key, value]) => {
                if (value === undefined || value === null) return;
                if (typeof value === 'boolean') {
                    params.append(key, value ? 'true' : 'false');
                } else {
                    params.append(key, String(value));
                }
            });

            const streamUrl = `http://localhost:5003/api/returns/compute_stream?${params.toString()}`;
            this.addLogMessage(`📡 以串流模式監控進度`, 'info');
            this.updateProgress(0, '已送出計算請求...');

            let processed = 0;
            let totalSymbols = 0;
            let summaryData = null;
            let seenSummary = false;

            const es = new EventSource(streamUrl);
            this.returnsEventSource = es;

            es.onmessage = (evt) => {
                if (!evt.data) return;
                let payloadData;
                try {
                    payloadData = JSON.parse(evt.data);
                } catch (err) {
                    console.error('progress event parse error', err, evt.data);
                    this.addLogMessage('⚠️ 解析進度資料失敗，略過該筆訊息', 'warning');
                    return;
                }

                const eventType = payloadData.event || 'progress';

                if (eventType === 'connected') {
                    this.updateProgress(1, '已連線，等待開始');
                    return;
                }

                if (eventType === 'start') {
                    totalSymbols = payloadData.total || 0;
                    processed = 0;
                    this.addLogMessage(`🚀 開始計算報酬（共 ${totalSymbols || '未知'} 檔）`, 'info');
                    this.updateProgress(totalSymbols ? 1 : 5, totalSymbols ? `準備處理 ${totalSymbols} 檔股票` : '準備處理股票');
                    return;
                }

                if (eventType === 'progress') {
                    processed = payloadData.index || processed;
                    const symbol = payloadData.symbol || '(未知代碼)';
                    const written = payloadData.written ?? 0;
                    const writtenNeon = payloadData.written_neon;
                    const reason = payloadData.reason;
                    const error = payloadData.error;
                    const neonError = payloadData.neon_error;

                    let message = `➡️ ${symbol} 處理完成`; 
                    if (written > 0) {
                        message += `，寫入 ${written} 筆`;
                    } else if (reason === 'no_prices') {
                        message += '，無價格資料（跳過）';
                    } else if (reason === 'empty_returns') {
                        message += '，無可計算的報酬（跳過）';
                    } else if (reason === 'already_up_to_date') {
                        message += '，資料已完整（fillMissing 已略過）';
                    } else if (reason === 'no_new_records') {
                        message += '，無新報酬需要寫入';
                    } else if (error) {
                        message += `，錯誤：${error}`;
                    } else {
                        message += '，無新增資料';
                    }

                    if (writtenNeon !== undefined && writtenNeon !== null) {
                        message += ` | Neon: ${writtenNeon} 筆`;
                    }
                    if (neonError) {
                        message += ` | Neon錯誤: ${neonError}`;
                    }

                    const progress = totalSymbols ? Math.min(99, Math.round((processed / totalSymbols) * 100)) : 50;
                    this.updateProgress(progress, `已處理 ${processed}${totalSymbols ? `/${totalSymbols}` : ''} 檔`);
                    const level = written > 0 ? 'info' : (error ? 'error' : (reason === 'already_up_to_date' ? 'info' : 'warning'));
                    this.addLogMessage(message, level);
                    return;
                }

                if (eventType === 'summary' && payloadData.summary) {
                    summaryData = payloadData.summary;
                    seenSummary = true;
                    this.renderReturnsSummary(summaryData, dbTarget);
                    return;
                }

                if (eventType === 'error') {
                    const errMsg = payloadData.error || '未知錯誤';
                    this.addLogMessage(`❌ 報酬計算錯誤：${errMsg}`, 'error');
                    this.updateProgress(0, '計算失敗');
                    return;
                }

                if (eventType === 'done') {
                    try { es.close(); } catch (_) {}
                    this.returnsEventSource = null;
                    if (!seenSummary && summaryData) {
                        this.renderReturnsSummary(summaryData, dbTarget);
                    }
                    if (!seenSummary && !summaryData) {
                        this.addLogMessage('⚠️ 計算完成但未收到摘要資料', 'warning');
                    }
                    this.updateProgress(100, '計算完成');
                    return;
                }

                // 其他事件類型直接記錄
                this.addLogMessage(`ℹ️ ${eventType}: ${JSON.stringify(payloadData)}`, 'info');
            };

            es.onerror = (evt) => {
                console.error('compute returns stream error', evt);
                this.addLogMessage('❌ 報酬計算串流中斷，請稍後重試', 'error');
                this.updateProgress(0, '串流中斷');
                try { es.close(); } catch (_) {}
                this.returnsEventSource = null;
            };
        } catch (e) {
            console.error('computeReturns error', e);
            this.addLogMessage(`計算報酬失敗：${e.message}`, 'error');
            this.updateProgress(0, '計算失敗');
        }
    }

    renderReturnsSummary(summary, dbTarget) {
        if (!summary || typeof summary !== 'object') {
            this.addLogMessage('⚠️ 無法顯示回傳摘要', 'warning');
            return;
        }

        const total = summary.total_written || 0;
        const totalNeon = summary.total_written_neon;

        if (totalNeon !== undefined) {
            this.addLogMessage(`✅ 報酬計算完成：本地 ${total} 筆，Neon ${totalNeon} 筆`, 'success');
        } else if (dbTarget === 'local') {
            this.addLogMessage(`✅ 報酬計算完成：本地資料庫 ${total} 筆`, 'success');
        } else if (dbTarget === 'neon') {
            this.addLogMessage(`✅ 報酬計算完成：Neon 資料庫 ${total} 筆`, 'success');
        } else {
            this.addLogMessage(`✅ 報酬計算完成：共寫入 ${total} 筆`, 'success');
        }

        if (Array.isArray(summary.symbols)) {
            const preview = summary.symbols.slice(0, 10);
            preview.forEach((s, i) => {
                const w = typeof s.written === 'number' ? s.written : 0;
                const wNeon = typeof s.written_neon === 'number' ? s.written_neon : null;
                const note = s.reason ? ` (${s.reason})` : (s.error ? ` (錯誤: ${s.error})` : '');

                let msg = `${i + 1}. ${s.symbol}: ${w} 筆`;
                if (wNeon !== null) {
                    msg += ` | Neon: ${wNeon} 筆`;
                }
                if (s.neon_error) {
                    msg += ` | Neon錯誤: ${s.neon_error}`;
                }
                msg += note;

                this.addLogMessage(msg, w > 0 ? 'info' : (s.error ? 'error' : 'warning'));
            });
            if (summary.symbols.length > preview.length) {
                this.addLogMessage(`... 其餘 ${summary.symbols.length - preview.length} 檔省略`, 'info');
            }
        }
    }

    // 透過後端 API 匯入 ^TWII 日K（yfinance）至 tw_stock_prices
    async importTwiiFromYFinance() {
        try {
            // 嘗試沿用目前「時間範圍設定」的日期；若無效則退回預設
            let start = null;
            let end = null;
            if (typeof this.getUpdateConfig === 'function') {
                try {
                    const cfg = this.getUpdateConfig();
                    if (cfg && cfg.valid && cfg.startDate && cfg.endDate) {
                        start = cfg.startDate;
                        end = cfg.endDate;
                    }
                } catch (_) {}
            }

            if (!start || !end) {
                const today = new Date();
                const todayStr = this.formatDate(today);
                // 若未指定則使用較長區間，交由後端 DEFAULT_START_DATE 控制
                start = null;
                end = todayStr;
            }

            const payload = {};
            if (start) payload.start = start;
            if (end) payload.end = end;
            if (this.useLocalDb) payload.use_local_db = true;

            const targetLabel = this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端';
            const rangeLabel = start && end ? `${start} ~ ${end}` : `預設起始 ~ ${payload.end || 'today'}`;
            this.addLogMessage(`📈 開始匯入加權指數 (^TWII)：範圍 ${rangeLabel}，目標：${targetLabel}`, 'info');

            const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
                ? window.location.origin
                : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

            const resp = await fetch(`${base}/api/prices/twii/import_yf`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }

            const fetched = data.fetched ?? 0;
            const inserted = data.inserted ?? 0;
            this.addLogMessage(`✅ 匯入加權指數完成：抓取 ${fetched} 筆，寫入 ${inserted} 筆`, 'success');

            try {
                const computePayload = {};
                computePayload.symbol = '^TWII';
                if (start) computePayload.start = start;
                if (end) computePayload.end = end;
                computePayload.fill_missing = true;
                if (this.useLocalDb) computePayload.use_local_db = true;

                this.addLogMessage('🧮 開始計算 ^TWII 報酬率並寫入 tw_stock_returns...', 'info');

                const retResp = await fetch(`${base}/api/returns/compute`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(computePayload)
                });
                const retData = await retResp.json();
                if (!retResp.ok || !retData.success) {
                    throw new Error(retData.error || `HTTP ${retResp.status}`);
                }

                const totalWritten = retData.total_written ?? 0;
                this.addLogMessage(`✅ ^TWII 報酬率計算完成：寫入 ${totalWritten} 筆`, 'success');
            } catch (retErr) {
                this.addLogMessage(`⚠️ ^TWII 報酬率計算失敗：${retErr.message}`, 'warning');
            }
        } catch (err) {
            this.addLogMessage(`❌ 匯入加權指數失敗：${err.message}`, 'error');
        }
    }
    
    // 安全的事件監聽器綁定方法
    safeAddEventListener(elementId, handler) {
        const element = document.getElementById(elementId);
        if (element) {
            element.addEventListener('click', handler);
            console.log(`✅ 綁定事件: ${elementId}`);
        } else {
            console.warn(`⚠️ 元素不存在: ${elementId}`);
        }
    }

    initializeDates() {
        const today = new Date();
        const lastYear = new Date(today.getFullYear() - 1, today.getMonth(), today.getDate());
        
        document.getElementById('startDate').value = this.formatDate(lastYear);
        document.getElementById('endDate').value = this.formatDate(today);

        // 初始化異常檢核日期（預設與更新日期一致，若元素存在）
        const anStart = document.getElementById('anomalyStartDate');
        const anEnd = document.getElementById('anomalyEndDate');
        if (anStart) anStart.value = this.formatDate(lastYear);
        if (anEnd) anEnd.value = this.formatDate(today);

        const t86Start = document.getElementById('t86StartDate');
        const t86End = document.getElementById('t86EndDate');
        if (t86Start) t86Start.value = this.formatDate(lastYear);
        if (t86End) t86End.value = this.formatDate(today);
    }

    initializeDisplayAreas() {
        // 初始化股票範圍顯示區域
        const limitInputs = document.getElementById('limitInputs');
        const rangeInputs = document.getElementById('rangeInputs');
        if (limitInputs) limitInputs.style.display = 'block';
        if (rangeInputs) rangeInputs.style.display = 'none';
        
        // 初始化日期範圍顯示區域
        const recentOptions = document.getElementById('recentOptions');
        const dateInputs = document.getElementById('dateInputs');
        if (recentOptions) recentOptions.style.display = 'block';
        if (dateInputs) dateInputs.style.display = 'none';
    }

    formatDate(date) {
        return date.toISOString().split('T')[0];
    }

    switchTab(tabName) {
        console.log(`🔄 切換到標籤: ${tabName}`);
        
        // 移除所有現代化標籤按鈕的 active 類
        document.querySelectorAll('.modern-tab-btn').forEach(btn => btn.classList.remove('active'));
        
        // 添加 active 類到點擊的標籤
        const activeTab = document.querySelector(`.modern-tab-btn[data-tab="${tabName}"]`);
        if (activeTab) {
            activeTab.classList.add('active');
            console.log(`✅ 標籤 ${tabName} 已激活`);
        } else {
            console.log(`❌ 找不到標籤: ${tabName}`);
        }

        // 切換標籤內容面板
        document.querySelectorAll('.tab-pane').forEach(pane => {
            pane.classList.remove('active');
            pane.style.display = 'none';
        });
        const targetPane = document.getElementById(`${tabName}Tab`);
        if (targetPane) {
            targetPane.classList.add('active');
            targetPane.style.display = 'block';
            console.log(`✅ 內容面板 ${tabName}Tab 已顯示`);
        } else {
            console.log(`❌ 找不到內容面板: ${tabName}Tab`);
        }

        const names = { 'update': '資料更新', 'query': '資料查詢', 'stats': '資料統計', 'settings': '系統設定', 'sync': '資料庫同步', 'bwibbu': 'BWIBBU 回朔', 't86': '三大法人 (T86)' };
        this.addLogMessage(`切換到${names[tabName] || tabName}頁面`, 'info');
        
        // 如果切換到同步頁面，自動檢查 Neon 連接
        if (tabName === 'sync') {
            setTimeout(() => {
                this.checkNeonConnection();
                this.setupSyncEventListeners();
            }, 100);
        }

        if (tabName === 'query') {
            setTimeout(() => {
                this.loadQueryTables();
            }, 100);
        }
    }

    getSelectedQueryTable() {
        try {
            const el = document.getElementById('queryTableSelect');
            if (!el) return '';
            return String(el.value || '').trim();
        } catch (e) {
            return '';
        }
    }

    async loadQueryTables() {
        const selectEl = document.getElementById('queryTableSelect');
        if (!selectEl) return;

        try {
            const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const params = new URLSearchParams();
            if (this.useLocalDb) params.set('use_local_db', 'true');
            const resp = await fetch(`${base}/api/tables${params.toString() ? `?${params.toString()}` : ''}`);
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.success) {
                return;
            }

            const current = this.getSelectedQueryTable();
            const tables = Array.isArray(data.tables) ? data.tables : [];
            const options = [{ label: '自動（依查詢類型）', value: '' }];
            tables.forEach(t => {
                const name = t && (t.name || t.tablename || t.table);
                if (name) options.push({ label: String(name), value: String(name) });
            });

            selectEl.innerHTML = '';
            options.forEach(opt => {
                const o = document.createElement('option');
                o.value = opt.value;
                o.textContent = opt.label;
                selectEl.appendChild(o);
            });

            if (current && options.some(o => o.value === current)) {
                selectEl.value = current;
            } else {
                selectEl.value = '';
            }
        } catch (e) {
        }
    }
    
    setupSyncEventListeners() {
        const btnLoadTables = document.getElementById('btnLoadTables');
        const btnSelectAll = document.getElementById('btnSelectAll');
        const btnDeselectAll = document.getElementById('btnDeselectAll');
        const btnClearSyncLog = document.getElementById('btnClearSyncLog');
        const btnExportTablesExcel = document.getElementById('btnExportTablesExcel');
        
        if (btnLoadTables && !btnLoadTables.dataset.listenerAdded) {
            btnLoadTables.addEventListener('click', () => this.loadTableList());
            btnLoadTables.dataset.listenerAdded = 'true';
        }
        
        if (btnSelectAll && !btnSelectAll.dataset.listenerAdded) {
            btnSelectAll.addEventListener('click', () => this.selectAllTables(true));
            btnSelectAll.dataset.listenerAdded = 'true';
        }
        
        if (btnDeselectAll && !btnDeselectAll.dataset.listenerAdded) {
            btnDeselectAll.addEventListener('click', () => this.selectAllTables(false));
            btnDeselectAll.dataset.listenerAdded = 'true';
        }

        if (btnClearSyncLog && !btnClearSyncLog.dataset.listenerAdded) {
            btnClearSyncLog.addEventListener('click', () => this.clearSyncLog());
            btnClearSyncLog.dataset.listenerAdded = 'true';
        }

        if (btnExportTablesExcel && !btnExportTablesExcel.dataset.listenerAdded) {
            btnExportTablesExcel.addEventListener('click', () => this.exportSyncTablesToExcel());
            btnExportTablesExcel.dataset.listenerAdded = 'true';
        }

        this.ensureDbSyncSse();
    }

    ensureDbSyncSse() {
        if (this._dbSyncSse) return;
        try {
            const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
                ? window.location.origin
                : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';
            const es = new EventSource(`${base}/api/stream/logs`);
            this._dbSyncSse = es;

            es.onopen = () => {
                this.showSyncLogPanel();
                this.addSyncLogLine('✅ 同步日誌已連線（SSE）', 'success');
            };

            es.onmessage = (evt) => {
                let payload;
                try {
                    payload = JSON.parse(evt.data);
                } catch (_) {
                    return;
                }
                if (!payload || payload.channel !== 'db_sync') return;
                this.handleDbSyncEvent(payload);
            };

            es.onerror = () => {
                this.addSyncLogLine('⚠️ 同步日誌連線中斷，將在下次進入頁面時重試', 'warning');
            };
        } catch (_) {
        }
    }

    handleDbSyncEvent(payload) {
        const msg = payload.message || '';
        const event = payload.event;
        const direction = payload.direction;
        const table = payload.table;

        const progressInfo = document.getElementById('syncProgressInfo');
        const logLabel = direction === 'download' ? '下載' : (direction === 'upload' ? '上傳' : '同步');

        if (event === 'start') {
            this.clearSyncLog(true);
            this.showSyncLogPanel();
            this.addSyncLogLine(`🚀 ${msg || `開始${logLabel}同步`}`, 'info');
            return;
        }

        if (event === 'local_connected' || event === 'neon_connected') {
            this.addSyncLogLine(`✅ ${msg}`, 'success');
            return;
        }

        if (event === 'table_start') {
            this.addSyncLogLine(`📦 ${msg}`, 'info');
            if (progressInfo) {
                progressInfo.textContent = `${logLabel}中：${table || ''}`;
                progressInfo.style.color = '';
            }
            return;
        }

        if (event === 'table_truncate') {
            this.addSyncLogLine(`🧹 ${msg}`, 'warning');
            return;
        }

        if (event === 'table_info') {
            this.addSyncLogLine(`📊 ${msg}`, 'info');
            return;
        }

        if (event === 'batch_progress') {
            this.addSyncLogLine(`⏳ ${msg}`, 'info');
            if (progressInfo) {
                progressInfo.textContent = msg;
                progressInfo.style.color = '';
            }
            return;
        }

        if (event === 'table_skip') {
            this.addSyncLogLine(`⚠️ ${msg}`, 'warning');
            return;
        }

        if (event === 'table_done') {
            this.addSyncLogLine(`✅ ${msg}`, 'success');
            return;
        }

        if (event === 'done') {
            this.addSyncLogLine(`🎉 ${msg}`, 'success');
            return;
        }

        if (event === 'error') {
            this.addSyncLogLine(`❌ ${msg}`, 'error');
            if (progressInfo) {
                progressInfo.textContent = msg || '同步發生錯誤';
                progressInfo.style.color = '#ef4444';
            }
            return;
        }

        if (msg) {
            this.addSyncLogLine(msg, 'info');
        }
    }

    showSyncLogPanel() {
        const section = document.getElementById('syncLogSection');
        if (section) section.style.display = 'block';
    }

    clearSyncLog(silent = false) {
        const panel = document.getElementById('syncLogPanel');
        if (!panel) return;
        panel.innerHTML = '<div class="sync-log-empty" style="color:#94a3b8;">尚未開始同步</div>';
        if (!silent) {
            this.addSyncLogLine('已清空日誌', 'info');
        }
    }

    addSyncLogLine(text, level = 'info') {
        const panel = document.getElementById('syncLogPanel');
        if (!panel) return;
        const empty = panel.querySelector('.sync-log-empty');
        if (empty) empty.remove();

        const line = document.createElement('div');
        const ts = new Date().toLocaleTimeString();

        let color = '#e2e8f0';
        if (level === 'success') color = '#22c55e';
        if (level === 'warning') color = '#f59e0b';
        if (level === 'error') color = '#ef4444';

        line.style.color = color;
        line.textContent = `[${ts}] ${text}`;
        panel.appendChild(line);
        panel.scrollTop = panel.scrollHeight;
    }
    
    async loadTableList() {
        try {
            this.addLogMessage('📋 載入表格列表...', 'info');

            const sourceSel = document.getElementById('syncTablesSource');
            const source = sourceSel && sourceSel.value ? sourceSel.value : 'local';
            this.addLogMessage(`📋 表格列表來源：${source === 'neon' ? 'Neon 雲端' : '本機資料庫'}`, 'info');

            this._lastSyncTablesSource = source;
            this._lastSyncTables = [];
            const btnExportTablesExcel = document.getElementById('btnExportTablesExcel');
            if (btnExportTablesExcel) btnExportTablesExcel.disabled = true;

            const qs = new URLSearchParams();
            qs.set('source', source);
            const response = await fetch(`/api/database-sync/tables?${qs.toString()}`);
            const data = await response.json();
            
            if (data.success && data.tables) {
                this._lastSyncTables = Array.isArray(data.tables) ? data.tables : [];
                this.displayTableList(data.tables);
                this.addLogMessage(`✅ 成功載入 ${data.tables.length} 個表格`, 'success');
            } else {
                throw new Error(data.error || '載入表格列表失敗');
            }
        } catch (error) {
            this.addLogMessage(`❌ 載入表格列表失敗: ${error.message}`, 'error');
        }
    }
    
    displayTableList(tables) {
        const tableListContainer = document.getElementById('tableListContainer');
        const tableList = document.getElementById('tableList');
        const btnExportTablesExcel = document.getElementById('btnExportTablesExcel');
        
        if (!tableList) return;
        
        tableList.innerHTML = '';

        this._syncTableCheckboxEls = [];
        
        tables.forEach(table => {
            const item = document.createElement('div');
            item.className = 'table-selection-item';
            item.innerHTML = `
                <input type="checkbox" id="table_${table.name}" value="${table.name}" checked>
                <div class="table-selection-info">
                    <div class="table-selection-name">${table.name}</div>
                    <div class="table-selection-meta">${table.rowCount.toLocaleString()} 行 · ${table.columnCount} 列</div>
                </div>
            `;
            
            const checkbox = item.querySelector('input[type="checkbox"]');
            if (checkbox) {
                this._syncTableCheckboxEls.push(checkbox);
            }
            checkbox.addEventListener('change', () => {
                item.classList.toggle('selected', checkbox.checked);
                this.updateTableSelectionCount();
            });
            
            item.addEventListener('click', (e) => {
                if (e.target.tagName !== 'INPUT') {
                    checkbox.checked = !checkbox.checked;
                    checkbox.dispatchEvent(new Event('change'));
                }
            });
            
            item.classList.add('selected');
            tableList.appendChild(item);
        });
        
        tableListContainer.style.display = 'block';
        this.updateTableSelectionCount();

        if (btnExportTablesExcel) {
            btnExportTablesExcel.disabled = !(Array.isArray(tables) && tables.length > 0);
        }
        
        // 啟用上傳按鈕
        const btnStartSync = document.getElementById('btnStartSync');
        if (btnStartSync) {
            btnStartSync.disabled = false;
        }

        const btnDownloadFromNeon = document.getElementById('btnDownloadFromNeon');
        if (btnDownloadFromNeon) {
            btnDownloadFromNeon.disabled = false;
        }
    }

    exportSyncTablesToExcel() {
        try {
            const tables = Array.isArray(this._lastSyncTables) ? this._lastSyncTables : [];
            if (!tables.length) {
                this.addLogMessage('⚠️ 尚未載入表格列表，請先點「載入表格列表」', 'warning');
                return;
            }

            const sel = this.getTableSelectionDebug ? this.getTableSelectionDebug() : { selectedTables: [] };
            const selectedTables = Array.isArray(sel.selectedTables) ? sel.selectedTables : [];
            if (selectedTables.length === 0) {
                this.addLogMessage('⚠️ 請至少勾選一個表格再匯出', 'warning');
                return;
            }

            const sourceSel = document.getElementById('syncTablesSource');
            const source = (sourceSel && sourceSel.value) ? sourceSel.value : (this._lastSyncTablesSource || 'local');

            const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
                ? window.location.origin
                : '';
            const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

            const qs = new URLSearchParams();
            qs.set('source', source);
            const url = `${base}/api/database-sync/export_csv?${qs.toString()}`;

            this.addLogMessage(`📤 產生 CSV(zip) 中（${source === 'neon' ? 'Neon' : '本機'}，${selectedTables.length} 表）...`, 'info');

            fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tables: selectedTables })
            }).then(async (resp) => {
                if (!resp.ok) {
                    let errMsg = `HTTP ${resp.status}`;
                    try {
                        const j = await resp.json();
                        if (j && j.error) errMsg = j.error;
                    } catch (_) {}
                    throw new Error(errMsg);
                }
                const blob = await resp.blob();

                const ts = new Date();
                const yyyy = ts.getFullYear();
                const mm = String(ts.getMonth() + 1).padStart(2, '0');
                const dd = String(ts.getDate()).padStart(2, '0');
                const filename = `sync_export_${source}_${yyyy}${mm}${dd}.zip`;

                const a = document.createElement('a');
                const objectUrl = URL.createObjectURL(blob);
                a.href = objectUrl;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(objectUrl);

                this.addLogMessage(`✅ 已匯出 CSV(zip)：${filename}`, 'success');
            }).catch((e) => {
                this.addLogMessage(`❌ 匯出 CSV(zip) 失敗: ${e.message}`, 'error');
            });
        } catch (e) {
            this.addLogMessage(`❌ 匯出 CSV(zip) 失敗: ${e.message}`, 'error');
        }
    }
    
    selectAllTables(select) {
        const checkboxes = (this._syncTableCheckboxEls && this._syncTableCheckboxEls.length > 0)
            ? this._syncTableCheckboxEls
            : Array.from(document.querySelectorAll('#tableList input[type="checkbox"]'));
        checkboxes.forEach(checkbox => {
            checkbox.checked = select;
            checkbox.closest('.table-selection-item').classList.toggle('selected', select);
        });
        this.updateTableSelectionCount();
    }
    
    updateTableSelectionCount() {
        const checkboxes = (this._syncTableCheckboxEls && this._syncTableCheckboxEls.length > 0)
            ? this._syncTableCheckboxEls
            : Array.from(document.querySelectorAll('#tableList input[type="checkbox"]'));
        const checked = Array.from(checkboxes).filter(cb => cb.checked).length;
        const total = checkboxes.length;
        
        const countElement = document.getElementById('tableSelectionCount');
        if (countElement) {
            countElement.textContent = `已選擇 ${checked} / ${total} 個表格`;
        }
        
        // 更新上傳按鈕狀態
        const btnStartSync = document.getElementById('btnStartSync');
        if (btnStartSync) {
            btnStartSync.disabled = checked === 0;
        }

        const btnDownloadFromNeon = document.getElementById('btnDownloadFromNeon');
        if (btnDownloadFromNeon) {
            btnDownloadFromNeon.disabled = checked === 0;
        }
    }
    
    getSelectedTables() {
        const sel = this.getTableSelectionDebug();
        return sel.selectedTables;
    }

    getTableSelectionDebug() {
        const cached = (this._syncTableCheckboxEls && this._syncTableCheckboxEls.length > 0)
            ? this._syncTableCheckboxEls
            : null;
        if (cached) {
            const checked = cached.filter(cb => cb && cb.checked);
            return {
                source: 'cached',
                selector: 'this._syncTableCheckboxEls',
                syncTabExists: !!document.getElementById('syncTab'),
                tableListExists: !!document.getElementById('tableList'),
                tableListChildCount: document.getElementById('tableList') ? document.getElementById('tableList').children.length : 0,
                inputsInTableList: document.getElementById('tableList') ? document.getElementById('tableList').querySelectorAll('input').length : 0,
                checkboxInSyncTab: document.getElementById('syncTab') ? document.getElementById('syncTab').querySelectorAll('input[type="checkbox"]').length : 0,
                idTableNodesInSyncTab: document.getElementById('syncTab') ? document.getElementById('syncTab').querySelectorAll('[id^="table_"]').length : 0,
                idTableNodesInTableList: document.getElementById('tableList') ? document.getElementById('tableList').querySelectorAll('[id^="table_"]').length : 0,
                total: cached.length,
                checked: checked.length,
                selectedTables: checked.map(cb => cb.value || cb.getAttribute('value'))
            };
        }

        const selectors = [
            '#syncTab #tableList input[type="checkbox"]',
            '#tableList input[type="checkbox"]',
            '#syncTab input[type="checkbox"][id^="table_"]',
            'input[type="checkbox"][id^="table_"]',
        ];

        let usedSelector = selectors[0];
        let nodes = [];
        for (const s of selectors) {
            const found = document.querySelectorAll(s);
            if (found && found.length > 0) {
                usedSelector = s;
                nodes = Array.from(found);
                break;
            }
        }

        const syncTabEl = document.getElementById('syncTab');
        const tableListEl = document.getElementById('tableList');
        const tableListChildCount = tableListEl ? tableListEl.children.length : 0;
        const inputsInTableList = tableListEl ? tableListEl.querySelectorAll('input').length : 0;
        const checkboxInSyncTab = syncTabEl ? syncTabEl.querySelectorAll('input[type="checkbox"]').length : 0;
        const idTableNodesInSyncTab = syncTabEl ? syncTabEl.querySelectorAll('[id^="table_"]').length : 0;
        const idTableNodesInTableList = tableListEl ? tableListEl.querySelectorAll('[id^="table_"]').length : 0;

        if (nodes.length === 0 && tableListEl) {
            const found = tableListEl.querySelectorAll('input[type="checkbox"]');
            if (found && found.length > 0) {
                usedSelector = 'tableListEl.querySelectorAll(input[type="checkbox"])';
                nodes = Array.from(found);
            }
        }

        // 若 DOM 有找到 checkbox，將其回填至快取，避免後續抓不到
        if ((!this._syncTableCheckboxEls || this._syncTableCheckboxEls.length === 0) && nodes.length > 0) {
            this._syncTableCheckboxEls = nodes;
        }

        const checked = nodes.filter(cb => cb && cb.checked);
        return {
            source: 'dom',
            selector: usedSelector,
            syncTabExists: !!syncTabEl,
            tableListExists: !!tableListEl,
            tableListChildCount,
            inputsInTableList,
            checkboxInSyncTab,
            idTableNodesInSyncTab,
            idTableNodesInTableList,
            total: nodes.length,
            checked: checked.length,
            selectedTables: checked.map(cb => cb.value || cb.getAttribute('value'))
        };
    }

    toggleRangeInputs() {
        const rangeInputs = document.getElementById('rangeInputs');
        const limitInputs = document.getElementById('limitInputs');
        const fromInput = document.getElementById('rangeFrom');
        const toInput = document.getElementById('rangeTo');
        
        // 隱藏所有輸入區域
        rangeInputs.style.display = 'none';
        limitInputs.style.display = 'none';
        fromInput.disabled = true;
        toInput.disabled = true;
        
        // 根據選擇顯示對應區域
        const selectedValue = document.querySelector('input[name="stockRange"]:checked').value;
        if (selectedValue === 'range') {
            rangeInputs.style.display = 'block';
            fromInput.disabled = false;
            toInput.disabled = false;
        } else if (selectedValue === 'limit') {
            limitInputs.style.display = 'block';
        }
    }

    toggleDateRangeInputs(e) {
        const recentOptions = document.getElementById('recentOptions');
        const dateInputs = document.getElementById('dateInputs');
        const startDate = document.getElementById('startDate');
        const endDate = document.getElementById('endDate');
        
        if (e.target.value === 'custom') {
            recentOptions.style.display = 'none';
            dateInputs.style.display = 'block';
            startDate.disabled = false;
            endDate.disabled = false;
            
            // 設定預設日期範圍（最近30天）
            const today = new Date();
            const thirtyDaysAgo = new Date(today);
            thirtyDaysAgo.setDate(today.getDate() - 30);
            
            endDate.value = today.toISOString().split('T')[0];
            startDate.value = thirtyDaysAgo.toISOString().split('T')[0];
        } else {
            recentOptions.style.display = 'block';
            dateInputs.style.display = 'none';
            startDate.disabled = true;
            endDate.disabled = true;
        }
    }

    async executeUpdate(configOverride = null) {
        console.log('📊 開始執行更新流程...');
        
        if (this.isUpdating) {
            this.addLogMessage('更新正在進行中，請稍候...', 'warning');
            return;
        }

        const startTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

        try {
            // 從新 UI 獲取配置，允許覆寫（供自動實驗使用）
            const baseConfig = this.getUpdateConfig();
            const config = configOverride ? { ...baseConfig, ...configOverride } : baseConfig;
            console.log('配置信息:', config);
            
            if (!config.valid) {
                this.addLogMessage(config.error, 'warning');
                return;
            }

            // 檢查是否需要執行特殊的批量更新
            if (config.executeListedStocks) {
                await this.updateAllListedStocks();
                return;
            }
            
            if (config.executeOtcStocks) {
                await this.updateAllOtcStocks();
                return;
            }

            // 更新操作狀態
            this.updateActionStatus('running', '正在執行...');
            
            // 開始計時並執行更新
            await this.startUpdateProcess(config);
            const endTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsed = endTime - startTime;
            const human = this.formatDuration(elapsed);
            this.addLogMessage(`✅ 更新完成，總耗時 ${human}`, 'success');
            this.updateActionStatus('ready', `已完成（${human}）`);
            this.updateProgress(100, `已完成（${human}）`);
            
        } catch (error) {
            console.error('執行更新時發生錯誤:', error);
            const endTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
            const elapsed = endTime - startTime;
            const human = this.formatDuration(elapsed);
            this.addLogMessage(`執行更新失敗: ${error.message}（總耗時 ${human}）`, 'error');
            this.updateActionStatus('error', `執行失敗（${human}）`);
        }
    }
    
    // 從新 UI 獲取更新配置
    getUpdateConfig() {
        console.log('🔍 獲取更新配置...');
        
        // 檢查是否選擇了預設時間範圍選項
        let activeTimeOption = document.querySelector('.quick-option.active');
        console.log('找到的活躍時間選項:', activeTimeOption);
        
        // 如果沒有活躍選項，強制設置默認選項（30天）
        if (!activeTimeOption) {
            console.log('沒有找到活躍的時間選項，嘗試設置默認選項...');
            const allQuickOptions = document.querySelectorAll('.quick-option[data-days]');
            console.log(`所有時間選項 (${allQuickOptions.length} 個):`, allQuickOptions);
            
            // 處理股票數量選項
            const countOptions = document.querySelectorAll('.count-option');
            countOptions.forEach(option => {
                option.addEventListener('click', () => {
                    // 移除所有活動狀態
                    countOptions.forEach(opt => opt.classList.remove('active'));
                    // 添加活動狀態到當前選項
                    option.classList.add('active');
                    
                    // 取消進階選項的選擇（互斥）
                    const advancedOptions = document.querySelectorAll('.advanced-option');
                    advancedOptions.forEach(opt => opt.classList.remove('active'));
                    console.log('📊 選擇股票數量選項，取消進階選項選擇');
                });
            });

            // 嘗試找到30天選項
            const defaultOption = document.querySelector('.quick-option[data-days="30"]');
            if (defaultOption) {
                // 清除所有活躍狀態
                allQuickOptions.forEach(opt => opt.classList.remove('active'));
                // 設置30天為活躍
                defaultOption.classList.add('active');
                activeTimeOption = defaultOption;
                console.log('✅ 強制設置30天為默認選項');
            } else {
                // 如果沒有30天選項，使用第一個可用選項
                const firstOption = allQuickOptions[0];
                if (firstOption) {
                    allQuickOptions.forEach(opt => opt.classList.remove('active'));
                    firstOption.classList.add('active');
                    activeTimeOption = firstOption;
                    console.log(`✅ 強制設置第一個選項 (${firstOption.getAttribute('data-days')}天) 為默認`);
                }
            }
        }
        
        let startDate, endDate;
        
        // 優先檢查自訂日期範圍
        const customToggle = document.querySelector('.custom-date-toggle');
        console.log('自訂日期切換按鈕:', customToggle);
        
        const isCustomActive = customToggle && customToggle.classList.contains('active');
        console.log('自訂日期範圍是否啟用:', isCustomActive);
        
        // 檢查自訂日期面板是否展開
        const customPanel = document.querySelector('.custom-date-panel');
        const isPanelActive = customPanel && customPanel.classList.contains('active');
        console.log('自訂日期面板是否展開:', isPanelActive);
        
        if (isCustomActive || isPanelActive) {
            // 使用自訂日期範圍
            startDate = document.getElementById('startDate')?.value;
            endDate = document.getElementById('endDate')?.value;
            
            console.log('自訂日期輸入值:', { startDate, endDate });
            
            if (!startDate || !endDate) {
                return { valid: false, error: '請設置自訂日期範圍' };
            }
            
            console.log(`📅 使用自訂日期範圍: ${startDate} 至 ${endDate}`);
        } else if (activeTimeOption) {
            // 使用預設時間範圍
            const daysStr = activeTimeOption.getAttribute('data-days');
            console.log(`取得 data-days 屬性: "${daysStr}"`);
            
            let days = parseInt(daysStr);
            console.log(`解析後的天數: ${days}`);
            
            // 如果還是無法獲取有效天數，使用硬編碼默認值
            if (isNaN(days) || days <= 0 || daysStr === null) {
                console.warn(`無效的天數設定，使用默認值30天。原值: ${daysStr}`);
                days = 30; // 硬編碼默認30天
            }
            
            // 使用更簡單的日期計算方法
            const today = new Date();
            console.log(`今天: ${today}`);
            
            const pastDate = new Date();
            pastDate.setFullYear(today.getFullYear());
            pastDate.setMonth(today.getMonth());
            pastDate.setDate(today.getDate() - days);
            
            console.log(`${days} 天前: ${pastDate}`);
            
            // 確保日期有效
            if (isNaN(today.getTime()) || isNaN(pastDate.getTime())) {
                console.error('日期計算錯誤 - 無效的日期對象');
                return { valid: false, error: '日期計算錯誤' };
            }
            
            try {
                endDate = today.toISOString().split('T')[0];
                startDate = pastDate.toISOString().split('T')[0];
                
                console.log(`📅 使用預設時間範圍: ${days} 天 (${startDate} 至 ${endDate})`);
            } catch (error) {
                console.error('日期轉換錯誤:', error);
                return { valid: false, error: '日期轉換失敗' };
            }
        } else {
            console.log('沒有選擇任何時間範圍選項，使用默認30天');
            // 使用默認30天作為後備方案
            const today = new Date();
            const pastDate = new Date(today.getTime() - (30 * 24 * 60 * 60 * 1000));
            
            endDate = today.toISOString().split('T')[0];
            startDate = pastDate.toISOString().split('T')[0];
            
            console.log(`📅 使用後備默認時間範圍: 30天 (${startDate} 至 ${endDate})`);
        }
        
        // 獲取股票數量限制
        const activeCountOption = document.querySelector('.count-option.active');
        let stockLimit = 50; // 默認值
        
        if (activeCountOption) {
            const count = activeCountOption.getAttribute('data-count');
            if (count) {
                stockLimit = parseInt(count);
            }
        }
        
        // 檢查是否選擇了進階選項
        let symbolRange = null;
        let updateAllStocks = false;
        let selectedIndices = [];
        const activeAdvancedOption = document.querySelector('.advanced-option.active');
        
        if (activeAdvancedOption) {
            const advancedType = activeAdvancedOption.getAttribute('data-type');
            console.log(`🔧 檢測到進階選項: ${advancedType}`);
            
            if (advancedType === 'all') {
                updateAllStocks = true;
                stockLimit = null; // 取消股票數量限制
                console.log('🌐 設置為更新所有股票模式');
            } else if (advancedType === 'listed') {
                // 標記為需要執行上市股票更新
                return { valid: true, executeListedStocks: true };
            } else if (advancedType === 'otc') {
                // 標記為需要執行上櫃股票更新
                return { valid: true, executeOtcStocks: true };
            } else if (advancedType === 'range') {
                const rangeFrom = document.getElementById('rangeFrom')?.value?.trim();
                const rangeTo = document.getElementById('rangeTo')?.value?.trim();
                
                if (rangeFrom && rangeTo) {
                    symbolRange = [rangeFrom, rangeTo];
                    console.log(`📊 設置股票代碼範圍: ${rangeFrom} - ${rangeTo}`);
                } else {
                    return { valid: false, error: '請輸入完整的股票代碼範圍' };
                }
            } else if (advancedType === 'indices') {
                const checkedIndices = document.querySelectorAll('.index-checkbox:checked');
                if (checkedIndices.length === 0) {
                    return { valid: false, error: '請至少選擇一個市場指數' };
                }
                
                selectedIndices = Array.from(checkedIndices).map(checkbox => {
                    const item = checkbox.closest('.index-item');
                    return item.dataset.symbol;
                });
                
                stockLimit = null; // 取消股票數量限制
                console.log(`📊 選擇的市場指數: ${selectedIndices.join(', ')}`);
            }
        }
        
        return {
            valid: true,
            startDate,
            endDate,
            stockLimit,
            symbolRange,
            updateAllStocks,
            selectedIndices,
            // 讀取效能參數（若不存在則使用預設值）
            batchSize: (() => {
                const el = document.getElementById('inputBatchSize');
                let v = parseInt(el?.value);
                if (isNaN(v)) v = 10;
                v = Math.max(1, Math.min(500, v));
                return v;
            })(),
            concurrency: (() => {
                const el = document.getElementById('inputConcurrency');
                let v = parseInt(el?.value);
                if (isNaN(v)) v = 20;
                v = Math.max(1, Math.min(100, v));
                return v;
            })(),
            interBatchDelay: (() => {
                const el = document.getElementById('inputInterBatchDelay');
                let v = parseInt(el?.value);
                if (isNaN(v)) v = 300;
                v = Math.max(0, Math.min(5000, v));
                return v;
            })()
        };
    }

    async startUpdateProcess(config) {
        // 更新按鈕狀態：禁用「執行」，啟用「取消」
        document.getElementById('executeUpdate').disabled = true;
        document.getElementById('cancelUpdate').disabled = false;

        this.isUpdating = true;
        this.updateProgress(0, '準備中...');
        
        const { startDate, endDate, stockLimit, symbolRange, updateAllStocks, selectedIndices, batchSize: updateBatchSize, concurrency: updateConcurrency, interBatchDelay: interBatchDelayMs } = config;
        
        this.addLogMessage(`開始更新股票數據`, 'info');
        this.addLogMessage(`📅 日期範圍: ${startDate} 至 ${endDate}`, 'info');
        
        if (updateAllStocks) {
            this.addLogMessage(`🌐 模式: 更新所有股票 (約2073檔)`, 'info');
        } else if (symbolRange) {
            this.addLogMessage(`🎯 股票代碼範圍: ${symbolRange[0]} 至 ${symbolRange[1]}`, 'info');
        } else if (selectedIndices && selectedIndices.length > 0) {
            this.addLogMessage(`📊 模式: 更新市場指數 (${selectedIndices.length}檔)`, 'info');
            this.addLogMessage(`📈 指數清單: ${selectedIndices.join(', ')}`, 'info');
        } else {
            this.addLogMessage(`📊 股票數量限制: ${stockLimit} 檔`, 'info');
        }
        
        try {
            // 紀錄效能參數
            this.addLogMessage(`⚙️ 參數設定 - 批次大小: ${updateBatchSize}、並行度: ${updateConcurrency}、批次間延遲: ${interBatchDelayMs} ms`, 'info');
            // 連接API服務器
            this.updateProgress(10, '正在連接 API 服務器...');
            this.addLogMessage('正在連接 API 服務器...', 'info');
            
            // 獲取股票代碼
            this.addLogMessage('抓取台灣股票代碼...', 'info');
            const symbolsUrl = this.useLocalDb ? 'http://localhost:5003/api/symbols?use_local_db=true' : 'http://localhost:5003/api/symbols';
            const symbolsResponse = await fetch(symbolsUrl);
            
            if (!symbolsResponse.ok) {
                throw new Error('無法連接到 API 服務器');
            }
            
            const symbolsData = await symbolsResponse.json();
            if (!symbolsData.success) {
                throw new Error(symbolsData.error || '獲取股票代碼失敗');
            }
            
            let symbols = symbolsData.data;

            // 一般股票更新流程中，不自動處理加權指數，改由專用「匯入加權指數」功能獨立處理
            if (!selectedIndices || selectedIndices.length === 0) {
                symbols = symbols.filter(stock => stock.symbol !== '^TWII');
            }
            
            // 根據配置處理股票列表
            if (updateAllStocks) {
                // 更新所有股票，不做任何限制
                this.addLogMessage(`🌐 準備更新所有 ${symbols.length} 檔股票`, 'info');
            } else if (symbolRange) {
                // 如果指定了股票代碼範圍，過濾符合範圍的股票
                const [fromCode, toCode] = symbolRange;
                symbols = symbols.filter(stock => {
                    const code = stock.symbol.replace(/\.(TW|TWO)$/, '');
                    return code >= fromCode && code <= toCode;
                });
                this.addLogMessage(`🎯 股票代碼範圍 ${fromCode}-${toCode}，找到 ${symbols.length} 檔股票`, 'info');
            } else if (selectedIndices && selectedIndices.length > 0) {
                // 如果選擇了市場指數，只處理選中的指數
                symbols = symbols.filter(stock => selectedIndices.includes(stock.symbol));
                this.addLogMessage(`📊 選擇的市場指數，找到 ${symbols.length} 檔指數`, 'info');
                
                // 如果沒有找到對應的指數，創建指數對象
                if (symbols.length === 0) {
                    symbols = selectedIndices.map(symbol => ({
                        symbol: symbol,
                        name: this.getIndexName(symbol),
                        market: symbol.startsWith('^') ? '指數' : 'ETF'
                    }));
                    this.addLogMessage(`📈 創建 ${symbols.length} 個指數對象進行更新`, 'info');
                }
            } else {
                // 使用股票數量限制
                symbols = symbols.slice(0, stockLimit);
                this.addLogMessage(`📊 限制處理前 ${stockLimit} 檔股票`, 'info');
            }
            
            this.addLogMessage(`✅ 準備處理 ${symbols.length} 檔股票`, 'success');
            
            // 設置更新選項：僅更新股價數據，不計算報酬率
            const updatePrices = true;   // 更新股價
            const updateReturns = false; // 不更新報酬率
            
            // 批量更新股票數據
            if (updatePrices || updateReturns) {
                this.updateProgress(20, '開始批量更新股票數據...');
                this.addLogMessage(`準備更新 ${symbols.length} 檔股票`, 'info');

                // 🌐 方案A：更新所有股票時只送一次請求，避免重複抓取「全市場按日資料」
                if (updateAllStocks) {
                    this.addLogMessage('🌐 全市場模式：改為單次請求（避免每批重複批量抓取）', 'info');
                    const batchStartTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

                    try {
                        this.addLogMessage(`🚀 批量抓取模式：一次處理 ${symbols.length} 檔股票`, 'info');
                        const fetchStartTime = new Date();
                        this.addLogMessage(`⏱️ 開始批量抓取: ${fetchStartTime.toLocaleString('zh-TW')}`, 'info');

                        const batchUpdateData = {
                            symbols: symbols.map(s => s.symbol),
                            update_prices: updatePrices,
                            update_returns: updateReturns,
                            start_date: startDate,
                            end_date: endDate,
                            respect_requested_range: true,
                            use_batch_mode: true,
                            use_local_db: this.useLocalDb
                        };

                        const resp = await fetch('http://localhost:5003/api/update', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(batchUpdateData)
                        });

                        if (!resp.ok) {
                            let bodyText = '';
                            try { bodyText = await resp.text(); } catch (_) { /* ignore */ }
                            throw new Error(`HTTP ${resp.status} ${resp.statusText || ''} ${bodyText ? '- ' + bodyText.slice(0, 300) : ''}`.trim());
                        }

                        let batchResult;
                        try {
                            batchResult = await resp.json();
                        } catch (e) {
                            throw new Error(`回應不是有效的 JSON：${e.message}`);
                        }

                        const fetchEndTime = new Date();
                        const fetchDuration = (fetchEndTime - fetchStartTime) / 1000;
                        this.addLogMessage(`⏱️ 批量抓取完成: ${fetchEndTime.toLocaleString('zh-TW')} (耗時 ${fetchDuration.toFixed(2)} 秒)`, 'info');

                        if (batchResult.success && batchResult.results) {
                            for (const result of batchResult.results) {
                                const stock = symbols.find(s => s.symbol === result.symbol);
                                const stockName = stock ? stock.name : result.symbol;

                                let storageInfo = [];
                                if (result.prices_updated !== undefined) storageInfo.push(`股價: ${result.prices_updated} 筆`);
                                if (result.mode) storageInfo.push(`模式: ${result.mode}`);

                                const statusText = storageInfo.length > 0 ? ` (${storageInfo.join(', ')})` : '';
                                this.addLogMessage(`✅ ${result.symbol} (${stockName}) 完成${statusText}`, 'success');
                            }
                        }

                        if (batchResult.errors && batchResult.errors.length > 0) {
                            for (const error of batchResult.errors) {
                                const stock = symbols.find(s => s.symbol === error.symbol);
                                const stockName = stock ? stock.name : error.symbol;
                                this.addLogMessage(`❌ ${error.symbol} (${stockName}) 失敗: ${error.error}`, 'error');
                            }
                        }

                        this.updateProgress(90, `已處理 ${symbols.length}/${symbols.length} 檔股票`);

                        const batchEndTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                        const batchElapsed = batchEndTime - batchStartTime;
                        const batchHuman = this.formatDuration(batchElapsed);
                        this.addLogMessage(`📦 全市場單次請求完成，耗時 ${batchHuman}`, 'info');
                    } catch (error) {
                        const batchEndTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                        const batchElapsed = batchEndTime - batchStartTime;
                        const batchHuman = this.formatDuration(batchElapsed);
                        this.addLogMessage(`全市場單次請求失敗: ${error.message}（耗時 ${batchHuman}）`, 'error');
                    }

                    // 跳過以下分批/並發流程
                    this.updateProgress(100, '更新完成');

                    this.addLogMessage('', 'info');
                    await this.computeReturnsAfterUpdate(symbols, startDate, endDate);
                    return;
                }
                
                // 分批處理避免超時
                const batchSize = updateBatchSize;
                const totalBatches = Math.ceil(symbols.length / batchSize);
                let processedCount = 0;
                
                // 🚀 使用並發處理批次，而非串行
                const maxConcurrentBatches = updateConcurrency; // 使用並行度設定
                this.addLogMessage(`🚀 並發模式：同時處理 ${maxConcurrentBatches} 個批次`, 'info');
                
                const processBatch = async (batchIndex) => {
                    const startIdx = batchIndex * batchSize;
                    const endIdx = Math.min(startIdx + batchSize, symbols.length);
                    const batchSymbols = symbols.slice(startIdx, endIdx);
                
                    this.addLogMessage(`開始批次 ${batchIndex + 1}/${totalBatches}，股票 ${startIdx + 1}-${endIdx}`, 'info');
                
                // 顯示當前批次的股票
                const symbolNames = batchSymbols.map(s => `${s.symbol}(${s.name})`).join(', ');
                this.addLogMessage(`當前批次: ${symbolNames}`, 'info');
                
                // 批次計時開始
                const batchStartTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

                try {
                    // 🚀 使用批量模式：一次發送整個批次
                    this.addLogMessage(`🚀 批量抓取模式：一次處理 ${batchSymbols.length} 檔股票`, 'info');
                    
                    const fetchStartTime = new Date();
                    this.addLogMessage(`⏱️ 開始批量抓取: ${fetchStartTime.toLocaleString('zh-TW')}`, 'info');

                    const batchUpdateData = {
                        symbols: batchSymbols.map(s => s.symbol),  // 發送整個批次
                        update_prices: updatePrices,
                        update_returns: updateReturns,
                        start_date: startDate,
                        end_date: endDate,
                        respect_requested_range: true,
                        use_batch_mode: true,  // 啟用批量模式
                        use_local_db: this.useLocalDb
                    };

                    const resp = await fetch('http://localhost:5003/api/update', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(batchUpdateData)
                    });
                    
                    if (!resp.ok) {
                        let bodyText = '';
                        try { bodyText = await resp.text(); } catch (_) { /* ignore */ }
                        throw new Error(`HTTP ${resp.status} ${resp.statusText || ''} ${bodyText ? '- ' + bodyText.slice(0, 300) : ''}`.trim());
                    }
                    
                    let batchResult;
                    try {
                        batchResult = await resp.json();
                    } catch (e) {
                        throw new Error(`回應不是有效的 JSON：${e.message}`);
                    }

                    const fetchEndTime = new Date();
                    const fetchDuration = (fetchEndTime - fetchStartTime) / 1000;
                    this.addLogMessage(`⏱️ 批量抓取完成: ${fetchEndTime.toLocaleString('zh-TW')} (耗時 ${fetchDuration.toFixed(2)} 秒)`, 'info');

                    // 處理批量結果
                    if (batchResult.success && batchResult.results) {
                        for (const result of batchResult.results) {
                            const stock = batchSymbols.find(s => s.symbol === result.symbol);
                            const stockName = stock ? stock.name : result.symbol;
                            
                            let storageInfo = [];
                            if (result.prices_updated !== undefined) storageInfo.push(`股價: ${result.prices_updated} 筆`);
                            if (result.mode) storageInfo.push(`模式: ${result.mode}`);

                            const statusText = storageInfo.length > 0 ? ` (${storageInfo.join(', ')})` : '';
                            this.addLogMessage(`✅ ${result.symbol} (${stockName}) 完成${statusText}`, 'success');
                        }
                        processedCount += batchSymbols.length;
                    }
                    
                    // 處理錯誤
                    if (batchResult.errors && batchResult.errors.length > 0) {
                        for (const error of batchResult.errors) {
                            const stock = batchSymbols.find(s => s.symbol === error.symbol);
                            const stockName = stock ? stock.name : error.symbol;
                            this.addLogMessage(`❌ ${error.symbol} (${stockName}) 失敗: ${error.error}`, 'error');
                        }
                    }

                    // 更新進度
                    const progress = 20 + (processedCount / symbols.length) * 70;
                    this.updateProgress(progress, `已處理 ${processedCount}/${symbols.length} 檔股票`);

                    const batchEndTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                    const batchElapsed = batchEndTime - batchStartTime;
                    const batchHuman = this.formatDuration(batchElapsed);
                    this.addLogMessage(`📦 批次 ${batchIndex + 1}/${totalBatches} 完成，耗時 ${batchHuman}，累計已處理 ${processedCount}/${symbols.length} 檔`, 'info');
                    
                    return { success: true, count: batchSymbols.length };
                } catch (error) {
                    const batchEndTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                    const batchElapsed = batchEndTime - batchStartTime;
                    const batchHuman = this.formatDuration(batchElapsed);
                    this.addLogMessage(`批次 ${batchIndex + 1} 處理失敗: ${error.message}（耗時 ${batchHuman}）`, 'error');
                    return { success: false, count: 0 };
                }
            };
            
            // 🚀 並發執行所有批次
            for (let i = 0; i < totalBatches; i += maxConcurrentBatches) {
                const batchPromises = [];
                for (let j = 0; j < maxConcurrentBatches && (i + j) < totalBatches; j++) {
                    batchPromises.push(processBatch(i + j));
                }
                
                // 等待當前這組批次完成
                await Promise.all(batchPromises);
                
                // 批次組間短暫延遲
                if (i + maxConcurrentBatches < totalBatches) {
                    await new Promise(resolve => setTimeout(resolve, interBatchDelayMs));
                }
            }
        }
            
            this.updateProgress(100, '更新完成');
            
            // 自動計算報酬率
            this.addLogMessage('', 'info'); // 空行分隔
            await this.computeReturnsAfterUpdate(symbols, startDate, endDate);
            
            // 顯示資料庫儲存總結
            this.addLogMessage('📊 正在統計資料庫儲存結果...', 'info');
            try {
                // 查詢資料庫中的總數據量
                const statsUrl = this.useLocalDb ? 'http://localhost:5003/api/health?use_local_db=true' : 'http://localhost:5003/api/health';
                const statsResponse = await fetch(statsUrl);
                if (statsResponse.ok) {
                    const statsData = await statsResponse.json();
                    
                    // 顯示完成訊息
                    this.addLogMessage('✅ 所有更新任務已完成！數據已成功儲存到資料庫', 'success');
                    
                    // 顯示資料庫連接資訊
                    if (statsData.database_connection) {
                        const dbConn = statsData.database_connection;
                        const isLocal = Boolean(dbConn.is_local);
                        const host = dbConn.host || (isLocal ? 'localhost' : 'remote');
                        const port = dbConn.port || (isLocal ? '5432' : 'n/a');
                        const database = dbConn.database || (isLocal ? 'postgres' : 'neon');
                        const user = dbConn.user || (isLocal ? 'postgres' : 'neon_user');
                        const label = isLocal ? '本地 PostgreSQL' : 'Neon 雲端';
                        this.addLogMessage(`🗄️ 資料庫連接 (${label}): ${user}@${host}:${port}/${database}`, 'info');
                    }
                    
                    // 顯示詳細的資料庫統計資訊
                    if (statsData.data_statistics) {
                        const priceStats = statsData.data_statistics.tw_stock_prices;
                        const returnStats = statsData.data_statistics.tw_stock_returns;
                        
                        // 股價數據統計
                        this.addLogMessage(`📈 股價數據統計: ${priceStats.total_records} 筆記錄，涵蓋 ${priceStats.unique_stocks} 檔股票`, 'info');
                        if (priceStats.date_range && priceStats.date_range.earliest && priceStats.date_range.latest) {
                            const startDate = new Date(priceStats.date_range.earliest).toLocaleDateString('zh-TW');
                            const endDate = new Date(priceStats.date_range.latest).toLocaleDateString('zh-TW');
                            this.addLogMessage(`📅 股價數據日期範圍: ${startDate} ~ ${endDate}`, 'info');
                        }
                        
                        // 報酬率數據統計
                        this.addLogMessage(`📊 報酬率數據統計: ${returnStats.total_records} 筆記錄，涵蓋 ${returnStats.unique_stocks} 檔股票`, 'info');
                        if (returnStats.date_range && returnStats.date_range.earliest && returnStats.date_range.latest) {
                            const startDate = new Date(returnStats.date_range.earliest).toLocaleDateString('zh-TW');
                            const endDate = new Date(returnStats.date_range.latest).toLocaleDateString('zh-TW');
                            this.addLogMessage(`📅 報酬率數據日期範圍: ${startDate} ~ ${endDate}`, 'info');
                        }
                        
                        // 顯示資料表資訊
                        this.addLogMessage(`🏷️ 資料表: tw_stock_prices (股價), tw_stock_returns (報酬率)`, 'info');
                    }
                    
                    this.addLogMessage('💾 您現在可以到「資料查詢」頁面查看已儲存的股票數據', 'info');
                } else {
                    this.addLogMessage('✅ 所有更新任務已完成', 'success');
                }
            } catch (error) {
                this.addLogMessage('✅ 所有更新任務已完成', 'success');
            }
            
        } catch (error) {
            this.addLogMessage(`更新失敗: ${error.message}`, 'error');
            this.updateProgress(0, '更新失敗');
        } finally {
            this.isUpdating = false;
            document.getElementById('executeUpdate').disabled = false;
            document.getElementById('cancelUpdate').disabled = true;
        }
    }

    async computeReturnsAfterUpdate(symbols, startDate, endDate) {
        try {
            this.addLogMessage('🧮 開始自動計算報酬率...', 'info');
            
            const symbolList = symbols.map(s => typeof s === 'string' ? s : s.symbol);
            
            const computePayload = {
                all: false,
                symbols: symbolList,
                start: startDate,
                end: endDate,
                fill_missing: true,
                use_local_db: this.useLocalDb
            };
            
            const response = await fetch('http://localhost:5003/api/returns/compute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(computePayload)
            });
            
            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }
            
            const result = await response.json();
            
            if (result.success) {
                const totalWritten = result.total_written || 0;
                this.addLogMessage(`✅ 報酬率計算完成: 共寫入 ${totalWritten} 筆記錄`, 'success');
                
                if (result.symbols && result.symbols.length > 0) {
                    const successSymbols = result.symbols.filter(s => s.written > 0);
                    this.addLogMessage(`📊 成功計算 ${successSymbols.length}/${result.symbols.length} 檔股票的報酬率`, 'info');
                    
                    successSymbols.slice(0, 5).forEach(s => {
                        this.addLogMessage(`  ✓ ${s.symbol}: ${s.written} 筆`, 'info');
                    });
                    
                    if (successSymbols.length > 5) {
                        this.addLogMessage(`  ... 其餘 ${successSymbols.length - 5} 檔省略`, 'info');
                    }
                }
            } else {
                this.addLogMessage(`⚠️ 報酬率計算失敗: ${result.error || '未知錯誤'}`, 'warning');
            }
        } catch (error) {
            this.addLogMessage(`❌ 報酬率計算失敗: ${error.message}`, 'error');
            console.error('報酬率計算錯誤:', error);
        }
    }


    cancelUpdate() {
        this.isUpdating = false;
        this.addLogMessage('用戶取消了更新操作', 'warning');
        this.updateProgress(0, '已取消');
        
        // 重置按鈕狀態
        const executeButton = document.getElementById('executeUpdate');
        const cancelButton = document.getElementById('cancelUpdate');
        
        if (executeButton) {
            executeButton.disabled = false;
            executeButton.textContent = '開始更新';
        }
        
        if (cancelButton) {
            cancelButton.disabled = true;
        }
        // 更新操作狀態
        this.updateActionStatus('ready', '準備就緒');
    }

    // 初始化切換選項
    initializeToggleOptions() {
        console.log('🔧 初始化切換選項...');
        
        // 綁定快速時間範圍選項
        const quickOptions = document.querySelectorAll('.quick-option[data-days]');
        console.log(`找到 ${quickOptions.length} 個快速時間選項`);
        
        quickOptions.forEach(option => {
            option.addEventListener('click', (e) => {
                const days = option.getAttribute('data-days');
                console.log(`點擊快速選項: ${days} 天`);
                
                // 移除其他選項的 active 類
                quickOptions.forEach(opt => opt.classList.remove('active'));
                // 添加當前選項的 active 類
                option.classList.add('active');
                
                // 取消自訂日期範圍的選擇
                const customToggle = document.querySelector('.custom-date-toggle');
                if (customToggle && customToggle.classList.contains('active')) {
                    customToggle.classList.remove('active');
                    // 隱藏自訂日期範圍輸入框
                    const customDateRange = document.querySelector('.custom-date-range');
                    if (customDateRange) {
                        customDateRange.style.display = 'none';
                    }
                    console.log('🔄 取消自訂日期範圍選擇');
                }
                
                // 設置股票數量限制
                const count = option.getAttribute('data-count');
                const limitInput = document.getElementById('stockLimit');
                if (limitInput && count) {
                    limitInput.value = count;
                }
                
                // 更新隱藏的輸入值
                const recentPeriodInput = document.getElementById('recentPeriod');
                if (recentPeriodInput) {
                    recentPeriodInput.value = days;
                    console.log(`設置 recentPeriod 值為: ${days}`);
                }
            });
        });
        
        // 綁定股票數量選項
        const stockCountOptions = document.querySelectorAll('.count-option[data-count]');
        console.log(`找到 ${stockCountOptions.length} 個股票數量選項`);
        
        stockCountOptions.forEach(option => {
            option.addEventListener('click', (e) => {
                const count = option.getAttribute('data-count');
                console.log(`點擊股票數量選項: ${count}`);
                
                // 移除其他選項的 active 類
                stockCountOptions.forEach(opt => opt.classList.remove('active'));
                // 添加當前選項的 active 類
                option.classList.add('active');
                
                // 取消進階選項的選擇（互斥）
                const advancedOptions = document.querySelectorAll('.advanced-option');
                advancedOptions.forEach(opt => opt.classList.remove('active'));
                console.log('📊 選擇股票數量選項，取消進階選項選擇');
                
                // 更新隱藏的輸入值
                const stockCountInput = document.getElementById('stockCount');
                if (stockCountInput) {
                    stockCountInput.value = count;
                    console.log(`設置 stockCount 值為: ${count}`);
                }
            });
        });
        
        // 綁定更新模式選項
        const updateModeOptions = document.querySelectorAll('.update-mode-option[data-mode]');
        console.log(`找到 ${updateModeOptions.length} 個更新模式選項`);
        
        updateModeOptions.forEach(option => {
            option.addEventListener('click', (e) => {
                const mode = option.getAttribute('data-mode');
                console.log(`點擊更新模式選項: ${mode}`);
                
                // 移除其他選項的 active 類
                updateModeOptions.forEach(opt => opt.classList.remove('active'));
                // 添加當前選項的 active 類
                option.classList.add('active');
                
                console.log(`設置更新模式為: ${mode}`);
            });
        });
        
        // 綁定內容選項切換
        const contentOptions = document.querySelectorAll('.content-option');
        console.log(`找到 ${contentOptions.length} 個內容選項`);
        
        contentOptions.forEach(option => {
            const toggle = option.querySelector('input[type="checkbox"]');
            if (toggle) {
                toggle.addEventListener('change', (e) => {
                    const content = option.getAttribute('data-content');
                    console.log(`切換內容選項 ${content}: ${e.target.checked}`);
                    
                    if (e.target.checked) {
                        option.classList.add('active');
                    } else {
                        option.classList.remove('active');
                    }
                });
                
                // 點擊整個選項區域也可以切換
                option.addEventListener('click', (e) => {
                    if (e.target !== toggle && !e.target.classList.contains('toggle-slider')) {
                        e.preventDefault();
                        toggle.checked = !toggle.checked;
                        toggle.dispatchEvent(new Event('change'));
                    }
                });
            }
        });
        
        // 處理自訂日期切換
        const customToggle = document.querySelector('.custom-toggle .toggle-btn');
        if (customToggle) {
            customToggle.addEventListener('click', () => {
                const panel = document.querySelector('.custom-date-panel');
                const arrow = customToggle.querySelector('.toggle-arrow');
                
                if (panel) {
                    panel.classList.toggle('active');
                    customToggle.classList.toggle('active');
                    
                    // 如果啟用自訂日期範圍，取消預設時間範圍選項的選擇
                    if (customToggle.classList.contains('active')) {
                        const quickOptions = document.querySelectorAll('.quick-option');
                        quickOptions.forEach(opt => opt.classList.remove('active'));
                        console.log('🔄 取消預設時間範圍選擇');
                    }
                    
                    if (arrow) {
                        arrow.style.transform = panel.classList.contains('active') ? 'rotate(180deg)' : 'rotate(0deg)';
                    }
                }
            });
        }
        
        // 處理進階選項切換
        const advancedToggle = document.querySelector('.advanced-toggle .toggle-btn');
        if (advancedToggle) {
            advancedToggle.addEventListener('click', () => {
                const panel = document.querySelector('.advanced-panel');
                const arrow = advancedToggle.querySelector('.toggle-arrow');
                
                if (panel) {
                    panel.classList.toggle('active');
                    advancedToggle.classList.toggle('active');
                    
                    if (arrow) {
                        arrow.style.transform = panel.classList.contains('active') ? 'rotate(180deg)' : 'rotate(0deg)';
                    }
                }
            });
        }
        
        // 頁面載入後預設展開「進階選項」面板，與點擊切換行為一致
        const advPanel = document.querySelector('.advanced-panel');
        if (advancedToggle && advPanel) {
            advPanel.classList.add('active');
            advancedToggle.classList.add('active');
            const advArrow = advancedToggle.querySelector('.toggle-arrow');
            if (advArrow) {
                advArrow.style.transform = 'rotate(180deg)';
            }
        }
        
        // 處理進階選項內的選擇
        const advancedOptions = document.querySelectorAll('.advanced-option');
        console.log(`🔧 找到 ${advancedOptions.length} 個進階選項`);
        
        advancedOptions.forEach((option, index) => {
            const optionType = option.getAttribute('data-type');
            console.log(`進階選項 ${index}: type="${optionType}"`);
            
            option.addEventListener('click', () => {
                console.log(`🖱️ 點選進階選項: ${optionType}`);
                
                // 移除所有活動狀態
                advancedOptions.forEach(opt => opt.classList.remove('active'));
                // 添加活動狀態到當前選項
                option.classList.add('active');
                console.log(`✅ 設置進階選項 "${optionType}" 為活躍狀態`);
                
                // 取消股票數量選項的選擇（互斥）
                const countOptions = document.querySelectorAll('.count-option');
                countOptions.forEach(opt => opt.classList.remove('active'));
                console.log('🔧 選擇進階選項，取消股票數量選項選擇');
                
                // 根據選項類型處理
                const rangeInputs = option.querySelector('.range-inputs');
                const indicesGrid = option.querySelector('.indices-grid');
                
                // 修正：效能參數也需要顯示其內部的輸入框（使用了相同的 range-inputs 類別）
                if ((optionType === 'range' || optionType === 'performance') && rangeInputs) {
                    rangeInputs.style.display = 'block';
                    console.log('📝 顯示範圍/效能輸入框');
                } else if (rangeInputs) {
                    rangeInputs.style.display = 'none';
                    console.log('📝 隱藏範圍/效能輸入框');
                }
                
                if (optionType === 'indices' && indicesGrid) {
                    indicesGrid.style.display = 'grid';
                    console.log('📊 顯示市場指數選項');
                } else if (indicesGrid) {
                    indicesGrid.style.display = 'none';
                    console.log('📊 隱藏市場指數選項');
                }
            });
        });
        
        // 初始化市場指數功能
        this.initializeMarketIndices();
    }
    
    // 獲取指數名稱的輔助方法
    getIndexName(symbol) {
        const indexNames = {
            '^TWII': '台灣加權指數',
            '0050.TW': '元大台灣50',
            '0056.TW': '元大高股息',
            '0051.TW': '元大中型100',
            '006208.TW': '富邦台50',
            '2330.TW': '台積電',
            '2317.TW': '鴻海'
        };
        return indexNames[symbol] || symbol;
    }
    
    // 初始化市場指數功能
    initializeMarketIndices() {
        console.log('📊 初始化市場指數功能...');
        
        // 全選按鈕
        const selectAllBtn = document.getElementById('selectAllIndices');
        if (selectAllBtn) {
            selectAllBtn.addEventListener('click', () => {
                const checkboxes = document.querySelectorAll('.index-checkbox');
                checkboxes.forEach(checkbox => {
                    checkbox.checked = true;
                });
                console.log('✅ 全選市場指數');
            });
        }
        
        // 清除按鈕
        const clearAllBtn = document.getElementById('clearAllIndices');
        if (clearAllBtn) {
            clearAllBtn.addEventListener('click', () => {
                const checkboxes = document.querySelectorAll('.index-checkbox');
                checkboxes.forEach(checkbox => {
                    checkbox.checked = false;
                });
                console.log('❌ 清除市場指數選擇');
            });
        }
        
        // 單個指數項目點擊
        const indexItems = document.querySelectorAll('.index-item');
        indexItems.forEach(item => {
            item.addEventListener('click', (e) => {
                // 如果點擊的是checkbox或label，讓默認行為處理
                if (e.target.classList.contains('index-checkbox') || 
                    e.target.classList.contains('index-label') ||
                    e.target.closest('.index-label')) {
                    return;
                }
                
                // 否則手動切換checkbox
                const checkbox = item.querySelector('.index-checkbox');
                if (checkbox) {
                    checkbox.checked = !checkbox.checked;
                    const symbol = item.dataset.symbol;
                    console.log(`📊 切換指數 ${symbol}: ${checkbox.checked ? '選中' : '取消'}`);
                }
            });
        });
    }
    
    // 初始化默認選項
    initializeDefaultOptions() {
        console.log('🔧 初始化默認選項...');
        
        // 設置默認選中的快速選項（30天）
        const allQuickOptions = document.querySelectorAll('.quick-option[data-days]');
        console.log(`找到 ${allQuickOptions.length} 個快速時間選項`);
        
        // 先清除所有選項的 active 狀態
        allQuickOptions.forEach(option => {
            option.classList.remove('active');
            console.log(`清除選項 ${option.getAttribute('data-days')} 天的 active 狀態`);
        });
        
        const defaultQuickOption = document.querySelector('.quick-option[data-days="30"]');
        if (defaultQuickOption) {
            defaultQuickOption.classList.add('active');
            console.log('✅ 設置默認快速選項: 30天');
            console.log('默認選項元素:', defaultQuickOption);
            console.log('默認選項 data-days:', defaultQuickOption.getAttribute('data-days'));
        } else {
            console.warn('⚠️ 未找到30天選項，嘗試選擇第一個可用選項');
            const firstQuickOption = document.querySelector('.quick-option[data-days]');
            if (firstQuickOption) {
                firstQuickOption.classList.add('active');
                console.log(`✅ 設置默認快速選項: ${firstQuickOption.getAttribute('data-days')}天`);
            } else {
                console.error('❌ 沒有找到任何快速時間選項');
            }
        }
        
        // 設置默認股票數量選項
        const allCountOptions = document.querySelectorAll('.count-option[data-count]');
        console.log(`找到 ${allCountOptions.length} 個股票數量選項`);
        
        // 先清除所有選項的 active 狀態
        allCountOptions.forEach(option => {
            option.classList.remove('active');
        });
        
        const defaultCountOption = document.querySelector('.count-option[data-count="50"]');
        if (defaultCountOption) {
            defaultCountOption.classList.add('active');
            console.log('✅ 設置默認股票數量選項: 50檔');
        }
    }

    // 初始化操作狀態
    initializeActionStatus() {
        this.updateActionStatus('ready', '準備就緒');
    }

    // 更新操作狀態
    updateActionStatus(status, text) {
        const actionStatus = document.getElementById('actionStatus');
        if (!actionStatus) return;
        
        const indicator = actionStatus.querySelector('.status-indicator');
        const statusText = actionStatus.querySelector('.status-text');
        
        // 移除所有狀態類
        indicator.classList.remove('ready', 'running', 'error');
        indicator.classList.add(status);
        
        if (statusText) {
            statusText.textContent = text;
        }
    }

    // 更新進度條
    updateProgress(percentage, message) {
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        const progressStatus = document.getElementById('progressStatus');
        
        if (progressFill) {
            progressFill.style.width = `${percentage}%`;
        }
        
        if (progressText) {
            progressText.textContent = `${percentage}%`;
        }
        
        if (progressStatus && message) {
            progressStatus.textContent = message;
        }
        
        console.log(`進度更新: ${percentage}% - ${message}`);
    }

    addLogMessage(message, type = 'info') {
        const logContainer = document.getElementById('logContent');
        if (!logContainer) {
            console.error('Log container not found');
            return;
        }

        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${type}`;
        logEntry.dataset.level = type;

        const timestamp = new Date().toLocaleString('zh-TW');
        logEntry.innerHTML = `
            <span class="log-time">[${timestamp}]</span>
            <span class="log-level">${type.toUpperCase()}:</span>
            <span class="log-message">${message}</span>
        `;

        logContainer.appendChild(logEntry);

        // 即時套用目前的等級篩選
        if (typeof this.applyLogFilter === 'function') {
            this.applyLogFilter();
        }

        // 依使用者設定自動捲動
        if (this.autoScrollLog) {
            logContainer.scrollTop = logContainer.scrollHeight;
        }
    }

    showMessage(message, type = 'info') {
        this.addLogMessage(message, type);
    }
    
    // 導出日誌為文字檔（可選自訂檔名）
    exportLog(customName) {
        const logContainer = document.getElementById('logContent');
        if (!logContainer) {
            console.warn('⚠️ 找不到日誌容器 #logContent');
            return;
        }
        // 將每個日誌項目的純文字匯出，保留時間與等級
        const lines = Array.from(logContainer.querySelectorAll('.log-entry')).map(entry => entry.textContent.trim());
        const text = lines.length > 0 ? lines.join('\n') : logContainer.textContent.trim();
        const blob = new Blob([text || '（目前沒有日誌內容）'], { type: 'text/plain;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        const ts = new Date().toISOString().replace(/[:.]/g, '-');
        const filename = customName ? `${customName}.txt` : `bdstock_logs_${ts}.txt`;
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        this.addLogMessage('已導出日誌檔案', 'success');
    }

    // 清除日誌內容
    clearLog() {
        const logContainer = document.getElementById('logContent');
        if (!logContainer) {
            console.warn('⚠️ 找不到日誌容器 #logContent');
            return;
        }
        logContainer.innerHTML = '';
        this.addLogMessage('日誌已清除', 'info');
    }

    // 股票數據查詢功能
    async queryPriceData() {
        try {
            const symbolInput = document.getElementById('tickerInput').value.trim();
            const startDate = document.getElementById('queryStartDate').value;
            const endDate = document.getElementById('queryEndDate').value;
            
            if (!symbolInput) {
                this.addLogMessage('請輸入股票代碼', 'warning');
                return;
            }
            
            // 支援多檔股票查詢
            const symbols = symbolInput.split(',').map(s => s.trim()).filter(s => s);
            
            if (symbols.length === 1) {
                // 單檔股票查詢
                await this.querySingleStockPrice(symbols[0], startDate, endDate);
            } else {
                // 多檔股票查詢
                await this.queryMultiStockPrice(symbols, startDate, endDate);
            }
            
        } catch (error) {
            this.addLogMessage(`查詢股價數據失敗: ${error.message}`, 'error');
        }
    }

    async querySingleStockPrice(symbol, startDate, endDate) {
        this.addLogMessage(`正在查詢 ${symbol} 的股價數據...`, 'info');
        
        const params = new URLSearchParams();
        if (startDate) params.append('start', startDate);
        if (endDate) params.append('end', endDate);

        const table = this.getSelectedQueryTable();
        if (table) params.append('table', table);
        
        if (this.useLocalDb) params.append('use_local_db', 'true');
        const response = await fetch(`http://localhost:5003/api/stock/${symbol}/prices?${params}`);
        
        if (!response.ok) {
            throw new Error(`查詢失敗: HTTP ${response.status}`);
        }
        
        const data = await response.json();
    if (typeof data.persisted_rows === 'number') {
        const label = this.useLocalDb ? '本地 PostgreSQL' : 'Neon 雲端';
        this.addLogMessage(`📝 入庫資訊（${label}）: persisted_rows=${data.persisted_rows}`, 'info');
    }
        
        if (data.success && data.data.length > 0) {
            this.addLogMessage(`✅ 查詢成功！找到 ${data.data.length} 筆 ${symbol} 的股價數據`, 'success');
            this.displayQueryResults(data.data, 'price');
        } else {
            this.addLogMessage(`❌ 未找到 ${symbol} 的股價數據`, 'warning');
            this.resetQueryResults();
        }
    }

    async queryMultiStockPrice(symbols, startDate, endDate) {
        this.addLogMessage(`正在查詢 ${symbols.length} 檔股票的股價數據...`, 'info');
        
        const allResults = [];
        let successCount = 0;
        
        for (const symbol of symbols) {
            try {
                this.addLogMessage(`📊 查詢 ${symbol}...`, 'info');
                
                const params = new URLSearchParams();
                if (startDate) params.append('start', startDate);
                if (endDate) params.append('end', endDate);
                
                const url = `http://localhost:5003/api/stock/${symbol}/prices?${params}`;
                const response = await fetch(this.useLocalDb ? `${url}&use_local_db=true` : url);
                
                if (response.ok) {
                    const data = await response.json();
                    if (data.success && data.data.length > 0) {
                        // 為每筆數據添加股票代碼
                        const dataWithSymbol = data.data.map(row => ({
                            ...row,
                            symbol: symbol
                        }));
                        allResults.push(...dataWithSymbol);
                        successCount++;
                        this.addLogMessage(`✅ ${symbol}: ${data.data.length} 筆數據`, 'success');
                    } else {
                        this.addLogMessage(`⚠️ ${symbol}: 無數據`, 'warning');
                    }
                } else {
                    this.addLogMessage(`❌ ${symbol}: 查詢失敗`, 'error');
                }
                
                // 添加小延遲避免過於頻繁的請求
                await new Promise(resolve => setTimeout(resolve, 100));
                
            } catch (error) {
                this.addLogMessage(`❌ ${symbol}: ${error.message}`, 'error');
            }
        }
        
        if (allResults.length > 0) {
            // 按日期和股票代碼排序
            allResults.sort((a, b) => {
                const dateCompare = new Date(b.date) - new Date(a.date);
                if (dateCompare !== 0) return dateCompare;
                return a.symbol.localeCompare(b.symbol);
            });
            
    }
}

async queryGenericTableData(tableName) {
    try {
        const symbolInput = document.getElementById('tickerInput')?.value?.trim() || '';
        const startDate = document.getElementById('queryStartDate')?.value || '';
        const endDate = document.getElementById('queryEndDate')?.value || '';

        this.lastQueryParams = {
            table: tableName,
            startDate,
            endDate,
            symbols: symbolInput,
        };

        const origin = (window && window.location && window.location.origin) ? window.location.origin : '';
        const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

        const qs = new URLSearchParams();
        qs.set('table', tableName);
        if (symbolInput) qs.set('symbol', symbolInput);
        if (startDate) qs.set('start', startDate);
        if (endDate) qs.set('end', endDate);
        qs.set('limit', '200');
        qs.set('offset', '0');
        if (this.useLocalDb) qs.set('use_local_db', 'true');

        const url = `${base}/api/query/table?${qs.toString()}`;
        this.addLogMessage(`📡 通用查詢：${url}`, 'info');

        const resp = await fetch(url);
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.success) {
            throw new Error(data.error || `HTTP ${resp.status}`);
        }

        const columns = Array.isArray(data.columns) ? data.columns : [];
        const rows = Array.isArray(data.rows) ? data.rows : [];
        this.displayGenericQueryResults(tableName, columns, rows);
    } catch (e) {
        this.addLogMessage(`通用查詢失敗: ${e.message}`, 'error');
        this.resetQueryResults();
    }
}

displayGenericQueryResults(tableName, columns, rows) {
    try {
        const resultsTable = document.getElementById('queryTable');
        if (!resultsTable) {
            this.addLogMessage('查詢結果表格未找到', 'error');
            return;
        }

        const resultsSubtitle = document.getElementById('resultsSubtitle');
        const recordCount = document.getElementById('recordCount');
        const dateRangeInfo = document.getElementById('dateRangeInfo');
        if (resultsSubtitle) {
            resultsSubtitle.textContent = `資料表 ${tableName}（動態查詢）`;
        }
        if (recordCount) {
            recordCount.textContent = (rows || []).length.toLocaleString();
        }
        if (dateRangeInfo) {
            dateRangeInfo.style.display = 'none';
        }

        const safeColumns = (columns || []).filter(c => typeof c === 'string' && c.trim());
        const headerCells = safeColumns.map(c => `<th><div class="th-content">${c}</div></th>`).join('');
        const bodyRows = (rows || []).map(r => {
            const cells = safeColumns.map(c => {
                let v = r ? r[c] : null;
                if (v === null || v === undefined) v = '';
                return `<td>${String(v)}</td>`;
            }).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        resultsTable.innerHTML = `
            <thead><tr>${headerCells}</tr></thead>
            <tbody>${bodyRows}</tbody>
        `;

        // 通用表格先不套用原本的排序/圖表（避免依賴固定欄位）
        this.initResultsViewToggle();

        this.addLogMessage(`✅ 已顯示 ${tableName}：${(rows || []).length} 筆`, 'success');
    } catch (e) {
        this.addLogMessage(`顯示通用查詢結果失敗: ${e.message}`, 'error');
    }
}

// 更新進度條
updateProgress(percentage, message) {
    const progressFill = document.getElementById('progressFill');
    const progressText = document.getElementById('progressText');
    const progressStatus = document.getElementById('progressStatus');

    if (progressFill) {
        progressFill.style.width = `${percentage}%`;
    }

    if (progressText) {
        progressText.textContent = `${percentage}%`;
    }

    if (progressStatus && message) {
        progressStatus.textContent = message;
    }

    console.log(`進度更新: ${percentage}% - ${message}`);
}

// ===== Summary Bar =====
initSummaryBar() {
    // Reset display
    this.updateSummaryDisplay({ total: 0, processed: 0, success: 0, failed: 0, elapsed: '00:00' });
}

updateSummaryDisplay({ total, processed, success, failed, elapsed }) {
    const elTotal = document.getElementById('summaryTotal');
    const elProcessed = document.getElementById('summaryProcessed');
    const elSuccess = document.getElementById('summarySuccess');
    const elFailed = document.getElementById('summaryFailed');
    const elElapsed = document.getElementById('summaryElapsed');
    if (elTotal) elTotal.textContent = total != null ? String(total) : '-';
    if (elProcessed) elProcessed.textContent = processed != null ? String(processed) : '-';
    if (elSuccess) elSuccess.textContent = success != null ? String(success) : '-';
    if (elFailed) elFailed.textContent = failed != null ? String(failed) : '-';
    if (elElapsed) elElapsed.textContent = elapsed || '00:00';
}

formatElapsed(ms) {
    const totalSeconds = Math.floor(ms / 1000);
    const m = String(Math.floor(totalSeconds / 60)).padStart(2, '0');
    const s = String(totalSeconds % 60).padStart(2, '0');
    return `${m}:${s}`;
}

startSummary(total) {
    this.summary = { total: total || 0, processed: 0, success: 0, failed: 0 };
    this.timerStart = Date.now();
    if (this.timerInterval) clearInterval(this.timerInterval);
    this.timerInterval = setInterval(() => {
        const elapsed = this.formatElapsed(Date.now() - this.timerStart);
        this.updateSummaryDisplay({ ...this.summary, elapsed });
    }, 500);
    this.updateSummaryDisplay({ ...this.summary, elapsed: '00:00' });
}

incrementSummary({ success }) {
    this.summary.processed += 1;
    if (success === true) this.summary.success += 1;
    if (success === false) this.summary.failed += 1;
    const elapsed = this.timerStart ? this.formatElapsed(Date.now() - this.timerStart) : '00:00';
    this.updateSummaryDisplay({ ...this.summary, elapsed });
}

finishSummary() {
    const elapsed = this.timerStart ? this.formatElapsed(Date.now() - this.timerStart) : '00:00';
    if (this.timerInterval) {
        clearInterval(this.timerInterval);
        this.timerInterval = null;
    }
    this.updateSummaryDisplay({ ...this.summary, elapsed });
}

// ===== API Health Polling =====
setApiHealthStatus(statusText, status) {
    const dot = document.getElementById('apiHealthDot');
    const text = document.getElementById('apiHealthText');
    if (text) text.textContent = statusText;
    if (dot) {
        const color = status === 'up' ? '#22c55e' : (status === 'unknown' ? '#999' : '#ef4444');
        dot.style.background = color;
    }
}

async pollApiHealthOnce() {
    try {
        const resp = await fetch('http://localhost:5003/api/test-connection');
        const data = await resp.json();
        if (data && data.success) {
            this.setApiHealthStatus('正常', 'up');
        } else {
            this.setApiHealthStatus('異常', 'down');
        }
    } catch (e) {
        this.setApiHealthStatus('無法連線', 'down');
    }
}

startApiHealthPolling() {
    // initial
    this.setApiHealthStatus('檢查中...', 'unknown');
    this.pollApiHealthOnce();
    // poll every 10s
    if (this.apiHealthTimer) clearInterval(this.apiHealthTimer);
    this.apiHealthTimer = setInterval(() => this.pollApiHealthOnce(), 10000);
}

// ===== Log Controls & Filtering =====
initLogControls() {
    // 等級篩選
    const levelSelect = document.getElementById('logLevelFilter');
    if (levelSelect) {
        levelSelect.value = this.currentLogFilter;
        levelSelect.addEventListener('change', () => {
            this.currentLogFilter = levelSelect.value || 'all';
            this.applyLogFilter();
        });
    }

    // 自動捲動
    const autoScrollChk = document.getElementById('autoScrollLog');
    if (autoScrollChk) {
        autoScrollChk.checked = this.autoScrollLog;
        autoScrollChk.addEventListener('change', () => {
            this.autoScrollLog = !!autoScrollChk.checked;
        });
    }
}

applyLogFilter() {
    const container = document.getElementById('logContent');
    if (!container) return;
    const entries = container.querySelectorAll('.log-entry');
    const filter = this.currentLogFilter || 'all';
    entries.forEach(el => {
        const level = el.dataset.level || 'info';
        el.style.display = (filter === 'all' || filter === level) ? '' : 'none';
    });
}

showMessage(message, type = 'info') {
    this.addLogMessage(message, type);
}

// 股票數據查詢功能
async queryPriceData() {
    try {
        const symbolInput = document.getElementById('tickerInput').value.trim();
        const startDate = document.getElementById('queryStartDate').value;
        const endDate = document.getElementById('queryEndDate').value;

        // 保存查詢參數以便在結果顯示時使用
        this.lastQueryParams = {
            startDate: startDate,
            endDate: endDate,
            symbols: symbolInput
        };

        if (!symbolInput) {
            this.addLogMessage('請輸入股票代碼', 'warning');
            return;
        }

        // 支援多檔股票查詢
        const symbols = symbolInput.split(',').map(s => s.trim()).filter(s => s);

        if (symbols.length === 1) {
            // 單檔股票查詢
            await this.querySingleStockPrice(symbols[0], startDate, endDate);
        } else {
            // 多檔股票查詢
            await this.queryMultiStockPrice(symbols, startDate, endDate);
        }

    } catch (error) {
        this.addLogMessage(`查詢股價數據失敗: ${error.message}`, 'error');
    }
}

// 報酬率數據查詢功能
async queryReturnData() {
    try {
        const symbolInput = document.getElementById('tickerInput').value.trim();
        const startDate = document.getElementById('queryStartDate').value;
        const endDate = document.getElementById('queryEndDate').value;
        
        // 保存查詢參數以便在結果顯示時使用
        this.lastQueryParams = {
            startDate: startDate,
            endDate: endDate,
            symbols: symbolInput
        };
        
        if (!symbolInput) {
            this.addLogMessage('請輸入股票代碼', 'warning');
            return;
        }
        
        // 支援多檔股票查詢
        const symbols = symbolInput.split(',').map(s => s.trim()).filter(s => s);
        
        if (symbols.length === 1) {
            // 單檔股票查詢
            await this.querySingleStockReturn(symbols[0], startDate, endDate);
        } else {
            // 多檔股票查詢
            await this.queryMultiStockReturn(symbols, startDate, endDate);
        }
        
    } catch (error) {
        this.addLogMessage(`查詢報酬率數據失敗: ${error.message}`, 'error');
    }
}

async querySingleStockReturn(symbol, startDate, endDate, frequency = 'daily') {
    this.addLogMessage(`正在查詢 ${symbol} 的${this.getFrequencyText(frequency)}報酬率數據...`, 'info');
    
    const params = new URLSearchParams();
    if (startDate) params.append('start', startDate);
    if (endDate) params.append('end', endDate);
    params.append('frequency', frequency);

    const table = this.getSelectedQueryTable();
    if (table) params.append('table', table);
    
    const response = await fetch(`http://localhost:5003/api/stock/${symbol}/returns?${params}`);
    
    if (!response.ok) {
        throw new Error(`查詢失敗: HTTP ${response.status}`);
    }
    
    const data = await response.json();
    
    if (data.success && data.data.length > 0) {
        // 顯示實際交易日範圍日誌
        if (data.data.length > 0) {
            const actualStart = data.data[data.data.length - 1].date;
            const actualEnd = data.data[0].date;
            this.addLogMessage(`📊 ${symbol} 實際交易日範圍: ${actualStart} ~ ${actualEnd}`, 'info');
        }
        
        this.addLogMessage(`✅ 查詢成功！找到 ${data.data.length} 筆 ${symbol} 的${this.getFrequencyText(frequency)}報酬率數據`, 'success');
        this.displayQueryResults(data.data, 'return', frequency);
    } else {
        this.addLogMessage(`❌ 未找到 ${symbol} 的報酬率數據`, 'warning');
        this.resetQueryResults();
    }
}

async queryMultiStockReturn(symbols, startDate, endDate, frequency = 'daily') {
    this.addLogMessage(`正在查詢 ${symbols.length} 檔股票的${this.getFrequencyText(frequency)}報酬率數據...`, 'info');
    
    const allResults = [];
    let successCount = 0;
    
    for (const symbol of symbols) {
        try {
            this.addLogMessage(`📊 查詢 ${symbol}...`, 'info');
            
            const params = new URLSearchParams();
            if (startDate) params.append('start', startDate);
            if (endDate) params.append('end', endDate);
            params.append('frequency', frequency);

            const table = this.getSelectedQueryTable();
            if (table) params.append('table', table);
            
            if (this.useLocalDb) params.append('use_local_db', 'true');
        const response = await fetch(`http://localhost:5003/api/stock/${symbol}/returns?${params}`);
            
            if (response.ok) {
                const data = await response.json();
                if (data.success && data.data.length > 0) {
                    // 為每筆數據添加股票代碼
                    const dataWithSymbol = data.data.map(row => ({
                        ...row,
                        symbol: symbol
                    }));
                    allResults.push(...dataWithSymbol);
                    successCount++;
                    
                    // 顯示實際交易日範圍日誌
                    if (data.data.length > 0) {
                        const actualStart = data.data[data.data.length - 1].date;
                        const actualEnd = data.data[0].date;
                        this.addLogMessage(`📊 ${symbol} 實際交易日範圍: ${actualStart} ~ ${actualEnd}`, 'info');
                    }
                    
                    this.addLogMessage(`✅ ${symbol}: ${data.data.length} 筆數據`, 'success');
                } else {
                    this.addLogMessage(`⚠️ ${symbol}: 無數據`, 'warning');
                }
            } else {
                this.addLogMessage(`❌ ${symbol}: 查詢失敗`, 'error');
            }
            
            // 添加小延遲避免過於頻繁的請求
            await new Promise(resolve => setTimeout(resolve, 100));
            
        } catch (error) {
            this.addLogMessage(`❌ ${symbol}: ${error.message}`, 'error');
        }
    }
    
    if (allResults.length > 0) {
        // 按日期和股票代碼排序
        allResults.sort((a, b) => {
            const dateCompare = new Date(b.date) - new Date(a.date);
            if (dateCompare !== 0) return dateCompare;
            return a.symbol.localeCompare(b.symbol);
        });
        
        this.addLogMessage(`✅ 多檔查詢完成！共找到 ${allResults.length} 筆報酬率數據 (成功: ${successCount}/${symbols.length})`, 'success');
        this.displayQueryResults(allResults, 'return-multi', frequency);
    } else {
        this.addLogMessage(`❌ 未找到任何報酬率數據`, 'warning');
        this.resetQueryResults();
    }
}

async querySingleStockPrice(symbol, startDate, endDate) {
    this.addLogMessage(`正在查詢 ${symbol} 的股價數據...`, 'info');

    const params = new URLSearchParams();
    if (startDate) params.append('start', startDate);
    if (endDate) params.append('end', endDate);

    const table = this.getSelectedQueryTable();
    if (table) params.append('table', table);

    if (this.useLocalDb) params.append('use_local_db', 'true');

    const response = await fetch(`http://localhost:5003/api/stock/${symbol}/prices?${params}`);

    if (!response.ok) {
        throw new Error(`查詢失敗: HTTP ${response.status}`);
    }

    const data = await response.json();

    if (data.success && data.data.length > 0) {
        this.addLogMessage(`✅ 查詢成功！找到 ${data.data.length} 筆 ${symbol} 的股價數據`, 'success');
        this.displayQueryResults(data.data, 'price');
    } else {
        this.addLogMessage(`❌ 未找到 ${symbol} 的股價數據`, 'warning');
        this.resetQueryResults();
    }
}

async queryMultiStockPrice(symbols, startDate, endDate) {
    this.addLogMessage(`正在查詢 ${symbols.length} 檔股票的股價數據...`, 'info');

    const allResults = [];
    let successCount = 0;

    for (const symbol of symbols) {
        try {
            this.addLogMessage(`📊 查詢 ${symbol}...`, 'info');

            const params = new URLSearchParams();
            if (startDate) params.append('start', startDate);
            if (endDate) params.append('end', endDate);

            const table = this.getSelectedQueryTable();
            if (table) params.append('table', table);

            if (this.useLocalDb) params.append('use_local_db', 'true');
            
            const response = await fetch(`http://localhost:5003/api/stock/${symbol}/prices?${params}`);
            
            if (response.ok) {
                const data = await response.json();
                if (data.success && data.data.length > 0) {
                    // 為每筆數據添加股票代碼
                    const dataWithSymbol = data.data.map(row => ({
                        ...row,
                        symbol: symbol
                    }));
                    allResults.push(...dataWithSymbol);
                    successCount++;
                    this.addLogMessage(`✅ ${symbol}: ${data.data.length} 筆數據`, 'success');
                } else {
                    this.addLogMessage(`⚠️ ${symbol}: 無數據`, 'warning');
                }
            } else {
                this.addLogMessage(`❌ ${symbol}: 查詢失敗`, 'error');
            }
            
            // 添加小延遲避免過於頻繁的請求
            await new Promise(resolve => setTimeout(resolve, 100));
            
        } catch (error) {
            this.addLogMessage(`❌ ${symbol}: ${error.message}`, 'error');
        }
    }
    
    if (allResults.length > 0) {
        // 按日期和股票代碼排序
        allResults.sort((a, b) => {
            const dateCompare = new Date(b.date) - new Date(a.date);
            if (dateCompare !== 0) return dateCompare;
            return a.symbol.localeCompare(b.symbol);
        });
        
        this.addLogMessage(`✅ 多檔查詢完成！共找到 ${allResults.length} 筆股價數據 (成功: ${successCount}/${symbols.length})`, 'success');
        this.displayQueryResults(allResults, 'price-multi');
    } else {
        this.addLogMessage(`❌ 未找到任何股價數據`, 'warning');
        this.resetQueryResults();
    }
}

// 新的統一查詢方法 - 適配新的 UI 設計
async executeQueryData() {
    try {
        const selectedTable = this.getSelectedQueryTable ? this.getSelectedQueryTable() : '';

        // 獲取查詢類型
        const queryTypeRadios = document.querySelectorAll('input[name="queryType"]');
        let queryType = 'price'; // 默認為股價
        for (const radio of queryTypeRadios) {
            if (radio.checked) {
                queryType = radio.value;
                break;
            }
        }

        console.log('執行查詢，類型:', queryType);

        // 若使用者有指定資料表，且不是股價/報酬率兩張預設表，改用通用查詢
        if (selectedTable && !['tw_stock_prices', 'tw_stock_returns'].includes(selectedTable)) {
            await this.queryGenericTableData(selectedTable);
            return;
        }

        // 根據查詢類型調用對應方法
        if (queryType === 'price') {
            await this.queryPriceData();
        } else if (queryType === 'return') {
            await this.queryReturnData();
        } else {
            this.addLogMessage('請選擇查詢類型', 'warning');
        }

    } catch (error) {
        this.addLogMessage(`執行查詢失敗: ${error.message}`, 'error');
        console.error('查詢執行錯誤:', error);
    }
}

// 清除查詢結果
clearQueryResults() {
    try {
        this.resetQueryResults();
        this.addLogMessage('已清除查詢結果', 'info');
    } catch (error) {
        this.addLogMessage(`清除結果失敗: ${error.message}`, 'error');
    }
}

// 初始化查詢類型選項交互
initQueryTypeOptions() {
    try {
        const queryOptions = document.querySelectorAll('.query-option');
            
            queryOptions.forEach(option => {
                option.addEventListener('click', (e) => {
                    // 如果點擊的是單選按鈕本身，不需要處理
                    if (e.target.type === 'radio') return;
                    
                    // 移除所有選項的 active 類
                    queryOptions.forEach(opt => opt.classList.remove('active'));
                    
                    // 為當前選項添加 active 類
                    option.classList.add('active');
                    
                    // 選中對應的單選按鈕
                    const radio = option.querySelector('input[type="radio"]');
                    if (radio) {
                        radio.checked = true;
                        console.log('查詢類型已切換至:', radio.value);
                    }
                });
            });

            // 為單選按鈕添加 change 事件
            const radioButtons = document.querySelectorAll('input[name="queryType"]');
            radioButtons.forEach(radio => {
                radio.addEventListener('change', (e) => {
                    if (e.target.checked) {
                        // 移除所有選項的 active 類
                        queryOptions.forEach(opt => opt.classList.remove('active'));
                        
                        // 為對應選項添加 active 類
                        const targetOption = document.querySelector(`.query-option[data-type="${e.target.value}"]`);
                        if (targetOption) {
                            targetOption.classList.add('active');
                        }
                        
                        console.log('查詢類型已變更為:', e.target.value);
                    }
                });
            });

            console.log('查詢類型選項交互已初始化');
            
        } catch (error) {
            console.error('初始化查詢類型選項失敗:', error);
        }
    }

    getFrequencyText(frequency) {
        const frequencyMap = {
            'daily': '日',
            'weekly': '週',
            'monthly': '月',
            'quarterly': '季',
            'yearly': '年'
        };
        return frequencyMap[frequency] || '日';
    }

    displayQueryResults(data, type, frequency = 'daily') {
        try {
            const resultsTable = document.getElementById('queryTable');
            if (!resultsTable) {
                this.addLogMessage('查詢結果表格未找到', 'error');
                return;
            }

            // 更新結果標題和統計
            this.updateResultsHeader(data, type, frequency);
            
            let headerHtml = '';
            let bodyHtml = '';
            
            if (type === 'price' || type === 'price-multi') {
                headerHtml = this.generatePriceTableHeader(type);
                bodyHtml = this.generatePriceTableBody(data, type);
            } else if (type === 'return') {
                headerHtml = this.generateReturnTableHeader(frequency);
                bodyHtml = this.generateReturnTableBody(data);
            }
            
            // 更新表格內容
            resultsTable.innerHTML = `
                <thead>
                    ${headerHtml}
                </thead>
                <tbody>
                    ${bodyHtml}
                </tbody>
            `;
            
            // 初始化視圖切換
            this.initResultsViewToggle();
            
            // 初始化表格排序功能
            this.initTableSorting(data, type);
            
            // 初始化圖表功能
            this.initChart(data, type, frequency);
            
            this.addLogMessage(`✅ 查詢結果已顯示，共 ${data.length} 筆記錄`, 'success');
            
        } catch (error) {
            this.addLogMessage(`顯示查詢結果失敗: ${error.message}`, 'error');
            console.error('顯示查詢結果錯誤:', error);
        }
    }

    // 更新結果標題區域
    updateResultsHeader(data, type, frequency) {
        const resultsSubtitle = document.getElementById('resultsSubtitle');
        const recordCount = document.getElementById('recordCount');
        const dateRangeInfo = document.getElementById('dateRangeInfo');
        
        if (resultsSubtitle) {
            const typeText = type === 'price' ? '股價數據' : `${this.getFrequencyText(frequency)}報酬率數據`;
            const timeRange = data.length > 0 ? `${data[data.length - 1].date} ~ ${data[0].date}` : '';
            resultsSubtitle.textContent = `${typeText} ${timeRange}`;
        }
        
        if (recordCount) {
            recordCount.textContent = data.length.toLocaleString();
        }
        
        // 顯示日期範圍資訊（請求範圍與實際交易日範圍）
        console.log('updateResultsHeader - dateRangeInfo:', dateRangeInfo);
        console.log('updateResultsHeader - lastQueryParams:', this.lastQueryParams);
        console.log('updateResultsHeader - data length:', data.length);
        
        if (dateRangeInfo && this.lastQueryParams) {
            const requestedStart = this.lastQueryParams.startDate || '未設定';
            const requestedEnd = this.lastQueryParams.endDate || '未設定';
            const actualStart = data.length > 0 ? data[data.length - 1].date : '無數據';
            const actualEnd = data.length > 0 ? data[0].date : '無數據';
            const tradingDaysCount = data.length;
            
            console.log('Date range info:', {
                requestedStart, requestedEnd, actualStart, actualEnd, tradingDaysCount
            });
            
            dateRangeInfo.innerHTML = `
                <div class="date-range-details">
                    <div class="date-range-item">
                        <span class="date-range-label">請求日期範圍:</span>
                        <span class="date-range-value">${requestedStart} ~ ${requestedEnd}</span>
                    </div>
                    <div class="date-range-item">
                        <span class="date-range-label">實際交易日範圍:</span>
                        <span class="date-range-value">${actualStart} ~ ${actualEnd}</span>
                        <span class="trading-days-count">(共 ${tradingDaysCount} 個交易日)</span>
                    </div>
                </div>
            `;
            dateRangeInfo.style.display = 'block';
            
            this.addLogMessage(`📅 日期範圍對比 - 請求: ${requestedStart} ~ ${requestedEnd}, 實際: ${actualStart} ~ ${actualEnd}`, 'info');
        } else {
            console.log('Date range info not displayed - missing element or params');
            if (!dateRangeInfo) console.log('dateRangeInfo element not found');
            if (!this.lastQueryParams) console.log('lastQueryParams not set');
        }
    }

    // 生成股價表格標題
    generatePriceTableHeader(type) {
        return `
            <tr>
                ${type === 'price-multi' ? '<th class="sortable" data-sort="symbol"><div class="th-content"><i class="fas fa-tag"></i> 股票代碼 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>' : ''}
                <th class="sortable" data-sort="date"><div class="th-content"><i class="fas fa-calendar"></i> 日期 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="open_price"><div class="th-content"><i class="fas fa-arrow-up"></i> 開盤價 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="high_price"><div class="th-content"><i class="fas fa-arrow-up text-success"></i> 最高價 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="low_price"><div class="th-content"><i class="fas fa-arrow-down text-danger"></i> 最低價 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="close_price"><div class="th-content"><i class="fas fa-chart-line"></i> 收盤價 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="volume"><div class="th-content"><i class="fas fa-chart-bar"></i> 成交量 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
            </tr>
        `;
    }

    // 生成股價表格內容
    generatePriceTableBody(data, type) {
        return data.map(row => {
            const openPrice = this.formatPrice(row.open_price);
            const highPrice = this.formatPrice(row.high_price);
            const lowPrice = this.formatPrice(row.low_price);
            const closePrice = this.formatPrice(row.close_price);
            const volume = this.formatVolume(row.volume);
            
            // 計算漲跌
            const priceChange = row.open_price && row.close_price ? 
                (row.close_price - row.open_price) : null;
            const changeClass = priceChange > 0 ? 'positive' : priceChange < 0 ? 'negative' : '';
            
            return `
                <tr>
                    ${type === 'price-multi' ? `<td class="symbol">${row.symbol}</td>` : ''}
                    <td>${this.formatDate(row.date)}</td>
                    <td class="number">${openPrice}</td>
                    <td class="number positive">${highPrice}</td>
                    <td class="number negative">${lowPrice}</td>
                    <td class="number ${changeClass}">${closePrice}</td>
                    <td class="number">${volume}</td>
                </tr>
            `;
        }).join('');
    }

    // 生成報酬率表格標題
    generateReturnTableHeader(frequency) {
        const frequencyText = this.getFrequencyText(frequency);
        return `
            <tr>
                <th class="sortable" data-sort="date"><div class="th-content"><i class="fas fa-calendar"></i> 日期 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="daily_return"><div class="th-content"><i class="fas fa-percentage"></i> ${frequencyText}報酬率 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
                <th class="sortable" data-sort="cumulative_return"><div class="th-content"><i class="fas fa-chart-line"></i> 累積報酬率 <span class="sort-indicator"><i class="fas fa-sort"></i></span></div></th>
            </tr>
        `;
    }

    // 生成報酬率表格內容
    generateReturnTableBody(data) {
        return data.map(row => {
            const dailyReturn = row.daily_return ? (row.daily_return * 100) : null;
            const cumulativeReturn = row.cumulative_return ? (row.cumulative_return * 100) : null;
            
            const dailyClass = dailyReturn > 0 ? 'positive' : dailyReturn < 0 ? 'negative' : '';
            const cumulativeClass = cumulativeReturn > 0 ? 'positive' : cumulativeReturn < 0 ? 'negative' : '';
            
            return `
                <tr>
                    <td>${this.formatDate(row.date)}</td>
                    <td class="number ${dailyClass}">${this.formatPercentage(dailyReturn)}</td>
                    <td class="number ${cumulativeClass}">${this.formatPercentage(cumulativeReturn)}</td>
                </tr>
            `;
        }).join('');
    }

    // 格式化價格
    formatPrice(price) {
        if (price === null || price === undefined) return '<span class="text-muted">N/A</span>';
        return price.toFixed(2);
    }

    // 格式化成交量
    formatVolume(volume) {
        if (!volume) return '<span class="text-muted">N/A</span>';
        if (volume >= 1000000) {
            return `${(volume / 1000000).toFixed(1)}M`;
        } else if (volume >= 1000) {
            return `${(volume / 1000).toFixed(1)}K`;
        }
        return volume.toLocaleString();
    }

    // 格式化百分比
    formatPercentage(value) {
        if (value === null || value === undefined) return '<span class="text-muted">N/A</span>';
        const sign = value > 0 ? '+' : '';
        return `${sign}${value.toFixed(4)}%`;
    }

    // 一般數值格式化（用於百分比欄位等，不加 % 號）
    formatNumber(value, digits = 2) {
        if (value === null || value === undefined || value === '' || Number.isNaN(Number(value))) {
            return '';
        }
        const num = Number(value);
        if (!Number.isFinite(num)) {
            return String(value);
        }
        return num.toFixed(digits);
    }

    // 格式化日期
    formatDate(dateString) {
        const date = new Date(dateString);
        if (Number.isNaN(date.getTime())) return dateString;
        return date.toLocaleDateString('zh-TW', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });
    }

    formatInteger(value) {
        if (value === null || value === undefined) return '0';
        const num = Number(value);
        if (Number.isNaN(num)) return String(value);
        return num.toLocaleString();
    }

    setTextContent(elementId, value) {
        const el = document.getElementById(elementId);
        if (el) el.textContent = value;
    }

    // 初始化結果視圖切換
    initResultsViewToggle() {
        const toggleBtns = document.querySelectorAll('.toggle-btn');
        const tableView = document.getElementById('tableView');
        const chartView = document.getElementById('chartView');
        
        toggleBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const viewType = btn.dataset.view;
                
                // 更新按鈕狀態
                toggleBtns.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                
                // 切換視圖
                if (viewType === 'table') {
                    tableView.classList.remove('hidden');
                    chartView.classList.add('hidden');
                } else if (viewType === 'chart') {
                    tableView.classList.add('hidden');
                    chartView.classList.remove('hidden');
                }
            });
        });
    }

    // 初始化表格排序功能
    initTableSorting(data, type) {
        this.currentData = data;
        this.currentType = type;
        this.sortState = {
            column: null,
            direction: 'asc' // 'asc' 或 'desc'
        };

        const sortableHeaders = document.querySelectorAll('.sortable');
        
        sortableHeaders.forEach(header => {
            header.addEventListener('click', () => {
                const sortColumn = header.dataset.sort;
                this.sortTable(sortColumn);
            });
            
            // 添加懸停效果
            header.style.cursor = 'pointer';
        });
    }

    // 排序表格
    sortTable(column) {
        try {
            // 更新排序狀態
            if (this.sortState.column === column) {
                this.sortState.direction = this.sortState.direction === 'asc' ? 'desc' : 'asc';
            } else {
                this.sortState.column = column;
                this.sortState.direction = 'asc';
            }

            // 排序數據
            const sortedData = [...this.currentData].sort((a, b) => {
                return this.compareValues(a[column], b[column], this.sortState.direction);
            });

            // 更新排序指示器
            this.updateSortIndicators(column, this.sortState.direction);

            // 重新渲染表格內容
            this.renderSortedTable(sortedData);

            this.addLogMessage(`📊 已按 ${this.getColumnDisplayName(column)} ${this.sortState.direction === 'asc' ? '升序' : '降序'} 排序`, 'info');

        } catch (error) {
            this.addLogMessage(`排序失敗: ${error.message}`, 'error');
            console.error('表格排序錯誤:', error);
        }
    }

    // 比較兩個值
    compareValues(a, b, direction) {
        // 處理 null/undefined 值
        if (a === null || a === undefined) a = '';
        if (b === null || b === undefined) b = '';

        // 數字比較
        if (typeof a === 'number' && typeof b === 'number') {
            return direction === 'asc' ? a - b : b - a;
        }

        // 日期比較
        if (this.isDateString(a) && this.isDateString(b)) {
            const dateA = new Date(a);
            const dateB = new Date(b);
            return direction === 'asc' ? dateA - dateB : dateB - dateA;
        }

        // 字符串比較
        const strA = String(a).toLowerCase();
        const strB = String(b).toLowerCase();
        
        if (direction === 'asc') {
            return strA.localeCompare(strB, 'zh-TW');
        } else {
            return strB.localeCompare(strA, 'zh-TW');
        }
    }

    // 檢查是否為日期字符串
    isDateString(value) {
        return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value);
    }

    // 更新排序指示器
    updateSortIndicators(activeColumn, direction) {
        const sortableHeaders = document.querySelectorAll('.sortable');
        
        sortableHeaders.forEach(header => {
            const indicator = header.querySelector('.sort-indicator i');
            const column = header.dataset.sort;
            
            if (column === activeColumn) {
                // 活躍列的指示器
                header.classList.add('sorted');
                if (direction === 'asc') {
                    indicator.className = 'fas fa-sort-up';
                    header.classList.add('sort-asc');
                    header.classList.remove('sort-desc');
                } else {
                    indicator.className = 'fas fa-sort-down';
                    header.classList.add('sort-desc');
                    header.classList.remove('sort-asc');
                }
            } else {
                // 非活躍列的指示器
                header.classList.remove('sorted', 'sort-asc', 'sort-desc');
                indicator.className = 'fas fa-sort';
            }
        });
    }

    // 重新渲染排序後的表格
    renderSortedTable(sortedData) {
        const resultsTable = document.getElementById('queryTable');
        const tbody = resultsTable.querySelector('tbody');
        
        let bodyHtml = '';
        
        if (this.currentType === 'price' || this.currentType === 'price-multi') {
            bodyHtml = this.generatePriceTableBody(sortedData, this.currentType);
        } else if (this.currentType === 'return') {
            bodyHtml = this.generateReturnTableBody(sortedData);
        }
        
        tbody.innerHTML = bodyHtml;
    }

    // 獲取列的顯示名稱
    getColumnDisplayName(column) {
        const columnNames = {
            'symbol': '股票代碼',
            'date': '日期',
            'open_price': '開盤價',
            'high_price': '最高價',
            'low_price': '最低價',
            'close_price': '收盤價',
            'volume': '成交量',
            'daily_return': '報酬率',
            'cumulative_return': '累積報酬率'
        };
        return columnNames[column] || column;
    }

    // 初始化圖表功能
    initChart(data, type, frequency) {
        // 先銷毀現有圖表
        if (this.currentChart) {
            try {
                this.currentChart.destroy();
                this.currentChart = null;
            } catch (error) {
                console.warn('銷毀現有圖表時出現警告:', error);
            }
        }

        this.chartData = data;
        this.chartType = type;
        this.chartFrequency = frequency;
        this.currentChartType = 'line';

        // 隱藏 Lightweight Charts 容器，顯示 canvas
        const lwContainer = document.getElementById('lightweightChart');
        if (lwContainer) {
            lwContainer.style.display = 'none';
        }
        const canvas = document.getElementById('dataChart');
        canvas.style.display = 'block';

        // 確保 canvas 清潔
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);

        const labels = this.chartData.map(item => this.formatDate(item.date));
        
        // 準備 OHLC 數據
        const ohlcData = this.chartData.map((item, index) => {
            const isUp = item.close_price >= item.open_price;
            return {
                x: index,
                open: item.open_price,
                high: item.high_price,
                low: item.low_price,
                close: item.close_price,
                isUp: isUp,
                date: item.date
            };
        });

        // 創建四個數據集來表示 OHLC
        const datasets = [
            {
                label: '開盤價',
                data: ohlcData.map(item => item.open),
                borderColor: '#ffa500',
                backgroundColor: 'rgba(255, 165, 0, 0.3)',
                borderWidth: 2,
                fill: false,
                pointStyle: 'line',
                pointRadius: 4,
                pointHoverRadius: 6,
                tension: 0
            },
            {
                label: '最高價',
                data: ohlcData.map(item => item.high),
                borderColor: '#00ff88',
                backgroundColor: 'rgba(0, 255, 136, 0.3)',
                borderWidth: 2,
                fill: false,
                pointStyle: 'triangle',
                pointRadius: 3,
                pointHoverRadius: 5,
                tension: 0
            },
            {
                label: '最低價',
                data: ohlcData.map(item => item.low),
                borderColor: '#ff4757',
                backgroundColor: 'rgba(255, 71, 87, 0.3)',
                borderWidth: 2,
                fill: false,
                pointStyle: 'rectRot',
                pointRadius: 3,
                pointHoverRadius: 5,
                tension: 0
            },
            {
                label: '收盤價',
                data: ohlcData.map(item => item.close),
                borderColor: '#00d4ff',
                backgroundColor: 'rgba(0, 212, 255, 0.3)',
                borderWidth: 3,
                fill: false,
                pointStyle: 'circle',
                pointRadius: 4,
                pointHoverRadius: 6,
                tension: 0
            }
        ];

        this.currentChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: 'K線圖 (OHLC)',
                        color: '#ffffff',
                        font: {
                            size: 16,
                            weight: 'bold'
                        },
                        padding: 20
                    },
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            color: '#ffffff',
                            font: {
                                size: 12
                            },
                            padding: 20,
                            usePointStyle: true
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(26, 31, 46, 0.9)',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#00d4ff',
                        borderWidth: 1,
                        cornerRadius: 8,
                        displayColors: true,
                        callbacks: {
                            afterBody: function(context) {
                                if (context.length > 0) {
                                    const dataIndex = context[0].dataIndex;
                                    const ohlc = ohlcData[dataIndex];
                                    const change = ohlc.close - ohlc.open;
                                    const changePercent = ((change / ohlc.open) * 100);
                                    
                                    return [
                                        '',
                                        `📊 OHLC 詳細資訊:`,
                                        `開盤: ${ohlc.open.toFixed(2)}`,
                                        `最高: ${ohlc.high.toFixed(2)}`,
                                        `最低: ${ohlc.low.toFixed(2)}`,
                                        `收盤: ${ohlc.close.toFixed(2)}`,
                                        `漲跌: ${change >= 0 ? '+' : ''}${change.toFixed(2)} (${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(2)}%)`,
                                        `振幅: ${((ohlc.high - ohlc.low) / ohlc.open * 100).toFixed(2)}%`,
                                        `趨勢: ${ohlc.isUp ? '📈 上漲' : '📉 下跌'}`
                                    ];
                                }
                                return [];
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        display: true,
                        title: {
                            display: true,
                            text: '日期',
                            color: '#ffffff',
                            font: {
                                size: 12,
                                weight: 'bold'
                            }
                        },
                        ticks: {
                            color: '#ffffff',
                            maxTicksLimit: 10
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)',
                            borderColor: 'rgba(255, 255, 255, 0.3)'
                        }
                    },
                    y: {
                        display: true,
                        title: {
                            display: true,
                            text: '價格 (OHLC)',
                            color: '#ffffff',
                            font: {
                                size: 12,
                                weight: 'bold'
                            }
                        },
                        ticks: {
                            color: '#ffffff',
                            callback: function(value) {
                                return value.toFixed(2);
                            }
                        },
                        grid: {
                            color: 'rgba(255, 255, 255, 0.1)',
                            borderColor: 'rgba(255, 255, 255, 0.3)'
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                elements: {
                    point: {
                        hoverRadius: 8
                    }
                }
            }
        });
    }

    // 創建普通圖表 (使用 Chart.js)
    createRegularChart() {
        const canvas = document.getElementById('dataChart');
        if (!canvas) {
            console.error('圖表 canvas 元素未找到');
            return;
        }

        // 隱藏 Lightweight Charts 容器，顯示 canvas
        const lwContainer = document.getElementById('lightweightChart');
        if (lwContainer) {
            lwContainer.style.display = 'none';
        }
        canvas.style.display = 'block';

        // 確保 canvas 清潔
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        
        if (this.chartType === 'price' || this.chartType === 'price-multi') {
            this.currentChart = this.createPriceChart(ctx);
        } else if (this.chartType === 'return') {
            this.currentChart = this.createReturnChart(ctx);
        }
    }

    // 創建股價圖表
    createPriceChart(ctx) {
        const labels = this.chartData.map(item => this.formatDate(item.date));
        
        let datasets = [];
        
        if (this.currentChartType === 'line') {
            datasets = [
                {
                    label: '收盤價',
                    data: this.chartData.map(item => item.close_price),
                    borderColor: '#00d4ff',
                    backgroundColor: 'rgba(0, 212, 255, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: '#00d4ff',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 6
                }
            ];
        } else if (this.currentChartType === 'bar') {
            datasets = [
                {
                    label: '成交量',
                    data: this.chartData.map(item => item.volume),
                    backgroundColor: 'rgba(0, 212, 255, 0.6)',
                    borderColor: '#00d4ff',
                    borderWidth: 1
                }
            ];
        }

        return new Chart(ctx, {
            type: this.currentChartType === 'bar' ? 'bar' : 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: this.getChartOptions('股價走勢圖')
        });
    }

    // 創建報酬率圖表
    createReturnChart(ctx) {
        const labels = this.chartData.map(item => this.formatDate(item.date));
        const frequencyText = this.getFrequencyText(this.chartFrequency);
        
        let datasets = [];
        
        if (this.currentChartType === 'line') {
            datasets = [
                {
                    label: `${frequencyText}報酬率`,
                    data: this.chartData.map(item => item.daily_return ? item.daily_return * 100 : null),
                    borderColor: '#00d4ff',
                    backgroundColor: 'rgba(0, 212, 255, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointBackgroundColor: '#00d4ff',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 6
                },
                {
                    label: '累積報酬率',
                    data: this.chartData.map(item => item.cumulative_return ? item.cumulative_return * 100 : null),
                    borderColor: '#00ff88',
                    backgroundColor: 'rgba(0, 255, 136, 0.1)',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.4,
                    pointBackgroundColor: '#00ff88',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 6
                }
            ];
        } else if (this.currentChartType === 'bar') {
            datasets = [
                {
                    label: `${frequencyText}報酬率`,
                    data: this.chartData.map(item => item.daily_return ? item.daily_return * 100 : null),
                    backgroundColor: this.chartData.map(item => {
                        const value = item.daily_return ? item.daily_return * 100 : 0;
                        return value >= 0 ? 'rgba(0, 255, 136, 0.6)' : 'rgba(255, 71, 87, 0.6)';
                    }),
                    borderColor: this.chartData.map(item => {
                        const value = item.daily_return ? item.daily_return * 100 : 0;
                        return value >= 0 ? '#00ff88' : '#ff4757';
                    }),
                    borderWidth: 1
                }
            ];
        }

        return new Chart(ctx, {
            type: this.currentChartType === 'bar' ? 'bar' : 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: this.getChartOptions(`${frequencyText}報酬率走勢圖`)
        });
    }

    // 獲取圖表配置選項
    getChartOptions(title) {
        const isCandlestick = this.currentChartType === 'candlestick';
        
        const baseOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: title,
                    color: '#ffffff',
                    font: {
                        size: 16,
                        weight: 'bold'
                    },
                    padding: 20
                },
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        color: '#ffffff',
                        font: {
                            size: 12
                        },
                        padding: 20,
                        usePointStyle: true
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(26, 31, 46, 0.9)',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: '#00d4ff',
                    borderWidth: 1,
                    cornerRadius: 8,
                    displayColors: true,
                    callbacks: {
                        label: function(context) {
                            if (isCandlestick && context.parsed.o !== undefined) {
                                // K 線圖的特殊工具提示
                                const data = context.parsed;
                                const change = data.c - data.o;
                                const changePercent = ((change / data.o) * 100);
                                
                                return [
                                    `開盤: ${data.o.toFixed(2)}`,
                                    `最高: ${data.h.toFixed(2)}`,
                                    `最低: ${data.l.toFixed(2)}`,
                                    `收盤: ${data.c.toFixed(2)}`,
                                    `漲跌: ${change >= 0 ? '+' : ''}${change.toFixed(2)} (${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(2)}%)`,
                                    `振幅: ${((data.h - data.l) / data.o * 100).toFixed(2)}%`
                                ];
                            } else {
                                // 普通圖表的工具提示
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    if (context.dataset.label.includes('報酬率')) {
                                        label += context.parsed.y.toFixed(4) + '%';
                                    } else if (context.dataset.label === '成交量') {
                                        label += context.parsed.y.toLocaleString();
                                    } else {
                                        label += context.parsed.y.toFixed(2);
                                    }
                                }
                                return label;
                            }
                        }
                    }
                }
            },
            interaction: {
                intersect: false,
                mode: 'index'
            },
            elements: {
                point: {
                    hoverRadius: 8
                }
            }
        };

        // 為蠟燭圖配置特殊的軸設置
        if (isCandlestick) {
            baseOptions.scales = {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day',
                        displayFormats: {
                            day: 'MM/dd'
                        }
                    },
                    title: {
                        display: true,
                        text: '日期',
                        color: '#ffffff',
                        font: {
                            size: 12,
                            weight: 'bold'
                        }
                    },
                    ticks: {
                        color: '#ffffff',
                        maxTicksLimit: 10
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)',
                        borderColor: 'rgba(255, 255, 255, 0.3)'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: '價格',
                        color: '#ffffff',
                        font: {
                            size: 12,
                            weight: 'bold'
                        }
                    },
                    ticks: {
                        color: '#ffffff',
                        callback: function(value) {
                            return value.toFixed(2);
                        }
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)',
                        borderColor: 'rgba(255, 255, 255, 0.3)'
                    }
                }
            };
        } else {
            // 普通圖表的軸設置
            baseOptions.scales = {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: '日期',
                        color: '#ffffff',
                        font: {
                            size: 12,
                            weight: 'bold'
                        }
                    },
                    ticks: {
                        color: '#ffffff',
                        maxTicksLimit: 10
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)',
                        borderColor: 'rgba(255, 255, 255, 0.3)'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: this.getYAxisLabel(),
                        color: '#ffffff',
                        font: {
                            size: 12,
                            weight: 'bold'
                        }
                    },
                    ticks: {
                        color: '#ffffff',
                        callback: function(value) {
                            if (this.chart.data.datasets[0].label.includes('報酬率')) {
                                return value.toFixed(2) + '%';
                            } else if (this.chart.data.datasets[0].label === '成交量') {
                                return value.toLocaleString();
                            } else {
                                return value.toFixed(2);
                            }
                        }
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)',
                        borderColor: 'rgba(255, 255, 255, 0.3)'
                    }
                }
            };
        }

        return baseOptions;
    }

    // 獲取 Y 軸標籤
    getYAxisLabel() {
        if (this.chartType === 'return') {
            return '報酬率 (%)';
        } else if (this.currentChartType === 'bar' && this.chartType === 'price') {
            return '成交量';
        } else {
            return '價格';
        }
    }

    // 獲取圖表類型名稱
    getChartTypeName(chartType) {
        const names = {
            'line': '線圖',
            'bar': '柱狀圖',
            'candlestick': 'K線圖'
        };
        return names[chartType] || chartType;
    }

    displayQueryStatistics(data, type) {
        if (!data || data.length === 0) return;
        
        let statsHtml = '<div class="query-stats">';
        
        if (type === 'price') {
            const prices = data.map(d => d.close_price).filter(p => p !== null && p !== undefined);
            const volumes = data.map(d => d.volume).filter(v => v !== null && v !== undefined);
            
            if (prices.length > 0) {
                const avgPrice = (prices.reduce((a, b) => a + b, 0) / prices.length).toFixed(2);
                const maxPrice = Math.max(...prices).toFixed(2);
                const minPrice = Math.min(...prices).toFixed(2);
                const totalVolume = volumes.reduce((a, b) => a + b, 0);
                
                statsHtml += `
                    <h4><i class="fas fa-chart-bar"></i> 統計資訊</h4>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <span class="stat-label">平均收盤價:</span>
                            <span class="stat-value">$${avgPrice}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">最高價:</span>
                            <span class="stat-value">$${maxPrice}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">最低價:</span>
                            <span class="stat-value">$${minPrice}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">總成交量:</span>
                            <span class="stat-value">${totalVolume.toLocaleString()}</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">數據筆數:</span>
                            <span class="stat-value">${data.length} 筆</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">價格波動:</span>
                            <span class="stat-value">${((maxPrice - minPrice) / minPrice * 100).toFixed(2)}%</span>
                        </div>
                    </div>
                `;
            }
        } else if (type === 'return') {
            const returns = data.map(d => d.daily_return).filter(r => r !== null && r !== undefined);
            
            if (returns.length > 0) {
                const avgReturn = (returns.reduce((a, b) => a + b, 0) / returns.length * 100).toFixed(4);
                const maxReturn = (Math.max(...returns) * 100).toFixed(4);
                const minReturn = (Math.min(...returns) * 100).toFixed(4);
                const volatility = this.calculateVolatility(returns);
                
                statsHtml += `
                    <h4><i class="fas fa-chart-line"></i> 報酬率統計</h4>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <span class="stat-label">平均報酬率:</span>
                            <span class="stat-value">${avgReturn}%</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">最高報酬率:</span>
                            <span class="stat-value">${maxReturn}%</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">最低報酬率:</span>
                            <span class="stat-value">${minReturn}%</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">波動率:</span>
                            <span class="stat-value">${volatility}%</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">數據筆數:</span>
                            <span class="stat-value">${data.length} 筆</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-label">正報酬天數:</span>
                            <span class="stat-value">${returns.filter(r => r > 0).length} 天</span>
                        </div>
                    </div>
                `;
            }
        }
        
        statsHtml += '</div>';
        
        // 在表格後面添加統計資訊
        const tableContainer = document.querySelector('.table-container');
        if (tableContainer) {
            let existingStats = tableContainer.querySelector('.query-stats');
            if (existingStats) {
                existingStats.remove();
            }
            tableContainer.insertAdjacentHTML('afterend', statsHtml);
        }
    }

    calculateVolatility(returns) {
        if (returns.length < 2) return '0.0000';
        
        const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
        const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / (returns.length - 1);
        const volatility = Math.sqrt(variance) * Math.sqrt(252) * 100; // 年化波動率
        
        return volatility.toFixed(4);
    }

    exportQueryResults() {
        const resultsTable = document.getElementById('queryTable');
        if (!resultsTable || !resultsTable.querySelector('tbody tr')) {
            this.addLogMessage('沒有查詢結果可以匯出', 'warning');
            return;
        }

        try {
            const symbol = document.getElementById('tickerInput').value.trim() || 'stock';
            const timestamp = new Date().toISOString().slice(0, 19).replace(/[:.]/g, '-');
            const filename = `${symbol}_query_results_${timestamp}.csv`;

            // 獲取表格數據
            const headers = Array.from(resultsTable.querySelectorAll('thead th')).map(th => th.textContent);
            const rows = Array.from(resultsTable.querySelectorAll('tbody tr')).map(tr => 
                Array.from(tr.querySelectorAll('td')).map(td => td.textContent)
            );

            // 生成 CSV 內容
            let csvContent = headers.join(',') + '\n';
            rows.forEach(row => {
                csvContent += row.join(',') + '\n';
            });

            // 創建下載連結
            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const link = document.createElement('a');
            const url = URL.createObjectURL(blob);
            
            link.setAttribute('href', url);
            link.setAttribute('download', filename);
            link.style.visibility = 'hidden';
            
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);

            this.addLogMessage(`✅ 查詢結果已匯出為 ${filename}`, 'success');
            
        } catch (error) {
            this.addLogMessage(`匯出失敗: ${error.message}`, 'error');
        }
    }

    resetQueryResults() {
        const resultsTable = document.getElementById('queryTable');
        if (resultsTable) {
            resultsTable.innerHTML = `
                <thead>
                    <tr>
                        <th>請執行查詢</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="no-data">請輸入股票代碼並點擊查詢按鈕</td>
                    </tr>
                </tbody>
            `;
        }
        
        // 重置標題
        const sectionTitle = resultsTable?.closest('.section-group')?.querySelector('h3');
        if (sectionTitle) {
            sectionTitle.innerHTML = `<i class="fas fa-table"></i> 查詢結果`;
        }
        
        this.addLogMessage('查詢結果已重置', 'info');
    }

    async refreshDatabaseStats() {
        this.addLogMessage('正在刷新資料庫統計...', 'info');
        await this.loadStatistics();
        this.addLogMessage('資料庫統計已更新', 'success');
    }

    async checkDatabaseConnection() {
        try {
            const response = await fetch(`http://localhost:5003/api/test-connection${this.useLocalDb ? '?use_local_db=true' : ''}`);
            const data = await response.json();
            
            if (data.success) {
                this.addLogMessage('資料庫連接正常', 'success');
            } else {
                this.addLogMessage('資料庫連接失敗', 'error');
            }
        } catch (error) {
            this.addLogMessage('無法連接到服務器', 'error');
        }
    }

    // 載入統計數據
    async loadStatistics() {
        console.log('📊 載入統計數據...');
        try {
            const response = await fetch(`http://localhost:5003/api/statistics${this.useLocalDb ? '?use_local_db=true' : ''}`);
            const data = await response.json();
            
            if (data.success) {
                this.updateStatisticsDisplay(data.data);
                console.log('✅ 統計數據載入成功');
            } else {
                console.error('❌ 統計數據載入失敗:', data.error);
                this.showStatisticsError('載入統計數據失敗');
            }
        } catch (error) {
            console.error('❌ 統計數據載入錯誤:', error);
            this.showStatisticsError('無法連接到統計服務');
        }
    }

    // 更新統計數據顯示
    updateStatisticsDisplay(stats) {
        // 更新總記錄數
        const totalRecordsEl = document.getElementById('totalRecords');
        if (totalRecordsEl) {
            totalRecordsEl.textContent = stats.totalRecords ? stats.totalRecords.toLocaleString() : '0';
        }

        // 更新股票數量
        const uniqueStocksEl = document.getElementById('uniqueStocks');
        if (uniqueStocksEl) {
            uniqueStocksEl.textContent = stats.uniqueStocks ? stats.uniqueStocks.toLocaleString() : '0';
        }

        // 更新日期範圍
        const dateRangeEl = document.getElementById('dateRange');
        if (dateRangeEl && stats.dateRange) {
            dateRangeEl.textContent = `${stats.dateRange.start} ~ ${stats.dateRange.end}`;
        }

        // 更新最後更新時間
        const lastUpdateEl = document.getElementById('lastUpdate');
        if (lastUpdateEl && stats.lastUpdate) {
            lastUpdateEl.textContent = new Date(stats.lastUpdate).toLocaleString('zh-TW');
        }

        console.log('📊 統計數據已更新:', stats);
    }

    // 顯示統計數據錯誤
    showStatisticsError(message) {
        const statsSummary = document.getElementById('statsSummary');
        if (statsSummary) {
            statsSummary.innerHTML = `
                <div class="error-message">
                    <i class="fas fa-exclamation-triangle"></i>
                    <span>${message}</span>
                </div>
            `;
        }
    }

    updateDatabaseStatus(status) {
        const dbStatusElement = document.getElementById('dbStatus');
        const dbStatusText = document.getElementById('dbStatusText');
        
        if (!dbStatusElement || !dbStatusText) return;
        
        // 移除所有狀態類別
        dbStatusElement.classList.remove('status-connected', 'status-error', 'status-checking');
        
        const statusTexts = {
            'connected': '資料庫狀態: 已連接',
            'error': '資料庫狀態: 連接失敗',
            'checking': '資料庫狀態: 檢查中...'
        };
        
        dbStatusText.textContent = statusTexts[status] || '資料庫狀態: 未知';
        dbStatusElement.classList.add(`status-${status}`);
    }

    async testDatabaseConnection() {
        this.addLogMessage('正在測試資料庫連接...', 'info');
        await this.checkDatabaseConnection();
    }

    saveDatabaseSettings() {
        this.addLogMessage('保存資料庫設定功能開發中...', 'info');
    }

    resetSystemSettings() {
        this.addLogMessage('重設系統設定功能開發中...', 'info');
    }

    saveSystemSettings() {
        this.addLogMessage('保存系統設定功能開發中...', 'info');
    }

    clearLog() {
        const logContainer = document.getElementById('logContent');
        if (!logContainer) {
            console.error('Log container not found');
            return;
        }
        logContainer.innerHTML = '';
        this.addLogMessage('日誌已清空', 'info');
    }

    exportLogCSV() {
        try {
            const logContainer = document.getElementById('logContent');
            if (!logContainer) {
                this.addLogMessage('找不到日誌容器，無法匯出', 'error');
                return;
            }

            const entries = Array.from(logContainer.querySelectorAll('.log-entry'));
            if (entries.length === 0) {
                this.addLogMessage('沒有日誌可匯出', 'warning');
                return;
            }

            const escapeCSV = (value) => {
                if (value === null || value === undefined) return '';
                const str = String(value).replace(/"/g, '""');
                return /[",\r\n]/.test(str) ? `"${str}"` : str;
            };

            const rows = [];
            // Header
            rows.push(['time', 'level', 'message']);

            // Data rows
            for (const el of entries) {
                const timeText = el.querySelector('.log-time')?.textContent?.trim() || '';
                // remove surrounding brackets [..]
                const time = timeText.replace(/^\[/, '').replace(/\]$/, '');
                let levelText = el.querySelector('.log-level')?.textContent?.trim() || '';
                levelText = levelText.replace(/:$/, '').toLowerCase();
                const message = el.querySelector('.log-message')?.textContent || '';
                rows.push([time, levelText, message]);
            }

            const csvContent = '\ufeff' + rows
                .map(cols => cols.map(escapeCSV).join(','))
                .join('\r\n');

            const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
            const filename = `app_log_${timestamp}.csv`;

            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);

            this.addLogMessage(`✅ 日誌已匯出為 ${filename}`, 'success');
        } catch (error) {
            console.error('Export log error:', error);
            this.addLogMessage(`匯出日誌失敗: ${error.message}`, 'error');
        }
    }

    // 統計功能相關方法
    setupStatsEventListeners() {
        // 市場總覽更新按鈕
        const refreshMarketBtn = document.getElementById('refreshMarketOverview');
        console.log('Market overview button found:', refreshMarketBtn);
        if (refreshMarketBtn) {
            refreshMarketBtn.addEventListener('click', () => {
                console.log('Market overview button clicked');
                this.refreshMarketOverview();
            });
        } else {
            console.error('refreshMarketOverview button not found');
        }

        // 排行榜查詢按鈕
        const refreshRankingsBtn = document.getElementById('refreshRankings');
        if (refreshRankingsBtn) {
            refreshRankingsBtn.addEventListener('click', () => {
                this.refreshRankings();
            });
        }

        // 個股分析按鈕
        const analyzeStockBtn = document.getElementById('analyzeStock');
        if (analyzeStockBtn) {
            analyzeStockBtn.addEventListener('click', () => {
                this.analyzeStock();
            });
        }

        // 個股輸入框回車事件
        const stockInput = document.getElementById('stockSymbolInput');
        if (stockInput) {
            stockInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.analyzeStock();
                }
            });
        }
    }

    async refreshMarketOverview() {
        try {
            console.log('refreshMarketOverview called');
            this.addLogMessage('正在獲取市場總覽...', 'info');
            
            const response = await fetch('/api/stats/overview');
            console.log('API response:', response);
            const result = await response.json();
            console.log('API result:', result);
            
            if (result.success) {
                const data = result.data;
                
                // 更新市場總覽數據
                const advancersEl = document.getElementById('advancers');
                const declinersEl = document.getElementById('decliners');
                const adRatioEl = document.getElementById('adRatio');
                const avgReturnEl = document.getElementById('avgReturn');
                
                console.log('Elements found:', {advancersEl, declinersEl, adRatioEl, avgReturnEl});
                
                if (advancersEl) advancersEl.textContent = data.advancers || '-';
                if (declinersEl) declinersEl.textContent = data.decliners || '-';
                if (adRatioEl) adRatioEl.textContent = 
                    data.advance_decline_ratio ? data.advance_decline_ratio.toFixed(2) : '-';
                if (avgReturnEl) avgReturnEl.textContent = 
                    data.avg_return ? (parseFloat(data.avg_return) * 100).toFixed(2) + '%' : '-';
                
                this.addLogMessage('市場總覽更新成功', 'success');
            } else {
                this.addLogMessage(`市場總覽更新失敗: ${result.error}`, 'error');
            }
        } catch (error) {
            console.error('Market overview error:', error);
            this.addLogMessage(`市場總覽更新錯誤: ${error.message}`, 'error');
        }
    }

    async refreshRankings() {
        try {
            const metric = document.getElementById('rankingMetric').value;
            const market = document.getElementById('rankingMarket').value;
            const limit = document.getElementById('rankingLimit').value;
            
            console.log('Rankings request:', {metric, market, limit});
            this.addLogMessage(`正在查詢排行榜 (${metric})...`, 'info');
            
            let url = `/api/stats/rankings?metric=${metric}&limit=${limit}`;
            if (market) {
                url += `&market=${market}`;
            }
            
            console.log('API URL:', url);
            const response = await fetch(url);
            console.log('API Response:', response.status, response.statusText);
            
            const result = await response.json();
            console.log('API Result:', result);
            
            if (result.success) {
                console.log('Rankings data:', result.data);
                this.updateRankingsTable(result.data.data);
                this.addLogMessage(`排行榜更新成功，共 ${result.data.count} 筆`, 'success');
            } else {
                console.error('Rankings API error:', result.error);
                this.addLogMessage(`排行榜查詢失敗: ${result.error}`, 'error');
            }
        } catch (error) {
            console.error('Rankings error:', error);
            this.addLogMessage(`排行榜查詢錯誤: ${error.message}`, 'error');
        }
    }

    updateRankingsTable(data) {
        const tbody = document.querySelector('#rankingsTable tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        if (!data || data.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="9" class="no-data">
                        <div class="no-data-content">
                            <i class="fas fa-info-circle"></i>
                            <span>沒有找到符合條件的數據</span>
                        </div>
                    </td>
                </tr>
            `;
            return;
        }
        
        data.forEach((item, index) => {
            const row = document.createElement('tr');
            
            // 格式化數值
            const formatPercent = (value) => {
                return value !== null && value !== undefined ? 
                    (value * 100).toFixed(2) + '%' : '-';
            };
            
            const formatNumber = (value) => {
                return value !== null && value !== undefined ? 
                    value.toLocaleString() : '-';
            };
            
            const formatPrice = (value) => {
                return value !== null && value !== undefined ? 
                    value.toFixed(2) : '-';
            };
            
            // 技術訊號
            let signals = [];
            if (item.technical_signals) {
                if (item.technical_signals.golden_cross) signals.push('黃金交叉');
                if (item.technical_signals.death_cross) signals.push('死亡交叉');
                if (item.technical_signals.breakout_20d_high) signals.push('突破20日高');
                if (item.technical_signals.breakdown_20d_low) signals.push('跌破20日低');
            }
            
            row.innerHTML = `
                <td>${index + 1}</td>
                <td>${item.symbol}</td>
                <td>${item.name}</td>
                <td>${formatPrice(item.current_price)}</td>
                <td>${formatPercent(item.returns?.['1d'])}</td>
                <td>${formatPercent(item.returns?.['1w'])}</td>
                <td>${formatPercent(item.returns?.['1m'])}</td>
                <td>${formatNumber(item.volume_metrics?.avg_volume)}</td>
                <td>${signals.join(', ') || '-'}</td>
            `;
            
            tbody.appendChild(row);
        });
    }

    async analyzeStock() {
        const symbolInput = document.getElementById('stockSymbolInput');
        const symbol = symbolInput.value.trim();
        
        if (!symbol) {
            this.addLogMessage('請輸入股票代碼', 'warning');
            return;
        }
        
        try {
            this.addLogMessage(`正在分析 ${symbol}...`, 'info');
            
            const response = await fetch(`/api/stats/stock/${symbol}`);
            const result = await response.json();
            
            if (result.success) {
                this.displayStockAnalysis(result.data);
                this.addLogMessage(`${symbol} 分析完成`, 'success');
            } else {
                this.addLogMessage(`${symbol} 分析失敗: ${result.error}`, 'error');
            }
        } catch (error) {
            console.error('Stock analysis error:', error);
            this.addLogMessage(`股票分析錯誤: ${error.message}`, 'error');
        }
    }

    displayStockAnalysis(data) {
        const resultsDiv = document.getElementById('stockAnalysisResults');
        if (!resultsDiv) return;
        
        // 格式化函數
        const formatPercent = (value) => {
            return value !== null && value !== undefined ? 
                (value * 100).toFixed(2) + '%' : '-';
        };
        
        const formatPrice = (value) => {
            return value !== null && value !== undefined ? 
                value.toFixed(2) : '-';
        };
        
        // 更新基本資訊
        document.getElementById('currentPrice').textContent = formatPrice(data.current_price);
        document.getElementById('dailyReturn').textContent = formatPercent(data.returns?.['1d']);
        document.getElementById('volatility').textContent = formatPercent(data.volatility);
        document.getElementById('maxDrawdown').textContent = formatPercent(data.max_drawdown);
        
        // 更新報酬分析
        document.getElementById('return1d').textContent = formatPercent(data.returns?.['1d']);
        document.getElementById('return1w').textContent = formatPercent(data.returns?.['1w']);
        document.getElementById('return1m').textContent = formatPercent(data.returns?.['1m']);
        document.getElementById('return3m').textContent = formatPercent(data.returns?.['3m']);
        document.getElementById('return1y').textContent = formatPercent(data.returns?.['1y']);
        
        // 更新移動平均線
        document.getElementById('ma5').textContent = formatPrice(data.moving_averages?.ma5);
        document.getElementById('ma10').textContent = formatPrice(data.moving_averages?.ma10);
        document.getElementById('ma20').textContent = formatPrice(data.moving_averages?.ma20);
        document.getElementById('ma60').textContent = formatPrice(data.moving_averages?.ma60);
        
        // 更新技術訊號
        this.updateTechnicalSignals(data.technical_signals);
        
        // 顯示結果區域
        resultsDiv.style.display = 'block';
    }

    updateTechnicalSignals(signals) {
        const signalsGrid = document.getElementById('technicalSignals');
        if (!signalsGrid || !signals) return;
        
        signalsGrid.innerHTML = '';
        
        const signalItems = [
            { key: 'golden_cross', label: '黃金交叉', icon: '🟡' },
            { key: 'death_cross', label: '死亡交叉', icon: '🔴' },
            { key: 'breakout_20d_high', label: '突破20日高', icon: '📈' },
            { key: 'breakdown_20d_low', label: '跌破20日低', icon: '📉' },
            { key: 'deviation_ma20', label: 'MA20乖離率', icon: '📊', isPercent: true }
        ];
        
        signalItems.forEach(item => {
            const signalDiv = document.createElement('div');
            signalDiv.className = 'signal-item';
            
            let value = '-';
            let status = 'neutral';
            
            if (item.key in signals) {
                if (item.isPercent) {
                    value = (signals[item.key] * 100).toFixed(2) + '%';
                    status = signals[item.key] > 0 ? 'positive' : 'negative';
                } else {
                    value = signals[item.key] ? '是' : '否';
                    status = signals[item.key] ? 'positive' : 'negative';
                }
            }
            
            signalDiv.innerHTML = `
                <div class="signal-icon">${item.icon}</div>
                <div class="signal-info">
                    <div class="signal-label">${item.label}</div>
                    <div class="signal-value ${status}">${value}</div>
                </div>
            `;
            
            signalsGrid.appendChild(signalDiv);
        });
    }

    // 批量更新所有上市股票
    async updateAllListedStocks() {
        if (this.isUpdating) {
            this.addLogMessage('目前有更新進行中，請稍後再試', 'warning');
            return;
        }

        try {
            this.isUpdating = true;
            
            // 更新操作狀態
            this.updateActionStatus('running', '正在更新上市股票...');

            this.addLogMessage('開始批量更新所有上市股票...', 'info');

            // 獲取所有上市股票代碼
            const response = await fetch(this.useLocalDb ? 'http://localhost:5003/api/symbols?use_local_db=true' : 'http://localhost:5003/api/symbols');
            const result = await response.json();
            
            if (!result.success) {
                throw new Error(result.error || '獲取股票清單失敗');
            }

            // 過濾出上市股票 (.TW 結尾)
            const listedStocks = result.data.filter(stock => 
                stock.symbol && stock.symbol.endsWith('.TW')
            );

            if (listedStocks.length === 0) {
                throw new Error('未找到上市股票');
            }

            this.addLogMessage(`找到 ${listedStocks.length} 支上市股票`, 'info');

            // 獲取日期範圍
            const dateRange = this.getSelectedDateRange();

            // 全市場單次請求（避免逐檔請求造成 429 與重複抓取）
            this.addLogMessage('🚀 上市全量模式：改為單次請求', 'info');
            const batchUpdateData = {
                symbols: listedStocks.map(s => s.symbol),
                start_date: dateRange.start,
                end_date: dateRange.end,
                update_prices: true,
                update_returns: false,
                respect_requested_range: true,
                use_batch_mode: true,
                use_local_db: this.useLocalDb
            };

            const resp = await fetch('http://localhost:5003/api/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(batchUpdateData)
            });

            if (!resp.ok) {
                let bodyText = '';
                try { bodyText = await resp.text(); } catch (_) { /* ignore */ }
                throw new Error(`HTTP ${resp.status} ${resp.statusText || ''} ${bodyText ? '- ' + bodyText.slice(0, 300) : ''}`.trim());
            }

            const batchResult = await resp.json();
            if (!batchResult.success) {
                throw new Error(batchResult.error || '批量更新失敗');
            }

            this.addLogMessage(`所有上市股票更新完成！共處理 ${listedStocks.length} 支股票`, 'success');
            this.updateActionStatus('ready', '上市股票更新完成');

        } catch (error) {
            console.error('批量更新上市股票失敗:', error);
            this.addLogMessage(`批量更新上市股票失敗: ${error.message}`, 'error');
            this.updateActionStatus('error', '上市股票更新失敗');
        } finally {
            this.isUpdating = false;
        }
    }

    // 批量更新所有上櫃股票
    async updateAllOtcStocks() {
        if (this.isUpdating) {
            this.addLogMessage('目前有更新進行中，請稍後再試', 'warning');
            return;
        }

        try {
            this.isUpdating = true;
            
            // 更新操作狀態
            this.updateActionStatus('running', '正在更新上櫃股票...');

            this.addLogMessage('開始批量更新所有上櫃股票...', 'info');

            // 獲取所有上櫃股票代碼
            const response = await fetch('http://localhost:5003/api/symbols');
            const result = await response.json();
            
            if (!result.success) {
                throw new Error(result.error || '獲取股票清單失敗');
            }

            // 過濾出上櫃股票 (.TWO 結尾)
            const otcStocks = result.data.filter(stock => 
                stock.symbol && stock.symbol.endsWith('.TWO')
            );

            if (otcStocks.length === 0) {
                throw new Error('未找到上櫃股票');
            }

            this.addLogMessage(`找到 ${otcStocks.length} 支上櫃股票`, 'info');

            // 獲取日期範圍
            const dateRange = this.getSelectedDateRange();

            // 全市場單次請求（避免逐檔請求造成 429 與重複抓取）
            this.addLogMessage('🚀 上櫃全量模式：改為單次請求', 'info');
            const batchUpdateData = {
                symbols: otcStocks.map(s => s.symbol),
                start_date: dateRange.start,
                end_date: dateRange.end,
                update_prices: true,
                update_returns: false,
                respect_requested_range: true,
                use_batch_mode: true,
                use_local_db: this.useLocalDb
            };

            const resp = await fetch('http://localhost:5003/api/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(batchUpdateData)
            });

            if (!resp.ok) {
                let bodyText = '';
                try { bodyText = await resp.text(); } catch (_) { /* ignore */ }
                throw new Error(`HTTP ${resp.status} ${resp.statusText || ''} ${bodyText ? '- ' + bodyText.slice(0, 300) : ''}`.trim());
            }

            const batchResult = await resp.json();
            if (!batchResult.success) {
                throw new Error(batchResult.error || '批量更新失敗');
            }

            this.addLogMessage(`所有上櫃股票更新完成！共處理 ${otcStocks.length} 支股票`, 'success');
            this.updateActionStatus('ready', '上櫃股票更新完成');

        } catch (error) {
            console.error('批量更新上櫃股票失敗:', error);
            this.addLogMessage(`批量更新上櫃股票失敗: ${error.message}`, 'error');
            this.updateActionStatus('error', '上櫃股票更新失敗');
        } finally {
            this.isUpdating = false;
        }
    }

    // 批量更新股票的通用方法
    async batchUpdateStocks(stocks, dateRange, progressElements) {
        const { progressFill, progressText, progressPercent, marketType } = progressElements;
        
        // 獲取效能參數
        const batchSize = parseInt(document.getElementById('inputBatchSize')?.value || '10');
        const concurrency = parseInt(document.getElementById('inputConcurrency')?.value || '20');
        const interBatchDelay = parseInt(document.getElementById('inputInterBatchDelay')?.value || '300');

        let completed = 0;
        let successful = 0;
        let failed = 0;

        // 分批處理
        for (let i = 0; i < stocks.length; i += batchSize) {
            const batch = stocks.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            const totalBatches = Math.ceil(stocks.length / batchSize);

            this.addLogMessage(`處理第 ${batchNumber}/${totalBatches} 批 ${marketType}股票 (${batch.length} 支)`, 'info');
            progressText.textContent = `處理第 ${batchNumber}/${totalBatches} 批 ${marketType}股票...`;

            // 並行處理當前批次
            const batchResults = await this.runWithConcurrency(
                batch,
                concurrency,
                async (stock) => {
                    try {
                        const response = await fetch('http://localhost:5003/api/update', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({
                                symbols: [stock.symbol],
                                start_date: dateRange.start,
                                end_date: dateRange.end,
                                batch_size: 1,
                                concurrency: 1,
                                inter_batch_delay: 0,
                                respect_requested_range: true,
                                use_local_db: this.useLocalDb
                            })
                        });

                        const result = await response.json();
                        
                        if (result.success) {
                            successful++;
                            return { success: true, symbol: stock.symbol };
                        } else {
                            failed++;
                            throw new Error(result.error || '更新失敗');
                        }
                    } catch (error) {
                        failed++;
                        throw error;
                    }
                }
            );

            // 更新進度
            completed += batch.length;
            const progress = Math.round((completed / stocks.length) * 100);
            progressFill.style.width = `${progress}%`;
            progressPercent.textContent = `${progress}%`;

            // 記錄批次結果
            const batchSuccessful = batchResults.filter(r => r.status === 'fulfilled').length;
            const batchFailed = batchResults.filter(r => r.status === 'rejected').length;
            
            this.addLogMessage(
                `第 ${batchNumber} 批完成: 成功 ${batchSuccessful}, 失敗 ${batchFailed}`, 
                batchFailed > 0 ? 'warning' : 'success'
            );

            // 批次間延遲
            if (i + batchSize < stocks.length && interBatchDelay > 0) {
                progressText.textContent = `批次間暫停 ${interBatchDelay}ms...`;
                await this.sleep(interBatchDelay);
            }
        }

        // 最終統計
        this.addLogMessage(
            `${marketType}股票批量更新完成: 總計 ${stocks.length} 支, 成功 ${successful} 支, 失敗 ${failed} 支`,
            failed > 0 ? 'warning' : 'success'
        );
    }

    // 簡化的批量更新方法，使用現有的進度條系統
    async batchUpdateStocksSimple(stocks, dateRange, marketType) {
        // 獲取效能參數
        const batchSize = parseInt(document.getElementById('inputBatchSize')?.value || '10');
        const concurrency = parseInt(document.getElementById('inputConcurrency')?.value || '20');
        const interBatchDelay = parseInt(document.getElementById('inputInterBatchDelay')?.value || '300');

        let completed = 0;
        let successful = 0;
        let failed = 0;

        // 初始化摘要
        this.startSummary(stocks.length);

        // 分批處理
        for (let i = 0; i < stocks.length; i += batchSize) {
            const batch = stocks.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            const totalBatches = Math.ceil(stocks.length / batchSize);

            this.addLogMessage(`處理第 ${batchNumber}/${totalBatches} 批 ${marketType}股票 (${batch.length} 支)`, 'info');
            this.updateProgress(0, `處理第 ${batchNumber}/${totalBatches} 批 ${marketType}股票...`);

            // 並行處理當前批次
            const batchResults = await this.runWithConcurrency(
                batch,
                concurrency,
                async (stock) => {
                    try {
                        const response = await fetch('http://localhost:5003/api/update', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({
                                symbols: [stock.symbol],
                                start_date: dateRange.start,
                                end_date: dateRange.end,
                                batch_size: 1,
                                concurrency: 1,
                                inter_batch_delay: 0,
                                respect_requested_range: true,
                                use_local_db: this.useLocalDb
                            })
                        });

                        const result = await response.json();
                        
                        if (result.success) {
                            successful++;
                            this.incrementSummary({ success: true });
                        } else {
                            failed++;
                            this.incrementSummary({ success: false });
                        }
                    } catch (error) {
                        failed++;
                        this.incrementSummary({ success: false });
                        this.addLogMessage(`❌ ${stock.symbol} 更新失敗: ${error.message}`, 'error');
                    }
                }
            );

            // 更新進度
            completed += batch.length;
            const progress = Math.round((completed / stocks.length) * 100);
            this.updateProgress(progress, `已處理 ${completed}/${stocks.length} 支股票`);

            // 記錄批次結果
            const batchSuccessful = batchResults.filter(r => r.status === 'fulfilled').length;
            const batchFailed = batchResults.filter(r => r.status === 'rejected').length;
            
            this.addLogMessage(
                `第 ${batchNumber} 批完成: 成功 ${batchSuccessful}, 失敗 ${batchFailed}`, 
                batchFailed > 0 ? 'warning' : 'success'
            );

            // 批次間延遲
            if (i + batchSize < stocks.length && interBatchDelay > 0) {
                this.updateProgress(progress, `批次間暫停 ${interBatchDelay}ms...`);
                await this.sleep(interBatchDelay);
            }
        }

        // 最終統計
        this.addLogMessage(
            `${marketType}股票批量更新完成: 總計 ${stocks.length} 支, 成功 ${successful} 支, 失敗 ${failed} 支`,
            failed > 0 ? 'warning' : 'success'
        );
        
        this.updateProgress(100, `${marketType}股票更新完成: ${successful}/${stocks.length} 成功`);
        this.finishSummary();
    }

    // 獲取選中的日期範圍
    getSelectedDateRange() {
        const startDateInput = document.getElementById('startDate');
        const endDateInput = document.getElementById('endDate');
        
        if (startDateInput && endDateInput && startDateInput.value && endDateInput.value) {
            return {
                start: startDateInput.value,
                end: endDateInput.value
            };
        }
        
        // 如果沒有自定義日期，使用快速選項
        const activeQuickOption = document.querySelector('.quick-option.active');
        if (activeQuickOption) {
            const days = parseInt(activeQuickOption.dataset.days);
            const endDate = new Date();
            const startDate = new Date();
            startDate.setDate(endDate.getDate() - days);
            
            return {
                start: startDate.toISOString().split('T')[0],
                end: endDate.toISOString().split('T')[0]
            };
        }
        
        // 默認最近30天
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(endDate.getDate() - 30);
        
        return {
            start: startDate.toISOString().split('T')[0],
            end: endDate.toISOString().split('T')[0]
        };
    }

    // ===== 資料庫同步功能 =====
    async checkNeonConnection() {
        const statusBadge = document.getElementById('neonStatusBadge');
        const btnStartSync = document.getElementById('btnStartSync');
        const btnDownloadFromNeon = document.getElementById('btnDownloadFromNeon');
        
        if (statusBadge) {
            statusBadge.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 檢查中...';
            statusBadge.className = 'status-badge';
        }
        
        try {
            const response = await fetch('/api/database-sync/status');
            const data = await response.json();
            
            if (data.connected) {
                if (statusBadge) {
                    statusBadge.innerHTML = '<i class="fas fa-check-circle"></i> 已連接';
                    statusBadge.className = 'status-badge success';
                }
                if (btnStartSync) {
                    btnStartSync.disabled = false;
                }
                if (btnDownloadFromNeon) {
                    btnDownloadFromNeon.disabled = false;
                }
                this.addLogMessage('✅ Neon 資料庫連接成功', 'success');
            } else {
                if (statusBadge) {
                    statusBadge.innerHTML = '<i class="fas fa-times-circle"></i> 未連接';
                    statusBadge.className = 'status-badge error';
                }
                if (btnStartSync) {
                    btnStartSync.disabled = true;
                }
                if (btnDownloadFromNeon) {
                    btnDownloadFromNeon.disabled = true;
                }
                this.addLogMessage(`❌ Neon 資料庫連接失敗: ${data.error}`, 'error');
            }
        } catch (error) {
            if (statusBadge) {
                statusBadge.innerHTML = '<i class="fas fa-times-circle"></i> 錯誤';
                statusBadge.className = 'status-badge error';
            }
            if (btnStartSync) {
                btnStartSync.disabled = true;
            }
            if (btnDownloadFromNeon) {
                btnDownloadFromNeon.disabled = true;
            }
            this.addLogMessage(`❌ 檢查連接時發生錯誤: ${error.message}`, 'error');
        }
    }

    async startDatabaseDownload() {
        const doTruncate = confirm('下載模式選擇：\n\n按「確定」：先清空本機選中表格，再從 Neon 重新下載（覆寫式）\n按「取消」：保留本機既有資料，只嘗試寫入 Neon 尚未存在於本機的資料');
        if (!confirm('確定要開始「Neon → 本機」下載同步嗎？此操作可能需要較長時間。')) {
            return;
        }

        const btnDownload = document.getElementById('btnDownloadFromNeon');
        const progressSection = document.getElementById('syncProgressSection');
        const resultsSection = document.getElementById('syncResultsSection');
        const progressBar = document.getElementById('syncProgressBar');
        const progressText = document.getElementById('syncProgressText');
        const progressInfo = document.getElementById('syncProgressInfo');

        if (btnDownload) {
            btnDownload.disabled = true;
            btnDownload.innerHTML = '<i class="fas fa-spinner fa-spin"></i> <span>下載中...</span>';
        }

        if (progressSection) {
            progressSection.style.display = 'block';
        }
        if (resultsSection) {
            resultsSection.style.display = 'none';
        }

        this.showSyncLogPanel();
        this.ensureDbSyncSse();

        const origin = (typeof window !== 'undefined' && window.location && window.location.origin)
            ? window.location.origin
            : '';
        const base = origin && origin !== 'file://' ? origin : 'http://localhost:5003';

        if (progressBar) {
            progressBar.style.width = '10%';
        }
        if (progressText) {
            progressText.textContent = '10%';
        }
        if (progressInfo) {
            progressInfo.textContent = '正在連接資料庫...';
            progressInfo.style.color = '';
        }

        this.addLogMessage('⬇️ 開始同步：Neon → 本機...', 'info');
        this.addLogMessage('☁️ 連接 Neon 資料庫...', 'info');

        try {
            let progress = 10;
            const progressInterval = setInterval(() => {
                if (progress < 90) {
                    progress += 5;
                    if (progressBar) {
                        progressBar.style.width = progress + '%';
                    }
                    if (progressText) {
                        progressText.textContent = progress + '%';
                    }
                    if (progressInfo) {
                        progressInfo.textContent = `正在下載數據... ${progress}%`;
                    }
                }
            }, 1000);

            const sel = this.getTableSelectionDebug();
            const countTextEl = document.getElementById('tableSelectionCount');
            const countText = countTextEl ? countTextEl.textContent : '';
            const cacheLen = (this._syncTableCheckboxEls && this._syncTableCheckboxEls.length) ? this._syncTableCheckboxEls.length : 0;
            this.addSyncLogLine(`📋 表格選擇：已勾選 ${sel.checked}/${sel.total}`, 'info');
            this.addSyncLogLine(`🔎 選取來源=${sel.source || ''} selector=${sel.selector || ''} cache=${cacheLen} UI=${countText}`, 'info');
            this.addSyncLogLine(`🔎 DOM: syncTab=${sel.syncTabExists} tableList=${sel.tableListExists} children=${sel.tableListChildCount} inputs=${sel.inputsInTableList} syncTabCheckbox=${sel.checkboxInSyncTab} id^table_(syncTab)=${sel.idTableNodesInSyncTab} id^table_(tableList)=${sel.idTableNodesInTableList}`, 'info');

            const selectedTables = sel.selectedTables;
            if (selectedTables.length === 0) {
                this.addSyncLogLine('⚠️ 未取得任何勾選表格（請確認表格列表是否在本頁面渲染且 checkbox 狀態正確）', 'warning');
                throw new Error('請至少選擇一個表格');
            }

            this.addSyncLogLine('📨 已送出同步請求（下載：Neon → 本機）', 'info');

            this.addLogMessage(`📋 準備下載 ${selectedTables.length} 個表格`, 'info');

            const response = await fetch(`${base}/api/database-sync/download`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    tables: selectedTables,
                    truncateLocal: doTruncate
                })
            });

            clearInterval(progressInterval);

            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                const text = await response.text();
                console.error('收到非 JSON 響應:', text);
                throw new Error('伺服器返回了非 JSON 響應，可能發生錯誤');
            }

            const data = await response.json();

            if (data.success) {
                if (progressBar) {
                    progressBar.style.width = '100%';
                }
                if (progressText) {
                    progressText.textContent = '100%';
                }
                if (progressInfo) {
                    progressInfo.textContent = `完成！共下載 ${data.totalRows} 行數據`;
                }

                this.addLogMessage('☁️ Neon 資料庫連接成功', 'success');
                this.addLogMessage('✅ 本地資料庫連接成功', 'success');

                if (data.tables) {
                    data.tables.forEach(table => {
                        if (table.success) {
                            this.addLogMessage(`✓ ${table.name}: ${table.insertedCount}/${table.rowCount} 行下載成功`, 'success');
                        } else {
                            this.addLogMessage(`✗ ${table.name}: ${table.error}`, 'error');
                        }
                    });
                }

                this.displaySyncResults(data);
                this.addLogMessage(`✅ 同步完成（下載：Neon → 本機）！總表格數: ${data.totalTables}, 總行數: ${data.totalRows}`, 'success');
            } else {
                throw new Error(data.error || '下載失敗');
            }

        } catch (error) {
            this.addLogMessage(`❌ 同步失敗（下載）：${error.message}`, 'error');
            this.addSyncLogLine(`❌ 同步請求失敗（下載）：${error.message}`, 'error');
            if (progressInfo) {
                progressInfo.textContent = `錯誤: ${error.message}`;
                progressInfo.style.color = '#ef4444';
            }
        } finally {
            if (btnDownload) {
                btnDownload.disabled = false;
                btnDownload.innerHTML = '<i class="fas fa-cloud-download-alt"></i> <span>從 Neon 下載到本機</span>';
            }
        }
    }

    async startDatabaseSync() {
        if (!confirm('確定要開始上傳資料庫嗎？此操作可能需要較長時間。')) {
            return;
        }

        const btnStartSync = document.getElementById('btnStartSync');
        const progressSection = document.getElementById('syncProgressSection');
        const resultsSection = document.getElementById('syncResultsSection');
        const progressBar = document.getElementById('syncProgressBar');
        const progressText = document.getElementById('syncProgressText');
        const progressInfo = document.getElementById('syncProgressInfo');

        // 禁用按鈕
        if (btnStartSync) {
            btnStartSync.disabled = true;
            btnStartSync.innerHTML = '<i class="fas fa-spinner fa-spin"></i> <span>上傳中...</span>';
        }

        // 顯示進度區域
        if (progressSection) {
            progressSection.style.display = 'block';
        }
        if (resultsSection) {
            resultsSection.style.display = 'none';
        }

        this.showSyncLogPanel();

        // 重置進度
        if (progressBar) {
            progressBar.style.width = '10%';
        }
        if (progressText) {
            progressText.textContent = '10%';
        }
        if (progressInfo) {
            progressInfo.textContent = '正在連接資料庫...';
        }

        this.addLogMessage('🚀 開始上傳資料庫到 Neon...', 'info');
        this.addLogMessage('📡 連接本地資料庫...', 'info');

        try {
            // 模擬進度更新
            let progress = 10;
            const progressInterval = setInterval(() => {
                if (progress < 90) {
                    progress += 5;
                    if (progressBar) {
                        progressBar.style.width = progress + '%';
                    }
                    if (progressText) {
                        progressText.textContent = progress + '%';
                    }
                    if (progressInfo) {
                        progressInfo.textContent = `正在上傳數據... ${progress}%`;
                    }
                }
            }, 1000);

            const sel = this.getTableSelectionDebug();
            const countTextEl = document.getElementById('tableSelectionCount');
            const countText = countTextEl ? countTextEl.textContent : '';
            const cacheLen = (this._syncTableCheckboxEls && this._syncTableCheckboxEls.length) ? this._syncTableCheckboxEls.length : 0;
            this.addSyncLogLine(`📋 表格選擇：已勾選 ${sel.checked}/${sel.total}`, 'info');
            this.addSyncLogLine(`🔎 選取來源=${sel.source || ''} selector=${sel.selector || ''} cache=${cacheLen} UI=${countText}`, 'info');
            this.addSyncLogLine(`🔎 DOM: syncTab=${sel.syncTabExists} tableList=${sel.tableListExists} children=${sel.tableListChildCount} inputs=${sel.inputsInTableList} syncTabCheckbox=${sel.checkboxInSyncTab} id^table_(syncTab)=${sel.idTableNodesInSyncTab} id^table_(tableList)=${sel.idTableNodesInTableList}`, 'info');

            const selectedTables = sel.selectedTables;
            if (selectedTables.length === 0) {
                this.addSyncLogLine('⚠️ 未取得任何勾選表格（請確認表格列表是否在本頁面渲染且 checkbox 狀態正確）', 'warning');
                throw new Error('請至少選擇一個表格');
            }

            this.addSyncLogLine('📨 已送出同步請求（上傳：本機 → Neon）', 'info');
            
            this.addLogMessage(`📋 準備上傳 ${selectedTables.length} 個表格`, 'info');
            
            const response = await fetch(`${base}/api/database-sync/upload`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    tables: selectedTables
                })
            });

            clearInterval(progressInterval);

            // 檢查響應是否為 JSON
            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                const text = await response.text();
                console.error('收到非 JSON 響應:', text);
                throw new Error('伺服器返回了非 JSON 響應，可能發生錯誤');
            }

            const data = await response.json();

            if (data.success) {
                // 更新進度到 100%
                if (progressBar) {
                    progressBar.style.width = '100%';
                }
                if (progressText) {
                    progressText.textContent = '100%';
                }
                if (progressInfo) {
                    progressInfo.textContent = `完成！共上傳 ${data.totalRows} 行數據`;
                }

                // 在日誌中顯示每個表格的結果
                this.addLogMessage('✅ 本地資料庫連接成功', 'success');
                this.addLogMessage('☁️ Neon 資料庫連接成功', 'success');
                
                if (data.tables) {
                    data.tables.forEach(table => {
                        if (table.success) {
                            this.addLogMessage(`✓ ${table.name}: ${table.insertedCount}/${table.rowCount} 行上傳成功`, 'success');
                        } else {
                            this.addLogMessage(`✗ ${table.name}: ${table.error}`, 'error');
                        }
                    });
                }

                // 顯示結果
                this.displaySyncResults(data);
                
                this.addLogMessage(`✅ 資料庫同步完成！總表格數: ${data.totalTables}, 總行數: ${data.totalRows}`, 'success');
            } else {
                throw new Error(data.error || '上傳失敗');
            }
        } catch (error) {
            this.addLogMessage(`❌ 資料庫同步失敗: ${error.message}`, 'error');
            this.addSyncLogLine(`❌ 同步請求失敗（上傳）：${error.message}`, 'error');
            
            if (progressInfo) {
                progressInfo.textContent = `錯誤: ${error.message}`;
                progressInfo.style.color = '#ef4444';
            }
        } finally {
            // 恢復按鈕
            if (btnStartSync) {
                btnStartSync.disabled = false;
                btnStartSync.innerHTML = '<i class="fas fa-cloud-upload-alt"></i> <span>開始上傳</span>';
            }
        }
    }

    displaySyncResults(data) {
        const resultsSection = document.getElementById('syncResultsSection');
        const summaryDiv = document.getElementById('syncResultsSummary');
        const detailsDiv = document.getElementById('syncResultsDetails');

        if (!resultsSection || !summaryDiv || !detailsDiv) return;

        // 顯示結果區域
        resultsSection.style.display = 'block';

        // 生成摘要
        const errorCount = data.errors ? data.errors.length : 0;
        const totalTables = data.totalTables || 0;
        const totalRows = data.totalRows || 0;

        const isDownload = data.direction === 'download';
        const rowLabel = isDownload ? '總下載行數' : '總上傳行數';
        
        summaryDiv.innerHTML = `
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-icon">
                        <i class="fas fa-table"></i>
                    </div>
                    <div class="stat-info">
                        <div class="stat-value">${totalTables}</div>
                        <div class="stat-label">總表格數</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon success">
                        <i class="fas fa-check"></i>
                    </div>
                    <div class="stat-info">
                        <div class="stat-value">${totalRows}</div>
                        <div class="stat-label">${rowLabel}</div>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-icon ${errorCount > 0 ? 'error' : 'success'}">
                        <i class="fas fa-${errorCount > 0 ? 'exclamation-triangle' : 'check-circle'}"></i>
                    </div>
                    <div class="stat-info">
                        <div class="stat-value">${errorCount}</div>
                        <div class="stat-label">錯誤數</div>
                    </div>
                </div>
            </div>
        `;

        // 生成詳細結果
        if (data.tables && data.tables.length > 0) {
            let detailsHTML = '<h4 style="margin-bottom: 15px;">表格詳情</h4><div class="table-results-list">';
            
            data.tables.forEach(table => {
                const iconClass = table.success ? 'fa-check-circle success' : 'fa-times-circle error';
                const statusText = table.success 
                    ? `${isDownload ? '下載' : '上傳'} ${table.insertedCount} / ${table.rowCount} 行`
                    : table.error;
                
                detailsHTML += `
                    <div class="table-result-item">
                        <div class="table-result-header">
                            <i class="fas ${iconClass}"></i>
                            <span class="table-name">${table.name}</span>
                        </div>
                        <div class="table-result-details ${table.success ? '' : 'error'}">
                            ${statusText}
                        </div>
                    </div>
                `;
            });
            
            detailsHTML += '</div>';
            detailsDiv.innerHTML = detailsHTML;
        }

        // 如果有錯誤，顯示錯誤列表
        if (data.errors && data.errors.length > 0) {
            let errorsHTML = '<h4 style="margin-top: 20px; margin-bottom: 15px;">錯誤詳情</h4><div class="error-list">';
            
            data.errors.forEach(error => {
                errorsHTML += `
                    <div class="error-item">
                        <strong>${error.table}:</strong> ${error.error}
                    </div>
                `;
            });
            
            errorsHTML += '</div>';
            detailsDiv.innerHTML += errorsHTML;
        }
    }
}

// Initialize app when DOM is ready
let app;
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        app = new TaiwanStockApp();
        // 模式切換：股價更新 <-> BWIBBU 回朔 <-> 融資融券 <-> 三大法人 (T86) <-> 月營收 <-> 損益表 <-> 權證
        try {
            const modeSelect = document.getElementById('modeSelect');
            const updateTabEl = document.getElementById('updateTab');
            const bwibbuTabEl = document.getElementById('bwibbuTab');
            const marginTabEl = document.getElementById('marginTab');
            const t86TabEl = document.getElementById('t86Tab');
            const revenueTabEl = document.getElementById('revenueTab');
            const incomeTabEl = document.getElementById('incomeTab');
            const balanceTabEl = document.getElementById('balanceTab');
            const cashflowTabEl = document.getElementById('cashflowTab');
            const ratiosTabEl = document.getElementById('ratiosTab');
            const warrantsTabEl = document.getElementById('warrantsTab');
            const queryTabEl = document.getElementById('queryTab');
            const bwibbuNavBtn = document.querySelector('[data-tab="bwibbu"]');
            const marginNavBtn = document.querySelector('[data-tab="margin"]');
            const t86NavBtn = document.querySelector('[data-tab="t86"]');
            const revenueNavBtn = document.querySelector('[data-tab="revenue"]');
            const incomeNavBtn = document.querySelector('[data-tab="income"]');
            const balanceNavBtn = document.querySelector('[data-tab="balance"]');
            const ratiosNavBtn = document.querySelector('[data-tab="ratios"]');
            if (bwibbuNavBtn) bwibbuNavBtn.style.display = 'none';
            if (marginNavBtn) marginNavBtn.style.display = 'none';
            if (t86NavBtn) t86NavBtn.style.display = 'none';
            if (revenueNavBtn) revenueNavBtn.style.display = 'none';
            if (incomeNavBtn) incomeNavBtn.style.display = 'none';
            if (balanceNavBtn) balanceNavBtn.style.display = 'none';
            if (ratiosNavBtn) ratiosNavBtn.style.display = 'none';

            const hideAllTabs = () => {
                if (updateTabEl) { updateTabEl.classList.remove('active'); updateTabEl.style.display = 'none'; }
                if (bwibbuTabEl) { bwibbuTabEl.classList.remove('active'); bwibbuTabEl.style.display = 'none'; }
                if (marginTabEl) { marginTabEl.classList.remove('active'); marginTabEl.style.display = 'none'; }
                if (t86TabEl) { t86TabEl.classList.remove('active'); t86TabEl.style.display = 'none'; }
                if (revenueTabEl) { revenueTabEl.classList.remove('active'); revenueTabEl.style.display = 'none'; }
                if (incomeTabEl) { incomeTabEl.classList.remove('active'); incomeTabEl.style.display = 'none'; }
                if (balanceTabEl) { balanceTabEl.classList.remove('active'); balanceTabEl.style.display = 'none'; }
                if (cashflowTabEl) { cashflowTabEl.classList.remove('active'); cashflowTabEl.style.display = 'none'; }
                if (ratiosTabEl) { ratiosTabEl.classList.remove('active'); ratiosTabEl.style.display = 'none'; }
                if (warrantsTabEl) { warrantsTabEl.classList.remove('active'); warrantsTabEl.style.display = 'none'; }
            };

            const applyMode = (mode) => {
                try { console.log('[Mode] applyMode DOMContentLoaded ->', mode); } catch {}
                const headerTitle = document.querySelector('.action-title');
                const execBtnText = document.querySelector('#executeUpdate .btn-text');

                hideAllTabs();

                if (mode === 'symbols') {
                    if (queryTabEl) { queryTabEl.style.display = ''; queryTabEl.classList.add('active'); }
                    try {
                        const block = document.getElementById('symbolsTable');
                        if (block) block.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    } catch (_) {}
                } else if (mode === 'etf') {
                    if (queryTabEl) { queryTabEl.style.display = ''; queryTabEl.classList.add('active'); }
                    try {
                        const block = document.getElementById('etfRefreshBtn') || document.getElementById('etfRefreshStatus');
                        if (block) block.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    } catch (_) {}
                } else if (mode === 'bwibbu') {
                    if (bwibbuTabEl) { bwibbuTabEl.style.display = ''; bwibbuTabEl.classList.add('active'); }
                    if (headerTitle) headerTitle.innerHTML = '<i class="fas fa-play-circle"></i> 執行 BWIBBU 回朔\n                                    執行操作';
                    if (execBtnText) execBtnText.textContent = '執行 BWIBBU 回朔';
                } else if (mode === 't86') {
                    if (t86TabEl) { t86TabEl.style.display = ''; t86TabEl.classList.add('active'); }
                    // 隱藏 update 的執行文案（該面板已被隱藏），不強制改 header
                } else if (mode === 'margin') {
                    if (marginTabEl) { marginTabEl.style.display = ''; marginTabEl.classList.add('active'); }
                    // 融資融券模式目前僅為占位畫面，不修改主執行區標題
                } else if (mode === 'revenue') {
                    if (revenueTabEl) { revenueTabEl.style.display = ''; revenueTabEl.classList.add('active'); }
                    // 月營收模式使用獨立面板，不修改主執行區標題
                } else if (mode === 'income') {
                    if (incomeTabEl) { incomeTabEl.style.display = ''; incomeTabEl.classList.add('active'); }
                    // 損益表模式使用獨立面板，不修改主執行區標題
                } else if (mode === 'balance') {
                    if (balanceTabEl) { balanceTabEl.style.display = ''; balanceTabEl.classList.add('active'); }
                } else if (mode === 'cashflow') {
                    if (cashflowTabEl) { cashflowTabEl.style.display = ''; cashflowTabEl.classList.add('active'); }
                } else if (mode === 'ratios') {
                    if (ratiosTabEl) { ratiosTabEl.style.display = ''; ratiosTabEl.classList.add('active'); }
                } else if (mode === 'warrants') {
                    if (warrantsTabEl) { warrantsTabEl.style.display = ''; warrantsTabEl.classList.add('active'); }
                    app.ensureWarrantsInitialized?.();
                } else {
                    if (updateTabEl) { updateTabEl.style.display = ''; updateTabEl.classList.add('active'); }
                    if (headerTitle) headerTitle.innerHTML = '<i class="fas fa-play-circle"></i> 執行股價更新\n                                    執行操作';
                    if (execBtnText) execBtnText.textContent = '執行股價更新';
                }
            };

            // 暴露給全域（配合 index.html 的 onchange）
            try { window.__applyMode = applyMode; } catch {}

            if (modeSelect) {
                applyMode(modeSelect.value);
                modeSelect.addEventListener('change', (e) => applyMode(e.target.value));
            }
        } catch (e) {
            console.warn('模式切換初始化失敗:', e);
        }
    });
} else {
    app = new TaiwanStockApp();
    // 模式切換：股價更新 <-> BWIBBU 回朔 <-> 融資融券 <-> T86 <-> 月營收 <-> 損益表 <-> 權證（非 loading 狀態初始化）
    try {
        const modeSelect = document.getElementById('modeSelect');
        const updateTabEl = document.getElementById('updateTab');
        const bwibbuTabEl = document.getElementById('bwibbuTab');
        const marginTabEl = document.getElementById('marginTab');
        const t86TabEl = document.getElementById('t86Tab');
        const revenueTabEl = document.getElementById('revenueTab');
        const incomeTabEl = document.getElementById('incomeTab');
        const balanceTabEl = document.getElementById('balanceTab');
        const cashflowTabEl = document.getElementById('cashflowTab');
        const ratiosTabEl = document.getElementById('ratiosTab');
        const warrantsTabEl = document.getElementById('warrantsTab');
        const queryTabEl = document.getElementById('queryTab');
        const bwibbuNavBtn = document.querySelector('[data-tab="bwibbu"]');
        const marginNavBtn = document.querySelector('[data-tab="margin"]');
        const t86NavBtn = document.querySelector('[data-tab="t86"]');
        const revenueNavBtn = document.querySelector('[data-tab="revenue"]');
        const incomeNavBtn = document.querySelector('[data-tab="income"]');
        const balanceNavBtn = document.querySelector('[data-tab="balance"]');
        const ratiosNavBtn = document.querySelector('[data-tab="ratios"]');
        if (bwibbuNavBtn) bwibbuNavBtn.style.display = 'none';
        if (marginNavBtn) marginNavBtn.style.display = 'none';
        if (t86NavBtn) t86NavBtn.style.display = 'none';
        if (revenueNavBtn) revenueNavBtn.style.display = 'none';
        if (incomeNavBtn) incomeNavBtn.style.display = 'none';
        if (balanceNavBtn) balanceNavBtn.style.display = 'none';
        if (ratiosNavBtn) ratiosNavBtn.style.display = 'none';

        const hideAllTabs = () => {
            if (updateTabEl) { updateTabEl.classList.remove('active'); updateTabEl.style.display = 'none'; }
            if (bwibbuTabEl) { bwibbuTabEl.classList.remove('active'); bwibbuTabEl.style.display = 'none'; }
            if (marginTabEl) { marginTabEl.classList.remove('active'); marginTabEl.style.display = 'none'; }
            if (t86TabEl) { t86TabEl.classList.remove('active'); t86TabEl.style.display = 'none'; }
            if (revenueTabEl) { revenueTabEl.classList.remove('active'); revenueTabEl.style.display = 'none'; }
            if (incomeTabEl) { incomeTabEl.classList.remove('active'); incomeTabEl.style.display = 'none'; }
            if (balanceTabEl) { balanceTabEl.classList.remove('active'); balanceTabEl.style.display = 'none'; }
            if (cashflowTabEl) { cashflowTabEl.classList.remove('active'); cashflowTabEl.style.display = 'none'; }
            if (ratiosTabEl) { ratiosTabEl.classList.remove('active'); ratiosTabEl.style.display = 'none'; }
            if (warrantsTabEl) { warrantsTabEl.classList.remove('active'); warrantsTabEl.style.display = 'none'; }
        };

        const applyMode = (mode) => {
            try { console.log('[Mode] applyMode immediate ->', mode); } catch {}
            const headerTitle = document.querySelector('.action-title');
            const execBtnText = document.querySelector('#executeUpdate .btn-text');

            hideAllTabs();

            if (mode === 'symbols') {
                if (queryTabEl) { queryTabEl.style.display = ''; queryTabEl.classList.add('active'); }
                try {
                    const block = document.getElementById('symbolsTable');
                    if (block) block.scrollIntoView({ behavior: 'smooth', block: 'start' });
                } catch (_) {}
            } else if (mode === 'etf') {
                if (queryTabEl) { queryTabEl.style.display = ''; queryTabEl.classList.add('active'); }
                try {
                    const block = document.getElementById('etfRefreshBtn') || document.getElementById('etfRefreshStatus');
                    if (block) block.scrollIntoView({ behavior: 'smooth', block: 'start' });
                } catch (_) {}
            } else if (mode === 'bwibbu') {
                if (bwibbuTabEl) { bwibbuTabEl.style.display = ''; bwibbuTabEl.classList.add('active'); }
                if (headerTitle) headerTitle.innerHTML = '<i class="fas fa-play-circle"></i> 執行 BWIBBU 回朔\n                                    執行操作';
                if (execBtnText) execBtnText.textContent = '執行 BWIBBU 回朔';
            } else if (mode === 't86') {
                if (t86TabEl) { t86TabEl.style.display = ''; t86TabEl.classList.add('active'); }
            } else if (mode === 'margin') {
                if (marginTabEl) { marginTabEl.style.display = ''; marginTabEl.classList.add('active'); }
                // 融資融券模式目前僅為占位畫面，不修改主執行區標題
            } else if (mode === 'revenue') {
                if (revenueTabEl) { revenueTabEl.style.display = ''; revenueTabEl.classList.add('active'); }
                // 月營收模式使用獨立面板，不修改主執行區標題
            } else if (mode === 'income') {
                if (incomeTabEl) { incomeTabEl.style.display = ''; incomeTabEl.classList.add('active'); }
                // 損益表模式使用獨立面板，不修改主執行區標題
            } else if (mode === 'balance') {
                if (balanceTabEl) { balanceTabEl.style.display = ''; balanceTabEl.classList.add('active'); }
            } else if (mode === 'cashflow') {
                if (cashflowTabEl) { cashflowTabEl.style.display = ''; cashflowTabEl.classList.add('active'); }
            } else if (mode === 'ratios') {
                if (ratiosTabEl) { ratiosTabEl.style.display = ''; ratiosTabEl.classList.add('active'); }
            } else if (mode === 'warrants') {
                if (warrantsTabEl) { warrantsTabEl.style.display = ''; warrantsTabEl.classList.add('active'); }
                app.ensureWarrantsInitialized?.();
            } else {
                if (updateTabEl) { updateTabEl.style.display = ''; updateTabEl.classList.add('active'); }
                if (headerTitle) headerTitle.innerHTML = '<i class="fas fa-play-circle"></i> 執行股價更新\n                                    執行操作';
                if (execBtnText) execBtnText.textContent = '執行股價更新';
            }
        };

        // 暴露給全域（配合 index.html 的 onchange）
        try { window.__applyMode = applyMode; } catch {}

        if (modeSelect) {
            applyMode(modeSelect.value);
            modeSelect.addEventListener('change', (e) => applyMode(e.target.value));
        }
    } catch (e) {
        console.warn('模式切換初始化失敗:', e);
    }
}
