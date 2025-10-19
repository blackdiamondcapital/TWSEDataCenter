// Taiwan Stock Data Update System - JavaScript
class TaiwanStockApp {
    constructor() {
        this.dbConfig = {
            host: 'localhost',
            port: '5432',
            user: 'postgres',
            password: 's8304021',
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
        this.init();
    }

    setupDatabaseTargetToggle() {
        const toggle = document.getElementById('dbTargetToggle');
        if (!toggle) return;

        const radios = toggle.querySelectorAll('input[name="dbTarget"]');
        radios.forEach((radio) => {
            radio.addEventListener('change', async (event) => {
                if (!event.target.checked) return;
                this.dbTarget = event.target.value === 'local' ? 'local' : 'remote';
                this.addLogMessage(`🔁 切換資料庫目標為 ${this.dbTarget === 'local' ? '本地 PostgreSQL' : 'Neon（雲端）'}`, 'info');
                await this.checkDatabaseConnection();
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
            const url = `http://localhost:5003/api/anomalies/export?${qs.toString()}`;
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
            const refetchOnly = !!document.getElementById('refetchOnlyToggle')?.checked;
            const ok = window.confirm(refetchOnly
                ? `將對 ${symbol || '全部股票'} 在 ${start}~${end} 期間進行：僅重抓（不刪除，不備份），threshold=${threshold}。是否繼續？`
                : `將對 ${symbol || '全部股票'} 在 ${start}~${end} 期間進行：備份→刪除→重抓，threshold=${threshold}。是否繼續？`
            );
            if (!ok) return;

            this.addLogMessage(`🧹 開始修復：symbol=${symbol || 'ALL'}, 範圍=${start}~${end}, 閾值=${threshold}${refetchOnly ? '（僅重抓）' : ''}`, 'info');
            const res = await fetch('http://localhost:5003/api/anomalies/fix', {
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
            const { symbol, start, end, threshold } = this.getAnomalyParams();
            if (!start || !end) {
                this.addLogMessage('請填寫開始與結束日期再執行修復', 'warning');
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
            const url = `http://localhost:5003/api/anomalies/fix_stream?${qs.toString()}`;
            this.addLogMessage(`🧹（串流）開始修復：symbol=${symbol || 'ALL'}, ${start}~${end}, threshold=${threshold}`, 'info');

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
        this.initializeDates();
        this.initializeDisplayAreas();
        this.checkDatabaseConnection();
        this.addLogMessage('系統已啟動', 'info');
        
        // 延遲初始化默認選項，確保 DOM 完全載入
        setTimeout(() => {
            this.initializeDefaultOptions();
            this.loadStatistics(); // 載入統計數據
        }, 100);

        // Init new UI behaviors
        this.initSummaryBar();
        this.initLogControls();
        this.startApiHealthPolling();
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

        // Auto experiments
        this.safeAddEventListener('startAutoExperiments', () => this.runAutoExperiments());
        
        // Anomaly detection & fix
        this.safeAddEventListener('detectAnomaliesBtn', () => this.detectAnomalies());
        this.safeAddEventListener('exportAnomaliesBtn', () => this.exportAnomalies());
        // 使用串流版本，於修復過程中即時顯示抓到的股價數據
        this.safeAddEventListener('fixAnomaliesBtn', () => this.fixAnomaliesStream());
        // Returns compute
        this.safeAddEventListener('computeReturnsBtn', () => this.computeReturnsFromUI());
        
        console.log('✅ 事件監聽器設置完成');
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
            const fillMissing = !!document.getElementById('returnsFillMissing')?.checked;
            
            // 獲取資料庫選擇
            const dbTarget = document.querySelector('input[name="returnsDbTarget"]:checked')?.value || 'local';

            const payload = {
                start: cfg.startDate,
                end: cfg.endDate,
                fillMissing: fillMissing,
            };
            
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

            this.addLogMessage(`🧮 開始計算報酬：${payload.symbol || `ALL${payload.limit ? ` (limit=${payload.limit})` : ''}`}，範圍 ${payload.start}~${payload.end}，fillMissing=${fillMissing}，資料庫: ${dbDesc}`, 'info');

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
        document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
        const targetPane = document.getElementById(`${tabName}Tab`);
        if (targetPane) {
            targetPane.classList.add('active');
            console.log(`✅ 內容面板 ${tabName}Tab 已顯示`);
        } else {
            console.log(`❌ 找不到內容面板: ${tabName}Tab`);
        }

        const names = { 'update': '資料更新', 'query': '資料查詢', 'stats': '資料統計', 'settings': '系統設定' };
        this.addLogMessage(`切換到${names[tabName] || tabName}頁面`, 'info');
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
                
                // 分批處理避免超時
                const batchSize = updateBatchSize;
                const totalBatches = Math.ceil(symbols.length / batchSize);
                let processedCount = 0;
                
                for (let i = 0; i < totalBatches; i++) {
                    const startIdx = i * batchSize;
                    const endIdx = Math.min(startIdx + batchSize, symbols.length);
                    const batchSymbols = symbols.slice(startIdx, endIdx);
                
                this.addLogMessage(`處理第 ${i + 1}/${totalBatches} 批，股票 ${startIdx + 1}-${endIdx}`, 'info');
                
                // 顯示當前批次的股票
                const symbolNames = batchSymbols.map(s => `${s.symbol}(${s.name})`).join(', ');
                this.addLogMessage(`當前批次: ${symbolNames}`, 'info');
                
                // 批次計時開始
                const batchStartTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();

                try {
                    // 以受控並發處理每檔股票
                    const concurrency = updateConcurrency;
                    await this.runWithConcurrency(batchSymbols, concurrency, async (stock) => {
                            this.addLogMessage(`🔄 正在抓取 ${stock.symbol} (${stock.name})...`, 'info');

                            const fetchStartTime = new Date();
                            this.addLogMessage(`⏱️ 開始抓取 ${stock.symbol} (${stock.name}) 股價數據: ${fetchStartTime.toLocaleString('zh-TW')}`, 'info');

                            const singleUpdateData = {
                                symbols: [stock.symbol],
                                update_prices: updatePrices,
                                update_returns: updateReturns,
                                start_date: startDate,
                                end_date: endDate,
                                respect_requested_range: true,
                                use_local_db: this.useLocalDb
                            };

                            const resp = await fetch('http://localhost:5003/api/update', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify(singleUpdateData)
                            });
                            if (!resp.ok) {
                                let bodyText = '';
                                try { bodyText = await resp.text(); } catch (_) { /* ignore */ }
                                throw new Error(`HTTP ${resp.status} ${resp.statusText || ''} ${bodyText ? '- ' + bodyText.slice(0, 300) : ''}`.trim());
                            }
                            let singleResult;
                            try {
                                singleResult = await resp.json();
                            } catch (e) {
                                throw new Error(`回應不是有效的 JSON：${e.message}`);
                            }

                            const fetchEndTime = new Date();
                            const fetchDuration = (fetchEndTime - fetchStartTime) / 1000;
                            this.addLogMessage(`⏱️ 完成抓取 ${stock.symbol} (${stock.name}) 並匯入: ${fetchEndTime.toLocaleString('zh-TW')} (耗時 ${fetchDuration.toFixed(2)} 秒)`, 'info');

                            if (singleResult.success && singleResult.results && singleResult.results.length > 0) {
                                const result = singleResult.results[0];

                                let storageInfo = [];
                                let duplicateInfo = [];
                                if (result.price_records !== undefined) storageInfo.push(`股價: ${result.price_records} 筆`);
                                if (result.return_records !== undefined) storageInfo.push(`報酬率: ${result.return_records} 筆`);
                                if (result.duplicate_records !== undefined && result.duplicate_records > 0) duplicateInfo.push(`重複跳過: ${result.duplicate_records} 筆`);

                                let statusText = '';
                                // 當股價沒有新增但存在重複資料時，顯示「資料庫中已有重複資料，未新增」
                                if (result.price_records === 0 && result.duplicate_records && result.duplicate_records > 0) {
                                    statusText = ` (資料庫中已有重複資料，未新增；${duplicateInfo.join(', ')})`;
                                } else {
                                    if (storageInfo.length > 0) statusText += ` (新增 ${storageInfo.join(', ')})`;
                                    if (duplicateInfo.length > 0) statusText += ` (${duplicateInfo.join(', ')})`;
                                }
                                if (!statusText) statusText = ' (無新數據)';

                                if (result.status === 'partial') {
                                    this.addLogMessage(`⚠️ ${stock.symbol} (${stock.name}) 部分完成${statusText}`, 'warning');
                                } else {
                                    this.addLogMessage(`✅ ${stock.symbol} (${stock.name}) 完成${statusText}`, 'success');
                                }

                                if (result.price_date_range) {
                                    const sd = new Date(result.price_date_range.start).toLocaleDateString('zh-TW');
                                    const ed = new Date(result.price_date_range.end).toLocaleDateString('zh-TW');
                                    this.addLogMessage(`📅 ${stock.symbol} 股價數據日期: ${sd} ~ ${ed}`, 'info');
                                }
                                if (result.return_date_range) {
                                    const sd = new Date(result.return_date_range.start).toLocaleDateString('zh-TW');
                                    const ed = new Date(result.return_date_range.end).toLocaleDateString('zh-TW');
                                    this.addLogMessage(`📊 ${stock.symbol} 報酬率數據日期: ${sd} ~ ${ed}`, 'info');
                                }
                            } else {
                                // 盡可能從回應找出清楚的錯誤訊息
                                const detail = singleResult?.error
                                    || singleResult?.message
                                    || singleResult?.detail
                                    || (() => {
                                        try { return JSON.stringify(singleResult).slice(0, 300); } catch (_) { return ''; }
                                    })()
                                    || '未知錯誤';
                                this.addLogMessage(`❌ ${stock.symbol} (${stock.name}) 失敗: ${detail}`,'error');
                            }

                            // 完成一檔後更新進度（並發安全：僅做加一）
                            processedCount++;
                            const progress = 20 + (processedCount / symbols.length) * 70;
                            this.updateProgress(progress, `已處理 ${processedCount}/${symbols.length} 檔股票`);
                        });

                        const batchEndTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                        const batchElapsed = batchEndTime - batchStartTime;
                        const batchHuman = this.formatDuration(batchElapsed);
                        this.addLogMessage(`📦 批次 ${i + 1}/${totalBatches} 完成，耗時 ${batchHuman}，累計已處理 ${processedCount}/${symbols.length} 檔`, 'info');
                    } catch (error) {
                        const batchEndTime = (typeof performance !== 'undefined' && performance.now) ? performance.now() : Date.now();
                        const batchElapsed = batchEndTime - batchStartTime;
                        const batchHuman = this.formatDuration(batchElapsed);
                        this.addLogMessage(`批次 ${i + 1} 處理失敗: ${error.message}（耗時 ${batchHuman}）`, 'error');
                    }

                    // 批次間短暫延遲，避免壓力峰值
                    if (i < totalBatches - 1) {
                        await new Promise(resolve => setTimeout(resolve, interBatchDelayMs));
                    }
                }
            }
            
            this.updateProgress(100, '更新完成');
            
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
                        const priceStats = statsData.data_statistics.stock_prices;
                        const returnStats = statsData.data_statistics.stock_returns;
                        
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
                        this.addLogMessage(`🏷️ 資料表: stock_prices (股價), stock_returns (報酬率)`, 'info');
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

    // 格式化日期
    formatDate(dateString) {
        const date = new Date(dateString);
        return date.toLocaleDateString('zh-TW', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });
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
            
            // 批量更新 - 使用現有的進度條系統
            await this.batchUpdateStocksSimple(listedStocks, dateRange, '上市');

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
            
            // 批量更新 - 使用現有的進度條系統
            await this.batchUpdateStocksSimple(otcStocks, dateRange, '上櫃');

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
}

// 初始化應用程式
document.addEventListener('DOMContentLoaded', () => {
    new TaiwanStockApp();
});
