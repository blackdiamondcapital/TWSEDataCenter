class BWIBBUBackfillApp {
    constructor() {
        this.apiBase = 'http://localhost:5004/api';
        this.isRunning = false;
        this.init();
    }
    
    init() {
        this.setDefaultDates();
        this.attachEventListeners();
    }
    
    setDefaultDates() {
        const today = new Date();
        const thirtyDaysAgo = new Date(today.getTime() - 30 * 24 * 60 * 60 * 1000);
        
        document.getElementById('endDate').valueAsDate = today;
        document.getElementById('startDate').valueAsDate = thirtyDaysAgo;
    }
    
    attachEventListeners() {
        document.getElementById('backfillBtn').addEventListener('click', () => this.startBackfill());
        document.getElementById('queryBtn').addEventListener('click', () => this.queryData());
    }
    
    addLog(message, type = 'info') {
        const logPanel = document.getElementById('logPanel');
        const line = document.createElement('div');
        line.className = `log-line ${type}`;
        const timestamp = new Date().toLocaleTimeString('zh-TW');
        line.textContent = `[${timestamp}] ${message}`;
        logPanel.appendChild(line);
        logPanel.scrollTop = logPanel.scrollHeight;
    }
    
    async startBackfill() {
        if (this.isRunning) return;
        
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;
        const dbType = document.getElementById('dbSelect').value;
        
        if (!startDate || !endDate) {
            alert('請選擇開始和結束日期');
            return;
        }
        
        this.isRunning = true;
        document.getElementById('backfillBtn').disabled = true;
        document.getElementById('progressSection').classList.add('active');
        document.getElementById('resultsSection').classList.remove('active');
        document.getElementById('logPanel').innerHTML = '';
        
        this.addLog(`開始回朔：${startDate} 至 ${endDate}（${dbType === 'neon' ? 'Neon' : '本地'}）`, 'info');
        
        try {
            const response = await fetch(`${this.apiBase}/backfill`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    start: startDate,
                    end: endDate,
                    use_local_db: dbType === 'local',
                    skip_existing: document.getElementById('skipExisting').checked
                })
            });
            
            const data = await response.json();
            
            if (data.success) {
                this.addLog(`✅ 回朔完成！寫入 ${data.total_records} 筆記錄`, 'success');
                if (data.daily_stats) {
                    const days = Object.keys(data.daily_stats).sort();
                    days.forEach(day => {
                        const s = data.daily_stats[day];
                        this.addLog(`- ${day} | 上市: ${s.twse_count} 筆 / ${s.twse_companies} 家，上櫃: ${s.tpex_count} 筆 / ${s.tpex_companies} 家，合計: ${s.total_count} 筆 / ${s.total_companies} 家`, 'info');
                    });
                    this.addLog(`寫入模式：${data.write_mode === 'insert_only' ? '只新增(跳過既有)' : '覆蓋更新(UPSERT)'}`, 'info');
                }
                this.showResults(data);
            } else {
                this.addLog(`❌ 回朔失敗：${data.error}`, 'error');
            }
        } catch (error) {
            this.addLog(`❌ 網路錯誤：${error.message}`, 'error');
        } finally {
            this.isRunning = false;
            document.getElementById('backfillBtn').disabled = false;
            document.getElementById('progressFill').style.width = '100%';
        }
    }
    
    showResults(data) {
        const resultsSection = document.getElementById('resultsSection');
        document.getElementById('resultMessage').textContent = data.message;
        document.getElementById('resultCount').textContent = `總筆數：${data.total_records}`;
        
        const datesList = document.getElementById('datesList');
        datesList.innerHTML = '';
        if (data.available_dates && data.available_dates.length > 0) {
            data.available_dates.forEach(date => {
                const span = document.createElement('span');
                span.textContent = date;
                datesList.appendChild(span);
            });
        } else {
            datesList.textContent = '無可用日期';
        }
        
        resultsSection.classList.add('active');
    }
    
    async queryData() {
        const startDate = document.getElementById('startDate').value;
        const endDate = document.getElementById('endDate').value;
        const dbType = document.getElementById('dbSelect').value;
        
        try {
            const params = new URLSearchParams({
                use_local_db: dbType === 'local'
            });
            if (startDate) params.append('start', startDate);
            if (endDate) params.append('end', endDate);
            
            const response = await fetch(`${this.apiBase}/query?${params.toString()}`);
            const data = await response.json();
            
            if (data.success) {
                this.addLog(`查詢完成：找到 ${data.total_count} 筆記錄，${data.dates.length} 個不同日期`, 'info');
                this.showResults({
                    message: '查詢成功',
                    total_records: data.total_count,
                    available_dates: data.dates
                });
            } else {
                this.addLog(`查詢失敗：${data.error}`, 'error');
            }
        } catch (error) {
            this.addLog(`查詢錯誤：${error.message}`, 'error');
        }
    }
}

// 頁面載入完成後初始化
document.addEventListener('DOMContentLoaded', () => {
    new BWIBBUBackfillApp();
});
