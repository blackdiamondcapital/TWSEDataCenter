# 📅 股價資料自動排程系統

自動抓取台灣股市資料並同步到本地端和 Neon 雲端資料庫。

## 🎯 功能特色

- ✅ **每日自動抓取**：週一至週五 15:30 自動抓取當日股價
- ✅ **每週完整回補**：週日 02:00 執行完整資料回補
- ✅ **雙資料庫同步**：同時更新本地 PostgreSQL 和 Neon 雲端資料庫
- ✅ **自動報酬率計算**：抓取完成後自動計算報酬率
- ✅ **詳細日誌記錄**：所有操作都有完整日誌
- ✅ **錯誤處理機制**：失敗自動重試，不影響系統穩定性

## 📦 安裝步驟

### 1. 安裝依賴套件

```bash
pip3 install apscheduler psycopg2-binary requests
```

### 2. 配置資料庫連線

編輯 `scheduler.py`，確認以下配置：

```python
# 本地資料庫配置
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 's8304021',  # 修改為你的密碼
    'database': 'postgres'
}

# Neon 資料庫 URL
NEON_DB_URL = 'postgresql://...'  # 修改為你的 Neon URL
```

### 3. 測試排程器

```bash
# 測試每日抓取功能
python3 scheduler.py --test

# 測試每週回補功能
python3 scheduler.py --weekly
```

### 4. 安裝系統服務

#### macOS (推薦使用 launchd)

```bash
# 執行安裝腳本
chmod +x install_scheduler.sh
./install_scheduler.sh
```

或手動安裝：

```bash
# 複製 plist 到 LaunchAgents
cp stock-scheduler.plist ~/Library/LaunchAgents/

# 載入服務
launchctl load ~/Library/LaunchAgents/stock-scheduler.plist

# 啟動服務
launchctl start com.stock.scheduler
```

#### Linux (使用 systemd)

1. 建立服務檔：

```bash
sudo nano /etc/systemd/system/stock-scheduler.service
```

2. 貼上以下內容（修改路徑和使用者）：

```ini
[Unit]
Description=Stock Data Scheduler
After=network.target postgresql.service

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/path/to/TWSEDataCenter-main
Environment="NEON_DATABASE_URL=your_neon_url"
ExecStart=/usr/bin/python3 /path/to/TWSEDataCenter-main/scheduler.py
Restart=always
RestartSec=60
StandardOutput=append:/path/to/TWSEDataCenter-main/scheduler_stdout.log
StandardError=append:/path/to/TWSEDataCenter-main/scheduler_stderr.log

[Install]
WantedBy=multi-user.target
```

3. 啟動服務：

```bash
sudo systemctl daemon-reload
sudo systemctl enable stock-scheduler
sudo systemctl start stock-scheduler
sudo systemctl status stock-scheduler
```

## 🎮 使用方式

### 手動執行

```bash
# 啟動排程器（持續運行）
python3 scheduler.py

# 測試執行一次抓取
python3 scheduler.py --test

# 測試執行每週回補
python3 scheduler.py --weekly
```

### 管理系統服務

#### macOS

```bash
# 啟動服務
launchctl start com.stock.scheduler

# 停止服務
launchctl stop com.stock.scheduler

# 重新載入服務
launchctl unload ~/Library/LaunchAgents/stock-scheduler.plist
launchctl load ~/Library/LaunchAgents/stock-scheduler.plist

# 查看服務狀態
launchctl list | grep stock
```

#### Linux

```bash
# 啟動服務
sudo systemctl start stock-scheduler

# 停止服務
sudo systemctl stop stock-scheduler

# 重啟服務
sudo systemctl restart stock-scheduler

# 查看狀態
sudo systemctl status stock-scheduler

# 查看日誌
sudo journalctl -u stock-scheduler -f
```

## 📊 監控與日誌

### 查看日誌

```bash
# 主要日誌（包含所有操作記錄）
tail -f scheduler.log

# 標準輸出日誌
tail -f scheduler_stdout.log

# 錯誤日誌
tail -f scheduler_stderr.log
```

### 日誌內容說明

- ✅ 成功標記：綠色勾號
- ❌ 錯誤標記：紅色叉號
- ⚠️  警告標記：黃色驚嘆號
- 📊 資料標記：圖表符號
- ☁️  雲端標記：雲朵符號

## 📅 排程時間表

| 任務 | 執行時間 | 說明 |
|------|---------|------|
| 每日抓取 | 週一至週五 15:30 | 抓取當日股價並同步 |
| 每週回補 | 週日 02:00 | 完整資料回補 |

## 🔧 進階配置

### 修改排程時間

編輯 `scheduler.py` 中的 `start()` 方法：

```python
# 每日排程：修改 hour 和 minute
self.scheduler.add_job(
    self.fetch_daily_data,
    CronTrigger(day_of_week='mon-fri', hour=15, minute=30),  # 修改這裡
    ...
)

# 每週排程：修改 day_of_week、hour 和 minute
self.scheduler.add_job(
    self.weekly_full_refresh,
    CronTrigger(day_of_week='sun', hour=2, minute=0),  # 修改這裡
    ...
)
```

### 添加新的排程任務

```python
# 例如：每月 1 號執行完整資料驗證
self.scheduler.add_job(
    self.verify_data,
    CronTrigger(day=1, hour=3, minute=0),
    id='monthly_verify',
    name='每月資料驗證'
)
```

## 🐛 故障排除

### 問題 1：排程器無法啟動

**解決方案**：
```bash
# 檢查 Python 路徑
which python3

# 檢查依賴套件
pip3 list | grep apscheduler

# 重新安裝依賴
pip3 install --upgrade apscheduler psycopg2-binary
```

### 問題 2：無法連接資料庫

**解決方案**：
```bash
# 測試本地資料庫連線
psql -h localhost -U postgres -d postgres

# 測試 Neon 資料庫連線
psql "$NEON_DATABASE_URL"

# 檢查防火牆設定
sudo ufw status
```

### 問題 3：資料同步失敗

**解決方案**：
```bash
# 查看詳細錯誤日誌
tail -100 scheduler.log | grep ERROR

# 手動測試同步
python3 scheduler.py --test

# 檢查資料庫表結構是否一致
psql -h localhost -U postgres -d postgres -c "\d tw_stock_prices"
```

### 問題 4：macOS launchd 服務無法啟動

**解決方案**：
```bash
# 檢查 plist 語法
plutil -lint ~/Library/LaunchAgents/stock-scheduler.plist

# 查看服務錯誤
launchctl error com.stock.scheduler

# 重新載入服務
launchctl unload ~/Library/LaunchAgents/stock-scheduler.plist
launchctl load ~/Library/LaunchAgents/stock-scheduler.plist
```

## 📧 通知設定（可選）

### 添加 Email 通知

在 `scheduler.py` 中添加：

```python
import smtplib
from email.mime.text import MIMEText

def send_notification(subject, message):
    """發送 Email 通知"""
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = 'your_email@gmail.com'
    msg['To'] = 'recipient@gmail.com'
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login('your_email@gmail.com', 'your_app_password')
        smtp.send_message(msg)
```

### 添加 LINE 通知

```python
import requests

def send_line_notify(message):
    """發送 LINE 通知"""
    url = 'https://notify-api.line.me/api/notify'
    headers = {'Authorization': f'Bearer {LINE_TOKEN}'}
    data = {'message': message}
    requests.post(url, headers=headers, data=data)
```

## 🔐 安全性建議

1. **不要將密碼硬編碼**：使用環境變數或配置檔
2. **定期更新密碼**：定期更換資料庫密碼
3. **限制資料庫存取**：只開放必要的 IP 位址
4. **加密敏感資料**：使用 SSL/TLS 連線
5. **定期備份**：設定自動備份機制

## 📈 效能優化

1. **批次處理**：使用 `execute_values` 批次插入資料
2. **連線池**：使用 `psycopg2.pool` 管理連線
3. **索引優化**：確保 `(symbol, date)` 有索引
4. **資料壓縮**：定期清理舊資料或歸檔

## 🤝 貢獻

歡迎提交 Issue 或 Pull Request！

## 📄 授權

MIT License
