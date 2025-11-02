# BWIBBU 歷史資料回朔工具

台灣股市本益比、殖利率及股價淨值比（BWIBBU）的批量回朔與入庫工具。

## 功能

- 📅 **日期範圍選擇**：選擇開始和結束日期，自動過濾工作日
- 🗄️ **資料庫切換**：支援 Neon（雲端）和本地 PostgreSQL
- 📊 **進度追蹤**：實時顯示回朔進度和日誌
- 🔍 **資料查詢**：查詢資料庫中已存儲的日期和筆數
- 💾 **自動去重**：使用 UPSERT 避免重複記錄

## 快速開始

### 1. 安裝依賴

```bash
cd /Users/liangfuting/CascadeProjects/BWIBBUBackfill
pip install -r requirements.txt
```

### 2. 設定環境變數

創建 `.env` 文件（可選，使用預設值）：

```env
# Neon 資料庫
NEON_DB_HOST=ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech
NEON_DB_NAME=neondb
NEON_DB_USER=neondb_owner
NEON_DB_PASSWORD=your_password

# 本地資料庫
LOCAL_DB_HOST=localhost
LOCAL_DB_PORT=5432
LOCAL_DB_NAME=twse_data
LOCAL_DB_USER=postgres
LOCAL_DB_PASSWORD=postgres
```

### 3. 啟動後端

```bash
python server.py
```

後端將在 `http://localhost:5004` 啟動。

### 4. 開啟前端

在瀏覽器中打開 `index.html`，或使用簡單 HTTP 伺服器：

```bash
python -m http.server 8000
```

然後訪問 `http://localhost:8000`

## API 端點

### POST /api/backfill

回朔指定日期範圍的 BWIBBU 資料。

**請求**：
```json
{
  "start": "2025-10-01",
  "end": "2025-11-02",
  "use_local_db": false
}
```

**回應**：
```json
{
  "success": true,
  "total_records": 1054,
  "available_dates": ["2025-10-31"],
  "message": "成功寫入 1054 筆記錄"
}
```

### GET /api/query

查詢資料庫中的 BWIBBU 資料。

**參數**：
- `use_local_db`：是否使用本地資料庫（true/false）
- `start`：開始日期（可選）
- `end`：結束日期（可選）

**回應**：
```json
{
  "success": true,
  "dates": ["2025-10-31"],
  "total_count": 1054
}
```

## 資料庫表結構

```sql
CREATE TABLE tw_stock_bwibbu (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    name VARCHAR(100),
    pe_ratio FLOAT,
    dividend_yield FLOAT,
    pb_ratio FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code, date)
);
```

## 使用說明

1. **設定日期範圍**：在 UI 中選擇開始和結束日期
2. **選擇目標資料庫**：Neon（雲端）或本地 PostgreSQL
3. **點擊「開始回朔」**：系統會自動：
   - 生成工作日清單
   - 逐日從 TWSE 抓取資料
   - 寫入目標資料庫
   - 顯示進度和結果
4. **查詢資料**：點擊「查詢資料」查看已存儲的日期和筆數

## 注意事項

- TWSE BWIBBU_d 介面僅提供交易日資料，非交易日會回傳「無符合資料」
- 抓取時會自動添加 0.2 秒延遲，避免被限流
- 使用 UPSERT 邏輯，重複日期的記錄會被更新而非重複插入
- 建議一次回朔不超過 120 天，避免耗時過長

## 故障排除

**連接資料庫失敗**：
- 檢查環境變數配置
- 確認 PostgreSQL 服務運行
- 驗證 Neon 連接字符串

**抓取資料為空**：
- 確認日期範圍內有交易日
- 檢查 TWSE 官方網站是否可訪問
- 查看日誌中的詳細錯誤信息

**進度卡住**：
- 檢查網路連接
- 查看瀏覽器控制台是否有錯誤
- 重新啟動後端服務

## 技術棧

- **後端**：Flask + psycopg2
- **前端**：Vanilla JavaScript + CSS3
- **資料庫**：PostgreSQL（Neon / 本地）
- **資料抓取**：requests + pandas

## 許可證

MIT
