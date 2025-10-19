# 報酬率上傳到 Neon 雲端資料庫指南

## 功能說明

系統現在支援**靈活選擇**報酬率存入的資料庫位置，有以下三種獨立模式：

### 模式 1：僅使用本地資料庫 ⚡
```json
{
  "use_local_db": true,
  "upload_to_neon": false
}
```
- ✅ 從本地資料庫讀取股價
- ✅ 計算報酬率
- ✅ **僅**存入本地資料庫
- 🚫 不上傳到雲端

### 模式 2：僅使用 Neon 雲端資料庫 ☁️
```json
{
  "use_local_db": false,
  "upload_to_neon": false
}
```
- ✅ 從 Neon 雲端資料庫讀取股價
- ✅ 計算報酬率
- ✅ **僅**存入 Neon 雲端資料庫
- 🚫 不存本地

### 模式 3：同時存入兩個資料庫 🔄
```json
{
  "use_local_db": true,
  "upload_to_neon": true
}
```
- ✅ 從本地資料庫讀取股價
- ✅ 計算報酬率
- ✅ 存入本地資料庫
- ✅ **同時**上傳到 Neon 雲端資料庫

## API 使用方式

### 模式 1：僅存本地資料庫

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "2330.TW",
    "use_local_db": true,
    "upload_to_neon": false
  }'
```

### 模式 2：僅存 Neon 雲端資料庫

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "2330.TW",
    "use_local_db": false
  }'
```

### 模式 3：同時存入兩個資料庫

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "2330.TW",
    "use_local_db": true,
    "upload_to_neon": true
  }'
```

### 參數說明

| 參數 | 類型 | 說明 | 預設值 |
|------|------|------|--------|
| `symbol` | string | 指定股票代碼（如 2330.TW） | null |
| `start` | string | 起始日期 YYYY-MM-DD | null |
| `end` | string | 結束日期 YYYY-MM-DD | null |
| `all` | boolean | 計算所有股票 | false |
| `limit` | integer | 限制處理股票數量 | null |
| `fill_missing` | boolean | 僅計算缺失的報酬率 | false |
| `use_local_db` | boolean | 使用本地資料庫 | false |
| `upload_to_neon` | boolean | 同時上傳到 Neon | false |

### 使用範例

#### 範例 1：計算 ^TWII 並僅存本地

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "^TWII",
    "use_local_db": true
  }'
```

**回應：**
```json
{
  "success": true,
  "total_written": 250,
  "symbols": [
    {
      "symbol": "^TWII",
      "written": 250
    }
  ]
}
```

#### 範例 1-2：計算 ^TWII 並僅存 Neon

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "^TWII",
    "use_local_db": false
  }'
```

**回應：**
```json
{
  "success": true,
  "total_written": 250,
  "symbols": [
    {
      "symbol": "^TWII",
      "written": 250
    }
  ]
}
```

#### 範例 1-3：計算 ^TWII 並同時存兩邊

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "^TWII",
    "use_local_db": true,
    "upload_to_neon": true
  }'
```

**回應：**
```json
{
  "success": true,
  "total_written": 250,
  "total_written_neon": 250,
  "symbols": [
    {
      "symbol": "^TWII",
      "written": 250,
      "written_neon": 250
    }
  ]
}
```

#### 範例 2：批量計算所有股票（限制 10 檔）

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "all": true,
    "limit": 10,
    "use_local_db": true,
    "upload_to_neon": true
  }'
```

#### 範例 3：指定日期範圍

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "2330.TW",
    "start": "2024-01-01",
    "end": "2024-12-31",
    "use_local_db": true,
    "upload_to_neon": true
  }'
```

#### 範例 4：僅填補缺失的報酬率

```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "2330.TW",
    "fill_missing": true,
    "use_local_db": true,
    "upload_to_neon": true
  }'
```

## Python 程式碼範例

### 使用 requests 庫

```python
import requests

# API 端點
url = "http://localhost:5003/api/returns/compute"

# 請求數據
data = {
    "symbol": "2330.TW",
    "use_local_db": True,
    "upload_to_neon": True
}

# 發送請求
response = requests.post(url, json=data)
result = response.json()

if result['success']:
    print(f"✅ 成功處理 {result['total_written']} 筆報酬率")
    print(f"☁️ 上傳到 Neon: {result.get('total_written_neon', 0)} 筆")
else:
    print(f"❌ 錯誤: {result['error']}")
```

### 批量處理多個股票

```python
import requests

symbols = ['2330.TW', '2317.TW', '2454.TW', '2881.TW']

for symbol in symbols:
    print(f"處理 {symbol}...")
    
    response = requests.post(
        "http://localhost:5003/api/returns/compute",
        json={
            "symbol": symbol,
            "use_local_db": True,
            "upload_to_neon": True
        }
    )
    
    result = response.json()
    if result['success']:
        stats = result['symbols'][0]
        print(f"  本地: {stats['written']} 筆")
        print(f"  Neon: {stats.get('written_neon', 0)} 筆")
    else:
        print(f"  錯誤: {result['error']}")
```

## 注意事項

1. **網路連線**：上傳到 Neon 需要網路連線
2. **執行時間**：同時上傳會增加處理時間
3. **錯誤處理**：如果 Neon 上傳失敗，本地資料庫仍會成功寫入
4. **重複資料**：使用 UPSERT 機制，重複資料會自動更新

## 日誌範例

成功上傳時的日誌：
```
INFO:returns_calc:☁️ 2330.TW 報酬率已上傳到 Neon: 250 筆
INFO:returns_calc:☁️ ^TWII 報酬率已上傳到 Neon: 19 筆
```

上傳失敗時的日誌：
```
ERROR:returns_calc:上傳 2330.TW 報酬率到 Neon 失敗: connection timeout
```

## 驗證上傳結果

### 在 Neon 資料庫中查詢

```sql
-- 查看已上傳的報酬率數量
SELECT symbol, COUNT(*) as count, MIN(date) as start_date, MAX(date) as end_date
FROM stock_returns
WHERE symbol = '2330.TW'
GROUP BY symbol;

-- 查看最新的報酬率數據
SELECT * FROM stock_returns
WHERE symbol = '2330.TW'
ORDER BY date DESC
LIMIT 10;
```

## 常見問題

### Q1: 如何確認資料已上傳到 Neon？

**A:** 檢查 API 回應中的 `total_written_neon` 欄位，或直接在 Neon 資料庫中查詢。

### Q2: 上傳失敗會影響本地資料庫嗎？

**A:** 不會。本地和 Neon 的寫入是獨立的，Neon 失敗不影響本地。

### Q3: 可以只上傳到 Neon 而不存本地嗎？

**A:** 可以！設定 `use_local_db: false`（不需要設 upload_to_neon），系統會**僅**使用 Neon 資料庫：

```json
{
  "symbol": "2330.TW",
  "use_local_db": false
}
```

這樣會從 Neon 讀取股價，計算後存回 Neon，完全不碰本地資料庫。

### Q4: 如何批量上傳所有股票的報酬率？

**A:** 使用 `all: true` 參數：
```bash
curl -X POST http://localhost:5003/api/returns/compute \
  -H "Content-Type: application/json" \
  -d '{
    "all": true,
    "use_local_db": true,
    "upload_to_neon": true
  }'
```

## 效能建議

- 單一股票上傳：約 1-2 秒
- 批量上傳（10 檔）：約 10-20 秒
- 建議批量上傳時使用 `limit` 參數分批處理
- 如果股價數據很多，考慮使用 `fill_missing` 僅上傳新數據
