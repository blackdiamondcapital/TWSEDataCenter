# 🚀 GitHub Actions 快速開始指南

## 📍 你現在在這裡

你已經成功推送代碼到 GitHub！現在需要完成最後的設定步驟。

---

## ⚙️ Step 1: 設定 Secret（必須完成！）

### 1.1 前往 Settings

在你的 GitHub 倉庫頁面：
```
https://github.com/blackdiamondcapital/TWSEDataCenter
```

點擊頂部的 **Settings** 標籤

### 1.2 進入 Secrets 設定

左側選單：
1. 找到 **Secrets and variables**
2. 點擊展開
3. 選擇 **Actions**

### 1.3 新增 Secret

1. 點擊右上角綠色按鈕：**New repository secret**

2. 填寫資料：
   ```
   Name: NEON_DATABASE_URL
   
   Secret: postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=require
   ```

3. 點擊 **Add secret**

✅ 完成後你會看到一個名為 `NEON_DATABASE_URL` 的 Secret

---

## 🧪 Step 2: 測試執行

### 2.1 前往 Actions 頁面

點擊頂部的 **Actions** 標籤

### 2.2 你會看到兩個 Workflows

- 📊 **Daily Stock Data Fetch** - 每日抓取
- 🔄 **Weekly Full Refresh** - 每週回補

### 2.3 手動執行測試

1. 點擊 **Daily Stock Data Fetch**

2. 右側會出現 **Run workflow** 按鈕（藍色）

3. 點擊 **Run workflow**

4. 在彈出的對話框中：
   - Branch: 選擇 `main`
   - 點擊綠色的 **Run workflow** 按鈕

### 2.4 查看執行結果

1. 頁面會自動刷新，顯示正在執行的 workflow

2. 點擊正在執行的 workflow 名稱

3. 點擊 **fetch-stock-data** job

4. 展開各個步驟查看詳細日誌

---

## ✅ 成功的標誌

如果看到以下訊息，表示成功：

```
🚀 開始執行 GitHub Actions 股價資料抓取任務
================================================================================
⏰ 執行時間：2025-10-25 15:30:00
📅 目標日期：2025-10-25

✅ 已連接到 Neon 資料庫

📊 開始抓取上市股票資料 (TWSE)...
✅ 上市股票：成功抓取 XXXX 筆資料

📊 開始抓取上櫃股票資料 (TPEX)...
✅ 上櫃股票：成功抓取 XXXX 筆資料

📦 準備同步 XXXX 筆資料到 Neon 資料庫...
✅ 成功同步 XXXX 筆資料到 Neon

================================================================================
🎉 GitHub Actions 任務執行完成！
================================================================================
```

---

## ⚠️ 如果失敗

### 常見錯誤 1：Secret 未設定

```
❌ 錯誤：未設定 NEON_DATABASE_URL
```

**解決方案：** 回到 Step 1 設定 Secret

### 常見錯誤 2：非交易日

```
⚠️ 今日無資料可同步（可能是非交易日或資料尚未公布）
```

**解決方案：** 這是正常的！週末和假日不會有資料

### 常見錯誤 3：資料庫連線失敗

```
❌ 資料庫連線失敗
```

**解決方案：** 
1. 檢查 Secret 的值是否正確
2. 確認 Neon 資料庫是否在線

---

## 📅 自動排程

設定完成後，系統會自動執行：

- **每日抓取**：週一至週五 15:30（台北時間）
- **每週回補**：週日 02:00（台北時間）

你不需要做任何事，GitHub Actions 會自動運行！

---

## 📊 監控執行狀態

### 查看歷史記錄

1. 前往 **Actions** 頁面
2. 查看所有執行記錄
3. 綠色勾號 ✅ = 成功
4. 紅色叉號 ❌ = 失敗

### Email 通知

GitHub 會自動發送 Email 通知：
- ✅ 執行成功（可選）
- ❌ 執行失敗（預設開啟）

---

## 🎯 完整流程圖

```
1. 設定 Secret (NEON_DATABASE_URL)
   ↓
2. 前往 Actions 頁面
   ↓
3. 選擇 "Daily Stock Data Fetch"
   ↓
4. 點擊 "Run workflow"
   ↓
5. 選擇 main 分支
   ↓
6. 點擊綠色 "Run workflow" 按鈕
   ↓
7. 等待執行完成（約 3-5 分鐘）
   ↓
8. 查看執行日誌
   ↓
9. 確認資料已同步到 Neon
```

---

## 🔍 驗證資料

### 方法 1：查看 Neon 資料庫

```sql
-- 連接到 Neon 資料庫
psql "$NEON_DATABASE_URL"

-- 查看今日資料
SELECT COUNT(*) FROM tw_stock_prices WHERE date = CURRENT_DATE;

-- 查看最新資料
SELECT * FROM tw_stock_prices 
ORDER BY date DESC, symbol 
LIMIT 10;
```

### 方法 2：查看 Actions 日誌

在 Actions 頁面的執行日誌中會顯示同步的資料筆數。

---

## 📞 需要協助？

### 檢查清單

- [ ] Secret 已設定（NEON_DATABASE_URL）
- [ ] Workflow 已手動執行測試
- [ ] 執行日誌顯示成功
- [ ] Neon 資料庫有新資料

### 如果還是有問題

1. 截圖錯誤訊息
2. 查看完整的執行日誌
3. 檢查 Neon 資料庫連線狀態

---

## 🎉 恭喜！

完成以上步驟後，你的股價資料抓取系統就會：

- ✅ 每天自動抓取股價資料
- ✅ 自動同步到 Neon 資料庫
- ✅ 電腦不開機也能運行
- ✅ 完全免費使用

**開始設定吧！** 🚀
