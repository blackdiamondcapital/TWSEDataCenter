# StockReturnsService

一個將資料庫中 `tw_stock_prices` 的股價資料，轉換為「日、週、月、季、年」報酬率並寫入 `tw_stock_returns` 的服務。

## 特色
- 從 PostgreSQL 讀取 `tw_stock_prices(symbol, date, close_price, ...)`
- 計算下列報酬率並回寫至 `tw_stock_returns`（若無欄位會自動新增）
  - daily_return: 當日相對於前一個交易日的報酬
  - weekly_return: 5 個交易日報酬（rolling 5）
  - monthly_return: 21 個交易日報酬（rolling 21）
  - quarterly_return: 63 個交易日報酬（rolling 63）
  - yearly_return: 252 個交易日報酬（rolling 252）
  - cumulative_return: 以首筆收盤價為基準的累積報酬（連乘）
- 支援全市場或指定代號、日期範圍、以及「只補缺」模式

## 安裝
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 環境變數
可在系統環境或 `.env`（可自行新增）設定，程式亦有預設值。
- DB_HOST（預設 localhost）
- DB_PORT（預設 5432）
- DB_USER（預設 postgres）
- DB_PASSWORD（預設 s8304021）
- DB_NAME（預設 postgres）

## 使用方式
- 計算所有股票全部日期：
```bash
python main.py --all
```

- 指定單一代號與日期範圍：
```bash
python main.py --symbol 2330.TW --start 2023-01-01 --end 2024-12-31
```

- 只補缺（只針對尚未有 `tw_stock_returns` 的日期列進行計算）：
```bash
python main.py --all --fill-missing
```

## 計算說明
- 交易日視為資料庫 `tw_stock_prices` 內實際存在的日期（自動跳過非交易日）。
- 週/月/季/年報酬率以滾動交易日數計算：5/21/63/252。
- 累積報酬以 `(1+daily_return)` 的連乘計算。

## 注意
- 本服務假設 `tw_stock_prices` 中至少含有欄位：`symbol, date, close_price`。
- 若 `tw_stock_returns` 不存在或缺少欄位，程式會自動建立/補齊。
