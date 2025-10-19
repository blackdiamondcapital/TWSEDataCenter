#!/bin/bash
# 驗證資料庫數據完整性

echo "========================================="
echo "台灣股票數據完整性驗證報告"
echo "時間: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="
echo ""

PGPASSWORD='s8304021'

echo "1️⃣  總體統計"
echo "-----------------------------------------"
psql -h localhost -U postgres -d postgres -c "
SELECT 
    COUNT(DISTINCT symbol) as 股票總數,
    COUNT(*) as 記錄總數,
    MIN(date) as 最早日期,
    MAX(date) as 最新日期
FROM stock_prices;
"

echo ""
echo "2️⃣  按年份統計"
echo "-----------------------------------------"
psql -h localhost -U postgres -d postgres -c "
SELECT 
    EXTRACT(YEAR FROM date) as 年份,
    COUNT(*) as 記錄數,
    COUNT(DISTINCT symbol) as 股票數,
    COUNT(DISTINCT date) as 交易日數
FROM stock_prices 
GROUP BY EXTRACT(YEAR FROM date) 
ORDER BY 年份;
"

echo ""
echo "3️⃣  數據完整度最高的前10檔股票"
echo "-----------------------------------------"
psql -h localhost -U postgres -d postgres -c "
SELECT 
    symbol as 股票代碼,
    COUNT(*) as 記錄數,
    MIN(date) as 起始日,
    MAX(date) as 結束日,
    ROUND((MAX(date) - MIN(date))::numeric, 0) as 日期跨度
FROM stock_prices 
GROUP BY symbol 
ORDER BY COUNT(*) DESC 
LIMIT 10;
"

echo ""
echo "4️⃣  台積電(2330.TW)歷史數據範例"
echo "-----------------------------------------"
psql -h localhost -U postgres -d postgres -c "
SELECT 
    date as 日期,
    open_price as 開盤,
    high_price as 最高,
    low_price as 最低,
    close_price as 收盤,
    volume as 成交量
FROM stock_prices 
WHERE symbol = '2330.TW' 
    AND date >= '2010-01-04' 
    AND date <= '2010-01-15'
ORDER BY date;
"

echo ""
echo "5️⃣  2010年1月數據抽查（前10筆）"
echo "-----------------------------------------"
psql -h localhost -U postgres -d postgres -c "
SELECT 
    symbol as 股票,
    date as 日期,
    close_price as 收盤價,
    volume as 成交量
FROM stock_prices 
WHERE date >= '2010-01-04' AND date <= '2010-01-31'
ORDER BY date, symbol
LIMIT 10;
"

echo ""
echo "6️⃣  最新數據（最近寫入的20筆）"
echo "-----------------------------------------"
psql -h localhost -U postgres -d postgres -c "
SELECT 
    symbol as 股票,
    date as 日期,
    close_price as 收盤價,
    volume as 成交量,
    created_at as 寫入時間
FROM stock_prices 
ORDER BY created_at DESC 
LIMIT 20;
"

echo ""
echo "========================================="
echo "✅ 驗證完成"
echo "========================================="
