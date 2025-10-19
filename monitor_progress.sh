#!/bin/bash
# 即時監控批次抓取進度

echo "========================================="
echo "批次抓取即時監控（每30秒更新）"
echo "按 Ctrl+C 停止監控"
echo "========================================="
echo ""

while true; do
    clear
    echo "========================================="
    echo "台灣股票數據批次抓取 - 即時監控"
    echo "時間: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================="
    echo ""
    
    # 檢查進程
    if pgrep -f batch_fetch.py > /dev/null; then
        echo "狀態: ✓ 批次抓取進程正在運行"
    else
        echo "狀態: ✗ 批次抓取進程未運行"
    fi
    
    echo ""
    echo "資料庫統計："
    echo "-----------------------------------------"
    
    # 查詢資料庫統計
    PGPASSWORD='s8304021' psql -h localhost -U postgres -d postgres -c "
    SELECT 
        COUNT(DISTINCT symbol) as 已抓取股票數,
        COUNT(*) as 總記錄數,
        MIN(date) as 最早日期,
        MAX(date) as 最新日期
    FROM stock_prices;
    " 2>/dev/null
    
    echo ""
    echo "最近10檔已更新的股票："
    echo "-----------------------------------------"
    PGPASSWORD='s8304021' psql -h localhost -U postgres -d postgres -c "
    SELECT 
        symbol as 股票代碼,
        COUNT(*) as 記錄數,
        MIN(date) as 起始日,
        MAX(date) as 結束日
    FROM stock_prices 
    GROUP BY symbol 
    ORDER BY MAX(created_at) DESC 
    LIMIT 10;
    " 2>/dev/null
    
    echo ""
    echo "========================================="
    echo "下次更新: 30秒後"
    echo "========================================="
    
    sleep 30
done
