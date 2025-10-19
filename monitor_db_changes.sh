#!/bin/bash
# 即時監控資料庫記錄數變化

echo "========================================="
echo "資料庫即時變化監控"
echo "每 3 秒更新一次 | 按 Ctrl+C 停止"
echo "========================================="
echo ""

prev_count=0

while true; do
    current=$(PGPASSWORD='s8304021' psql -h localhost -U postgres -d postgres -t -c "SELECT COUNT(*) FROM stock_prices;" 2>/dev/null | xargs)
    
    if [ ! -z "$current" ]; then
        diff=$((current - prev_count))
        
        if [ $diff -gt 0 ]; then
            echo "[$(date '+%H:%M:%S')] 📊 總記錄數: $current (+$diff 新增)"
        else
            echo "[$(date '+%H:%M:%S')] 📊 總記錄數: $current"
        fi
        
        prev_count=$current
    else
        echo "[$(date '+%H:%M:%S')] ⚠️  無法連接資料庫"
    fi
    
    sleep 3
done
