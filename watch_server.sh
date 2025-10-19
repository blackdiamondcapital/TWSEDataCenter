#!/bin/bash
# 即時監控後端處理進度

echo "========================================="
echo "後端 API 處理進度監控"
echo "按 Ctrl+C 停止"
echo "========================================="
echo ""

# 找到 server.py 的進程並查看其輸出
# 如果在終端機中執行的，可以看到即時日誌

tail -f <(ps aux | grep "python.*server.py" | grep -v grep | awk '{print "/proc/"$2"/fd/1"}' 2>/dev/null) 2>/dev/null || \
echo "提示：請在執行 server.py 的終端機視窗查看即時日誌"
echo ""
echo "或者執行以下指令查看資料庫變化："
echo ""
echo 'watch -n 5 "PGPASSWORD=s8304021 psql -h localhost -U postgres -d postgres -c \"SELECT COUNT(*) FROM stock_prices;\""'
