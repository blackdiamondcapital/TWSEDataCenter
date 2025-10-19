#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""檢查 Volume 欄位問題"""

import requests
from datetime import datetime

# 測試 API 返回的欄位
today = datetime.now()
roc_date = f"{today.year - 1911:03d}{today.month:02d}{today.day:02d}"

url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
params = {'date': roc_date}

response = requests.get(url, params=params, timeout=15)

if response.status_code == 200:
    data = response.json()
    
    # 找到 6488
    found = [item for item in data if item.get('SecuritiesCompanyCode') == '6488']
    
    if found:
        item = found[0]
        print("6488 完整數據:")
        print("=" * 60)
        for key, value in item.items():
            print(f"{key:30s}: {value}")
        
        print("\n" + "=" * 60)
        print("Volume 相關欄位:")
        print("=" * 60)
        
        # 檢查可能的 volume 欄位
        volume_fields = ['Volume', 'TradingShares', 'TradeVolume', 'TradingVolume', 
                        'SharesTraded', 'TotalVolume', '成交股數']
        
        for field in volume_fields:
            if field in item:
                print(f"✅ 找到: {field} = {item[field]}")
        
        # 測試解析
        print("\n" + "=" * 60)
        print("解析測試:")
        print("=" * 60)
        
        # 原始代碼
        volume1 = int(item.get('TradingShares', '0').replace(',', '')) if item.get('TradingShares') else 0
        print(f"使用 TradingShares: {volume1}")
        
        # 檢查實際的成交股數欄位
        if 'TradingShares' in item:
            raw = item['TradingShares']
            print(f"TradingShares 原始值: '{raw}' (type: {type(raw)})")
            try:
                cleaned = str(raw).replace(',', '').strip()
                print(f"清理後: '{cleaned}'")
                parsed = int(cleaned)
                print(f"解析為整數: {parsed}")
            except Exception as e:
                print(f"解析失敗: {e}")
