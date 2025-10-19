#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
櫃買中心 API 修復腳本
測試並更新上櫃股票數據抓取邏輯
"""

import requests
import time
from datetime import datetime
import json

def test_tpex_monthly_api(stock_code, year, month):
    """測試櫃買中心月度 API"""
    # 新的API端點
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"
    
    params = {
        'l': 'zh-tw',
        'o': 'json',
        'd': f'{year-1911}/{month:02d}',  # 轉換為民國年
        'stkno': stock_code
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.tpex.org.tw/'
    }
    
    print(f"\n測試 {stock_code} {year}-{month:02d}")
    print(f"URL: {url}")
    print(f"Params: {params}")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {data.keys()}")
            
            if 'aaData' in data and data['aaData']:
                print(f"✅ 成功獲取 {len(data['aaData'])} 筆數據")
                print(f"示例數據：{data['aaData'][0]}")
                return True, data
            else:
                print(f"⚠️ 無數據: {data}")
                return False, data
        else:
            print(f"❌ HTTP 錯誤: {response.status_code}")
            return False, None
            
    except Exception as e:
        print(f"❌ 請求失敗: {e}")
        return False, None

def improved_fetch_tpex_stock_data(stock_code, start_date, end_date):
    """改進的櫃買中心數據抓取（月度查詢）"""
    from datetime import datetime, timedelta
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    result = []
    current_date = start_dt.replace(day=1)  # 從月初開始
    
    while current_date <= end_dt:
        year = current_date.year
        month = current_date.month
        
        # 使用月度 API
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php"
        params = {
            'l': 'zh-tw',
            'o': 'json',
            'd': f'{year-1911}/{month:02d}',
            'stkno': stock_code
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.tpex.org.tw/'
        }
        
        print(f"抓取 {stock_code} {year}-{month:02d}")
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'aaData' in data and data['aaData']:
                    for row in data['aaData']:
                        try:
                            # 資料格式: [日期, 收盤, 漲跌, 開盤, 最高, 最低, 成交量, ...]
                            date_str = row[0].strip()  # 格式: "113/10/01"
                            
                            # 轉換民國年為西元年
                            date_parts = date_str.split('/')
                            if len(date_parts) == 3:
                                year_roc = int(date_parts[0]) + 1911
                                month_val = int(date_parts[1])
                                day_val = int(date_parts[2])
                                
                                trade_date = datetime(year_roc, month_val, day_val)
                                
                                # 檢查是否在範圍內
                                if start_dt <= trade_date <= end_dt:
                                    close_price = float(row[1].replace(',', '')) if row[1] and row[1] != '---' else None
                                    open_price = float(row[3].replace(',', '')) if row[3] and row[3] != '---' else None
                                    high_price = float(row[4].replace(',', '')) if row[4] and row[4] != '---' else None
                                    low_price = float(row[5].replace(',', '')) if row[5] and row[5] != '---' else None
                                    volume = int(row[6].replace(',', '')) if row[6] and row[6] != '---' else 0
                                    
                                    if close_price is not None:
                                        result.append({
                                            'ticker': f"{stock_code}.TWO",
                                            'Date': trade_date.strftime('%Y-%m-%d'),
                                            'Open': round(open_price, 2) if open_price else None,
                                            'High': round(high_price, 2) if high_price else None,
                                            'Low': round(low_price, 2) if low_price else None,
                                            'Close': round(close_price, 2),
                                            'Volume': volume
                                        })
                        except Exception as e:
                            print(f"解析行失敗: {e}, row: {row}")
                            continue
        except Exception as e:
            print(f"請求失敗: {e}")
        
        # 移到下個月
        if month == 12:
            current_date = current_date.replace(year=year+1, month=1)
        else:
            current_date = current_date.replace(month=month+1)
        
        # 避免請求過快
        time.sleep(1)
    
    if result:
        result.sort(key=lambda x: x['Date'])
        print(f"✅ 成功獲取 {stock_code} 共 {len(result)} 筆")
    else:
        print(f"❌ {stock_code} 無數據")
    
    return result

if __name__ == '__main__':
    # 測試幾個上櫃股票
    test_stocks = [
        '6488',  # 環球晶
        '3529',  # 力旺
        '006201', # 元大富櫃50
    ]
    
    print("=" * 60)
    print("測試櫃買中心 API")
    print("=" * 60)
    
    for stock in test_stocks:
        success, data = test_tpex_monthly_api(stock, 2025, 10)
        time.sleep(2)  # 避免請求過快
    
    print("\n" + "=" * 60)
    print("測試改進的抓取函數")
    print("=" * 60)
    
    # 測試改進的函數
    result = improved_fetch_tpex_stock_data('6488', '2025-09-18', '2025-10-18')
    if result:
        print(f"\n成功示例:")
        for item in result[:3]:
            print(json.dumps(item, indent=2, ensure_ascii=False))
