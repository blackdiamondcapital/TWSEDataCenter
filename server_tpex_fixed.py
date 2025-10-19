#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正版：使用櫃買中心傳統API獲取正確的歷史數據
"""

import requests
import logging
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

def fetch_tpex_stock_data_fixed(stock_code, start_date, end_date):
    """
    使用櫃買中心傳統API獲取上櫃股票數據（提供正確的歷史數據）
    """
    try:
        # 將日期轉換為datetime
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        result = []
        current_date = start_dt
        
        # 使用櫃買中心傳統API
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.tpex.org.tw/'
        }
        
        logger.info(f"使用櫃買中心傳統API抓取 {stock_code}，日期範圍: {start_date} ~ {end_date}")
        
        # 計算總天數
        total_days = (end_dt - start_dt).days + 1
        processed_days = 0
        success_count = 0
        
        # 逐日獲取數據
        while current_date <= end_dt:
            # 跳過週末（TPEX 非交易日）
            if current_date.weekday() >= 5:  # 5=週六, 6=週日
                current_date = current_date + timedelta(days=1)
                processed_days += 1
                continue
            
            # 轉換為民國年格式 YYY/MM/DD（傳統API使用斜線）
            roc_date = f"{current_date.year - 1911:03d}/{current_date.month:02d}/{current_date.day:02d}"
            
            url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
            params = {
                'l': 'zh-tw',
                'd': roc_date,
                'se': 'AL'  # 全部股票
            }
            
            # 添加重試機制
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('stat') == 'ok' and 'tables' in data:
                            tables = data['tables']
                            found_stock = False
                            
                            # 搜尋所有表格中的目標股票
                            for table in tables:
                                if 'data' in table:
                                    table_data = table['data']
                                    
                                    # 查找目標股票
                                    for row in table_data:
                                        if (isinstance(row, list) and len(row) >= 8 and 
                                            row[0] == stock_code):
                                            
                                            found_stock = True
                                            date_str = current_date.strftime('%Y-%m-%d')
                                            
                                            try:
                                                # 解析價格數據（傳統API格式）
                                                # row[0]=代碼, row[1]=名稱, row[2]=收盤, row[3]=漲跌
                                                # row[4]=開盤, row[5]=最高, row[6]=最低, row[7]=成交量
                                                
                                                close_str = str(row[2]).replace(',', '').strip()
                                                open_str = str(row[4]).replace(',', '').strip()
                                                high_str = str(row[5]).replace(',', '').strip()
                                                low_str = str(row[6]).replace(',', '').strip()
                                                volume_str = str(row[7]).replace(',', '').strip()
                                                
                                                # 檢查是否為有效數據（避免 "----" 等無效值）
                                                if (close_str not in ['----', '---', '', '0'] and 
                                                    volume_str not in ['0', '', '----']):
                                                    
                                                    close_price = float(close_str)
                                                    open_price = float(open_str) if open_str not in ['----', '---', ''] else None
                                                    high_price = float(high_str) if high_str not in ['----', '---', ''] else None
                                                    low_price = float(low_str) if low_str not in ['----', '---', ''] else None
                                                    volume = int(volume_str)
                                                    
                                                    if close_price > 0 and volume > 0:
                                                        result.append({
                                                            'ticker': f"{stock_code}.TWO",
                                                            'Date': date_str,
                                                            'Open': round(open_price, 2) if open_price else None,
                                                            'High': round(high_price, 2) if high_price else None,
                                                            'Low': round(low_price, 2) if low_price else None,
                                                            'Close': round(close_price, 2),
                                                            'Volume': volume
                                                        })
                                                        success_count += 1
                                                        logger.debug(f"✓ {stock_code} {date_str}: {close_price} (Vol: {volume:,})")
                                                    
                                            except (ValueError, IndexError) as e:
                                                logger.warning(f"解析 {stock_code} {date_str} 數據失敗: {e}, row: {row}")
                                            
                                            break  # 找到股票後跳出內層循環
                                    
                                    if found_stock:
                                        break  # 找到股票後跳出表格循環
                            
                            if not found_stock:
                                logger.debug(f"- {stock_code} {current_date.strftime('%Y-%m-%d')}: 無交易或未找到")
                            
                            success = True
                        else:
                            logger.warning(f"櫃買中心傳統API 回應狀態異常: {data.get('stat')}")
                            success = True  # 避免重試
                    else:
                        logger.warning(f"櫃買中心傳統API HTTP {response.status_code}，日期: {current_date.strftime('%Y-%m-%d')}")
                        success = True  # 跳過此日期
                        
                except requests.exceptions.Timeout:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"櫃買中心傳統API 超時，第 {retry_count} 次重試: {current_date.strftime('%Y-%m-%d')}")
                        time.sleep(2)
                    else:
                        logger.error(f"櫃買中心傳統API 超時，已達最大重試次數: {current_date.strftime('%Y-%m-%d')}")
                        break
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"櫃買中心傳統API 請求失敗: {e}，第 {retry_count} 次重試")
                        time.sleep(2)
                    else:
                        logger.error(f"櫃買中心傳統API 請求失敗: {e}，已達最大重試次數")
                        break
            
            # 移動到下一天
            current_date = current_date + timedelta(days=1)
            processed_days += 1
            
            # 進度提示（每10天）
            if processed_days % 10 == 0:
                logger.info(f"櫃買中心傳統API {stock_code} 進度: {processed_days}/{total_days} 天，成功 {success_count} 筆")
            
            # 避免請求過於頻繁（每秒最多2次）
            time.sleep(0.5)
        
        # 按日期排序（不需要去重，因為傳統API提供正確的歷史數據）
        if result:
            result.sort(key=lambda x: x['Date'])
            logger.info(f"✅ 成功從櫃買中心傳統API獲取 {stock_code} 數據，共 {len(result)} 筆")
        else:
            logger.warning(f"⚠️ 櫃買中心傳統API {stock_code} 在指定期間({start_date} ~ {end_date})沒有抓到任何資料")
        
        return result
        
    except Exception as e:
        logger.error(f"從櫃買中心傳統API獲取 {stock_code} 數據失敗: {e}")
        import traceback
        traceback.print_exc()
        return None

# 測試函數
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # 測試抓取
    result = fetch_tpex_stock_data_fixed('1565', '2025-10-15', '2025-10-17')
    
    if result:
        print(f"\n📊 抓取結果 ({len(result)} 筆):")
        for r in result:
            print(f"  {r['Date']}: O={r['Open']} H={r['High']} L={r['Low']} C={r['Close']} V={r['Volume']:,}")
    else:
        print("❌ 抓取失敗")
