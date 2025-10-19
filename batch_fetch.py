#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批次抓取所有股票從2010年至今的數據
"""

import requests
import json
import time
from datetime import datetime

# 配置
API_BASE = "http://localhost:5003"
START_DATE = "2010-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')
BATCH_SIZE = 10  # 每批處理10檔股票，避免超時

def get_all_symbols():
    """獲取所有股票代碼"""
    response = requests.get(f"{API_BASE}/api/symbols")
    data = response.json()
    return [s['symbol'] for s in data['data']]

def update_batch(symbols_batch, batch_num, total_batches):
    """更新一批股票數據"""
    print(f"\n{'='*60}")
    print(f"處理第 {batch_num}/{total_batches} 批，共 {len(symbols_batch)} 檔股票")
    print(f"股票: {', '.join(symbols_batch[:5])}{'...' if len(symbols_batch) > 5 else ''}")
    print(f"{'='*60}")
    
    payload = {
        "symbols": symbols_batch,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "update_prices": True,
        "force_full_refresh": False
    }
    
    try:
        response = requests.post(
            f"{API_BASE}/api/update",
            json=payload,
            timeout=600  # 10分鐘超時
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✓ 批次完成")
            if result.get('success'):
                print(f"  成功處理的股票數: {len(result.get('results', []))}")
                if result.get('errors'):
                    print(f"  錯誤數: {len(result.get('errors'))}")
            return True
        else:
            print(f"✗ HTTP 錯誤: {response.status_code}")
            print(f"  回應: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"✗ 請求超時")
        return False
    except Exception as e:
        print(f"✗ 錯誤: {e}")
        return False

def main():
    print("="*60)
    print("開始批次抓取台灣股票數據")
    print(f"日期範圍: {START_DATE} 至 {END_DATE}")
    print("="*60)
    
    # 獲取所有股票代碼
    print("\n正在獲取股票代碼列表...")
    all_symbols = get_all_symbols()
    total = len(all_symbols)
    print(f"✓ 共找到 {total} 檔股票")
    
    # 分批處理
    batches = [all_symbols[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    total_batches = len(batches)
    
    print(f"\n將分成 {total_batches} 批次處理，每批 {BATCH_SIZE} 檔")
    print(f"預估總時間: {total_batches * 2} - {total_batches * 10} 分鐘")
    print("\n開始執行...")
    
    start_time = time.time()
    success_count = 0
    fail_count = 0
    
    for i, batch in enumerate(batches, 1):
        if update_batch(batch, i, total_batches):
            success_count += 1
        else:
            fail_count += 1
        
        # 批次間延遲，避免過載
        if i < total_batches:
            print("等待 3 秒後處理下一批...")
            time.sleep(3)
    
    # 統計
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print("批次抓取完成！")
    print(f"{'='*60}")
    print(f"總批次: {total_batches}")
    print(f"成功: {success_count}")
    print(f"失敗: {fail_count}")
    print(f"總耗時: {elapsed/60:.1f} 分鐘")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
