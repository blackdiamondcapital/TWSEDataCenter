#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Force-refresh Taiwan stock price data from 2010 onward.

This script hits the backend `/api/update` endpoint in batches with
`force_full_refresh=True` so each symbol is re-downloaded starting from
2010-01-01 up to today.

Usage examples:
    python3 full_refresh_2010.py                # default settings
    python3 full_refresh_2010.py --batch-size 5 # smaller batches
    python3 full_refresh_2010.py --start-index 50 --limit 100  # partial run
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from datetime import datetime
from typing import Iterable, List

import requests

API_BASE = "http://localhost:5003"
DEFAULT_START_DATE = "2010-01-01"
BATCH_SIZE_DEFAULT = 10
REQUEST_TIMEOUT = 900  # seconds
SLEEP_BETWEEN_BATCHES = 3  # seconds


def fetch_all_symbols() -> List[str]:
    """Retrieve the complete symbol list from the backend."""
    resp = requests.get(f"{API_BASE}/api/symbols", timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    symbols = [item["symbol"] for item in payload.get("data", [])]
    if not symbols:
        raise RuntimeError("後端回傳的股票清單為空，無法進行回補")
    return symbols


def batched(iterable: Iterable[str], size: int) -> Iterable[List[str]]:
    """Yield lists of length <= size from iterable."""
    batch: List[str] = []
    for value in iterable:
        batch.append(value)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def run_batch(batch_symbols: List[str], batch_idx: int, total_batches: int) -> bool:
    """Send one /api/update request for the given batch of symbols."""
    print("=" * 70)
    print(f"📦 批次 {batch_idx}/{total_batches} | 股票數: {len(batch_symbols)}")
    preview = ", ".join(batch_symbols[:5])
    if len(batch_symbols) > 5:
        preview += "..."
    print(f"📋 股票: {preview}")
    print("=" * 70)

    payload = {
        "symbols": batch_symbols,
        "start_date": DEFAULT_START_DATE,
        "end_date": datetime.now().strftime("%Y-%m-%d"),
        "update_prices": True,
        "force_full_refresh": True,
        "force_start_date": DEFAULT_START_DATE,
    }

    try:
        print(f"⏳ 發送請求至後端 API...")
        start_time = time.time()
        resp = requests.post(
            f"{API_BASE}/api/update",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        elapsed = time.time() - start_time
        resp.raise_for_status()
        result = resp.json()
        
        if not result.get("success", False):
            print("✗ 批次失敗: 後端回傳 success=false")
            print(result)
            return False

        # 顯示每一檔股票的詳細資訊
        results = result.get("results", [])
        errors = result.get("errors", [])
        
        print(f"\n⏱️  API 回應時間: {elapsed:.1f} 秒")
        print(f"\n📊 各股票抓取詳情:")
        print("-" * 70)
        
        for i, stock_result in enumerate(results, 1):
            symbol = stock_result.get("symbol", "未知")
            status = stock_result.get("status", "unknown")
            
            # 取得記錄數資訊
            new_records = stock_result.get("price_records", 0)
            duplicate_records = stock_result.get("duplicate_records", 0)
            existing_records = stock_result.get("existing_records", 0)
            
            # 取得日期範圍
            date_range = stock_result.get("price_date_range", {})
            start_date = date_range.get("start", "N/A")
            end_date = date_range.get("end", "N/A")
            trading_days = date_range.get("trading_days_count", 0)
            
            # 狀態圖示
            status_icon = "✓" if status == "success" else "⚠️"
            
            # 顯示股票資訊
            print(f"{status_icon} [{i}/{len(results)}] {symbol}")
            print(f"   └─ 新增: {new_records:>4} 筆 | 重複: {duplicate_records:>4} 筆 | 總計: {existing_records:>4} 筆")
            
            if trading_days > 0:
                print(f"   └─ 日期: {start_date} ~ {end_date} ({trading_days} 個交易日)")
            else:
                print(f"   └─ 無新數據")
        
        print("-" * 70)
        
        # 批次摘要
        total_new = sum(r.get("price_records", 0) for r in results)
        total_duplicate = sum(r.get("duplicate_records", 0) for r in results)
        
        print(f"\n✅ 批次完成 | 成功: {len(results)} 檔")
        print(f"   總新增記錄: {total_new:,} 筆")
        print(f"   總重複記錄: {total_duplicate:,} 筆")
        
        if errors:
            print(f"\n❌ 失敗: {len(errors)} 檔")
            for error in errors:
                print(f"   - {error}")
            return False
        
        return True
        
    except requests.exceptions.RequestException as exc:
        print(f"\n✗ 批次失敗 (HTTP 例外): {exc}")
        return False
    except ValueError as exc:
        print(f"\n✗ 批次失敗 (JSON 解析錯誤): {exc}")
        if 'resp' in locals():
            print(f"回應內容: {resp.text[:500]}")
        return False
    except Exception as exc:
        print(f"\n✗ 批次失敗 (未預期錯誤): {exc}")
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="強制回補 2010 年起的股票數據")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE_DEFAULT,
        help=f"每批處理的股票數量 (預設 {BATCH_SIZE_DEFAULT})",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="從第幾檔股票開始 (用於續跑)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="最多處理多少檔股票 (預設全部)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        symbols = fetch_all_symbols()
    except Exception as exc:  # noqa: BLE001 - 顯示明確錯誤
        print(f"✗ 無法取得股票代碼列表: {exc}")
        return 1

    if args.start_index >= len(symbols):
        print("✗ start-index 大於股票清單長度")
        return 1

    symbols = symbols[args.start_index :]
    if args.limit is not None:
        symbols = symbols[: args.limit]

    total_symbols = len(symbols)
    if total_symbols == 0:
        print("✗ 沒有要處理的股票")
        return 1

    batches = list(batched(symbols, max(args.batch_size, 1)))
    total_batches = len(batches)

    print("=" * 70)
    print("強制回補台灣股票數據 (2010-01-01 起) 開始")
    print(f"總股票數: {total_symbols}")
    print(f"批次大小: {args.batch_size}")
    print(f"總批次: {total_batches}")
    print("=" * 70)

    start_ts = time.time()
    success_batches = 0
    failed_batches = 0
    total_new_records = 0
    total_processed_symbols = 0

    for idx, batch in enumerate(batches, start=1):
        batch_start = time.time()
        ok = run_batch(batch, idx, total_batches)
        batch_duration = time.time() - batch_start
        
        if ok:
            success_batches += 1
            total_processed_symbols += len(batch)
        else:
            failed_batches += 1
        
        # 進度統計
        completed_pct = (idx / total_batches) * 100
        elapsed = time.time() - start_ts
        avg_time_per_batch = elapsed / idx
        remaining_batches = total_batches - idx
        eta_seconds = remaining_batches * avg_time_per_batch
        
        print(f"\n📊 總進度: {idx}/{total_batches} ({completed_pct:.1f}%)")
        print(f"⏱️  已經過: {elapsed/60:.1f} 分鐘 | 預估剩餘: {eta_seconds/60:.1f} 分鐘")
        print(f"✅ 成功: {success_batches} 批 | ❌ 失敗: {failed_batches} 批")
        
        if idx < total_batches:
            print(f"\n⏸️  等待 {SLEEP_BETWEEN_BATCHES} 秒後進行下一批...\n")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    duration = time.time() - start_ts
    success_rate = (success_batches / total_batches * 100) if total_batches > 0 else 0
    
    print("\n" + "=" * 70)
    print("🎉 回補流程完成")
    print("=" * 70)
    print(f"📊 統計摘要:")
    print(f"   總批次數: {total_batches}")
    print(f"   成功批次: {success_batches} ({success_rate:.1f}%)")
    print(f"   失敗批次: {failed_batches}")
    print(f"   處理股票: {total_processed_symbols}/{total_symbols}")
    print(f"\n⏱️  時間統計:")
    print(f"   總耗時: {duration/60:.1f} 分鐘 ({duration/3600:.2f} 小時)")
    print(f"   平均每批: {duration/total_batches:.1f} 秒")
    if total_processed_symbols > 0:
        print(f"   平均每檔: {duration/total_processed_symbols:.1f} 秒")
    print("=" * 70)

    return 0 if failed_batches == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
