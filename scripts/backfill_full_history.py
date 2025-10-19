#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""抓取每檔股票從 2010-01-01 到最近交易日的完整歷史數據"""

import argparse
import sys
import time
from datetime import datetime, date
from typing import Iterable, List

import requests

API_BASE = "http://localhost:5003"
DEFAULT_START = "2010-01-01"


def chunk_list(items: List[str], size: int) -> Iterable[List[str]]:
    """Yield successive chunks from list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_symbols(suffix_filter: str = None) -> List[str]:
    """Fetch all symbols from backend."""
    resp = requests.get(f"{API_BASE}/api/symbols", timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(payload.get("error", "未知錯誤：symbols API 失敗"))
    symbols = [s["symbol"] for s in payload.get("data", []) if s.get("symbol")]
    
    if suffix_filter:
        return [s for s in symbols if s.endswith(suffix_filter)]
    return symbols


def post_update(
    symbols: List[str],
    start: str,
    end: str,
    timeout: int,
    force_full_refresh: bool = False,
) -> dict:
    """Call backend update API for specified symbols and date range."""
    body = {
        "symbols": symbols,
        "start_date": start,
        "end_date": end,
        "update_prices": True,
        "force_full_refresh": force_full_refresh,
        "force_start_date": start if force_full_refresh else None,
    }
    resp = requests.post(
        f"{API_BASE}/api/update",
        json=body,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def update_stocks(symbols: List[str], args: argparse.Namespace) -> None:
    """逐檔或分批更新股票數據"""
    total = len(symbols)
    if total == 0:
        print("無符合的股票代碼，略過")
        return

    end_date = args.end or date.today().strftime("%Y-%m-%d")
    print(f"共 {total} 檔股票")
    print(f"日期範圍: {args.start} 至 {end_date}")
    print(f"批次設定: 每批 {args.chunk_size} 檔，間隔 {args.pause}s，逾時 {args.timeout}s\n")

    success_count = 0
    failed_count = 0
    skipped_count = 0
    total_records = 0

    for batch_idx, batch in enumerate(chunk_list(symbols, args.chunk_size), start=1):
        batch_size = len(batch)
        total_batches = (total + args.chunk_size - 1) // args.chunk_size
        
        print(f"[批次 {batch_idx}/{total_batches}] 處理 {batch[0]} .. {batch[-1]} ({batch_size} 檔)")
        
        try:
            result = post_update(
                batch,
                args.start,
                end_date,
                args.timeout,
                force_full_refresh=args.force_full_refresh,
            )
            
            if result.get("success"):
                results = result.get("results", [])
                errors = result.get("errors", [])
                
                # 統計本批次結果
                batch_success = 0
                batch_failed = 0
                batch_skipped = 0
                batch_records = 0
                
                for r in results:
                    symbol = r.get("symbol", "")
                    status = r.get("status", "")
                    price_records = r.get("price_records", 0)
                    duplicate_records = r.get("duplicate_records", 0)
                    
                    if status == "success" and price_records > 0:
                        batch_success += 1
                        batch_records += price_records
                        date_range = r.get("price_date_range", {})
                        if date_range:
                            print(f"  ✓ {symbol}: {price_records} 筆 (重複{duplicate_records}筆) "
                                  f"[{date_range.get('start')} ~ {date_range.get('end')}]")
                        else:
                            print(f"  ✓ {symbol}: {price_records} 筆 (重複{duplicate_records}筆)")
                    elif price_records == 0 and duplicate_records > 0:
                        batch_skipped += 1
                        existing_records = r.get("existing_records")
                        if existing_records is not None:
                            print(
                                f"  ⊘ {symbol}: 無新資料（資料庫已是最新，重複{duplicate_records}筆，"
                                f"現有總筆數 {existing_records}）"
                            )
                        else:
                            print(
                                f"  ⊘ {symbol}: 無新資料（資料庫已是最新，重複{duplicate_records}筆）"
                            )
                    elif status == "partial" or price_records == 0:
                        batch_skipped += 1
                        print(f"  ⊘ {symbol}: 無數據（可能尚未上市、已下市或無對應資料）")
                    else:
                        batch_failed += 1
                        print(f"  ✗ {symbol}: 狀態 {status}")
                
                # 處理錯誤
                for e in errors:
                    batch_failed += 1
                    symbol = e.get("symbol", "")
                    error = e.get("error", "未知錯誤")
                    print(f"  ✗ {symbol}: {error}")
                
                success_count += batch_success
                failed_count += batch_failed
                skipped_count += batch_skipped
                total_records += batch_records
                
                summary = result.get("summary", {})
                print(f"  批次小計: 成功 {batch_success}, 無數據 {batch_skipped}, 失敗 {batch_failed}, "
                      f"寫入 {batch_records} 筆")
                print(f"  累計: 成功 {success_count}/{total}, 總寫入 {total_records} 筆\n")
                
            else:
                failed_count += batch_size
                print(f"  ✗ 批次失敗：{result.get('error', '未知錯誤')}\n")
                
        except requests.HTTPError as http_err:
            failed_count += batch_size
            print(f"  ✗ HTTP 錯誤：{http_err}\n")
        except requests.RequestException as req_err:
            failed_count += batch_size
            print(f"  ✗ 請求錯誤：{req_err}\n")
        except Exception as err:
            failed_count += batch_size
            print(f"  ✗ 未預期錯誤：{err}\n")

        # 批次間延遲
        if batch_idx * args.chunk_size < total:
            time.sleep(args.pause)

    print("=" * 80)
    print(f"完成統計:")
    print(f"  總股票數: {total}")
    print(f"  成功: {success_count} 檔")
    print(f"  無數據: {skipped_count} 檔")
    print(f"  失敗: {failed_count} 檔")
    print(f"  總寫入記錄: {total_records} 筆")
    print("=" * 80)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取每檔股票從 2010-01-01 到最近交易日的完整歷史數據"
    )
    parser.add_argument(
        "--start", 
        default=DEFAULT_START, 
        help="開始日期 (YYYY-MM-DD)，預設 2010-01-01"
    )
    parser.add_argument(
        "--end", 
        default=None, 
        help="結束日期 (YYYY-MM-DD)，預設今天"
    )
    parser.add_argument(
        "--chunk-size", 
        type=int, 
        default=3, 
        help="每批處理的股票數，預設 3（因為時間跨度長）"
    )
    parser.add_argument(
        "--pause", 
        type=float, 
        default=5.0, 
        help="批次間隔秒數，預設 5"
    )
    parser.add_argument(
        "--timeout", 
        type=int, 
        default=900, 
        help="單批請求逾時秒數，預設 900（15分鐘）"
    )
    parser.add_argument(
        "--market",
        choices=["tw", "two", "all"],
        default="all",
        help="選擇市場：tw=上市, two=上櫃, all=全部",
    )
    parser.add_argument(
        "--force-full-refresh",
        action="store_true",
        help="強制重新抓取所有日期（忽略資料庫既有最新日期）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制處理前 N 檔（測試用）",
    )
    parser.add_argument(
        "--reverse-order",
        action="store_true",
        help="將股票清單反向處理（從尾端開始）",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    # 驗證日期格式
    try:
        datetime.strptime(args.start, "%Y-%m-%d")
        if args.end:
            datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式錯誤，需要 YYYY-MM-DD 格式")
        return 1

    # 取得股票清單
    try:
        print("正在取得股票清單...")
        
        if args.market == "tw":
            symbols = fetch_symbols(".TW")
            print(f"找到 {len(symbols)} 檔上市股票\n")
        elif args.market == "two":
            symbols = fetch_symbols(".TWO")
            print(f"找到 {len(symbols)} 檔上櫃股票\n")
        else:  # all
            tw_symbols = fetch_symbols(".TW")
            two_symbols = fetch_symbols(".TWO")
            symbols = tw_symbols + two_symbols
            print(f"找到 {len(tw_symbols)} 檔上市股票")
            print(f"找到 {len(two_symbols)} 檔上櫃股票")
            print(f"總計 {len(symbols)} 檔\n")
            
    except Exception as exc:
        print(f"取得股票清單失敗：{exc}")
        return 1

    if args.reverse_order:
        symbols = list(reversed(symbols))
        print("[反向模式] 將從清單尾端開始處理\n")

    # 限制數量（測試用）
    if args.limit is not None:
        symbols = symbols[: args.limit]
        print(f"[測試模式] 僅處理前 {len(symbols)} 檔\n")

    # 執行更新
    update_stocks(symbols, args)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
