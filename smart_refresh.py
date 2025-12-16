#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""智能回補：優先處理缺資料的股票，並顯示詳細進度"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
import requests

API_BASE = "http://localhost:5003"
DEFAULT_START_DATE = "2010-01-01"
DEFAULT_START_YEAR = int(DEFAULT_START_DATE.split("-")[0])
REQUEST_TIMEOUT = 900
SLEEP_BETWEEN_STOCKS = 0.5  # 每檔股票間延遲

MIN_TRADING_DAYS_PAST_YEAR = 200
MIN_TRADING_DAYS_CURRENT_YEAR = 120

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 's8304021',
    'database': 'postgres'
}


def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def get_symbol_year_counts(conn: psycopg2.extensions.connection, symbol: str) -> Dict[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
                SELECT EXTRACT(YEAR FROM date) AS year, COUNT(*) AS count
                FROM tw_stock_prices
                WHERE symbol = %s
                GROUP BY EXTRACT(YEAR FROM date)
            """,
            (symbol,),
        )
        rows = cur.fetchall()

    return {int(row["year"]): row["count"] for row in rows}


def calculate_missing_year_ranges(
    symbol_info: Dict[str, Optional[datetime]],
    year_counts: Dict[int, int],
) -> List[Tuple[str, str]]:
    today = datetime.now().date()
    current_year = today.year

    earliest: Optional[datetime] = symbol_info.get("earliest")
    earliest_year = earliest.year if earliest else None
    total_records = sum(year_counts.values())

    # 智能判斷起始年份：
    # 1. 如果資料庫中記錄數 >= 1000 且 earliest_year > 2010，認為是真的晚上市
    # 2. 否則從 2010 年開始（避免因資料不完整而誤判）
    if earliest_year and earliest_year > DEFAULT_START_YEAR and total_records >= 1000:
        start_year = earliest_year
        print(f"   💡 確認股票晚於2010年上市，從 {earliest_year} 年開始")
    else:
        start_year = DEFAULT_START_YEAR
        if earliest_year and earliest_year > DEFAULT_START_YEAR and total_records < 1000:
            print(f"   ⚠️  資料庫僅有 {total_records} 筆，無法確定上市時間，從2010年開始完整抓取")

    missing_years: List[int] = []
    for year in range(start_year, current_year + 1):
        count = year_counts.get(year, 0)
        threshold = (
            MIN_TRADING_DAYS_CURRENT_YEAR if year == current_year else MIN_TRADING_DAYS_PAST_YEAR
        )
        if count < threshold:
            missing_years.append(year)

    if not missing_years:
        return []

    ranges: List[Tuple[int, int]] = []
    range_start = missing_years[0]
    prev_year = missing_years[0]
    for year in missing_years[1:]:
        if year == prev_year + 1:
            prev_year = year
            continue
        ranges.append((range_start, prev_year))
        range_start = year
        prev_year = year
    ranges.append((range_start, prev_year))

    fetch_ranges: List[Tuple[str, str]] = []
    for range_start_year, range_end_year in ranges:
        start_date = date(range_start_year, 1, 1)
        end_date = today if range_end_year == current_year else date(range_end_year, 12, 31)

        if earliest and range_start_year == earliest.year:
            earliest_date = earliest.date() if isinstance(earliest, datetime) else earliest
            start_date = max(start_date, earliest_date)

        if start_date > end_date:
            continue

        fetch_ranges.append((start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))

    return fetch_ranges


def detect_actual_listing_date(symbol: str) -> Optional[date]:
    """探測股票的實際上市日期（透過小範圍API查詢）"""
    try:
        # 先嘗試從2010年開始抓取最近3個月的資料來判斷
        test_payload = {
            "symbols": [symbol],
            "start_date": DEFAULT_START_DATE,
            "end_date": "2010-03-31",
            "update_prices": False,  # 不實際更新，只查詢
        }
        
        resp = requests.post(
            f"{API_BASE}/api/update",
            json=test_payload,
            timeout=30,
        )
        
        if resp.status_code == 200:
            result = resp.json()
            results = result.get("results", [])
            if results and results[0].get("price_records", 0) > 0:
                # 如果2010年有資料，就從2010年開始
                return datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d").date()
        
        # 如果2010年沒有資料，這支股票可能是後來才上市的
        # 返回None，讓程序依賴資料庫中的earliest或使用默認值
        return None
        
    except Exception:
        return None


def split_range_by_month(start_date_str: str, end_date_str: str) -> List[Tuple[str, str]]:
    """將日期區間按月份拆分"""
    start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    monthly_ranges = []
    current = start
    
    while current <= end:
        # 計算當月最後一天
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        
        month_end = next_month - timedelta(days=1)
        month_end = min(month_end, end)  # 不超過結束日期
        
        monthly_ranges.append((
            current.strftime("%Y-%m-%d"),
            month_end.strftime("%Y-%m-%d")
        ))
        
        current = next_month
    
    return monthly_ranges


def get_incomplete_symbols() -> List[dict]:
    """取得資料不完整的股票清單（優先順序排序）"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        WITH symbol_stats AS (
            SELECT
                symbol,
                COUNT(*) AS total_records,
                COUNT(DISTINCT EXTRACT(YEAR FROM date)) AS year_count,
                MIN(date) AS earliest,
                MAX(date) AS latest
            FROM tw_stock_prices
            GROUP BY symbol
        )
        SELECT
            symbol,
            total_records,
            year_count,
            earliest,
            latest
        FROM symbol_stats
        WHERE year_count < 16 OR total_records < 3000
        ORDER BY year_count ASC, total_records ASC
        """
    )

    results = [dict(record) for record in cur.fetchall()]
    cur.close()
    conn.close()
    return results


def fetch_single_stock(symbol: str, idx: int, total: int, verify: bool = False) -> dict:
    """抓取單一股票，顯示詳細進度"""
    print("\n" + "=" * 80)
    print(f"📊 [{idx}/{total}] 處理股票: {symbol}")
    print("=" * 80)
    conn = get_db_connection()
    year_counts = get_symbol_year_counts(conn, symbol)

    symbol_info = {
        "earliest": None,
        "latest": None,
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(date) AS earliest, MAX(date) AS latest
            FROM tw_stock_prices
            WHERE symbol = %s
            """,
            (symbol,)
        )
        row = cur.fetchone()
        if row:
            symbol_info["earliest"] = row["earliest"]
            symbol_info["latest"] = row["latest"]

    # 顯示現有資料範圍
    if symbol_info["earliest"] and symbol_info["latest"]:
        print(f"   📅 現有資料: {symbol_info['earliest']} ~ {symbol_info['latest']}")
        total_records = sum(year_counts.values())
        print(f"   📊 現有記錄: {total_records:,} 筆，涵蓋 {len(year_counts)} 年")
    else:
        print("   ℹ️  資料庫中無此股票資料")
        # 對於新股票，嘗試探測實際上市日期
        print("   🔍 檢查股票實際上市時間...")
        listing_date = detect_actual_listing_date(symbol)
        if listing_date and listing_date.year > DEFAULT_START_YEAR:
            # 更新symbol_info，讓後續邏輯使用探測到的日期
            symbol_info["earliest"] = datetime.combine(listing_date, datetime.min.time())
            print(f"   💡 偵測到股票約於 {listing_date.year} 年後上市")

    fetch_ranges = calculate_missing_year_ranges(symbol_info, year_counts)
    
    # 顯示缺失年份資訊（與實際抓取邏輯一致）
    if year_counts or fetch_ranges:
        today = datetime.now().date()
        current_year = today.year
        
        # 決定實際起始年份（與 calculate_missing_year_ranges 中的邏輯一致）
        earliest = symbol_info.get("earliest")
        earliest_year = earliest.year if earliest else None
        total_records = sum(year_counts.values())
        
        if earliest_year and earliest_year > DEFAULT_START_YEAR and total_records >= 1000:
            actual_start_year = earliest_year
        else:
            actual_start_year = DEFAULT_START_YEAR
        
        missing_years = []
        for year in range(actual_start_year, current_year + 1):
            count = year_counts.get(year, 0)
            threshold = MIN_TRADING_DAYS_CURRENT_YEAR if year == current_year else MIN_TRADING_DAYS_PAST_YEAR
            if count < threshold:
                missing_years.append(f"{year}({count})")
        
        if missing_years:
            print(f"   ⚠️  缺失年份: {', '.join(missing_years[:10])}{'...' if len(missing_years) > 10 else ''}")
            print(f"   📋 需回補: {len(missing_years)} 個年份")
        else:
            print(f"   ✅ 年份資料完整 ({actual_start_year}-{current_year})")

    if not fetch_ranges:
        print("ℹ️  無缺失年份，略過")
        result = {
            "symbol": symbol,
            "success": True,
            "new_records": 0,
            "total_records": sum(year_counts.values()),
            "elapsed": 0,
        }
        if verify:
            verify_after_fetch(symbol, conn)
        conn.close()
        return result

    total_new_records = 0
    total_duplicates = 0
    total_existing_records = 0
    start_time = time.time()

    # 計算總月份數
    all_monthly_ranges = []
    for range_start, range_end in fetch_ranges:
        monthly = split_range_by_month(range_start, range_end)
        all_monthly_ranges.extend(monthly)
    
    total_months = len(all_monthly_ranges)
    print(f"\n  📅 總共需抓取 {total_months} 個月份")

    for month_idx, (month_start, month_end) in enumerate(all_monthly_ranges, start=1):
        # 計算月份進度
        progress_pct = (month_idx / total_months) * 100
        filled = int(progress_pct / 2)  # 50格進度條
        progress_bar = "█" * filled + "░" * (50 - filled)
        
        # 月份標籤
        month_label = datetime.strptime(month_start, "%Y-%m-%d").strftime("%Y年%m月")
        
        print(f"\n  🔄 [{month_idx}/{total_months}] {month_label} ({month_start} ~ {month_end})")
        print(f"     {progress_bar} {progress_pct:.1f}%")

        payload = {
            "symbols": [symbol],
            "start_date": month_start,
            "end_date": month_end,
            "update_prices": True,
            "force_full_refresh": True,
            "force_start_date": month_start,
        }

        try:
            resp = requests.post(
                f"{API_BASE}/api/update",
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            result = resp.json()

            if not result.get("success", False):
                print(f"  ❌ 月份失敗: {result.get('error', '未知錯誤')}")
                continue

            results = result.get("results", [])
            if not results:
                print("  ⚠️  無資料")
                continue

            stock_result = results[0]
            new_records = stock_result.get("price_records", 0)
            duplicate_records = stock_result.get("duplicate_records", 0)
            existing_records = stock_result.get("existing_records", 0)

            total_new_records += new_records
            total_duplicates += duplicate_records
            total_existing_records = max(total_existing_records, existing_records)

            status_icon = "✅" if new_records > 0 else "⏭️"
            print(f"     {status_icon} 新增: {new_records} 筆 | 累計: {total_new_records:,} 筆")
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ 請求失敗: {str(e)}")
            continue

        time.sleep(0.5)  # 減少延遲

    elapsed = time.time() - start_time

    final_icon = "✅" if total_new_records > 0 else "🔄"
    print(f"\n{final_icon} 股票完成 (耗時: {elapsed:.1f} 秒)")
    print(f"   📈 新增記錄: {total_new_records:,} 筆")
    print(f"   🔄 更新記錄: {total_duplicates:,} 筆")
    print(f"   📊 總記錄數: {total_existing_records:,} 筆")
    
    if total_new_records > 0:
        print(f"   ⚡ 抓取速度: {total_new_records/elapsed:.0f} 筆/秒")

    if verify:
        verify_after_fetch(symbol, conn)

    conn.close()

    return {
        "symbol": symbol,
        "success": True,
        "new_records": total_new_records,
        "total_records": total_existing_records,
        "elapsed": elapsed,
    }


def verify_after_fetch(symbol: str, conn: psycopg2.extensions.connection) -> None:
    """抓取後驗證資料完整性"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    EXTRACT(YEAR FROM date) AS year,
                    COUNT(*) AS count
                FROM tw_stock_prices
                WHERE symbol = %s
                GROUP BY EXTRACT(YEAR FROM date)
                ORDER BY year
                """,
                (symbol,),
            )
            year_counts = cur.fetchall()

        if year_counts:
            print(f"\n   📊 年度分布:")
            for row in year_counts:
                year = int(row["year"])
                count = row["count"]
                bar = "█" * min(int(count / 10), 50)
                print(f"      {year}: {count:>3} 筆 {bar}")
    except Exception as exc:
        print(f"   ⚠️  驗證失敗: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="智能回補缺失的股票數據")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="最多處理多少檔股票 (預設全部缺資料的股票)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="每檔抓取後驗證年度分布",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="只處理指定股票，使用逗號分隔，例如 2002.TW,2330.TW",
    )
    args = parser.parse_args()
    
    print("=" * 80)
    print("🔍 正在掃描資料庫，找出缺資料的股票...")
    print("=" * 80)
    
    incomplete = get_incomplete_symbols()
    
    if not incomplete:
        print("\n✅ 太好了！所有股票資料都很完整！")
        return 0
    
    total = len(incomplete)
    print(f"\n📋 找到 {total} 檔需要回補的股票")

    if args.symbols:
        target_symbols = {s.strip() for s in args.symbols.split(',') if s.strip()}
        incomplete = [info for info in incomplete if info['symbol'] in target_symbols]
        missing_symbols = target_symbols - {info['symbol'] for info in incomplete}
        if missing_symbols:
            print(f"⚠️  下列股票未在缺資料清單中，將略過: {', '.join(sorted(missing_symbols))}")

    if args.limit:
        incomplete = incomplete[:args.limit]

    print(f"📌 本次處理: {len(incomplete)} 檔")

    if not incomplete:
        print("⚠️  沒有符合條件的股票需要處理")
        return 0
    
    print("\n" + "=" * 80)
    print("開始智能回補")
    print("=" * 80)
    
    start_ts = time.time()
    success_count = 0
    failed_count = 0
    total_new_records = 0
    
    for idx, stock_info in enumerate(incomplete, start=1):
        symbol = stock_info['symbol']
        current_records = stock_info['total_records']
        current_years = stock_info['year_count']
        
        print(f"\n💡 當前狀態: {current_records} 筆, {current_years} 年")
        
        result = fetch_single_stock(symbol, idx, len(incomplete), verify=args.verify)

        if result['success']:
            success_count += 1
            total_new_records += result.get('new_records', 0)
        else:
            failed_count += 1

        elapsed = time.time() - start_ts
        progress_pct = (idx / len(incomplete)) * 100
        avg_time = elapsed / idx
        eta_seconds = (len(incomplete) - idx) * avg_time

        print(f"\n{'─' * 80}")
        print(f"📊 總進度: {idx}/{len(incomplete)} ({progress_pct:.1f}%)")
        print(f"⏱️  已用時間: {elapsed/60:.1f} 分 | 預估剩餘: {eta_seconds/60:.1f} 分")
        print(f"✅ 成功: {success_count} | ❌ 失敗: {failed_count}")
        print(f"📈 累計新增: {total_new_records:,} 筆")
        
        if idx < len(incomplete):
            print(f"\n⏸️  等待 {SLEEP_BETWEEN_STOCKS} 秒...\n")
            time.sleep(SLEEP_BETWEEN_STOCKS)
    
    # 最終統計
    duration = time.time() - start_ts
    success_rate = (success_count / len(incomplete) * 100) if incomplete else 0
    
    print("\n" + "=" * 80)
    print("🎉 智能回補完成")
    print("=" * 80)
    print(f"📊 統計摘要:")
    print(f"   處理股票: {len(incomplete)} 檔")
    print(f"   成功: {success_count} ({success_rate:.1f}%)")
    print(f"   失敗: {failed_count}")
    print(f"   新增記錄: {total_new_records:,} 筆")
    print(f"\n⏱️  總耗時: {duration/60:.1f} 分鐘 ({duration/3600:.2f} 小時)")
    if success_count > 0:
        print(f"   平均每檔: {duration/success_count:.1f} 秒")
    print("=" * 80)
    
    return 0 if failed_count == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
