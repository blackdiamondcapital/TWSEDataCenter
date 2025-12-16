#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""逐檔抓取台股歷史股價資料，避免批次更新"""

import argparse
import sys
import time
from datetime import date, datetime
from typing import Iterable, List

import psycopg2
import psycopg2.extras
import requests

API_BASE = "http://localhost:5003"
DEFAULT_START = "2010-01-01"

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "s8304021",
    "database": "postgres",
}


def fetch_symbols() -> List[str]:
    resp = requests.get(f"{API_BASE}/api/symbols", timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(payload.get("error", "無法取得股票清單"))
    return [item["symbol"] for item in payload.get("data", []) if item.get("symbol")]


def filter_symbols(symbols: Iterable[str], market: str) -> List[str]:
    if market == "all":
        return list(symbols)
    suffix = ".TW" if market == "tw" else ".TWO"
    return [s for s in symbols if s.endswith(suffix)]


def post_update(symbol: str, start: str, end: str, timeout: int, force_full_refresh: bool) -> dict:
    payload = {
        "symbols": [symbol],
        "start_date": start,
        "end_date": end,
        "update_prices": True,
        "force_start_date": start,
        "force_full_refresh": force_full_refresh,
    }
    resp = requests.post(f"{API_BASE}/api/update", json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_existing_summary(cursor, symbol: str):
    cursor.execute(
        """
        SELECT COUNT(*) AS count, MIN(date) AS earliest, MAX(date) AS latest
        FROM tw_stock_prices
        WHERE symbol = %s
        """,
        (symbol,),
    )
    row = cursor.fetchone()
    if not row:
        return 0, None, None
    count = int(row["count"] or 0)
    return count, row["earliest"], row["latest"]


def determine_missing_year_ranges(
    cursor,
    symbol: str,
    start_date: date,
    end_date: date,
    min_full_year: int,
    min_partial_year: int,
):
    cursor.execute(
        """
        SELECT EXTRACT(YEAR FROM date)::int AS year, COUNT(*) AS count
        FROM tw_stock_prices
        WHERE symbol = %s AND date BETWEEN %s AND %s
        GROUP BY year
        """,
        (symbol, start_date, end_date),
    )
    year_counts = {row["year"]: int(row["count"] or 0) for row in cursor.fetchall()}

    missing = []
    current_year = start_date.year
    while current_year <= end_date.year:
        year_start = max(date(current_year, 1, 1), start_date)
        year_end = min(date(current_year, 12, 31), end_date)
        is_partial_year = (
            (current_year == start_date.year and year_start > date(current_year, 1, 1))
            or (current_year == end_date.year and year_end < date(current_year, 12, 31))
        )
        threshold = min_partial_year if is_partial_year else min_full_year
        if year_counts.get(current_year, 0) < threshold:
            missing.append((year_start, year_end))
        current_year += 1

    return missing


def format_progress(index: int, total: int) -> str:
    return f"[{index}/{total}] ({index / total:.2%})"


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="逐檔抓取台股歷史股價資料")
    parser.add_argument("--start", default=DEFAULT_START, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="結束日 (YYYY-MM-DD)，預設今天")
    parser.add_argument("--market", choices=["tw", "two", "all"], default="all", help="市場範圍")
    parser.add_argument("--pause", type=float, default=2.0, help="每檔股票間的間隔秒數")
    parser.add_argument("--timeout", type=int, default=900, help="單檔請求逾時秒數")
    parser.add_argument("--limit", type=int, default=None, help="僅處理前 N 檔，用於測試")
    parser.add_argument("--force-full-refresh", action="store_true", help="強制忽略資料庫最新日期，完整回補")
    parser.add_argument("--symbols", nargs="*", help="指定股票代碼，若提供則僅處理這些股票")
    parser.add_argument("--show-existing", action="store_true", help="顯示資料庫既有的日期範圍與筆數")
    parser.add_argument("--min-full-year", type=int, default=240, help="完整年度至少幾筆視為完成 (預設240)")
    parser.add_argument("--min-partial-year", type=int, default=50, help="首尾年度至少幾筆視為完成 (預設50)")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    # 驗證日期格式
    try:
        start_date_obj = datetime.strptime(args.start, "%Y-%m-%d").date()
        if args.end:
            end_date_obj = datetime.strptime(args.end, "%Y-%m-%d").date()
        else:
            end_date_obj = date.today()
    except ValueError:
        print("日期格式錯誤，請使用 YYYY-MM-DD")
        return 1

    end_date = end_date_obj.strftime("%Y-%m-%d")

    if args.symbols:
        symbols = args.symbols
    else:
        all_symbols = fetch_symbols()
        symbols = filter_symbols(all_symbols, args.market)

    if args.limit is not None:
        symbols = symbols[: args.limit]

    total = len(symbols)
    if total == 0:
        print("沒有需要處理的股票代碼")
        return 0

    print(f"共 {total} 檔股票，日期區間 {args.start} ~ {end_date}")
    print(f"force_full_refresh={'ON' if args.force_full_refresh else 'OFF'}，暫停 {args.pause}s\n")

    success = 0
    skipped = 0
    failed = 0
    inserted_records = 0

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"❌ 無法連線資料庫：{exc}")
        print("    需要資料庫資訊以判斷缺漏區間，請確認連線設定後再試")
        return 1

    for idx, symbol in enumerate(symbols, start=1):
        prefix = format_progress(idx, total)
        print(f"{prefix} 處理 {symbol}...")
        try:
            existing_count, earliest, latest = fetch_existing_summary(cursor, symbol)
            missing_ranges = determine_missing_year_ranges(
                cursor,
                symbol,
                start_date_obj,
                end_date_obj,
                args.min_full_year,
                args.min_partial_year,
            )

            if not missing_ranges:
                skipped += 1
                if args.show_existing:
                    if existing_count:
                        print(
                            "  ⊘ {}: 已有完整資料，共 {} 筆 ({} ~ {})\n".format(
                                symbol,
                                existing_count,
                                earliest,
                                latest,
                            )
                        )
                    else:
                        print(f"  ⊘ {symbol}: 資料庫尚無資料，但未偵測到缺漏區間\n")
                else:
                    print(f"  ⊘ {symbol}: 已有完整資料\n")
                continue

            symbol_inserted = 0
            for range_start, range_end in missing_ranges:
                print(f"    ↳ 回補區間 {range_start} ~ {range_end}...")
                result = post_update(
                    symbol,
                    range_start.strftime("%Y-%m-%d"),
                    range_end.strftime("%Y-%m-%d"),
                    args.timeout,
                    args.force_full_refresh,
                )

                if not result.get("success"):
                    failed += 1
                    print(f"      ✗ {symbol}: {result.get('error', '未知錯誤')}\n")
                    continue

                entries = result.get("results", [])
                if not entries:
                    print("      ⊘ 無結果\n")
                    continue

                info = entries[0]
                records = info.get("price_records", 0)
                duplicates = info.get("duplicate_records", 0)
                status = info.get("status", "unknown")
                rng = info.get("price_date_range", {})

                if records > 0:
                    symbol_inserted += records
                    inserted_records += records
                    print(
                        f"      ✓ {symbol}: 寫入 {records} 筆 (重複 {duplicates}) "
                        f"[{rng.get('start')} ~ {rng.get('end')}] status={status}\n"
                    )
                else:
                    print(f"      ⊘ {symbol}: 無新增資料 status={status}\n")

                if args.pause > 0:
                    time.sleep(args.pause / 2)

            if symbol_inserted > 0:
                success += 1
            else:
                skipped += 1
                if args.show_existing:
                    print(f"  ⊘ {symbol}: 所有缺漏區間呼叫後仍無新增資料，請檢查 API 回應\n")
                else:
                    print(f"  ⊘ {symbol}: 無新增資料\n")

        except requests.HTTPError as err:
            failed += 1
            print(f"  ✗ {symbol}: HTTP {err}\n")
        except requests.RequestException as err:
            failed += 1
            print(f"  ✗ {symbol}: 請求錯誤 {err}\n")
        except Exception as err:  # pylint: disable=broad-except
            failed += 1
            print(f"  ✗ {symbol}: 未預期錯誤 {err}\n")

        if idx < total and args.pause > 0:
            time.sleep(args.pause)

    print("=" * 80)
    print("完成統計:")
    print(f"  成功 {success} 檔")
    print(f"  無新增資料 {skipped} 檔")
    print(f"  失敗 {failed} 檔")
    print(f"  累計寫入 {inserted_records} 筆")
    print("=" * 80)

    cursor.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
