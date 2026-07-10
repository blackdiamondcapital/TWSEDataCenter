#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""檢查資料庫缺漏並補抓 2010 起的股價資料"""

import argparse
import collections
import json
import os
import sys
import time
from datetime import date
from math import ceil
from typing import Dict, Iterable, List, Tuple

import psycopg2
import requests

API_BASE = "http://localhost:5003"
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD", ""),
    "database": "postgres",
}

DEFAULT_START_YEAR = 2010
DEFAULT_CHUNK_SIZE = 5
DEFAULT_PAUSE = 2.0
DEFAULT_LOG_FILE = "backfill_missing_ranges.log"


def daterange_for_year(year: int, today: date) -> Tuple[date, date]:
    if year == today.year:
        return date(year, 1, 1), today
    return date(year, 1, 1), date(year, 12, 31)


def init_logger(log_path: str):
    if log_path:
        with open(log_path, "w", encoding="utf-8") as fp:
            fp.write(f"backfill_missing_ranges started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")


def log(msg: str, log_path: str):
    timestamp = time.strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    if log_path:
        with open(log_path, "a", encoding="utf-8") as fp:
            fp.write(line + "\n")


def fetch_symbols(cursor, include_api: bool = False) -> List[str]:
    cursor.execute("SELECT DISTINCT symbol FROM tw_stock_prices ORDER BY symbol;")
    symbols = [row[0] for row in cursor.fetchall() if row[0]]
    if include_api:
        try:
            resp = requests.get(f"{API_BASE}/api/symbols", timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("success"):
                api_symbols = [s.get("symbol") for s in payload.get("data", []) if s.get("symbol")]
                symbols = sorted(set(symbols).union(api_symbols))
        except Exception as exc:  # pylint: disable=broad-except
            log(f"⚠️ 無法從 API 取得 symbol 清單：{exc}", None)
    return symbols


def detect_missing_ranges(cursor, symbols: Iterable[str], start_year: int, today: date,
                          min_full_year_days: int, min_partial_year_days: int,
                          log_path: str = None) -> Dict[Tuple[str, date, date], int]:
    tasks = {}
    for idx, symbol in enumerate(symbols, start=1):
        if not symbol:
            continue
        for year in range(start_year, today.year + 1):
            start, end = daterange_for_year(year, today)
            if end < start:
                continue
            cursor.execute(
                "SELECT COUNT(*) FROM tw_stock_prices WHERE symbol = %s AND date BETWEEN %s AND %s",
                (symbol, start, end),
            )
            count = cursor.fetchone()[0]
            threshold = min_full_year_days if year < today.year else min_partial_year_days
            if count >= threshold:
                continue
            key = (symbol, start, end)
            tasks[key] = count
        if idx % 100 == 0 or idx == len(symbols):
            log(f"已掃描 {idx}/{len(symbols)} 檔股票", log_path)
    return tasks


def group_tasks(tasks: Dict[Tuple[str, date, date], int], chunk_size: int) -> Dict[Tuple[date, date], List[str]]:
    grouped: Dict[Tuple[date, date], List[str]] = collections.defaultdict(list)
    for (symbol, start, end) in tasks:
        grouped[(start, end)].append(symbol)
    # 依開始日期排序，利於觀察
    return dict(sorted(grouped.items(), key=lambda item: item[0][0]))


def post_update(symbols: List[str], start: date, end: date, timeout: int, force_full_refresh: bool) -> dict:
    payload = {
        "symbols": symbols,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "update_prices": True,
        "force_start_date": start.isoformat(),
        "force_full_refresh": force_full_refresh,
    }
    resp = requests.post(f"{API_BASE}/api/update", json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


class ProgressTracker:
    def __init__(self, total_batches: int):
        self.total_batches = total_batches
        self.completed_batches = 0
        self.successful_symbols = 0
        self.skipped_symbols = 0
        self.failed_symbols = 0
        self.total_records_inserted = 0

    def update_with_batch(self, result: dict):
        self.completed_batches += 1
        if not result:
            return
        for item in result.get("results", []):
            price_records = item.get("price_records", 0)
            status = item.get("status")
            if price_records > 0:
                self.successful_symbols += 1
                self.total_records_inserted += price_records
            elif status in ("success", "partial"):
                self.skipped_symbols += 1
        self.failed_symbols += len(result.get("errors", []))

    def summary(self) -> dict:
        return {
            "completed_batches": self.completed_batches,
            "total_batches": self.total_batches,
            "successful_symbols": self.successful_symbols,
            "skipped_symbols": self.skipped_symbols,
            "failed_symbols": self.failed_symbols,
            "total_records_inserted": self.total_records_inserted,
        }


def execute_backfill(grouped: Dict[Tuple[date, date], List[str]], chunk_size: int, pause: float,
                     timeout: int, force_full_refresh: bool, log_path: str = None):
    total_batches = sum(ceil(len(symbols) / chunk_size) for symbols in grouped.values())
    tracker = ProgressTracker(total_batches)

    for (start, end), symbols in grouped.items():
        log(f"=== 補抓 {start} ~ {end} ({len(symbols)} 檔) ===", log_path)
        for i in range(0, len(symbols), chunk_size):
            batch = symbols[i:i + chunk_size]
            batch_no = tracker.completed_batches + 1
            log(f"批次 {batch_no}/{total_batches}: {batch}", log_path)
            result = None
            try:
                result = post_update(batch, start, end, timeout, force_full_refresh)
                if result.get("success"):
                    for r in result.get("results", []):
                        symbol = r.get("symbol")
                        price_records = r.get("price_records", 0)
                        duplicate_records = r.get("duplicate_records", 0)
                        status = r.get("status")
                        if price_records:
                            rng = r.get("price_date_range", {})
                            log(
                                f"  ✓ {symbol}: 寫入 {price_records} 筆 (重複 {duplicate_records}) "
                                f"[{rng.get('start')} ~ {rng.get('end')}]",
                                log_path,
                            )
                        else:
                            log(f"  ⊘ {symbol}: status={status}", log_path)
                    for err in result.get("errors", []):
                        log(f"  ✗ {err.get('symbol')}: {err.get('error')}", log_path)
                else:
                    log(f"  ✗ 批次失敗：{result.get('error')}", log_path)
            except requests.RequestException as exc:
                log(f"  ✗ HTTP 請求錯誤：{exc}", log_path)
            tracker.update_with_batch(result or {})

            summary = tracker.summary()
            log(
                f"  ➜ 進度 {summary['completed_batches']}/{summary['total_batches']} 批；"
                f"成功 {summary['successful_symbols']} 檔，"
                f"跳過 {summary['skipped_symbols']} 檔，"
                f"失敗 {summary['failed_symbols']} 檔，"
                f"累計寫入 {summary['total_records_inserted']} 筆",
                log_path,
            )

            if pause > 0 and (i + chunk_size) < len(symbols):
                time.sleep(pause)

    log("🎉 全部補抓流程完成", log_path)


def parse_args():
    parser = argparse.ArgumentParser(description="補抓缺漏年份的台股股價資料")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--pause", type=float, default=DEFAULT_PAUSE)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--min-full-year", type=int, default=200, help="完整年度至少幾筆視為已完成")
    parser.add_argument("--min-partial-year", type=int, default=50, help="當年度至少幾筆視為已完成")
    parser.add_argument("--include-api-symbols", action="store_true", help="包含 API 清單中但資料庫尚未出現的股票")
    parser.add_argument("--force-full-refresh", action="store_true", help="強制忽略現有最新日期進行完整回補")
    parser.add_argument("--log", default=DEFAULT_LOG_FILE, help="進度日誌輸出檔案 (預設 backfill_missing_ranges.log)")
    return parser.parse_args()


def main():
    args = parse_args()
    today = date.today()

    log_path = args.log
    init_logger(log_path)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        symbols = fetch_symbols(cursor, include_api=args.include_api_symbols)
        log(f"總共有 {len(symbols)} 檔股票需要檢查", log_path)

        tasks = detect_missing_ranges(
            cursor,
            symbols,
            start_year=args.start_year,
            today=today,
            min_full_year_days=args.min_full_year,
            min_partial_year_days=args.min_partial_year,
            log_path=log_path,
        )

        if not tasks:
            log("🎉 已完成，無缺漏資料", log_path)
            return

        log(f"共發現 {len(tasks)} 個年份缺漏需要補抓", log_path)
        grouped = group_tasks(tasks, args.chunk_size)
        execute_backfill(grouped, args.chunk_size, args.pause, args.timeout, args.force_full_refresh, log_path)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
