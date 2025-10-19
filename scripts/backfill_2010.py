#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Utility script to backfill Taiwan stock prices for 2010 via backend API."""

import argparse
import sys
import time
from datetime import datetime
from typing import Iterable, List

import requests

API_BASE = "http://localhost:5003"
DEFAULT_START = "2010-01-01"
DEFAULT_END = "2010-12-31"


def chunk_list(items: List[str], size: int) -> Iterable[List[str]]:
    """Yield successive chunks from list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_symbols(suffix_filter: str) -> List[str]:
    """Fetch all symbols from backend and filter by suffix."""
    resp = requests.get(f"{API_BASE}/api/symbols", timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(payload.get("error", "未知錯誤：symbols API 失敗"))
    symbols = [s["symbol"] for s in payload.get("data", []) if s.get("symbol")]
    return [s for s in symbols if s.endswith(suffix_filter)]


def post_update(symbols: List[str], start: str, end: str, timeout: int) -> dict:
    """Call backend update API for specified symbols and date range."""
    body = {
        "symbols": symbols,
        "start_date": start,
        "end_date": end,
        "update_prices": True,
    }
    resp = requests.post(
        f"{API_BASE}/api/update",
        json=body,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def update_market(symbols: List[str], label: str, args: argparse.Namespace) -> None:
    total = len(symbols)
    if total == 0:
        print(f"[{label}] 無符合的股票代碼，略過")
        return

    print(f"[{label}] 共 {total} 檔，將以每批 {args.chunk_size} 檔執行，間隔 {args.pause}s")
    success = 0
    failed = 0

    for batch_idx, batch in enumerate(chunk_list(symbols, args.chunk_size), start=1):
        print(
            f"[{label}] 批次 {batch_idx}/{(total + args.chunk_size - 1) // args.chunk_size}: "
            f"{batch[0]} .. {batch[-1]}"
        )
        try:
            result = post_update(batch, args.start, args.end, args.timeout)
            if result.get("success"):
                success += len(batch)
                summary = result.get("summary", {})
                print(
                    f"    成功，累計成功 {success} 檔，summary: total={summary.get('total')}, "
                    f"success={summary.get('success')}"
                )
            else:
                failed += len(batch)
                print(f"    失敗：{result.get('error', '未知錯誤')}" )
        except requests.HTTPError as http_err:
            failed += len(batch)
            print(f"    HTTP 錯誤：{http_err}")
        except requests.RequestException as req_err:
            failed += len(batch)
            print(f"    請求錯誤：{req_err}")
        except Exception as err:  # pylint: disable=broad-except
            failed += len(batch)
            print(f"    未預期錯誤：{err}")

        if batch_idx * args.chunk_size < total:
            time.sleep(args.pause)

    print(f"[{label}] 完成：成功 {success} 檔，失敗 {failed} 檔\n")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Taiwan stock prices for 2010")
    parser.add_argument("--start", default=DEFAULT_START, help="開始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", default=DEFAULT_END, help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--chunk-size", type=int, default=5, help="每批處理的股票數")
    parser.add_argument("--pause", type=float, default=3.0, help="批次間隔秒數")
    parser.add_argument(
        "--timeout", type=int, default=600, help="單批請求逾時秒數"
    )
    parser.add_argument(
        "--market",
        choices=["tw", "two", "all"],
        default="all",
        help="選擇上市、上櫃或全部市場",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制處理前 N 檔（測試用）",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    # 驗證日期格式
    for label, value in (("start", args.start), ("end", args.end)):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            print(f"日期格式錯誤：{label}={value}")
            return 1

    try:
        markets = []
        if args.market in ("tw", "all"):
            markets.append(("上市", fetch_symbols(".TW")))
        if args.market in ("two", "all"):
            markets.append(("上櫃", fetch_symbols(".TWO")))
    except Exception as exc:  # pylint: disable=broad-except
        print(f"取得股票清單失敗：{exc}")
        return 1

    for label, symbols in markets:
        if args.limit is not None:
            symbols = symbols[: args.limit]
        update_market(symbols, label, args)

    print("全部市場批次呼叫結束")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
