#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quick tester for fetching historical 三大法人資料（TWSE T86 與 TPEX).

Usage examples:
    python3 t86_fetch_test.py --start 2025-11-10 --end 2025-11-14 --market twse
    python3 t86_fetch_test.py --start 2025-11-10 --end 2025-11-14 --market both --output t86_sample.csv

Notes:
- TWSE 與 TPEX API 都僅支援「單日」查詢，本工具會逐日迴圈呼叫。
- 請求間會加入延遲以避免被官方伺服器限流，可用 --sleep 調整。
- 回傳資料將彙整到同一 CSV，新增 `market` 欄位區分 TWSE / TPEX。
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import requests

TWSE_API_URL = "https://www.twse.com.tw/fund/T86"
TPEX_API_URL = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"

REQUEST_DELAY = 0.7  # seconds – stay polite to remote servers

TWSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://www.twse.com.tw/zh/trading/historical/fund/T86.html",
}

TPEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-TW,zh;q=0.9",
    "Referer": "https://www.tpex.org.tw",
}


@dataclass
class InvestorFlowRecord:
    date: date
    market: str  # "TWSE" or "TPEX"
    stock_no: str
    stock_name: str
    foreign_buy: int
    foreign_sell: int
    foreign_net: int
    foreign_dealer_buy: int
    foreign_dealer_sell: int
    foreign_dealer_net: int
    foreign_total_buy: int
    foreign_total_sell: int
    foreign_total_net: int
    investment_trust_buy: int
    investment_trust_sell: int
    investment_trust_net: int
    dealer_self_buy: int
    dealer_self_sell: int
    dealer_self_net: int
    dealer_hedge_buy: int
    dealer_hedge_sell: int
    dealer_hedge_net: int
    dealer_total_buy: int
    dealer_total_sell: int
    dealer_total_net: int
    overall_net: int


def parse_int(value: Optional[str]) -> int:
    if value is None:
        return 0
    value = value.replace(",", "").strip()
    if value in {"", "-"}:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def iter_dates(start: date, end: date) -> Iterable[date]:
    current = start
    step = timedelta(days=1)
    while current <= end:
        yield current
        current += step


def roc_date_str(target_date: date) -> str:
    return f"{target_date.year - 1911:03d}/{target_date.month:02d}/{target_date.day:02d}"


def fetch_twse_t86_by_date(session: requests.Session, target_date: date) -> List[InvestorFlowRecord]:
    params = {
        "response": "json",
        "date": target_date.strftime("%Y%m%d"),
        "selectType": "ALLBUT0999",
    }
    resp = session.get(TWSE_API_URL, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("stat") != "OK":
        return []

    data = payload.get("data") or []
    result: List[InvestorFlowRecord] = []
    for row in data:
        if len(row) < 19:
            continue

        fb = parse_int(row[2])
        fs = parse_int(row[3])
        fn = parse_int(row[4])
        fdb = parse_int(row[5])
        fds = parse_int(row[6])
        fdn = parse_int(row[7])
        itb = parse_int(row[8])
        its = parse_int(row[9])
        itn = parse_int(row[10])
        dealer_total_net = parse_int(row[11])
        dsb = parse_int(row[12])
        dss = parse_int(row[13])
        dsn = parse_int(row[14])
        dhb = parse_int(row[15])
        dhs = parse_int(row[16])
        dhn = parse_int(row[17])
        overall = parse_int(row[18])

        record = InvestorFlowRecord(
            date=target_date,
            market="TWSE",
            stock_no=(row[0] or "").strip(),
            stock_name=(row[1] or "").strip(),
            foreign_buy=fb,
            foreign_sell=fs,
            foreign_net=fn,
            foreign_dealer_buy=fdb,
            foreign_dealer_sell=fds,
            foreign_dealer_net=fdn,
            foreign_total_buy=fb + fdb,
            foreign_total_sell=fs + fds,
            foreign_total_net=fn + fdn,
            investment_trust_buy=itb,
            investment_trust_sell=its,
            investment_trust_net=itn,
            dealer_self_buy=dsb,
            dealer_self_sell=dss,
            dealer_self_net=dsn,
            dealer_hedge_buy=dhb,
            dealer_hedge_sell=dhs,
            dealer_hedge_net=dhn,
            dealer_total_buy=dsb + dhb,
            dealer_total_sell=dss + dhs,
            dealer_total_net=dealer_total_net if dealer_total_net else dsn + dhn,
            overall_net=overall,
        )
        result.append(record)
    return result


def fetch_tpex_t86_by_date(session: requests.Session, target_date: date) -> List[InvestorFlowRecord]:
    params = {
        "l": "zh-tw",
        "date": roc_date_str(target_date),
        "json": "1",
    }
    resp = session.get(TPEX_API_URL, params=params, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    tables = payload.get("tables") or []
    if not tables:
        return []

    data = tables[0].get("data") or []
    result: List[InvestorFlowRecord] = []
    for row in data:
        if len(row) < 24:
            continue

        fb = parse_int(row[2])
        fs = parse_int(row[3])
        fn = parse_int(row[4])
        fdb = parse_int(row[5])
        fds = parse_int(row[6])
        fdn = parse_int(row[7])
        ftb = parse_int(row[8])
        fts = parse_int(row[9])
        ftn = parse_int(row[10])
        itb = parse_int(row[11])
        its = parse_int(row[12])
        itn = parse_int(row[13])
        dsb = parse_int(row[14])
        dss = parse_int(row[15])
        dsn = parse_int(row[16])
        dhb = parse_int(row[17])
        dhs = parse_int(row[18])
        dhn = parse_int(row[19])
        dtb = parse_int(row[20])
        dts = parse_int(row[21])
        dtn = parse_int(row[22])
        overall = parse_int(row[23])

        record = InvestorFlowRecord(
            date=target_date,
            market="TPEX",
            stock_no=(row[0] or "").strip(),
            stock_name=(row[1] or "").strip(),
            foreign_buy=fb,
            foreign_sell=fs,
            foreign_net=fn,
            foreign_dealer_buy=fdb,
            foreign_dealer_sell=fds,
            foreign_dealer_net=fdn,
            foreign_total_buy=ftb,
            foreign_total_sell=fts,
            foreign_total_net=ftn,
            investment_trust_buy=itb,
            investment_trust_sell=its,
            investment_trust_net=itn,
            dealer_self_buy=dsb,
            dealer_self_sell=dss,
            dealer_self_net=dsn,
            dealer_hedge_buy=dhb,
            dealer_hedge_sell=dhs,
            dealer_hedge_net=dhn,
            dealer_total_buy=dtb,
            dealer_total_sell=dts,
            dealer_total_net=dtn,
            overall_net=overall,
        )
        result.append(record)
    return result


def parse_int(value: Optional[str]) -> int:
    if value is None:
        return 0
    value = value.replace(",", "").strip()
    if value in {"", "-"}:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def iter_dates(start: date, end: date) -> Iterable[date]:
    current = start
    step = timedelta(days=1)
    while current <= end:
        yield current
        current += step


def write_csv(records: List[InvestorFlowRecord], output_path: Path) -> None:
    if not records:
        print("⚠️  沒有資料可寫入 CSV。", file=sys.stderr)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow([
            "date",
            "market",
            "stock_no",
            "stock_name",
            "foreign_buy",
            "foreign_sell",
            "foreign_net",
            "foreign_dealer_buy",
            "foreign_dealer_sell",
            "foreign_dealer_net",
            "foreign_total_buy",
            "foreign_total_sell",
            "foreign_total_net",
            "investment_trust_buy",
            "investment_trust_sell",
            "investment_trust_net",
            "dealer_self_buy",
            "dealer_self_sell",
            "dealer_self_net",
            "dealer_hedge_buy",
            "dealer_hedge_sell",
            "dealer_hedge_net",
            "dealer_total_buy",
            "dealer_total_sell",
            "dealer_total_net",
            "overall_net",
        ])
        for rec in records:
            writer.writerow([
                rec.date.isoformat(),
                rec.market,
                rec.stock_no,
                rec.stock_name,
                rec.foreign_buy,
                rec.foreign_sell,
                rec.foreign_net,
                rec.foreign_dealer_buy,
                rec.foreign_dealer_sell,
                rec.foreign_dealer_net,
                rec.foreign_total_buy,
                rec.foreign_total_sell,
                rec.foreign_total_net,
                rec.investment_trust_buy,
                rec.investment_trust_sell,
                rec.investment_trust_net,
                rec.dealer_self_buy,
                rec.dealer_self_sell,
                rec.dealer_self_net,
                rec.dealer_hedge_buy,
                rec.dealer_hedge_sell,
                rec.dealer_hedge_net,
                rec.dealer_total_buy,
                rec.dealer_total_sell,
                rec.dealer_total_net,
                rec.overall_net,
            ])
    print(f"✅ CSV 已輸出到 {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TWSE/TPEX 三大法人資料 for a date range")
    parser.add_argument("--start", required=True, help="開始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="結束日期 YYYY-MM-DD")
    parser.add_argument("--output", help="輸出 CSV 路徑（若省略則僅列印摘要）")
    parser.add_argument(
        "--sleep",
        type=float,
        default=REQUEST_DELAY,
        help="每次請求間的秒數延遲 (default: 0.7)",
    )
    parser.add_argument(
        "--market",
        choices=["twse", "tpex", "both"],
        default="twse",
        help="要抓取的市場：twse（上市）、tpex（上櫃）、both（兩者）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as exc:
        print(f"❌ 日期格式錯誤: {exc}", file=sys.stderr)
        return 1

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    twse_session = requests.Session()
    twse_session.headers.update(TWSE_HEADERS)

    tpex_session = requests.Session()
    tpex_session.headers.update(TPEX_HEADERS)

    markets = [args.market] if args.market != "both" else ["twse", "tpex"]

    all_records: List[InvestorFlowRecord] = []
    skipped: List[str] = []

    for current_date in iter_dates(start_date, end_date):
        if "twse" in markets:
            try:
                twse_records = fetch_twse_t86_by_date(twse_session, current_date)
            except requests.HTTPError as exc:
                print(f"❌ {current_date} [TWSE] HTTP 錯誤: {exc}", file=sys.stderr)
                skipped.append(f"{current_date} [TWSE] HTTP")
            except Exception as exc:
                print(f"❌ {current_date} [TWSE] 取得失敗: {exc}", file=sys.stderr)
                skipped.append(f"{current_date} [TWSE] ERR")
            else:
                if twse_records:
                    all_records.extend(twse_records)
                    print(f"📅 {current_date} [TWSE] -> {len(twse_records)} 筆資料")
                else:
                    print(f"⚠️ {current_date} [TWSE] 無資料或非交易日")
                    skipped.append(f"{current_date} [TWSE] 空")
            time.sleep(max(args.sleep, 0))

        if "tpex" in markets:
            try:
                tpex_records = fetch_tpex_t86_by_date(tpex_session, current_date)
            except requests.HTTPError as exc:
                print(f"❌ {current_date} [TPEX] HTTP 錯誤: {exc}", file=sys.stderr)
                skipped.append(f"{current_date} [TPEX] HTTP")
            except Exception as exc:
                print(f"❌ {current_date} [TPEX] 取得失敗: {exc}", file=sys.stderr)
                skipped.append(f"{current_date} [TPEX] ERR")
            else:
                if tpex_records:
                    all_records.extend(tpex_records)
                    print(f"📅 {current_date} [TPEX] -> {len(tpex_records)} 筆資料")
                else:
                    print(f"⚠️ {current_date} [TPEX] 無資料或非交易日")
                    skipped.append(f"{current_date} [TPEX] 空")
            time.sleep(max(args.sleep, 0))

    total_days = (end_date - start_date).days + 1
    print("\n✅ 完成下載。")
    print(f"總筆數: {len(all_records)} | 查詢天數: {total_days} | 目標市場: {', '.join(m.upper() for m in markets)}")

    if all_records:
        by_market = defaultdict(int)
        for rec in all_records:
            by_market[rec.market] += 1
        print("市場筆數：")
        for market_label, count in sorted(by_market.items()):
            print(f"  - {market_label}: {count} 筆")

    if skipped:
        print(f"⚠️ 有 {len(skipped)} 個請求沒有資料或失敗：")
        for item in skipped:
            print(f"    • {item}")

    if args.output:
        write_csv(all_records, Path(args.output))

    return 0


if __name__ == "__main__":
    sys.exit(main())
