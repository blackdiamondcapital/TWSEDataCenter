#!/usr/bin/env python3
"""Backfill TWSE warrant daily trades (OHLC + turnover/volume) via MI_INDEX.

Writes into tw_warrant_trade. Intended for GitHub Actions / local ops with
NEON_DATABASE_URL or DATABASE_URL.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any

import psycopg2
import requests
from psycopg2.extras import execute_values

UA = {"User-Agent": "Mozilla/5.0 (compatible; QuantGemsWarrantBackfill/1.0)"}


def parse_num(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).replace(",", "").strip()
    if not s or s in ("--", "-", "---", "null", "None"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def is_warrant_like(name: str | None) -> bool:
    s = name or ""
    return any(k in s for k in ("購", "售", "牛", "熊", "權證"))


def fetch_mi_index(trade_date: date) -> dict[str, dict] | None:
    ymd = trade_date.strftime("%Y%m%d")
    url = (
        "https://www.twse.com.tw/exchangeReport/MI_INDEX"
        f"?response=json&date={ymd}&type=ALL"
    )
    resp = requests.get(url, timeout=120, headers=UA)
    if resp.status_code != 200:
        raise RuntimeError(f"MI_INDEX HTTP {resp.status_code} for {ymd}")
    payload = resp.json()
    if not isinstance(payload, dict):
        return None
    if payload.get("stat") and payload.get("stat") != "OK":
        return None

    tables = payload.get("tables") or []
    target = None
    for table in tables:
        if not isinstance(table, dict):
            continue
        fields = table.get("fields") or []
        if "收盤價" in fields and "證券代號" in fields:
            target = table
            break
    if not target:
        return {}

    fields = target.get("fields") or []
    idx = {name: i for i, name in enumerate(fields)}
    out: dict[str, dict] = {}
    for row in target.get("data") or []:
        if not isinstance(row, list):
            continue
        code = str(row[idx["證券代號"]]).strip() if "證券代號" in idx else ""
        if not code:
            continue
        name = str(row[idx["證券名稱"]]).strip() if "證券名稱" in idx else None
        if not is_warrant_like(name):
            continue
        shares = parse_num(row[idx["成交股數"]]) if "成交股數" in idx else None
        volume_lots = int(shares / 1000) if shares is not None else None
        out[code] = {
            "warrant_name": name,
            "open_price": parse_num(row[idx["開盤價"]]) if "開盤價" in idx else None,
            "high_price": parse_num(row[idx["最高價"]]) if "最高價" in idx else None,
            "low_price": parse_num(row[idx["最低價"]]) if "最低價" in idx else None,
            "close_price": parse_num(row[idx["收盤價"]]) if "收盤價" in idx else None,
            "price_change": parse_num(row[idx["漲跌價差"]]) if "漲跌價差" in idx else None,
            "turnover": parse_num(row[idx["成交金額"]]) if "成交金額" in idx else None,
            "volume": volume_lots,
        }
    return out


def upsert_day(conn, trade_date: date, quotes: dict[str, dict]) -> int:
    rows = []
    for code, q in quotes.items():
        if (
            q.get("close_price") is None
            and q.get("open_price") is None
            and not (q.get("turnover") or 0)
            and not (q.get("volume") or 0)
        ):
            continue
        rows.append(
            (
                trade_date,
                trade_date,
                code,
                q.get("warrant_name"),
                q.get("turnover"),
                q.get("volume"),
                q.get("open_price"),
                q.get("high_price"),
                q.get("low_price"),
                q.get("close_price"),
                q.get("price_change"),
            )
        )
    if not rows:
        return 0

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO tw_warrant_trade (
                out_date, trade_date, warrant_code, warrant_name,
                turnover, volume,
                open_price, high_price, low_price, close_price, price_change,
                updated_at
            ) VALUES %s
            ON CONFLICT (warrant_code, trade_date) DO UPDATE SET
                out_date = EXCLUDED.out_date,
                warrant_name = COALESCE(EXCLUDED.warrant_name, tw_warrant_trade.warrant_name),
                turnover = COALESCE(EXCLUDED.turnover, tw_warrant_trade.turnover),
                volume = COALESCE(EXCLUDED.volume, tw_warrant_trade.volume),
                open_price = COALESCE(EXCLUDED.open_price, tw_warrant_trade.open_price),
                high_price = COALESCE(EXCLUDED.high_price, tw_warrant_trade.high_price),
                low_price = COALESCE(EXCLUDED.low_price, tw_warrant_trade.low_price),
                close_price = COALESCE(EXCLUDED.close_price, tw_warrant_trade.close_price),
                price_change = COALESCE(EXCLUDED.price_change, tw_warrant_trade.price_change),
                updated_at = NOW()
            """,
            rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
            page_size=2000,
        )
    conn.commit()
    return len(rows)


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill TWSE warrant trades via MI_INDEX")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--sleep", type=float, default=2.5, help="Seconds between MI_INDEX calls")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip dates that already have rows in tw_warrant_trade",
    )
    args = parser.parse_args(argv)

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    if end < start:
        start, end = end, start

    db_url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ missing NEON_DATABASE_URL / DATABASE_URL", flush=True)
        return 1

    print(f"📅 backfill warrants {start} → {end}", flush=True)
    conn = psycopg2.connect(db_url)
    try:
        existing: set[date] = set()
        if args.skip_existing:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM tw_warrant_trade
                    WHERE trade_date BETWEEN %s AND %s
                    """,
                    (start, end),
                )
                for (d,) in cur.fetchall() or []:
                    if isinstance(d, datetime):
                        existing.add(d.date())
                    else:
                        existing.add(d)
            print(f"🗂 existing trade dates in range: {len(existing)}", flush=True)

        ok_days = 0
        skip_days = 0
        empty_days = 0
        total_rows = 0
        errors: list[str] = []

        days = list(daterange(start, end))
        for i, d in enumerate(days, 1):
            if d in existing:
                skip_days += 1
                print(f"[{i}/{len(days)}] skip existing {d}", flush=True)
                continue
            try:
                quotes = fetch_mi_index(d)
                if quotes is None:
                    empty_days += 1
                    print(f"[{i}/{len(days)}] no session {d}", flush=True)
                else:
                    n = upsert_day(conn, d, quotes)
                    if n == 0:
                        empty_days += 1
                        print(f"[{i}/{len(days)}] empty warrants {d}", flush=True)
                    else:
                        ok_days += 1
                        total_rows += n
                        print(f"[{i}/{len(days)}] {d} upserted {n}", flush=True)
            except Exception as exc:
                errors.append(f"{d}: {exc}")
                print(f"[{i}/{len(days)}] ERROR {d}: {exc}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
            time.sleep(max(0.0, args.sleep))

        print("=" * 60, flush=True)
        print(
            f"done ok_days={ok_days} skip={skip_days} empty={empty_days} "
            f"rows={total_rows} errors={len(errors)}",
            flush=True,
        )
        for e in errors[:20]:
            print(f"  - {e}", flush=True)
        return 1 if errors and ok_days == 0 else 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
