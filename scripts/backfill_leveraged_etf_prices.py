#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回補上市槓桿/反向 ETF 與缺 K 線之 ETF（00631L、00632R、006208 等）。

依 TWSE STOCK_DAY 逐月抓取，寫入 tw_stock_prices（預設使用 .env 的 DATABASE_URL / NEON）。

用法：
  cd 台股數據資料抓取＿桌機
  python3 scripts/backfill_leveraged_etf_prices.py
  python3 scripts/backfill_leveraged_etf_prices.py --dry-run
  python3 scripts/backfill_leveraged_etf_prices.py --symbols 00631L,00632R
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(ROOT, ".env"))

from server import (  # noqa: E402
    DEFAULT_START_DATE,
    DatabaseManager,
    StockAPI,
    _upsert_prices,
)

_SUFFIX_RE = re.compile(r"\.(TW|TWO|TPEX|TSE)$", re.I)
_NAME_ETF_HINT = re.compile(r"ETF|指數股票型|正\d|反\d|槓桿|反向|2倍|2X|3倍|3X", re.I)
_LEVERAGED_CODE = re.compile(r"^\d{4,5}[LRK]$")


def log(msg: str) -> None:
    print(msg, flush=True)


def base_code(symbol: str) -> str:
    return _SUFFIX_RE.sub("", str(symbol or "").strip().upper())


def tw_symbol(base: str) -> str:
    b = base_code(base)
    return b if b.endswith(".TW") else f"{b}.TW"


def is_leveraged_candidate(base: str, name: str = "") -> bool:
    if not StockAPI.is_twse_listing_code(base):
        return False
    if _LEVERAGED_CODE.fullmatch(base):
        return True
    label = str(name or "")
    if re.search(r"正\d|反\d|槓桿|反向", label):
        return True
    # 期信/結構型：00635U 等，名稱常含「期」
    if re.fullmatch(r"^\d{4,5}[U]$", base) and "期" in label:
        return True
    return False


def is_etf_candidate(base: str, name: str = "") -> bool:
    if not StockAPI.is_twse_listing_code(base):
        return False
    label = str(name or "")
    if is_leveraged_candidate(base, name):
        return True
    if _NAME_ETF_HINT.search(label):
        return True
    if base.startswith("00") and re.fullmatch(r"\d{4,6}", base):
        return True
    return False


def load_price_counts(conn, bases: list[str]) -> dict[str, int]:
    if not bases:
        return {}
    bases = sorted(set(bases))
    out: dict[str, int] = {b: 0 for b in bases}
    chunk = 400
    with conn.cursor() as cur:
        for i in range(0, len(bases), chunk):
            part = bases[i : i + chunk]
            cur.execute(
                """
                SELECT regexp_replace(upper(trim(symbol)), '\\.(TW|TWO|TPEX|TSE)$', '', 'i') AS base,
                       COUNT(*)::int AS cnt
                FROM tw_stock_prices
                WHERE regexp_replace(upper(trim(symbol)), '\\.(TW|TWO|TPEX|TSE)$', '', 'i') = ANY(%s)
                GROUP BY 1
                """,
                (part,),
            )
            for row in cur.fetchall():
                if isinstance(row, dict):
                    base = row.get("base")
                    cnt = row.get("cnt")
                else:
                    base, cnt = row[0], row[1]
                out[str(base).upper()] = int(cnt or 0)
    return out


def discover_from_isin(api: StockAPI, mode: str = "leveraged") -> list[dict]:
    rows = api.fetch_twse_symbols()
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        sym = tw_symbol(row.get("symbol") or "")
        base = base_code(sym)
        if base in seen:
            continue
        name = str(row.get("name") or "")
        if mode == "leveraged" and not is_leveraged_candidate(base, name):
            continue
        if mode == "etf" and not is_etf_candidate(base, name):
            continue
        if mode == "all" and not StockAPI.is_twse_listing_code(base):
            continue
        seen.add(base)
        out.append({"symbol": sym, "name": name, "source": "isin"})
    return out


def discover_from_db(conn) -> list[dict]:
    sql = """
        WITH priced AS (
          SELECT symbol, COUNT(*)::int AS cnt
          FROM tw_stock_prices
          GROUP BY symbol
        )
        SELECT s.symbol,
               COALESCE(s.short_name, s.name, '') AS name,
               COALESCE(p.cnt, 0) AS price_cnt
        FROM tw_stock_symbols s
        LEFT JOIN priced p ON p.symbol = s.symbol
        WHERE lower(trim(COALESCE(s.market, ''))) IN (
          'listed', 'twse', 'tse', '上市', 'etf'
        )
    """
    out: list[dict] = []
    seen: set[str] = set()
    with conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            if isinstance(row, dict):
                symbol = row.get("symbol")
                name = row.get("name")
                price_cnt = row.get("price_cnt")
            else:
                symbol, name, price_cnt = row[0], row[1], row[2]
            base = base_code(symbol)
            if base in seen:
                continue
            if not is_etf_candidate(base, name):
                continue
            seen.add(base)
            out.append(
                {
                    "symbol": tw_symbol(base),
                    "name": name,
                    "price_cnt": int(price_cnt or 0),
                    "source": "db",
                }
            )
    return out


def merge_targets(
    isin_rows: list[dict],
    db_rows: list[dict],
    price_counts: dict[str, int],
    min_records: int,
    mode: str,
) -> list[dict]:
    by_base: dict[str, dict] = {}
    for row in isin_rows + db_rows:
        sym = tw_symbol(row["symbol"])
        base = base_code(sym)
        name = str(row.get("name") or "")
        if mode == "leveraged" and not is_leveraged_candidate(base, name):
            continue
        if mode == "etf" and not is_etf_candidate(base, name):
            continue
        prev = by_base.get(base)
        by_base[base] = {
            "symbol": sym,
            "name": name or (prev or {}).get("name") or "",
            "price_cnt": int(price_counts.get(base, 0)),
        }
    targets = [v for v in by_base.values() if v["price_cnt"] < min_records]
    targets.sort(key=lambda x: (x["price_cnt"], x["symbol"]))
    return targets


def df_to_records(df) -> list[dict]:
    if df is None or getattr(df, "empty", True):
        return []
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "Date": row.get("Date") if "Date" in row else row.get("date"),
                "Open": row.get("Open") if "Open" in row else row.get("open_price"),
                "High": row.get("High") if "High" in row else row.get("high_price"),
                "Low": row.get("Low") if "Low" in row else row.get("low_price"),
                "Close": row.get("Close") if "Close" in row else row.get("close_price"),
                "Volume": row.get("Volume") if "Volume" in row else row.get("volume"),
            }
        )
    return records


def upsert_symbol_meta(cur, symbol: str, name: str) -> None:
    cur.execute(
        """
        INSERT INTO tw_stock_symbols (symbol, name, short_name, market)
        VALUES (%s, %s, %s, 'listed')
        ON CONFLICT (symbol) DO UPDATE SET
          name = COALESCE(NULLIF(TRIM(EXCLUDED.name), ''), tw_stock_symbols.name),
          short_name = COALESCE(NULLIF(TRIM(EXCLUDED.short_name), ''), tw_stock_symbols.short_name),
          market = COALESCE(NULLIF(TRIM(EXCLUDED.market), ''), tw_stock_symbols.market)
        """,
        (symbol, name, name),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="回補槓桿/反向與缺資料 ETF 日 K")
    parser.add_argument("--start", default=DEFAULT_START_DATE, help="起始日 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="結束日，預設今天")
    parser.add_argument("--min-records", type=int, default=200, help="低於此筆數才回補")
    parser.add_argument("--symbols", default="", help="只處理指定代號，逗號分隔（可不含 .TW）")
    parser.add_argument("--sleep", type=float, default=0.35, help="每檔間隔秒數")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--mode",
        choices=("leveraged", "etf", "all"),
        default="leveraged",
        help="leveraged=槓桿/反向優先（預設）；etf=所有 00 開頭等；all=ISIN 可交易代號",
    )
    parser.add_argument(
        "--batch-index",
        type=int,
        default=0,
        help="GitHub Actions 分批：第幾批（0 起算）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="每批檔數；0 表示不分批",
    )
    args = parser.parse_args()

    end_date = args.end or datetime.now().strftime("%Y-%m-%d")
    api = StockAPI()
    db = DatabaseManager()
    if not db.connect():
        log("❌ 資料庫連線失敗，請確認 .env 的 DATABASE_URL / DB_*")
        return 1
    db.create_tables()

    mode_key = "all" if args.mode == "all" else args.mode
    isin_rows = discover_from_isin(api, mode=mode_key)
    db_rows = discover_from_db(db.connection)
    merged_bases: set[str] = set()
    for row in isin_rows + db_rows:
        merged_bases.add(base_code(row["symbol"]))
    price_counts = load_price_counts(db.connection, list(merged_bases))
    targets = merge_targets(
        isin_rows,
        db_rows,
        price_counts,
        args.min_records,
        "all" if args.mode == "all" else args.mode,
    )

    if args.symbols.strip():
        allow = {base_code(s) for s in args.symbols.split(",") if s.strip()}
        targets = [t for t in targets if base_code(t["symbol"]) in allow]
        for base in sorted(allow):
            if not any(base_code(t["symbol"]) == base for t in targets):
                targets.append({"symbol": tw_symbol(base), "name": "", "price_cnt": 0})

    total_targets = len(targets)
    if args.batch_size and args.batch_size > 0:
        start_i = max(0, args.batch_index) * args.batch_size
        targets = targets[start_i : start_i + args.batch_size]
        log(
            f"📦 分批 batch_index={args.batch_index} batch_size={args.batch_size} "
            f"→ 本批 {len(targets)} 檔（全體 {total_targets} 檔）"
        )

    log(f"📋 ISIN ETF 候選 {len(isin_rows)} 檔；DB 候選 {len(db_rows)} 檔")
    log(f"🎯 待回補（現有 K 線 < {args.min_records} 筆）: {len(targets)} 檔")
    for t in targets[:30]:
        log(f"   - {t['symbol']} ({t.get('name') or '—'}) 現有 {t.get('price_cnt', 0)} 筆")
    if len(targets) > 30:
        log(f"   … 其餘 {len(targets) - 30} 檔")

    if args.dry_run:
        db.disconnect()
        return 0

    cursor = db.connection.cursor()
    cursor.table_prices = db.table_prices
    ok = 0
    fail = 0
    inserted_total = 0

    for idx, item in enumerate(targets, start=1):
        sym = item["symbol"]
        name = item.get("name") or ""
        log(f"\n[{idx}/{len(targets)}] 抓取 {sym} …")
        try:
            upsert_symbol_meta(cursor, sym, name)
            df = api.fetch_stock_data(sym, args.start, end_date)
            records = df_to_records(df)
            if not records:
                log(f"   ⚠️ 無資料")
                fail += 1
                db.connection.commit()
                time.sleep(args.sleep)
                continue
            n = _upsert_prices(cursor, sym, records, prices_table=db.table_prices)
            db.connection.commit()
            inserted_total += n
            ok += 1
            log(f"   ✅ 寫入 {n} 筆（區間 {args.start} ~ {end_date}）")
        except Exception as exc:
            db.connection.rollback()
            fail += 1
            log(f"   ❌ 失敗: {exc}")
        time.sleep(args.sleep)

    cursor.close()
    db.disconnect()
    log(f"\n完成：成功 {ok} 檔、失敗 {fail} 檔，本次 upsert 列數約 {inserted_total}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
