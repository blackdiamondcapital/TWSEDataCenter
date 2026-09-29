#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回補上市 ETF 日 K（槓桿／反向／債券／主動型等 00xxxx）。

依 TWSE STOCK_DAY 逐月抓取，寫入 tw_stock_prices（.env 的 DATABASE_URL / NEON）。

用法：
  python3 scripts/backfill_leveraged_etf_prices.py --mode etf --dry-run
  python3 scripts/backfill_leveraged_etf_prices.py --mode etf --refresh-stale
  python3 scripts/backfill_leveraged_etf_prices.py --symbols 00631L,00710B
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta

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
_NAME_EXCLUDE = re.compile(r"購|售|認購|認售|牛熊|權證|受益憑證")
_LEVERAGED_CODE = re.compile(r"^00\d{2,4}[LRK]$")
_FUTURES_ETF_CODE = re.compile(r"^00\d{2,4}U$")
_PLAIN_ETF_CODE = re.compile(r"^00\d{2,4}$")
_LETTER_ETF_CODE = re.compile(r"^00\d{2,4}[A-Z]$")


def log(msg: str) -> None:
    print(msg, flush=True)


def base_code(symbol: str) -> str:
    return _SUFFIX_RE.sub("", str(symbol or "").strip().upper())


def tw_symbol(base: str) -> str:
    b = base_code(base)
    return b if b.endswith(".TW") else f"{b}.TW"


def is_excluded_derivative_name(name: str) -> bool:
    return bool(_NAME_EXCLUDE.search(str(name or "")))


def is_leveraged_candidate(base: str, name: str = "") -> bool:
    if not StockAPI.is_twse_listing_code(base):
        return False
    if is_excluded_derivative_name(name):
        return False
    if _LEVERAGED_CODE.fullmatch(base):
        return True
    label = str(name or "")
    if _FUTURES_ETF_CODE.fullmatch(base) and ("期" in label or re.search(r"正\d|反\d|槓桿|反向", label)):
        return True
    if base.startswith("00") and re.search(r"正\d|反\d|槓桿|反向", label):
        return True
    return False


def is_etf_candidate(base: str, name: str = "") -> bool:
    if not StockAPI.is_twse_listing_code(base):
        return False
    if is_excluded_derivative_name(name):
        return False
    if is_leveraged_candidate(base, name):
        return True
    if _PLAIN_ETF_CODE.fullmatch(base) or _LETTER_ETF_CODE.fullmatch(base):
        return True
    label = str(name or "")
    if base.startswith("00") and _NAME_ETF_HINT.search(label):
        return True
    return False


def load_price_meta(conn, bases: list[str]) -> dict[str, dict]:
    if not bases:
        return {}
    bases = sorted(set(bases))
    out: dict[str, dict] = {b: {"cnt": 0, "latest": None} for b in bases}
    chunk = 400
    with conn.cursor() as cur:
        for i in range(0, len(bases), chunk):
            part = bases[i : i + chunk]
            cur.execute(
                """
                SELECT regexp_replace(upper(trim(symbol)), '\\.(TW|TWO|TPEX|TSE)$', '', 'i') AS base,
                       COUNT(*)::int AS cnt,
                       MAX(date)::date AS latest
                FROM tw_stock_prices
                WHERE regexp_replace(upper(trim(symbol)), '\\.(TW|TWO|TPEX|TSE)$', '', 'i') = ANY(%s)
                GROUP BY 1
                """,
                (part,),
            )
            for row in cur.fetchall():
                if isinstance(row, dict):
                    base, cnt, latest = row.get("base"), row.get("cnt"), row.get("latest")
                else:
                    base, cnt, latest = row[0], row[1], row[2]
                out[str(base).upper()] = {
                    "cnt": int(cnt or 0),
                    "latest": latest.isoformat() if hasattr(latest, "isoformat") else (str(latest)[:10] if latest else None),
                }
    return out


def market_latest_date(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date)::date AS d FROM tw_stock_prices")
        row = cur.fetchone()
        if not row:
            return None
        d = row["d"] if isinstance(row, dict) else row[0]
        if not d:
            return None
        return d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]


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
        SELECT s.symbol,
               COALESCE(s.short_name, s.name, '') AS name
        FROM tw_stock_symbols s
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
            else:
                symbol, name = row[0], row[1]
            base = base_code(symbol)
            if base in seen:
                continue
            if not is_etf_candidate(base, name):
                continue
            seen.add(base)
            out.append({"symbol": tw_symbol(base), "name": name, "source": "db"})
    return out


def merge_targets(
    isin_rows: list[dict],
    db_rows: list[dict],
    price_meta: dict[str, dict],
    min_records: int,
    mode: str,
    *,
    refresh_stale: bool = False,
    market_as_of: str | None = None,
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
        meta = price_meta.get(base) or {"cnt": 0, "latest": None}
        prev = by_base.get(base)
        by_base[base] = {
            "symbol": sym,
            "name": name or (prev or {}).get("name") or "",
            "price_cnt": int(meta.get("cnt") or 0),
            "latest": meta.get("latest"),
        }

    targets = []
    for v in by_base.values():
        need = v["price_cnt"] < min_records
        if refresh_stale and market_as_of:
            latest = v.get("latest")
            if latest is None or str(latest)[:10] < market_as_of:
                need = True
        if need:
            targets.append(v)
    targets.sort(key=lambda x: (0 if not x.get("latest") else 1, x.get("latest") or "", x["price_cnt"], x["symbol"]))
    return targets


def fetch_start_for_target(item: dict, default_start: str) -> str:
    """已有歷史者從最新日往前幾天續抓，加快過期補齊。"""
    latest = item.get("latest")
    if not latest:
        return default_start
    try:
        dt = datetime.strptime(str(latest)[:10], "%Y-%m-%d")
        start = (dt - timedelta(days=7)).strftime("%Y-%m-%d")
        return max(default_start, start) if default_start else start
    except Exception:
        return default_start


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


def reconnect_db(db: DatabaseManager) -> None:
    try:
        db.disconnect()
    except Exception:
        pass
    if not db.connect():
        raise RuntimeError("資料庫重新連線失敗")
    db.create_tables()


def safe_rollback(db: DatabaseManager) -> None:
    try:
        if db.connection and not db.connection.closed:
            db.connection.rollback()
    except Exception:
        pass


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
    parser = argparse.ArgumentParser(description="回補／續補上市 ETF 日 K")
    parser.add_argument("--start", default=DEFAULT_START_DATE, help="起始日 YYYY-MM-DD（無歷史時用）")
    parser.add_argument("--end", default=None, help="結束日，預設今天")
    parser.add_argument("--min-records", type=int, default=200, help="低於此筆數才回補")
    parser.add_argument(
        "--refresh-stale",
        action="store_true",
        help="連同「最新日 < 市場最新交易日」的標的一併續補",
    )
    parser.add_argument("--symbols", default="", help="只處理指定代號，逗號分隔")
    parser.add_argument("--sleep", type=float, default=0.35, help="每檔間隔秒數")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--mode",
        choices=("leveraged", "etf", "all"),
        default="leveraged",
        help="leveraged / etf / all",
    )
    parser.add_argument("--batch-index", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=0, help="每批檔數；0=不分批")
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
    price_meta = load_price_meta(db.connection, list(merged_bases))
    as_of = market_latest_date(db.connection)
    targets = merge_targets(
        isin_rows,
        db_rows,
        price_meta,
        args.min_records,
        mode_key,
        refresh_stale=args.refresh_stale,
        market_as_of=as_of,
    )

    if args.symbols.strip():
        allow = {base_code(s) for s in args.symbols.split(",") if s.strip()}
        targets = [t for t in targets if base_code(t["symbol"]) in allow]
        for base in sorted(allow):
            if not any(base_code(t["symbol"]) == base for t in targets):
                meta = price_meta.get(base) or {"cnt": 0, "latest": None}
                targets.append(
                    {
                        "symbol": tw_symbol(base),
                        "name": "",
                        "price_cnt": int(meta.get("cnt") or 0),
                        "latest": meta.get("latest"),
                    }
                )

    total_targets = len(targets)
    if args.batch_size and args.batch_size > 0:
        start_i = max(0, args.batch_index) * args.batch_size
        targets = targets[start_i : start_i + args.batch_size]
        log(
            f"📦 分批 batch_index={args.batch_index} batch_size={args.batch_size} "
            f"→ 本批 {len(targets)} 檔（全體 {total_targets} 檔）"
        )

    log(f"📋 ISIN ETF 候選 {len(isin_rows)} 檔；DB 候選 {len(db_rows)} 檔")
    log(f"📅 市場最新交易日：{as_of or '—'}")
    hint = f"K 線 < {args.min_records}"
    if args.refresh_stale:
        hint += " 或 最新日過期"
    log(f"🎯 待回補（{hint}）: {len(targets)} 檔")
    for t in targets[:30]:
        log(
            f"   - {t['symbol']} ({t.get('name') or '—'}) "
            f"現有 {t.get('price_cnt', 0)} 筆 latest={t.get('latest') or '—'}"
        )
    if len(targets) > 30:
        log(f"   … 其餘 {len(targets) - 30} 檔")

    if args.dry_run:
        db.disconnect()
        return 0

    if not targets:
        db.disconnect()
        log("ℹ️  本批無待回補標的")
        return 0

    db.disconnect()
    ok = 0
    skipped = 0
    hard_fail = 0
    inserted_total = 0

    for idx, item in enumerate(targets, start=1):
        sym = item["symbol"]
        name = item.get("name") or ""
        range_start = fetch_start_for_target(item, args.start)
        log(f"\n[{idx}/{len(targets)}] 抓取 {sym} （{range_start} ~ {end_date}）…")
        cursor = None
        try:
            df = api.fetch_stock_data(sym, range_start, end_date)
            records = df_to_records(df)
            if not records:
                log("   ⚠️ 無資料（略過，不計失敗）")
                skipped += 1
                time.sleep(args.sleep)
                continue

            reconnect_db(db)
            cursor = db.connection.cursor()
            cursor.table_prices = db.table_prices
            upsert_symbol_meta(cursor, sym, name)
            n = _upsert_prices(cursor, sym, records, prices_table=db.table_prices)
            db.connection.commit()
            inserted_total += n
            ok += 1
            log(f"   ✅ 寫入 {n} 筆（區間 {range_start} ~ {end_date}）")
        except Exception as exc:
            safe_rollback(db)
            hard_fail += 1
            log(f"   ❌ 失敗: {exc}")
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            try:
                db.disconnect()
            except Exception:
                pass
        time.sleep(args.sleep)

    log(
        f"\n完成：成功 {ok} 檔、略過 {skipped} 檔、失敗 {hard_fail} 檔，"
        f"本次 upsert 列數約 {inserted_total}"
    )
    return 0 if hard_fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
