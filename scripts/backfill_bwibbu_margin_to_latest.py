#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 tw_stock_bwibbu、margin_trades 從庫內最新日補到股價最新交易日。"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(ROOT, ".env"))

# findb / 主站實際讀的是 margin_trades，不是空的 tw_margin_trades
os.environ.setdefault("NEON_MARGIN_TRADES_TABLE", "margin_trades")
os.environ.setdefault("MARGIN_TRADES_TABLE", "margin_trades")

LOG_PATH = os.path.join(ROOT, "bwibbu_margin_scheduled.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def trading_dates(cur, after: date, until: date) -> list[date]:
    cur.execute(
        """
        SELECT DISTINCT date
        FROM tw_stock_prices
        WHERE symbol IN ('2330', '2330.TW')
          AND date > %s
          AND date <= %s
        ORDER BY date
        """,
        (after, until),
    )
    out = []
    for row in cur.fetchall():
        value = row["date"] if isinstance(row, dict) else row[0]
        if value:
            out.append(value)
    return out


def max_date(cur, table: str, col: str = "date"):
    cur.execute(f"SELECT MAX({col}) AS mx FROM {table}")
    row = cur.fetchone()
    if not row:
        return None
    return row["mx"] if isinstance(row, dict) else row[0]


def backfill_bwibbu(stock_api, db, dates: list[date], sleep_seconds: float) -> dict:
    inserted = 0
    empty = 0
    failed = 0
    for i, day in enumerate(dates, start=1):
        try:
            twse = stock_api.fetch_twse_bwibbu_by_date(day) or []
            tpex = stock_api.fetch_tpex_bwibbu_by_date(day) or []
            records = twse + tpex
            if not records:
                empty += 1
                logger.info("BWIBBU %s 無資料 (%d/%d)", day, i, len(dates))
            else:
                n = stock_api.upsert_bwibbu_records(records, db_manager=db)
                inserted += n
                logger.info("BWIBBU %s 寫入 %d 筆（TWSE %d / TPEX %d）(%d/%d)", day, n, len(twse), len(tpex), i, len(dates))
        except Exception as exc:
            failed += 1
            logger.warning("BWIBBU %s 失敗: %s", day, exc)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return {"inserted": inserted, "empty": empty, "failed": failed, "days": len(dates)}


def backfill_margin(stock_api, db, dates: list[date], sleep_seconds: float) -> dict:
    inserted = 0
    empty = 0
    failed = 0
    for i, day in enumerate(dates, start=1):
        try:
            records, _, _ = stock_api.fetch_margin_range(day, day, market="both", sleep_seconds=0)
            if not records:
                empty += 1
                logger.info("融資融券 %s 無資料 (%d/%d)", day, i, len(dates))
            else:
                n = stock_api.upsert_margin_records(records, db_manager=db)
                inserted += n
                logger.info("融資融券 %s 寫入 %d 筆 (%d/%d)", day, n, i, len(dates))
        except Exception as exc:
            failed += 1
            logger.warning("融資融券 %s 失敗: %s", day, exc)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return {"inserted": inserted, "empty": empty, "failed": failed, "days": len(dates)}


def weekday_range(after: date, until: date) -> list[date]:
    days = []
    day = after + timedelta(days=1)
    while day <= until:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


def run(args: argparse.Namespace) -> int:
    from server import DatabaseManager, stock_api

    # GitHub 週五排程常延到週六凌晨；週末仍應補齊庫內缺的交易日。
    now = datetime.now()
    logger.info("開始補齊 BWIBBU／融資融券（%s）", now.strftime("%Y-%m-%d %H:%M"))

    db = DatabaseManager(use_local=False)
    if not db.connect():
        raise RuntimeError("資料庫連線失敗")
    logger.info("margin 寫入表：%s", db.table_margin)

    try:
        cur = db.connection.cursor()
        latest = max_date(cur, "tw_stock_prices")
        if args.until:
            latest = date.fromisoformat(args.until)
        if not latest:
            raise RuntimeError("找不到股價最新日")

        bwibbu_from = date.fromisoformat(args.bwibbu_from) if args.bwibbu_from else max_date(cur, "tw_stock_bwibbu")
        margin_from = date.fromisoformat(args.margin_from) if args.margin_from else max_date(cur, db.table_margin)
        if bwibbu_from is None:
            bwibbu_from = latest - timedelta(days=7)
        if margin_from is None:
            margin_from = latest - timedelta(days=7)

        logger.info("股價最新交易日 %s；BWIBBU 庫內 %s；融資融券 庫內 %s", latest, bwibbu_from, margin_from)

        if not args.skip_bwibbu:
            days = trading_dates(cur, bwibbu_from, latest) or weekday_range(bwibbu_from, latest)
            logger.info("BWIBBU 待補 %d 個交易日", len(days))
            stats = backfill_bwibbu(stock_api, db, days, args.sleep)
            logger.info("BWIBBU 結果 %s", stats)

        if not args.skip_margin:
            days = trading_dates(cur, margin_from, latest) or weekday_range(margin_from, latest)
            logger.info("融資融券 待補 %d 個交易日", len(days))
            stats = backfill_margin(stock_api, db, days, args.sleep)
            logger.info("融資融券 結果 %s", stats)

        cur.execute("SELECT MAX(date) AS mx FROM tw_stock_bwibbu")
        logger.info("完成後 BWIBBU MAX(date)=%s", cur.fetchone())
        cur.execute(f"SELECT MAX(date) AS mx FROM {db.table_margin}")
        logger.info("完成後 %s MAX(date)=%s", db.table_margin, cur.fetchone())
        return 0
    finally:
        db.disconnect()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--until", default="")
    parser.add_argument("--bwibbu-from", default="")
    parser.add_argument("--margin-from", default="")
    parser.add_argument("--sleep", type=float, default=0.45)
    parser.add_argument("--skip-bwibbu", action="store_true")
    parser.add_argument("--skip-margin", action="store_true")
    parser.add_argument("--include-weekend", action="store_true", help="相容舊參數；週末一律補缺的交易日")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
