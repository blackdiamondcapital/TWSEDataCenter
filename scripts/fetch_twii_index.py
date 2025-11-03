#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import psycopg2
import requests
from psycopg2.extras import execute_values

LOG_PATH = "twii_index.log"

handlers = [
    logging.FileHandler(LOG_PATH, encoding="utf-8"),
    logging.StreamHandler(sys.stdout),
]
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

FMTQIK_URL = "https://www.twse.com.tw/exchangeReport/FMTQIK"


def parse_roc_date(roc_str: str):
    try:
        year, month, day = roc_str.split("/")
        return date(int(year) + 1911, int(month), int(day))
    except Exception:
        return None


def safe_float(value):
    if value in (None, "", "--", "---"):
        return None
    try:
        return float(value.replace(",", ""))
    except Exception:
        return None


def safe_volume(value):
    if value in (None, "", "--", "---"):
        return 0
    try:
        return int(value.replace(",", "")) * 1000  # 單位：千
    except Exception:
        return 0


def fetch_twii_for_day(target: date):
    logger.info("Fetch TWII for %s", target)
    params = {"response": "json", "date": target.replace(day=1).strftime("%Y%m%d")}
    resp = requests.get(FMTQIK_URL, params=params, timeout=15)
    resp.raise_for_status()

    payload = resp.json()
    if payload.get("stat") != "OK":
        logger.warning("FMTQIK stat not OK: %s", payload.get("stat"))
        return None

    for row in payload.get("data", []):
        row_date = parse_roc_date(row[0]) if row else None
        if row_date != target:
            continue

        close_price = safe_float(row[4] if len(row) > 4 else None)
        if close_price is None:
            logger.warning("FMTQIK row missing close price.")
            return None

        turnover = safe_volume(row[2] if len(row) > 2 else None)
        return (
            "^TWII",
            target,
            close_price,
            close_price,
            close_price,
            close_price,
            turnover,
        )
    logger.info("No TWII row for %s", target)
    return None


def upsert_twii_record(conn, record):
    sql = """
        INSERT INTO stock_prices
        (symbol, date, open_price, high_price, low_price, close_price, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, [record])
    conn.commit()


def upsert_twii_return(conn, record):
    symbol, target_date, *_rest = record
    close_price = record[5]

    daily_return = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close_price
                FROM stock_prices
                WHERE symbol = %s AND date < %s AND close_price IS NOT NULL
                ORDER BY date DESC
                LIMIT 1
                """,
                (symbol, target_date),
            )
            prev_row = cur.fetchone()

        if prev_row and prev_row[0] is not None and close_price is not None:
            prev_close = Decimal(str(prev_row[0]))
            curr_close = Decimal(str(close_price))
            if prev_close != 0:
                daily_return = (curr_close - prev_close) / prev_close
            else:
                logger.warning("Previous close is zero for %s on %s", symbol, target_date)
        else:
            logger.info(
                "No previous close for %s before %s; skip daily return",
                symbol,
                target_date,
            )
    except (InvalidOperation, psycopg2.Error) as exc:
        logger.exception(
            "Failed to compute daily return for %s on %s: %s",
            symbol,
            target_date,
            exc,
        )
        daily_return = None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_returns
                (symbol, date, daily_return, weekly_return, monthly_return, cumulative_return)
                VALUES (%s, %s, %s, NULL, NULL, NULL)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    daily_return = EXCLUDED.daily_return,
                    weekly_return = COALESCE(EXCLUDED.weekly_return, stock_returns.weekly_return),
                    monthly_return = COALESCE(EXCLUDED.monthly_return, stock_returns.monthly_return),
                    cumulative_return = COALESCE(EXCLUDED.cumulative_return, stock_returns.cumulative_return)
                """,
                (symbol, target_date, daily_return),
            )
        conn.commit()
        if daily_return is not None:
            logger.info("✅ TWII daily return %.6f synced for %s", daily_return, target_date)
        else:
            logger.info("⚠️ TWII daily return unavailable for %s", target_date)
    except psycopg2.Error as exc:
        conn.rollback()
        logger.exception(
            "Upsert TWII return failed for %s on %s: %s",
            symbol,
            target_date,
            exc,
        )


def main():
    logger.info("=" * 80)
    logger.info("🚀 Start TWII fetch job at %s", datetime.now())
    target = date.today()
    logger.info("📅 抓取日期：%s", target)

    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        logger.error("NEON_DATABASE_URL is not set")
        sys.exit(1)

    try:
        record = fetch_twii_for_day(target)
        if not record:
            logger.info("No TWII data available today.")
            return
    except Exception as exc:
        logger.exception("TWII fetch failed: %s", exc)
        sys.exit(1)

    try:
        conn = psycopg2.connect(url)
        upsert_twii_record(conn, record)
        upsert_twii_return(conn, record)
        conn.close()
        logger.info("✅ TWII data & return synced for %s", target)
    except Exception as exc:
        logger.exception("Database sync failed: %s", exc)
        sys.exit(1)

    logger.info("🎉 TWII job finished successfully")


if __name__ == "__main__":
    main()
