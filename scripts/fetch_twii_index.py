#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

import argparse
import requests
import psycopg2
from psycopg2.extras import execute_values
import yfinance as yf
from dotenv import load_dotenv

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
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def safe_volume(value):
    if value in (None, "", "--", "---"):
        return 0
    try:
        # yfinance Volume 已是股數，不需再乘 1000
        return int(float(str(value).replace(",", "")))
    except Exception:
        return 0


def fetch_twse_turnover_for_day(target: date):
    """從證交所 FMTQIK 取當日加權指數成交金額(千元) -> 轉成元回傳。
    若無法取得則回傳 None。
    """
    try:
        month_anchor = target.replace(day=1)
        params = {"response": "json", "date": month_anchor.strftime("%Y%m%d")}
        resp = requests.get(FMTQIK_URL, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        payload = resp.json()
        if payload.get("stat") != "OK" or not payload.get("data"):
            return None
        for row in payload.get("data", []):
            # row[0] 民國日期, row[2] 成交金額(千元)
            try:
                roc = row[0]
                y, m, d = roc.split("/")
                greg = date(int(y) + 1911, int(m), int(d))
            except Exception:
                continue
            if greg != target:
                continue
            val = row[2] if len(row) > 2 else None
            if val in (None, "", "--", "---"):
                return None
            try:
                return int(val.replace(",", ""))
            except Exception:
                return None
        return None
    except Exception:
        return None


def fetch_twii_for_day(target: date):
    logger.info("Fetch ^TWII from yfinance for %s", target)
    start = target.strftime("%Y-%m-%d")
    end = (target + timedelta(days=1)).strftime("%Y-%m-%d")  # yfinance end 為開區間
    try:
        ticker = yf.Ticker("^TWII")
        df = ticker.history(start=start, end=end, auto_adjust=False)
    except Exception as exc:
        logger.exception("yfinance fetch failed for %s: %s", target, exc)
        return None

    if df is None or df.empty:
        logger.info("No ^TWII data from yfinance for %s", target)
        return None

    row = df.iloc[0]
    open_price = safe_float(row.get("Open"))
    high_price = safe_float(row.get("High"))
    low_price = safe_float(row.get("Low"))
    close_price = safe_float(row.get("Close"))
    volume = fetch_twse_turnover_for_day(target) or 0

    if close_price is None:
        logger.warning("yfinance row missing close price for %s", target)
        return None

    

    return (
        "^TWII",
        target,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
    )


def upsert_twii_record(conn, record):
    sql = """
        INSERT INTO tw_stock_prices
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
                FROM tw_stock_prices
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
            logger.info("No previous close for %s before %s; skip daily return", symbol, target_date)
    except (InvalidOperation, psycopg2.Error) as exc:
        logger.exception("Failed to compute daily return for %s on %s: %s", symbol, target_date, exc)
        daily_return = None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tw_stock_returns
                (symbol, date, daily_return, weekly_return, monthly_return, cumulative_return)
                VALUES (%s, %s, %s, NULL, NULL, NULL)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    daily_return = EXCLUDED.daily_return,
                    weekly_return = COALESCE(EXCLUDED.weekly_return, tw_stock_returns.weekly_return),
                    monthly_return = COALESCE(EXCLUDED.monthly_return, tw_stock_returns.monthly_return),
                    cumulative_return = COALESCE(EXCLUDED.cumulative_return, tw_stock_returns.cumulative_return)
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
        logger.exception("Upsert TWII return failed for %s on %s: %s", symbol, target_date, exc)


def main():
    logger.info("=" * 80)
    logger.info("🚀 Start TWII fetch job at %s", datetime.now())
    # 支援指定日期：優先讀取 CLI --date，其次環境變數 TWII_TARGET_DATE，否則使用今天
    parser = argparse.ArgumentParser(description="Fetch ^TWII for a specific date (default: today)")
    parser.add_argument("--date", "-d", dest="date_str", help="Target date in YYYY-MM-DD")
    args = parser.parse_args()

    target = date.today()
    env_date = os.getenv("TWII_TARGET_DATE")
    try:
        if args.date_str:
            target = date.fromisoformat(args.date_str)
        elif env_date:
            target = date.fromisoformat(env_date)
    except Exception:
        logger.error("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)

    logger.info("📅 抓取日期：%s", target)

    load_dotenv()
    url = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
    conn = None
    try:
        if url:
            conn = psycopg2.connect(url)
        else:
            host = os.getenv("DB_HOST", "localhost")
            port = int(os.getenv("DB_PORT", "5432"))
            user = os.getenv("DB_USER", "postgres")
            password = os.getenv("DB_PASSWORD", "s8304021")
            database = os.getenv("DB_NAME", "postgres")
            sslmode = os.getenv("DB_SSLMODE", "prefer")
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                sslmode=sslmode,
            )
    except Exception as exc:
        logger.exception("Database connection failed: %s", exc)
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
