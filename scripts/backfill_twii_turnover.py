#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
from datetime import date, timedelta

import psycopg2
import requests
from dotenv import load_dotenv

from fetch_twii_index import fetch_twse_turnover_for_day, logger, FMTQIK_URL, parse_roc_date


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception:
        raise argparse.ArgumentTypeError("Invalid date format, use YYYY-MM-DD")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill ^TWII turnover (成交金額, 元) into tw_stock_prices.volume",
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        default=date(2010, 1, 1),
        help="Start date (YYYY-MM-DD), default: 2010-01-01",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        default=date.today(),
        help="End date (YYYY-MM-DD, inclusive), default: today",
    )
    args = parser.parse_args()

    start: date = args.start
    end: date = args.end
    if end < start:
        parser.error("--end must be >= --start")

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
        logger.exception("DB connect failed: %s", exc)
        sys.exit(1)

    logger.info("Start backfill ^TWII turnover from %s to %s", start, end)

    d = start
    month_cache = {}
    try:
        while d <= end:
            ma = d.replace(day=1)
            key = ma.isoformat()
            if key not in month_cache:
                try:
                    resp = requests.get(FMTQIK_URL, params={"response": "json", "date": ma.strftime("%Y%m%d")}, timeout=15)
                    data_map = {}
                    if resp.status_code == 200:
                        payload = resp.json()
                        if payload.get("stat") == "OK" and payload.get("data"):
                            for row in payload.get("data", []):
                                try:
                                    dt = parse_roc_date(row[0])
                                    if not dt:
                                        continue
                                    val = row[2] if len(row) > 2 else None
                                    if val not in (None, "", "--", "---"):
                                        data_map[dt] = int(str(val).replace(",", ""))
                                except Exception:
                                    pass
                    month_cache[key] = data_map
                except Exception:
                    month_cache[key] = {}

            vol = month_cache.get(key, {}).get(d)
            if not vol:
                vol = fetch_twse_turnover_for_day(d)
            if not vol:
                logger.info("No TWSE turnover for %s; skip", d)
                d += timedelta(days=1)
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tw_stock_prices (symbol, date, volume)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol, date) DO UPDATE
                        SET volume = EXCLUDED.volume
                        """,
                        ("^TWII", d, vol),
                    )
                conn.commit()
                logger.info("Upserted turnover for %s: %s", d, vol)
            except psycopg2.Error as exc:
                conn.rollback()
                logger.exception("Failed to upsert turnover for %s: %s", d, exc)

            d += timedelta(days=1)

    finally:
        conn.close()
        logger.info("Backfill finished for %s ~ %s", start, end)


if __name__ == "__main__":
    main()
