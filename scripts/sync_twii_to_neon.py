#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import date

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


SYMBOL = "^TWII"


def get_local_conn():
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "s8304021")
    database = os.getenv("DB_NAME", "postgres")
    sslmode = os.getenv("DB_SSLMODE", "prefer")
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        sslmode=sslmode,
    )


def get_neon_conn():
    url = (
        os.getenv("DATABASE_URL")
        or os.getenv("NEON_DATABASE_URL")
        or "postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
    )
    return psycopg2.connect(url)


def fetch_twii_from_local(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, date, open_price, high_price, low_price, close_price, volume
            FROM tw_stock_prices
            WHERE symbol = %s
            ORDER BY date
            """,
            (SYMBOL,),
        )
        prices = cur.fetchall()

        cur.execute(
            """
            SELECT symbol, date, daily_return, weekly_return, monthly_return, cumulative_return
            FROM tw_stock_returns
            WHERE symbol = %s
            ORDER BY date
            """,
            (SYMBOL,),
        )
        returns = cur.fetchall()
    return prices, returns


def upsert_twii_to_neon(conn, prices, returns):
    with conn.cursor() as cur:
        if prices:
            execute_values(
                cur,
                """
                INSERT INTO tw_stock_prices
                (symbol, date, open_price, high_price, low_price, close_price, volume)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume
                """,
                prices,
            )
        if returns:
            execute_values(
                cur,
                """
                INSERT INTO tw_stock_returns
                (symbol, date, daily_return, weekly_return, monthly_return, cumulative_return)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    daily_return = EXCLUDED.daily_return,
                    weekly_return = COALESCE(EXCLUDED.weekly_return, tw_stock_returns.weekly_return),
                    monthly_return = COALESCE(EXCLUDED.monthly_return, tw_stock_returns.monthly_return),
                    cumulative_return = COALESCE(EXCLUDED.cumulative_return, tw_stock_returns.cumulative_return)
                """,
                returns,
            )
    conn.commit()


def main():
    load_dotenv()
    try:
        local_conn = get_local_conn()
    except Exception as exc:
        print(f"[ERROR] Connect local DB failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        neon_conn = get_neon_conn()
    except Exception as exc:
        print(f"[ERROR] Connect Neon DB failed: {exc}", file=sys.stderr)
        local_conn.close()
        sys.exit(1)

    try:
        prices, returns = fetch_twii_from_local(local_conn)
        print(f"Fetched {len(prices)} price rows and {len(returns)} return rows for {SYMBOL} from local DB")
        upsert_twii_to_neon(neon_conn, prices, returns)
        print("Sync to Neon completed.")
    finally:
        local_conn.close()
        neon_conn.close()


if __name__ == "__main__":
    main()
