#!/usr/bin/env python3
"""Idempotently backfill TTM financial ratios from Neon statement tables."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from financial_ratios_service import (  # noqa: E402
    FINANCIAL_RATIO_COLS,
    compute_records_from_connection,
    fetch_symbol_codes,
    format_period,
    period_ordinal,
)
from table_config import (  # noqa: E402
    balance_sheet_table,
    cash_flow_table,
    financial_ratios_table,
    income_statement_table,
)

load_dotenv(os.path.join(ROOT, ".env"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("financial-ratios-backfill")


def periods_between(start: str, end: str) -> list[str]:
    start_ordinal, end_ordinal = period_ordinal(start), period_ordinal(end)
    if start_ordinal > end_ordinal:
        raise ValueError("--from-period 不可晚於 --to-period")
    return [format_period(value) for value in range(start_ordinal, end_ordinal + 1)]


def ensure_contract(connection, table: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol VARCHAR(20) NOT NULL,
                period VARCHAR(16) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, period)
            )
            """
        )
        for column in FINANCIAL_RATIO_COLS:
            cursor.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                f"{column} NUMERIC(20,10)"
            )
    connection.commit()


def upsert_records(connection, table: str, records: list[dict]) -> int:
    if not records:
        return 0
    columns = ["symbol", "period", *FINANCIAL_RATIO_COLS]
    values = [tuple(record.get(column) for column in columns) for record in records]
    update_sql = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in FINANCIAL_RATIO_COLS
    )
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            f"""
            INSERT INTO {table} ({", ".join(columns)}) VALUES %s
            ON CONFLICT (symbol, period) DO UPDATE SET
                {update_sql},
                updated_at = CURRENT_TIMESTAMP
            """,
            values,
            page_size=500,
        )
    connection.commit()
    return len(values)


def batched(values: list[str], size: int):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def run(args: argparse.Namespace) -> int:
    if args.shard_count < 1:
        raise ValueError("--shard-count 必須至少為 1")
    if not 0 <= args.shard_index < args.shard_count:
        raise ValueError("--shard-index 必須介於 0 與 shard-count-1")
    if args.batch_size < 1:
        raise ValueError("--batch-size 必須至少為 1")

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if db_url:
        connection_args = (db_url,)
        connection_kwargs = {}
    else:
        required = ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD")
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise RuntimeError(
                "未設定 DATABASE_URL／NEON_DATABASE_URL，且缺少 "
                + "、".join(missing)
            )
        connection_args = ()
        connection_kwargs = {
            "host": os.environ["DB_HOST"],
            "port": int(os.environ.get("DB_PORT", "5432")),
            "dbname": os.environ["DB_NAME"],
            "user": os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
        }

    income_table = income_statement_table(use_neon=True)
    balance_table = balance_sheet_table(use_neon=True)
    cash_table = cash_flow_table(use_neon=True)
    ratios_table = financial_ratios_table(use_neon=True)
    totals = Counter(success=0, missing=0, not_applicable=0, errors=0, upserted=0)

    with psycopg2.connect(*connection_args, **connection_kwargs) as connection:
        if not args.dry_run:
            ensure_contract(connection, ratios_table)

        for period in periods_between(args.from_period, args.to_period):
            codes = fetch_symbol_codes(
                connection,
                period=period,
                income_table=income_table,
                code_from=args.code_from,
                code_to=args.code_to,
            )
            codes = [
                code for index, code in enumerate(codes)
                if index % args.shard_count == args.shard_index
            ]
            logger.info(
                "期間=%s 分片=%d/%d 股票=%d dry_run=%s",
                period,
                args.shard_index,
                args.shard_count,
                len(codes),
                args.dry_run,
            )
            for code_batch in batched(codes, args.batch_size):
                records, stats = compute_records_from_connection(
                    connection,
                    period=period,
                    codes=code_batch,
                    income_table=income_table,
                    balance_table=balance_table,
                    cash_flow_table=cash_table,
                )
                totals.update(stats)
                if not args.dry_run:
                    try:
                        totals["upserted"] += upsert_records(
                            connection, ratios_table, records
                        )
                    except Exception:
                        connection.rollback()
                        totals["errors"] += len(records)
                        logger.exception(
                            "期間=%s 批次=%s～%s upsert 失敗",
                            period,
                            code_batch[0],
                            code_batch[-1],
                        )
                logger.info(
                    "期間=%s 進度=%s 統計=%s",
                    period,
                    code_batch[-1],
                    dict(totals),
                )

    logger.info(
        "完成：成功=%d 缺資料=%d 不適用=%d 錯誤=%d 寫入=%d",
        totals["success"],
        totals["missing"],
        totals["not_applicable"],
        totals["errors"],
        totals["upserted"],
    )
    return 1 if totals["errors"] else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="從 Neon 三大報表回補 TTM 財務比率")
    parser.add_argument("--from-period", required=True, help="起始季，例如 2024Q1 或 202401")
    parser.add_argument("--to-period", required=True, help="結束季，例如 2024Q4 或 202404")
    parser.add_argument("--code-from", help="股票代號起點（含）")
    parser.add_argument("--code-to", help="股票代號終點（含）")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true", help="計算但不建表或寫入")
    return parser


def main() -> int:
    try:
        return run(build_parser().parse_args())
    except Exception:
        logger.exception("財務比率回補失敗")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
