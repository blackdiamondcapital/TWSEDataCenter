#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fetch quarterly financial statements and persist them to Neon."""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Callable

_PERIOD_RE = re.compile(r"^(\d{4})[Qq]([1-4])$")

import pandas as pd
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

load_dotenv(os.path.join(ROOT, ".env"))

LOG_PATH = os.environ.get(
    "QUARTERLY_STATEMENTS_LOG",
    os.path.join(ROOT, "quarterly_statements_scheduled.log"),
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StatementSpec:
    key: str
    label: str
    fetcher: Callable[..., pd.DataFrame]
    blocked_error: type[Exception]
    upsert_method: str


def resolve_quarter(today: date | None = None) -> tuple[int, int]:
    """Return the latest quarter whose statutory filing deadline has passed."""
    current = today or date.today()
    if current.month >= 11:
        return current.year, 3
    if current.month >= 8:
        return current.year, 2
    if current.month >= 5:
        return current.year, 1
    return current.year - 1, 4


def _parse_single_period(token: str) -> tuple[int, int]:
    match = _PERIOD_RE.match(token.strip())
    if not match:
        raise ValueError(f"無效的期間格式：{token!r}，請使用例如 2024Q1")
    year = int(match.group(1))
    season = int(match.group(2))
    if not 2000 <= year <= 2100:
        raise ValueError(f"year 必須介於 2000 到 2100：{year}")
    return year, season


def _period_to_ordinal(year: int, season: int) -> int:
    return year * 4 + (season - 1)


def _ordinal_to_period(ordinal: int) -> tuple[int, int]:
    return ordinal // 4, ordinal % 4 + 1


def parse_periods(periods_str: str) -> list[tuple[int, int]]:
    """Parse period specs like 2024Q1, 2024Q1,2024Q3, or 2024Q1-2024Q4."""
    text = periods_str.strip()
    if not text:
        raise ValueError("periods 不可為空")

    result: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()

    for chunk in text.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            start_str, end_str = chunk.split("-", 1)
            start = _parse_single_period(start_str)
            end = _parse_single_period(end_str)
            start_ord = _period_to_ordinal(*start)
            end_ord = _period_to_ordinal(*end)
            if start_ord > end_ord:
                raise ValueError(f"期間區間起點不可晚於終點：{chunk}")
            for ordinal in range(start_ord, end_ord + 1):
                period = _ordinal_to_period(ordinal)
                if period not in seen:
                    seen.add(period)
                    result.append(period)
        else:
            period = _parse_single_period(chunk)
            if period not in seen:
                seen.add(period)
                result.append(period)

    if not result:
        raise ValueError("未解析到任何有效期間")
    return result


def resolve_periods(args: argparse.Namespace) -> list[tuple[int, int]]:
    if args.periods:
        return parse_periods(args.periods)
    if args.year is not None or args.season is not None:
        if args.year is None or args.season is None:
            raise ValueError("year 與 season 必須同時提供或同時省略")
        return [(args.year, args.season)]
    return [resolve_quarter()]


def _statement_specs() -> dict[str, StatementSpec]:
    from balance_sheet_service import (
        MopsBlockedError as BalanceBlockedError,
        fetch_all_balance_sheets,
    )
    from cash_flow_service import (
        MopsBlockedError as CashFlowBlockedError,
        fetch_all_cash_flows,
    )
    from income_statement_service import (
        MopsBlockedError as IncomeBlockedError,
        fetch_all_incomes,
    )

    return {
        "income": StatementSpec(
            "income",
            "損益表",
            fetch_all_incomes,
            IncomeBlockedError,
            "upsert_income_statements",
        ),
        "balance": StatementSpec(
            "balance",
            "資產負債表",
            fetch_all_balance_sheets,
            BalanceBlockedError,
            "upsert_balance_sheets",
        ),
        "cashflow": StatementSpec(
            "cashflow",
            "現金流量表",
            fetch_all_cash_flows,
            CashFlowBlockedError,
            "upsert_cash_flows",
        ),
    }


class StatementRunner:
    def __init__(
        self,
        *,
        year: int,
        season: int,
        code_from: str | None,
        code_to: str | None,
        delay: float,
        pause_every: int,
        pause_seconds: float,
        retry_max: int,
        retry_wait_seconds: float,
        write_to_db: bool,
        flush_every: int,
    ) -> None:
        self.year = year
        self.season = season
        self.code_from = code_from
        self.code_to = code_to
        self.delay = delay
        self.pause_every = pause_every
        self.pause_seconds = pause_seconds
        self.retry_max = retry_max
        self.retry_wait_seconds = retry_wait_seconds
        self.write_to_db = write_to_db
        self.flush_every = max(1, flush_every)
        self.stock_api = None
        self.db_manager = None

    def _flush(self, spec: StatementSpec, pending: list[dict]) -> int:
        if not pending:
            return 0
        if not self.write_to_db:
            pending.clear()
            return 0

        if self.stock_api is None:
            from server import stock_api

            self.stock_api = stock_api

        records = list(pending)
        method = getattr(self.stock_api, spec.upsert_method)
        if self.db_manager is None:
            from server import DatabaseManager

            self.db_manager = DatabaseManager(use_local=False)
        inserted = method(records, db_manager=self.db_manager)
        pending.clear()
        logger.info("[%s] 已寫入 Neon：%d 筆", spec.key, inserted)
        return inserted

    def close(self) -> None:
        if self.db_manager is not None:
            self.db_manager.disconnect()
            self.db_manager = None

    def run_statement(self, spec: StatementSpec) -> dict[str, int | str]:
        pending: list[dict] = []
        stats = {
            "statement": spec.key,
            "success": 0,
            "empty": 0,
            "errors": 0,
            "persisted": 0,
            "blocks": 0,
        }
        resume_from = self.code_from
        last_code: str | None = None

        def row_cb(code: str, frame: pd.DataFrame) -> None:
            records = frame.to_dict(orient="records")
            if not records:
                return
            pending.extend(records)
            if len(pending) >= self.flush_every:
                stats["persisted"] = int(stats["persisted"]) + self._flush(spec, pending)

        def progress_cb(
            index: int,
            total: int,
            code: str,
            status: str,
            detail: str | None,
        ) -> None:
            nonlocal last_code
            if code:
                last_code = code
            if status == "success":
                stats["success"] = int(stats["success"]) + 1
            elif status == "empty":
                stats["empty"] = int(stats["empty"]) + 1
            elif status == "error":
                stats["errors"] = int(stats["errors"]) + 1

            if status in {"success", "empty", "error"}:
                logger.info(
                    "[%s] %d/%d 股票=%s 狀態=%s%s",
                    spec.key,
                    index,
                    total,
                    code or "-",
                    status,
                    f" 原因={detail}" if detail else "",
                )

        attempt = 0
        while True:
            fetch_kwargs = {
                "year": str(self.year),
                "season": str(self.season),
                "delay": self.delay,
                "progress_cb": progress_cb,
                "row_cb": row_cb,
                "code_from": resume_from,
                "code_to": self.code_to,
                "pause_every": self.pause_every or None,
                "pause_seconds": self.pause_seconds,
            }
            if spec.key == "income":
                fetch_kwargs["raise_on_block"] = True

            try:
                logger.info(
                    "[%s] 開始抓取 %d Q%d，代號 %s～%s%s",
                    spec.key,
                    self.year,
                    self.season,
                    resume_from or "最小",
                    self.code_to or "最大",
                    f"，第 {attempt + 1} 次嘗試" if attempt else "",
                )
                spec.fetcher(**fetch_kwargs)
                stats["persisted"] = int(stats["persisted"]) + self._flush(spec, pending)
                break
            except spec.blocked_error as exc:
                stats["persisted"] = int(stats["persisted"]) + self._flush(spec, pending)
                stats["blocks"] = int(stats["blocks"]) + 1
                if attempt >= self.retry_max:
                    raise RuntimeError(
                        f"{spec.label}在股票 {last_code or resume_from or '?'} 遭 MOPS 封鎖，"
                        f"已達重試上限 {self.retry_max}"
                    ) from exc
                attempt += 1
                resume_from = last_code or resume_from
                logger.warning(
                    "[%s] MOPS 封鎖於 %s，%.0f 秒後續抓（%d/%d）",
                    spec.key,
                    resume_from or "?",
                    self.retry_wait_seconds,
                    attempt,
                    self.retry_max,
                )
                time.sleep(self.retry_wait_seconds)

        logger.info(
            "[%s] 完成：成功=%d 空白=%d 錯誤=%d 寫入=%d 封鎖=%d",
            spec.key,
            stats["success"],
            stats["empty"],
            stats["errors"],
            stats["persisted"],
            stats["blocks"],
        )
        return stats


def run(args: argparse.Namespace) -> int:
    periods = resolve_periods(args)

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not args.no_write_db and not db_url:
        raise RuntimeError("未設定 DATABASE_URL 或 NEON_DATABASE_URL")

    specs = _statement_specs()
    selected = list(specs) if args.statement == "all" else [args.statement]
    period_labels = ", ".join(f"{year}Q{season}" for year, season in periods)
    logger.info("=" * 72)
    logger.info("季度三大報表抓取：%s（共 %d 期）", period_labels, len(periods))
    logger.info("報表：%s", ", ".join(specs[key].label for key in selected))
    logger.info("股票代號：%s～%s", args.code_from or "最小", args.code_to or "最大")
    logger.info("資料庫寫入：%s", "停用（驗證模式）" if args.no_write_db else "Neon")
    logger.info("=" * 72)

    failed = 0
    for period_index, (year, season) in enumerate(periods, start=1):
        logger.info("-" * 72)
        logger.info("開始第 %d/%d 期：%d Q%d", period_index, len(periods), year, season)
        logger.info("-" * 72)

        runner = StatementRunner(
            year=year,
            season=season,
            code_from=args.code_from,
            code_to=args.code_to,
            delay=args.delay,
            pause_every=args.pause_every,
            pause_seconds=args.pause_minutes * 60,
            retry_max=args.retry_max,
            retry_wait_seconds=args.retry_wait_minutes * 60,
            write_to_db=not args.no_write_db,
            flush_every=args.flush_every,
        )

        period_failed = 0
        try:
            for key in selected:
                try:
                    runner.run_statement(specs[key])
                except Exception:
                    period_failed += 1
                    logger.exception("[%s] %d Q%d 抓取失敗", key, year, season)
                    if not args.continue_on_error:
                        break
        finally:
            runner.close()

        failed += period_failed
        if period_failed and not args.continue_on_error:
            break

    if failed:
        logger.error("季度報表任務結束：%d 個報表失敗", failed)
        return 1
    logger.info("季度報表任務全部完成")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抓取季度三大財務報表並寫入 Neon")
    parser.add_argument(
        "--periods",
        help="多期格式，例如 2024Q1、2024Q1,2024Q2 或 2024Q1-2024Q4",
    )
    parser.add_argument("--year", type=int, help="西元年度；與 --season 搭配使用")
    parser.add_argument("--season", type=int, choices=(1, 2, 3, 4), help="季度 1-4")
    parser.add_argument(
        "--statement",
        choices=("all", "income", "balance", "cashflow"),
        default="all",
        help="抓取全部或單一報表",
    )
    parser.add_argument("--code-from", help="股票代號起始（含）")
    parser.add_argument("--code-to", help="股票代號結束（含）")
    parser.add_argument("--delay", type=float, default=0.8, help="每檔請求間隔秒數")
    parser.add_argument("--pause-every", type=int, default=60, help="每 N 檔批次休息")
    parser.add_argument("--pause-minutes", type=float, default=2.0, help="每批休息分鐘")
    parser.add_argument("--retry-max", type=int, default=8, help="MOPS 封鎖重試上限")
    parser.add_argument("--retry-wait-minutes", type=float, default=5.0, help="封鎖等待分鐘")
    parser.add_argument("--flush-every", type=int, default=10, help="每 N 筆寫入一次 Neon")
    parser.add_argument("--no-write-db", action="store_true", help="只抓取驗證，不寫資料庫")
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="單一報表失敗後繼續下一張（預設啟用）",
    )
    return parser


def main() -> int:
    try:
        return run(build_parser().parse_args())
    except Exception:
        logger.exception("季度三大報表任務失敗")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
