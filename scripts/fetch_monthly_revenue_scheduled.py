#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""排程用月營收抓取：預設抓最新一期並寫入資料庫。"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

load_dotenv(os.path.join(ROOT, '.env'))

LOG_PATH = os.path.join(ROOT, 'monthly_revenue_scheduled.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def run(*, use_local_db: bool, market: str, year: int | None, month: int | None) -> int:
    from server import DatabaseManager, stock_api

    target_label = '本地 PostgreSQL' if use_local_db else 'Neon（雲端）'
    logger.info('=' * 60)
    logger.info('開始排程月營收抓取')
    logger.info('執行時間：%s', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info('資料庫目標：%s', target_label)
    logger.info('市場：%s', market.upper())
    if year and month:
        logger.info('指定年月：%04d-%02d', year, month)
    else:
        logger.info('年月：自動（最新一期）')
    logger.info('=' * 60)

    records, summary = stock_api.fetch_monthly_revenue(year=year, month=month, market=market)
    summary = summary or {}
    logger.info('抓取完成：%s 筆', len(records))

    per_market = summary.get('per_market') or {}
    if per_market:
        logger.info('TWSE=%s, TPEX=%s', per_market.get('TWSE', 0), per_market.get('TPEX', 0))

    inserted = 0
    db_manager = None
    if records:
        db_manager = DatabaseManager(use_local=use_local_db)
        try:
            inserted = stock_api.upsert_monthly_revenue(records, db_manager=db_manager)
            table_name = getattr(db_manager, 'table_revenue', 'tw_stock_monthly_revenue')
            logger.info('寫入 %s：%s 筆', table_name, inserted)
        finally:
            try:
                db_manager.disconnect()
            except Exception:
                pass
    else:
        logger.warning('本次無資料可寫入（可能尚未公告）')

    period = summary.get('year'), summary.get('month')
    if period[0] and period[1]:
        logger.info('實際期別：%04d-%02d', period[0], period[1])

    logger.info('月營收排程任務完成')
    return 0 if records else 2


def main() -> int:
    parser = argparse.ArgumentParser(description='排程抓取上市/上櫃月營收並寫入資料庫')
    parser.add_argument('--use-local-db', action='store_true', help='寫入本地 PostgreSQL（預設 Neon）')
    parser.add_argument('--market', default='both', choices=['both', 'twse', 'tpex'])
    parser.add_argument('--year', type=int, help='西元年（省略則抓最新一期）')
    parser.add_argument('--month', type=int, help='月份 1-12（省略則抓最新一期）')
    args = parser.parse_args()

    try:
        return run(
            use_local_db=args.use_local_db,
            market=args.market,
            year=args.year,
            month=args.month,
        )
    except Exception:
        logger.exception('月營收排程抓取失敗')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
