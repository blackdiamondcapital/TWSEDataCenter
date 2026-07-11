1#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import json
import csv
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta, date
import logging
from bs4 import BeautifulSoup
import urllib3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from flask import Flask, jsonify, request, send_from_directory, send_file, Response, stream_with_context
from flask_cors import CORS
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values, Json
from psycopg2.extensions import register_adapter
import json as json_lib

# 註冊 dict 類型適配器
register_adapter(dict, Json)
from urllib.parse import urlparse
import os
import socket
import math
import threading  # 添加线程模块
import subprocess
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import zipfile
import tempfile
from functools import partial
from typing import Optional
import argparse

import re

from income_statement_service import fetch_all_incomes, fetch_income_row, TARGET_ORDER
from balance_sheet_service import (
    fetch_all_balance_sheets,
    fetch_balance_sheet_row,
    MopsBlockedError as BalanceMopsBlockedError,
    TARGET_ORDER as BALANCE_TARGET_ORDER,
)
from cash_flow_service import (
    fetch_all_cash_flows,
    fetch_cash_flow_row,
    MopsBlockedError as CashFlowMopsBlockedError,
    TARGET_ORDER as CASH_FLOW_TARGET_ORDER,
)

from table_config import (
    resolve_use_neon,
    stock_prices_table,
    stock_returns_table,
    institutional_trades_table,
    margin_trades_table,
    monthly_revenue_table,
    income_statement_table,
    balance_sheet_table,
    cash_flow_table,
    financial_ratios_table,
)

from returns_calc import compute_returns as compute_returns_task
from cloud_jobs_api import cloud_jobs_blueprint

# 配置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env if present (optional dependency)
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

DEFAULT_START_DATE = '2010-01-01'

FMTQIK_URL = 'https://www.twse.com.tw/exchangeReport/FMTQIK'

# 全局数据库表锁（防止并发修改表结构导致死锁）
db_table_lock = threading.Lock()
# 全局更新鎖，避免並行 /api/update 造成重複抓取
update_lock = threading.Lock()
db_sync_lock = threading.Lock()

FINANCIAL_RATIO_COLS = [
    # 方案 B：以 Neon 既有格式為準（snake_case）
    "assets",
    "equity",
    "revenue",
    "gross_profit",
    "op_profit",
    "net_profit",
    "gross_margin",
    "op_margin",
    "net_margin",
    "roa",
    "roe",
    "debt_ratio",
    "current_ratio",
    "quick_ratio",
]


def _safe_div(n, d):
    try:
        if n is None or d is None:
            return None
        dn = float(d)
        if dn == 0:
            return None
        return float(n) / dn
    except Exception:
        return None


def _compute_ratios_record(row: dict) -> dict:
    revenue = row.get('Revenue')
    gross_profit = row.get('GrossProfitFromOperations')
    op_profit = row.get('ProfitLossFromOperatingActivities')
    net_profit = row.get('ProfitLoss')

    assets = row.get('Assets')
    liabilities = row.get('Liabilities')
    equity_parent = row.get('EquityAttributableToOwnersOfParent')
    current_assets = row.get('CurrentAssets')
    current_liabilities = row.get('CurrentLiabilities')

    quick_assets = 0.0
    quick_used = False
    for k in (
        'CashAndCashEquivalents',
        'AccountsReceivableNet',
        'OtherCurrentReceivables',
        'CurrentFinancialAssetsAtAmortisedCost',
        'CurrentFinancialAssetsAtFairValueThroughProfitOrLoss',
        'CurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome',
        'OtherCurrentFinancialAssets',
    ):
        v = row.get(k)
        if v is None:
            continue
        try:
            quick_assets += float(v)
            quick_used = True
        except Exception:
            continue

    return {
        'assets': assets,
        'equity': equity_parent,
        'revenue': revenue,
        'gross_profit': gross_profit,
        'op_profit': op_profit,
        'net_profit': net_profit,
        'gross_margin': _safe_div(gross_profit, revenue),
        'op_margin': _safe_div(op_profit, revenue),
        'net_margin': _safe_div(net_profit, revenue),
        'roa': _safe_div(net_profit, assets),
        'roe': _safe_div(net_profit, equity_parent),
        'debt_ratio': _safe_div(liabilities, assets),
        'current_ratio': _safe_div(current_assets, current_liabilities),
        'quick_ratio': _safe_div(quick_assets, current_liabilities) if quick_used else None,
    }

# 获取当前目录作为静态文件目录
current_dir = os.path.dirname(os.path.abspath(__file__))
frontend_dir = os.path.join(current_dir, 'frontend')

app = Flask(__name__, static_folder=frontend_dir, static_url_path='')
allowed_origins = [
    origin.strip()
    for origin in os.environ.get(
        'ALLOWED_ORIGINS',
        'http://localhost:5003,http://127.0.0.1:5003,http://localhost:5500,http://127.0.0.1:5500',
    ).split(',')
    if origin.strip()
]
CORS(app, resources={r"/api/*": {"origins": allowed_origins}})
app.register_blueprint(cloud_jobs_blueprint)

# SSE 事件佇列（推進度/警告到前端）
sse_queue = Queue()

def push_sse(channel: str, event: str, message: str | None = None, **extra):
    try:
        payload = {
            'channel': channel,
            'event': event,
            'message': message,
        }
        if extra:
            # 避免覆寫保留欄位，讓前端事件分類穩定
            for k, v in extra.items():
                if k in ('channel', 'event', 'message'):
                    continue
                payload[k] = v
        sse_queue.put(payload, timeout=0.1)
    except Exception:
        pass


@app.route('/api/stream/logs', methods=['GET'])
def stream_logs():
    """Server-Sent Events: 推送後端進度/警告到前端。"""
    def event_stream():
        heartbeat_interval = 10
        last_heartbeat = time.time()
        while True:
            try:
                item = sse_queue.get(timeout=1)
                try:
                    payload = json.dumps(item, ensure_ascii=False)
                except TypeError:
                    payload = json.dumps({'channel': 'system', 'event': 'error', 'message': 'encode_failed'})
                yield f"data: {payload}\n\n"
            except Exception:
                pass
            now = time.time()
            if now - last_heartbeat >= heartbeat_interval:
                last_heartbeat = now
                yield "data: {\"channel\":\"system\",\"event\":\"heartbeat\"}\n\n"

    headers = {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
    }
    return Response(event_stream(), headers=headers)

class TableNameAwareCursor(RealDictCursor):
    """Cursor that automatically maps logical table names to environment-specific ones."""

    def __init__(
        self,
        *args,
        table_prices="tw_stock_prices",
        table_returns="tw_stock_returns",
        table_institutional="tw_institutional_trades",
        **kwargs,
    ):
        self._table_prices = table_prices
        self._table_returns = table_returns
        self._table_institutional = table_institutional
        super().__init__(*args, **kwargs)

    def _adapt_query(self, query):
        if not isinstance(query, str):
            return query
        prices = self._table_prices or "tw_stock_prices"
        returns = self._table_returns or "tw_stock_returns"
        institutional = self._table_institutional or "tw_institutional_trades"
        if prices != "tw_stock_prices":
            query = query.replace("tw_stock_prices", prices)
        if returns != "tw_stock_returns":
            query = query.replace("tw_stock_returns", returns)
        if institutional != "tw_institutional_trades":
            query = query.replace("tw_institutional_trades", institutional)
        return query

    def execute(self, query, vars=None):
        return super().execute(self._adapt_query(query), vars)

    def executemany(self, query, vars_list):
        return super().executemany(self._adapt_query(query), vars_list)

    def mogrify(self, query, vars=None):
        return super().mogrify(self._adapt_query(query), vars)


class DatabaseManager:
    def __init__(self, use_local: bool = False):
        self.use_local = use_local
        self.db_url = (
            None if use_local else (
                os.environ.get('DATABASE_URL')
                or os.environ.get('NEON_DATABASE_URL')
            )
        )
        ssl_default = 'require' if self.db_url else 'prefer'
        self.db_config = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': os.environ.get('DB_PORT', '5432'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', ''),
            'database': os.environ.get('DB_NAME', 'postgres'),
            'sslmode': os.environ.get('DB_SSLMODE', ssl_default)
        }
        self.connection = None
        self.is_neon = resolve_use_neon(use_local=self.use_local, db_url=self.db_url)
        self.table_prices = stock_prices_table(use_neon=self.is_neon)
        self.table_returns = stock_returns_table(use_neon=self.is_neon)
        self.table_institutional = institutional_trades_table(use_neon=self.is_neon)
        self.table_margin = margin_trades_table(use_neon=self.is_neon)
        self.table_revenue = monthly_revenue_table(use_neon=self.is_neon)
        self.table_income = income_statement_table(use_neon=self.is_neon)
        self.table_balance = balance_sheet_table(use_neon=self.is_neon)
        self.table_cash_flow = cash_flow_table(use_neon=self.is_neon)
        self.table_financial_ratios = financial_ratios_table(use_neon=self.is_neon)
        self._cursor_factory = partial(
            TableNameAwareCursor,
            table_prices=self.table_prices,
            table_returns=self.table_returns,
            table_institutional=self.table_institutional,
        )

    def ensure_prices_unique(self):
        """確保價格表 (symbol, date) 有 UNIQUE/INDEX。

        ON CONFLICT (symbol, date) 需要對應的 unique/exclusion constraint。
        若既有資料含重複，會先自動去重後再建索引。
        """
        if self.connection is None:
            if not self.connect():
                return False

        cursor = self.connection.cursor()
        try:
            # 如果表不存在（例如 create_tables 因鎖超時略過），先補一個最小可用表結構
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_prices} (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                );
                """
            )

            # 先嘗試建立唯一索引（若已存在則不動作）
            cursor.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {self.table_prices}_symbol_date_idx
                ON {self.table_prices}(symbol, date);
                """
            )
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"ensure_prices_unique error: {e}")
            try:
                self.connection.rollback()
            except Exception:
                pass

            msg = str(e)
            if 'duplicated' not in msg and 'duplicate' not in msg:
                try:
                    cursor.close()
                except Exception:
                    pass
                return False

            # 有重複鍵：先去重，再重試建立索引
            try:
                cursor = self.connection.cursor()
                cursor.execute(
                    f"""
                    DELETE FROM {self.table_prices} t
                    USING (
                      SELECT ctid,
                             ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY ctid) AS rn
                      FROM {self.table_prices}
                    ) d
                    WHERE t.ctid = d.ctid AND d.rn > 1;
                    """
                )
                self.connection.commit()
                cursor.close()
            except Exception as cleanup_exc:
                logger.error(f"ensure_prices_unique cleanup error: {cleanup_exc}")
                try:
                    self.connection.rollback()
                except Exception:
                    pass
                try:
                    cursor.close()
                except Exception:
                    pass
                return False

            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS {self.table_prices}_symbol_date_idx
                    ON {self.table_prices}(symbol, date);
                    """
                )
                self.connection.commit()
                cursor.close()
                return True
            except Exception as e2:
                logger.error(f"ensure_prices_unique retry error: {e2}")
                try:
                    self.connection.rollback()
                except Exception:
                    pass
                try:
                    cursor.close()
                except Exception:
                    pass
                return False

    def connection_info(self):
        if self.db_url:
            parsed = urlparse(self.db_url)
            return {
                'driver': parsed.scheme,
                'host': parsed.hostname,
                'port': parsed.port,
                'database': parsed.path.lstrip('/') if parsed.path else None,
                'user': parsed.username,
                'sslmode': self.db_config.get('sslmode'),
                'is_local': self.use_local
            }
        return {
            'driver': 'psycopg2_params',
            'host': self.db_config.get('host'),
            'port': self.db_config.get('port'),
            'database': self.db_config.get('database'),
            'user': self.db_config.get('user'),
            'sslmode': self.db_config.get('sslmode'),
            'is_local': self.use_local
        }

    @staticmethod
    def _resolve_use_local(value) -> bool:
        if isinstance(value, str):
            return value.lower() == 'true'
        if isinstance(value, (bool, int)):
            return bool(value)
        return False

    @staticmethod
    def from_request_payload(payload: dict | None) -> "DatabaseManager":
        use_local = DatabaseManager._resolve_use_local(payload.get('use_local_db')) if payload else False
        return DatabaseManager(use_local=use_local)

    @staticmethod
    def from_request_args(args) -> "DatabaseManager":
        val = None
        if args is not None:
            if hasattr(args, 'get'):
                val = args.get('use_local_db')
            elif isinstance(args, dict):
                val = args.get('use_local_db')
        use_local = DatabaseManager._resolve_use_local(val)
        return DatabaseManager(use_local=use_local)

    def connect(self):
        """連接到PostgreSQL資料庫"""
        try:
            if self.db_url:
                # 解析 URL 改用字典參數連線，避免 URI 解析問題
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(self.db_url)
                
                conn_params = {
                    'host': parsed.hostname,
                    'port': parsed.port or 5432,
                    'user': parsed.username,
                    'password': parsed.password,
                    'database': parsed.path.lstrip('/') if parsed.path else 'postgres',
                    'cursor_factory': self._cursor_factory,
                    'sslmode': 'require'  # 強制使用 require，不使用 channel_binding
                }
                
                self.connection = psycopg2.connect(**conn_params)
            else:
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['database'],
                    cursor_factory=self._cursor_factory,
                    sslmode=self.db_config.get('sslmode', 'prefer')
                )
            logger.info("資料庫連接成功")
            return True
        except Exception as e:
            logger.error(f"資料庫連接失敗: {e}")
            return False

    def disconnect(self):
        """斷開資料庫連接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("資料庫連接已關閉")
    
    def test_connection(self):
        """測試資料庫連接"""
        try:
            if self.connect():
                cursor = self.connection.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                cursor.close()
                self.disconnect()
                return True, f"PostgreSQL版本: {version['version']}"
            else:
                return False, "無法連接到資料庫"
        except Exception as e:
            return False, f"連接測試失敗: {e}"
    
    def create_tables(self):
        """创建股票数据表（带锁保护）"""
        # 获取表锁，防止并发修改表结构
        acquired = db_table_lock.acquire(timeout=30)
        if not acquired:
            logger.warning("获取表锁超时，跳过表检查")
            # 即使略過建表，也要確保 prices 的 unique index 存在，否則 ON CONFLICT 會直接報錯
            try:
                if self.connection is None:
                    if not self.connect():
                        return False
                self.ensure_prices_unique()
            except Exception:
                pass
            return True
        
        try:
            if self.connection is None:
                if not self.connect():
                    return False
            
            cursor = self.connection.cursor()
            
            # 創建股票代碼表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tw_stock_symbols (
                    symbol VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    market VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 創建股價數據表
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_prices} (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                );
                """
            )

            # 確保 (symbol, date) unique index 存在（並自動處理重複）
            self.ensure_prices_unique()

            # 創建報酬率數據表
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_returns} (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    daily_return DECIMAL(10,6),
                    weekly_return DECIMAL(10,6),
                    monthly_return DECIMAL(10,6),
                    cumulative_return DECIMAL(10,6),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                );
                """
            )
            cursor.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {self.table_returns}_symbol_date_idx
                ON {self.table_returns}(symbol, date);
                """
            )

            # 為現有表添加新欄位（如果不存在）
            try:
                cursor.execute(
                    f"""
                    ALTER TABLE {self.table_returns} 
                    ADD COLUMN IF NOT EXISTS weekly_return DECIMAL(10,6),
                    ADD COLUMN IF NOT EXISTS monthly_return DECIMAL(10,6);
                    """
                )
            except Exception as e:
                logger.warning(f"添加新欄位時出現警告: {e}")
                # 嘗試單獨添加每個欄位
                try:
                    cursor.execute(
                        f"ALTER TABLE {self.table_returns} ADD COLUMN IF NOT EXISTS weekly_return DECIMAL(10,6);"
                    )
                    cursor.execute(
                        f"ALTER TABLE {self.table_returns} ADD COLUMN IF NOT EXISTS monthly_return DECIMAL(10,6);"
                    )
                except Exception as e2:
                    logger.warning(f"單獨添加欄位也失敗: {e2}")
            
            # 建立異常備份表（若不存在）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices_backup_anomaly (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume BIGINT,
                    backup_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reason TEXT,
                    rule_version VARCHAR(50),
                    threshold NUMERIC(10,6)
                );
            """)

            # 建立異常稽核表（若不存在）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_anomaly_audit (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    start_date DATE,
                    end_date DATE,
                    deleted_count INTEGER DEFAULT 0,
                    refetched_count INTEGER DEFAULT 0,
                    rule_version VARCHAR(50),
                    threshold NUMERIC(10,6),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # 建立 BWIBBU 指標資料表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tw_stock_bwibbu (
                    code VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    name VARCHAR(100),
                    pe_ratio NUMERIC(14,4),
                    dividend_yield NUMERIC(14,4),
                    pb_ratio NUMERIC(14,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (code, date)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tw_stock_bwibbu_date_idx
                ON tw_stock_bwibbu(date)
            """)

            # 建立三大法人 (T86) 資料表
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_institutional} (
                    date DATE NOT NULL,
                    market VARCHAR(10) NOT NULL,
                    stock_no VARCHAR(20) NOT NULL,
                    stock_name VARCHAR(100),
                    foreign_buy BIGINT,
                    foreign_sell BIGINT,
                    foreign_net BIGINT,
                    foreign_dealer_buy BIGINT,
                    foreign_dealer_sell BIGINT,
                    foreign_dealer_net BIGINT,
                    foreign_total_buy BIGINT,
                    foreign_total_sell BIGINT,
                    foreign_total_net BIGINT,
                    investment_trust_buy BIGINT,
                    investment_trust_sell BIGINT,
                    investment_trust_net BIGINT,
                    dealer_self_buy BIGINT,
                    dealer_self_sell BIGINT,
                    dealer_self_net BIGINT,
                    dealer_hedge_buy BIGINT,
                    dealer_hedge_sell BIGINT,
                    dealer_hedge_net BIGINT,
                    dealer_total_buy BIGINT,
                    dealer_total_sell BIGINT,
                    dealer_total_net BIGINT,
                    overall_net BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, market, stock_no)
                );
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.table_institutional}_date_idx
                ON {self.table_institutional}(date);
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.table_institutional}_stock_idx
                ON {self.table_institutional}(stock_no, date DESC);
                """
            )

            # 建立融資融券資料表
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_margin} (
                    date DATE NOT NULL,
                    market VARCHAR(10) NOT NULL,
                    stock_no VARCHAR(20) NOT NULL,
                    stock_name VARCHAR(100),
                    margin_prev_balance BIGINT,
                    margin_buy BIGINT,
                    margin_sell BIGINT,
                    margin_repay BIGINT,
                    margin_balance BIGINT,
                    margin_limit BIGINT,
                    short_prev_balance BIGINT,
                    short_sell BIGINT,
                    short_buy BIGINT,
                    short_repay BIGINT,
                    short_balance BIGINT,
                    short_limit BIGINT,
                    offset_quantity BIGINT,
                    note VARCHAR(200),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, market, stock_no)
                );
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.table_margin}_date_idx
                ON {self.table_margin}(date);
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.table_margin}_stock_idx
                ON {self.table_margin}(stock_no, date DESC);
                """
            )

            # 建立月營收資料表
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_revenue} (
                    revenue_month DATE NOT NULL,
                    market VARCHAR(10) NOT NULL,
                    stock_no VARCHAR(20) NOT NULL,
                    stock_name VARCHAR(100),
                    industry VARCHAR(100),
                    report_date DATE,
                    month_revenue BIGINT,
                    last_month_revenue BIGINT,
                    last_year_month_revenue BIGINT,
                    mom_change_pct NUMERIC(20,6),
                    yoy_change_pct NUMERIC(20,6),
                    acc_revenue BIGINT,
                    last_year_acc_revenue BIGINT,
                    acc_change_pct NUMERIC(20,6),
                    note VARCHAR(200),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (revenue_month, market, stock_no)
                );
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.table_revenue}_month_idx
                ON {self.table_revenue}(revenue_month);
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self.table_revenue}_stock_idx
                ON {self.table_revenue}(stock_no, revenue_month DESC);
                """
            )

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tw_warrant_trade (
                    out_date DATE,
                    trade_date DATE NOT NULL,
                    warrant_code VARCHAR(20) NOT NULL,
                    warrant_name VARCHAR(100),
                    turnover NUMERIC(20,2),
                    volume BIGINT,
                    raw_out_date_text VARCHAR(20),
                    raw_trade_date_text VARCHAR(20),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (warrant_code, trade_date)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tw_warrant_trade_trade_date_idx
                ON tw_warrant_trade(trade_date DESC);
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tpex_warrant_master (
                    warrant_code VARCHAR(20) PRIMARY KEY,
                    report_date DATE,
                    warrant_name VARCHAR(100),
                    listed_date DATE,
                    expiry_date DATE,
                    underlying_code VARCHAR(20),
                    underlying_name VARCHAR(100),
                    warrant_type VARCHAR(20),
                    exercise_style VARCHAR(20),
                    cap_price NUMERIC(20,6),
                    floor_price NUMERIC(20,6),
                    reset_flag VARCHAR(5),
                    latest_exercise_price NUMERIC(20,6),
                    latest_exercise_ratio NUMERIC(20,10),
                    initial_issuance BIGINT,
                    accumulated_issuance BIGINT,
                    accumulated_canceled BIGINT,
                    market VARCHAR(10) DEFAULT 'TPEX',
                    raw_report_date_text VARCHAR(20),
                    raw_listed_date_text VARCHAR(20),
                    raw_expiry_date_text VARCHAR(20),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tpex_warrant_master_underlying_idx
                ON tpex_warrant_master(underlying_code);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tpex_warrant_master_expiry_idx
                ON tpex_warrant_master(expiry_date);
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tpex_warrant_daily_quotes (
                    trade_date DATE NOT NULL,
                    warrant_code VARCHAR(20) NOT NULL,
                    warrant_name VARCHAR(100),
                    open_price NUMERIC(20,6),
                    high_price NUMERIC(20,6),
                    low_price NUMERIC(20,6),
                    close_price NUMERIC(20,6),
                    price_change NUMERIC(20,6),
                    trade_volume BIGINT,
                    transaction_count BIGINT,
                    trade_value NUMERIC(20,2),
                    underlying_code VARCHAR(20),
                    underlying_name VARCHAR(100),
                    underlying_close_price NUMERIC(20,6),
                    underlying_price_change NUMERIC(20,6),
                    market VARCHAR(10) DEFAULT 'TPEX',
                    raw_trade_date_text VARCHAR(20),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (trade_date, warrant_code)
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tpex_warrant_daily_quotes_code_idx
                ON tpex_warrant_daily_quotes(warrant_code, trade_date DESC);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS tpex_warrant_daily_quotes_trade_date_idx
                ON tpex_warrant_daily_quotes(trade_date DESC);
            """)

            # 建立損益表資料表（寬表，每檔股票每期一列）
            income_columns_sql = ",\n".join([
                f'    "{col}" NUMERIC(20,4)' for col in TARGET_ORDER
            ])
            create_income_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_income} (
                    "股票代號" VARCHAR(20) NOT NULL,
                    period VARCHAR(16) NOT NULL,
{income_columns_sql},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY ("股票代號", period)
                );
            """
            cursor.execute(create_income_sql)

            # 建立資產負債表資料表（寬表，每檔股票每期一列）
            balance_columns_sql = ",\n".join([
                f'    "{col}" NUMERIC(20,4)' for col in BALANCE_TARGET_ORDER
            ])
            create_balance_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_balance} (
                    "股票代號" VARCHAR(20) NOT NULL,
                    period VARCHAR(16) NOT NULL,
{balance_columns_sql},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY ("股票代號", period)
                );
            """
            cursor.execute(create_balance_sql)

            # 建立現金流量表（寬表，每檔股票每期一列）
            cash_flow_columns_sql = ",\n".join([
                f'    "{col}" NUMERIC(20,4)' for col in CASH_FLOW_TARGET_ORDER
            ])
            create_cash_flow_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_cash_flow} (
                    "股票代號" VARCHAR(20) NOT NULL,
                    period VARCHAR(16) NOT NULL,
{cash_flow_columns_sql},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY ("股票代號", period)
                );
            """
            cursor.execute(create_cash_flow_sql)

            ratios_columns_sql = ",\n".join([
                f'    {col} NUMERIC(20,10)' for col in FINANCIAL_RATIO_COLS
            ])
            create_ratios_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_financial_ratios} (
                    symbol VARCHAR(20) NOT NULL,
                    period VARCHAR(16) NOT NULL,
{ratios_columns_sql},
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, period)
                );
            """
            cursor.execute(create_ratios_sql)

            for col in FINANCIAL_RATIO_COLS:
                try:
                    cursor.execute(
                        f'ALTER TABLE {self.table_financial_ratios} ADD COLUMN IF NOT EXISTS {col} NUMERIC(20,10);'
                    )
                except Exception as e:
                    logger.warning("financial ratios table add column %s warning: %s", col, e)

            # 確保既有 tw_balance_sheets 表若是舊版，也會補齊所有目標欄位
            for col in BALANCE_TARGET_ORDER:
                try:
                    cursor.execute(
                        f'ALTER TABLE {self.table_balance} ADD COLUMN IF NOT EXISTS "{col}" NUMERIC(20,4);'
                    )
                except Exception as e:
                    logger.warning("balance table add column %s warning: %s", col, e)

            for col in CASH_FLOW_TARGET_ORDER:
                try:
                    cursor.execute(
                        f'ALTER TABLE {self.table_cash_flow} ADD COLUMN IF NOT EXISTS "{col}" NUMERIC(20,4);'
                    )
                except Exception as e:
                    logger.warning("cash flow table add column %s warning: %s", col, e)
            
            self.connection.commit()
            cursor.close()
            logger.info("资料库表创建成功")
            return True
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            try:
                if self.connection:
                    self.connection.rollback()
            except Exception:
                pass
            return False
        finally:
            # 确保释放锁
            db_table_lock.release()
            logger.debug("表锁已释放")

class StockAPI:
    TWSE_T86_URL = "https://www.twse.com.tw/fund/T86"
    TPEX_T86_URL = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    TWSE_MARGIN_URL = "https://www.twse.com.tw/exchangeReport/MI_MARGN"
    TPEX_MARGIN_URL = "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php"

    def __init__(self):
        self.symbols_cache = None
        self.cache_time = None
        self.db_manager = DatabaseManager()
        self.max_workers = 10  # 並行抓取的最大線程數
        self.bwibbu_cache = None
        self.bwibbu_cache_time = None
        self.bwibbu_cache_ttl = 900  # 預設 15 分鐘快取
        self.twse_session = requests.Session()
        self.twse_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-TW,zh;q=0.9',
            'Referer': 'https://www.twse.com.tw/zh/trading/historical/bwibbu-day.html',
            'Origin': 'https://www.twse.com.tw',
            'X-Requested-With': 'XMLHttpRequest'
        })
        try:
            # 預熱 historical 頁面以取得必要 cookie
            self.twse_session.get('https://www.twse.com.tw/zh/trading/historical/bwibbu-day.html', timeout=10)
        except Exception as exc:
            logger.warning(f"初始化 TWSE session 失敗: {exc}")
        # TPEX session for 上櫃 BWIBBU 指標
        self.tpex_session = requests.Session()
        self.tpex_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json,text/html',
            'Referer': 'https://www.tpex.org.tw'
        })
        try:
            # 預熱 TPEX 相關頁面
            self.tpex_session.get('https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge.php', timeout=10)
        except Exception as exc:
            logger.warning(f"初始化 TPEX session 失敗: {exc}")

    @staticmethod
    def _ensure_date(target_date):
        if isinstance(target_date, str):
            return datetime.strptime(target_date, '%Y-%m-%d').date()
        if isinstance(target_date, datetime):
            return target_date.date()
        if isinstance(target_date, date):
            return target_date
        raise ValueError(f"不支援的日期格式: {target_date}")

    @staticmethod
    def _t86_parse_int(value):
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            try:
                return int(value)
            except Exception:
                return 0
        value = str(value).replace(',', '').strip()
        if value in {'', '-', '--', '---', '----', 'NaN', 'null', 'None'}:
            return 0
        try:
            return int(float(value))
        except Exception:
            return 0
    
    @staticmethod
    def _parse_roc_yyyymm(roc_yyyymm: str) -> date:
        """將民國年月（例如 11410）轉為西元日期（當月第一天）。"""
        if not roc_yyyymm:
            raise ValueError("資料年月為空")
        s = str(roc_yyyymm).strip()
        if not s.isdigit() or len(s) < 4:
            raise ValueError(f"無法解析資料年月: {roc_yyyymm}")
        year_roc = int(s[:-2])
        month = int(s[-2:])
        year = year_roc + 1911
        return date(year, month, 1)

    @staticmethod
    def _parse_roc_yyyymmdd(roc_yyyymmdd: str) -> date | None:
        """將民國年月日（例如 1141117）轉為西元日期。"""
        if not roc_yyyymmdd:
            return None
        s = str(roc_yyyymmdd).strip()
        if not s.isdigit() or len(s) != 7:
            return None
        try:
            roc_year = int(s[:3])
            month = int(s[3:5])
            day = int(s[5:7])
            year = roc_year + 1911
            return date(year, month, day)
        except Exception:
            return None

    @staticmethod
    def _parse_decimal(value):
        """將字串解析為浮點數，小數欄位 '-' 視為 None。"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                v = float(value)
                if not math.isfinite(v):
                    return None
                return v
            except Exception:
                return None
        s = str(value).replace(',', '').strip()
        s_lower = s.lower()
        if s_lower in {'', '-', '--', '---', '----', 'nan', 'null', 'none', 'inf', '+inf', '-inf', 'infinity', '+infinity', '-infinity'}:
            return None
        try:
            v = float(s)
            if not math.isfinite(v):
                return None
            return v
        except Exception:
            return None
        
    def fetch_twse_symbols(self):
        """抓取台灣上市公司股票代碼"""
        try:
            url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
            # 加入SSL憑證驗證處理和User-Agent
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, timeout=10, verify=False, headers=headers)
            response.encoding = 'big5'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table = soup.find('table', {'class': 'h4'})
            if not table:
                tables = soup.find_all('table')
                if tables:
                    table = tables[0]
                else:
                    return []
            
            rows = table.find_all('tr')[1:]
            symbols = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 1 and cols[0].text.strip():
                    code_name = cols[0].text.strip().split()
                    if len(code_name) >= 2 and code_name[0].isdigit():
                        symbols.append({
                            'symbol': code_name[0] + '.TW', 
                            'name': code_name[1],
                            'market': '上市'
                        })
            
            logger.info(f"取得 {len(symbols)} 檔上市股票")
            return symbols
        except Exception as e:
            logger.error(f"抓取上市股票失敗: {e}")
            # 返回備用的熱門上市股票清單
            return self.get_backup_twse_symbols()

    def fetch_otc_symbols(self):
        """抓取台灣櫃檯買賣中心股票代碼"""
        try:
            url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, timeout=10, verify=False, headers=headers)
            response.encoding = 'big5'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            table = soup.find('table', {'class': 'h4'})
            if not table:
                tables = soup.find_all('table')
                if tables:
                    table = tables[0]
                else:
                    return []
            
            rows = table.find_all('tr')[1:]
            symbols = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 1 and cols[0].text.strip():
                    code_name = cols[0].text.strip().split()
                    if len(code_name) >= 2 and code_name[0].isdigit():
                        symbols.append({
                            'symbol': code_name[0] + '.TWO', 
                            'name': code_name[1],
                            'market': '上櫃'
                        })
            
            logger.info(f"取得 {len(symbols)} 檔櫃檯股票")
            return symbols
        except Exception as e:
            logger.error(f"抓取櫃檯股票失敗: {e}")
            # 返回備用的熱門櫃檯股票清單
            return self.get_backup_otc_symbols()
    
    def get_backup_twse_symbols(self):
        """備用的熱門上市股票清單"""
        backup_symbols = [
            {'symbol': '2330.TW', 'name': '台積電', 'market': '上市'},
            {'symbol': '2317.TW', 'name': '鴻海', 'market': '上市'},
            {'symbol': '2454.TW', 'name': '聯發科', 'market': '上市'},
            {'symbol': '2881.TW', 'name': '富邦金', 'market': '上市'},
            {'symbol': '2882.TW', 'name': '國泰金', 'market': '上市'},
            {'symbol': '2886.TW', 'name': '兆豐金', 'market': '上市'},
            {'symbol': '2891.TW', 'name': '中信金', 'market': '上市'},
            {'symbol': '2892.TW', 'name': '第一金', 'market': '上市'},
            {'symbol': '2303.TW', 'name': '聯電', 'market': '上市'},
            {'symbol': '2308.TW', 'name': '台達電', 'market': '上市'},
            {'symbol': '2382.TW', 'name': '廣達', 'market': '上市'},
            {'symbol': '2412.TW', 'name': '中華電', 'market': '上市'},
            {'symbol': '2474.TW', 'name': '可成', 'market': '上市'},
            {'symbol': '3008.TW', 'name': '大立光', 'market': '上市'},
            {'symbol': '3711.TW', 'name': '日月光投控', 'market': '上市'},
            {'symbol': '5880.TW', 'name': '合庫金', 'market': '上市'},
            {'symbol': '6505.TW', 'name': '台塑化', 'market': '上市'},
            {'symbol': '1301.TW', 'name': '台塑', 'market': '上市'},
            {'symbol': '1303.TW', 'name': '南亞', 'market': '上市'},
            {'symbol': '1326.TW', 'name': '台化', 'market': '上市'},
            {'symbol': '2002.TW', 'name': '中鋼', 'market': '上市'},
            {'symbol': '2207.TW', 'name': '和泰車', 'market': '上市'},
            {'symbol': '2357.TW', 'name': '華碩', 'market': '上市'},
            {'symbol': '2395.TW', 'name': '研華', 'market': '上市'},
            {'symbol': '2408.TW', 'name': '南亞科', 'market': '上市'},
            {'symbol': '2409.TW', 'name': '友達', 'market': '上市'},
            {'symbol': '2603.TW', 'name': '長榮', 'market': '上市'},
            {'symbol': '2609.TW', 'name': '陽明', 'market': '上市'},
            {'symbol': '2615.TW', 'name': '萬海', 'market': '上市'},
            {'symbol': '3034.TW', 'name': '聯詠', 'market': '上市'},
            {'symbol': '3045.TW', 'name': '台灣大', 'market': '上市'},
            {'symbol': '4904.TW', 'name': '遠傳', 'market': '上市'},
            {'symbol': '6415.TW', 'name': '矽力-KY', 'market': '上市'},
            {'symbol': '2327.TW', 'name': '國巨', 'market': '上市'},
            {'symbol': '2379.TW', 'name': '瑞昱', 'market': '上市'},
            {'symbol': '2884.TW', 'name': '玉山金', 'market': '上市'},
            {'symbol': '2885.TW', 'name': '元大金', 'market': '上市'},
            {'symbol': '3231.TW', 'name': '緯創', 'market': '上市'},
            {'symbol': '3481.TW', 'name': '群創', 'market': '上市'},
            {'symbol': '6669.TW', 'name': '緯穎', 'market': '上市'},
            {'symbol': '1216.TW', 'name': '統一', 'market': '上市'},
            {'symbol': '1101.TW', 'name': '台泥', 'market': '上市'},
            {'symbol': '1102.TW', 'name': '亞泥', 'market': '上市'},
            {'symbol': '2105.TW', 'name': '正新', 'market': '上市'},
            {'symbol': '2201.TW', 'name': '裕隆', 'market': '上市'},
            {'symbol': '2301.TW', 'name': '光寶科', 'market': '上市'},
            {'symbol': '2324.TW', 'name': '仁寶', 'market': '上市'},
            {'symbol': '2356.TW', 'name': '英業達', 'market': '上市'},
            {'symbol': '2801.TW', 'name': '彰銀', 'market': '上市'},
            {'symbol': '2880.TW', 'name': '華南金', 'market': '上市'}
        ]
        logger.info(f"使用備用上市股票清單: {len(backup_symbols)} 檔")
        return backup_symbols
    
    def get_backup_otc_symbols(self):
        """備用的熱門櫃檯股票清單"""
        backup_symbols = [
            {'symbol': '1565.TWO', 'name': '精華', 'market': '上櫃'},
            {'symbol': '3529.TWO', 'name': '力旺', 'market': '上櫃'},
            {'symbol': '4966.TWO', 'name': '譜瑞-KY', 'market': '上櫃'},
            {'symbol': '6446.TWO', 'name': '藥華藥', 'market': '上櫃'},
            {'symbol': '6488.TWO', 'name': '環球晶', 'market': '上櫃'},
            {'symbol': '8299.TWO', 'name': '群聯', 'market': '上櫃'}
        ]
        logger.info(f"使用備用櫃檯股票清單: {len(backup_symbols)} 檔")
        return backup_symbols

    def get_market_indices(self):
        """獲取台灣主要市場指數和代表性股票"""
        indices = [
            {
                'symbol': '^TWII',
                'name': '台灣加權指數',
                'market': '指數'
            },
            {
                'symbol': '^OTC',
                'name': '櫃買指數',
                'market': '指數'
            },
            {
                'symbol': '0050.TW',
                'name': '元大台灣50',
                'market': 'ETF'
            },
            {
                'symbol': '0056.TW',
                'name': '元大高股息',
                'market': 'ETF'
            },
            {
                'symbol': '0051.TW',
                'name': '元大中型100',
                'market': 'ETF'
            },
            {
                'symbol': '006208.TW',
                'name': '富邦台50',
                'market': 'ETF'
            },
            {
                'symbol': '2330.TW',
                'name': '台積電',
                'market': '權值股'
            },
            {
                'symbol': '2317.TW',
                'name': '鴻海',
                'market': '權值股'
            }
        ]
        
        logger.info(f"添加 {len(indices)} 個市場指數/ETF")
        return indices

    def get_all_symbols(self, force_refresh=False):
        """獲取所有台灣股票代碼"""
        # 檢查快取
        if not force_refresh and self.symbols_cache and self.cache_time:
            if time.time() - self.cache_time < 3600:  # 1小時快取
                return self.symbols_cache
        
        # 抓取新數據
        twse_symbols = self.fetch_twse_symbols()
        otc_symbols = self.fetch_otc_symbols()
        market_indices = self.get_market_indices()
        all_symbols = twse_symbols + otc_symbols + market_indices
        
        # 過濾掉權證等衍生商品
        filtered_symbols = []
        for symbol in all_symbols:
            if not any(keyword in symbol['name'] for keyword in ['購', '牛熊證', '權證']):
                filtered_symbols.append(symbol)
        
        # 更新快取
        self.symbols_cache = filtered_symbols
        self.cache_time = time.time()
        
        logger.info(f"總共取得 {len(filtered_symbols)} 檔股票")
        return filtered_symbols

    def fetch_stock_data(self, symbol, start_date=None, end_date=None):
        """從台灣證交所或櫃買中心獲取股票數據"""
        try:
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = DEFAULT_START_DATE
            
            logger.info(f"下載 {symbol} 股價數據，時間範圍: {start_date} 到 {end_date}")
            
            # 台灣加權指數 (^TWII) —— 直接使用 yfinance（不再備援 FMTQIK）
            if symbol == '^TWII':
                logger.info(f"檢測到台灣加權指數 {symbol}，強制使用 yfinance 抓取歷史數據（停用 FMTQIK）")
                yf_result = self.fetch_twii_with_yfinance(start_date, end_date)
                if yf_result:
                    return yf_result
                logger.error("yfinance 抓取 ^TWII 失敗，返回 None")
                return None
            
            # 櫃買指數（OTC）
            if symbol == '^OTC':
                logger.info(f"檢測到櫃買指數 {symbol}，強制使用 yfinance 抓取歷史數據")
                yf_result = self.fetch_otc_with_yfinance(start_date, end_date)
                if yf_result:
                    return yf_result
                logger.warning("yfinance 抓取 ^OTC 失敗，返回 None")
                return None
            
            # 解析股票代碼
            if '.TW' in symbol or '.TWO' in symbol:
                stock_code = symbol.split('.')[0]
                market_suffix = symbol.split('.')[1]
            else:
                stock_code = symbol
                market_suffix = None

            # 僅在單檔抓取路徑限制：只處理 4 碼純數字股票代號（避免 020001.TWO 這類 5/6 碼）
            if market_suffix in ('TW', 'TWO') and str(stock_code).isdigit() and len(str(stock_code)) != 4:
                logger.info(f"跳過非四碼股票代號: {symbol}")
                return None
            
            # 判斷是上市還是上櫃股票：優先使用代碼後綴判斷
            if market_suffix == 'TWO':
                # 明確為上櫃
                logger.info(f"檢測到上櫃股票 {symbol}，使用櫃買中心API")
                result = self.fetch_tpex_stock_data(stock_code, start_date, end_date)
            elif market_suffix == 'TW':
                # 明確為上市
                logger.info(f"檢測到上市股票 {symbol}，使用證交所API")
                result = self.fetch_twse_stock_data(stock_code, start_date, end_date)
            else:
                # 無後綴時，才用啟發式判斷
                if self.is_otc_stock(stock_code):
                    logger.info(f"判定為上櫃股票 {symbol}，使用櫃買中心API")
                    result = self.fetch_tpex_stock_data(stock_code, start_date, end_date)
                else:
                    logger.info(f"判定為上市股票 {symbol}，使用證交所API")
                    result = self.fetch_twse_stock_data(stock_code, start_date, end_date)
            
            if result:
                logger.info(f"成功獲取 {symbol} 數據，共 {len(result)} 筆")
                # 將list格式轉換為DataFrame格式
                if isinstance(result, list) and result:
                    import pandas as pd
                    df = pd.DataFrame(result)
                    return df
                return result

            enable_yf = str(os.getenv('ENABLE_YFINANCE_FALLBACK', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
            if enable_yf:
                yf_records = self.fetch_stock_with_yfinance(symbol, start_date, end_date)
                if yf_records:
                    logger.info(f"使用 yfinance 備援取得 {symbol} {len(yf_records)} 筆")
                    df = pd.DataFrame(yf_records)
                    return df

            logger.error("台灣API獲取失敗")
            return None
            
        except Exception as e:
            logger.error(f"下載 {symbol} 股價失敗: {e}")
            return None

    def fetch_stock_with_yfinance(self, symbol, start_date, end_date):
        try:
            start_ts = pd.to_datetime(start_date)
            end_ts = pd.to_datetime(end_date)

            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_ts, end=end_ts + pd.Timedelta(days=1), interval="1d", auto_adjust=False)
            if df is None or df.empty:
                return None

            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC')
            df.index = df.index.tz_convert('Asia/Taipei').tz_localize(None)

            records = []
            for idx, row in df.iterrows():
                dt = idx
                if isinstance(dt, pd.Timestamp):
                    dt = dt.to_pydatetime()
                records.append({
                    'ticker': symbol,
                    'Date': dt.strftime('%Y-%m-%d'),
                    'Open': float(row.get('Open')) if pd.notna(row.get('Open')) else None,
                    'High': float(row.get('High')) if pd.notna(row.get('High')) else None,
                    'Low': float(row.get('Low')) if pd.notna(row.get('Low')) else None,
                    'Close': float(row.get('Close')) if pd.notna(row.get('Close')) else None,
                    'Volume': int(row.get('Volume')) if pd.notna(row.get('Volume')) else None,
                })

            records.sort(key=lambda x: x['Date'])
            return records or None
        except Exception as e:
            logger.warning(f"yfinance 備援抓取 {symbol} 失敗: {e}")
            return None
    

    def fetch_twse_all_stocks_day(self, target_date):
        """從證交所 API 一次獲取所有股票的單日數據（批量抓取）
        Args:
            target_date: datetime 對象或 'YYYY-MM-DD' 字串
        Returns:
            dict: {stock_code: {date, open, high, low, close, volume}, ...}
        """
        try:
            if isinstance(target_date, str):
                target_dt = datetime.strptime(target_date, '%Y-%m-%d')
            else:
                target_dt = target_date
            
            # 使用證交所的每日收盤行情 API
            url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
            params = {
                'response': 'json',
                'date': target_dt.strftime('%Y%m%d'),
                'type': 'ALLBUT0999'  # 全部（除了指數）
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=15, verify=False)
            
            if response.status_code != 200:
                logger.warning(f"批量抓取 {target_dt.strftime('%Y-%m-%d')} 失敗: HTTP {response.status_code}")
                return {}
            
            data = response.json()
            
            if data.get('stat') != 'OK':
                logger.warning(f"批量抓取 {target_dt.strftime('%Y-%m-%d')} 回傳非 OK: {data.get('stat')}")
                return {}
            
            result = {}
            
            # 解析 data9 (個股日成交資訊) - 新版格式
            if 'data9' in data and data['data9']:
                for row in data['data9']:
                    try:
                        if len(row) < 9:
                            continue
                        
                        stock_code = row[0].strip()
                        if not stock_code or not stock_code.isdigit():
                            continue
                        
                        # 解析價格和成交量
                        close_str = row[8].replace(',', '').strip()
                        open_str = row[5].replace(',', '').strip()
                        high_str = row[6].replace(',', '').strip()
                        low_str = row[7].replace(',', '').strip()
                        volume_str = row[2].replace(',', '').strip()
                        
                        if close_str in ['--', '---', ''] or volume_str in ['--', '0', '']:
                            continue
                        
                        close_price = float(close_str)
                        volume = int(volume_str)
                        
                        if close_price <= 0 or volume <= 0 or close_price >= 30000:
                            continue
                        
                        result[stock_code] = {
                            'ticker': f"{stock_code}.TW",
                            'Date': target_dt.strftime('%Y-%m-%d'),
                            'Open': float(open_str) if open_str not in ['--', '---', ''] else None,
                            'High': float(high_str) if high_str not in ['--', '---', ''] else None,
                            'Low': float(low_str) if low_str not in ['--', '---', ''] else None,
                            'Close': round(close_price, 2),
                            'Volume': volume
                        }
                    except (ValueError, IndexError) as e:
                        continue
            
            # 如果 data9 不存在，嘗試解析 tables 格式 (舊版格式，如 2010 年)
            elif 'tables' in data and data['tables']:
                for table in data['tables']:
                    # 尋找每日收盤行情的表格
                    if 'title' in table and '每日收盤行情' in table['title'] and 'data' in table:
                        for row in table['data']:
                            try:
                                if len(row) < 9:
                                    continue
                                
                                stock_code = row[0].strip()
                                if not stock_code or not stock_code.isdigit():
                                    continue
                                
                                # 解析價格和成交量 (舊格式欄位順序相同)
                                close_str = row[8].replace(',', '').strip()
                                open_str = row[5].replace(',', '').strip()
                                high_str = row[6].replace(',', '').strip()
                                low_str = row[7].replace(',', '').strip()
                                volume_str = row[2].replace(',', '').strip()
                                
                                # 移除 HTML 標籤 (舊格式可能包含 <p> 標籤)
                                import re
                                close_str = re.sub(r'<[^>]+>', '', close_str)
                                open_str = re.sub(r'<[^>]+>', '', open_str)
                                high_str = re.sub(r'<[^>]+>', '', high_str)
                                low_str = re.sub(r'<[^>]+>', '', low_str)
                                
                                if close_str in ['--', '---', ''] or volume_str in ['--', '0', '']:
                                    continue
                                
                                close_price = float(close_str)
                                volume = int(volume_str)
                                
                                if close_price <= 0 or volume <= 0 or close_price >= 30000:
                                    continue
                                
                                result[stock_code] = {
                                    'ticker': f"{stock_code}.TW",
                                    'Date': target_dt.strftime('%Y-%m-%d'),
                                    'Open': float(open_str) if open_str not in ['--', '---', ''] else None,
                                    'High': float(high_str) if high_str not in ['--', '---', ''] else None,
                                    'Low': float(low_str) if low_str not in ['--', '---', ''] else None,
                                    'Close': round(close_price, 2),
                                    'Volume': volume
                                }
                            except (ValueError, IndexError) as e:
                                continue
                        break  # 找到目標表格後跳出迴圈
            
            logger.debug(f"批量抓取 {target_dt.strftime('%Y-%m-%d')} 成功，共 {len(result)} 檔股票")
            return result
            
        except Exception as e:
            logger.error(f"批量抓取 {target_date} 失敗: {e}")
            return {}

    def fetch_tpex_all_stocks_day(self, target_date):
        try:
            if isinstance(target_date, str):
                target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            elif isinstance(target_date, datetime):
                target_dt = target_date.date()
            else:
                target_dt = target_date

            roc_date = f"{target_dt.year - 1911}/{target_dt.month:02d}/{target_dt.day:02d}"
            url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
            params = {
                'l': 'zh-tw',
                'd': roc_date,
                'se': 'AL',
                'o': 'json'
            }

            headers = {
                'Accept': 'application/json, text/plain, */*',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.tpex.org.tw/'
            }

            data = None
            last_exc = None
            for attempt in range(3):
                try:
                    response = self.tpex_session.get(url, params=params, headers=headers, timeout=20)
                except Exception as exc:
                    last_exc = exc
                    if attempt < 2:
                        time.sleep(0.8 * (attempt + 1))
                        continue
                    logger.warning(
                        f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 失敗: {type(exc).__name__}: {exc}"
                    )
                    return {}
                if response.status_code != 200:
                    if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                        time.sleep(0.8 * (attempt + 1))
                        continue
                    logger.warning(f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 失敗: HTTP {response.status_code}")
                    return {}

                content_type = (response.headers.get('Content-Type') or '').lower()
                text_head = (response.text or '')[:80].lstrip()
                is_json_like = text_head.startswith('{') or text_head.startswith('[')

                if 'application/json' not in content_type and not is_json_like:
                    if attempt < 2:
                        time.sleep(0.8 * (attempt + 1))
                        continue
                    snippet = (response.text or '')[:200].replace('\n', ' ').replace('\r', ' ')
                    logger.warning(
                        f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 回傳非 JSON"
                        f" (content-type={content_type or 'n/a'})"
                        f" head={snippet}"
                    )
                    return {}

                try:
                    data = response.json()
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt < 2:
                        time.sleep(0.8 * (attempt + 1))
                        continue
                    snippet = (response.text or '')[:200].replace('\n', ' ').replace('\r', ' ')
                    logger.warning(
                        f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 回傳非 JSON"
                        f" (json_error={type(exc).__name__})"
                        f" head={snippet}"
                    )
                    return {}

            if data is None:
                logger.warning(f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 失敗: {last_exc}")
                return {}

            if str(data.get('stat', '')).lower() != 'ok':
                logger.warning(f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 回傳非 ok: {data.get('stat')}")
                return {}

            result = {}
            tables = data.get('tables') or []
            for table in tables:
                rows = table.get('data') if isinstance(table, dict) else None
                if not rows:
                    continue

                for row in rows:
                    try:
                        if not (isinstance(row, list) and len(row) >= 8):
                            continue

                        stock_code = str(row[0]).strip()
                        if not stock_code or not stock_code.isdigit():
                            continue

                        close_str = str(row[2]).replace(',', '').strip()
                        open_str = str(row[4]).replace(',', '').strip()
                        high_str = str(row[5]).replace(',', '').strip()
                        low_str = str(row[6]).replace(',', '').strip()
                        volume_str = str(row[7]).replace(',', '').strip()

                        invalid = {'----', '---', '--', '', '0', 'NaN', 'null', 'None'}
                        if close_str in invalid or volume_str in invalid:
                            continue

                        close_price = float(close_str)
                        volume = int(float(volume_str))
                        if close_price <= 0 or volume <= 0 or close_price >= 30000:
                            continue

                        result[stock_code] = {
                            'ticker': f"{stock_code}.TWO",
                            'Date': target_dt.strftime('%Y-%m-%d'),
                            'Open': float(open_str) if open_str not in invalid else None,
                            'High': float(high_str) if high_str not in invalid else None,
                            'Low': float(low_str) if low_str not in invalid else None,
                            'Close': round(close_price, 2),
                            'Volume': volume
                        }
                    except Exception:
                        continue

            logger.debug(f"TPEX 批量抓取 {target_dt.strftime('%Y-%m-%d')} 成功，共 {len(result)} 檔股票")
            return result
        except Exception as e:
            logger.error(f"TPEX 批量抓取 {target_date} 失敗: {e}")
            return {}

    def fetch_tpex_stock_data_batch(self, stock_codes, start_date, end_date):
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            dates_to_fetch = []
            current = start_dt
            while current <= end_dt:
                if current.weekday() < 5:
                    dates_to_fetch.append(current)
                current += timedelta(days=1)

            logger.info(f"TPEX 批量抓取模式：{len(stock_codes)} 檔股票，{len(dates_to_fetch)} 個交易日")

            all_data = {}
            max_workers = int(os.getenv('TPEX_BATCH_WORKERS', str(min(3, max(1, len(dates_to_fetch))))))
            sleep_s = float(os.getenv('TPEX_BATCH_SLEEP', '0.2'))
            processed_days = 0
            total_days = len(dates_to_fetch)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_date = {executor.submit(self.fetch_tpex_all_stocks_day, date): date for date in dates_to_fetch}

                for future in as_completed(future_to_date):
                    date_val = future_to_date[future]
                    try:
                        day_data = future.result()
                        for stock_code in stock_codes:
                            if stock_code in day_data:
                                all_data.setdefault(stock_code, []).append(day_data[stock_code])
                    except Exception as e:
                        logger.error(f"TPEX 抓取 {date_val.strftime('%Y-%m-%d')} 失敗: {e}")

                    processed_days += 1
                    if total_days and (processed_days % 50 == 0 or processed_days == total_days):
                        pct = processed_days * 100.0 / total_days
                        logger.info(f"TPEX 批量抓取進度: {processed_days}/{total_days} ({pct:.1f}%)")

                    if sleep_s and sleep_s > 0:
                        time.sleep(sleep_s)

            for stock_code in all_data:
                all_data[stock_code].sort(key=lambda x: x['Date'])

            logger.info(f" TPEX 批量抓取完成，成功抓取 {len(all_data)} 檔股票")
            if not all_data:
                enable_yf = str(os.getenv('ENABLE_YFINANCE_FALLBACK', '1')).strip().lower() not in ('0', 'false', 'no', 'off')
                max_fallback = int(os.getenv('YFINANCE_FALLBACK_MAX_SYMBOLS', '50'))
                if enable_yf and len(stock_codes) <= max_fallback:
                    for code in stock_codes:
                        yf_records = self.fetch_stock_with_yfinance(f"{code}.TWO", start_date, end_date)
                        if yf_records:
                            all_data[code] = yf_records
                    if all_data:
                        logger.info(f" yfinance 備援完成，成功抓取 {len(all_data)} 檔股票")
            return all_data
        except Exception as e:
            logger.error(f"TPEX 批量抓取失敗: {e}")
            return {}

    def fetch_twse_bwibbu_all(self, force_refresh: bool = False):
        """抓取證交所 BWIBBU_ALL 指標資料。"""
        try:
            if (
                not force_refresh
                and self.bwibbu_cache is not None
                and self.bwibbu_cache_time is not None
                and time.time() - self.bwibbu_cache_time < self.bwibbu_cache_ttl
            ):
                return self.bwibbu_cache

            url = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError("TWSE BWIBBU_ALL 回傳非預期格式")

            self.bwibbu_cache = data
            self.bwibbu_cache_time = time.time()
            return data

        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            logger.error(f"抓取 BWIBBU_ALL 失敗: {exc}")
            raise

    def fetch_twse_bwibbu_by_date(self, target_date, retries: int = 3, sleep_between: float = 0.8):
        """抓取指定日期的 BWIBBU 指標（使用 TWSE BWIBBU_d 介面）。
        Args:
            target_date: datetime.date 或 'YYYY-MM-DD' 字串
        Returns:
            list[dict]: 每筆含 Code/Name/DividendYield/PEratio/PBratio/Date(ISO YYYY-MM-DD)
        """
        if isinstance(target_date, str):
            try:
                dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            except Exception:
                logger.error(f"抓取 BWIBBU_d 日期格式錯誤: {target_date}")
                return []
        else:
            dt = target_date

        ymd = dt.strftime('%Y%m%d')
        url = "https://www.twse.com.tw/exchangeReport/BWIBBU_d"
        params = {
            'response': 'json',
            'date': ymd,
            'selectType': 'ALL'
        }

        for attempt in range(1, retries + 1):
            try:
                resp = self.twse_session.get(url, params=params, timeout=15)
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}")
                try:
                    js = resp.json()
                except ValueError:
                    raise ValueError('non-json response')

                rows = js.get('data') or []
                stat_val = js.get('stat', '')
                logger.info(f"TWSE BWIBBU_d {ymd}: stat={stat_val}, rows={len(rows)}")
                if stat_val and '沒有符合' in stat_val:
                    return []
                if not rows:
                    return []
                
                fields = js.get('fields') or []
                out = []
                first_row_logged = False
                for r in rows:
                    try:
                        # 建立欄位索引，欄位名稱去除所有空白
                        data_map = {}
                        for idx, val in enumerate(r):
                            key = fields[idx] if idx < len(fields) else str(idx)
                            key = ''.join(key.split())  # 移除所有空白
                            data_map[key] = (val or '').strip() if isinstance(val, str) else str(val or '')

                        if not first_row_logged:
                            logger.info(f"TWSE BWIBBU_d {ymd} first row data_map keys: {list(data_map.keys())}")
                            first_row_logged = True

                        code = data_map.get('證券代號') or ''
                        name = data_map.get('證券名稱') or ''
                        dy_raw = data_map.get('殖利率(%)') or ''
                        pe_raw = data_map.get('本益比') or ''
                        pb_raw = data_map.get('股價淨值比') or ''

                        def to_num(s: str) -> str:
                            if s in ('', 'NaN', 'null', 'None', '--', '---'):
                                return ''
                            try:
                                return f"{float(s):.4f}"
                            except Exception:
                                return ''

                        out.append({
                            'Code': code,
                            'Name': name,
                            'DividendYield': to_num(dy_raw),
                            'PEratio': to_num(pe_raw),
                            'PBratio': to_num(pb_raw),
                            'Date': dt.strftime('%Y-%m-%d')
                        })
                    except Exception as parse_err:
                        logger.debug(f"TWSE BWIBBU_d {ymd} row parse error: {parse_err}")
                        continue
                logger.info(f"TWSE BWIBBU_d {ymd}: parsed {len(out)} records")
                return out
            except Exception as exc:
                logger.warning(f"抓取 BWIBBU_d {ymd} 失敗 (attempt {attempt}/{retries}): {exc}")
                if attempt == retries:
                    logger.error(f"抓取 BWIBBU_d {ymd} 最終失敗")
                    return []
                time.sleep(sleep_between)

    def fetch_tpex_bwibbu_by_date(self, target_date, retries: int = 3, sleep_between: float = 0.8):
        """抓取指定日期的 TPEX（上櫃）本益比/殖利率/淨值比。
        回傳 list[dict]，鍵為 Code/Name/DividendYield/PEratio/PBratio/Date。
        """
        if isinstance(target_date, str):
            try:
                dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            except Exception:
                return []
        else:
            dt = target_date

        # TPEX 歷史查詢頁面 JSON 介面，使用 ROC 日期 YYY/MM/DD
        roc_year = dt.year - 1911
        roc_date = f"{roc_year:03d}/{dt.month:02d}/{dt.day:02d}"
        url = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php"
        params = { 'l': 'zh-tw', 'o': 'json', 'd': roc_date }

        for attempt in range(1, retries + 1):
            try:
                resp = self.tpex_session.get(url, params=params, timeout=20)
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}")
                js = resp.json()
                tables = js.get('tables') or []
                if not tables:
                    return []
                data_rows = tables[0].get('data') or []
                if not data_rows or len(data_rows) <= 1:
                    return []
                rows = data_rows[1:]  # 第一列為表頭

                def to_num(val):
                    if val in (None, '', '----', '---', '-', 'NaN', 'null', 'None'):
                        return ''
                    try:
                        return f"{float(str(val).replace(',', '').strip()):.4f}"
                    except Exception:
                        return ''

                out = []
                for r in rows:
                    try:
                        code = (r[0] or '').strip()
                        name = (r[1] or '').strip()
                        pe_raw = (r[2] or '').strip() if len(r) > 2 else ''
                        # r[3] 每股股利，略過
                        dy_raw = (r[5] or '').strip() if len(r) > 5 else ''
                        pb_raw = (r[6] or '').strip() if len(r) > 6 else ''
                        out.append({
                            'Code': code,
                            'Name': name,
                            'DividendYield': to_num(dy_raw),
                            'PEratio': to_num(pe_raw),
                            'PBratio': to_num(pb_raw),
                            'Date': dt.strftime('%Y-%m-%d')
                        })
                    except Exception:
                        continue
                return out
            except Exception as exc:
                logger.warning(f"抓取 TPEX BWIBBU {roc_date} 失敗 (attempt {attempt}/{retries}): {exc}")
                if attempt == retries:
                    return []
                time.sleep(sleep_between)

    def fetch_tpex_t86_by_date(self, target_date):
        """抓取指定日期的櫃買中心三大法人買賣超資料。"""
        dt = self._ensure_date(target_date)
        roc_date = f"{dt.year - 1911:03d}/{dt.month:02d}/{dt.day:02d}"
        url = self.TPEX_T86_URL
        params = {
            "l": "zh-tw",
            "o": "json",
            "d": roc_date,
            "se": "AL",
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge.php",
            "X-Requested-With": "XMLHttpRequest",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        payload = None
        last_error = None
        for attempt in range(1, 4):
            try:
                resp = self.tpex_session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=30,
                    allow_redirects=True,
                )
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(
                        f"HTTP {resp.status_code}", response=resp
                    )
                resp.raise_for_status()
                response_text = resp.text or ""
                content_type = (resp.headers.get("Content-Type") or "").lower()
                if not response_text.strip():
                    raise ValueError("TPEX 回傳空內容")
                if "json" not in content_type and response_text.lstrip()[:1] not in ("{", "["):
                    snippet = response_text[:300].replace("\r", " ").replace("\n", " ")
                    raise ValueError(f"TPEX 回傳非 JSON：{snippet}")
                payload = resp.json()
                if not isinstance(payload, dict):
                    raise ValueError(f"TPEX 回傳格式錯誤：{type(payload).__name__}")
                break
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "TPEX T86 %s 抓取失敗：attempt=%s/3 error=%s",
                    dt.isoformat(),
                    attempt,
                    exc,
                )
                if attempt < 3:
                    time.sleep(1.5 * attempt)

        if payload is None:
            raise RuntimeError(
                f"TPEX T86 {dt.isoformat()} 抓取失敗：{last_error}"
            )

        stat = str(payload.get("stat") or payload.get("status") or "").strip()
        if stat and stat.lower() not in {"ok", "success"}:
            no_data_messages = ("沒有符合條件的資料", "查無資料", "無資料", "很抱歉")
            if any(msg in stat for msg in no_data_messages):
                logger.info("TPEX T86 %s 無交易資料：%s", dt.isoformat(), stat)
                return []

        tables = payload.get("tables") or []
        if not isinstance(tables, list) or not tables:
            logger.info("TPEX T86 %s 無資料表", dt.isoformat())
            return []

        table = next(
            (
                item for item in tables
                if isinstance(item, dict) and isinstance(item.get("data"), list)
            ),
            None,
        )
        if not table:
            return []

        rows = table.get("data") or []
        fields = table.get("fields") or table.get("columns") or []

        def normalize_label(value):
            return re.sub(r"[\s\n\r（）()－\-_/]", "", str(value or ""))

        normalized_fields = [normalize_label(x) for x in fields]

        def find_index(*keywords):
            for idx, label in enumerate(normalized_fields):
                if all(keyword in label for keyword in keywords):
                    return idx
            return None

        # 欄位名稱優先；若 API 未提供 fields，使用櫃買中心既有欄位順序。
        index_map = {
            "stock_no": find_index("代號"),
            "stock_name": find_index("名稱"),
            "foreign_buy": find_index("外資", "買進"),
            "foreign_sell": find_index("外資", "賣出"),
            "foreign_net": find_index("外資", "買賣超"),
            "foreign_dealer_buy": find_index("外資自營商", "買進"),
            "foreign_dealer_sell": find_index("外資自營商", "賣出"),
            "foreign_dealer_net": find_index("外資自營商", "買賣超"),
            "foreign_total_buy": find_index("外資及陸資", "買進"),
            "foreign_total_sell": find_index("外資及陸資", "賣出"),
            "foreign_total_net": find_index("外資及陸資", "買賣超"),
            "investment_trust_buy": find_index("投信", "買進"),
            "investment_trust_sell": find_index("投信", "賣出"),
            "investment_trust_net": find_index("投信", "買賣超"),
            "dealer_self_buy": find_index("自營商自行買賣", "買進"),
            "dealer_self_sell": find_index("自營商自行買賣", "賣出"),
            "dealer_self_net": find_index("自營商自行買賣", "買賣超"),
            "dealer_hedge_buy": find_index("自營商避險", "買進"),
            "dealer_hedge_sell": find_index("自營商避險", "賣出"),
            "dealer_hedge_net": find_index("自營商避險", "買賣超"),
            "dealer_total_buy": find_index("自營商", "買進"),
            "dealer_total_sell": find_index("自營商", "賣出"),
            "dealer_total_net": find_index("自營商", "買賣超"),
            "overall_net": find_index("三大法人", "買賣超"),
        }

        fallback_indexes = {
            "stock_no": 0,
            "stock_name": 1,
            "foreign_buy": 2,
            "foreign_sell": 3,
            "foreign_net": 4,
            "foreign_dealer_buy": 5,
            "foreign_dealer_sell": 6,
            "foreign_dealer_net": 7,
            "foreign_total_buy": 8,
            "foreign_total_sell": 9,
            "foreign_total_net": 10,
            "investment_trust_buy": 11,
            "investment_trust_sell": 12,
            "investment_trust_net": 13,
            "dealer_self_buy": 14,
            "dealer_self_sell": 15,
            "dealer_self_net": 16,
            "dealer_hedge_buy": 17,
            "dealer_hedge_sell": 18,
            "dealer_hedge_net": 19,
            "dealer_total_buy": 20,
            "dealer_total_sell": 21,
            "dealer_total_net": 22,
            "overall_net": 23,
        }

        def get_value(row, key, default=None):
            idx = index_map.get(key)
            if idx is None:
                idx = fallback_indexes[key]
            if not isinstance(row, list) or idx >= len(row):
                return default
            return row[idx]

        results = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue

            stock_no = str(get_value(row, "stock_no", "") or "").strip()
            stock_name = str(get_value(row, "stock_name", "") or "").strip()
            if not stock_no or stock_no in {"代號", "證券代號"}:
                continue

            record = {
                "date": dt.isoformat(),
                "market": "TPEX",
                "stock_no": stock_no,
                "stock_name": stock_name,
            }
            for key in fallback_indexes:
                if key in {"stock_no", "stock_name"}:
                    continue
                record[key] = self._t86_parse_int(get_value(row, key, 0))

            # 某些版本不提供彙總欄位時，自行計算。
            if index_map.get("foreign_total_buy") is None and len(row) <= fallback_indexes["foreign_total_buy"]:
                record["foreign_total_buy"] = record["foreign_buy"] + record["foreign_dealer_buy"]
                record["foreign_total_sell"] = record["foreign_sell"] + record["foreign_dealer_sell"]
                record["foreign_total_net"] = record["foreign_net"] + record["foreign_dealer_net"]
            if index_map.get("dealer_total_buy") is None and len(row) <= fallback_indexes["dealer_total_buy"]:
                record["dealer_total_buy"] = record["dealer_self_buy"] + record["dealer_hedge_buy"]
                record["dealer_total_sell"] = record["dealer_self_sell"] + record["dealer_hedge_sell"]
                record["dealer_total_net"] = record["dealer_self_net"] + record["dealer_hedge_net"]
            if index_map.get("overall_net") is None and len(row) <= fallback_indexes["overall_net"]:
                record["overall_net"] = (
                    record["foreign_total_net"]
                    + record["investment_trust_net"]
                    + record["dealer_total_net"]
                )

            results.append(record)

        logger.info("TPEX T86 %s 抓取 %s 筆", dt.isoformat(), len(results))
        return results

    def fetch_twse_t86_by_date(self, target_date):
        dt = self._ensure_date(target_date)
        urls = [
            "https://www.twse.com.tw/rwd/zh/fund/T86",
            "https://www.twse.com.tw/fund/T86",
        ]
        params = {
            "response": "json",
            "date": dt.strftime("%Y%m%d"),
            "selectType": "ALLBUT0999",
            "_": str(int(time.time() * 1000)),
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.twse.com.tw/zh/trading/foreign/t86.html",
            "X-Requested-With": "XMLHttpRequest",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            # 移除 __init__ 中 BWIBBU session 設定的 Origin
            "Origin": None,
        }
        payload = None
        last_error = None
        used_url = None
        for url in urls:
            for attempt in range(1, 4):
                try:
                    resp = self.twse_session.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=30,
                        allow_redirects=True,
                    )
                    if resp.status_code in (429, 500, 502, 503, 504):
                        raise requests.HTTPError(
                            f"HTTP {resp.status_code}",
                            response=resp,
                        )
                    resp.raise_for_status()
                    response_text = resp.text or ""
                    content_type = (
                        resp.headers.get("Content-Type") or ""
                    ).lower()
                    text_head = response_text.lstrip()[:1]
                    if not response_text.strip():
                        raise ValueError("TWSE 回傳空內容")
                    if (
                        "json" not in content_type
                        and text_head not in ("{", "[")
                    ):
                        snippet = (
                            response_text[:300]
                            .replace("\r", " ")
                            .replace("\n", " ")
                        )
                        raise ValueError(
                            f"TWSE 回傳非 JSON：{snippet}"
                        )
                    try:
                        payload = resp.json()
                    except (ValueError, json.JSONDecodeError) as exc:
                        snippet = (
                            response_text[:300]
                            .replace("\r", " ")
                            .replace("\n", " ")
                        )
                        raise ValueError(
                            f"TWSE JSON 解析失敗：{exc}；內容：{snippet}"
                        ) from exc
                    if not isinstance(payload, dict):
                        raise ValueError(
                            f"TWSE 回傳格式錯誤：{type(payload).__name__}"
                        )
                    used_url = url
                    break
                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "TWSE T86 %s 抓取失敗：url=%s attempt=%s/3 error=%s",
                        dt.isoformat(),
                        url,
                        attempt,
                        exc,
                    )
                    if attempt < 3:
                        time.sleep(1.5 * attempt)
            if payload is not None:
                break
        if payload is None:
            raise RuntimeError(
                f"TWSE T86 {dt.isoformat()} 抓取失敗：{last_error}"
            )
        stat = str(payload.get("stat") or "").strip()
        data_rows = payload.get("data") or []
        if stat.upper() != "OK":
            no_data_messages = (
                "沒有符合條件的資料",
                "查無資料",
                "無資料",
                "很抱歉",
            )
            if any(message in stat for message in no_data_messages):
                logger.info(
                    "TWSE T86 %s 無交易資料：%s",
                    dt.isoformat(),
                    stat,
                )
                return []
            raise RuntimeError(
                f"TWSE T86 {dt.isoformat()} 回傳異常：stat={stat}"
            )
        if not isinstance(data_rows, list):
            raise RuntimeError(
                f"TWSE T86 {dt.isoformat()} data 欄位不是陣列"
            )
        results = []
        for row in data_rows:
            if not isinstance(row, list) or len(row) < 19:
                continue
            stock_no = str(row[0] or "").strip()
            stock_name = str(row[1] or "").strip()
            if not stock_no:
                continue
            foreign_buy = self._t86_parse_int(row[2])
            foreign_sell = self._t86_parse_int(row[3])
            foreign_net = self._t86_parse_int(row[4])
            foreign_dealer_buy = self._t86_parse_int(row[5])
            foreign_dealer_sell = self._t86_parse_int(row[6])
            foreign_dealer_net = self._t86_parse_int(row[7])
            investment_trust_buy = self._t86_parse_int(row[8])
            investment_trust_sell = self._t86_parse_int(row[9])
            investment_trust_net = self._t86_parse_int(row[10])
            dealer_total_net = self._t86_parse_int(row[11])
            dealer_self_buy = self._t86_parse_int(row[12])
            dealer_self_sell = self._t86_parse_int(row[13])
            dealer_self_net = self._t86_parse_int(row[14])
            dealer_hedge_buy = self._t86_parse_int(row[15])
            dealer_hedge_sell = self._t86_parse_int(row[16])
            dealer_hedge_net = self._t86_parse_int(row[17])
            overall_net = self._t86_parse_int(row[18])
            results.append({
                "date": dt.isoformat(),
                "market": "TWSE",
                "stock_no": stock_no,
                "stock_name": stock_name,
                "foreign_buy": foreign_buy,
                "foreign_sell": foreign_sell,
                "foreign_net": foreign_net,
                "foreign_dealer_buy": foreign_dealer_buy,
                "foreign_dealer_sell": foreign_dealer_sell,
                "foreign_dealer_net": foreign_dealer_net,
                "foreign_total_buy": (
                    foreign_buy + foreign_dealer_buy
                ),
                "foreign_total_sell": (
                    foreign_sell + foreign_dealer_sell
                ),
                "foreign_total_net": (
                    foreign_net + foreign_dealer_net
                ),
                "investment_trust_buy": investment_trust_buy,
                "investment_trust_sell": investment_trust_sell,
                "investment_trust_net": investment_trust_net,
                "dealer_self_buy": dealer_self_buy,
                "dealer_self_sell": dealer_self_sell,
                "dealer_self_net": dealer_self_net,
                "dealer_hedge_buy": dealer_hedge_buy,
                "dealer_hedge_sell": dealer_hedge_sell,
                "dealer_hedge_net": dealer_hedge_net,
                "dealer_total_buy": (
                    dealer_self_buy + dealer_hedge_buy
                ),
                "dealer_total_sell": (
                    dealer_self_sell + dealer_hedge_sell
                ),
                "dealer_total_net": dealer_total_net,
                "overall_net": overall_net,
            })
        logger.info(
            "TWSE T86 %s 抓取 %s 筆，來源=%s",
            dt.isoformat(),
            len(results),
            used_url,
        )
        return results

    def fetch_t86_range(self, start_date, end_date, market: str = 'both', sleep_seconds: float = 0.6):
        start_dt = self._ensure_date(start_date)
        end_dt = self._ensure_date(end_date)
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt

        market_key = (market or 'both').lower()
        if market_key not in {'twse', 'tpex', 'both'}:
            raise ValueError("market 必須為 'twse'、'tpex' 或 'both'")

        markets = {'twse', 'tpex'} if market_key == 'both' else {market_key}

        results: list[dict] = []
        daily_stats: list[dict] = []
        total_twse = 0
        total_tpex = 0

        current = start_dt
        while current <= end_dt:
            day_records = []
            twse_count = 0
            tpex_count = 0

            if 'twse' in markets:
                try:
                    twse_records = self.fetch_twse_t86_by_date(current)
                except Exception as exc:
                    logger.warning(f"TWSE T86 {current} 抓取失敗: {exc}")
                    twse_records = []
                day_records.extend(twse_records)
                twse_count = len(twse_records)

            if 'tpex' in markets:
                try:
                    tpex_records = self.fetch_tpex_t86_by_date(current)
                except Exception as exc:
                    logger.warning(f"TPEX T86 {current} 抓取失敗: {exc}")
                    tpex_records = []
                day_records.extend(tpex_records)
                tpex_count = len(tpex_records)

            if day_records:
                results.extend(day_records)

            daily_stats.append({
                'date': current.isoformat(),
                'twse_count': twse_count,
                'tpex_count': tpex_count,
                'total_count': twse_count + tpex_count,
            })

            total_twse += twse_count
            total_tpex += tpex_count

            if sleep_seconds and sleep_seconds > 0:
                time.sleep(sleep_seconds)

            current += timedelta(days=1)

        summary = {
            'start_date': start_dt.isoformat(),
            'end_date': end_dt.isoformat(),
            'markets': sorted(markets),
            'days_processed': len(daily_stats),
            'total_records': len(results),
            'per_market': {
                'TWSE': total_twse,
                'TPEX': total_tpex,
            },
        }

        return results, summary, daily_stats

    def upsert_t86_records(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        """Persist T86 records into institutional trades table."""
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager()
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_institutional', 'tw_institutional_trades')

            def to_int(value):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return 0

            values = []
            for rec in records:
                date_str = rec.get('date')
                stock_no = (rec.get('stock_no') or '').strip()
                if not date_str or not stock_no:
                    continue
                try:
                    record_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except Exception:
                    continue

                market = (rec.get('market') or '').strip().upper() or 'TWSE'
                stock_name = rec.get('stock_name')

                values.append((
                    record_date,
                    market,
                    stock_no,
                    stock_name,
                    to_int(rec.get('foreign_buy')),
                    to_int(rec.get('foreign_sell')),
                    to_int(rec.get('foreign_net')),
                    to_int(rec.get('foreign_dealer_buy')),
                    to_int(rec.get('foreign_dealer_sell')),
                    to_int(rec.get('foreign_dealer_net')),
                    to_int(rec.get('foreign_total_buy')),
                    to_int(rec.get('foreign_total_sell')),
                    to_int(rec.get('foreign_total_net')),
                    to_int(rec.get('investment_trust_buy')),
                    to_int(rec.get('investment_trust_sell')),
                    to_int(rec.get('investment_trust_net')),
                    to_int(rec.get('dealer_self_buy')),
                    to_int(rec.get('dealer_self_sell')),
                    to_int(rec.get('dealer_self_net')),
                    to_int(rec.get('dealer_hedge_buy')),
                    to_int(rec.get('dealer_hedge_sell')),
                    to_int(rec.get('dealer_hedge_net')),
                    to_int(rec.get('dealer_total_buy')),
                    to_int(rec.get('dealer_total_sell')),
                    to_int(rec.get('dealer_total_net')),
                    to_int(rec.get('overall_net')),
                ))

            if not values:
                return 0

            insert_sql = f"""
                INSERT INTO {table_name} (
                    date, market, stock_no, stock_name,
                    foreign_buy, foreign_sell, foreign_net,
                    foreign_dealer_buy, foreign_dealer_sell, foreign_dealer_net,
                    foreign_total_buy, foreign_total_sell, foreign_total_net,
                    investment_trust_buy, investment_trust_sell, investment_trust_net,
                    dealer_self_buy, dealer_self_sell, dealer_self_net,
                    dealer_hedge_buy, dealer_hedge_sell, dealer_hedge_net,
                    dealer_total_buy, dealer_total_sell, dealer_total_net,
                    overall_net
                )
                VALUES %s
                ON CONFLICT (date, market, stock_no) DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    foreign_buy = EXCLUDED.foreign_buy,
                    foreign_sell = EXCLUDED.foreign_sell,
                    foreign_net = EXCLUDED.foreign_net,
                    foreign_dealer_buy = EXCLUDED.foreign_dealer_buy,
                    foreign_dealer_sell = EXCLUDED.foreign_dealer_sell,
                    foreign_dealer_net = EXCLUDED.foreign_dealer_net,
                    foreign_total_buy = EXCLUDED.foreign_total_buy,
                    foreign_total_sell = EXCLUDED.foreign_total_sell,
                    foreign_total_net = EXCLUDED.foreign_total_net,
                    investment_trust_buy = EXCLUDED.investment_trust_buy,
                    investment_trust_sell = EXCLUDED.investment_trust_sell,
                    investment_trust_net = EXCLUDED.investment_trust_net,
                    dealer_self_buy = EXCLUDED.dealer_self_buy,
                    dealer_self_sell = EXCLUDED.dealer_self_sell,
                    dealer_self_net = EXCLUDED.dealer_self_net,
                    dealer_hedge_buy = EXCLUDED.dealer_hedge_buy,
                    dealer_hedge_sell = EXCLUDED.dealer_hedge_sell,
                    dealer_hedge_net = EXCLUDED.dealer_hedge_net,
                    dealer_total_buy = EXCLUDED.dealer_total_buy,
                    dealer_total_sell = EXCLUDED.dealer_total_sell,
                    dealer_total_net = EXCLUDED.dealer_total_net,
                    overall_net = EXCLUDED.overall_net,
                    updated_at = CURRENT_TIMESTAMP
            """

            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def upsert_financial_ratios(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager(use_local=self.use_local)
            own_manager = True

        # 若外部已提供 db_manager 且連線已存在，避免每批次重複 connect/create_tables，
        # 否則會因 create_tables 的鎖而造成嚴重延遲。
        if getattr(db, 'connection', None) is None:
            if not db.connect():
                raise RuntimeError("資料庫連線失敗")

        try:
            if own_manager:
                db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_financial_ratios', financial_ratios_table(use_neon=db.is_neon))

            metric_cols = list(FINANCIAL_RATIO_COLS)
            insert_cols = ['symbol', 'period'] + list(metric_cols)
            insert_cols_sql = ", ".join(insert_cols)

            def _normalize_symbol(value: str) -> str:
                s = (value or '').strip()
                if not s:
                    return ''
                if '.' in s:
                    return s
                return f"{s}.TW"

            def _to_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).replace(',', '').strip()
                if not s:
                    return None
                try:
                    return float(s)
                except Exception:
                    return None

            values = []
            for rec in records:
                code = _normalize_symbol(rec.get('symbol') or rec.get('股票代號') or '')
                period = str(rec.get('period') or '').strip()
                if not code or not period:
                    continue
                row_vals = [code, period]
                for col in metric_cols:
                    row_vals.append(_to_num(rec.get(col)))
                values.append(tuple(row_vals))

            if not values:
                try:
                    sample = records[0] if records else None
                    logger.warning(
                        "[ratios][upsert] skipped all records: total=%d sample_keys=%s sample_symbol=%s sample_period=%s",
                        len(records),
                        [] if not isinstance(sample, dict) else list(sample.keys())[:20],
                        None if not isinstance(sample, dict) else sample.get('symbol'),
                        None if not isinstance(sample, dict) else sample.get('period'),
                    )
                except Exception:
                    pass
                return 0

            update_assignments = ", ".join([f"{col} = EXCLUDED.{col}" for col in metric_cols])
            insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols_sql})
                VALUES %s
                ON CONFLICT (symbol, period) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """
            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def upsert_balance_sheets(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        """將資產負債表寬表資料寫入 tw_balance_sheets 資料表。

        每一列應至少包含："股票代號"、"period" 欄位，其餘欄位依 BALANCE_TARGET_ORDER 對應。
        """
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager(use_local=self.use_local)
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_balance', balance_sheet_table(use_neon=db.is_neon))

            metric_cols = list(BALANCE_TARGET_ORDER)
            insert_cols = ['"股票代號"', 'period'] + [f'"{c}"' for c in metric_cols]
            insert_cols_sql = ", ".join(insert_cols)

            def _to_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).replace(',', '').strip()
                if not s:
                    return None
                try:
                    return float(s)
                except Exception:
                    return None

            values = []
            for rec in records:
                code = (rec.get('股票代號') or '').strip()
                period = str(rec.get('period') or '').strip()
                if not code or not period:
                    continue
                row_vals = [code, period]
                for col in metric_cols:
                    row_vals.append(_to_num(rec.get(col)))
                values.append(tuple(row_vals))

            if not values:
                return 0

            update_assignments = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in metric_cols])
            insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols_sql})
                VALUES %s
                ON CONFLICT ("股票代號", period) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """
            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def upsert_cash_flows(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        """將現金流量表寬表資料新增或更新至資料庫。"""
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager(use_local=self.use_local)
            own_manager = True
        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_cash_flow', cash_flow_table(use_neon=db.is_neon))
            metric_cols = list(CASH_FLOW_TARGET_ORDER)
            insert_cols = ['"股票代號"', 'period'] + [f'"{col}"' for col in metric_cols]

            def _to_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                try:
                    return float(str(value).replace(',', '').strip())
                except (TypeError, ValueError):
                    return None

            values = []
            for record in records:
                code = str(record.get('股票代號') or '').strip()
                period = str(record.get('period') or '').strip()
                if not code or not period:
                    continue
                values.append(
                    tuple([code, period] + [_to_num(record.get(col)) for col in metric_cols])
                )
            if not values:
                return 0

            update_assignments = ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in metric_cols]
            )
            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(insert_cols)})
                VALUES %s
                ON CONFLICT ("股票代號", period) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """
            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def upsert_income_statements(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        """將損益表寬表資料寫入 income_statements / tw_income_statements 資料表。

        每一列應至少包含："股票代號"、"period" 欄位，其餘欄位依 TARGET_ORDER 對應。
        """
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager(use_local=self.use_local)
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_income', income_statement_table(use_neon=db.is_neon))

            metric_cols = list(TARGET_ORDER)
            insert_cols = ['"股票代號"', 'period'] + [f'"{c}"' for c in metric_cols]
            insert_cols_sql = ", ".join(insert_cols)

            def _to_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).replace(',', '').strip()
                if not s:
                    return None
                try:
                    return float(s)
                except Exception:
                    return None

            values = []
            for rec in records:
                code = (rec.get('股票代號') or '').strip()
                period = str(rec.get('period') or '').strip()
                if not code or not period:
                    continue
                row_vals = [code, period]
                for col in metric_cols:
                    row_vals.append(_to_num(rec.get(col)))
                values.append(tuple(row_vals))

            if not values:
                return 0

            update_assignments = ", ".join([
                f'"{col}" = EXCLUDED."{col}"' for col in metric_cols
            ])

            insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols_sql})
                VALUES %s
                ON CONFLICT ("股票代號", period) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """

            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def fetch_twse_margin_by_date(self, target_date):
        """抓取單日 TWSE 融資融券資料 (MI_MARGN)。"""
        dt = self._ensure_date(target_date)
        params = {
            'response': 'json',
            'date': dt.strftime('%Y%m%d'),
            'selectType': 'ALL',
        }

        resp = self.twse_session.get(self.TWSE_MARGIN_URL, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get('stat') != 'OK':
            logger.info(f"TWSE MI_MARGN {dt} stat={payload.get('stat')}, 無資料")
            return []

        tables = payload.get('tables') or []
        target_table = None
        for tbl in tables:
            fields = tbl.get('fields') or []
            # 尋找含有股票代號/名稱欄位的彙總表
            if fields and fields[0] == '代號' and len(fields) >= 16:
                target_table = tbl
                break

        if not target_table:
            logger.warning(f"TWSE MI_MARGN {dt} 找不到彙總表")
            return []

        data_rows = target_table.get('data') or []
        results: list[dict] = []
        for row in data_rows:
            if len(row) < 16:
                continue
            stock_no = (row[0] or '').strip()
            stock_name = (row[1] or '').strip()
            if not stock_no:
                continue

            try:
                margin_buy = self._t86_parse_int(row[2])
                margin_sell = self._t86_parse_int(row[3])
                margin_repay = self._t86_parse_int(row[4])
                margin_prev_balance = self._t86_parse_int(row[5])
                margin_balance = self._t86_parse_int(row[6])
                margin_limit = self._t86_parse_int(row[7])

                short_buy = self._t86_parse_int(row[8])
                short_sell = self._t86_parse_int(row[9])
                short_repay = self._t86_parse_int(row[10])
                short_prev_balance = self._t86_parse_int(row[11])
                short_balance = self._t86_parse_int(row[12])
                short_limit = self._t86_parse_int(row[13])

                offset_quantity = self._t86_parse_int(row[14])
                note = (row[15] or '').strip() if len(row) > 15 and row[15] is not None else ''
            except Exception:
                continue

            results.append({
                'date': dt.isoformat(),
                'market': 'TWSE',
                'stock_no': stock_no,
                'stock_name': stock_name,
                'margin_prev_balance': margin_prev_balance,
                'margin_buy': margin_buy,
                'margin_sell': margin_sell,
                'margin_repay': margin_repay,
                'margin_balance': margin_balance,
                'margin_limit': margin_limit,
                'short_prev_balance': short_prev_balance,
                'short_sell': short_sell,
                'short_buy': short_buy,
                'short_repay': short_repay,
                'short_balance': short_balance,
                'short_limit': short_limit,
                'offset_quantity': offset_quantity,
                'note': note,
            })

        logger.info(f"TWSE MI_MARGN {dt} 抓取 {len(results)} 筆")
        return results

    def fetch_tpex_margin_by_date(self, target_date):
        """抓取單日 TPEX 上櫃融資融券餘額資料。"""
        dt = self._ensure_date(target_date)
        roc_date = f"{dt.year - 1911:03d}/{dt.month:02d}/{dt.day:02d}"
        params = {
            'l': 'zh-tw',
            'd': roc_date,
            'stkno': '0',
            't': '0',
        }

        resp = self.tpex_session.get(self.TPEX_MARGIN_URL, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get('stat', '').lower() != 'ok':
            logger.info(f"TPEX margin_balance {dt} stat={payload.get('stat')}, 無資料")
            return []

        tables = payload.get('tables') or []
        if not tables:
            return []

        table0 = tables[0]
        data_rows = table0.get('data') or []
        results: list[dict] = []
        for row in data_rows:
            if len(row) < 20:
                continue
            stock_no = (row[0] or '').strip()
            stock_name = (row[1] or '').strip()
            if not stock_no:
                continue

            try:
                margin_prev_balance = self._t86_parse_int(row[2])
                margin_buy = self._t86_parse_int(row[3])
                margin_sell = self._t86_parse_int(row[4])
                margin_repay = self._t86_parse_int(row[5])
                margin_balance = self._t86_parse_int(row[6])
                margin_limit = self._t86_parse_int(row[9])

                short_prev_balance = self._t86_parse_int(row[10])
                short_sell = self._t86_parse_int(row[11])
                short_buy = self._t86_parse_int(row[12])
                short_repay = self._t86_parse_int(row[13])
                short_balance = self._t86_parse_int(row[14])
                short_limit = self._t86_parse_int(row[17])

                offset_quantity = self._t86_parse_int(row[18])
                note = (row[19] or '').strip() if len(row) > 19 and row[19] is not None else ''
            except Exception:
                continue

            results.append({
                'date': dt.isoformat(),
                'market': 'TPEX',
                'stock_no': stock_no,
                'stock_name': stock_name,
                'margin_prev_balance': margin_prev_balance,
                'margin_buy': margin_buy,
                'margin_sell': margin_sell,
                'margin_repay': margin_repay,
                'margin_balance': margin_balance,
                'margin_limit': margin_limit,
                'short_prev_balance': short_prev_balance,
                'short_sell': short_sell,
                'short_buy': short_buy,
                'short_repay': short_repay,
                'short_balance': short_balance,
                'short_limit': short_limit,
                'offset_quantity': offset_quantity,
                'note': note,
            })

        logger.info(f"TPEX margin_balance {dt} 抓取 {len(results)} 筆")
        return results

    def fetch_margin_range(self, start_date, end_date, market: str = 'both', sleep_seconds: float = 0.6):
        """抓取融資融券區間資料，支援 TWSE / TPEX / both。"""
        start_dt = self._ensure_date(start_date)
        end_dt = self._ensure_date(end_date)
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt

        market_key = (market or 'both').lower()
        if market_key not in {'twse', 'tpex', 'both'}:
            raise ValueError("market 必須為 'twse'、'tpex' 或 'both'")

        markets = {'twse', 'tpex'} if market_key == 'both' else {market_key}

        results: list[dict] = []
        daily_stats: list[dict] = []
        total_twse = 0
        total_tpex = 0

        current = start_dt
        while current <= end_dt:
            day_records: list[dict] = []
            twse_count = 0
            tpex_count = 0

            if 'twse' in markets:
                try:
                    twse_records = self.fetch_twse_margin_by_date(current)
                except Exception as exc:
                    logger.warning(f"TWSE margin {current} 抓取失敗: {exc}")
                    twse_records = []
                day_records.extend(twse_records)
                twse_count = len(twse_records)

            if 'tpex' in markets:
                try:
                    tpex_records = self.fetch_tpex_margin_by_date(current)
                except Exception as exc:
                    logger.warning(f"TPEX margin {current} 抓取失敗: {exc}")
                    tpex_records = []
                day_records.extend(tpex_records)
                tpex_count = len(tpex_records)

            if day_records:
                results.extend(day_records)

            daily_stats.append({
                'date': current.isoformat(),
                'twse_count': twse_count,
                'tpex_count': tpex_count,
                'total_count': twse_count + tpex_count,
            })

            total_twse += twse_count
            total_tpex += tpex_count

            if sleep_seconds and sleep_seconds > 0:
                time.sleep(sleep_seconds)

            current += timedelta(days=1)

        summary = {
            'start_date': start_dt.isoformat(),
            'end_date': end_dt.isoformat(),
            'markets': sorted(markets),
            'days_processed': len(daily_stats),
            'total_records': len(results),
            'per_market': {
                'TWSE': total_twse,
                'TPEX': total_tpex,
            },
        }

        return results, summary, daily_stats

    def upsert_margin_records(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        """將融資融券資料寫入 margin trades 資料表。"""
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager()
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_margin', 'tw_margin_trades')

            def to_int(value):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return 0

            values = []
            for rec in records:
                date_str = rec.get('date')
                stock_no = (rec.get('stock_no') or '').strip()
                if not date_str or not stock_no:
                    continue
                try:
                    record_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except Exception:
                    continue

                market = (rec.get('market') or '').strip().upper() or 'TWSE'
                stock_name = rec.get('stock_name')

                values.append((
                    record_date,
                    market,
                    stock_no,
                    stock_name,
                    to_int(rec.get('margin_prev_balance')),
                    to_int(rec.get('margin_buy')),
                    to_int(rec.get('margin_sell')),
                    to_int(rec.get('margin_repay')),
                    to_int(rec.get('margin_balance')),
                    to_int(rec.get('margin_limit')),
                    to_int(rec.get('short_prev_balance')),
                    to_int(rec.get('short_sell')),
                    to_int(rec.get('short_buy')),
                    to_int(rec.get('short_repay')),
                    to_int(rec.get('short_balance')),
                    to_int(rec.get('short_limit')),
                    to_int(rec.get('offset_quantity')),
                    rec.get('note'),
                ))

            if not values:
                return 0

            insert_sql = f"""
                INSERT INTO {table_name} (
                    date, market, stock_no, stock_name,
                    margin_prev_balance, margin_buy, margin_sell, margin_repay, margin_balance, margin_limit,
                    short_prev_balance, short_sell, short_buy, short_repay, short_balance, short_limit,
                    offset_quantity, note
                )
                VALUES %s
                ON CONFLICT (date, market, stock_no) DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    margin_prev_balance = EXCLUDED.margin_prev_balance,
                    margin_buy = EXCLUDED.margin_buy,
                    margin_sell = EXCLUDED.margin_sell,
                    margin_repay = EXCLUDED.margin_repay,
                    margin_balance = EXCLUDED.margin_balance,
                    margin_limit = EXCLUDED.margin_limit,
                    short_prev_balance = EXCLUDED.short_prev_balance,
                    short_sell = EXCLUDED.short_sell,
                    short_buy = EXCLUDED.short_buy,
                    short_repay = EXCLUDED.short_repay,
                    short_balance = EXCLUDED.short_balance,
                    short_limit = EXCLUDED.short_limit,
                    offset_quantity = EXCLUDED.offset_quantity,
                    note = EXCLUDED.note,
                    updated_at = CURRENT_TIMESTAMP
            """

            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def fetch_monthly_revenue(self, year: int | str | None = None, month: int | str | None = None, market: str = 'both'):
        """抓取指定年月或最新一個月的上市/上櫃月營收資料。"""
        # 先標準化與驗證市場參數
        market_key = (market or 'both').lower()
        if market_key not in {'twse', 'tpex', 'both'}:
            raise ValueError("market 必須為 'twse'、'tpex' 或 'both'")
        markets = {'twse', 'tpex'} if market_key == 'both' else {market_key}

        # 若有明確指定 year/month，直接使用 MOPS HTML 版本抓取
        if year is not None and month is not None:
            try:
                y = int(year)
                m = int(month)
                if m < 1 or m > 12:
                    raise ValueError
            except Exception:
                raise ValueError("year / month 參數格式錯誤，需為有效西元年與月份")

            return self.fetch_monthly_revenue_html(y, m, market=market)

        # 僅提供 year 或 month 其中一個屬於錯誤狀況
        if (year is None) ^ (month is None):
            raise ValueError("year 與 month 需同時提供或同時省略")

        # 未指定 year/month 時，改為從當前時間往回尋找「最近一個有資料的月份」，完全以 MOPS HTML 為主
        today = date.today()
        # 從上個月開始往回找，避免當月尚未出表
        if today.month == 1:
            cand_year = today.year - 1
            cand_month = 12
        else:
            cand_year = today.year
            cand_month = today.month - 1

        for _ in range(24):  # 最多往回找 24 個月份
            records, summary = self.fetch_monthly_revenue_html(cand_year, cand_month, market=market)
            if records:
                return records, summary

            # 無資料則往前一個月
            if cand_month == 1:
                cand_year -= 1
                cand_month = 12
            else:
                cand_month -= 1

        # 若 24 個月份內皆無資料，回傳空結果與基本摘要資訊
        return [], {
            'year': None,
            'month': None,
            'roc_yyyymm': None,
            'markets': sorted(list(markets)),
            'total_records': 0,
            'per_market': {'TWSE': 0, 'TPEX': 0},
        }

    def fetch_twse_monthly_revenue_html(self, year: int, month: int) -> list[dict]:
        """改用 MOPS HTML 報表抓取 TWSE 上市公司指定年月的月營收（歷史用）。

        來源：mops.twse.com.tw t21/sii/t21sc03_民國年_月(_0).html
        僅回傳單一年月資料，欄位對齊現有 JSON 版本的欄位命名。
        """
        try:
            y = int(year)
            m = int(month)
            if m < 1 or m > 12:
                raise ValueError
        except Exception:
            raise ValueError("year / month 參數格式錯誤，需為有效西元年與月份")

        roc_year = y - 1911 if y > 1990 else y
        if roc_year <= 0:
            raise ValueError("year 轉民國後需大於 0")

        # IFRS 之後網址結尾帶有 _0，之前則無
        if roc_year <= 98:
            url = f"https://mopsov.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{m}.html"
        else:
            url = f"https://mopsov.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{m}_0.html"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'
        }

        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.encoding = 'big5'
        except Exception as exc:
            logger.warning(f"TWSE 月營收 HTML 抓取失敗: {exc}")
            return []

        try:
            # 解析所有 table，挑出欄位數合理者（排除說明文字）
            dfs = pd.read_html(io.StringIO(resp.text))
        except Exception as exc:
            logger.warning(f"TWSE 月營收 HTML 解析失敗: {exc}")
            return []

        tables = [df for df in dfs if 5 < df.shape[1] <= 15]
        if not tables:
            logger.info("TWSE 月營收 HTML 無符合欄位數的資料表")
            return []

        df = pd.concat(tables, ignore_index=True)

        def _norm_cell(v):
            try:
                s = '' if v is None else str(v)
            except Exception:
                s = ''
            return s.replace('\u3000', ' ').strip()

        def _compact_ws(s: str) -> str:
            return ''.join(str(s).split())

        def _norm_columns(frame: pd.DataFrame) -> list[str]:
            cols: list[str] = []
            for c in list(frame.columns):
                if isinstance(c, tuple):
                    parts = [_norm_cell(p) for p in c if _norm_cell(p) and _norm_cell(p).lower() != 'nan']
                    cols.append(''.join(parts).strip())
                else:
                    cols.append(_norm_cell(c))
            return cols

        def _find_col(cols: list[str], keywords: list[str]) -> str | None:
            cols_norm = [_compact_ws(_norm_cell(c)) for c in cols]
            keywords_norm = [_compact_ws(_norm_cell(k)) for k in keywords]

            for idx, c_norm in enumerate(cols_norm):
                for k_norm in keywords_norm:
                    if not k_norm:
                        continue
                    if k_norm == c_norm or k_norm in c_norm:
                        return cols[idx]
            return None

        df.columns = _norm_columns(df)
        code_col = _find_col(list(df.columns), ['公司代號'])
        month_rev_col = _find_col(list(df.columns), ['當月營收'])
        mom_col = _find_col(list(df.columns), ['上月比較增減(%)', '上月比較增減'])
        yoy_col = _find_col(list(df.columns), ['去年同月增減(%)', '去年同月增減'])
        if code_col is None or month_rev_col is None:
            try:
                probe = df.apply(lambda col: col.map(_norm_cell))
                header_mask = probe.apply(
                    lambda r: r.astype(str).map(_compact_ws).str.contains(_compact_ws('公司代號'), na=False).any(),
                    axis=1,
                )
                header_idx_list = probe.index[header_mask].tolist()
            except Exception:
                header_idx_list = []

            if not header_idx_list:
                try:
                    sample_first_col = df.iloc[:15, 0].astype(str).map(_norm_cell).tolist()
                except Exception:
                    sample_first_col = []
                logger.info(
                    "TWSE 月營收 HTML 找不到 '公司代號' 標題列"
                    f"; columns={list(df.columns)[:20]}"
                    f"; first_col_sample={sample_first_col}"
                )
                return []

            header_idx = header_idx_list[0]
            df.columns = [_norm_cell(x) for x in df.iloc[header_idx].tolist()]
            df = df[header_idx + 1:]
            df.columns = _norm_columns(df)
            code_col = _find_col(list(df.columns), ['公司代號'])
            month_rev_col = _find_col(list(df.columns), ['當月營收'])
            mom_col = _find_col(list(df.columns), ['上月比較增減(%)', '上月比較增減'])
            yoy_col = _find_col(list(df.columns), ['去年同月增減(%)', '去年同月增減'])

        if code_col is None or month_rev_col is None:
            logger.info(f"TWSE 月營收 HTML 欄位名稱不符合預期; columns={list(df.columns)[:30]}")
            return []

        rename_map: dict[str, str] = {}
        if code_col != '公司代號':
            rename_map[code_col] = '公司代號'
        if month_rev_col != '當月營收':
            rename_map[month_rev_col] = '當月營收'
        if mom_col is not None and mom_col != '上月比較增減(%)':
            rename_map[mom_col] = '上月比較增減(%)'
        if yoy_col is not None and yoy_col != '去年同月增減(%)':
            rename_map[yoy_col] = '去年同月增減(%)'
        if rename_map:
            df = df.rename(columns=rename_map)

        # 清理與過濾資料列
        df = df[df['公司代號'].notna()]
        df['公司代號'] = df['公司代號'].astype(str).str.strip()
        df = df[df['公司代號'] != '合計']

        try:
            revenue_month = date(y, m, 1).isoformat()
        except Exception:
            raise ValueError("year/month 無法轉成日期")

        results: list[dict] = []
        for _, row in df.iterrows():
            stock_no = str(row.get('公司代號') or '').strip()
            if not stock_no:
                continue

            results.append({
                'revenue_month': revenue_month,
                'market': 'TWSE',
                'stock_no': stock_no,
                'stock_name': row.get('公司名稱'),
                'industry': row.get('產業別'),
                'report_date': None,
                'month_revenue': self._t86_parse_int(row.get('當月營收')),
                'last_month_revenue': self._t86_parse_int(row.get('上月營收')),
                'last_year_month_revenue': self._t86_parse_int(row.get('去年當月營收')),
                'mom_change_pct': self._parse_decimal(row.get('上月比較增減(%)')),
                'yoy_change_pct': self._parse_decimal(row.get('去年同月增減(%)')),
                'acc_revenue': self._t86_parse_int(row.get('當月累計營收') or row.get('本年累計營收')),
                'last_year_acc_revenue': self._t86_parse_int(row.get('去年累計營收')),
                'acc_change_pct': self._parse_decimal(row.get('前期比較增減(%)')),
                'note': row.get('備註'),
            })

        logger.info(f"TWSE 月營收 HTML {roc_year:03d}{m:02d} 抓取 {len(results)} 筆")
        return results

    def fetch_tpex_monthly_revenue_html(self, year: int, month: int) -> list[dict]:
        """改用 MOPS HTML 報表抓取 TPEX 上櫃公司指定年月的月營收（歷史用）。

        來源：mops.twse.com.tw t21/otc/t21sc03_民國年_月(_0).html
        """
        try:
            y = int(year)
            m = int(month)
            if m < 1 or m > 12:
                raise ValueError
        except Exception:
            raise ValueError("year / month 參數格式錯誤，需為有效西元年與月份")

        roc_year = y - 1911 if y > 1990 else y
        if roc_year <= 0:
            raise ValueError("year 轉民國後需大於 0")

        if roc_year <= 98:
            url = f"https://mopsov.twse.com.tw/nas/t21/otc/t21sc03_{roc_year}_{m}.html"
        else:
            url = f"https://mopsov.twse.com.tw/nas/t21/otc/t21sc03_{roc_year}_{m}_0.html"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'
        }

        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.encoding = 'big5'
        except Exception as exc:
            logger.warning(f"TPEX 月營收 HTML 抓取失敗: {exc}")
            return []

        try:
            dfs = pd.read_html(io.StringIO(resp.text))
        except Exception as exc:
            logger.warning(f"TPEX 月營收 HTML 解析失敗: {exc}")
            return []

        tables = [df for df in dfs if 5 < df.shape[1] <= 15]
        if not tables:
            logger.info("TPEX 月營收 HTML 無符合欄位數的資料表")
            return []

        df = pd.concat(tables, ignore_index=True)

        def _norm_cell(v):
            try:
                s = '' if v is None else str(v)
            except Exception:
                s = ''
            return s.replace('\u3000', ' ').strip()

        def _compact_ws(s: str) -> str:
            return ''.join(str(s).split())

        def _norm_columns(frame: pd.DataFrame) -> list[str]:
            cols: list[str] = []
            for c in list(frame.columns):
                if isinstance(c, tuple):
                    parts = [_norm_cell(p) for p in c if _norm_cell(p) and _norm_cell(p).lower() != 'nan']
                    cols.append(''.join(parts).strip())
                else:
                    cols.append(_norm_cell(c))
            return cols

        def _find_col(cols: list[str], keywords: list[str]) -> str | None:
            cols_norm = [_compact_ws(_norm_cell(c)) for c in cols]
            keywords_norm = [_compact_ws(_norm_cell(k)) for k in keywords]

            for idx, c_norm in enumerate(cols_norm):
                for k_norm in keywords_norm:
                    if not k_norm:
                        continue
                    if k_norm == c_norm or k_norm in c_norm:
                        return cols[idx]
            return None

        df.columns = _norm_columns(df)
        code_col = _find_col(list(df.columns), ['公司代號'])
        month_rev_col = _find_col(list(df.columns), ['當月營收'])
        mom_col = _find_col(list(df.columns), ['上月比較增減(%)', '上月比較增減'])
        yoy_col = _find_col(list(df.columns), ['去年同月增減(%)', '去年同月增減'])
        if code_col is None or month_rev_col is None:
            try:
                probe = df.apply(lambda col: col.map(_norm_cell))
                header_mask = probe.apply(
                    lambda r: r.astype(str).map(_compact_ws).str.contains(_compact_ws('公司代號'), na=False).any(),
                    axis=1,
                )
                header_idx_list = probe.index[header_mask].tolist()
            except Exception:
                header_idx_list = []

            if not header_idx_list:
                try:
                    sample_first_col = df.iloc[:15, 0].astype(str).map(_norm_cell).tolist()
                except Exception:
                    sample_first_col = []
                logger.info(
                    "TPEX 月營收 HTML 找不到 '公司代號' 標題列"
                    f"; columns={list(df.columns)[:20]}"
                    f"; first_col_sample={sample_first_col}"
                )
                return []

            header_idx = header_idx_list[0]
            df.columns = [_norm_cell(x) for x in df.iloc[header_idx].tolist()]
            df = df[header_idx + 1:]
            df.columns = _norm_columns(df)
            code_col = _find_col(list(df.columns), ['公司代號'])
            month_rev_col = _find_col(list(df.columns), ['當月營收'])
            mom_col = _find_col(list(df.columns), ['上月比較增減(%)', '上月比較增減'])
            yoy_col = _find_col(list(df.columns), ['去年同月增減(%)', '去年同月增減'])

        if code_col is None or month_rev_col is None:
            logger.info(f"TPEX 月營收 HTML 欄位名稱不符合預期; columns={list(df.columns)[:30]}")
            return []

        rename_map: dict[str, str] = {}
        if code_col != '公司代號':
            rename_map[code_col] = '公司代號'
        if month_rev_col != '當月營收':
            rename_map[month_rev_col] = '當月營收'
        if mom_col is not None and mom_col != '上月比較增減(%)':
            rename_map[mom_col] = '上月比較增減(%)'
        if yoy_col is not None and yoy_col != '去年同月增減(%)':
            rename_map[yoy_col] = '去年同月增減(%)'
        if rename_map:
            df = df.rename(columns=rename_map)

        df = df[df['公司代號'].notna()]
        df['公司代號'] = df['公司代號'].astype(str).str.strip()
        df = df[df['公司代號'] != '合計']

        try:
            revenue_month = date(y, m, 1).isoformat()
        except Exception:
            raise ValueError("year/month 無法轉成日期")

        results: list[dict] = []
        for _, row in df.iterrows():
            stock_no = str(row.get('公司代號') or '').strip()
            if not stock_no:
                continue

            results.append({
                'revenue_month': revenue_month,
                'market': 'TPEX',
                'stock_no': stock_no,
                'stock_name': row.get('公司名稱'),
                'industry': row.get('產業別'),
                'report_date': None,
                'month_revenue': self._t86_parse_int(row.get('當月營收')),
                'last_month_revenue': self._t86_parse_int(row.get('上月營收')),
                'last_year_month_revenue': self._t86_parse_int(row.get('去年當月營收')),
                'mom_change_pct': self._parse_decimal(row.get('上月比較增減(%)')),
                'yoy_change_pct': self._parse_decimal(row.get('去年同月增減(%)')),
                'acc_revenue': self._t86_parse_int(row.get('當月累計營收') or row.get('本年累計營收')),
                'last_year_acc_revenue': self._t86_parse_int(row.get('去年累計營收')),
                'acc_change_pct': self._parse_decimal(row.get('前期比較增減(%)')),
                'note': row.get('備註'),
            })

        logger.info(f"TPEX 月營收 HTML {roc_year:03d}{m:02d} 抓取 {len(results)} 筆")
        return results

    def fetch_monthly_revenue_html(self, year: int, month: int, market: str = 'both'):
        """使用 HTML 報表抓取指定年月的上市/上櫃月營收，用於歷史批次抓取。

        這不影響現有 JSON 版 fetch_monthly_revenue，僅供 /api/revenue/fetch_range 使用。
        """
        market_key = (market or 'both').lower()
        if market_key not in {'twse', 'tpex', 'both'}:
            raise ValueError("market 必須為 'twse'、'tpex' 或 'both'")

        markets = {'twse', 'tpex'} if market_key == 'both' else {market_key}

        results: list[dict] = []
        per_market = {'TWSE': 0, 'TPEX': 0}

        if 'twse' in markets:
            try:
                twse_records = self.fetch_twse_monthly_revenue_html(year, month)
            except Exception as exc:
                logger.warning(f"TWSE 月營收 HTML 抓取 {year}-{month:02d} 失敗: {exc}")
                twse_records = []
            results.extend(twse_records)
            per_market['TWSE'] = len(twse_records)

        if 'tpex' in markets:
            try:
                tpex_records = self.fetch_tpex_monthly_revenue_html(year, month)
            except Exception as exc:
                logger.warning(f"TPEX 月營收 HTML 抓取 {year}-{month:02d} 失敗: {exc}")
                tpex_records = []
            results.extend(tpex_records)
            per_market['TPEX'] = len(tpex_records)

        roc_year = int(year) - 1911
        roc_yyyymm = f"{roc_year:03d}{int(month):02d}"

        summary = {
            'year': int(year),
            'month': int(month),
            'roc_yyyymm': roc_yyyymm,
            'markets': sorted(list(markets)),
            'total_records': len(results),
            'per_market': per_market,
        }

        return results, summary

    def import_mops_csv_monthly_revenue(
        self,
        download_dir: str | None = None,
        db_manager: DatabaseManager | None = None,
    ) -> dict:
        """從本機 MOPS 月營收 CSV 檔 (t21sc03_*.csv) 匯入 monthly_revenue_table。"""
        if download_dir is None or not str(download_dir).strip():
            home = os.path.expanduser("~")
            download_dir = os.path.join(home, "Downloads", "mops_csv")

        download_dir = os.path.abspath(download_dir)

        if not os.path.isdir(download_dir):
            raise ValueError(f"MOPS 月營收目錄不存在: {download_dir}")

        files = [f for f in os.listdir(download_dir) if f.startswith("t21sc03_") and f.endswith(".csv")]
        files.sort()

        if not files:
            logger.info("import_mops_csv_monthly_revenue: 目錄中沒有任何 t21sc03_*.csv 檔案")
            return {
                'download_dir': download_dir,
                'files': 0,
                'total_rows': 0,
                'inserted_rows': 0,
            }

        def _parse_mops_roc_ym(s: str) -> date | None:
            """將 'YYY/M' 或 'YYY/MM' 轉為西元年月第一天。"""
            if not s:
                return None
            parts = str(s).strip().split('/')
            if len(parts) < 2:
                return None
            try:
                roc_year = int(parts[0])
                month = int(parts[1])
                if month < 1 or month > 12:
                    return None
                year = roc_year + 1911
                return date(year, month, 1)
            except Exception:
                return None

        def _parse_mops_roc_ymd(s: str) -> date | None:
            """將 'YYY/MM/DD' 轉為西元日期。"""
            if not s:
                return None
            parts = str(s).strip().split('/')
            if len(parts) != 3:
                return None
            try:
                roc_year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                year = roc_year + 1911
                return date(year, month, day)
            except Exception:
                return None

        all_records: list[dict] = []
        total_rows = 0

        for fname in files:
            path = os.path.join(download_dir, fname)
            market = 'TPEX' if 'otc' in fname.lower() else 'TWSE'
            try:
                with open(path, 'r', encoding='utf-8-sig', newline='') as f:
                    reader = csv.reader(f)
                    header = next(reader, None)
                    file_rows = 0

                    for row in reader:
                        # 期待至少 14 個欄位（index 0~13）
                        if not row or len(row) < 14:
                            continue

                        report_date_str = row[0]
                        data_ym_str = row[1]
                        stock_no = (row[2] or '').strip().strip('"')
                        stock_name = (row[3] or '').strip().strip('"')
                        industry = (row[4] or '').strip().strip('"')

                        if not stock_no or stock_no == '合計':
                            continue

                        revenue_month = _parse_mops_roc_ym(data_ym_str)
                        if revenue_month is None:
                            continue

                        report_date = _parse_mops_roc_ymd(report_date_str)

                        record = {
                            'revenue_month': revenue_month.isoformat(),
                            'market': market,
                            'stock_no': stock_no,
                            'stock_name': stock_name,
                            'industry': industry,
                            'report_date': report_date.isoformat() if report_date else None,
                            'month_revenue': self._t86_parse_int(row[5]) if len(row) > 5 else None,
                            'last_month_revenue': self._t86_parse_int(row[6]) if len(row) > 6 else None,
                            'last_year_month_revenue': self._t86_parse_int(row[7]) if len(row) > 7 else None,
                            'mom_change_pct': self._parse_decimal(row[8]) if len(row) > 8 else None,
                            'yoy_change_pct': self._parse_decimal(row[9]) if len(row) > 9 else None,
                            'acc_revenue': self._t86_parse_int(row[10]) if len(row) > 10 else None,
                            'last_year_acc_revenue': self._t86_parse_int(row[11]) if len(row) > 11 else None,
                            'acc_change_pct': self._parse_decimal(row[12]) if len(row) > 12 else None,
                            'note': row[13] if len(row) > 13 else None,
                        }

                        all_records.append(record)
                        file_rows += 1
                        total_rows += 1

                    logger.info("MOPS CSV %s 解析 %d 筆原始列", fname, file_rows)
            except Exception as exc:
                logger.warning("解析 MOPS CSV %s 失敗: %s", fname, exc)
                continue

        if not all_records:
            logger.info("import_mops_csv_monthly_revenue: 所有 CSV 無可匯入資料")
            return {
                'download_dir': download_dir,
                'files': len(files),
                'total_rows': 0,
                'inserted_rows': 0,
            }

        inserted = self.upsert_monthly_revenue(all_records, db_manager=db_manager)

        return {
            'download_dir': download_dir,
            'files': len(files),
            'total_rows': total_rows,
            'inserted_rows': inserted,
        }

    def download_mops_monthly_revenue_csv(
        self,
        start_year_tw: int,
        end_year_tw: int,
        download_dir: str | None = None,
        delay_between: float = 2.0,
        max_retries: int = 3,
        market: str = 'both',
    ) -> dict:
        home_dir = os.path.expanduser("~")
        if not download_dir:
            download_dir = os.environ.get('MOPS_DOWNLOAD_DIR') or os.path.join(home_dir, "Downloads", "mops_csv")
        download_dir = os.path.abspath(download_dir)
        os.makedirs(download_dir, exist_ok=True)

        try:
            start_year_tw = int(start_year_tw)
            end_year_tw = int(end_year_tw)
        except Exception:
            raise ValueError("start_year_tw / end_year_tw 需為民國年整數")
        if end_year_tw < start_year_tw:
            raise ValueError("end_year_tw 不可小於 start_year_tw")

        market_key = (market or 'both').lower()
        if market_key not in {'twse', 'tpex', 'both'}:
            raise ValueError("market 必須為 'twse'、'tpex' 或 'both'")
        market_set: set[str] = {'twse', 'tpex'} if market_key == 'both' else {market_key}

        tasks: list[tuple[str, int, int]] = []
        for y_tw in range(start_year_tw, end_year_tw + 1):
            for m in range(1, 13):
                for mk in market_set:
                    tasks.append((mk, y_tw, m))

        chrome_options = Options()
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "safebrowsing.disable_download_protection": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        if os.environ.get('CLOUD_DEPLOYMENT'):
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        driver = webdriver.Chrome(options=chrome_options)
        driver.set_window_size(1200, 800)
        wait = WebDriverWait(driver, 10)

        def _download_one(market_key: str, year_tw: int, month: int, retry: int) -> bool:
            subdir = "sii" if market_key == "twse" else "otc"
            url = f"https://mopsov.twse.com.tw/nas/t21/{subdir}/t21sc03_{year_tw}_{month}_0.html"
            suffix = "sii" if market_key == "twse" else "otc"
            filename = f"t21sc03_{suffix}_{year_tw}_{month}_0.csv"
            filepath = os.path.join(download_dir, filename)
            if os.path.exists(filepath):
                logger.info(f"已存在，跳過：{filename}")
                return True

            market_label = 'TWSE' if market_key == 'twse' else 'TPEX'
            logger.info(f"[{retry + 1}/{max_retries}] 正在下載：{market_label} {year_tw}年 第{month}月 → {filename}")
            try:
                driver.get(url)
                time.sleep(3)
                button = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//input[@value='另存CSV']"))
                )
                button.click()

                timeout = 30
                start_ts = time.time()
                while time.time() - start_ts < timeout:
                    crdownloads = [
                        f for f in os.listdir(download_dir) if f.endswith(".crdownload")
                    ]
                    csv_files = [
                        f
                        for f in os.listdir(download_dir)
                        if f.endswith(".csv") and not f.endswith(".crdownload")
                    ]
                    if csv_files and not crdownloads:
                        latest_file = max(
                            [os.path.join(download_dir, f) for f in csv_files],
                            key=os.path.getctime,
                        )
                        os.rename(latest_file, filepath)
                        logger.info(f"下載成功：{filename}")
                        return True
                    time.sleep(1)
                logger.warning(f"下載逾時：{filename}")
                return False
            except Exception as exc:
                logger.warning(f"下載失敗（{year_tw}年{month}月）：{exc}")
                return False
            finally:
                time.sleep(delay_between)

        success_count = 0
        failed: list[dict] = []
        try:
            total = len(tasks)
            for idx, (mk, year_tw, month) in enumerate(tasks, 1):
                logger.info(f"進度：{idx}/{total} {mk.upper()} {year_tw}年 第{month}月")
                downloaded = False
                for retry in range(max_retries):
                    if _download_one(mk, year_tw, month, retry):
                        success_count += 1
                        downloaded = True
                        break
                if not downloaded:
                    failed.append({"market": mk.upper(), "year_tw": year_tw, "month": month})
        finally:
            try:
                driver.quit()
            except Exception:
                pass

        return {
            "download_dir": download_dir,
            "markets": sorted({mk.upper() for mk in market_set}),
            "total_tasks": len(tasks),
            "success_count": success_count,
            "failed_count": len(failed),
            "failed_tasks": failed,
        }

    def upsert_monthly_revenue(self, records: list[dict], db_manager: DatabaseManager | None = None) -> int:
        """將月營收資料寫入 monthly revenue 資料表。"""
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager()
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_revenue', 'tw_stock_monthly_revenue')

            def to_int(value):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return 0

            values = []
            seen_keys: set[tuple[date, str, str]] = set()
            for rec in records:
                rm_str = rec.get('revenue_month')
                stock_no = (rec.get('stock_no') or '').strip()
                if not rm_str or not stock_no:
                    continue
                try:
                    rm_date = datetime.strptime(rm_str, '%Y-%m-%d').date()
                except Exception:
                    continue

                market = (rec.get('market') or '').strip().upper() or 'TWSE'
                stock_name = rec.get('stock_name')
                industry = rec.get('industry')

                report_date = None
                rd_str = rec.get('report_date')
                if rd_str:
                    try:
                        report_date = datetime.strptime(rd_str, '%Y-%m-%d').date()
                    except Exception:
                        report_date = None

                key = (rm_date, market, stock_no)
                if key in seen_keys:
                    # 同一批次中已經有相同 PK 的資料，避免在單一 INSERT 中觸發
                    # "ON CONFLICT DO UPDATE command cannot affect row a second time" 錯誤
                    continue
                seen_keys.add(key)

                values.append((
                    rm_date,
                    market,
                    stock_no,
                    stock_name,
                    industry,
                    report_date,
                    to_int(rec.get('month_revenue')),
                    to_int(rec.get('last_month_revenue')),
                    to_int(rec.get('last_year_month_revenue')),
                    rec.get('mom_change_pct'),
                    rec.get('yoy_change_pct'),
                    to_int(rec.get('acc_revenue')),
                    to_int(rec.get('last_year_acc_revenue')),
                    rec.get('acc_change_pct'),
                    rec.get('note'),
                ))

            if not values:
                return 0

            insert_sql = f"""
                INSERT INTO {table_name} (
                    revenue_month, market, stock_no, stock_name, industry, report_date,
                    month_revenue, last_month_revenue, last_year_month_revenue,
                    mom_change_pct, yoy_change_pct,
                    acc_revenue, last_year_acc_revenue, acc_change_pct, note
                )
                VALUES %s
                ON CONFLICT (revenue_month, market, stock_no) DO UPDATE SET
                    stock_name = EXCLUDED.stock_name,
                    industry = EXCLUDED.industry,
                    report_date = EXCLUDED.report_date,
                    month_revenue = EXCLUDED.month_revenue,
                    last_month_revenue = EXCLUDED.last_month_revenue,
                    last_year_month_revenue = EXCLUDED.last_year_month_revenue,
                    mom_change_pct = EXCLUDED.mom_change_pct,
                    yoy_change_pct = EXCLUDED.yoy_change_pct,
                    acc_revenue = EXCLUDED.acc_revenue,
                    last_year_acc_revenue = EXCLUDED.last_year_acc_revenue,
                    acc_change_pct = EXCLUDED.acc_change_pct,
                    note = EXCLUDED.note,
                    updated_at = CURRENT_TIMESTAMP
            """

            execute_values(cursor, insert_sql, values, page_size=1000)
            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def fetch_twse_stock_data_batch(self, stock_codes, start_date, end_date):
        """批量抓取多支股票的歷史數據（使用批量 API + 多線程）"""
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            # 生成所有需要抓取的日期（排除週末）
            dates_to_fetch = []
            current = start_dt
            while current <= end_dt:
                if current.weekday() < 5:  # 週一到週五
                    dates_to_fetch.append(current)
                current += timedelta(days=1)

            logger.info(f"批量抓取模式：{len(stock_codes)} 檔股票，{len(dates_to_fetch)} 個交易日")

            # 使用多線程並行抓取每一天的數據
            all_data = {}  # {stock_code: [records]}
            processed_days = 0
            total_days = len(dates_to_fetch)

            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_date = {executor.submit(self.fetch_twse_all_stocks_day, date): date
                                  for date in dates_to_fetch}

                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        day_data = future.result()

                        # 將當天數據分配到各股票
                        for stock_code in stock_codes:
                            if stock_code in day_data:
                                all_data.setdefault(stock_code, []).append(day_data[stock_code])
                    except Exception as e:
                        logger.error(f"抓取 {date.strftime('%Y-%m-%d')} 失敗: {e}")

                    processed_days += 1
                    if total_days and (processed_days % 50 == 0 or processed_days == total_days):
                        pct = processed_days * 100.0 / total_days
                        logger.info(f"TWSE 批量抓取進度: {processed_days}/{total_days} ({pct:.1f}%)")

                    # 避免請求過於頻繁
                    time.sleep(0.3)

            # 排序每支股票的數據
            for stock_code in all_data:
                all_data[stock_code].sort(key=lambda x: x['Date'])

            logger.info(f"批量抓取完成，成功抓取 {len(all_data)} 檔股票")
            return all_data

        except Exception as e:
            logger.error(f"批量抓取失敗: {e}")
            return {}

    def upsert_bwibbu_records(self, records, db_manager: DatabaseManager | None = None):
        """將 BWIBBU 指標資料寫入資料庫。"""
        if not records:
            return 0

        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager()
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()

            values = []
            for rec in records:
                code = rec.get('Code') or rec.get('code')
                date_str = rec.get('Date') or rec.get('date')
                if not code or not date_str:
                    continue

                try:
                    if '-' in date_str:
                        record_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif len(date_str) == 7:  # ROC yyyMMdd
                        roc_year = int(date_str[:3])
                        year = roc_year + 1911
                        month = int(date_str[3:5])
                        day = int(date_str[5:7])
                        record_date = date(year, month, day)
                    elif len(date_str) == 8:  # YYYYMMDD
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                        record_date = date(year, month, day)
                    else:
                        continue
                except Exception:
                    continue

                try:
                    pe_ratio = float(rec.get('PEratio')) if rec.get('PEratio') not in (None, '', '-') else None
                except ValueError:
                    pe_ratio = None
                try:
                    dividend_yield = float(rec.get('DividendYield')) if rec.get('DividendYield') not in (None, '', '-') else None
                except ValueError:
                    dividend_yield = None
                try:
                    pb_ratio = float(rec.get('PBratio')) if rec.get('PBratio') not in (None, '', '-') else None
                except ValueError:
                    pb_ratio = None

                values.append((
                    code,
                    record_date,
                    rec.get('Name'),
                    pe_ratio,
                    dividend_yield,
                    pb_ratio
                ))

            if not values:
                return 0

            execute_values(
                cursor,
                """
                INSERT INTO tw_stock_bwibbu (code, date, name, pe_ratio, dividend_yield, pb_ratio)
                VALUES %s
                ON CONFLICT (code, date) DO UPDATE SET
                    name = EXCLUDED.name,
                    pe_ratio = EXCLUDED.pe_ratio,
                    dividend_yield = EXCLUDED.dividend_yield,
                    pb_ratio = EXCLUDED.pb_ratio,
                    updated_at = CURRENT_TIMESTAMP
                """,
                values,
                page_size=500
            )

            db.connection.commit()
            return len(values)
        finally:
            if own_manager:
                db.disconnect()

    def get_latest_bwibbu_date(self, db_manager: DatabaseManager | None = None):
        """抓取資料庫中最新一批 BWIBBU 日期。"""
        own_manager = False
        db = db_manager
        if db is None:
            db = DatabaseManager()
            own_manager = True

        if not db.connect():
            raise RuntimeError("資料庫連線失敗")

        try:
            db.create_tables()
            cursor = db.connection.cursor()
            cursor.execute("SELECT MAX(date) FROM tw_stock_bwibbu")
            result = cursor.fetchone()
            cursor.close()
            if not result:
                return None
            value = result[0] if not isinstance(result, dict) else result.get('max') or result.get('max(date)')
            return value
        finally:
            if own_manager:
                db.disconnect()
    
    def fetch_twse_stock_data(self, stock_code, start_date, end_date):
        """從台灣證交所 API 獲取股票數據"""
        try:
            # 將日期轉換為證交所 API 格式
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            result = []
            current_date = start_dt
            
            # 逐月獲取數據（證交所 API 限制）
            while current_date <= end_dt:
                year = current_date.year
                month = current_date.month
                
                # 證交所 API URL
                url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                params = {
                    'response': 'json',
                    'date': f'{year}{month:02d}01',
                    'stockNo': stock_code
                }
                
                logger.info(f"獲取 {stock_code} {year}-{month:02d} 數據")
                
                # 添加重試機制
                max_retries = 3
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36'
                        }
                        response = requests.get(url, params=params, headers=headers, timeout=15, verify=False)
                        if response.status_code == 200:
                            success = True
                        elif response.status_code == 500:
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"HTTP 500 錯誤，第 {retry_count} 次重試 {stock_code} {year}-{month:02d}")
                                time.sleep(2)  # 等待2秒後重試
                                continue
                            else:
                                logger.error(f"HTTP 500 錯誤，已達最大重試次數，跳過 {stock_code} {year}-{month:02d}")
                                break
                        else:
                            logger.error(f"HTTP {response.status_code} 錯誤，跳過 {stock_code} {year}-{month:02d}")
                            break
                    except requests.exceptions.Timeout:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"請求超時，第 {retry_count} 次重試 {stock_code} {year}-{month:02d}")
                            time.sleep(3)
                            continue
                        else:
                            logger.error(f"請求超時，已達最大重試次數，跳過 {stock_code} {year}-{month:02d}")
                            break
                    except Exception as e:
                        logger.error(f"請求異常: {e}，跳過 {stock_code} {year}-{month:02d}")
                        break
                
                if success and response.status_code == 200:
                    data = response.json()
                    if data.get('stat') != 'OK':
                        logger.warning(f"TWSE 回傳非 OK: stat={data.get('stat')} msg={data.get('msg')}")
                    
                    if data.get('stat') == 'OK' and data.get('data'):
                        for row in data['data']:
                            try:
                                # 解析日期 (民國年/月/日)
                                date_parts = row[0].split('/')
                                if len(date_parts) == 3:
                                    year_roc = int(date_parts[0]) + 1911  # 民國年轉西元年
                                    month_val = int(date_parts[1])
                                    day_val = int(date_parts[2])
                                    
                                    trade_date = datetime(year_roc, month_val, day_val)
                                    
                                    # 檢查是否在指定範圍內
                                    if start_dt <= trade_date <= end_dt:
                                        # 移除千分位逗號並轉換數值
                                        volume = int(row[1].replace(',', '')) if row[1] != '--' else 0
                                        open_price = float(row[3].replace(',', '')) if row[3] != '--' else None
                                        high_price = float(row[4].replace(',', '')) if row[4] != '--' else None
                                        low_price = float(row[5].replace(',', '')) if row[5] != '--' else None
                                        close_price = float(row[6].replace(',', '')) if row[6] != '--' else None
                                        
                                        # 驗證所有價格都小於30000
                                        if (close_price is not None and close_price > 0 and
                                            (open_price is None or open_price < 30000) and 
                                            (high_price is None or high_price < 30000) and 
                                            (low_price is None or low_price < 30000) and 
                                            close_price < 30000):
                                            result.append({
                                                'ticker': f"{stock_code}.TW",
                                                'Date': trade_date.strftime('%Y-%m-%d'),
                                                'Open': round(open_price, 2) if open_price is not None else None,
                                                'High': round(high_price, 2) if high_price is not None else None,
                                                'Low': round(low_price, 2) if low_price is not None else None,
                                                'Close': round(close_price, 2),
                                                'Volume': volume
                                            })
                                        else:
                                            logger.warning(f"價格超過30000，跳過 {trade_date.strftime('%Y-%m-%d')}: "
                                                          f"O:{open_price}, H:{high_price}, L:{low_price}, C:{close_price}")
                            except (ValueError, IndexError) as e:
                                logger.warning(f"解析數據行失敗: {row}, 錯誤: {e}")
                                continue
                
                # 移到下個月：強制設為次月的1號以避免例如 1/31 -> 2/31 造成 ValueError
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1, day=1)
                
                time.sleep(1.5)  # 增加延遲避免請求過於頻繁
            
            # 按日期排序
            result.sort(key=lambda x: x['Date'])
            return result
            
        except Exception as e:
            logger.error(f"從證交所獲取 {stock_code} 數據失敗: {e}")
            return None
    
    def is_otc_stock(self, stock_code):
        """判斷是否為上櫃股票"""
        try:
            # 先查詢我們的股票清單來確定市場
            if hasattr(self, 'symbols_cache') and self.symbols_cache:
                for stock in self.symbols_cache:
                    if stock['symbol'] == stock_code or stock['symbol'].startswith(stock_code + '.'):
                        return stock.get('market') == '上櫃'
            
            # 如果快取中找不到，使用已知的上櫃股票代碼範圍和特定股票
            code_num = int(stock_code)
            
            # 已知的上櫃股票代碼（部分範例）
            known_otc_stocks = {
                # 科技類
                '3443', '4966', '6488', '3034', '3702', '4904', '5269', '6415',
                # 其他產業
                '1565', '1569', '1580', '2596', '2633', '2719', '2724', '2729',
                '3131', '3149', '3163', '3167', '3169', '3171', '3176', '3178',
                '4102', '4106', '4108', '4116', '4119', '4126', '4128', '4129',
                '5203', '5222', '5234', '5243', '5245', '5251', '5263', '5264',
                '6104', '6116', '6120', '6121', '6122', '6126', '6128', '6129',
                '7556', '7557', '7561', '7566', '7567', '7568', '7569', '7570',
                '8024', '8027', '8028', '8029', '8032', '8033', '8034', '8035',
                '9188', '9802', '9910', '9911', '9912', '9914', '9917', '9918'
            }
            
            if stock_code in known_otc_stocks:
                return True
            
            # 上櫃股票通常集中在某些代碼範圍
            # 1000-1999: 部分傳統產業（上櫃較多）
            # 2000-2999: 部分食品、服務業（上櫃較多）
            # 3000-3999: 部分電子股（上櫃較多）
            # 4000-4999: 部分紡織、電子股（上櫃較多）
            # 5000-5999: 部分電機股（上櫃較多）
            # 6000-6999: 部分電子、生技股（上櫃較多）
            # 7000-7999: 部分玻璃陶瓷、其他產業（上櫃較多）
            # 8000-8999: 部分其他產業（上櫃較多）
            # 9000-9999: 部分綜合、其他產業（上櫃較多）
            
            if (1500 <= code_num <= 1999 or 
                2500 <= code_num <= 2999 or 
                3000 <= code_num <= 3999 or 
                4000 <= code_num <= 4999 or 
                5200 <= code_num <= 5999 or 
                6100 <= code_num <= 6999 or 
                7500 <= code_num <= 7999 or 
                8000 <= code_num <= 8999 or 
                9100 <= code_num <= 9999):
                return True
            
            return False
        except:
            return False
    
    def fetch_tpex_stock_data(self, stock_code, start_date, end_date):
        """從櫃買中心傳統 API 獲取上櫃股票數據（提供正確的歷史數據）"""
        try:
            # 將日期轉換為datetime
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            use_parallel_threshold_days = int(os.getenv('TPEX_SINGLE_PARALLEL_THRESHOLD_DAYS', '60'))
            if (end_dt - start_dt).days + 1 >= use_parallel_threshold_days:
                batch_data = self.fetch_tpex_stock_data_batch([stock_code], start_date, end_date)
                return batch_data.get(stock_code, []) if isinstance(batch_data, dict) else []
            
            result = []
            current_date = start_dt
            
            sleep_s = float(os.getenv('TPEX_SINGLE_SLEEP', '0.05'))
            max_retries = int(os.getenv('TPEX_SINGLE_RETRIES', '2'))
            
            logger.info(f"使用櫃買中心 API 抓取 {stock_code}，日期範圍: {start_date} ~ {end_date}")
            
            # 計算總天數
            total_days = (end_dt - start_dt).days + 1
            processed_days = 0
            success_count = 0
            
            # 逐日獲取數據
            while current_date <= end_dt:
                # 跳過週末（TPEX 非交易日）
                if current_date.weekday() >= 5:  # 5=週六, 6=週日
                    current_date = current_date + timedelta(days=1)
                    processed_days += 1
                    continue

                retry_count = 0
                while retry_count <= max_retries:
                    try:
                        day_data = self.fetch_tpex_all_stocks_day(current_date)
                        if day_data and stock_code in day_data:
                            result.append(day_data[stock_code])
                            success_count += 1
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count > max_retries:
                            logger.warning(f"TPEX 抓取失敗: {stock_code} {current_date.strftime('%Y-%m-%d')}: {e}")
                            break
                        time.sleep(0.5)
                
                # 移動到下一天
                current_date = current_date + timedelta(days=1)
                processed_days += 1
                
                # 進度提示（每20天，減少日誌噪音）
                if processed_days % 20 == 0:
                    logger.info(f"櫃買中心 {stock_code} 進度: {processed_days}/{total_days} 天，成功 {success_count} 筆")
                
                if sleep_s and sleep_s > 0:
                    time.sleep(sleep_s)
            
            # 按日期排序（不需要去重，因為傳統API提供正確的歷史數據）
            if result:
                result.sort(key=lambda x: x['Date'])
                logger.info(f"✅ 成功從櫃買中心傳統API獲取 {stock_code} 數據，共 {len(result)} 筆")
            else:
                logger.warning(f"⚠️ 櫃買中心傳統API {stock_code} 在指定期間({start_date} ~ {end_date})沒有抓到任何資料")
            
            return result
            
        except Exception as e:
            logger.error(f"從櫃買中心傳統API獲取 {stock_code} 數據失敗: {e}")
            import traceback
            traceback.print_exc()
            return None
 
    def fetch_twii_with_yfinance(self, start_date, end_date):
        """使用 yfinance 抓取台灣加權指數 (^TWII) 日K 資料。"""
        try:
            if not start_date:
                start_date = DEFAULT_START_DATE
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            start_ts = pd.to_datetime(start_date)
            end_ts = pd.to_datetime(end_date)

            logger.info(f"使用 yfinance 抓取 ^TWII，範圍: {start_ts.date()} ~ {end_ts.date()}")

            # 使用 Ticker.history 可取得單一層欄位：Open/High/Low/Low/Close/Volume
            ticker = yf.Ticker("^TWII")
            df = ticker.history(start=start_ts, end=end_ts + pd.Timedelta(days=1), interval="1d", auto_adjust=False)
            if df is None or df.empty:
                logger.warning("yfinance 取得 ^TWII 無資料")
                return None

            # 以台北時區對齊日期：若有 tz 則轉為 Asia/Taipei 再去 tz；若無 tz 則假設為 UTC 再轉 Asia/Taipei
            try:
                if getattr(df.index, "tz", None) is not None:
                    df = df.tz_convert("Asia/Taipei").tz_localize(None)
                else:
                    df.index = df.index.tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None)
            except Exception:
                # 若轉換失敗，至少嘗試去除 tz，避免报错
                try:
                    df = df.tz_convert(None)
                except Exception:
                    df.index = df.index.tz_localize(None)

            records = []
            for idx, row in df.iterrows():
                try:
                    d = idx.date() if hasattr(idx, "date") else pd.to_datetime(idx).date()
                    date_str = d.strftime('%Y-%m-%d')
                    open_p = float(row["Open"]) if not pd.isna(row["Open"]) else None
                    high_p = float(row["High"]) if not pd.isna(row["High"]) else None
                    low_p = float(row["Low"]) if not pd.isna(row["Low"]) else None
                    close_p = float(row["Close"]) if not pd.isna(row["Close"]) else None
                    vol = int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
                except Exception as exc:
                    logger.debug(f"跳過無法解析的 ^TWII 列: {idx}, err={exc}")
                    continue

                if close_p is None:
                    continue

                records.append({
                    'ticker': '^TWII',
                    'Date': date_str,
                    'Open': round(open_p, 2) if open_p is not None else None,
                    'High': round(high_p, 2) if high_p is not None else None,
                    'Low': round(low_p, 2) if low_p is not None else None,
                    'Close': round(close_p, 2),
                    'Volume': vol,
                })

            records.sort(key=lambda x: x['Date'])
            logger.info(f"yfinance 成功取得 ^TWII {len(records)} 筆")
            return records or None
        except Exception as e:
            logger.error(f"使用 yfinance 抓取 ^TWII 失敗: {e}")
            return None

    def fetch_otc_with_yfinance(self, start_date, end_date):
        try:
            if not start_date:
                start_date = DEFAULT_START_DATE
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            start_ts = pd.to_datetime(start_date)
            end_ts = pd.to_datetime(end_date)

            candidates = ["^TWOII", "^TWO", "^OTC", "^TPEX", "OTC.TW"]
            records = []
            for sym in candidates:
                try:
                    ticker = yf.Ticker(sym)
                    df = ticker.history(start=start_ts, end=end_ts + pd.Timedelta(days=1), interval="1d", auto_adjust=False)
                    if df is None or df.empty:
                        continue

                    # 與加權指數相同：先轉成台北時區再移除 tz，確保日期對齊
                    try:
                        if getattr(df.index, "tz", None) is not None:
                            df = df.tz_convert("Asia/Taipei").tz_localize(None)
                        else:
                            df.index = df.index.tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None)
                    except Exception:
                        try:
                            df = df.tz_convert(None)
                        except Exception:
                            df.index = df.index.tz_localize(None)

                    tmp = []
                    for idx, row in df.iterrows():
                        try:
                            d = idx.date() if hasattr(idx, "date") else pd.to_datetime(idx).date()
                            date_str = d.strftime('%Y-%m-%d')
                            open_p = float(row.get("Open")) if not pd.isna(row.get("Open")) else None
                            high_p = float(row.get("High")) if not pd.isna(row.get("High")) else None
                            low_p = float(row.get("Low")) if not pd.isna(row.get("Low")) else None
                            close_p = float(row.get("Close")) if not pd.isna(row.get("Close")) else None
                            vol = int(row.get("Volume")) if not pd.isna(row.get("Volume")) else 0
                        except Exception:
                            continue
                        if close_p is None:
                            continue
                        tmp.append({
                            'ticker': '^OTC',
                            'Date': date_str,
                            'Open': round(open_p, 2) if open_p is not None else None,
                            'High': round(high_p, 2) if high_p is not None else None,
                            'Low': round(low_p, 2) if low_p is not None else None,
                            'Close': round(close_p, 2),
                            'Volume': vol,
                        })
                    if tmp:
                        tmp.sort(key=lambda x: x['Date'])
                        records = tmp
                        logger.info(f"yfinance 命中 OTC 代號 {sym}，取得 {len(tmp)} 筆")
                        break
                except Exception:
                    continue
            if not records:
                logger.warning("yfinance 未能取得任何 OTC 指數資料")
            return records or None
        except Exception:
            return None

    def fetch_twse_index_data(self, index_code, start_date, end_date):
        """從台灣證交所 API 獲取指數數據"""
        try:
            # 將日期轉換為證交所 API 格式
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            result = []
            current_date = start_dt
            
            # 逐月獲取指數數據（證交所FMTQIK API按月提供數據）
            while current_date <= end_dt:
                year = current_date.year
                month = current_date.month
                
                # 使用證交所市場成交資訊API (FMTQIK)
                url = "https://www.twse.com.tw/exchangeReport/FMTQIK"
                params = {
                    'response': 'json',
                    'date': f'{year}{month:02d}01'
                }
                
                logger.info(f"獲取加權指數 {year}-{month:02d} 數據")
                
                try:
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('stat') == 'OK' and data.get('data'):
                            # FMTQIK API 數據格式: ["日期","成交股數","成交金額","成交筆數","發行量加權股價指數","漲跌點數"]
                            for row in data['data']:
                                try:
                                    # 解析日期 (民國年/月/日)
                                    date_parts = row[0].split('/')
                                    if len(date_parts) == 3:
                                        year_roc = int(date_parts[0]) + 1911  # 民國年轉西元年
                                        month_val = int(date_parts[1])
                                        day_val = int(date_parts[2])
                                        
                                        trade_date = datetime(year_roc, month_val, day_val)
                                        
                                        # 檢查是否在指定範圍內
                                        if start_dt <= trade_date <= end_dt:
                                            # 發行量加權股價指數在第5個欄位 (index 4)
                                            # 數據格式: ["日期","成交股數","成交金額","成交筆數","發行量加權股價指數","漲跌點數"]
                                            if len(row) >= 5:
                                                index_value = None
                                                
                                                # 嘗試從第5欄位（index 4）獲取指數值
                                                if row[4] != '--' and row[4].strip():
                                                    try:
                                                        candidate_value = float(row[4].replace(',', ''))
                                                        # 檢查是否為合理的指數值（8000-30000之間）
                                                        if 8000 <= candidate_value <= 30000:
                                                            index_value = candidate_value
                                                        else:
                                                            logger.warning(f"第5欄位值異常: {candidate_value}，嘗試其他欄位")
                                                    except ValueError:
                                                        logger.warning(f"第5欄位無法解析: '{row[4]}'")
                                                
                                                # 如果第5欄位不合理，嘗試其他可能的欄位
                                                if index_value is None:
                                                    for col_idx in [5, 3, 2]:  # 嘗試第6、4、3欄位
                                                        if len(row) > col_idx and row[col_idx] != '--' and row[col_idx].strip():
                                                            try:
                                                                candidate_value = float(row[col_idx].replace(',', ''))
                                                                if 8000 <= candidate_value <= 30000:
                                                                    index_value = candidate_value
                                                                    logger.info(f"在第{col_idx+1}欄位找到合理指數值: {candidate_value}")
                                                                    break
                                                            except ValueError:
                                                                continue
                                                
                                                # 解析成交金額（千元），轉為元（×1000）
                                                turnover_value = 0
                                                try:
                                                    # 第3欄位（index 2）為成交金額（千元）
                                                    if len(row) > 2 and row[2] not in (None, '--', ''):
                                                        turnover_value = int(row[2].replace(',', '')) * 1000
                                                except Exception:
                                                    turnover_value = 0

                                                # 如果找到合理的指數值，再次驗證是否小於30000
                                                if index_value is not None and index_value < 30000:
                                                    result.append({
                                                        'ticker': '^TWII',
                                                        'Date': trade_date.strftime('%Y-%m-%d'),
                                                        'Open': round(index_value, 2),
                                                        'High': round(index_value, 2),
                                                        'Low': round(index_value, 2),
                                                        'Close': round(index_value, 2),
                                                        'Volume': turnover_value  # 以成交金額（元）入庫到 volume
                                                    })
                                                    logger.info(f"成功解析 {trade_date.strftime('%Y-%m-%d')} 加權指數: {index_value}")
                                                elif index_value is not None:
                                                    logger.warning(f"指數值超過30000，跳過 {trade_date.strftime('%Y-%m-%d')}: {index_value}")
                                                else:
                                                    logger.warning(f"無法找到合理的指數值，跳過 {trade_date.strftime('%Y-%m-%d')}")
                                                    logger.debug(f"完整數據行: {row}")
                                                    
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"解析指數數據行失敗: {row}, 錯誤: {e}")
                                    continue
                                
                except requests.RequestException as e:
                    logger.warning(f"請求 {year}-{month:02d} 數據失敗: {e}")
                
                # 移到下個月
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
                
                time.sleep(0.3)  # 避免請求過於頻繁
            
            # 按日期排序
            result.sort(key=lambda x: x['Date'])
            logger.info(f"成功獲取加權指數數據 {len(result)} 筆")
            return result if result else None
            
        except Exception as e:
            logger.error(f"從證交所獲取指數數據失敗: {e}")
            return None

    def fetch_tpex_index_data(self, start_date, end_date):
        """從櫃買中心 openapi 獲取櫃買指數 (^OTC) 數據"""
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')

            url = "https://www.tpex.org.tw/openapi/v1/tpex_index"
            logger.info(f"獲取櫃買指數 (^OTC) 數據，範圍 {start_date} ~ {end_date}")
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("櫃買指數 openapi 回應非列表，返回 None")
                return None

            results = []
            for row in data:
                try:
                    raw_date = str(row.get('Date'))
                    if len(raw_date) == 8:
                        trade_date = datetime.strptime(raw_date, '%Y%m%d')
                    else:
                        trade_date = datetime.strptime(raw_date, '%Y-%m-%d')
                    if not (start_dt <= trade_date <= end_dt):
                        continue
                    results.append({
                        'ticker': '^OTC',
                        'Date': trade_date.strftime('%Y-%m-%d'),
                        'Open': float(row.get('Open')) if row.get('Open') not in (None, '') else None,
                        'High': float(row.get('High')) if row.get('High') not in (None, '') else None,
                        'Low': float(row.get('Low')) if row.get('Low') not in (None, '') else None,
                        'Close': float(row.get('Close')) if row.get('Close') not in (None, '') else None,
                        # openapi 未提供成交量，填 0
                        'Volume': 0
                    })
                except Exception as exc:
                    logger.debug(f"跳過無法解析的櫃買指數列: {row}, err={exc}")
                    continue

            results.sort(key=lambda x: x['Date'])
            if results:
                logger.info(f"成功取得櫃買指數 {len(results)} 筆")
                return results
            # fallback to yfinance when openapi has no data for requested range
            yf_records = self.fetch_otc_with_yfinance(start_date, end_date)
            if yf_records:
                logger.info(f"使用 yfinance 備援取得 ^OTC {len(yf_records)} 筆")
                return yf_records
            return None
        except Exception as e:
            logger.error(f"從櫃買中心獲取櫃買指數失敗: {e}")
            return None

    def calculate_returns(self, price_data, frequency='daily'):
        """計算報酬率"""
        if price_data is None or (hasattr(price_data, 'empty') and price_data.empty) or (isinstance(price_data, list) and len(price_data) == 0):
            return []
        
        # Handle both DataFrame and list inputs
        if isinstance(price_data, pd.DataFrame):
            df = price_data.copy()
        else:
            df = pd.DataFrame(price_data)
        
        # Normalize column names to handle both formats
        date_col = 'date' if 'date' in df.columns else 'Date'
        close_col = 'close_price' if 'close_price' in df.columns else 'Close'
        
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
        
        if len(df) < 2:
            return []
        
        results = []
        
        if frequency == 'daily':
            df['return'] = df[close_col].pct_change()
            # 計算累積報酬率
            df['cumulative_return'] = (1 + df['return']).cumprod() - 1
            
            for _, row in df.iterrows():
                if pd.notna(row['return']):
                    results.append({
                        'ticker': row.get('ticker', '^TWII'),  # Default to ^TWII if no ticker
                        'Date': row[date_col].strftime('%Y-%m-%d'),
                        'frequency': 'daily',
                        'return': round(float(row['return']), 6),
                        'cumulative_return': round(float(row['cumulative_return']), 6) if pd.notna(row['cumulative_return']) else 0.0
                    })
        
        elif frequency == 'weekly':
            weekly = df.set_index(date_col)[close_col].resample('W').last().pct_change(fill_method=None).dropna()
            # 計算累積報酬率
            weekly_cumulative = (1 + weekly).cumprod() - 1
            for date, ret in weekly.items():
                results.append({
                    'ticker': df.get('ticker', '^TWII').iloc[0] if 'ticker' in df.columns else '^TWII',
                    'Date': date.strftime('%Y-%m-%d'),
                    'frequency': 'weekly',
                    'return': round(float(ret), 6),
                    'cumulative_return': round(float(weekly_cumulative[date]), 6) if date in weekly_cumulative.index else 0.0
                })
        
        elif frequency == 'monthly':
            # 使用 pandas 月末別名 'M'（跨版本最相容）。'ME' 可能在部分版本無效，會導致 "Invalid frequency: ME"。
            monthly = df.set_index(date_col)[close_col].resample('M').last().pct_change(fill_method=None).dropna()
            # 計算累積報酬率
            monthly_cumulative = (1 + monthly).cumprod() - 1
            for date, ret in monthly.items():
                results.append({
                    'ticker': df.get('ticker', '^TWII').iloc[0] if 'ticker' in df.columns else '^TWII',
                    'Date': date.strftime('%Y-%m-%d'),
                    'frequency': 'monthly',
                    'return': round(float(ret), 6),
                    'cumulative_return': round(float(monthly_cumulative[date]), 6) if date in monthly_cumulative.index else 0.0
                })
        
        return results



# 初始化 API 實例
stock_api = StockAPI()

# 註冊 BWIBBU Blueprint（統一於本服務下提供 /api/bwibbu/*）
try:
    from bwibbu_blueprint import create_bwibbu_blueprint
    app.register_blueprint(create_bwibbu_blueprint(DatabaseManager, stock_api))
    logger.info("BWIBBU Blueprint 已註冊於 /api/bwibbu")
except Exception as e:
    logger.warning(f"BWIBBU Blueprint 註冊失敗: {e}")

# 主頁路由 - 提供前端 UI
@app.route('/')
def index():
    """提供主頁面"""
    return send_file(os.path.join(frontend_dir, 'index.html'))

@app.route('/<path:filename>')
def static_files(filename):
    """提供靜態文件"""
    return send_from_directory(frontend_dir, filename)

# API 路由定義

@app.route('/api/twse/bwibbu', methods=['GET'])
def get_twse_bwibbu_all():
    """從資料庫讀取 BWIBBU 指標。
    Query:
      - start: YYYY-MM-DD（可空）
      - end: YYYY-MM-DD（可空）
      - mode: latest_in_range | timeseries（預設 latest_in_range，只取範圍內最接近 end 的一天）
      - limit: 顯示筆數上限（僅對單日資料有意義）
    回傳：{ success, count, latestDate, usedDate, data }
    """
    try:
        db = DatabaseManager.from_request_args(request.args)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        try:
            db.create_tables()
            # 取得最新日期（資料庫中最大者）
            latest_date = stock_api.get_latest_bwibbu_date(db_manager=db)

            # 解析查詢參數
            mode = (request.args.get('mode') or 'latest_in_range').strip().lower()
            start_str = request.args.get('start')
            end_str = request.args.get('end')
            limit_param = request.args.get('limit')
            limit_value = None
            if limit_param is not None:
                try:
                    limit_value = int(limit_param)
                except Exception:
                    limit_value = None

            # 取得可用日期清單
            cur = db.connection.cursor()
            cur.execute("SELECT DISTINCT date FROM tw_stock_bwibbu ORDER BY date ASC")
            date_rows = cur.fetchall()
            available_dates = [ (r['date'] if isinstance(r, dict) else r[0]) for r in date_rows ]

            if not available_dates:
                return jsonify({'success': True, 'count': 0, 'latestDate': None, 'usedDate': None, 'data': []})

            # 將 start/end 轉為 date 物件
            def to_date(s):
                if not s:
                    return None
                try:
                    return datetime.strptime(s, '%Y-%m-%d').date()
                except Exception:
                    return None

            start_dt = to_date(start_str)
            end_dt = to_date(end_str)

            if end_dt is None:
                end_dt = available_dates[-1]
            if start_dt is None:
                start_dt = available_dates[0]

            # 過濾出範圍內的日期
            dates_in_range = [d for d in available_dates if d >= start_dt and d <= end_dt]

            if mode != 'timeseries':
                # latest_in_range：取範圍內最接近 end 的一天
                if not dates_in_range:
                    return jsonify({'success': True, 'count': 0, 'latestDate': latest_date.isoformat() if latest_date else None, 'usedDate': None, 'data': []})
                used_date = dates_in_range[-1]
                cur.execute(
                    """
                    SELECT code, name, pe_ratio, dividend_yield, pb_ratio
                    FROM tw_stock_bwibbu WHERE date=%s ORDER BY code
                    """,
                    (used_date,)
                )
                rows = cur.fetchall()
                data = []
                for row in rows:
                    if isinstance(row, dict):
                        code = row.get('code'); name = row.get('name'); pe = row.get('pe_ratio'); dy = row.get('dividend_yield'); pb = row.get('pb_ratio')
                    else:
                        code, name, pe, dy, pb = row
                    data.append({
                        'Code': code,
                        'Name': name,
                        'PEratio': '' if pe is None else str(pe),
                        'DividendYield': '' if dy is None else str(dy),
                        'PBratio': '' if pb is None else str(pb),
                        'Date': used_date.isoformat()
                    })
                if isinstance(limit_value, int) and limit_value >= 0:
                    data = data[:limit_value]
                return jsonify({
                    'success': True,
                    'count': len(data),
                    'latestDate': latest_date.isoformat() if latest_date else None,
                    'usedDate': used_date.isoformat(),
                    'data': data
                })
            else:
                # timeseries：回傳多日，每日全市場資料量很大，此處僅回傳該範圍內的日期清單供前端後續分頁請求
                # 若要一次回傳所有資料，可能過大，不建議。這裡先回傳 dates，前端可逐日請求（或之後擴充 /api/twse/bwibbu/by-date?date=...）
                return jsonify({
                    'success': True,
                    'count': len(dates_in_range),
                    'latestDate': latest_date.isoformat() if latest_date else None,
                    'usedDate': None,
                    'dates': [d.isoformat() for d in dates_in_range],
                    'data': []
                })
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"取得 BWIBBU_ALL 資料失敗: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.get('/api/financial-ratios')
def api_financial_ratios():
    year = request.args.get('year')
    season = request.args.get('season')
    code = request.args.get('code')
    code_from = request.args.get('code_from')
    code_to = request.args.get('code_to')

    if not year or not season:
        return jsonify({'error': 'year and season are required'}), 400

    period_label = f"{str(year)}{int(str(season)):02d}"

    write_raw = request.args.get('write_to_db') or request.args.get('import_db')
    write_to_db = False
    if write_raw is not None:
        v = str(write_raw).strip().lower()
        write_to_db = v in ('1', 'true', 'yes', 'y')

    from datetime import datetime as _dt
    global financial_ratios_status
    financial_ratios_status = {
        'running': True,
        'startedAt': _dt.utcnow().isoformat(),
        'finishedAt': None,
        'phase': 'querying',
        'year': str(year),
        'season': str(season),
        'period': period_label,
        'total': None,
        'processed': 0,
        'success_count': 0,
        'error_count': 0,
        'current_code': None,
        'error': None,
        'db_write_enabled': bool(write_to_db),
        'db_inserted_rows': 0,
        'db_batches': 0,
        'db_last_commit_at': None,
    }

    db = DatabaseManager.from_request_args(request.args)
    if not db.connect():
        try:
            financial_ratios_status['running'] = False
            financial_ratios_status['finishedAt'] = _dt.utcnow().isoformat()
            financial_ratios_status['phase'] = 'error'
            financial_ratios_status['error'] = '資料庫連線失敗'
        except Exception:
            pass
        return jsonify({'error': '資料庫連線失敗'}), 500

    inserted = 0
    try:
        if not db.create_tables():
            return jsonify({'error': '資料庫初始化失敗'}), 500

        where = ["i.period = %s", "b.period = %s", "i.\"股票代號\" = b.\"股票代號\""]
        params = [period_label, period_label]
        if code:
            where.append("i.\"股票代號\" = %s")
            params.append(str(code).strip())
        else:
            cf = str(code_from).strip() if code_from else None
            ct = str(code_to).strip() if code_to else None
            if cf:
                where.append("i.\"股票代號\" >= %s")
                params.append(cf)
            if ct:
                where.append("i.\"股票代號\" <= %s")
                params.append(ct)

        select_cols_income = [
            'i."股票代號" as "股票代號"',
            'i.period as period',
            'i."Revenue" as "Revenue"',
            'i."GrossProfitFromOperations" as "GrossProfitFromOperations"',
            'i."ProfitLossFromOperatingActivities" as "ProfitLossFromOperatingActivities"',
            'i."ProfitLoss" as "ProfitLoss"',
        ]
        select_cols_balance = [
            'b."Assets" as "Assets"',
            'b."Liabilities" as "Liabilities"',
            'b."EquityAttributableToOwnersOfParent" as "EquityAttributableToOwnersOfParent"',
            'b."CurrentAssets" as "CurrentAssets"',
            'b."CurrentLiabilities" as "CurrentLiabilities"',
            'b."CashAndCashEquivalents" as "CashAndCashEquivalents"',
            'b."AccountsReceivableNet" as "AccountsReceivableNet"',
            'b."OtherCurrentReceivables" as "OtherCurrentReceivables"',
            'b."CurrentFinancialAssetsAtAmortisedCost" as "CurrentFinancialAssetsAtAmortisedCost"',
            'b."CurrentFinancialAssetsAtFairValueThroughProfitOrLoss" as "CurrentFinancialAssetsAtFairValueThroughProfitOrLoss"',
            'b."CurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome" as "CurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome"',
            'b."OtherCurrentFinancialAssets" as "OtherCurrentFinancialAssets"',
        ]

        sql_text = (
            "SELECT "
            + ", ".join(select_cols_income + select_cols_balance)
            + f" FROM {db.table_income} i JOIN {db.table_balance} b ON (i.\"股票代號\" = b.\"股票代號\" AND i.period = b.period)"
            + " WHERE "
            + " AND ".join(where)
        )

        with db.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql_text, params)
            rows = cur.fetchall() or []

        out = []
        missing_symbol = 0
        invalid_period = 0
        buffer = []
        batches = 0
        batch_commit_size = 500

        try:
            financial_ratios_status['total'] = int(len(rows))
            financial_ratios_status['phase'] = 'computing'
        except Exception:
            pass

        for idx, r in enumerate(rows, start=1):
            try:
                code_raw = r.get('股票代號')
                symbol = f"{str(code_raw).strip()}.TW" if code_raw is not None and str(code_raw).strip() else None
                if not symbol:
                    missing_symbol += 1
                base = {'symbol': symbol, 'period': r.get('period')}
                if not base.get('period'):
                    invalid_period += 1
                base.update(_compute_ratios_record(r))
                out.append(base)

                if write_to_db:
                    buffer.append(base)

                try:
                    financial_ratios_status['processed'] = int(idx)
                    financial_ratios_status['current_code'] = base.get('symbol')
                    financial_ratios_status['success_count'] = int(financial_ratios_status.get('success_count') or 0) + 1
                except Exception:
                    pass

                if write_to_db and len(buffer) >= batch_commit_size:
                    batch_inserted = stock_api.upsert_financial_ratios(buffer, db_manager=db)
                    inserted += int(batch_inserted or 0)
                    batches += 1
                    buffer = []
                    try:
                        financial_ratios_status['db_inserted_rows'] = int(inserted)
                        financial_ratios_status['db_batches'] = int(batches)
                        financial_ratios_status['db_last_commit_at'] = _dt.utcnow().isoformat()
                    except Exception:
                        pass
            except Exception as e:
                try:
                    financial_ratios_status['processed'] = int(idx)
                    financial_ratios_status['error_count'] = int(financial_ratios_status.get('error_count') or 0) + 1
                    financial_ratios_status['error'] = str(e)
                except Exception:
                    pass

        if write_to_db and buffer:
            batch_inserted = stock_api.upsert_financial_ratios(buffer, db_manager=db)
            inserted += int(batch_inserted or 0)
            batches += 1
            try:
                financial_ratios_status['db_inserted_rows'] = int(inserted)
                financial_ratios_status['db_batches'] = int(batches)
                financial_ratios_status['db_last_commit_at'] = _dt.utcnow().isoformat()
            except Exception:
                pass

        try:
            financial_ratios_status['running'] = False
            financial_ratios_status['finishedAt'] = _dt.utcnow().isoformat()
            financial_ratios_status['phase'] = 'done'
        except Exception:
            pass

        payload = {
            'meta': {
                'year': str(year),
                'season': str(season),
                'period': period_label,
                'rows': len(out),
                'write_to_db': bool(write_to_db),
                'inserted': int(inserted),
                'batches': int(batches),
                'missing_symbol': int(missing_symbol),
                'invalid_period': int(invalid_period),
            },
            'data': out,
        }
        return jsonify(payload)
    except Exception as exc:
        try:
            financial_ratios_status['running'] = False
            financial_ratios_status['finishedAt'] = _dt.utcnow().isoformat()
            financial_ratios_status['phase'] = 'error'
            financial_ratios_status['error'] = str(exc)
        except Exception:
            pass
        logger.error(f"financial-ratios error: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        try:
            db.disconnect()
        except Exception:
            pass


@app.get('/api/financial-ratios/status')
def api_financial_ratios_status():
    """Return current progress status of financial-ratios computation."""

    try:
        return jsonify({'success': True, 'status': financial_ratios_status})
    except Exception as e:
        logger.error(f"financial-ratios status error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/twse/bwibbu/refresh_range', methods=['POST'])
def refresh_twse_bwibbu_range():
    """批次刷新指定日期區間的 BWIBBU 指標（使用 BWIBBU_d）。
    JSON body: { start: 'YYYY-MM-DD', end: 'YYYY-MM-DD', use_local_db?: bool }
    回傳每一日寫入筆數與總結。
    """
    try:
        if request.method == 'OPTIONS':
            return jsonify({'success': True}), 200
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400
        body = request.get_json() or {}
        start_str = body.get('start')
        end_str = body.get('end')
        if not start_str or not end_str:
            return jsonify({'success': False, 'error': '缺少 start 或 end'}), 400

        try:
            start_dt = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_str, '%Y-%m-%d').date()
        except Exception:
            return jsonify({'success': False, 'error': '日期格式錯誤，需 YYYY-MM-DD'}), 400
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        total_inserted = 0
        per_day = []
        processed_days = 0
        try:
            db.create_tables()
            current = start_dt
            while current <= end_dt:
                records = stock_api.fetch_twse_bwibbu_by_date(current)
                inserted = 0
                if records:
                    inserted = stock_api.upsert_bwibbu_records(records, db_manager=db)
                    total_inserted += inserted
                    processed_days += 1
                per_day.append({
                    'date': current.isoformat(),
                    'fetched': len(records),
                    'inserted': inserted
                })
                time.sleep(0.6)  # 禮貌延遲，避免 TWSE 防爬
                current += timedelta(days=1)

            return jsonify({
                'success': True,
                'start': start_dt.isoformat(),
                'end': end_dt.isoformat(),
                'daysProcessed': processed_days,
                'totalInserted': total_inserted,
                'details': per_day
            })
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"刷新 BWIBBU 區間失敗: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.route('/api/twse/bwibbu/refresh', methods=['POST'])
def refresh_twse_bwibbu():
    """手動刷新並儲存 BWIBBU_ALL 指標資料。Body: {force_refresh, fetch_only}"""
    try:
        payload = request.get_json(silent=True) or {}
        force_refresh = bool(payload.get('force_refresh', True))
        fetch_only = bool(payload.get('fetch_only', False))

        db = DatabaseManager.from_request_payload(payload)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        inserted = 0
        try:
            db.create_tables()
            data = stock_api.fetch_twse_bwibbu_all(force_refresh=force_refresh)
            if not fetch_only:
                inserted = stock_api.upsert_bwibbu_records(data, db_manager=db)
            return jsonify({
                'success': True,
                'inserted': inserted,
                'count': len(data)
            })
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"刷新 BWIBBU_ALL 失敗: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500


def _upsert_prices(cursor, symbol, price_records, prices_table: str = None):
    """將價格資料批量 upsert 進 tw_stock_prices。price_records: list[dict] with keys Date/Open/High/Low/Close/Volume 或對應小寫欄位。
    """
    if not price_records:
        logger.warning(f"_upsert_prices: {symbol} 收到空資料，跳過")
        return 0
    logger.info(f"_upsert_prices: 準備寫入 {symbol} 的 {len(price_records)} 筆資料")
    def _norm_date(val):
        if val is None:
            return None
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime().date().strftime('%Y-%m-%d')
        if isinstance(val, datetime):
            return val.date().strftime('%Y-%m-%d')
        if isinstance(val, date):
            return val.strftime('%Y-%m-%d')
        if isinstance(val, str):
            return val[:10]
        try:
            if hasattr(val, 'to_pydatetime'):
                return val.to_pydatetime().date().strftime('%Y-%m-%d')
        except Exception:
            pass
        return None

    raw_values = []
    for pr in price_records:
        record_date = _norm_date(pr.get('date') or pr.get('Date'))
        if not record_date:
            continue
        volume_value = None
        if 'volume' in pr:
            volume_value = pr.get('volume')
        elif 'Volume' in pr:
            volume_value = pr.get('Volume')
        raw_values.append(
            (
                symbol,
                record_date,
                pr.get('open_price') or pr.get('Open'),
                pr.get('high_price') or pr.get('High'),
                pr.get('low_price') or pr.get('Low'),
                pr.get('close_price') or pr.get('Close'),
                volume_value
            )
        )
    if not raw_values:
        return 0

    # 去重以避免同一批次內重複 (symbol, date) 造成 ON CONFLICT 二次命中
    dedup = {}
    for v in raw_values:
        dedup[(v[0], v[1])] = v
    values = list(dedup.values())

    if not prices_table:
        prices_table = getattr(cursor, 'table_prices', None) or 'tw_stock_prices'

    upsert_sql = f"""
        INSERT INTO {prices_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume
    """
    execute_values(cursor, upsert_sql, values, page_size=500)
    logger.info(f"_upsert_prices: 成功寫入 {symbol} 的 {len(values)} 筆資料")
    return len(values)

@app.route('/api/prices/twii/import_yf', methods=['POST'])
def import_twii_from_yfinance():
    """使用 yfinance 匯入 ^TWII 日K 至 tw_stock_prices。
    JSON body: { start?: 'YYYY-MM-DD', end?: 'YYYY-MM-DD', use_local_db?: bool }
    若未提供 start / end，分別使用 DEFAULT_START_DATE 與今天。
    """
    try:
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400

        body = request.get_json() or {}
        start_str = body.get('start') or DEFAULT_START_DATE
        end_str = body.get('end') or datetime.now().strftime('%Y-%m-%d')

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        inserted = 0
        count = 0
        deleted = 0
        kept = 0
        skipped = 0
        duplicated = 0
        try:
            db.create_tables()
            # on conflict 需要 unique index；若 create_tables 因鎖超時略過，也要補上
            db.ensure_prices_unique()
            records = stock_api.fetch_twii_with_yfinance(start_str, end_str)
            # 指數成交金額：使用 TWSE FMTQIK 的「成交金額」（單位：元）覆寫 Volume
            def _normalize_to_date_str(value):
                if value is None:
                    return None
                if isinstance(value, str):
                    return value[:10]
                if isinstance(value, datetime):
                    return value.date().isoformat()
                if isinstance(value, date):
                    return value.isoformat()
                return str(value)[:10]

            def _parse_roc_date(roc_str):
                try:
                    y, m, d = str(roc_str).split('/')
                    return date(int(y) + 1911, int(m), int(d))
                except Exception:
                    return None

            def _month_start_iter(start_d, end_d):
                cur = date(start_d.year, start_d.month, 1)
                end_anchor = date(end_d.year, end_d.month, 1)
                while cur <= end_anchor:
                    yield cur
                    if cur.month == 12:
                        cur = date(cur.year + 1, 1, 1)
                    else:
                        cur = date(cur.year, cur.month + 1, 1)

            def fetch_twse_turnover_map(start_iso, end_iso):
                try:
                    start_d = date.fromisoformat(str(start_iso)[:10])
                    end_d = date.fromisoformat(str(end_iso)[:10])
                except Exception:
                    return {}
                if end_d < start_d:
                    return {}

                turnover_by_date = {}
                session = requests.Session()
                for anchor in _month_start_iter(start_d, end_d):
                    try:
                        resp = session.get(
                            FMTQIK_URL,
                            params={'response': 'json', 'date': anchor.strftime('%Y%m%d')},
                            timeout=15,
                        )
                        if resp.status_code != 200:
                            continue
                        payload = resp.json()
                        if payload.get('stat') != 'OK' or not payload.get('data'):
                            continue
                        fields = payload.get('fields') or []
                        amount_idx = 2
                        try:
                            if '成交金額' in fields:
                                amount_idx = fields.index('成交金額')
                        except Exception:
                            amount_idx = 2
                        for row in payload.get('data', []):
                            if not row or len(row) <= amount_idx:
                                continue
                            d_obj = _parse_roc_date(row[0])
                            if not d_obj:
                                continue
                            if d_obj < start_d or d_obj > end_d:
                                continue
                            raw_amount = row[amount_idx]
                            if raw_amount in (None, '', '--', '---'):
                                continue
                            try:
                                amount = int(str(raw_amount).replace(',', ''))
                            except Exception:
                                continue
                            turnover_by_date[d_obj.isoformat()] = amount
                    except Exception:
                        continue
                return turnover_by_date

            turnover_map = fetch_twse_turnover_map(start_str, end_str)
            # 你的需求：沒對到 FMTQIK 的日期不要出現 -> 只保留能取得成交金額的日期
            def _try_parse_date(s):
                try:
                    return date.fromisoformat(str(s)[:10])
                except Exception:
                    return None

            start_range = _try_parse_date(start_str)
            end_range = _try_parse_date(end_str)
            if start_range and end_range and end_range < start_range:
                start_range, end_range = end_range, start_range

            def _pick_turnover_match(d_str):
                if not isinstance(turnover_map, dict) or not turnover_map:
                    return None, None, False
                if d_str in turnover_map:
                    return turnover_map.get(d_str), d_str, False
                d_obj = _try_parse_date(d_str)
                if not d_obj:
                    return None, None, False
                d_prev = (d_obj - timedelta(days=1)).isoformat()
                if d_prev in turnover_map:
                    return turnover_map.get(d_prev), d_prev, True
                d_next = (d_obj + timedelta(days=1)).isoformat()
                if d_next in turnover_map:
                    return turnover_map.get(d_next), d_next, True
                return None, None, False

            # 已改為完全依賴 yfinance 資料，不再套用 FMTQIK turnover 覆寫/去重，直接寫入
            if records:
                count = len(records)
                cur = db.connection.cursor()
                inserted = _upsert_prices(cur, '^TWII', records, prices_table=db.table_prices)
                db.connection.commit()
            return jsonify({
                'success': True,
                'symbol': '^TWII',
                'start': start_str,
                'end': end_str,
                'fetched': count,
                'inserted': inserted,
                'deleted_in_range': deleted,
                'skipped_no_turnover': skipped,
                'skipped_duplicate_date': duplicated
            })
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"import_twii_from_yfinance 錯誤: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500

def _validate_refetched_records(cur, symbol, recs, threshold=0.5):
    """依前一交易日收盤價檢核：
    - 跳過 close<=0 或缺失
    - |pct_change| > threshold 視為可疑（預設 50%），寫入備份異常表但不 upsert
    回傳 (filtered_recs, skipped_recs)
    """
    if not recs:
        return [], []
    # 若未指定門檻（None）則不進行過濾，直接回傳
    if threshold is None:
        return recs, []
    # 取得 DB 先前收盤
    first_date = recs[0].get('date') or recs[0].get('Date')
    prev_close = None
    try:
        cur.execute("SELECT close_price FROM tw_stock_prices WHERE symbol=%s AND date < %s ORDER BY date DESC LIMIT 1", [symbol, first_date])
        row = cur.fetchone()
        if row:
            prev_close = float(row['close_price']) if isinstance(row, dict) else float(row[0])
    except Exception:
        prev_close = None

    # 依日期排序
    def _get_date(r):
        return r.get('date') or r.get('Date')
    recs_sorted = sorted(recs, key=_get_date)

    filtered = []
    skipped = []
    for r in recs_sorted:
        close = r.get('close_price') or r.get('Close')
        if close is None or close <= 0:
            skipped.append((r, 'close_le_zero_or_null'))
            continue
        if prev_close is not None and prev_close != 0:
            pct = abs(float(close) - float(prev_close)) / abs(float(prev_close))
            if pct > threshold:
                skipped.append((r, f'pct_change_gt_{threshold}'))
                # 更新 prev_close 為當前（即便跳過，以便後續比較較為連續）
                prev_close = float(close)
                continue
        filtered.append(r)
        prev_close = float(close)
    return filtered, skipped

def _detect_price_anomalies(cursor, symbol=None, start_date=None, end_date=None, threshold=0.2):
    """以相鄰收盤價漲跌幅偵測異常，返回 list[dict]。"""
    conds = []
    params = []
    if symbol:
        conds.append("symbol = %s")
        params.append(symbol)
    if start_date:
        conds.append("date >= %s")
        params.append(start_date)
    if end_date:
        conds.append("date <= %s")
        params.append(end_date)
    where_sql = ("WHERE " + " AND ".join(conds)) if conds else ""
    sql = f"""
        WITH px AS (
            SELECT symbol, date, close_price,
                   LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
            FROM tw_stock_prices
            {where_sql}
        )
        SELECT symbol, date, close_price, prev_close,
               CASE WHEN prev_close IS NOT NULL AND prev_close <> 0
                    THEN ABS(close_price - prev_close) / ABS(prev_close)
               END AS pct_change
        FROM px
        WHERE prev_close IS NOT NULL
          AND prev_close <> 0
          AND (CASE WHEN prev_close <> 0 THEN ABS(close_price - prev_close) / ABS(prev_close) END) > %s
        ORDER BY symbol, date
    """
    cursor.execute(sql, params + [threshold])
    rows = cursor.fetchall()
    # 正規化輸出
    anomalies = []
    for r in rows:
        date_val = r['date']
        anomalies.append({
            'symbol': r['symbol'],
            'date': date_val.strftime('%Y-%m-%d') if hasattr(date_val, 'strftime') else str(date_val),
            'close': float(r['close_price']) if r['close_price'] is not None else None,
            'prev_close': float(r['prev_close']) if r['prev_close'] is not None else None,
            'pct_change': float(r['pct_change']) if r['pct_change'] is not None else None
        })
    return anomalies

@app.route('/api/anomalies/detect', methods=['GET'])
def detect_anomalies():
    """偵測 tw_stock_prices 異常跳動。query: symbol, start, end, threshold=0.2"""
    try:
        symbol = request.args.get('symbol')
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        threshold = float(request.args.get('threshold', '0.2'))

        db = DatabaseManager.from_request_args(request.args)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
        try:
            # 確保需要的資料表存在
            try:
                db.create_tables()
            except Exception as _:
                pass
            cur = db.connection.cursor()
            anomalies = _detect_price_anomalies(cur, symbol, start_date, end_date, threshold)
            return jsonify({'success': True, 'count': len(anomalies), 'threshold': threshold, 'data': anomalies})
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"detect_anomalies 錯誤: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/income-statement/import', methods=['POST', 'OPTIONS'])
def api_income_statement_import():
    """將前端提供的損益表寬表資料寫入資料庫。

    Body(JSON): {
        rows: [...],        # 由 /api/income-statement 回傳的每列資料
        use_local_db?: bool # true 則寫入本地 PostgreSQL，否則寫入 Neon
    }
    """
    try:
        if request.method == 'OPTIONS':
            return jsonify({'success': True}), 200
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400

        body = request.get_json() or {}
        rows = body.get('rows') or body.get('data') or []
        if not isinstance(rows, list):
            return jsonify({'success': False, 'error': 'rows 必須為陣列'}), 400

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        try:
            inserted = stock_api.upsert_income_statements(rows, db_manager=db)
            return jsonify({'success': True, 'inserted': inserted, 'count': len(rows)})
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"income-statement import error: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.route('/api/balance-sheet/import', methods=['POST', 'OPTIONS'])
def api_balance_sheet_import():
    """將前端提供的資產負債表寬表資料寫入資料庫。

    Body(JSON): {
        rows: [...],        # 由 /api/balance-sheet 回傳的每列資料
        use_local_db?: bool # true 則寫入本地 PostgreSQL，否則寫入 Neon
    }
    """
    try:
        if request.method == 'OPTIONS':
            return jsonify({'success': True}), 200
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400

        body = request.get_json() or {}
        rows = body.get('rows') or body.get('data') or []
        if not isinstance(rows, list):
            return jsonify({'success': False, 'error': 'rows 必須為陣列'}), 400

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        try:
            inserted = stock_api.upsert_balance_sheets(rows, db_manager=db)
            return jsonify({'success': True, 'inserted': inserted, 'count': len(rows)})
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"balance-sheet import error: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.route('/api/cash-flow-statement/import', methods=['POST', 'OPTIONS'])
def api_cash_flow_import():
    """將前端提供的現金流量表寬表資料寫入資料庫。"""
    try:
        if request.method == 'OPTIONS':
            return jsonify({'success': True}), 200
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400
        body = request.get_json() or {}
        rows = body.get('rows') or body.get('data') or []
        if not isinstance(rows, list):
            return jsonify({'success': False, 'error': 'rows 必須為陣列'}), 400

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
        try:
            inserted = stock_api.upsert_cash_flows(rows, db_manager=db)
            return jsonify({'success': True, 'inserted': inserted, 'count': len(rows)})
        finally:
            db.disconnect()
    except Exception as exc:
        logger.error(f"cash-flow-statement import error: {exc}")
        return jsonify({'success': False, 'error': str(exc)}), 500


@app.route('/api/anomalies/export', methods=['GET'])
def export_anomalies():
    """匯出異常清單（CSV，Excel 可開啟）。Query: symbol, start, end, threshold=0.2"""
    try:
        symbol = request.args.get('symbol')
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        threshold = float(request.args.get('threshold', '0.2'))

        db = DatabaseManager.from_request_args(request.args)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
        try:
            # 確保表存在
            try:
                db.create_tables()
            except Exception:
                pass
            cur = db.connection.cursor()
            anomalies = _detect_price_anomalies(cur, symbol, start_date, end_date, threshold)

            # 轉 CSV
            import io, csv
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['symbol', 'date', 'prev_close', 'close', 'pct_change'])
            for a in anomalies:
                writer.writerow([a['symbol'], a['date'], a['prev_close'], a['close'], a['pct_change']])
            csv_data = output.getvalue()

            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"anomalies_{ts}.csv"
            headers = {
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
            return Response(csv_data, mimetype='text/csv; charset=utf-8', headers=headers)
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"export_anomalies 錯誤: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/anomalies/fix', methods=['POST'])
def fix_anomalies():
    """備份+刪除異常資料，並重抓指定範圍。
    JSON body: {symbol (可選), start, end, threshold=0.2, ruleVersion='rules_v1_pct', refetchPaddingDays=5}
    若未指定 symbol，將對全市場在期間內進行偵測與修復（較耗時）。
    """
    try:
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400
        body = request.get_json() or {}
        symbol = body.get('symbol')
        start_date = body.get('start')
        end_date = body.get('end')
        threshold = float(body.get('threshold', 0.2))
        rule_ver = body.get('ruleVersion', 'rules_v1_pct')
        pad_days = int(body.get('refetchPaddingDays', 5))
        refetch_only = bool(body.get('refetchOnly', True))
        # 新增：重抓驗證門檻，可調整或關閉（None 表示不驗證）
        rv_thresh = body.get('refetchValidationThreshold', 0.5)
        try:
            if rv_thresh is not None:
                rv_thresh = float(rv_thresh)
        except Exception:
            rv_thresh = 0.5

        db = DatabaseManager.from_request_args(request.args)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
        cur = db.connection.cursor()
        try:
            # 確保需要的資料表存在（包含備份與稽核表）
            try:
                db.create_tables()
            except Exception as _:
                pass
            anomalies = _detect_price_anomalies(cur, symbol, start_date, end_date, threshold)
            if not anomalies:
                return jsonify({'success': True, 'message': '未發現異常', 'deleted': 0, 'refetched': 0, 'data': []})

            # 依 symbol 彙總日期
            by_symbol = {}
            for a in anomalies:
                by_symbol.setdefault(a['symbol'], set()).add(a['date'])

            total_deleted = 0
            total_refetched = 0
            details = []

            for sym, date_set in by_symbol.items():
                date_list = sorted(list(date_set))
                if not refetch_only:
                    # 備份 + 刪除
                    placeholders = ','.join(['%s'] * len(date_list))
                    cur.execute(
                        f"""
                            INSERT INTO stock_prices_backup_anomaly
                                (symbol, date, open_price, high_price, low_price, close_price, volume, reason, rule_version, threshold)
                            SELECT symbol, date, open_price, high_price, low_price, close_price, volume,
                                   'pct_change_gt_threshold', %s, %s
                            FROM tw_stock_prices
                            WHERE symbol = %s AND date IN ({placeholders})
                        """,
                        [rule_ver, threshold, sym] + date_list
                    )
                    cur.execute(
                        f"DELETE FROM tw_stock_prices WHERE symbol = %s AND date IN ({placeholders})",
                        [sym] + date_list
                    )
                    total_deleted += cur.rowcount if cur.rowcount else 0

                # 重抓：擴大區間避免缺邊
                refetch_start = min(date_list)
                refetch_end = max(date_list)
                try:
                    rs = datetime.strptime(refetch_start, '%Y-%m-%d') - timedelta(days=pad_days)
                    re = datetime.strptime(refetch_end, '%Y-%m-%d') + timedelta(days=pad_days)
                    rs_str = rs.strftime('%Y-%m-%d')
                    re_str = re.strftime('%Y-%m-%d')
                except Exception:
                    rs_str = start_date
                    re_str = end_date

                price_df = stock_api.fetch_stock_data(sym, rs_str, re_str)
                symbol_inserted = 0
                preview = []
                fetched_count = 0
                skipped_count = 0
                validated_count = 0
                fetch_error = None
                if price_df is not None and ((hasattr(price_df, 'empty') and not price_df.empty) or (isinstance(price_df, list) and price_df)):
                    # 標準化成 list[dict]
                    recs = price_df.to_dict('records') if hasattr(price_df, 'to_dict') else price_df
                    fetched_count = len(recs)
                    # 驗證與過濾
                    # 刪除+重抓模式下，預設不進行驗證過濾（確保能覆蓋錯值），可由參數覆寫
                    eff_thresh = (rv_thresh if refetch_only else None)
                    recs_filtered, recs_skipped = _validate_refetched_records(cur, sym, recs, threshold=eff_thresh)
                    symbol_inserted = _upsert_prices(cur, sym, recs_filtered, prices_table=db.table_prices)
                    total_refetched += symbol_inserted

                # 稽核紀錄
                cur.execute(
                    """
                        INSERT INTO stock_anomaly_audit
                            (symbol, start_date, end_date, deleted_count, refetched_count, rule_version, threshold)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    [sym, start_date or rs_str, end_date or re_str, (0 if refetch_only else total_deleted), total_refetched, rule_ver, threshold]
                )
                # 每檔提交，讓資料在過程中即時生效
                db.connection.commit()

                details.append({
                    'symbol': sym,
                    'dates': date_list,
                    'refetch_range': {'start': rs_str, 'end': re_str},
                    'fetched': fetched_count,
                    'inserted': symbol_inserted,
                    'skipped': skipped_count,
                    'validated': validated_count,
                    'error': fetch_error
                })

            # 最終提交（多數情況已於每檔提交，這裡作為保險）
            db.connection.commit()
            return jsonify({'success': True, 'deleted': (0 if refetch_only else total_deleted), 'refetched': total_refetched, 'count': len(anomalies), 'details': details, 'refetchOnly': refetch_only})
        except Exception as e:
            db.connection.rollback()
            logger.error(f"fix_anomalies 失敗: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"fix_anomalies 外層錯誤: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/anomalies/fix_stream', methods=['GET'])
def fix_anomalies_stream():
    """以 Server-Sent Events (SSE) 方式串流修復進度與抓到的股價預覽。
    Query: symbol(可選), start, end, threshold=0.2, refetchOnly=true, refetchPaddingDays=5
    注意：此端點僅執行重抓(upsert)，不進行刪除/備份，以確保即時回報（等同 refetchOnly）。
    """
    try:
        symbol = request.args.get('symbol')
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        threshold = float(request.args.get('threshold', '0.2'))
        pad_days = int(request.args.get('refetchPaddingDays', '5'))
        # 串流期間可能因 debug middleware 失去 request context，先把 args 快照下來
        args_snapshot = request.args.to_dict(flat=True)
        # 新增：重抓驗證門檻（串流端點為 refetch-only 模式，預設 0.5，可由 query 覆寫）
        rv_thresh_param = request.args.get('refetchValidationThreshold')
        rv_thresh = None
        try:
            rv_thresh = float(rv_thresh_param) if rv_thresh_param is not None else 0.5
        except Exception:
            rv_thresh = 0.5

        def sse_format(obj: dict) -> str:
            import json
            return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

        def generate():
            db = DatabaseManager.from_request_args(args_snapshot)
            if not db.connect():
                yield sse_format({'type': 'error', 'message': '資料庫連線失敗'})
                return
            try:
                try:
                    db.create_tables()
                except Exception:
                    pass

                # 先送出一筆資料，避免瀏覽器/代理以為連線閒置而中斷
                yield sse_format({'type': 'ping'})
                yield sse_format({'type': 'start', 'symbol': symbol, 'start': start_date, 'end': end_date, 'threshold': threshold})

                def _ensure_connection():
                    if db.connection is None or getattr(db.connection, 'closed', 1) != 0:
                        try:
                            db.disconnect()
                        except Exception:
                            pass
                        if not db.connect():
                            raise RuntimeError('資料庫連線失敗')
                        try:
                            db.create_tables()
                        except Exception:
                            pass

                def _is_retryable_db_error(err: Exception) -> bool:
                    if not isinstance(err, psycopg2.Error):
                        return False
                    msg = str(err)
                    return (
                        isinstance(err, (psycopg2.OperationalError, psycopg2.InterfaceError))
                        or 'connection already closed' in msg
                        or 'cursor already closed' in msg
                        or 'SSL connection has been closed unexpectedly' in msg
                        or 'server closed the connection unexpectedly' in msg
                        or 'terminating connection' in msg
                        or 'current transaction is aborted' in msg
                    )

                def _run_db(step: str, op, *, commit: bool = False):
                    last_err = None
                    for attempt in range(2):
                        cur_local = None
                        try:
                            _ensure_connection()
                            cur_local = db.connection.cursor()
                            result = op(cur_local)
                            if commit:
                                db.connection.commit()
                            return result
                        except Exception as e:
                            last_err = e
                            try:
                                if getattr(db, 'connection', None) is not None:
                                    db.connection.rollback()
                            except Exception:
                                pass
                            if attempt == 0 and _is_retryable_db_error(e):
                                try:
                                    db.disconnect()
                                except Exception:
                                    pass
                                continue
                            raise RuntimeError(f"{step} 失敗: {e}")
                        finally:
                            if cur_local is not None:
                                try:
                                    cur_local.close()
                                except Exception:
                                    pass
                    if last_err is not None:
                        raise last_err

                anomalies = _run_db(
                    'detect_anomalies',
                    lambda cur: _detect_price_anomalies(cur, symbol, start_date, end_date, threshold),
                    commit=False,
                )
                if not anomalies:
                    yield sse_format({'type': 'done', 'success': True, 'count': 0, 'refetched': 0, 'details': []})
                    return

                # 依 symbol 彙總日期
                by_symbol: dict[str, set] = {}
                for a in anomalies:
                    by_symbol.setdefault(a['symbol'], set()).add(a['date'])

                total_refetched = 0
                details = []

                for sym, date_set in by_symbol.items():
                    date_list = sorted(list(date_set))
                    refetch_start = min(date_list)
                    refetch_end = max(date_list)
                    try:
                        rs = datetime.strptime(refetch_start, '%Y-%m-%d') - timedelta(days=pad_days)
                        re = datetime.strptime(refetch_end, '%Y-%m-%d') + timedelta(days=pad_days)
                        rs_str = rs.strftime('%Y-%m-%d')
                        re_str = re.strftime('%Y-%m-%d')
                    except Exception:
                        rs_str = start_date
                        re_str = end_date

                    yield sse_format({'type': 'symbol_start', 'symbol': sym, 'refetch_range': {'start': rs_str, 'end': re_str}, 'dates': date_list})

                    price_df = stock_api.fetch_stock_data(sym, rs_str, re_str)

                    symbol_inserted = 0
                    preview = []
                    fetched_count = 0
                    skipped_count = 0
                    validated_count = 0
                    fetch_error = None
                    if price_df is not None and ((hasattr(price_df, 'empty') and not price_df.empty) or (isinstance(price_df, list) and price_df)):
                        # 標準化成 list[dict]
                        recs = price_df.to_dict('records') if hasattr(price_df, 'to_dict') else price_df
                        fetched_count = len(recs)
                        # 驗證與過濾（串流端點預設 refetch-only，使用 rv_thresh）
                        def _upsert_op(cur):
                            recs_filtered, recs_skipped = _validate_refetched_records(cur, sym, recs, threshold=rv_thresh)
                            return _upsert_prices(cur, sym, recs_filtered, prices_table=db.table_prices)

                        symbol_inserted = int(_run_db(f'upsert_prices[{sym}]', _upsert_op, commit=True) or 0)
                        total_refetched += symbol_inserted

                    # 稽核（以refetch-only記錄）
                    def _audit_op(cur):
                        cur.execute(
                            """
                                INSERT INTO stock_anomaly_audit
                                    (symbol, start_date, end_date, deleted_count, refetched_count, rule_version, threshold)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            [sym, start_date or rs_str, end_date or re_str, 0, total_refetched, 'rules_v1_pct', threshold]
                        )

                    _run_db(f'audit_insert[{sym}]', _audit_op, commit=True)

                    info = {
                        'symbol': sym,
                        'inserted': int(symbol_inserted),
                        'preview': preview,
                        'refetch_range': {'start': rs_str, 'end': re_str}
                    }
                    details.append(info)
                    yield sse_format({'type': 'symbol_done', **info})

                try:
                    if getattr(db, 'connection', None) is not None:
                        db.connection.commit()
                except Exception:
                    pass
                yield sse_format({'type': 'done', 'success': True, 'count': len(anomalies), 'refetched': total_refetched, 'details': details})
            except GeneratorExit:
                try:
                    pass
                finally:
                    return
            except Exception as e:
                try:
                    if getattr(db, 'connection', None) is not None:
                        db.connection.rollback()
                except Exception:
                    pass
                yield sse_format({'type': 'error', 'message': str(e), 'error_type': type(e).__name__})
            finally:
                try:
                    db.disconnect()
                except Exception:
                    pass

        headers = {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        }
        return Response(stream_with_context(generate()), headers=headers)
    except Exception as e:
        logger.error(f"fix_anomalies_stream 外層錯誤: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/prices/refetch_range', methods=['POST'])
def refetch_prices_range():
    """刪除指定股票在日期區間內的全部股價資料，並整段重抓後寫回。
    JSON body: { symbol, start, end, use_local_db?: bool }
    """
    try:
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400

        body = request.get_json() or {}
        symbol = body.get('symbol')
        start_date = body.get('start')
        end_date = body.get('end')

        if not symbol or not start_date or not end_date:
            return jsonify({'success': False, 'error': '需要參數 symbol, start, end'}), 400

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        cur = db.connection.cursor()
        try:
            try:
                db.create_tables()
            except Exception:
                pass

            # 先備份整段（沿用 stock_prices_backup_anomaly 表）
            cur.execute(
                """
                    INSERT INTO stock_prices_backup_anomaly
                        (symbol, date, open_price, high_price, low_price, close_price, volume, reason, rule_version, threshold)
                    SELECT symbol, date, open_price, high_price, low_price, close_price, volume,
                           'range_refetch', 'range_refetch', NULL
                    FROM tw_stock_prices
                    WHERE symbol = %s AND date >= %s AND date <= %s
                """,
                [symbol, start_date, end_date]
            )

            # 刪除整段
            cur.execute(
                """
                    DELETE FROM tw_stock_prices
                    WHERE symbol = %s AND date >= %s AND date <= %s
                """,
                [symbol, start_date, end_date]
            )
            deleted_count = cur.rowcount if cur.rowcount else 0
            db.connection.commit()

            # 整段重抓並寫回（upsert）
            price_df = stock_api.fetch_stock_data(symbol, start_date, end_date)
            fetched_count = 0
            inserted_count = 0

            if price_df is not None and (
                (hasattr(price_df, 'empty') and not price_df.empty)
                or (isinstance(price_df, list) and price_df)
            ):
                recs = price_df.to_dict('records') if hasattr(price_df, 'to_dict') else price_df
                fetched_count = len(recs)
                inserted_count = _upsert_prices(cur, symbol, recs, prices_table=db.table_prices)
                db.connection.commit()

            return jsonify({
                'success': True,
                'symbol': symbol,
                'start': start_date,
                'end': end_date,
                'deleted': deleted_count,
                'fetched': fetched_count,
                'inserted': inserted_count,
            })
        except Exception as e:
            try:
                db.connection.rollback()
            except Exception:
                pass
            logger.error(f"refetch_prices_range 失敗: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"refetch_prices_range 外層錯誤: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/prices/refetch_range_by_anomalies', methods=['POST'])
def refetch_prices_range_by_anomalies():
    try:
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400

        body = request.get_json() or {}
        symbol = body.get('symbol')
        start_date = body.get('start')
        end_date = body.get('end')
        threshold = float(body.get('threshold', 0.2))

        if not start_date or not end_date:
            return jsonify({'success': False, 'error': '需要參數 start, end'}), 400

        db = DatabaseManager.from_request_payload(body)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        cur = db.connection.cursor()
        try:
            try:
                db.create_tables()
            except Exception:
                pass

            anomalies = _detect_price_anomalies(cur, symbol, start_date, end_date, threshold)
            symbols = sorted({a.get('symbol') for a in (anomalies or []) if a.get('symbol')})
            if not symbols:
                return jsonify({
                    'success': True,
                    'message': '未發現異常',
                    'start': start_date,
                    'end': end_date,
                    'threshold': threshold,
                    'symbols': [],
                    'details': [],
                    'deleted': 0,
                    'fetched': 0,
                    'inserted': 0,
                })

            details = []
            total_deleted = 0
            total_fetched = 0
            total_inserted = 0

            anomalies_by_symbol = {}
            for a in (anomalies or []):
                try:
                    sym = a.get('symbol')
                    d = a.get('date')
                    if not sym or not d:
                        continue
                    anomalies_by_symbol.setdefault(sym, set()).add(str(d))
                except Exception:
                    continue

            for sym in symbols:
                anomaly_dates = sorted(anomalies_by_symbol.get(sym, set()))
                if not anomaly_dates:
                    continue

                anomaly_dates_param = []
                for d in anomaly_dates:
                    try:
                        anomaly_dates_param.append(datetime.strptime(str(d), '%Y-%m-%d').date())
                    except Exception:
                        continue
                if not anomaly_dates_param:
                    continue

                cur.execute(
                    """
                        INSERT INTO stock_prices_backup_anomaly
                            (symbol, date, open_price, high_price, low_price, close_price, volume, reason, rule_version, threshold)
                        SELECT symbol, date, open_price, high_price, low_price, close_price, volume,
                               'anomaly_date_refetch', 'anomaly_date_refetch', %s
                        FROM tw_stock_prices
                        WHERE symbol = %s AND date = ANY(%s)
                    """,
                    [threshold, sym, anomaly_dates_param]
                )

                cur.execute(
                    """
                        DELETE FROM tw_stock_prices
                        WHERE symbol = %s AND date = ANY(%s)
                    """,
                    [sym, anomaly_dates_param]
                )
                deleted_count = cur.rowcount if cur.rowcount else 0
                total_deleted += deleted_count
                db.connection.commit()

                fetched_count = 0
                inserted_count = 0
                for d in anomaly_dates:
                    try:
                        price_df = stock_api.fetch_stock_data(sym, d, d)
                        if price_df is None:
                            continue
                        if not ((hasattr(price_df, 'empty') and not price_df.empty) or (isinstance(price_df, list) and price_df)):
                            continue
                        recs = price_df.to_dict('records') if hasattr(price_df, 'to_dict') else price_df
                        if not recs:
                            continue
                        fetched_count += len(recs)
                        inserted_count += _upsert_prices(cur, sym, recs, prices_table=db.table_prices)
                        db.connection.commit()
                    except Exception as _:
                        continue

                total_fetched += fetched_count
                total_inserted += inserted_count

                details.append({
                    'symbol': sym,
                    'anomaly_dates': anomaly_dates,
                    'deleted': deleted_count,
                    'fetched': fetched_count,
                    'inserted': inserted_count,
                })

            return jsonify({
                'success': True,
                'start': start_date,
                'end': end_date,
                'threshold': threshold,
                'symbols': symbols,
                'details': details,
                'deleted': total_deleted,
                'fetched': total_fetched,
                'inserted': total_inserted,
            })
        except Exception as e:
            try:
                db.connection.rollback()
            except Exception:
                pass
            logger.error(f"refetch_prices_range_by_anomalies 失敗: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            db.disconnect()
    except Exception as e:
        logger.error(f"refetch_prices_range_by_anomalies 外層錯誤: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/symbols', methods=['GET'])
def get_symbols():
    """獲取所有股票代碼"""
    try:
        force_refresh = request.args.get('refresh', 'false').lower() == 'true'
        symbols = stock_api.get_all_symbols(force_refresh)
        
        # 支援範圍篩選
        start_code = request.args.get('start')
        end_code = request.args.get('end')
        
        if start_code and end_code:
            try:
                start_num = int(start_code)
                end_num = int(end_code)
                filtered_symbols = []
                
                for symbol in symbols:
                    code = symbol['symbol'].split('.')[0]
                    if code.isdigit():
                        code_num = int(code)
                        if start_num <= code_num <= end_num:
                            filtered_symbols.append(symbol)
                
                symbols = filtered_symbols
            except ValueError:
                pass
        
        return jsonify({
            'success': True,
            'data': symbols,
            'count': len(symbols)
        })
    except Exception as e:
        logger.exception('批量更新失敗')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/symbols/refresh_from_exchanges', methods=['POST'])
def refresh_symbols_from_exchanges():
    """Refresh symbols table from official exchanges (TWSE/TPEx).

    Body(JSON):
      - target: 'local' | 'remote' | 'both' (default: 'both')
      - table: table name (default: 'tw_stock_symbols')
    """
    try:
        body = request.get_json(silent=True) or {}
        target = str(body.get('target') or 'both').strip().lower()
        table = str(body.get('table') or 'tw_stock_symbols').strip()
        if target not in ('local', 'remote', 'both'):
            return jsonify({'success': False, 'error': 'invalid target'}), 400

        script_path = os.path.join(os.path.dirname(__file__), 'scripts', 'seed_stock_symbols_from_exchanges.py')
        if not os.path.exists(script_path):
            return jsonify({'success': False, 'error': f'seeder script not found: {script_path}'}), 500

        def run_once(mode: str):
            env = os.environ.copy()
            env['SYMBOLS_TABLE'] = table
            if mode == 'local':
                env['FORCE_LOCAL_DB'] = '1'
            else:
                env.pop('FORCE_LOCAL_DB', None)

            if mode == 'remote':
                if not (env.get('DATABASE_URL') or env.get('NEON_DATABASE_URL')):
                    try:
                        # Align with DatabaseManager behavior: use fallback Neon URL if env not provided
                        fallback_url = DatabaseManager(use_local=False).db_url
                        if fallback_url:
                            env['NEON_DATABASE_URL'] = fallback_url
                    except Exception:
                        pass
                if not (env.get('DATABASE_URL') or env.get('NEON_DATABASE_URL')):
                    return {
                        'mode': mode,
                        'ok': False,
                        'returncode': None,
                        'stdout': '',
                        'stderr': 'NEON_DATABASE_URL not configured',
                    }

            cmd = [sys.executable, script_path]
            res = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=900)
            return {
                'mode': mode,
                'ok': res.returncode == 0,
                'returncode': res.returncode,
                'stdout': res.stdout[-20000:] if res.stdout else '',
                'stderr': res.stderr[-20000:] if res.stderr else '',
            }

        results = []
        if target == 'local':
            results.append(run_once('local'))
        elif target == 'remote':
            results.append(run_once('remote'))
        else:
            results.append(run_once('local'))
            results.append(run_once('remote'))

        ok = all(r.get('ok') for r in results)
        return jsonify({'success': ok, 'results': results})

    except subprocess.TimeoutExpired:
        return jsonify({'success': False, 'error': 'refresh timed out'}), 504
    except Exception as e:
        logger.exception('refresh symbols from exchanges failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/symbols/refresh_etf_names', methods=['POST'])
def refresh_etf_names_from_isin():
    """Refresh ETF symbols/names into symbols table from ISIN (ETF list).

    Body(JSON):
      - target: 'local' | 'remote' | 'both' (default: 'both')
      - table: table name (default: 'tw_stock_symbols')
    """
    try:
        body = request.get_json(silent=True) or {}
        target = str(body.get('target') or 'both').strip().lower()
        table = str(body.get('table') or 'tw_stock_symbols').strip()
        if target not in ('local', 'remote', 'both'):
            return jsonify({'success': False, 'error': 'invalid target'}), 400

        script_path = os.path.join(os.path.dirname(__file__), 'scripts', 'seed_etf_symbols_from_isin.py')
        if not os.path.exists(script_path):
            return jsonify({'success': False, 'error': f'seeder script not found: {script_path}'}), 500

        def run_once(mode: str):
            env = os.environ.copy()
            env['SYMBOLS_TABLE'] = table
            if mode == 'local':
                env['FORCE_LOCAL_DB'] = '1'
            else:
                env.pop('FORCE_LOCAL_DB', None)

            if mode == 'remote':
                if not (env.get('DATABASE_URL') or env.get('NEON_DATABASE_URL')):
                    try:
                        fallback_url = DatabaseManager(use_local=False).db_url
                        if fallback_url:
                            env['NEON_DATABASE_URL'] = fallback_url
                    except Exception:
                        pass
                if not (env.get('DATABASE_URL') or env.get('NEON_DATABASE_URL')):
                    return {
                        'mode': mode,
                        'ok': False,
                        'returncode': None,
                        'stdout': '',
                        'stderr': 'NEON_DATABASE_URL not configured',
                    }

            cmd = [sys.executable, script_path]
            res = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=900)
            return {
                'mode': mode,
                'ok': res.returncode == 0,
                'returncode': res.returncode,
                'stdout': res.stdout[-20000:] if res.stdout else '',
                'stderr': res.stderr[-20000:] if res.stderr else '',
            }

        results = []
        if target == 'local':
            results.append(run_once('local'))
        elif target == 'remote':
            results.append(run_once('remote'))
        else:
            results.append(run_once('local'))
            results.append(run_once('remote'))

        ok = all(r.get('ok') for r in results)
        return jsonify({'success': ok, 'results': results})

    except subprocess.TimeoutExpired:
        return jsonify({'success': False, 'error': 'refresh timed out'}), 504
    except Exception as e:
        logger.exception('refresh etf names failed')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/api/income-statement')
def api_income_statement():
    """Return wide-format income statement data for all stocks for a given year/season.

    Query params:
        year:   e.g. 2025
        season: 1-4
        code:   optional, single stock code
        code_from/code_to: optional range filter
        pause_every/pause_minutes: optional throttle controls
        retry_on_block/retry_wait_minutes/retry_max: optional resume controls
    """

    from datetime import datetime as _dt

    year = request.args.get('year')
    season = request.args.get('season')
    code = request.args.get('code')
    code_from = request.args.get('code_from')
    code_to = request.args.get('code_to')

    pause_every: Optional[int] = None
    pause_seconds: float = 0.0
    pause_every_raw = request.args.get('pause_every')
    pause_minutes_raw = request.args.get('pause_minutes')
    retry_on_block_raw = request.args.get('retry_on_block')
    retry_wait_minutes_raw = request.args.get('retry_wait_minutes')
    retry_max_raw = request.args.get('retry_max')
    try:
        if pause_every_raw is not None:
            v = int(str(pause_every_raw).strip())
            if v > 0:
                pause_every = v
    except Exception:
        pause_every = None
    try:
        if pause_minutes_raw is not None:
            mv = float(str(pause_minutes_raw).strip())
            if mv > 0:
                pause_seconds = mv * 60.0
    except Exception:
        pause_seconds = 0.0

    retry_on_block = False
    try:
        if retry_on_block_raw is not None:
            rv = str(retry_on_block_raw).strip().lower()
            retry_on_block = rv in ('1', 'true', 'yes', 'y')
    except Exception:
        retry_on_block = False

    retry_wait_seconds = 300.0
    try:
        if retry_wait_minutes_raw is not None:
            mw = float(str(retry_wait_minutes_raw).strip())
            if mw > 0:
                retry_wait_seconds = mw * 60.0
    except Exception:
        retry_wait_seconds = 300.0

    retry_max = 1
    try:
        if retry_max_raw is not None:
            rm = int(str(retry_max_raw).strip())
            if rm >= 0:
                retry_max = rm
    except Exception:
        retry_max = 1

    if pause_every and pause_seconds > 0:
        logger.info(
            "[income][throttle] enabled: pause_every=%d, pause_minutes=%.2f",
            pause_every,
            pause_seconds / 60.0,
        )

    if not year or not season:
        return jsonify({'error': 'year and season are required'}), 400

    if code:
        try:
            df_single = fetch_income_row(str(code), str(year), str(season))
        except Exception as e:
            logger.error(f"income-statement single fetch failed for {code}: {e}")
            return jsonify({'error': 'internal error fetching income statements'}), 500

        if df_single.empty:
            return jsonify([])

        data_json = df_single.to_json(orient='records', force_ascii=False)
        return Response(data_json, mimetype='application/json; charset=utf-8')

    global income_fetch_status
    income_fetch_status = {
        'running': True,
        'startedAt': _dt.utcnow().isoformat(),
        'finishedAt': None,
        'year': str(year),
        'season': str(season),
        'total': None,
        'processed': 0,
        'success_count': 0,
        'error_count': 0,
        'current_code': None,
        'error': None,
        'stopped_reason': None,
        'paused': False,
        'resumeAt': None,
        'block_count': 0,
    }

    def _income_progress_cb(idx, total, code, status, detail):
        global income_fetch_status
        st = income_fetch_status
        st['total'] = total
        st['processed'] = max(int(st.get('processed') or 0), int(idx))
        st['current_code'] = code
        if status == 'success':
            st['success_count'] = int(st.get('success_count') or 0) + 1
        elif status == 'error':
            st['error_count'] = int(st.get('error_count') or 0) + 1
            # 只記錄最後一個錯誤訊息即可
            if detail:
                st['error'] = str(detail)
                low = str(detail).lower()
                if (
                    'mops' in low
                    or 'blocked' in low
                    or 'too many requests' in low
                    or 'captcha' in low
                    or '驗證' in str(detail)
                    or '封鎖' in str(detail)
                ):
                    st['stopped_reason'] = 'mops_blocked'

    write_raw = request.args.get('write_to_db') or request.args.get('import_db')
    write_to_db = False
    if write_raw is not None:
        v = str(write_raw).strip().lower()
        write_to_db = v in ('1', 'true', 'yes', 'y')

    db = None
    cursor = None
    insert_sql = None
    buffer_values = []
    batch_size = 1
    metric_cols = list(TARGET_ORDER)
    inserted_rows = 0
    batches_committed = 0

    def _to_num_for_income(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).replace(',', '').strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None

    row_cb = None
    if write_to_db:
        try:
            logger.info(
                "[income][db-stream] write_to_db enabled for year=%s season=%s",
                year,
                season,
            )
            db = DatabaseManager.from_request_args(request.args)
            if not db.connect():
                logger.error("[income][db-stream] 資料庫連線失敗")
                return jsonify({'error': '資料庫連線失敗'}), 500
            logger.info("[income][db-stream] connected to DB: %s", db.connection_info())
            if not db.create_tables():
                logger.error("[income][db-stream] 資料庫初始化失敗 (create_tables 返回 False)")
                return jsonify({'error': '資料庫初始化失敗'}), 500
            cursor = db.connection.cursor()
            table_name = getattr(db, 'table_income', income_statement_table(use_neon=db.is_neon))
            insert_cols = ['"股票代號"', 'period'] + [f'"{c}"' for c in metric_cols]
            insert_cols_sql = ", ".join(insert_cols)
            update_assignments = ", ".join([
                f'"{col}" = EXCLUDED."{col}"' for col in metric_cols
            ])
            insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols_sql})
                VALUES %s
                ON CONFLICT ("股票代號", period) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """

            def _row_cb(code_for_cb, row_df):
                nonlocal buffer_values, inserted_rows, batches_committed
                if row_df is None or row_df.empty:
                    return
                recs = row_df.to_dict(orient='records')
                if not recs:
                    return
                logger.info(
                    "[income][db-stream] buffering %d row(s) for stock=%s period=%s",
                    len(recs),
                    code_for_cb,
                    recs[0].get('period'),
                )
                for rec in recs:
                    stock_code = (rec.get('股票代號') or '').strip()
                    period_val = str(rec.get('period') or '').strip()
                    if not stock_code or not period_val:
                        continue
                    row_vals = [stock_code, period_val]
                    for col in metric_cols:
                        row_vals.append(_to_num_for_income(rec.get(col)))
                    buffer_values.append(tuple(row_vals))
                if buffer_values and len(buffer_values) >= batch_size:
                    batch_len = len(buffer_values)
                    logger.info(
                        "[income][db-batch] inserting %d buffered row(s) into %s",
                        batch_len,
                        table_name,
                    )
                    execute_values(cursor, insert_sql, buffer_values, page_size=batch_size)
                    db.connection.commit()
                    inserted_rows += batch_len
                    batches_committed += 1
                    logger.info(
                        "[income][db-batch] commit done (total_inserted=%d, batches=%d)",
                        inserted_rows,
                        batches_committed,
                    )
                    buffer_values.clear()

            row_cb = _row_cb
        except Exception as db_exc:
            logger.error(f"income-statement db init error: {db_exc}")
            if db is not None:
                try:
                    db.disconnect()
                except Exception:
                    pass
            return jsonify({'error': '資料庫初始化失敗'}), 500

    try:
        import time as _time
        from datetime import timedelta as _td
        from income_statement_service import MopsBlockedError as _IncomeMopsBlockedError

        df_parts = []
        resume_from_code = None
        block_count = 0

        while True:
            try:
                df_part = fetch_all_incomes(
                    str(year),
                    str(season),
                    progress_cb=_income_progress_cb,
                    row_cb=row_cb,
                    code_from=(resume_from_code or code_from),
                    code_to=code_to,
                    pause_every=pause_every,
                    pause_seconds=pause_seconds,
                    raise_on_block=bool(retry_on_block),
                )
                if df_part is not None and not df_part.empty:
                    df_parts.append(df_part)
                break
            except _IncomeMopsBlockedError as e:
                if (not retry_on_block) or (block_count >= retry_max):
                    raise

                block_count += 1
                resume_from_code = income_fetch_status.get('current_code') or resume_from_code
                try:
                    income_fetch_status['block_count'] = int(block_count)
                except Exception:
                    pass

                if write_to_db and db is not None and cursor is not None and buffer_values:
                    batch_len = len(buffer_values)
                    logger.info(
                        "[income][db-batch] flushing %d buffered row(s) before retry",
                        batch_len,
                    )
                    execute_values(cursor, insert_sql, buffer_values, page_size=batch_size)
                    db.connection.commit()
                    inserted_rows += batch_len
                    batches_committed += 1
                    buffer_values.clear()

                try:
                    income_fetch_status['paused'] = True
                    income_fetch_status['resumeAt'] = (_dt.utcnow() + _td(seconds=retry_wait_seconds)).isoformat()
                    income_fetch_status['error'] = str(e)
                    income_fetch_status['stopped_reason'] = 'mops_blocked'
                except Exception:
                    pass

                _time.sleep(retry_wait_seconds)

                try:
                    income_fetch_status['paused'] = False
                    income_fetch_status['resumeAt'] = None
                except Exception:
                    pass
                continue

        if df_parts:
            non_empty = [d for d in df_parts if d is not None and not d.empty]
            if non_empty:
                df = pd.concat(non_empty, ignore_index=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        if write_to_db and db is not None and cursor is not None and buffer_values:
            batch_len = len(buffer_values)
            logger.info(
                "[income][db-final] flushing last %d buffered row(s) into %s",
                batch_len,
                table_name,
            )
            execute_values(cursor, insert_sql, buffer_values, page_size=batch_size)
            db.connection.commit()
            inserted_rows += batch_len
            batches_committed += 1
            logger.info(
                "[income][db-final] commit done (total_inserted=%d, batches=%d)",
                inserted_rows,
                batches_committed,
            )
            buffer_values.clear()

        income_fetch_status['running'] = False
        income_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
    except Exception as e:
        from income_statement_service import MopsBlockedError as _IncomeMopsBlockedError
        if isinstance(e, _IncomeMopsBlockedError):
            logger.warning(f"income-statement blocked by MOPS: {e}")
            income_fetch_status['running'] = False
            income_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
            income_fetch_status['error'] = str(e)
            income_fetch_status['stopped_reason'] = 'mops_blocked'
            if write_to_db and db is not None:
                try:
                    if db.connection is not None:
                        db.connection.rollback()
                except Exception:
                    pass
            if write_to_db and db is not None:
                try:
                    db.disconnect()
                except Exception:
                    pass
            return (
                jsonify(
                    {
                        'error': (
                            'MOPS/TWSE 顯示「因安全性考量無法存取」頁面，可能已觸發防護機制。'
                            '請降低抓取頻率（提高 pause_every / pause_minutes）、縮小 code_from/code_to 範圍，'
                            '或稍後再試。'
                        )
                    }
                ),
                429,
            )
        logger.error(f"income-statement fetch failed: {e}")
        income_fetch_status['running'] = False
        income_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
        income_fetch_status['error'] = str(e)
        if income_fetch_status.get('stopped_reason') is None:
            income_fetch_status['stopped_reason'] = 'server_error'
        if write_to_db and db is not None:
            try:
                if db.connection is not None:
                    db.connection.rollback()
            except Exception:
                pass
        if write_to_db and db is not None:
            try:
                db.disconnect()
            except Exception:
                pass

        msg = str(e)
        low = msg.lower()
        if (
            'connection reset' in low
            or 'connection aborted' in low
            or 'connectionerror' in low
            or 'read timed out' in low
            or 'timed out' in low
            or 'isin' in low
        ):
            return (
                jsonify(
                    {
                        'error': (
                            '抓取股票清單（ISIN）時網路連線被重設/逾時，請稍後再試。\n'
                            '若持續發生，可降低抓取頻率或先確認網路/DNS 是否穩定。'
                        )
                    }
                ),
                503,
            )

        return jsonify({'error': 'internal error fetching income statements'}), 500
    finally:
        if write_to_db and db is not None:
            try:
                logger.info(
                    "[income][db-summary] year=%s season=%s write_to_db=%s total_inserted=%d batches=%d",
                    year,
                    season,
                    write_to_db,
                    inserted_rows,
                    batches_committed,
                )
            except Exception:
                pass
            try:
                db.disconnect()
            except Exception:
                pass

    if df.empty:
        return jsonify([])

    meta = dict(income_fetch_status or {})
    meta['returned_rows'] = int(getattr(df, 'shape', [0])[0] or 0)
    meta['write_to_db'] = bool(write_to_db)
    if write_to_db:
        meta['db_inserted_rows'] = int(inserted_rows)
        meta['db_batches'] = int(batches_committed)

    include_meta_raw = request.args.get('include_meta') or request.args.get('with_meta')
    include_meta = False
    if include_meta_raw is not None:
        include_meta = str(include_meta_raw).strip().lower() in ('1', 'true', 'yes', 'y')

    if include_meta:
        payload = {
            'meta': meta,
            'data': df.to_dict(orient='records'),
        }
        resp = jsonify(payload)
    else:
        data_json = df.to_json(orient='records', force_ascii=False)
        resp = Response(data_json, mimetype='application/json; charset=utf-8')

    try:
        resp.headers['X-Income-Year'] = str(year)
        resp.headers['X-Income-Season'] = str(season)
        resp.headers['X-Income-Total'] = '' if meta.get('total') is None else str(meta.get('total'))
        resp.headers['X-Income-Processed'] = str(meta.get('processed') or 0)
        resp.headers['X-Income-Success'] = str(meta.get('success_count') or 0)
        resp.headers['X-Income-Errors'] = str(meta.get('error_count') or 0)
        resp.headers['X-Income-Stopped-Reason'] = '' if meta.get('stopped_reason') is None else str(meta.get('stopped_reason'))
        resp.headers['X-Income-Last-Error'] = '' if meta.get('error') is None else str(meta.get('error'))[:500]
        resp.headers['X-Income-Returned-Rows'] = str(meta.get('returned_rows') or 0)
        if write_to_db:
            resp.headers['X-Income-DB-Inserted-Rows'] = str(meta.get('db_inserted_rows') or 0)
            resp.headers['X-Income-DB-Batches'] = str(meta.get('db_batches') or 0)
    except Exception:
        pass

    return resp


@app.get('/api/balance-sheet')
def api_balance_sheet():
    """Return wide-format balance sheet data for all stocks for a given year/season.

    Query params:
        year:   e.g. 2025
        season: 1-4
        code:   optional, single stock code
        code_from/code_to: optional range filter
        pause_every/pause_minutes: optional throttle controls
        retry_on_block/retry_wait_minutes/retry_max: optional resume controls
    """

    from datetime import datetime as _dt

    year = request.args.get('year')
    season = request.args.get('season')
    code = request.args.get('code')
    code_from = request.args.get('code_from')
    code_to = request.args.get('code_to')

    pause_every: Optional[int] = None
    pause_seconds: float = 0.0
    pause_every_raw = request.args.get('pause_every')
    pause_minutes_raw = request.args.get('pause_minutes')
    retry_on_block_raw = request.args.get('retry_on_block')
    retry_wait_minutes_raw = request.args.get('retry_wait_minutes')
    retry_max_raw = request.args.get('retry_max')
    try:
        if pause_every_raw is not None:
            v = int(str(pause_every_raw).strip())
            if v > 0:
                pause_every = v
    except Exception:
        pause_every = None
    try:
        if pause_minutes_raw is not None:
            mv = float(str(pause_minutes_raw).strip())
            if mv > 0:
                pause_seconds = mv * 60.0
    except Exception:
        pause_seconds = 0.0

    retry_on_block = False
    try:
        if retry_on_block_raw is not None:
            rv = str(retry_on_block_raw).strip().lower()
            retry_on_block = rv in ('1', 'true', 'yes', 'y')
    except Exception:
        retry_on_block = False

    retry_wait_seconds = 300.0
    try:
        if retry_wait_minutes_raw is not None:
            mw = float(str(retry_wait_minutes_raw).strip())
            if mw > 0:
                retry_wait_seconds = mw * 60.0
    except Exception:
        retry_wait_seconds = 300.0

    retry_max = 1
    try:
        if retry_max_raw is not None:
            rm = int(str(retry_max_raw).strip())
            if rm >= 0:
                retry_max = rm
    except Exception:
        retry_max = 1

    if pause_every and pause_seconds > 0:
        logger.info(
            "[balance][throttle] enabled: pause_every=%d, pause_minutes=%.2f",
            pause_every,
            pause_seconds / 60.0,
        )

    if not year or not season:
        return jsonify({'error': 'year and season are required'}), 400

    if code:
        try:
            df_single = fetch_balance_sheet_row(str(code), str(year), str(season))
        except BalanceMopsBlockedError as e:
            logger.warning(f"balance-sheet blocked by MOPS for {code}: {e}")
            return (
                jsonify(
                    {
                        'error': (
                            'MOPS/TWSE 顯示「因安全性考量無法存取」頁面，可能已觸發防護機制。'
                            '請降低抓取頻率（提高 pause_every / pause_minutes）、縮小 code_from/code_to 範圍，'
                            '或稍後再試。'
                        )
                    }
                ),
                429,
            )
        except Exception as e:
            logger.error(f"balance-sheet single fetch failed for {code}: {e}")
            return jsonify({'error': 'internal error fetching balance sheet'}), 500

        if df_single.empty:
            return jsonify([])

        data_json = df_single.to_json(orient='records', force_ascii=False)
        return Response(data_json, mimetype='application/json; charset=utf-8')

    # 先解析是否啟用資料庫寫入，再初始化進度狀態
    write_raw = request.args.get('write_to_db') or request.args.get('import_db')
    write_to_db = False
    if write_raw is not None:
        v = str(write_raw).strip().lower()
        write_to_db = v in ('1', 'true', 'yes', 'y')

    global balance_fetch_status
    balance_fetch_status = {
        'running': True,
        'startedAt': _dt.utcnow().isoformat(),
        'finishedAt': None,
        'year': str(year),
        'season': str(season),
        'total': None,
        'processed': 0,
        'success_count': 0,
        'error_count': 0,
        'current_code': None,
        'error': None,
        'db_write_enabled': bool(write_to_db),
        'db_inserted_rows': 0,
        'db_batches_committed': 0,
        'db_last_commit_at': None,
        'paused': False,
        'resumeAt': None,
        'block_count': 0,
    }

    def _balance_progress_cb(idx, total, code_for_cb, status, detail):
        global balance_fetch_status
        st = balance_fetch_status
        st['total'] = total
        st['processed'] = max(int(st.get('processed') or 0), int(idx))
        st['current_code'] = code_for_cb
        if status == 'success':
            st['success_count'] = int(st.get('success_count') or 0) + 1
        elif status == 'error':
            st['error_count'] = int(st.get('error_count') or 0) + 1
            if detail:
                st['error'] = str(detail)

    db = None
    insert_sql = None
    buffer_values = []
    batch_size = 1
    metric_cols = list(BALANCE_TARGET_ORDER)
    inserted_rows = 0
    batches_committed = 0

    def _ensure_db_connection():
        nonlocal db
        if db is None:
            raise RuntimeError('db not initialized')
        conn = getattr(db, 'connection', None)
        closed = getattr(conn, 'closed', 1) if conn is not None else 1
        if conn is None or closed:
            try:
                db.disconnect()
            except Exception:
                pass
            if not db.connect():
                raise RuntimeError('資料庫連線失敗')
            try:
                db.create_tables()
            except Exception:
                pass

    def _flush_balance_buffer():
        nonlocal buffer_values, inserted_rows, batches_committed
        if not buffer_values:
            return
        if db is None or insert_sql is None:
            return
        _ensure_db_connection()
        batch_len = len(buffer_values)
        table_name = getattr(db, 'table_balance', balance_sheet_table(use_neon=db.is_neon))
        logger.info(
            "[balance][db-batch] inserting %d buffered row(s) into %s",
            batch_len,
            table_name,
        )
        with db.connection.cursor() as _cur:
            execute_values(_cur, insert_sql, buffer_values, page_size=batch_size)
        db.connection.commit()
        inserted_rows += batch_len
        batches_committed += 1
        try:
            balance_fetch_status['db_inserted_rows'] = int(inserted_rows)
            balance_fetch_status['db_batches_committed'] = int(batches_committed)
            balance_fetch_status['db_last_commit_at'] = _dt.utcnow().isoformat()
        except Exception:
            pass
        logger.info(
            "[balance][db-batch] commit done (total_inserted=%d, batches=%d)",
            inserted_rows,
            batches_committed,
        )
        buffer_values.clear()

    def _to_num_for_balance(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).replace(',', '').strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None

    row_cb = None
    if write_to_db:
        try:
            logger.info(
                "[balance][db-stream] write_to_db enabled for year=%s season=%s",
                year,
                season,
            )
            db = DatabaseManager.from_request_args(request.args)
            if not db.connect():
                logger.error("[balance][db-stream] 資料庫連線失敗")
                return jsonify({'error': '資料庫連線失敗'}), 500
            logger.info("[balance][db-stream] connected to DB: %s", db.connection_info())
            if not db.create_tables():
                logger.error("[balance][db-stream] 資料庫初始化失敗 (create_tables 返回 False)")
                return jsonify({'error': '資料庫初始化失敗'}), 500
            table_name = getattr(db, 'table_balance', balance_sheet_table(use_neon=db.is_neon))
            insert_cols = ['"股票代號"', 'period'] + [f'"{c}"' for c in metric_cols]
            insert_cols_sql = ", ".join(insert_cols)
            update_assignments = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in metric_cols])
            insert_sql = f"""
                INSERT INTO {table_name} ({insert_cols_sql})
                VALUES %s
                ON CONFLICT ("股票代號", period) DO UPDATE SET
                    {update_assignments},
                    updated_at = CURRENT_TIMESTAMP
            """

            def _row_cb(code_for_cb, row_df):
                nonlocal buffer_values, inserted_rows, batches_committed
                if row_df is None or row_df.empty:
                    return
                recs = row_df.to_dict(orient='records')
                if not recs:
                    return
                logger.info(
                    "[balance][db-stream] buffering %d row(s) for stock=%s period=%s",
                    len(recs),
                    code_for_cb,
                    recs[0].get('period'),
                )
                for rec in recs:
                    stock_code = (rec.get('股票代號') or '').strip()
                    period_val = str(rec.get('period') or '').strip()
                    if not stock_code or not period_val:
                        continue
                    row_vals = [stock_code, period_val]
                    for col in metric_cols:
                        row_vals.append(_to_num_for_balance(rec.get(col)))
                    buffer_values.append(tuple(row_vals))

                if buffer_values and len(buffer_values) >= batch_size:
                    _flush_balance_buffer()

            row_cb = _row_cb
        except Exception as db_exc:
            logger.error(f"balance-sheet db init error: {db_exc}")
            if db is not None:
                try:
                    db.disconnect()
                except Exception:
                    pass
            return jsonify({'error': '資料庫初始化失敗'}), 500

    df_parts = []
    resume_from_code = None
    block_count = 0

    total_codes = None
    code_index = None
    if retry_on_block:
        try:
            from income_statement_service import fetch_all_stock_codes as _fetch_all_stock_codes

            codes_all = _fetch_all_stock_codes()
            cf0 = str(code_from).strip() if code_from else None
            ct0 = str(code_to).strip() if code_to else None
            if cf0 and ct0 and cf0 > ct0:
                cf0, ct0 = ct0, cf0
            if cf0 is not None or ct0 is not None:
                filtered = []
                for c in codes_all:
                    if cf0 and c < cf0:
                        continue
                    if ct0 and c > ct0:
                        continue
                    filtered.append(c)
                codes_all = filtered
            total_codes = len(codes_all)
            code_index = {c: i for i, c in enumerate(codes_all, 1)}
        except Exception:
            total_codes = None
            code_index = None

    try:
        import time as _time
        from datetime import timedelta as _td

        while True:
            offset = 0
            if total_codes is not None and code_index is not None and resume_from_code:
                try:
                    offset = int(code_index.get(str(resume_from_code)) or 0)
                except Exception:
                    offset = 0

            def _progress_cb(idx, total_inner, code_for_cb, status, detail):
                if total_codes is not None:
                    _balance_progress_cb(offset + int(idx), int(total_codes), code_for_cb, status, detail)
                else:
                    _balance_progress_cb(idx, total_inner, code_for_cb, status, detail)

            try:
                df_part = fetch_all_balance_sheets(
                    str(year),
                    str(season),
                    progress_cb=_progress_cb,
                    row_cb=row_cb,
                    code_from=(resume_from_code or code_from),
                    code_to=code_to,
                    resume_after=bool(resume_from_code),
                    pause_every=pause_every,
                    pause_seconds=pause_seconds,
                )
                if df_part is not None and not df_part.empty:
                    df_parts.append(df_part)
                break
            except BalanceMopsBlockedError as e:
                if (not retry_on_block) or (block_count >= retry_max):
                    raise

                block_count += 1
                resume_from_code = balance_fetch_status.get('current_code') or resume_from_code
                try:
                    balance_fetch_status['block_count'] = int(block_count)
                except Exception:
                    pass

                if write_to_db and db is not None and buffer_values:
                    _flush_balance_buffer()

                try:
                    balance_fetch_status['paused'] = True
                    balance_fetch_status['resumeAt'] = (_dt.utcnow() + _td(seconds=retry_wait_seconds)).isoformat()
                    balance_fetch_status['error'] = str(e)
                except Exception:
                    pass

                _time.sleep(retry_wait_seconds)

                try:
                    balance_fetch_status['paused'] = False
                    balance_fetch_status['resumeAt'] = None
                except Exception:
                    pass

                continue

        if df_parts:
            non_empty = [d for d in df_parts if d is not None and not d.empty]
            if non_empty:
                df = pd.concat(non_empty, ignore_index=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()

        if write_to_db and db is not None and buffer_values:
            _flush_balance_buffer()

        balance_fetch_status['running'] = False
        balance_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
    except BalanceMopsBlockedError as e:
        logger.warning(f"balance-sheet blocked by MOPS: {e}")
        balance_fetch_status['running'] = False
        balance_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
        balance_fetch_status['error'] = str(e)
        if write_to_db and db is not None:
            try:
                if db.connection is not None:
                    db.connection.rollback()
            except Exception:
                pass
        if write_to_db and db is not None:
            try:
                db.disconnect()
            except Exception:
                pass
        return (
            jsonify(
                {
                    'error': (
                        'MOPS/TWSE 顯示「因安全性考量無法存取」頁面，可能已觸發防護機制。'
                        '請降低抓取頻率（提高 pause_every / pause_minutes）、縮小 code_from/code_to 範圍，'
                        '或稍後再試。'
                    )
                }
            ),
            429,
        )
    except Exception as e:
        logger.exception("balance-sheet fetch failed")
        balance_fetch_status['running'] = False
        balance_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
        balance_fetch_status['error'] = str(e)
        if write_to_db and db is not None:
            try:
                if db.connection is not None:
                    db.connection.rollback()
            except Exception:
                pass
        if write_to_db and db is not None:
            try:
                db.disconnect()
            except Exception:
                pass
        return jsonify({'error': f'internal error fetching balance sheet: {e}'}), 500

    if write_to_db and db is not None:
        try:
            logger.info(
                "[balance][db-summary] year=%s season=%s write_to_db=%s total_inserted=%d batches=%d",
                year,
                season,
                write_to_db,
                inserted_rows,
                batches_committed,
            )
        except Exception:
            pass
        try:
            db.disconnect()
        except Exception:
            pass

    if df.empty:
        return jsonify([])

    data_json = df.to_json(orient='records', force_ascii=False)
    return Response(data_json, mimetype='application/json; charset=utf-8')


@app.get('/api/balance-sheet/status')
def api_balance_sheet_status():
    """Return current progress status of balance-sheet fetching."""

    try:
        return jsonify({'success': True, 'status': balance_fetch_status})
    except Exception as e:
        logger.error(f"balance-sheet status error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.get('/api/cash-flow-statement')
def api_cash_flow_statement():
    """抓取單一股票、代號範圍或全市場的現金流量表。"""
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    import time as _time

    year = request.args.get('year')
    season = request.args.get('season')
    code = (request.args.get('code') or '').strip()
    code_from = (request.args.get('code_from') or '').strip() or None
    code_to = (request.args.get('code_to') or '').strip() or None
    if not year or not season:
        return jsonify({'error': 'year and season are required'}), 400
    try:
        if int(season) not in (1, 2, 3, 4):
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({'error': 'season must be 1-4'}), 400

    def _bool_arg(name, default=False):
        value = request.args.get(name)
        if value is None:
            return default
        return str(value).strip().lower() in ('1', 'true', 'yes', 'y')

    def _int_arg(name, default, minimum=0):
        try:
            return max(minimum, int(str(request.args.get(name, default)).strip()))
        except (TypeError, ValueError):
            return default

    def _float_arg(name, default, minimum=0.0):
        try:
            return max(minimum, float(str(request.args.get(name, default)).strip()))
        except (TypeError, ValueError):
            return default

    if code:
        try:
            frame = fetch_cash_flow_row(code, str(year), str(season))
            if frame.empty:
                return jsonify([])
            return Response(
                frame.to_json(orient='records', force_ascii=False),
                mimetype='application/json; charset=utf-8',
            )
        except CashFlowMopsBlockedError:
            return jsonify({'error': 'MOPS/TWSE 暫時封鎖存取，請稍後再試。'}), 429
        except Exception as exc:
            logger.exception("cash-flow single fetch failed")
            return jsonify({'error': f'internal error fetching cash flow: {exc}'}), 500

    pause_every = _int_arg('pause_every', 0) or None
    pause_seconds = _float_arg('pause_minutes', 0.0) * 60
    retry_on_block = _bool_arg('retry_on_block')
    retry_wait_seconds = _float_arg('retry_wait_minutes', 5.0, 0.01) * 60
    retry_max = _int_arg('retry_max', 1)
    write_to_db = _bool_arg('write_to_db') or _bool_arg('import_db')

    global cash_flow_fetch_status
    cash_flow_fetch_status = {
        'running': True,
        'startedAt': _dt.now(_tz.utc).isoformat(),
        'finishedAt': None,
        'year': str(year),
        'season': str(season),
        'total': None,
        'processed': 0,
        'success_count': 0,
        'error_count': 0,
        'current_code': None,
        'error': None,
        'db_write_enabled': write_to_db,
        'db_inserted_rows': 0,
        'paused': False,
        'resumeAt': None,
        'block_count': 0,
    }

    collected: dict[tuple[str, str], dict] = {}
    db = None
    insert_sql = None

    def _to_num(value):
        if value is None:
            return None
        try:
            return float(str(value).replace(',', '').strip())
        except (TypeError, ValueError):
            return None

    try:
        if write_to_db:
            db = DatabaseManager.from_request_args(request.args)
            if not db.connect() or not db.create_tables():
                raise RuntimeError('資料庫初始化失敗')
            table_name = getattr(db, 'table_cash_flow', cash_flow_table(use_neon=db.is_neon))
            columns = ['"股票代號"', 'period'] + [
                f'"{column}"' for column in CASH_FLOW_TARGET_ORDER
            ]
            updates = ", ".join(
                f'"{column}" = EXCLUDED."{column}"'
                for column in CASH_FLOW_TARGET_ORDER
            )
            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES %s
                ON CONFLICT ("股票代號", period) DO UPDATE SET
                    {updates}, updated_at = CURRENT_TIMESTAMP
            """

        def _row_cb(_code, row_frame):
            if row_frame is None or row_frame.empty:
                return
            for record in row_frame.to_dict(orient='records'):
                key = (str(record.get('股票代號') or ''), str(record.get('period') or ''))
                if not all(key):
                    continue
                collected[key] = record
                if db is not None and insert_sql is not None:
                    values = tuple(
                        [key[0], key[1]]
                        + [_to_num(record.get(column)) for column in CASH_FLOW_TARGET_ORDER]
                    )
                    with db.connection.cursor() as cursor:
                        execute_values(cursor, insert_sql, [values], page_size=1)
                    db.connection.commit()
                    cash_flow_fetch_status['db_inserted_rows'] = (
                        int(cash_flow_fetch_status.get('db_inserted_rows') or 0) + 1
                    )

        from income_statement_service import fetch_all_stock_codes as _fetch_codes
        all_codes = _fetch_codes()
        if code_from or code_to:
            lower, upper = code_from, code_to
            if lower and upper and lower > upper:
                lower, upper = upper, lower
            all_codes = [
                item for item in all_codes
                if (not lower or item >= lower) and (not upper or item <= upper)
            ]
        code_index = {item: index for index, item in enumerate(all_codes, 1)}
        total_codes = len(all_codes)
        resume_from = None
        block_count = 0

        while True:
            offset = code_index.get(resume_from, 0) if resume_from else 0

            def _progress_cb(index, _total, current_code, status, detail):
                state = cash_flow_fetch_status
                state['total'] = total_codes
                state['processed'] = min(total_codes, offset + int(index))
                state['current_code'] = current_code
                if status == 'success':
                    state['success_count'] = len(collected)
                elif status == 'error':
                    state['error_count'] = int(state.get('error_count') or 0) + 1
                    state['error'] = detail

            try:
                fetch_all_cash_flows(
                    str(year),
                    str(season),
                    progress_cb=_progress_cb,
                    row_cb=_row_cb,
                    code_from=(resume_from or code_from),
                    code_to=code_to,
                    resume_after=bool(resume_from),
                    pause_every=pause_every,
                    pause_seconds=pause_seconds,
                )
                break
            except CashFlowMopsBlockedError as exc:
                if not retry_on_block or block_count >= retry_max:
                    raise
                block_count += 1
                resume_from = cash_flow_fetch_status.get('current_code') or resume_from
                cash_flow_fetch_status.update({
                    'paused': True,
                    'resumeAt': (_dt.now(_tz.utc) + _td(seconds=retry_wait_seconds)).isoformat(),
                    'block_count': block_count,
                    'error': str(exc),
                })
                _time.sleep(retry_wait_seconds)
                cash_flow_fetch_status.update({'paused': False, 'resumeAt': None})

        cash_flow_fetch_status.update({
            'running': False,
            'finishedAt': _dt.now(_tz.utc).isoformat(),
            'processed': total_codes,
            'success_count': len(collected),
            'current_code': None,
            'error': None,
        })
        return jsonify(list(collected.values()))
    except CashFlowMopsBlockedError as exc:
        cash_flow_fetch_status.update({
            'running': False,
            'finishedAt': _dt.now(_tz.utc).isoformat(),
            'error': str(exc),
        })
        return jsonify({'error': 'MOPS/TWSE 暫時封鎖存取，請降低頻率或稍後再試。'}), 429
    except Exception as exc:
        logger.exception("cash-flow fetch failed")
        cash_flow_fetch_status.update({
            'running': False,
            'finishedAt': _dt.now(_tz.utc).isoformat(),
            'error': str(exc),
        })
        return jsonify({'error': f'internal error fetching cash flow: {exc}'}), 500
    finally:
        if db is not None:
            db.disconnect()


@app.get('/api/cash-flow-statement/status')
def api_cash_flow_statement_status():
    return jsonify({'success': True, 'status': cash_flow_fetch_status})


@app.get('/api/income-statement/status')
def api_income_statement_status():
    """Return current progress status of income-statement fetching."""

    try:
        return jsonify({'success': True, 'status': income_fetch_status})
    except Exception as e:
        logger.error(f"income-statement status error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

# Debug routes
@app.route('/api/debug/routes', methods=['GET'])
def list_routes():
    try:
        routes = []
        for rule in app.url_map.iter_rules():
            methods = sorted([m for m in rule.methods if m not in ('HEAD', 'OPTIONS')])
            routes.append({
                'rule': str(rule),
                'endpoint': rule.endpoint,
                'methods': methods
            })
        return jsonify({'success': True, 'count': len(routes), 'routes': routes})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _is_safe_identifier(name: str) -> bool:
    if not name:
        return False
    try:
        return re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', str(name)) is not None
    except Exception:
        return False


def _resolve_table_override(cursor, table_name: str) -> str:
    if not _is_safe_identifier(table_name):
        raise ValueError('invalid table name')
    cursor.execute(
        """
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        """,
        [table_name],
    )
    row = cursor.fetchone()
    cnt = 0
    if isinstance(row, dict):
        cnt = int(row.get('cnt') or 0)
    elif isinstance(row, (list, tuple)) and row:
        cnt = int(row[0] or 0)
    if cnt <= 0:
        raise ValueError('table not found')
    return table_name


@app.route('/api/tables', methods=['GET'])
def list_tables_for_query():
    try:
        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500
        try:
            cursor = db_manager.connection.cursor()
            cursor.execute(
                """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY tablename
                """
            )
            rows = cursor.fetchall() or []
            tables = []
            for r in rows:
                if isinstance(r, dict):
                    name = r.get('tablename')
                elif isinstance(r, (list, tuple)) and r:
                    name = r[0]
                else:
                    name = None
                if name:
                    tables.append({'name': str(name)})
            return jsonify({'success': True, 'tables': tables})
        finally:
            db_manager.disconnect()
    except Exception as e:
        logger.error(f"list_tables_for_query failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/query/table', methods=['GET'])
def query_table_generic():
    try:
        table = request.args.get('table')
        if not table:
            return jsonify({'success': False, 'error': 'missing table'}), 400

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        symbol = request.args.get('symbol')
        start_date = request.args.get('start')
        end_date = request.args.get('end')

        try:
            limit = int(limit) if limit not in (None, '', 'null') else 200
        except Exception:
            limit = 200
        try:
            offset = int(offset) if offset not in (None, '', 'null') else 0
        except Exception:
            offset = 0
        limit = max(1, min(int(limit), 2000))
        offset = max(0, int(offset))

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500
        try:
            cursor = db_manager.connection.cursor()
            table_name = _resolve_table_override(cursor, str(table).strip())

            cursor.execute(
                """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position
                """,
                [table_name],
            )
            col_rows = cursor.fetchall() or []
            columns = []
            for r in col_rows:
                if isinstance(r, dict):
                    c = r.get('column_name')
                elif isinstance(r, (list, tuple)) and r:
                    c = r[0]
                else:
                    c = None
                if c:
                    columns.append(str(c))

            if not columns:
                return jsonify({'success': False, 'error': 'table has no columns'}), 400

            where_parts = [sql.SQL('TRUE')]
            params = []

            if symbol and 'symbol' in columns:
                symbol_list = [s.strip() for s in str(symbol).split(',') if s.strip()]
                if len(symbol_list) == 1:
                    where_parts.append(sql.SQL('symbol = %s'))
                    params.append(symbol_list[0])
                elif len(symbol_list) > 1:
                    where_parts.append(sql.SQL('symbol = ANY(%s)'))
                    params.append(symbol_list)

            if start_date and 'date' in columns:
                where_parts.append(sql.SQL('date >= %s'))
                params.append(start_date)
            if end_date and 'date' in columns:
                where_parts.append(sql.SQL('date <= %s'))
                params.append(end_date)

            query = sql.SQL('SELECT * FROM {} WHERE ').format(sql.Identifier(table_name))
            query = sql.Composed([query, sql.SQL(' AND ').join(where_parts)])

            if 'date' in columns:
                query = sql.Composed([query, sql.SQL(' ORDER BY date DESC')])

            query = sql.Composed([query, sql.SQL(' LIMIT %s OFFSET %s')])
            params.extend([limit, offset])

            cursor.execute(query, params)
            rows = cursor.fetchall() or []

            # Normalize rows to list[dict]
            out_rows = []
            for r in rows:
                if isinstance(r, dict):
                    out_rows.append(r)
                elif isinstance(r, (list, tuple)):
                    out_rows.append({columns[i]: r[i] if i < len(r) else None for i in range(len(columns))})
                else:
                    out_rows.append({'value': r})

            # JSON-safe date conversion
            for rr in out_rows:
                if not isinstance(rr, dict):
                    continue
                for k, v in list(rr.items()):
                    if isinstance(v, (datetime, date)):
                        rr[k] = v.strftime('%Y-%m-%d')
            return jsonify({
                'success': True,
                'table': table_name,
                'columns': columns,
                'rows': out_rows,
                'count': len(out_rows),
                'limit': limit,
                'offset': offset,
            })
        finally:
            db_manager.disconnect()
    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('query_table_generic failed')
        return jsonify({'success': False, 'error': str(e)}), 500


# 權證匯入進度狀態（提供前端查詢進度用）
income_fetch_status = {
    'running': False,
    'startedAt': None,
    'finishedAt': None,
    'year': None,
    'season': None,
    'total': None,
    'processed': 0,
    'success_count': 0,
    'error_count': 0,
    'current_code': None,
    'error': None,
}

tpex_warrant_master_import_status = {
    'running': False,
    'startedAt': None,
    'finishedAt': None,
    'total': None,
    'processed': 0,
    'importedCount': 0,
    'error': None,
}

tpex_warrant_daily_import_status = {
    'running': False,
    'startedAt': None,
    'finishedAt': None,
    'total': None,
    'processed': 0,
    'importedCount': 0,
    'tradeDate': None,
    'error': None,
}


def _parse_roc_date_text(text: str | None):
    if not text:
        return None
    s = str(text).strip()
    if not s.isdigit() or len(s) != 7:
        return None
    try:
        roc_year = int(s[:3])
        month = int(s[3:5])
        day = int(s[5:7])
        year = roc_year + 1911
        return date(year, month, day)
    except Exception:
        return None


def _to_decimal_or_none(val):
    if val is None:
        return None
    s = str(val).replace(',', '').strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_int_or_none(val):
    num = _to_decimal_or_none(val)
    if num is None:
        return None
    try:
        return int(num)
    except Exception:
        return None


def _fetch_json_list(url: str):
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f'API 狀態碼 {resp.status_code}')
    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f'API JSON 格式錯誤: {e}')
    if not isinstance(data, list) or not data:
        raise RuntimeError('API 回傳資料為空')
    return data


balance_fetch_status = {
    'running': False,
    'startedAt': None,
    'finishedAt': None,
    'year': None,
    'season': None,
    'total': None,
    'processed': 0,
    'success_count': 0,
    'error_count': 0,
    'current_code': None,
    'error': None,
}

cash_flow_fetch_status = {
    'running': False,
    'startedAt': None,
    'finishedAt': None,
    'year': None,
    'season': None,
    'total': None,
    'processed': 0,
    'success_count': 0,
    'error_count': 0,
    'current_code': None,
    'error': None,
    'db_write_enabled': False,
    'db_inserted_rows': 0,
    'paused': False,
    'resumeAt': None,
    'block_count': 0,
}


warrants_import_status = {
    'running': False,
    'startedAt': None,
    'finishedAt': None,
    'total': None,
    'processed': 0,
    'importedCount': 0,
    'error': None,
}


@app.route('/api/warrants/import-latest', methods=['POST'])
def import_latest_warrants():
    """從 TWSE API 抓取最新權證資料並匯入 tw_warrant_trade。

    資料來源：https://openapi.twse.com.tw/v1/opendata/t187ap42_L
    寫入欄位：out_date, trade_date, warrant_code, warrant_name, turnover, volume,
             raw_out_date_text, raw_trade_date_text, updated_at
    唯一鍵： (warrant_code, trade_date)
    """
    try:
        global warrants_import_status
        warrants_import_status = {
            'running': True,
            'startedAt': datetime.utcnow().isoformat(),
            'finishedAt': None,
            'total': None,
            'processed': 0,
            'importedCount': 0,
            'error': None,
        }

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            # 1) 呼叫 TWSE API
            twse_url = 'https://openapi.twse.com.tw/v1/opendata/t187ap42_L'
            resp = requests.get(twse_url, timeout=30)
            if resp.status_code != 200:
                warrants_import_status['running'] = False
                warrants_import_status['finishedAt'] = datetime.utcnow().isoformat()
                warrants_import_status['error'] = f'TWSE API 狀態碼 {resp.status_code}'
                return jsonify({'success': False, 'error': f'TWSE API 狀態碼 {resp.status_code}'}), 502

            try:
                data = resp.json()
            except Exception as e:
                logger.exception('解析 TWSE API JSON 失敗')
                warrants_import_status['running'] = False
                warrants_import_status['finishedAt'] = datetime.utcnow().isoformat()
                warrants_import_status['error'] = f'TWSE API JSON 格式錯誤: {e}'
                return jsonify({'success': False, 'error': f'TWSE API JSON 格式錯誤: {e}'}), 502

            if not isinstance(data, list) or not data:
                warrants_import_status['running'] = False
                warrants_import_status['finishedAt'] = datetime.utcnow().isoformat()
                warrants_import_status['error'] = 'TWSE API 回傳資料為空'
                return jsonify({'success': False, 'error': 'TWSE API 回傳資料為空'}), 502

            def parse_roc_date(text: str | None):
                if not text:
                    return None
                s = str(text).strip()
                if not s.isdigit() or len(s) != 7:
                    return None
                try:
                    roc_year = int(s[:3])
                    month = int(s[3:5])
                    day = int(s[5:7])
                    year = roc_year + 1911
                    return date(year, month, day)
                except Exception:
                    return None

            def to_number(val):
                if val is None:
                    return None
                s = str(val).replace(',', '').strip()
                if not s:
                    return None
                try:
                    n = float(s)
                    return n
                except Exception:
                    return None

            first = data[0]
            trade_date_text = first.get('交易日期') or first.get('出表日期')
            trade_date_obj = parse_roc_date(trade_date_text)

            cursor = db_manager.connection.cursor()
            cursor.execute('BEGIN')

            sql = (
                """
                INSERT INTO tw_warrant_trade (
                    out_date,
                    trade_date,
                    warrant_code,
                    warrant_name,
                    turnover,
                    volume,
                    raw_out_date_text,
                    raw_trade_date_text,
                    updated_at
                )
                VALUES %s
                ON CONFLICT (warrant_code, trade_date) DO UPDATE SET
                    out_date = EXCLUDED.out_date,
                    warrant_name = EXCLUDED.warrant_name,
                    turnover = EXCLUDED.turnover,
                    volume = EXCLUDED.volume,
                    raw_out_date_text = EXCLUDED.raw_out_date_text,
                    raw_trade_date_text = EXCLUDED.raw_trade_date_text,
                    updated_at = NOW()
                """
            )

            affected = 0
            total = len(data)
            warrants_import_status['total'] = total
            batch_size = 1000
            batch_rows = []
            for item in data:
                out_text = item.get('出表日期')
                trade_text = item.get('交易日期')
                out_date_obj = parse_roc_date(out_text)
                trade_obj = parse_roc_date(trade_text)

                # 若交易日期無法解析則略過
                if trade_obj is None:
                    continue

                out_date_val = out_date_obj or trade_obj
                code = str(item.get('權證代號') or '').strip()
                name = str(item.get('權證名稱') or '').strip() or None
                if not code:
                    continue

                turnover = to_number(item.get('成交金額'))
                volume_num = to_number(item.get('成交張數'))
                if volume_num is not None:
                    try:
                        volume_num = int(volume_num)
                    except Exception:
                        volume_num = None

                row_tuple = (
                    out_date_val,
                    trade_obj,
                    code,
                    name,
                    turnover,
                    volume_num,
                    out_text,
                    trade_text,
                )
                batch_rows.append(row_tuple)

                if len(batch_rows) >= batch_size:
                    # 使用 execute_values 批次插入，並在模板中為 updated_at 加上 NOW()
                    execute_values(
                        cursor,
                        sql,
                        batch_rows,
                        template='(%s,%s,%s,%s,%s,%s,%s,%s,NOW())',
                        page_size=batch_size,
                    )
                    affected += len(batch_rows)
                    warrants_import_status['processed'] = affected
                    warrants_import_status['importedCount'] = affected
                    batch_rows = []

            if batch_rows:
                execute_values(
                    cursor,
                    sql,
                    batch_rows,
                    template='(%s,%s,%s,%s,%s,%s,%s,%s,NOW())',
                    page_size=batch_size,
                )
                affected += len(batch_rows)
                warrants_import_status['processed'] = affected
                warrants_import_status['importedCount'] = affected

            db_manager.connection.commit()

            trade_date_str = (
                trade_date_obj.strftime('%Y-%m-%d') if isinstance(trade_date_obj, (datetime, date)) else None
            )

            warrants_import_status['running'] = False
            warrants_import_status['finishedAt'] = datetime.utcnow().isoformat()
            warrants_import_status['error'] = None
            warrants_import_status['importedCount'] = affected
            warrants_import_status['tradeDate'] = trade_date_str

            return jsonify({
                'success': True,
                'message': '權證資料匯入完成',
                'importedCount': affected,
                'tradeDate': trade_date_str,
            })
        except Exception as e:
            logger.exception('匯入最新權證資料失敗')
            try:
                if db_manager.connection is not None:
                    db_manager.connection.rollback()
            except Exception:
                pass
            warrants_import_status['running'] = False
            warrants_import_status['finishedAt'] = datetime.utcnow().isoformat()
            warrants_import_status['error'] = str(e)
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            db_manager.disconnect()
    except Exception as e:
        logger.exception('匯入最新權證資料失敗（外層）')
        warrants_import_status['running'] = False
        warrants_import_status['finishedAt'] = datetime.utcnow().isoformat()
        warrants_import_status['error'] = str(e)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/import-status', methods=['GET'])
def get_warrants_import_status_api():
    """回傳最新的權證匯入進度狀態，供前端輪詢顯示。"""
    try:
        return jsonify({'success': True, 'status': warrants_import_status})
    except Exception as e:
        logger.exception('取得權證匯入狀態失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/tpex/import-master', methods=['POST'])
def import_tpex_warrant_master():
    """從 TPEX API 抓取上櫃權證主檔並匯入 tpex_warrant_master。"""
    try:
        global tpex_warrant_master_import_status
        tpex_warrant_master_import_status = {
            'running': True,
            'startedAt': datetime.utcnow().isoformat(),
            'finishedAt': None,
            'total': None,
            'processed': 0,
            'importedCount': 0,
            'error': None,
        }

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            db_manager.create_tables()
            data = _fetch_json_list('https://www.tpex.org.tw/openapi/v1/tpex_warrant_issue')
            cursor = db_manager.connection.cursor()
            cursor.execute('BEGIN')

            sql = """
                INSERT INTO tpex_warrant_master (
                    warrant_code, report_date, warrant_name, listed_date, expiry_date,
                    underlying_code, underlying_name, warrant_type, exercise_style,
                    cap_price, floor_price, reset_flag, latest_exercise_price,
                    latest_exercise_ratio, initial_issuance, accumulated_issuance,
                    accumulated_canceled, raw_report_date_text, raw_listed_date_text,
                    raw_expiry_date_text, updated_at
                ) VALUES %s
                ON CONFLICT (warrant_code) DO UPDATE SET
                    report_date = EXCLUDED.report_date,
                    warrant_name = EXCLUDED.warrant_name,
                    listed_date = EXCLUDED.listed_date,
                    expiry_date = EXCLUDED.expiry_date,
                    underlying_code = EXCLUDED.underlying_code,
                    underlying_name = EXCLUDED.underlying_name,
                    warrant_type = EXCLUDED.warrant_type,
                    exercise_style = EXCLUDED.exercise_style,
                    cap_price = EXCLUDED.cap_price,
                    floor_price = EXCLUDED.floor_price,
                    reset_flag = EXCLUDED.reset_flag,
                    latest_exercise_price = EXCLUDED.latest_exercise_price,
                    latest_exercise_ratio = EXCLUDED.latest_exercise_ratio,
                    initial_issuance = EXCLUDED.initial_issuance,
                    accumulated_issuance = EXCLUDED.accumulated_issuance,
                    accumulated_canceled = EXCLUDED.accumulated_canceled,
                    raw_report_date_text = EXCLUDED.raw_report_date_text,
                    raw_listed_date_text = EXCLUDED.raw_listed_date_text,
                    raw_expiry_date_text = EXCLUDED.raw_expiry_date_text,
                    updated_at = NOW()
            """

            total = len(data)
            tpex_warrant_master_import_status['total'] = total
            rows = []
            batch_size = 1000
            affected = 0

            for item in data:
                code = str(item.get('Code') or '').strip()
                if not code:
                    continue
                rows.append((
                    code,
                    _parse_roc_date_text(item.get('Date')),
                    str(item.get('Name') or '').strip() or None,
                    _parse_roc_date_text(item.get('ListedDate')),
                    _parse_roc_date_text(item.get('ExpiryDate')),
                    str(item.get('UnderlyingStockCode') or '').strip() or None,
                    str(item.get('UnderlyingStock') or '').strip() or None,
                    str(item.get('Type') or '').strip() or None,
                    str(item.get('American/European') or '').strip() or None,
                    _to_decimal_or_none(item.get('CapPrice/Index')),
                    _to_decimal_or_none(item.get('FloorPrice/Index')),
                    str(item.get('Reset') or '').strip() or None,
                    _to_decimal_or_none(item.get('LatestExercisePrice')),
                    _to_decimal_or_none(item.get('Latest ExerciseRatio')),
                    _to_int_or_none(item.get('InitialIssuance')),
                    _to_int_or_none(item.get('Accum.Accum.Issuance')),
                    _to_int_or_none(item.get('Accum.CanceledWarrant')),
                    item.get('Date'),
                    item.get('ListedDate'),
                    item.get('ExpiryDate'),
                ))
                if len(rows) >= batch_size:
                    execute_values(cursor, sql, rows, template='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())', page_size=batch_size)
                    affected += len(rows)
                    tpex_warrant_master_import_status['processed'] = affected
                    tpex_warrant_master_import_status['importedCount'] = affected
                    rows = []

            if rows:
                execute_values(cursor, sql, rows, template='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())', page_size=batch_size)
                affected += len(rows)

            db_manager.connection.commit()
            tpex_warrant_master_import_status['running'] = False
            tpex_warrant_master_import_status['finishedAt'] = datetime.utcnow().isoformat()
            tpex_warrant_master_import_status['processed'] = affected
            tpex_warrant_master_import_status['importedCount'] = affected
            return jsonify({'success': True, 'message': 'TPEX 權證主檔匯入完成', 'importedCount': affected})
        except Exception as e:
            logger.exception('匯入 TPEX 權證主檔失敗')
            try:
                db_manager.connection.rollback()
            except Exception:
                pass
            tpex_warrant_master_import_status['running'] = False
            tpex_warrant_master_import_status['finishedAt'] = datetime.utcnow().isoformat()
            tpex_warrant_master_import_status['error'] = str(e)
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            db_manager.disconnect()
    except Exception as e:
        logger.exception('匯入 TPEX 權證主檔失敗（外層）')
        tpex_warrant_master_import_status['running'] = False
        tpex_warrant_master_import_status['finishedAt'] = datetime.utcnow().isoformat()
        tpex_warrant_master_import_status['error'] = str(e)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/tpex/import-daily', methods=['POST'])
def import_tpex_warrant_daily():
    """從 TPEX API 抓取上櫃權證日行情並匯入 tpex_warrant_daily_quotes。"""
    try:
        global tpex_warrant_daily_import_status
        tpex_warrant_daily_import_status = {
            'running': True,
            'startedAt': datetime.utcnow().isoformat(),
            'finishedAt': None,
            'total': None,
            'processed': 0,
            'importedCount': 0,
            'tradeDate': None,
            'error': None,
        }

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            db_manager.create_tables()
            data = _fetch_json_list('https://www.tpex.org.tw/openapi/v1/tpex_warrant_daily_quts')
            first_trade_date = _parse_roc_date_text(data[0].get('Date'))
            trade_date_str = first_trade_date.strftime('%Y-%m-%d') if first_trade_date else None
            tpex_warrant_daily_import_status['tradeDate'] = trade_date_str

            cursor = db_manager.connection.cursor()
            cursor.execute('BEGIN')

            sql = """
                INSERT INTO tpex_warrant_daily_quotes (
                    trade_date, warrant_code, warrant_name, open_price, high_price, low_price,
                    close_price, price_change, trade_volume, transaction_count, trade_value,
                    underlying_code, underlying_name, underlying_close_price,
                    underlying_price_change, raw_trade_date_text, updated_at
                ) VALUES %s
                ON CONFLICT (trade_date, warrant_code) DO UPDATE SET
                    warrant_name = EXCLUDED.warrant_name,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    price_change = EXCLUDED.price_change,
                    trade_volume = EXCLUDED.trade_volume,
                    transaction_count = EXCLUDED.transaction_count,
                    trade_value = EXCLUDED.trade_value,
                    underlying_code = EXCLUDED.underlying_code,
                    underlying_name = EXCLUDED.underlying_name,
                    underlying_close_price = EXCLUDED.underlying_close_price,
                    underlying_price_change = EXCLUDED.underlying_price_change,
                    raw_trade_date_text = EXCLUDED.raw_trade_date_text,
                    updated_at = NOW()
            """

            total = len(data)
            tpex_warrant_daily_import_status['total'] = total
            rows = []
            batch_size = 1000
            affected = 0

            for item in data:
                trade_date = _parse_roc_date_text(item.get('Date'))
                code = str(item.get('Code') or '').strip()
                if trade_date is None or not code:
                    continue
                rows.append((
                    trade_date,
                    code,
                    str(item.get('Name') or '').strip() or None,
                    _to_decimal_or_none(item.get('Open')),
                    _to_decimal_or_none(item.get('High')),
                    _to_decimal_or_none(item.get('Low')),
                    _to_decimal_or_none(item.get('Close')),
                    _to_decimal_or_none(item.get('Change')),
                    _to_int_or_none(item.get('TradeVol.')),
                    _to_int_or_none(item.get('No.OfTransactions')),
                    _to_decimal_or_none(item.get('TradeValue')),
                    str(item.get('UnderlyingStockCode') or '').strip() or None,
                    str(item.get('UnderlyingStock') or '').strip() or None,
                    _to_decimal_or_none(item.get('UnderlyingStockClosePrice')),
                    _to_decimal_or_none(item.get('UnderlyingStock PriceChange')),
                    item.get('Date'),
                ))
                if len(rows) >= batch_size:
                    execute_values(cursor, sql, rows, template='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())', page_size=batch_size)
                    affected += len(rows)
                    tpex_warrant_daily_import_status['processed'] = affected
                    tpex_warrant_daily_import_status['importedCount'] = affected
                    rows = []

            if rows:
                execute_values(cursor, sql, rows, template='(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())', page_size=batch_size)
                affected += len(rows)

            db_manager.connection.commit()
            tpex_warrant_daily_import_status['running'] = False
            tpex_warrant_daily_import_status['finishedAt'] = datetime.utcnow().isoformat()
            tpex_warrant_daily_import_status['processed'] = affected
            tpex_warrant_daily_import_status['importedCount'] = affected
            return jsonify({'success': True, 'message': 'TPEX 權證日行情匯入完成', 'importedCount': affected, 'tradeDate': trade_date_str})
        except Exception as e:
            logger.exception('匯入 TPEX 權證日行情失敗')
            try:
                db_manager.connection.rollback()
            except Exception:
                pass
            tpex_warrant_daily_import_status['running'] = False
            tpex_warrant_daily_import_status['finishedAt'] = datetime.utcnow().isoformat()
            tpex_warrant_daily_import_status['error'] = str(e)
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            db_manager.disconnect()
    except Exception as e:
        logger.exception('匯入 TPEX 權證日行情失敗（外層）')
        tpex_warrant_daily_import_status['running'] = False
        tpex_warrant_daily_import_status['finishedAt'] = datetime.utcnow().isoformat()
        tpex_warrant_daily_import_status['error'] = str(e)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/tpex/import-status', methods=['GET'])
def get_tpex_warrant_import_status_api():
    """回傳 TPEX 權證主檔與日行情匯入狀態。"""
    try:
        return jsonify({
            'success': True,
            'master': tpex_warrant_master_import_status,
            'daily': tpex_warrant_daily_import_status,
        })
    except Exception as e:
        logger.exception('取得 TPEX 權證匯入狀態失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/tpex/dates', methods=['GET'])
def get_tpex_warrant_dates():
    """取得 tpex_warrant_daily_quotes 中可用的交易日期清單。"""
    try:
        limit = request.args.get('limit', default=60, type=int)
        if not isinstance(limit, int) or limit <= 0:
            limit = 60
        limit = min(limit, 365)

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            db_manager.create_tables()
            cursor = db_manager.connection.cursor()
            cursor.execute(
                """
                SELECT DISTINCT trade_date
                FROM tpex_warrant_daily_quotes
                WHERE trade_date IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
        finally:
            db_manager.disconnect()

        dates = []
        for row in rows:
            d = row.get('trade_date') if isinstance(row, dict) else (row[0] if row else None)
            if isinstance(d, (datetime, date)):
                dates.append(d.strftime('%Y-%m-%d'))
            elif isinstance(d, str):
                dates.append(d[:10])
        return jsonify({'success': True, 'dates': dates})
    except Exception as e:
        logger.exception('取得 TPEX 權證日期列表失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/tpex', methods=['GET'])
def query_tpex_warrants():
    """依日期與關鍵字查詢 TPEX 權證日行情。"""
    try:
        date_str = request.args.get('date')
        keyword = (request.args.get('keyword') or '').strip()
        page = request.args.get('page', default=1, type=int) or 1
        page_size = request.args.get('pageSize', default=50, type=int) or 50

        page = max(1, page)
        page_size = max(10, min(200, page_size))
        offset = (page - 1) * page_size

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            db_manager.create_tables()
            cursor = db_manager.connection.cursor()

            target_date = date_str
            if not target_date:
                cursor.execute("SELECT MAX(trade_date) AS latest FROM tpex_warrant_daily_quotes")
                row = cursor.fetchone()
                latest = row.get('latest') if isinstance(row, dict) else (row[0] if row else None)
                if not latest:
                    return jsonify({'success': True, 'data': [], 'total': 0, 'page': page, 'pageSize': page_size, 'date': None})
                target_date = latest.strftime('%Y-%m-%d') if isinstance(latest, (datetime, date)) else str(latest)[:10]

            where_clauses = ['trade_date = %s']
            params = [target_date]
            if keyword:
                where_clauses.append('(warrant_code ILIKE %s OR warrant_name ILIKE %s OR underlying_code ILIKE %s OR underlying_name ILIKE %s)')
                pattern = f'%{keyword}%'
                params.extend([pattern, pattern, pattern, pattern])
            where_sql = ' WHERE ' + ' AND '.join(where_clauses)

            cursor.execute(f'SELECT COUNT(*) AS cnt FROM tpex_warrant_daily_quotes{where_sql}', params)
            row = cursor.fetchone()
            total = int((row.get('cnt', 0) if isinstance(row, dict) else (row[0] if row else 0)) or 0)

            query_params = list(params) + [page_size, offset]
            cursor.execute(
                f"""
                SELECT
                    trade_date, warrant_code, warrant_name, open_price, high_price, low_price,
                    close_price, price_change, trade_volume, transaction_count, trade_value,
                    underlying_code, underlying_name, underlying_close_price, underlying_price_change
                FROM tpex_warrant_daily_quotes
                {where_sql}
                ORDER BY trade_value DESC NULLS LAST,
                         trade_volume DESC NULLS LAST,
                         warrant_code ASC
                LIMIT %s OFFSET %s
                """,
                query_params,
            )
            rows = cursor.fetchall()
        finally:
            db_manager.disconnect()

        data = []
        for row in rows:
            if isinstance(row, dict):
                item = row
            else:
                item = {
                    'trade_date': row[0],
                    'warrant_code': row[1],
                    'warrant_name': row[2],
                    'open_price': row[3],
                    'high_price': row[4],
                    'low_price': row[5],
                    'close_price': row[6],
                    'price_change': row[7],
                    'trade_volume': row[8],
                    'transaction_count': row[9],
                    'trade_value': row[10],
                    'underlying_code': row[11],
                    'underlying_name': row[12],
                    'underlying_close_price': row[13],
                    'underlying_price_change': row[14],
                }
            tdate = item.get('trade_date')
            data.append({
                'trade_date': tdate.strftime('%Y-%m-%d') if isinstance(tdate, (datetime, date)) else (str(tdate)[:10] if tdate else None),
                'warrant_code': item.get('warrant_code'),
                'warrant_name': item.get('warrant_name'),
                'open_price': float(item['open_price']) if item.get('open_price') is not None else None,
                'high_price': float(item['high_price']) if item.get('high_price') is not None else None,
                'low_price': float(item['low_price']) if item.get('low_price') is not None else None,
                'close_price': float(item['close_price']) if item.get('close_price') is not None else None,
                'price_change': float(item['price_change']) if item.get('price_change') is not None else None,
                'trade_volume': int(item['trade_volume']) if item.get('trade_volume') is not None else None,
                'transaction_count': int(item['transaction_count']) if item.get('transaction_count') is not None else None,
                'trade_value': float(item['trade_value']) if item.get('trade_value') is not None else None,
                'underlying_code': item.get('underlying_code'),
                'underlying_name': item.get('underlying_name'),
                'underlying_close_price': float(item['underlying_close_price']) if item.get('underlying_close_price') is not None else None,
                'underlying_price_change': float(item['underlying_price_change']) if item.get('underlying_price_change') is not None else None,
            })

        return jsonify({'success': True, 'data': data, 'total': total, 'page': page, 'pageSize': page_size, 'date': target_date})
    except Exception as e:
        logger.exception('查詢 TPEX 權證日行情失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants/dates', methods=['GET'])
def get_warrant_dates():
    """取得權證可用的交易日期清單，支援 TWSE / TPEX / 全市場。"""
    try:
        limit = request.args.get('limit', default=60, type=int)
        market = (request.args.get('market') or 'twse').strip().lower()
        if not isinstance(limit, int) or limit <= 0:
            limit = 60
        limit = min(limit, 365)

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            db_manager.create_tables()
            cursor = db_manager.connection.cursor()
            if market == 'tpex':
                cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM tpex_warrant_daily_quotes
                    WHERE trade_date IS NOT NULL
                    ORDER BY trade_date DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            elif market in {'both', 'all'}:
                cursor.execute(
                    """
                    SELECT trade_date
                    FROM (
                        SELECT DISTINCT trade_date FROM tw_warrant_trade WHERE trade_date IS NOT NULL
                        UNION
                        SELECT DISTINCT trade_date FROM tpex_warrant_daily_quotes WHERE trade_date IS NOT NULL
                    ) t
                    ORDER BY trade_date DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                cursor.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM tw_warrant_trade
                    WHERE trade_date IS NOT NULL
                    ORDER BY trade_date DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cursor.fetchall()
        finally:
            db_manager.disconnect()

        dates: list[str] = []
        for row in rows:
            if isinstance(row, dict):
                d = row.get('trade_date')
            else:
                d = row[0] if row else None
            if isinstance(d, (datetime, date)):
                dates.append(d.strftime('%Y-%m-%d'))
            elif isinstance(d, str):
                dates.append(d[:10])

        return jsonify({'success': True, 'dates': dates})
    except Exception as e:
        logger.exception('取得權證日期列表失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/warrants', methods=['GET'])
def query_warrants():
    """依日期、關鍵字與市場查詢權證資料。

    Query parameters:
        date: 交易日期 (YYYY-MM-DD)，若未提供則使用資料表中最新日期
        keyword: 權證代號或名稱關鍵字（模糊查詢）
        market: twse | tpex | both，預設 twse
        page: 第幾頁（預設 1）
        pageSize: 每頁筆數（預設 50，區間 10~200）
    """
    try:
        date_str = request.args.get('date')
        keyword = (request.args.get('keyword') or '').strip()
        market = (request.args.get('market') or 'twse').strip().lower()
        page = request.args.get('page', default=1, type=int) or 1
        page_size = request.args.get('pageSize', default=50, type=int) or 50

        page = max(1, page)
        page_size = max(10, min(200, page_size))
        offset = (page - 1) * page_size

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            db_manager.create_tables()
            cursor = db_manager.connection.cursor()

            target_date = date_str
            if not target_date:
                if market == 'tpex':
                    cursor.execute("SELECT MAX(trade_date) AS latest FROM tpex_warrant_daily_quotes")
                elif market in {'both', 'all'}:
                    cursor.execute(
                        """
                        SELECT MAX(trade_date) AS latest
                        FROM (
                            SELECT trade_date FROM tw_warrant_trade
                            UNION ALL
                            SELECT trade_date FROM tpex_warrant_daily_quotes
                        ) t
                        """
                    )
                else:
                    cursor.execute("SELECT MAX(trade_date) AS latest FROM tw_warrant_trade")
                row = cursor.fetchone()
                latest = None
                if row:
                    if isinstance(row, dict):
                        latest = row.get('latest')
                    else:
                        latest = row[0]
                if not latest:
                    return jsonify({
                        'success': True,
                        'data': [],
                        'total': 0,
                        'page': page,
                        'pageSize': page_size,
                        'date': None,
                    })
                if isinstance(latest, (datetime, date)):
                    target_date = latest.strftime('%Y-%m-%d')
                else:
                    target_date = str(latest)[:10]

            where_clauses = ["trade_date = %s"]
            params: list = [target_date]

            if keyword:
                pattern = f"%{keyword}%"
                if market == 'tpex':
                    where_clauses.append("(warrant_code ILIKE %s OR warrant_name ILIKE %s OR underlying_code ILIKE %s OR underlying_name ILIKE %s)")
                    params.extend([pattern, pattern, pattern, pattern])
                elif market in {'both', 'all'}:
                    where_clauses.append("(warrant_code ILIKE %s OR warrant_name ILIKE %s OR underlying_code ILIKE %s OR underlying_name ILIKE %s)")
                    params.extend([pattern, pattern, pattern, pattern])
                else:
                    where_clauses.append("(warrant_code ILIKE %s OR warrant_name ILIKE %s)")
                    params.extend([pattern, pattern])

            where_sql = " WHERE " + " AND ".join(where_clauses)

            if market == 'tpex':
                base_sql = f"""
                    SELECT
                        'TPEX' AS market,
                        trade_date,
                        warrant_code,
                        warrant_name,
                        underlying_code,
                        underlying_name,
                        trade_value,
                        trade_volume
                    FROM tpex_warrant_daily_quotes
                    {where_sql}
                """
            elif market in {'both', 'all'}:
                base_sql = f"""
                    SELECT
                        market,
                        trade_date,
                        warrant_code,
                        warrant_name,
                        underlying_code,
                        underlying_name,
                        trade_value,
                        trade_volume
                    FROM (
                        SELECT
                            'TWSE' AS market,
                            trade_date,
                            warrant_code,
                            warrant_name,
                            NULL::VARCHAR(20) AS underlying_code,
                            NULL::VARCHAR(100) AS underlying_name,
                            turnover AS trade_value,
                            volume AS trade_volume
                        FROM tw_warrant_trade
                        UNION ALL
                        SELECT
                            'TPEX' AS market,
                            trade_date,
                            warrant_code,
                            warrant_name,
                            underlying_code,
                            underlying_name,
                            trade_value,
                            trade_volume
                        FROM tpex_warrant_daily_quotes
                    ) merged
                    {where_sql}
                """
            else:
                base_sql = f"""
                    SELECT
                        'TWSE' AS market,
                        trade_date,
                        warrant_code,
                        warrant_name,
                        NULL::VARCHAR(20) AS underlying_code,
                        NULL::VARCHAR(100) AS underlying_name,
                        turnover AS trade_value,
                        volume AS trade_volume
                    FROM tw_warrant_trade
                    {where_sql}
                """

            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM ({base_sql}) q",
                params,
            )
            row = cursor.fetchone()
            if isinstance(row, dict):
                total = row.get('cnt', 0)
            else:
                total = row[0] if row else 0
            total = int(total or 0)

            params_with_paging = list(params) + [page_size, offset]
            cursor.execute(
                f"""
                SELECT
                    market,
                    trade_date,
                    warrant_code,
                    warrant_name,
                    underlying_code,
                    underlying_name,
                    trade_value,
                    trade_volume
                FROM ({base_sql}) q
                ORDER BY trade_value DESC NULLS LAST,
                         trade_volume DESC NULLS LAST,
                         warrant_code ASC
                LIMIT %s OFFSET %s
                """,
                params_with_paging,
            )
            rows = cursor.fetchall()
        finally:
            db_manager.disconnect()

        data = []
        for row in rows:
            if isinstance(row, dict):
                market_name = row.get('market')
                tdate = row.get('trade_date')
                code = row.get('warrant_code')
                name = row.get('warrant_name')
                underlying_code = row.get('underlying_code')
                underlying_name = row.get('underlying_name')
                trade_value = row.get('trade_value')
                trade_volume = row.get('trade_volume')
            else:
                market_name, tdate, code, name, underlying_code, underlying_name, trade_value, trade_volume = row

            if isinstance(tdate, (datetime, date)):
                tdate_str = tdate.strftime('%Y-%m-%d')
            elif isinstance(tdate, str):
                tdate_str = tdate[:10]
            else:
                tdate_str = None

            try:
                trade_value_val = float(trade_value) if trade_value is not None else None
            except Exception:
                trade_value_val = None

            try:
                trade_volume_val = int(trade_volume) if trade_volume is not None else None
            except Exception:
                trade_volume_val = None

            data.append({
                'market': market_name,
                'trade_date': tdate_str,
                'warrant_code': code,
                'warrant_name': name,
                'underlying_code': underlying_code,
                'underlying_name': underlying_name,
                'trade_value': trade_value_val,
                'trade_volume': trade_volume_val,
                'turnover': trade_value_val,
                'volume': trade_volume_val,
            })

        return jsonify({
            'success': True,
            'data': data,
            'total': total,
            'page': page,
            'pageSize': page_size,
            'date': target_date,
            'market': market,
        })
    except Exception as e:
        logger.exception('查詢權證資料失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/t86/fetch', methods=['GET'])
def fetch_t86_range_api():
    """抓取 TWSE/TPEX 三大法人（T86）資料區間。

    Query parameters:
        start: YYYY-MM-DD (required)
        end: YYYY-MM-DD (required)
        market: twse | tpex | both (optional, default both)
        sleep: float seconds between days (optional, default 0.6)

    回傳: { success, summary, daily_stats, count, data }
    """
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        market = request.args.get('market', 'both')
        sleep_param = request.args.get('sleep')

        if not start or not end:
            return jsonify({'success': False, 'error': '需要 start 與 end 參數'}), 400

        try:
            sleep_seconds = float(sleep_param) if sleep_param is not None else 0.6
        except ValueError:
            sleep_seconds = 0.6

        records, summary, daily_stats = stock_api.fetch_t86_range(start, end, market=market, sleep_seconds=sleep_seconds)

        persist_flag = (request.args.get('persist', 'true').lower() != 'false')
        inserted = 0
        if persist_flag and records:
            db_manager = DatabaseManager.from_request_args(request.args)
            try:
                inserted = stock_api.upsert_t86_records(records, db_manager=db_manager)
            except Exception as db_exc:
                inserted = 0
                logger.exception('T86 資料寫入失敗')
            finally:
                try:
                    db_manager.disconnect()
                except Exception:
                    pass

        return jsonify({
            'success': True,
            'summary': summary,
            'daily_stats': daily_stats,
            'count': len(records),
            'persisted': inserted,
            'persist_enabled': persist_flag,
            'data': records,
        })
    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('fetch_t86_range_api 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/t86/export', methods=['GET'])
def export_t86_csv():
    """匯出 TWSE/TPEX 三大法人資料為 CSV 檔案。"""
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        market = request.args.get('market', 'both')
        sleep_param = request.args.get('sleep')

        if not start or not end:
            return jsonify({'success': False, 'error': '需要 start 與 end 參數'}), 400

        try:
            sleep_seconds = float(sleep_param) if sleep_param is not None else 0.6
        except ValueError:
            sleep_seconds = 0.6

        records, summary, daily_stats = stock_api.fetch_t86_range(start, end, market=market, sleep_seconds=sleep_seconds)

        if not records:
            return jsonify({'success': False, 'error': '查無資料可匯出'}), 404

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'date', 'market', 'stock_no', 'stock_name',
            'foreign_buy', 'foreign_sell', 'foreign_net',
            'foreign_dealer_buy', 'foreign_dealer_sell', 'foreign_dealer_net',
            'foreign_total_buy', 'foreign_total_sell', 'foreign_total_net',
            'investment_trust_buy', 'investment_trust_sell', 'investment_trust_net',
            'dealer_self_buy', 'dealer_self_sell', 'dealer_self_net',
            'dealer_hedge_buy', 'dealer_hedge_sell', 'dealer_hedge_net',
            'dealer_total_buy', 'dealer_total_sell', 'dealer_total_net',
            'overall_net'
        ])

        for record in records:
            writer.writerow([
                record.get('date'),
                record.get('market'),
                record.get('stock_no'),
                record.get('stock_name'),
                record.get('foreign_buy'),
                record.get('foreign_sell'),
                record.get('foreign_net'),
                record.get('foreign_dealer_buy'),
                record.get('foreign_dealer_sell'),
                record.get('foreign_dealer_net'),
                record.get('foreign_total_buy'),
                record.get('foreign_total_sell'),
                record.get('foreign_total_net'),
                record.get('investment_trust_buy'),
                record.get('investment_trust_sell'),
                record.get('investment_trust_net'),
                record.get('dealer_self_buy'),
                record.get('dealer_self_sell'),
                record.get('dealer_self_net'),
                record.get('dealer_hedge_buy'),
                record.get('dealer_hedge_sell'),
                record.get('dealer_hedge_net'),
                record.get('dealer_total_buy'),
                record.get('dealer_total_sell'),
                record.get('dealer_total_net'),
                record.get('overall_net'),
            ])

        output.seek(0)
        filename = f"t86_{market}_{start}_{end}.csv"
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )

    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('export_t86_csv 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/margin/fetch', methods=['GET'])
def fetch_margin_range_api():
    """抓取 TWSE/TPEX 融資融券資料區間。

    Query parameters:
        start: YYYY-MM-DD (required)
        end: YYYY-MM-DD (required)
        market: twse | tpex | both (optional, default both)
        sleep: float seconds between days (optional, default 0.6)

    回傳: { success, summary, daily_stats, count, data }
    """
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        market = request.args.get('market', 'both')
        sleep_param = request.args.get('sleep')

        if not start or not end:
            return jsonify({'success': False, 'error': '需要 start 與 end 參數'}), 400

        try:
            sleep_seconds = float(sleep_param) if sleep_param is not None else 0.6
        except ValueError:
            sleep_seconds = 0.6

        records, summary, daily_stats = stock_api.fetch_margin_range(start, end, market=market, sleep_seconds=sleep_seconds)

        persist_flag = (request.args.get('persist', 'true').lower() != 'false')
        inserted = 0
        if persist_flag and records:
            db_manager = DatabaseManager.from_request_args(request.args)
            try:
                inserted = stock_api.upsert_margin_records(records, db_manager=db_manager)
            except Exception:
                inserted = 0
                logger.exception('Margin 資料寫入失敗')
            finally:
                try:
                    db_manager.disconnect()
                except Exception:
                    pass

        return jsonify({
            'success': True,
            'summary': summary,
            'daily_stats': daily_stats,
            'count': len(records),
            'persisted': inserted,
            'persist_enabled': persist_flag,
            'data': records,
        })
    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('fetch_margin_range_api 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/margin/export', methods=['GET'])
def export_margin_csv():
    """匯出 TWSE/TPEX 融資融券資料為 CSV 檔案。"""
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        market = request.args.get('market', 'both')
        sleep_param = request.args.get('sleep')

        if not start or not end:
            return jsonify({'success': False, 'error': '需要 start 與 end 參數'}), 400

        try:
            sleep_seconds = float(sleep_param) if sleep_param is not None else 0.6
        except ValueError:
            sleep_seconds = 0.6

        records, summary, daily_stats = stock_api.fetch_margin_range(start, end, market=market, sleep_seconds=sleep_seconds)

        if not records:
            return jsonify({'success': False, 'error': '查無資料可匯出'}), 404

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'date', 'market', 'stock_no', 'stock_name',
            'margin_prev_balance', 'margin_buy', 'margin_sell', 'margin_repay', 'margin_balance', 'margin_limit',
            'short_prev_balance', 'short_sell', 'short_buy', 'short_repay', 'short_balance', 'short_limit',
            'offset_quantity', 'note',
        ])

        for record in records:
            writer.writerow([
                record.get('date'),
                record.get('market'),
                record.get('stock_no'),
                record.get('stock_name'),
                record.get('margin_prev_balance'),
                record.get('margin_buy'),
                record.get('margin_sell'),
                record.get('margin_repay'),
                record.get('margin_balance'),
                record.get('margin_limit'),
                record.get('short_prev_balance'),
                record.get('short_sell'),
                record.get('short_buy'),
                record.get('short_repay'),
                record.get('short_balance'),
                record.get('short_limit'),
                record.get('offset_quantity'),
                (record.get('note') or '').strip() if isinstance(record.get('note'), str) else record.get('note'),
            ])

        output.seek(0)
        filename = f"margin_{market}_{start}_{end}.csv"
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )

    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('export_margin_csv 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/revenue/fetch_range', methods=['GET'])
def fetch_revenue_range_api():
    """抓取 TWSE/TPEX 月營收「月份區間」資料（使用 MOPS HTML 報表）。

    Query parameters:
        start: YYYY-MM (required)
        end:   YYYY-MM (required)
        market: twse | tpex | both (optional, default both)
        sleep: float seconds between months (optional, default 1.0)
        persist: true | false (optional, default true)

    回傳: { success, summary, monthly_stats, count }
    """
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        market = request.args.get('market', 'both')
        sleep_param = request.args.get('sleep')
        include_data_flag = (request.args.get('include_data', 'false').lower() == 'true')
        max_records_param = request.args.get('max_records')

        if not start or not end:
            return jsonify({'success': False, 'error': '需要 start 與 end 參數 (YYYY-MM)'}), 400

        def parse_ym(s: str) -> tuple[int, int]:
            try:
                parts = s.split('-')
                if len(parts) != 2:
                    raise ValueError
                y = int(parts[0])
                m = int(parts[1])
                if m < 1 or m > 12:
                    raise ValueError
                return y, m
            except Exception:
                raise ValueError('年月格式錯誤，需 YYYY-MM')

        start_y, start_m = parse_ym(start)
        end_y, end_m = parse_ym(end)

        # 生成月份序列
        months = []
        cy, cm = start_y, start_m
        while (cy < end_y) or (cy == end_y and cm <= end_m):
            months.append((cy, cm))
            if cm == 12:
                cy += 1
                cm = 1
            else:
                cm += 1

        try:
            sleep_seconds = float(sleep_param) if sleep_param is not None else 1.0
        except ValueError:
            sleep_seconds = 1.0

        try:
            max_records = int(max_records_param) if max_records_param is not None else 2000
        except Exception:
            max_records = 2000
        if max_records < 0:
            max_records = 0

        persist_flag = (request.args.get('persist', 'true').lower() != 'false')

        total_inserted = 0
        total_records = 0
        monthly_stats = []
        merged_records: list[dict] = []
        total_per_market = {'TWSE': 0, 'TPEX': 0}

        db_manager = None
        if persist_flag:
            db_manager = DatabaseManager.from_request_args(request.args)
            if not db_manager.connect():
                return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500

        try:
            for (yy, mm) in months:
                try:
                    records, summary = stock_api.fetch_monthly_revenue_html(yy, mm, market=market)
                except ValueError as ve:
                    monthly_stats.append({
                        'year': yy,
                        'month': mm,
                        'error': str(ve),
                        'total_records': 0,
                        'per_market': {'TWSE': 0, 'TPEX': 0},
                        'persisted': 0,
                    })
                    continue

                month_inserted = 0
                if persist_flag and records:
                    try:
                        month_inserted = stock_api.upsert_monthly_revenue(records, db_manager=db_manager)
                    except Exception as db_exc:
                        logger.exception('月營收歷史資料寫入失敗')
                        month_inserted = 0

                if include_data_flag and records and max_records != 0:
                    if max_records > 0:
                        remaining = max_records - len(merged_records)
                        if remaining > 0:
                            merged_records.extend(records[:remaining])
                    else:
                        merged_records.extend(records)

                total_inserted += month_inserted
                total_records += len(records)

                try:
                    per_market = summary.get('per_market') or {}
                    total_per_market['TWSE'] += int(per_market.get('TWSE') or 0)
                    total_per_market['TPEX'] += int(per_market.get('TPEX') or 0)
                except Exception:
                    pass

                monthly_stats.append({
                    'year': summary.get('year'),
                    'month': summary.get('month'),
                    'roc_yyyymm': summary.get('roc_yyyymm'),
                    'total_records': summary.get('total_records', len(records)),
                    'per_market': summary.get('per_market', {}),
                    'persisted': month_inserted,
                })

                if sleep_seconds and sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            summary_out = {
                'start': start,
                'end': end,
                'period': f"{start} ~ {end}",
                'monthsProcessed': len(months),
                'total_records': total_records,
                'per_market': total_per_market,
                'totalInserted': total_inserted,
                'persist_enabled': persist_flag,
                'include_data': include_data_flag,
            }

            return jsonify({
                'success': True,
                'summary': summary_out,
                'monthly_stats': monthly_stats,
                'count': total_records,
                'data': merged_records if include_data_flag else [],
            })
        finally:
            if db_manager is not None:
                try:
                    db_manager.disconnect()
                except Exception:
                    pass
    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('fetch_revenue_range_api 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/revenue/import_from_csv', methods=['POST'])
def import_revenue_from_csv_api():
    """從本機 MOPS 月營收 CSV 檔匯入 monthly_revenue_table。"""
    try:
        payload = request.get_json(silent=True) or {}
        download_dir = (
            payload.get('dir')
            or payload.get('path')
            or payload.get('download_dir')
        )

        if not download_dir:
            home = os.path.expanduser('~')
            download_dir = os.path.join(home, 'Downloads', 'mops_csv')

        db_manager = DatabaseManager.from_request_payload(payload)
        try:
            summary = stock_api.import_mops_csv_monthly_revenue(
                download_dir=download_dir,
                db_manager=db_manager,
            )
        finally:
            try:
                db_manager.disconnect()
            except Exception:
                pass

        return jsonify({
            'success': True,
            'summary': summary,
        })
    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('import_revenue_from_csv_api 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/revenue/download_mops_csv', methods=['POST'])
def download_revenue_mops_csv_api():
    try:
        payload = request.get_json(silent=True) or {}
        start_year_tw = payload.get('start_year_tw', payload.get('start_roc_year', 109))
        end_year_tw = payload.get('end_year_tw', payload.get('end_roc_year', start_year_tw))
        download_dir = (
            payload.get('dir')
            or payload.get('path')
            or payload.get('download_dir')
        )
        delay_between = payload.get('delay_between', 2.0)
        max_retries = payload.get('max_retries', 3)
        market = payload.get('market', payload.get('market_type', 'both')) or 'both'

        start_year_tw = int(start_year_tw)
        end_year_tw = int(end_year_tw)
        delay_between = float(delay_between)
        max_retries = int(max_retries)

        summary = stock_api.download_mops_monthly_revenue_csv(
            start_year_tw=start_year_tw,
            end_year_tw=end_year_tw,
            download_dir=download_dir,
            delay_between=delay_between,
            max_retries=max_retries,
            market=market,
        )

        import_after = bool(payload.get('import_after', False))
        import_summary = None
        if import_after:
            db_manager = DatabaseManager.from_request_payload(payload)
            try:
                import_summary = stock_api.import_mops_csv_monthly_revenue(
                    download_dir=summary.get('download_dir'),
                    db_manager=db_manager,
                )
            finally:
                try:
                    db_manager.disconnect()
                except Exception:
                    pass

        return jsonify({
            'success': True,
            'download_summary': summary,
            'import_summary': import_summary,
        })
    except Exception as e:
        logger.exception('download_revenue_mops_csv_api 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/revenue/fetch', methods=['GET'])
def fetch_revenue_api():
    """抓取上市/上櫃月營收資料，支援指定西元年/月與市場選擇。

    Query parameters:
        year: int (optional, 西元年)
        month: int (optional, 1-12)
        market: twse | tpex | both (optional, default both)
        persist: true | false (optional, default true)

    若 year/month 省略，則抓最新一個月。

    回傳: { success, summary, count, data, persisted, persist_enabled }
    """
    try:
        year_param = request.args.get('year')
        month_param = request.args.get('month')
        market = request.args.get('market', 'both')

        year = int(year_param) if year_param is not None else None
        month = int(month_param) if month_param is not None else None

        records, summary = stock_api.fetch_monthly_revenue(year=year, month=month, market=market)
        if summary is None:
            summary = {}

        persist_flag = (request.args.get('persist', 'true').lower() != 'false')
        inserted = 0
        revenue_table_name = None
        if persist_flag and records:
            db_manager = DatabaseManager.from_request_args(request.args)
            try:
                revenue_table_name = getattr(db_manager, 'table_revenue', 'tw_stock_monthly_revenue')
                inserted = stock_api.upsert_monthly_revenue(records, db_manager=db_manager)
            except Exception:
                inserted = 0
                logger.exception('月營收資料寫入失敗')
            finally:
                try:
                    db_manager.disconnect()
                except Exception:
                    pass

        # 將實際來源網址與資料表名稱附加到 summary，方便前端日誌顯示
        if isinstance(summary, dict):
            try:
                markets_in_summary = summary.get('markets') or []
                roc_yyyymm = summary.get('roc_yyyymm')
                source_urls: dict[str, str] = {}
                try:
                    if roc_yyyymm:
                        roc_str = str(roc_yyyymm).strip()
                        if len(roc_str) >= 4:
                            roc_year = int(roc_str[:-2])
                            m_val = int(roc_str[-2:])
                            if roc_year > 0 and 1 <= m_val <= 12:
                                if roc_year <= 98:
                                    twse_url = f"https://mopsov.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{m_val}.html"
                                    tpex_url = f"https://mopsov.twse.com.tw/nas/t21/otc/t21sc03_{roc_year}_{m_val}.html"
                                else:
                                    twse_url = f"https://mopsov.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{m_val}_0.html"
                                    tpex_url = f"https://mopsov.twse.com.tw/nas/t21/otc/t21sc03_{roc_year}_{m_val}_0.html"

                                if 'twse' in markets_in_summary:
                                    source_urls['TWSE'] = twse_url
                                if 'tpex' in markets_in_summary:
                                    source_urls['TPEX'] = tpex_url
                except Exception:
                    pass

                summary['source_urls'] = source_urls
                if revenue_table_name:
                    summary['revenue_table'] = revenue_table_name
            except Exception:
                # 附加額外摘要資訊失敗時不影響主要回應
                pass

        return jsonify({
            'success': True,
            'summary': summary,
            'count': len(records),
            'persisted': inserted,
            'persist_enabled': persist_flag,
            'data': records,
        })
    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('fetch_revenue_api 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/revenue/export', methods=['GET'])
def export_revenue_csv():
    """匯出上市/上櫃月營收資料為 CSV 檔案。"""
    try:
        year_param = request.args.get('year')
        month_param = request.args.get('month')
        market = request.args.get('market', 'both')

        year = int(year_param) if year_param is not None else None
        month = int(month_param) if month_param is not None else None

        records, summary = stock_api.fetch_monthly_revenue(year=year, month=month, market=market)

        if not records:
            return jsonify({'success': False, 'error': '查無資料可匯出'}), 404

        year_used = summary.get('year')
        month_used = summary.get('month')
        ym_label = f"{year_used}-{month_used:02d}" if (year_used and month_used) else 'latest'

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'revenue_month', 'market', 'stock_no', 'stock_name', 'industry', 'report_date',
            'month_revenue', 'last_month_revenue', 'last_year_month_revenue',
            'mom_change_pct', 'yoy_change_pct',
            'acc_revenue', 'last_year_acc_revenue', 'acc_change_pct', 'note',
        ])

        for rec in records:
            writer.writerow([
                rec.get('revenue_month'),
                rec.get('market'),
                rec.get('stock_no'),
                rec.get('stock_name'),
                rec.get('industry'),
                rec.get('report_date'),
                rec.get('month_revenue'),
                rec.get('last_month_revenue'),
                rec.get('last_year_month_revenue'),
                rec.get('mom_change_pct'),
                rec.get('yoy_change_pct'),
                rec.get('acc_revenue'),
                rec.get('last_year_acc_revenue'),
                rec.get('acc_change_pct'),
                rec.get('note'),
            ])

        output.seek(0)
        filename = f"revenue_{market}_{ym_label}.csv"
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )

    except ValueError as ve:
        return jsonify({'success': False, 'error': str(ve)}), 400
    except Exception as e:
        logger.exception('export_revenue_csv 失敗')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/debug/fetch_source', methods=['GET'])
def debug_fetch_source():
    """調用內部抓取邏輯 fetch_stock_data() 並回傳原始抓到的資料摘要，用於診斷來源是否為空。
    Query: symbol, start, end
    """
    try:
        symbol = request.args.get('symbol')
        start = request.args.get('start')
        end = request.args.get('end')
        if not symbol or not start or not end:
            return jsonify({'success': False, 'error': '需要參數 symbol, start, end'}), 400
        stock_api = StockAPI()
        df_or_list = stock_api.fetch_stock_data(symbol, start, end)
        data = []
        if df_or_list is None:
            return jsonify({'success': True, 'count': 0, 'data': []})
        if hasattr(df_or_list, 'to_dict'):
            rows = df_or_list.to_dict('records')
        else:
            rows = df_or_list
        data = rows[:10]
        return jsonify({'success': True, 'count': len(rows), 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stock/<symbol>/prices', methods=['GET'])
def get_stock_prices(symbol):
    """從資料庫或 API 獲取股票價格數據"""
    try:
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        table_override = request.args.get('table')

        # 如果是台灣加權指數，直接從 API 抓取
        if symbol == '^TWII':
            logger.info(f"偵測到 {symbol}，直接呼叫 API 抓取最新數據")
            data = stock_api.fetch_stock_data(symbol, start_date, end_date)

            if data is None:
                return jsonify({
                    'success': False,
                    'error': f'無法從台灣交易所/櫃買中心 API 獲取 {symbol} 數據'
                }), 404

            # 支援 DataFrame 或清單格式
            if hasattr(data, 'empty'):
                if data.empty:
                    return jsonify({
                        'success': False,
                        'error': f'{symbol} 數據為空'
                    }), 404
                price_data = data.to_dict('records')
            elif isinstance(data, list):
                price_data = data
            else:
                logger.warning(f"{symbol} 資料格式非預期: {type(data)}")
                return jsonify({
                    'success': False,
                    'error': f'{symbol} 數據格式非預期'
                }), 500

            # 日期對象轉換為字符串
            for record in price_data:
                if not isinstance(record, dict):
                    continue
                # 兼容鍵名大小寫
                record_date = record.get('date') or record.get('Date')
                if isinstance(record_date, pd.Timestamp):
                    record['date'] = record_date.strftime('%Y-%m-%d')
                elif isinstance(record_date, date):
                    record['date'] = record_date.strftime('%Y-%m-%d')
                elif isinstance(record_date, str):
                    record['date'] = record_date[:10]

                # 正規化欄位為資料庫 schema
                if 'open_price' not in record:
                    record['open_price'] = record.get('open') if 'open' in record else record.get('Open')
                if 'high_price' not in record:
                    record['high_price'] = record.get('high') if 'high' in record else record.get('High')
                if 'low_price' not in record:
                    record['low_price'] = record.get('low') if 'low' in record else record.get('Low')
                if 'close_price' not in record:
                    record['close_price'] = record.get('close') if 'close' in record else record.get('Close')
                if 'volume' not in record:
                    record['volume'] = record.get('volume') if 'volume' in record else record.get('Volume', 0)

            # 將資料寫入資料庫（upsert）
            inserted = 0
            try:
                db_manager = DatabaseManager.from_request_args(request.args)
                if db_manager.connect():
                    db_manager.create_tables()
                    cursor = db_manager.connection.cursor()
                    prices_table = db_manager.table_prices
                    rows = []
                    for r in price_data:
                        try:
                            rows.append((
                                '^TWII',
                                r.get('date'),
                                float(r.get('open_price')) if r.get('open_price') is not None else None,
                                float(r.get('high_price')) if r.get('high_price') is not None else None,
                                float(r.get('low_price')) if r.get('low_price') is not None else None,
                                float(r.get('close_price')) if r.get('close_price') is not None else None,
                                int(r.get('volume') or 0)
                            ))
                        except Exception:
                            continue

                    if rows:
                        execute_values(
                            cursor,
                            f"""
                            INSERT INTO {prices_table}
                                (symbol, date, open_price, high_price, low_price, close_price, volume)
                            VALUES %s
                            ON CONFLICT (symbol, date) DO UPDATE SET
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close_price = EXCLUDED.close_price,
                                volume = EXCLUDED.volume
                            """,
                            rows,
                            page_size=500
                        )
                        db_manager.connection.commit()
                        inserted = len(rows)
                    cursor.close()
                else:
                    logger.error('^TWII 入庫時無法連接資料庫')
            finally:
                try:
                    db_manager.disconnect()
                except Exception:
                    pass

            return jsonify({
                'success': True,
                'data': price_data,
                'count': len(price_data),
                'persisted_rows': inserted
            })

        # 對於其他股票，從資料庫查詢
        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({
                'success': False,
                'error': '資料庫連接失敗'
            }), 500
        
        try:
            cursor = db_manager.connection.cursor()
            prices_table = db_manager.table_prices
            if table_override:
                prices_table = _resolve_table_override(cursor, str(table_override).strip())

            # 先檢查表是否存在並獲取欄位資訊
            cursor.execute(
                """
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
                """,
                [prices_table]
            )
            results = cursor.fetchall()
            columns = []
            for row in results:
                if isinstance(row, dict):
                    columns.append(row['column_name'])
                elif isinstance(row, (list, tuple)):
                    columns.append(row[0])
                else:
                    columns.append(str(row))
            
            if not columns:
                # 表不存在，嘗試創建
                logger.info(f"{prices_table} 表不存在，嘗試創建...")
                db_manager.create_tables()
                return jsonify({
                    'success': False,
                    'error': '資料庫表不存在，已嘗試創建，請重新查詢'
                }), 500
            
            # 檢查是否有必要的欄位，如果沒有則重新創建表
            required_columns = ['symbol', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            missing_columns = [col for col in required_columns if col not in columns]
            
            if missing_columns:
                logger.warning(f"{prices_table} 表缺少欄位: {missing_columns}")
                # 刪除舊表並重新創建
                cursor.execute(f"DROP TABLE IF EXISTS {prices_table} CASCADE")
                cursor.execute(f"DROP TABLE IF EXISTS {db_manager.table_returns} CASCADE")
                db_manager.connection.commit()
                logger.info("已刪除舊表，重新創建...")
                db_manager.create_tables()
                return jsonify({
                    'success': False,
                    'error': '資料庫表結構不完整，已重新創建，請重新查詢'
                }), 500
            
            logger.info(f"{prices_table} 表欄位: {columns}")
            
            # 根據實際欄位構建查詢
            if 'open_price' in columns:
                query = sql.SQL(
                    "SELECT date, open_price, high_price, low_price, close_price, volume FROM {} WHERE symbol = %s"
                ).format(sql.Identifier(prices_table))
            else:
                # 如果沒有 open_price 等欄位，可能是舊的表結構
                query = sql.SQL(
                    "SELECT date, close_price, volume FROM {} WHERE symbol = %s"
                ).format(sql.Identifier(prices_table))
            
            # 支援多種股票代碼格式查詢
            # 如果輸入的是純數字代碼，嘗試匹配完整格式
            if symbol.isdigit():
                # 先嘗試直接查詢，如果沒有結果，再嘗試添加後綴
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {} WHERE symbol = %s").format(sql.Identifier(prices_table)),
                    [symbol]
                )
                result = cursor.fetchone()
                count = result[0] if isinstance(result, (list, tuple)) else result.get('count', 0)
                
                if count == 0:
                    # 嘗試查找帶有 .TW 或 .TWO 後綴的股票
                    cursor.execute(
                        sql.SQL(
                            "SELECT symbol FROM {} WHERE symbol IN (%s, %s) LIMIT 1"
                        ).format(sql.Identifier(prices_table)),
                        [f"{symbol}.TW", f"{symbol}.TWO"]
                    )
                    
                    result = cursor.fetchone()
                    if result:
                        found_symbol = result[0] if isinstance(result, (list, tuple)) else result.get('symbol')
                        if found_symbol:
                            symbol = found_symbol  # 使用找到的完整格式
            
            params = [symbol]
            
            if start_date:
                query = sql.Composed([query, sql.SQL(" AND date >= %s")])
                params.append(start_date)

            if end_date:
                query = sql.Composed([query, sql.SQL(" AND date <= %s")])
                params.append(end_date)

            query = sql.Composed([query, sql.SQL(" ORDER BY date ASC")])

            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                return jsonify({
                    'success': True,
                    'data': [],
                    'count': 0,
                    'message': f'沒有找到 {symbol} 的股價數據'
                })
            
            price_data = []
            for row in results:
                record = {
                    'date': row['date'].strftime('%Y-%m-%d') if row['date'] else None,
                }
                
                # 根據實際欄位動態添加數據
                if 'open_price' in row:
                    record['open_price'] = float(row['open_price']) if row['open_price'] else None
                if 'high_price' in row:
                    record['high_price'] = float(row['high_price']) if row['high_price'] else None
                if 'low_price' in row:
                    record['low_price'] = float(row['low_price']) if row['low_price'] else None
                if 'close_price' in row:
                    record['close_price'] = float(row['close_price']) if row['close_price'] else None
                if 'volume' in row:
                    record['volume'] = int(row['volume']) if row['volume'] else None
                
                price_data.append(record)
            
            return jsonify({
                'success': True,
                'data': price_data,
                'count': len(price_data)
            })
            
            db_manager.connection.commit()
        except Exception as batch_error:
            db_manager.connection.rollback()
            logger.error(f"批次更新失敗: {batch_error}")
            errors.append({'symbol': 'batch', 'error': str(batch_error)})
        finally:
            db_manager.disconnect()
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"獲取 {symbol} 股價失敗: {e}")
        logger.error(f"詳細錯誤: {error_details}")
        return jsonify({
            'success': False,
            'error': f"查詢股價數據時發生錯誤: {str(e)}",
            'details': error_details if app.debug else None
        }), 500

@app.route('/api/stock/<symbol>/returns', methods=['GET'])
def get_stock_returns(symbol):
    """從資料庫獲取股票報酬率數據"""
    try:
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        table_override = request.args.get('table')
        
        # 連接資料庫
        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({
                'success': False,
                'error': '資料庫連接失敗'
            }), 500
        
        try:
            cursor = db_manager.connection.cursor()

            returns_table = db_manager.table_returns
            if table_override:
                returns_table = _resolve_table_override(cursor, str(table_override).strip())
            
            # 構建查詢語句
            query = sql.SQL(
                "SELECT date, daily_return, weekly_return, monthly_return, cumulative_return FROM {} WHERE symbol = %s"
            ).format(sql.Identifier(returns_table))
            
            # 支援多種股票代碼格式查詢
            # 如果輸入的是純數字代碼，嘗試匹配完整格式
            if symbol.isdigit():
                # 先嘗試直接查詢，如果沒有結果，再嘗試添加後綴
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {} WHERE symbol = %s").format(sql.Identifier(returns_table)),
                    [symbol],
                )
                result = cursor.fetchone()
                count = result[0] if isinstance(result, (list, tuple)) else result.get('count', 0)
                
                if count == 0:
                    # 嘗試查找帶有 .TW 或 .TWO 後綴的股票
                    cursor.execute(
                        sql.SQL("SELECT symbol FROM {} WHERE symbol IN (%s, %s) LIMIT 1").format(
                            sql.Identifier(returns_table)
                        ),
                        [f"{symbol}.TW", f"{symbol}.TWO"],
                    )
                    
                    result = cursor.fetchone()
                    if result:
                        found_symbol = result[0] if isinstance(result, (list, tuple)) else result.get('symbol')
                        if found_symbol:
                            symbol = found_symbol  # 使用找到的完整格式
            
            params = [symbol]
            
            if start_date:
                query = sql.Composed([query, sql.SQL(" AND date >= %s")])
                params.append(start_date)
            
            if end_date:
                query = sql.Composed([query, sql.SQL(" AND date <= %s")])
                params.append(end_date)

            query = sql.Composed([query, sql.SQL(" ORDER BY date ASC")])

            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # 轉換為字典格式
            returns_data = []
            for row in results:
                if isinstance(row, (list, tuple)):
                    date_val, daily_ret, weekly_ret, monthly_ret, cumulative_ret = row
                else:
                    date_val = row.get('date')
                    daily_ret = row.get('daily_return')
                    weekly_ret = row.get('weekly_return')
                    monthly_ret = row.get('monthly_return')
                    cumulative_ret = row.get('cumulative_return')
                
                returns_data.append({
                    'date': date_val.strftime('%Y-%m-%d') if date_val else None,
                    'daily_return': float(daily_ret) if daily_ret is not None else None,
                    'weekly_return': float(weekly_ret) if weekly_ret is not None else None,
                    'monthly_return': float(monthly_ret) if monthly_ret is not None else None,
                    'cumulative_return': float(cumulative_ret) if cumulative_ret is not None else None
                })
            
            # 計算實際返回的日期範圍
            actual_date_range = {}
            if returns_data:
                dates = [pd.to_datetime(record['date']) for record in returns_data if record['date']]
                if dates:
                    actual_date_range = {
                        'start': min(dates).strftime('%Y-%m-%d'),
                        'end': max(dates).strftime('%Y-%m-%d'),
                        'trading_days_count': len(dates)
                    }
            
            return jsonify({
                'success': True,
                'data': returns_data,
                'count': len(returns_data),
                'date_range': {
                    'requested': {
                        'start': start_date,
                        'end': end_date
                    },
                    'actual': actual_date_range
                }
            })
            
        finally:
            db_manager.disconnect()
            
    except Exception as e:
        logger.error(f"獲取 {symbol} 報酬率失敗: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/returns/compute', methods=['POST'])
def compute_returns_api():
    """觸發計算 tw_stock_returns 的 API。
    JSON body 支援參數：
      - symbol: 指定單一股票代碼（例如 2330.TW）；若未提供且 all=false，會自動以 all=true。
      - start: 起始日期 YYYY-MM-DD（可選）
      - end: 結束日期 YYYY-MM-DD（可選）
      - all: 是否處理所有在 tw_stock_prices 出現過的股票（預設 false）
      - limit: 當 all=true 時限制處理檔數（可選）
      - fillMissing/fill_missing: 僅計算尚未存在於 tw_stock_returns 的日期（布林，可選）
      - use_local_db: 使用本地資料庫（預設 false）
      - upload_to_neon: 同時上傳報酬率到 Neon 雲端資料庫（預設 false）
    回傳：{ success, total_written, symbols: [{symbol, written, ...}] }
    """
    try:
        if not request.is_json:
            return jsonify({'success': False, 'error': '需要 JSON body'}), 400

        body = request.get_json() or {}
        symbols = body.get('symbols')
        symbol = body.get('symbol')
        start = body.get('start')
        end = body.get('end')
        all_flag = bool(body.get('all', False))
        limit = body.get('limit')
        fill_missing = bool(body.get('fillMissing', body.get('fill_missing', False)))
        use_local_db = bool(body.get('use_local_db', False))
        upload_to_neon = bool(body.get('upload_to_neon', False))
        batch_size = body.get('batch_size')
        max_workers = body.get('max_workers')

        if isinstance(symbols, list):
            symbols = [s for s in symbols if isinstance(s, str) and s.strip()]
        else:
            symbols = None

        # 若未提供 symbol/symbols 且未指定 all，就預設 all=true
        if not symbol and not symbols and not all_flag:
            all_flag = True

        # use_local_db=True 表示使用本地資料庫，use_neon=False
        # use_local_db=False 表示使用 Neon 資料庫，use_neon=True
        use_neon = not use_local_db

        result = compute_returns_task(
            symbols=symbols,
            symbol=symbol,
            start=start,
            end=end,
            all=all_flag,
            limit=limit,
            fill_missing=fill_missing,
            use_neon=use_neon,
            upload_to_neon=upload_to_neon,
            batch_size=batch_size,
            max_workers=max_workers,
        )
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.exception('計算報酬率 API 錯誤')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/returns/compute_stream')
def compute_returns_stream():
    """以 Server-Sent Events 方式回傳報酬率計算進度"""
    try:
        params = request.args or {}

        def _to_bool(val, default=False):
            if val is None:
                return default
            if isinstance(val, bool):
                return val
            return str(val).lower() in ('1', 'true', 'yes', 'on')

        symbols = params.getlist('symbols') if hasattr(params, 'getlist') else None
        if symbols:
            symbols = [s for s in symbols if isinstance(s, str) and s.strip()]
        else:
            symbols = None

        symbol = params.get('symbol')
        start = params.get('start')
        end = params.get('end')
        all_flag = _to_bool(params.get('all'), False)
        limit = params.get('limit')
        limit = int(limit) if limit not in (None, '', 'null') else None
        fill_missing = _to_bool(params.get('fillMissing') or params.get('fill_missing'), False)
        use_local_db = _to_bool(params.get('use_local_db'), False)
        upload_to_neon = _to_bool(params.get('upload_to_neon'), False)
        batch_size = params.get('batch_size')
        max_workers = params.get('max_workers')
        try:
            batch_size = int(batch_size) if batch_size not in (None, '', 'null') else None
        except Exception:
            batch_size = None
        try:
            max_workers = int(max_workers) if max_workers not in (None, '', 'null') else None
        except Exception:
            max_workers = None

        if not symbol and not symbols and not all_flag:
            all_flag = True

        use_neon = not use_local_db

        progress_queue: Queue = Queue()

        def progress_callback(event: dict):
            try:
                progress_queue.put(event, timeout=1)
            except Exception:
                logger.exception("progress_queue put 失敗")

        def run_task():
            try:
                result = compute_returns_task(
                    symbols=symbols,
                    symbol=symbol,
                    start=start,
                    end=end,
                    all=all_flag,
                    limit=limit,
                    fill_missing=fill_missing,
                    use_neon=use_neon,
                    upload_to_neon=upload_to_neon,
                    batch_size=batch_size,
                    max_workers=max_workers,
                    progress_callback=progress_callback,
                )
                progress_queue.put({'event': 'summary', 'summary': result})
            except Exception as task_err:
                logger.exception('compute_returns_stream 執行失敗')
                progress_queue.put({'event': 'error', 'error': str(task_err)})
            finally:
                progress_queue.put({'event': 'done'})
                progress_queue.put(None)

        worker = threading.Thread(target=run_task, daemon=True)
        worker.start()

        def event_stream():
            # 心跳避免前端斷線
            heartbeat_interval = 10
            last_heartbeat = time.time()
            while True:
                try:
                    item = progress_queue.get(timeout=1)
                    if item is None:
                        break
                    try:
                        payload = json.dumps(item, ensure_ascii=False)
                    except TypeError as encode_err:
                        logger.exception('progress encode 失敗: %s', encode_err)
                        payload = json.dumps({'event': 'error', 'error': 'encode_failed'})
                    yield f"data: {payload}\n\n"
                except Exception:
                    pass
                now = time.time()
                if now - last_heartbeat >= heartbeat_interval:
                    last_heartbeat = now
                    yield "data: {\"event\":\"heartbeat\"}\n\n"
        headers = {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
        return Response(event_stream(), headers=headers)
    except Exception as e:
        logger.exception('建立報酬率串流失敗')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/update', methods=['POST'])
def update_stocks():
    """批量更新股票數據"""
    db_manager = None
    acquired_update_lock = update_lock.acquire(blocking=False)
    if not acquired_update_lock:
        logger.warning("已有 /api/update 流程正在執行，拒絕並行請求")
        return jsonify({'success': False, 'error': '已有更新作業正在執行，請稍後再試'}), 429
    try:
        index_symbols = None

        def _parse_date_str(val: str | None):
            if not val:
                return None
            try:
                return datetime.strptime(val, '%Y-%m-%d')
            except Exception:
                return None

        def _normalize_date(value):
            if value is None:
                return None
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime().date().strftime('%Y-%m-%d')
            if isinstance(value, datetime):
                return value.date().strftime('%Y-%m-%d')
            if isinstance(value, date):
                return value.strftime('%Y-%m-%d')
            if isinstance(value, str):
                return value[:10]
            return None

        # 檢查請求數據
        if not request.is_json:
            return jsonify({
                'success': False,
                'error': '請求必須是 JSON 格式'
            }), 400
        
        data = request.get_json()
        if data is None:
            return jsonify({
                'success': False,
                'error': '無效的 JSON 數據'
            }), 400
            
        symbols = data.get('symbols', [])
        start_date = data.get('start_date', DEFAULT_START_DATE)
        end_date = data.get('end_date', None)  # 添加 end_date 參數
        update_prices = data.get('update_prices', True)
        force_full_refresh = bool(data.get('force_full_refresh', False))
        force_start_date = data.get('force_start_date')
        respect_requested_range = bool(data.get('respect_requested_range', False))
        # 新增：是否使用批量抓取模式（預設 True，可提升速度）
        use_batch_mode = bool(data.get('use_batch_mode', True))
        # 是否同步計算報酬率（預設啟用）
        update_returns = bool(data.get('update_returns', True))
        # ⚠️ 預設不再同步加權指數，僅在明確指定 fetch_market_index=true 時才執行
        fetch_market_index = bool(data.get('fetch_market_index', False))
        # 指定要同步哪些市場指數（例如 ['^OTC']），未指定時預設同時同步 ^TWII 與 ^OTC
        if index_symbols is None:
            try:
                req_indices = data.get('index_symbols') if isinstance(data, dict) else None
                if isinstance(req_indices, list) and req_indices:
                    index_symbols = [str(s) for s in req_indices]
                else:
                    index_symbols = ['^TWII', '^OTC'] if fetch_market_index else []
            except Exception:
                index_symbols = ['^TWII', '^OTC'] if fetch_market_index else []
        # 若只想同步市場指數（例如 ^TWII / ^OTC），避免 symbols 為空時自動載入股票清單
        only_market_index = bool(data.get('only_market_index', False))
        
        if not symbols:
            if only_market_index and fetch_market_index:
                symbols = []
            else:
                # 如果沒有指定股票，獲取所有股票
                try:
                    all_symbols = stock_api.get_all_symbols()
                    symbols = [s['symbol'] for s in all_symbols[:50]]  # 限制50檔避免超時
                except Exception as e:
                    logger.error(f"獲取股票代碼失敗: {e}")
                    return jsonify({
                        'success': False,
                        'error': f'獲取股票代碼失敗: {str(e)}'
                    }), 500
        
        results = []
        errors = []
        missing_symbols_batch = []
        missing_symbols_individual = []
        
        # 連接資料庫
        db_manager = DatabaseManager.from_request_payload(data)
        logger.info("嘗試連接資料庫...")
        if not db_manager.connect():
            logger.error("資料庫連接失敗")
            return jsonify({
                'success': False,
                'error': '資料庫連接失敗'
            }), 500
        
        logger.info("資料庫連接成功，檢查連接狀態...")
        if db_manager.connection is None:
            logger.error("資料庫連接物件為 None")
            return jsonify({
                'success': False,
                'error': '資料庫連接物件為空'
            }), 500
        
        cursor = None

        # 確保資料庫表格存在
        try:
            db_manager.create_tables()
            logger.info("資料庫表格檢查/建立完成")
        except Exception as e:
            logger.error(f"建立資料庫表格失敗: {e}")
            return jsonify({
                'success': False,
                'error': f'建立資料庫表格失敗: {str(e)}'
            }), 500

        def _reconnect_db():
            nonlocal cursor
            try:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = None
                db_manager.disconnect()
            except Exception:
                pass
            if not db_manager.connect():
                raise RuntimeError('資料庫重新連線失敗')
            db_manager.create_tables()
            cursor = db_manager.connection.cursor()

        def _execute_values_with_retry(upsert_sql, values, *, page_size=1000, max_retries=1):
            last_err = None
            for attempt in range(max_retries + 1):
                try:
                    if db_manager.connection is None or getattr(db_manager.connection, 'closed', 1) != 0:
                        _reconnect_db()
                    cur_local = db_manager.connection.cursor()
                    try:
                        execute_values(cur_local, upsert_sql, values, page_size=page_size)
                        db_manager.connection.commit()
                        return
                    finally:
                        try:
                            cur_local.close()
                        except Exception:
                            pass
                except psycopg2.Error as e:
                    last_err = e
                    msg = str(e)
                    # 若交易已中止，必須先 rollback，否則後續 SQL 會全部被拒絕
                    try:
                        if db_manager.connection is not None:
                            db_manager.connection.rollback()
                    except Exception:
                        pass
                    retryable = (
                        isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError))
                        or 'connection already closed' in msg
                        or 'cursor already closed' in msg
                        or 'SSL connection has been closed unexpectedly' in msg
                        or 'current transaction is aborted' in msg
                    )
                    if attempt >= max_retries or not retryable:
                        raise
                    try:
                        _reconnect_db()
                    except Exception:
                        raise last_err
        
        returns_started = False
        try:
            # 再次確認連接狀態
            if db_manager.connection is None:
                logger.error("資料庫連接在 create_tables 後變為 None")
                return jsonify({
                    'success': False,
                    'error': '資料庫連接丟失'
                }), 500
                
            cursor = db_manager.connection.cursor()
            prices_table = db_manager.table_prices

            # 預先查詢每個 symbol 在 prices/returns 的最新日期，用於增量更新
            latest_price_date_map = {}
            try:
                if symbols:
                    # 使用單次查詢獲取所有 symbols 的最新日期
                    placeholders = ','.join(['%s'] * len(symbols))
                    cursor.execute(f"""
                        SELECT symbol, MAX(date) AS max_date
                        FROM {prices_table}
                        WHERE symbol IN ({placeholders})
                        GROUP BY symbol
                    """, symbols)
                    for row in cursor.fetchall():
                        # row 是 RealDictCursor，鍵為 'symbol', 'max_date'
                        latest_price_date_map[row['symbol']] = row['max_date']
            except Exception as e:
                # 若前序 SQL 失敗導致交易中止，必須 rollback 才能繼續執行後續指數同步
                if isinstance(e, psycopg2.Error):
                    try:
                        db_manager.connection.rollback()
                    except Exception:
                        pass
                logger.warning(f"查詢最新股價日期失敗，將以請求日期為準: {e}")
                latest_price_date_map = {}

            # 同步市場指數的資料範圍（加權、櫃買）
            index_sync_summary = []
            if update_prices and fetch_market_index:
                for index_symbol in index_symbols:
                    try:
                        # 若交易處於 aborted 狀態，先 rollback 以恢復可用狀態
                        try:
                            if getattr(db_manager, 'connection', None) is not None:
                                db_manager.connection.rollback()
                        except Exception:
                            pass
                        cursor.execute(
                            f"SELECT MAX(date) AS max_date FROM {prices_table} WHERE symbol = %s",
                            (index_symbol,)
                        )
                        row = cursor.fetchone() or {}
                        latest_index_date = row.get('max_date') if isinstance(row, dict) else (row[0] if row else None)

                        requested_start = force_start_date or start_date or DEFAULT_START_DATE
                        requested_end = end_date or datetime.now().strftime('%Y-%m-%d')

                        effective_index_start = requested_start
                        if respect_requested_range and start_date:
                            effective_index_start = start_date
                        elif not force_full_refresh and latest_index_date is not None:
                            try:
                                next_day = (latest_index_date + timedelta(days=1)).strftime('%Y-%m-%d')
                                if next_day > effective_index_start:
                                    effective_index_start = next_day
                            except Exception:
                                pass

                        if effective_index_start and requested_end and effective_index_start <= requested_end:
                            logger.info(
                                f"同步市場指數 {index_symbol}，日期範圍: {effective_index_start} ~ {requested_end}"
                            )
                            index_data = stock_api.fetch_stock_data(index_symbol, effective_index_start, requested_end)
                            if index_data is not None:
                                if isinstance(index_data, pd.DataFrame):
                                    raw_records = index_data.to_dict('records')
                                else:
                                    raw_records = index_data if isinstance(index_data, list) else []

                                rows = []

                                def _to_float(val):
                                    if val in (None, '', '--', '---'):
                                        return None
                                    try:
                                        return float(val)
                                    except Exception:
                                        try:
                                            return float(str(val).replace(',', ''))
                                        except Exception:
                                            return None

                                def _to_int(val):
                                    if val in (None, '', '--', '---'):
                                        return 0
                                    try:
                                        return int(val)
                                    except Exception:
                                        try:
                                            return int(float(str(val).replace(',', '')))
                                        except Exception:
                                            return 0

                                for rec in raw_records:
                                    if not isinstance(rec, dict):
                                        continue
                                    date_val = _normalize_date(rec.get('date') or rec.get('Date'))
                                    if not date_val:
                                        continue
                                    rows.append(
                                        (
                                            index_symbol,
                                            date_val,
                                            _to_float(rec.get('open_price') or rec.get('Open') or rec.get('open')),
                                            _to_float(rec.get('high_price') or rec.get('High') or rec.get('high')),
                                            _to_float(rec.get('low_price') or rec.get('Low') or rec.get('low')),
                                            _to_float(rec.get('close_price') or rec.get('Close') or rec.get('close')),
                                            _to_int(rec.get('volume') or rec.get('Volume'))
                                        )
                                    )

                                if rows:
                                    upsert_sql = f"""
                                        INSERT INTO {prices_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
                                        VALUES %s
                                        ON CONFLICT (symbol, date) DO UPDATE SET
                                            open_price = EXCLUDED.open_price,
                                            high_price = EXCLUDED.high_price,
                                            low_price = EXCLUDED.low_price,
                                            close_price = EXCLUDED.close_price,
                                            volume = EXCLUDED.volume
                                    """
                                    _execute_values_with_retry(upsert_sql, rows, page_size=500)
                                    index_sync_summary.append({
                                        'symbol': index_symbol,
                                        'status': 'success',
                                        'prices_updated': len(rows),
                                        'mode': 'index'
                                    })
                                    logger.info(f"{index_symbol} 指數同步完成，寫入 {len(rows)} 筆資料")
                                else:
                                    logger.info(f"{index_symbol} 指數資料為空，略過寫入")
                            else:
                                logger.info(f"{index_symbol} 指數抓取結果為 None，略過寫入")
                        else:
                            logger.info(f"{index_symbol} 指數無需更新（起始日期晚於結束日期）")
                    except Exception as index_exc:
                        logger.exception(f"同步指數 {index_symbol} 失敗: {index_exc}")
                        if isinstance(index_exc, psycopg2.Error):
                            try:
                                db_manager.connection.rollback()
                            except Exception:
                                pass
                        errors.append({'symbol': index_symbol, 'error': str(index_exc)})

            def _process_symbols_individual(symbols_to_process):
                for i, symbol in enumerate(symbols_to_process):
                    try:
                        result = {'symbol': symbol, 'status': 'success'}
                        
                        if update_prices:
                            existing_total = None
                            try:
                                cursor.execute(
                                    f"SELECT COUNT(*) AS total FROM {prices_table} WHERE symbol = %s",
                                    (symbol,)
                                )
                                total_row = cursor.fetchone()
                                if total_row and 'total' in total_row:
                                    existing_total = total_row['total']
                            except Exception as e:
                                logger.warning(f"統計 {symbol} 現有筆數失敗: {e}")

                            # 決定實際開始日期
                            effective_start_date = force_start_date or start_date

                            if respect_requested_range and start_date:
                                effective_start_date = start_date
                                logger.info(
                                    f"尊重請求範圍: {symbol} 將以 {effective_start_date} 作為起始日期"
                                )
                            elif not force_full_refresh:
                                # 增量更新：若資料庫已有資料，從最新日期的翌日開始抓取
                                latest_dt = latest_price_date_map.get(symbol)
                                if latest_dt is not None:
                                    try:
                                        next_day = (latest_dt + timedelta(days=1)).strftime('%Y-%m-%d')
                                        if end_date is None or next_day <= (end_date or next_day):
                                            if next_day > effective_start_date:
                                                effective_start_date = next_day
                                    except Exception as _:
                                        pass
                            else:
                                logger.info(f"force_full_refresh 啟用，將以 {effective_start_date} 作為起始日期")

                            logger.info(f"獲取 {symbol} 股價數據，請求日期範圍: {effective_start_date} 到 {end_date}")
                            price_data = stock_api.fetch_stock_data(symbol, effective_start_date, end_date)
                            
                            if price_data is not None and (
                                (isinstance(price_data, pd.DataFrame) and not price_data.empty) or
                                (isinstance(price_data, list) and len(price_data) > 0)
                            ):
                                # 儲存股價數據到資料庫（批量 upsert）
                                dates = []

                                # 標準化資料為 list[dict]
                                if isinstance(price_data, pd.DataFrame):
                                    price_records = price_data.to_dict('records')
                                else:
                                    price_records = price_data

                                # 準備批量資料
                                values = []
                                dedup = {}
                                for pr in price_records:
                                    record_date = pr.get('date') or pr.get('Date')
                                    if not record_date:
                                        continue
                                    key = (symbol, str(record_date)[:10])
                                    dedup[key] = (
                                        symbol,
                                        str(record_date)[:10],
                                        pr.get('open_price') or pr.get('Open'),
                                        pr.get('high_price') or pr.get('High'),
                                        pr.get('low_price') or pr.get('Low'),
                                        pr.get('close_price') or pr.get('Close'),
                                        pr.get('volume') or pr.get('Volume')
                                    )
                                if dedup:
                                    values = list(dedup.values())
                                    dates.extend([v[1] for v in values])

                                duplicate_count = 0
                                if values:
                                    # 在 upsert 前統計資料庫已存在的日期（重複筆數）
                                    try:
                                        date_list = [v[1] for v in values]
                                        logger.info(f"📊 {symbol} 準備檢查重複：本次抓取 {len(date_list)} 筆數據")
                                        
                                        if date_list:
                                            # 動態 placeholders 查詢既有日期
                                            date_placeholders = ','.join(['%s'] * len(date_list))
                                            cursor.execute(
                                                f"""
                                                    SELECT date FROM {prices_table}
                                                    WHERE symbol = %s AND date IN ({date_placeholders})
                                                """,
                                                [symbol] + date_list
                                            )
                                            existing_rows = cursor.fetchall()
                                            logger.info(f"🔍 {symbol} 資料庫查詢：找到 {len(existing_rows)} 筆已存在記錄")
                                            
                                            # 正規化為字串日期集合
                                            existing_dates = set()
                                            for row in existing_rows:
                                                d = row['date']
                                                existing_dates.add(d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d))
                                            # 與 values 的日期交集
                                            duplicate_count = len(existing_dates)
                                            
                                            logger.info(f"📋 {symbol} 重複統計：duplicate_count = {duplicate_count}")
                                            if duplicate_count > 0:
                                                logger.info(f"⚠️ {symbol} 重複日期範例：{list(existing_dates)[:5]}")
                                            else:
                                                logger.info(f"✅ {symbol} 無重複，全部為新數據")
                                    except Exception as e:
                                        # 若是 psycopg2 造成交易中止，需 rollback 才能繼續後續 SQL
                                        if isinstance(e, psycopg2.Error):
                                            try:
                                                db_manager.connection.rollback()
                                            except Exception:
                                                pass
                                        logger.warning(f"統計 {symbol} 既有日期失敗，略過重複統計: {e}")
                                        duplicate_count = 0

                                    upsert_sql = f"""
                                        INSERT INTO {prices_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
                                        VALUES %s
                                        ON CONFLICT (symbol, date) DO UPDATE SET
                                            open_price = EXCLUDED.open_price,
                                            high_price = EXCLUDED.high_price,
                                            low_price = EXCLUDED.low_price,
                                            close_price = EXCLUDED.close_price,
                                            volume = EXCLUDED.volume
                                    """
                                    try:
                                        _execute_values_with_retry(upsert_sql, values, page_size=1000)
                                    except Exception as e:
                                        logger.warning(f"批量寫入 {symbol} 價格數據失敗，將嘗試較小批次: {e}")
                                        # 回退為小批次
                                        batch = 200
                                        for idx in range(0, len(values), batch):
                                            sub = values[idx:idx+batch]
                                            _execute_values_with_retry(upsert_sql, sub, page_size=len(sub))
                                else:
                                    # 沒有任何有效資料
                                    missing_symbols_individual.append(symbol)
                                # 真正新增筆數 = 擬寫入總筆數 - 已存在筆數（近似計算）
                                new_insert_count = max(len(values) - duplicate_count, 0)
                                result['price_records'] = new_insert_count
                                result['duplicate_records'] = duplicate_count
                                if existing_total is not None:
                                    result['existing_records'] = existing_total + new_insert_count

                                # 添加日期範圍資訊
                                if dates:
                                    dates = sorted([str(d) for d in dates])
                                    result['price_date_range'] = {
                                        'start': dates[0],
                                        'end': dates[-1],
                                        'requested_start': start_date,
                                        'requested_end': end_date,
                                        'trading_days_count': len(dates)
                                    }
                            else:
                                result['price_records'] = 0
                                result['status'] = 'partial'
                                missing_symbols_individual.append(symbol)
                                if existing_total is not None:
                                    result['existing_records'] = existing_total
                        
                        # 報酬率計算已停用（僅處理股價）
                        # 如需啟用，請使用 /api/returns/compute 端點
                        pass  # 僅處理股價，不計算報酬率
                        
                        if False:  # 報酬率計算區塊已完全停用
                            pass  # 以下代碼不會執行
                        
                        results.append(result)
                    
                    except Exception as e:
                        errors.append({'symbol': symbol, 'error': str(e)})
                        logger.error(f"更新 {symbol} 失敗: {e}")

            # 🚀 批量抓取模式：一次性抓取所有股票
            if update_prices and use_batch_mode and len(symbols) > 1:
                logger.info(f"🚀 啟用批量抓取模式，準備抓取 {len(symbols)} 檔股票")
                
                # 過濾出上市股票代碼（去除 .TW 後綴）
                twse_codes = []
                tpex_codes = []
                for sym in symbols:
                    if sym.endswith('.TW'):
                        code = sym.split('.')[0]
                        if code.isdigit():
                            twse_codes.append(code)
                    elif sym.endswith('.TWO'):
                        code = sym.split('.')[0]
                        if code.isdigit():
                            tpex_codes.append(code)

                processed_twse_symbols = set()
                processed_tpex_symbols = set()
                
                if twse_codes:
                    # 決定實際開始日期
                    effective_start_date = force_start_date or start_date
                    if not end_date:
                        end_date = datetime.now().strftime('%Y-%m-%d')
                    
                    logger.info(f"批量抓取 {len(twse_codes)} 檔上市股票，日期範圍: {effective_start_date} ~ {end_date}")
                    
                    # 批量抓取
                    batch_data = stock_api.fetch_twse_stock_data_batch(twse_codes, effective_start_date, end_date)
                    
                    # 將批量數據寫入資料庫
                    for stock_code, price_records in batch_data.items():
                        symbol = f"{stock_code}.TW"
                        try:
                            if price_records:
                                dedup = {}
                                for pr in price_records:
                                    d = pr.get('Date')
                                    if not d:
                                        continue
                                    key = (symbol, str(d)[:10])
                                    dedup[key] = (
                                        symbol,
                                        str(d)[:10],
                                        pr.get('Open'),
                                        pr.get('High'),
                                        pr.get('Low'),
                                        pr.get('Close'),
                                        pr.get('Volume')
                                    )
                                values = list(dedup.values())
                                
                                if values:
                                    upsert_sql = f"""
                                        INSERT INTO {prices_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
                                        VALUES %s
                                        ON CONFLICT (symbol, date) DO UPDATE SET
                                            open_price = EXCLUDED.open_price,
                                            high_price = EXCLUDED.high_price,
                                            low_price = EXCLUDED.low_price,
                                            close_price = EXCLUDED.close_price,
                                            volume = EXCLUDED.volume
                                    """
                                    _execute_values_with_retry(upsert_sql, values, page_size=1000)

                                    processed_twse_symbols.add(symbol)
                                    
                                    results.append({
                                        'symbol': symbol,
                                        'status': 'success',
                                        'prices_updated': len(values),
                                        'mode': 'batch'
                                    })
                                    logger.info(f"✅ {symbol} 批量寫入 {len(values)} 筆")
                                else:
                                    missing_symbols_batch.append(symbol)
                            else:
                                missing_symbols_batch.append(symbol)
                        except Exception as e:
                            logger.error(f"批量寫入 {symbol} 失敗: {e}")
                            errors.append({'symbol': symbol, 'error': str(e)})
                    
                    logger.info(f"🎉 批量抓取完成，成功處理 {len(results)} 檔股票")

                if tpex_codes:
                    effective_start_date = force_start_date or start_date
                    if not end_date:
                        end_date = datetime.now().strftime('%Y-%m-%d')

                    logger.info(f"批量抓取 {len(tpex_codes)} 檔上櫃股票，日期範圍: {effective_start_date} ~ {end_date}")
                    batch_data = stock_api.fetch_tpex_stock_data_batch(tpex_codes, effective_start_date, end_date)

                    # 將全部股票的值一次性 upsert，避免逐檔執行多次 SQL 造成開銷
                    all_values_for_db = []
                    per_symbol_counts = {}

                    for stock_code, price_records in batch_data.items():
                        symbol = f"{stock_code}.TWO"
                        try:
                            if price_records:
                                dedup = {}
                                for pr in price_records:
                                    d = pr.get('Date')
                                    if not d:
                                        continue
                                    key = (symbol, str(d)[:10])
                                    dedup[key] = (
                                        symbol,
                                        str(d)[:10],
                                        pr.get('Open'),
                                        pr.get('High'),
                                        pr.get('Low'),
                                        pr.get('Close'),
                                        pr.get('Volume')
                                    )

                                values = list(dedup.values())

                                if values:
                                    all_values_for_db.extend(values)
                                    per_symbol_counts[symbol] = per_symbol_counts.get(symbol, 0) + len(values)
                                    processed_tpex_symbols.add(symbol)
                                else:
                                    missing_symbols_batch.append(symbol)
                            else:
                                missing_symbols_batch.append(symbol)
                        except Exception as e:
                            logger.error(f"批量寫入 {symbol} 失敗: {e}")
                            errors.append({'symbol': symbol, 'error': str(e)})

                    if all_values_for_db:
                        upsert_sql = f"""
                            INSERT INTO {prices_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
                            VALUES %s
                            ON CONFLICT (symbol, date) DO UPDATE SET
                                open_price = EXCLUDED.open_price,
                                high_price = EXCLUDED.high_price,
                                low_price = EXCLUDED.low_price,
                                close_price = EXCLUDED.close_price,
                                volume = EXCLUDED.volume
                        """
                        _execute_values_with_retry(upsert_sql, all_values_for_db, page_size=2000)

                        for sym, cnt in per_symbol_counts.items():
                            results.append({
                                'symbol': sym,
                                'status': 'success',
                                'prices_updated': cnt,
                                'mode': 'batch'
                            })
                            logger.info(f"✅ {sym} 批量寫入 {cnt} 筆")
                    else:
                        # 全部空資料
                        missing_symbols_batch.extend([f"{code}.TWO" for code in tpex_codes])

                remaining_symbols = [s for s in symbols if s not in processed_twse_symbols and s not in processed_tpex_symbols]

            # 🔄 逐檔抓取模式（備用或非批量模式）
            else:
                remaining_symbols = symbols

            if remaining_symbols:
                _process_symbols_individual(remaining_symbols)

            # ✅ 股價更新完成後，自動啟動報酬率計算（背景執行，避免阻塞 API 回應）
            if update_returns:
                try:
                    use_neon = not bool(data.get('use_local_db', False))
                    returns_end = end_date or datetime.now().strftime('%Y-%m-%d')

                    returns_t0 = time.perf_counter()
                    returns_last_emit_t = 0.0
                    returns_last_index = 0

                    def _returns_progress(event: dict):
                        try:
                            # 確保 payload 可 JSON 序列化
                            safe = event if isinstance(event, dict) else {'event': 'progress', 'message': str(event)}
                            evt = safe.get('event', 'progress')

                            nonlocal returns_last_emit_t, returns_last_index
                            msg = safe.get('message')

                            # 針對 progress 事件補齊 message（前端較容易顯示）
                            if evt == 'progress':
                                idx = int(safe.get('index') or 0)
                                total = int(safe.get('total') or 0)

                                # 簡易節流：至少前進 1 檔，且每 0.25 秒最多推一次
                                now_t = time.perf_counter()
                                if idx <= returns_last_index and (now_t - returns_last_emit_t) < 0.25:
                                    return
                                returns_last_index = max(returns_last_index, idx)
                                returns_last_emit_t = now_t

                                pct = round((idx / total) * 100, 2) if total else None
                                elapsed_s = now_t - returns_t0
                                eta_s = None
                                if idx > 0 and total and elapsed_s > 0:
                                    avg = elapsed_s / idx
                                    eta_s = max(0.0, avg * (total - idx))

                                sym = safe.get('symbol')
                                written = safe.get('written')
                                reason = safe.get('reason')
                                error = safe.get('error')

                                parts = []
                                if total:
                                    parts.append(f"{idx}/{total}")
                                if pct is not None:
                                    parts.append(f"{pct}%")
                                if sym:
                                    parts.append(str(sym))
                                if error:
                                    parts.append(f"ERROR: {error}")
                                else:
                                    if written is not None:
                                        parts.append(f"written={written}")
                                    if reason:
                                        parts.append(f"reason={reason}")
                                if eta_s is not None:
                                    eta_min = int(eta_s // 60)
                                    eta_sec = int(eta_s % 60)
                                    parts.append(f"ETA {eta_min}m{eta_sec:02d}s")

                                msg = "🧮 報酬率進度: " + " | ".join(parts)
                                safe['progress_pct'] = pct
                                safe['eta_seconds'] = int(eta_s) if eta_s is not None else None

                            push_sse('returns', evt, msg, **safe)
                        except Exception:
                            pass

                    def _run_returns():
                        try:
                            logger.info("📈 開始計算報酬率")
                            push_sse('returns', 'start', '開始計算報酬率')
                            report = compute_returns_task(
                                symbols=symbols,
                                start=start_date,
                                end=returns_end,
                                all=False,
                                limit=None,
                                fill_missing=True,
                                use_neon=use_neon,
                                upload_to_neon=False,
                                progress_callback=_returns_progress,
                            )
                            push_sse('returns', 'summary', '報酬率計算完成', summary=report)
                            push_sse('returns', 'done', '報酬率計算完成')
                            logger.info("📈 報酬率計算完成")
                        except Exception as exc:
                            push_sse('returns', 'error', str(exc))
                            logger.error(f"報酬率計算失敗: {exc}")

                    threading.Thread(target=_run_returns, daemon=True).start()
                    returns_started = True
                except Exception as exc:
                    logger.error(f"初始化報酬率計算失敗: {exc}")

            db_manager.connection.commit()
        except Exception as batch_error:
            db_manager.connection.rollback()
            logger.error(f"批次更新失敗: {batch_error}")
            errors.append({'symbol': 'batch', 'error': str(batch_error)})
        finally:
            db_manager.disconnect()
        
        error_message = None
        if errors:
            try:
                error_message = errors[0].get('error') if isinstance(errors[0], dict) else str(errors[0])
            except Exception:
                error_message = None

        return jsonify({
            'success': len(errors) == 0,
            'results': results,
            'errors': errors,
            'index_sync_summary': index_sync_summary,
            'missing_symbols': {
                'batch': sorted(list(set(missing_symbols_batch))),
                'individual': sorted(list(set(missing_symbols_individual)))
            },
            'returns': {
                'enabled': bool(update_returns),
                'started': bool(returns_started)
            },
            'error': error_message,
            'summary': {
                'total': len(symbols),
                'success': len(results),
                'failed': len(errors)
            }
        })
    except Exception as e:
        logger.error(f"批量更新失敗: {e}")
        # 確保資料庫連接被關閉
        if db_manager:
            try:
                db_manager.disconnect()
            except Exception:
                pass
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        if acquired_update_lock:
            try:
                update_lock.release()
            except Exception:
                pass

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康檢查 - 包含資料庫連接狀態和數據統計"""
    try:
        # 檢查資料庫連接
        db_manager = DatabaseManager.from_request_args(request.args)
        db_connected, db_message = db_manager.test_connection()
        
        if db_connected:
            # 獲取資料庫統計資訊
            try:
                if db_manager.connect():
                    cursor = db_manager.connection.cursor()
                    
                    # 查詢股價數據統計
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_records, 
                            COUNT(DISTINCT symbol) as unique_stocks,
                            MIN(date) as earliest_date,
                            MAX(date) as latest_date
                        FROM tw_stock_prices;
                    """)
                    price_stats = cursor.fetchone()
                    
                    # 查詢報酬率數據統計
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_records, 
                            COUNT(DISTINCT symbol) as unique_stocks,
                            MIN(date) as earliest_date,
                            MAX(date) as latest_date
                        FROM tw_stock_returns;
                    """)
                    return_stats = cursor.fetchone()
                    
                    # 獲取資料庫連接資訊
                    db_info = db_manager.connection_info()
                    
                    db_manager.disconnect()
                    
                    return jsonify({
                        'status': 'healthy',
                        'database': 'connected',
                        'database_info': db_message,
                        'database_connection': db_info,
                        'data_statistics': {
                            'tw_stock_prices': {
                                'total_records': price_stats['total_records'],
                                'unique_stocks': price_stats['unique_stocks'],
                                'date_range': {
                                    'earliest': price_stats['earliest_date'].isoformat() if price_stats['earliest_date'] else None,
                                    'latest': price_stats['latest_date'].isoformat() if price_stats['latest_date'] else None
                                }
                            },
                            'tw_stock_returns': {
                                'total_records': return_stats['total_records'],
                                'unique_stocks': return_stats['unique_stocks'],
                                'date_range': {
                                    'earliest': return_stats['earliest_date'].isoformat() if return_stats['earliest_date'] else None,
                                    'latest': return_stats['latest_date'].isoformat() if return_stats['latest_date'] else None
                                }
                            }
                        },
                        'timestamp': datetime.now().isoformat(),
                        'version': '1.0.0'
                    })
                else:
                    return jsonify({
                        'status': 'healthy',
                        'database': 'connected',
                        'database_info': db_message,
                        'timestamp': datetime.now().isoformat(),
                        'version': '1.0.0'
                    })
            except Exception as stats_error:
                return jsonify({
                    'status': 'healthy',
                    'database': 'connected',
                    'database_info': db_message,
                    'stats_error': str(stats_error),
                    'timestamp': datetime.now().isoformat(),
                    'version': '1.0.0'
                })
        else:
            return jsonify({
                'status': 'warning',
                'database': 'disconnected',
                'database_error': db_message,
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0'
            }), 503
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        }), 500

@app.route('/api/test-connection', methods=['GET'])
def test_connection():
    """測試資料庫連接"""
    try:
        db_manager = DatabaseManager.from_request_args(request.args)
        info = db_manager.connection_info()
        logger.info(
            "🔌 測試連線 -> driver=%s host=%s port=%s db=%s user=%s sslmode=%s",
            info.get('driver'),
            info.get('host'),
            info.get('port'),
            info.get('database'),
            info.get('user'),
            info.get('sslmode')
        )
        if db_manager.connect():
            db_manager.disconnect()
            return jsonify({
                'success': True,
                'status': 'connected',
                'message': '資料庫連接成功',
                'connection': info
            })
        else:
            return jsonify({
                'success': False,
                'status': 'disconnected',
                'message': '資料庫連接失敗',
                'connection': info
            }), 500
    except Exception as e:
        logger.error(f"測試資料庫連接錯誤: {e}")
        return jsonify({
            'success': False,
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/stocks/<symbol>/price-history', methods=['GET'])
def get_price_history(symbol):
    """獲取股票K線歷史數據 - 用於前端圖表展示"""
    try:
        period = request.args.get('period', '1M')
        
        # 根據period參數確定天數
        period_days = {
            '1D': 60,
            '1W': 90,
            '1M': 120,
            '3M': 180,
            '6M': 365,
            '1Y': 730
        }
        days = period_days.get(period, 120)
        
        # 計算起始日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"獲取 {symbol} K線數據，period={period}, days={days}")
        
        # 連接數據庫
        db_manager = DatabaseManager()
        if not db_manager.connect():
            return jsonify({
                'code': 500,
                'message': '資料庫連接失敗',
                'data': None
            }), 500
        
        try:
            cursor = db_manager.connection.cursor()
            
            # 查詢K線數據
            query = """
                SELECT 
                    date,
                    open_price as open,
                    high_price as high,
                    low_price as low,
                    close_price as close,
                    volume
                FROM tw_stock_prices
                WHERE symbol = %s
                    AND date >= %s
                    AND date <= %s
                    AND open_price IS NOT NULL
                    AND close_price IS NOT NULL
                ORDER BY date ASC
            """
            
            cursor.execute(query, (symbol, start_date.date(), end_date.date()))
            results = cursor.fetchall()
            
            # 轉換為前端需要的格式
            data = []
            for row in results:
                data.append({
                    'date': row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], (datetime, date)) else str(row['date']),
                    'open': float(row['open']) if row['open'] is not None else None,
                    'high': float(row['high']) if row['high'] is not None else None,
                    'low': float(row['low']) if row['low'] is not None else None,
                    'close': float(row['close']) if row['close'] is not None else None,
                    'volume': int(row['volume']) if row['volume'] is not None else 0
                })
            
            cursor.close()
            db_manager.disconnect()
            
            logger.info(f"成功獲取 {symbol} {len(data)} 條K線數據")
            
            return jsonify({
                'code': 0,
                'message': 'success',
                'data': data
            })
            
        except Exception as e:
            logger.error(f"查詢K線數據錯誤: {e}")
            db_manager.disconnect()
            return jsonify({
                'code': 500,
                'message': f'查詢失敗: {str(e)}',
                'data': None
            }), 500
            
    except Exception as e:
        logger.error(f"獲取K線數據錯誤: {e}")
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@app.route('/api/stocks/<symbol>/quote', methods=['GET'])
def get_stock_quote(symbol):
    """取得個股最新報價與漲跌幅"""
    try:
        db_manager = DatabaseManager()
        if not db_manager.connect():
            return jsonify({
                'code': 500,
                'message': '資料庫連接失敗',
                'data': None
            }), 500

        cursor = db_manager.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT date,
                       open_price,
                       high_price,
                       low_price,
                       close_price,
                       volume
                FROM tw_stock_prices
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 2
                """,
                (symbol,)
            )
            rows = cursor.fetchall()

            if not rows:
                return jsonify({
                    'code': 404,
                    'message': f'找不到 {symbol} 的報價資料',
                    'data': None
                }), 404

            latest = rows[0]
            previous = rows[1] if len(rows) > 1 else None

            latest_close = float(latest['close_price']) if latest['close_price'] is not None else None
            previous_close = float(previous['close_price']) if previous and previous['close_price'] is not None else None

            if latest_close is None:
                return jsonify({
                    'code': 404,
                    'message': f'{symbol} 缺少收盤價資料',
                    'data': None
                }), 404

            change = latest_close - previous_close if previous_close is not None else 0.0
            change_pct = (change / previous_close * 100) if previous_close not in (None, 0) else 0.0

            data = {
                'symbol': symbol,
                'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], (datetime, date)) else str(latest['date']),
                'open': float(latest['open_price']) if latest['open_price'] is not None else None,
                'high': float(latest['high_price']) if latest['high_price'] is not None else None,
                'low': float(latest['low_price']) if latest['low_price'] is not None else None,
                'close': latest_close,
                'volume': int(latest['volume']) if latest['volume'] is not None else 0,
                'change': round(change, 4),
                'changePercent': round(change_pct, 4)
            }

            return jsonify({
                'code': 0,
                'message': 'success',
                'data': data
            })

        except Exception as e:
            logger.error(f"查詢報價資料錯誤: {e}")
            return jsonify({
                'code': 500,
                'message': f'查詢失敗: {str(e)}',
                'data': None
            }), 500
        finally:
            cursor.close()
            db_manager.disconnect()

    except Exception as e:
        logger.error(f"取得報價資料錯誤: {e}")
        return jsonify({
            'code': 500,
            'message': str(e),
            'data': None
        }), 500


@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """獲取資料庫統計信息"""
    try:
        db_manager = DatabaseManager()
        if not db_manager.connect():
            return jsonify({
                'success': False,
                'error': '無法連接到資料庫'
            }), 500
        
        cursor = db_manager.connection.cursor()
        
        # 初始化統計數據
        total_records = 0
        unique_stocks = 0
        date_range_result = None
        last_update_result = None
        
        try:
            # 獲取總記錄數 - 檢查表是否存在
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'tw_stock_prices'
            """)
            if cursor.fetchone()[0] > 0:
                cursor.execute("SELECT COUNT(*) FROM tw_stock_prices")
                total_records = cursor.fetchone()[0]
                
                # 獲取日期範圍
                cursor.execute("""
                    SELECT MIN(date) as start_date, MAX(date) as end_date 
                    FROM tw_stock_prices 
                    WHERE date IS NOT NULL
                """)
                date_range_result = cursor.fetchone()
                
                # 獲取最後更新時間
                cursor.execute("""
                    SELECT MAX(updated_at) as last_update 
                    FROM tw_stock_prices 
                    WHERE updated_at IS NOT NULL
                """)
                last_update_result = cursor.fetchone()
        except Exception as e:
            logger.warning(f"tw_stock_prices 表查詢錯誤: {e}")
        
        try:
            # 獲取股票數量 - 檢查表是否存在
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'stock_symbols'
            """)
            if cursor.fetchone()[0] > 0:
                cursor.execute("SELECT COUNT(DISTINCT symbol) FROM stock_symbols")
                unique_stocks = cursor.fetchone()[0]
        except Exception as e:
            logger.warning(f"stock_symbols 表查詢錯誤: {e}")
        
        db_manager.disconnect()
        
        # 準備統計數據
        stats = {
            'totalRecords': total_records or 0,
            'uniqueStocks': unique_stocks or 0,
            'dateRange': {
                'start': str(date_range_result[0]) if date_range_result[0] else None,
                'end': str(date_range_result[1]) if date_range_result[1] else None
            } if date_range_result else None,
            'lastUpdate': str(last_update_result[0]) if last_update_result and last_update_result[0] else None
        }
        
        return jsonify({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        logger.error(f"獲取統計信息錯誤: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/database-sync/status', methods=['GET'])
def database_sync_status():
    """檢查 Neon 資料庫連接狀態"""
    try:
        neon_url = (
            os.environ.get('DATABASE_URL')
            or os.environ.get('NEON_DATABASE_URL')
        )
        if not neon_url:
            return jsonify({
                'success': False,
                'connected': False,
                'error': 'NEON_DATABASE_URL not configured'
            })
        
        # 測試連接
        conn = psycopg2.connect(neon_url, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        cursor.execute('SELECT NOW() as current_time')
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'connected': True,
            'currentTime': result['current_time'].isoformat() if result else None
        })
    except Exception as e:
        logger.error(f"Neon 連接檢查失敗: {e}")
        return jsonify({
            'success': False,
            'connected': False,
            'error': str(e)
        })

@app.route('/api/database-sync/tables', methods=['GET'])
def get_database_tables():
    """獲取指定資料庫（本機/Neon）的所有表格列表"""
    try:
        source = (request.args.get('source') or '').strip().lower()
        use_local_flag = DatabaseManager._resolve_use_local(request.args.get('use_local_db'))

        # Compatibility:
        # - if source is provided, prefer it
        # - else fall back to use_local_db
        is_local = True
        if source == 'neon':
            is_local = False
        elif source == 'local':
            is_local = True
        else:
            is_local = bool(use_local_flag)

        if is_local:
            conn = psycopg2.connect(
                host=os.environ.get('DB_HOST', 'localhost'),
                port=os.environ.get('DB_PORT', '5432'),
                user=os.environ.get('DB_USER', 'postgres'),
                password=os.environ.get('DB_PASSWORD', ''),
                database=os.environ.get('DB_NAME', 'postgres'),
                cursor_factory=RealDictCursor,
                sslmode=os.environ.get('DB_SSLMODE', 'prefer'),
            )
        else:
            neon_url = (
                os.environ.get('DATABASE_URL')
                or os.environ.get('NEON_DATABASE_URL')
            )
            if not neon_url:
                return jsonify({'success': False, 'error': 'NEON_DATABASE_URL not configured'}), 500
            from urllib.parse import urlparse
            parsed = urlparse(neon_url)
            conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip('/') if parsed.path else 'postgres',
                cursor_factory=RealDictCursor,
                sslmode='require',
            )

        cursor = conn.cursor()
        
        # 獲取所有表格及其行數
        cursor.execute("""
            SELECT 
                tablename,
                (SELECT COUNT(*) FROM information_schema.columns 
                 WHERE table_schema = 'public' AND table_name = t.tablename) as column_count
            FROM pg_tables t
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        tables = cursor.fetchall()
        
        # 獲取每個表格的行數
        table_list = []
        for table in tables:
            table_name = table['tablename']
            try:
                cursor.execute(
                    sql.SQL('SELECT COUNT(*) as count FROM {}').format(sql.Identifier(table_name))
                )
                row_count = cursor.fetchone()['count']
                table_list.append({
                    'name': table_name,
                    'rowCount': row_count,
                    'columnCount': table['column_count']
                })
            except Exception as e:
                logger.warning(f"無法獲取 {table_name} 的行數: {e}")
                table_list.append({
                    'name': table_name,
                    'rowCount': 0,
                    'columnCount': table['column_count']
                })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'tables': table_list
        })
    except Exception as e:
        logger.error(f"獲取表格列表失敗: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/database-sync/debug_routes', methods=['GET'])
def database_sync_debug_routes():
    try:
        routes = []
        for rule in app.url_map.iter_rules():
            if str(rule.rule).startswith('/api/database-sync'):
                routes.append({
                    'rule': str(rule.rule),
                    'methods': sorted([m for m in rule.methods if m not in ('HEAD', 'OPTIONS')]),
                    'endpoint': str(rule.endpoint),
                })
        routes.sort(key=lambda x: x['rule'])
        return jsonify({'success': True, 'routes': routes})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/database-sync/export_csv', methods=['GET', 'POST'])
def export_database_tables_csv_zip():
    try:
        source = (request.args.get('source') or '').strip().lower()
        use_local_flag = DatabaseManager._resolve_use_local(request.args.get('use_local_db'))

        is_local = True
        if source == 'neon':
            is_local = False
        elif source == 'local':
            is_local = True
        else:
            is_local = bool(use_local_flag)

        tables = None
        if request.method == 'GET':
            tables_qs = request.args.get('tables')
            if tables_qs:
                tables = [t.strip() for t in str(tables_qs).split(',') if t.strip()]
        else:
            if not request.is_json:
                return jsonify({'success': False, 'error': '需要 JSON body'}), 400
            body = request.get_json() or {}
            tables = body.get('tables')
            if not isinstance(tables, list) or not tables:
                return jsonify({'success': False, 'error': 'tables must be a non-empty list'}), 400

            tables = [str(t).strip() for t in tables if str(t).strip()]
        if not tables:
            return jsonify({'success': False, 'error': 'tables must be a non-empty list'}), 400

        if is_local:
            conn = psycopg2.connect(
                host=os.environ.get('DB_HOST', 'localhost'),
                port=os.environ.get('DB_PORT', '5432'),
                user=os.environ.get('DB_USER', 'postgres'),
                password=os.environ.get('DB_PASSWORD', ''),
                database=os.environ.get('DB_NAME', 'postgres'),
                cursor_factory=RealDictCursor,
                sslmode=os.environ.get('DB_SSLMODE', 'prefer'),
            )
        else:
            neon_url = (
                os.environ.get('DATABASE_URL')
                or os.environ.get('NEON_DATABASE_URL')
            )
            if not neon_url:
                return jsonify({'success': False, 'error': 'NEON_DATABASE_URL not configured'}), 500
            from urllib.parse import urlparse
            parsed = urlparse(neon_url)
            conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                user=parsed.username,
                password=parsed.password,
                database=parsed.path.lstrip('/') if parsed.path else 'postgres',
                cursor_factory=RealDictCursor,
                sslmode='require',
            )

        tmp_path = None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                """
            )
            existing = {r['tablename'] for r in (cur.fetchall() or []) if isinstance(r, dict) and r.get('tablename')}
            safe_tables = [t for t in tables if t in existing]
            missing = [t for t in tables if t not in existing]
            if not safe_tables:
                return jsonify({'success': False, 'error': f'No valid tables. Missing: {missing}'}), 400

            with tempfile.NamedTemporaryFile(prefix='sync_export_', suffix='.zip', delete=False) as tmpf:
                tmp_path = tmpf.name

            with zipfile.ZipFile(tmp_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                for t in safe_tables:
                    try:
                        cur.execute(sql.SQL('SELECT * FROM {}').format(sql.Identifier(t)))
                        cols = [desc[0] for desc in (cur.description or [])]
                        with zf.open(f'{t}.csv', 'w') as zentry:
                            wrapper = io.TextIOWrapper(zentry, encoding='utf-8', newline='')
                            writer = csv.writer(wrapper)
                            writer.writerow(cols)
                            while True:
                                rows = cur.fetchmany(5000)
                                if not rows:
                                    break
                                for r in rows:
                                    if isinstance(r, dict):
                                        writer.writerow([r.get(c) for c in cols])
                                    else:
                                        writer.writerow(list(r))
                            wrapper.flush()
                    except Exception as te:
                        logger.warning(f"export_csv table failed: {t} err={te}")
                        with zf.open(f'{t}.error.txt', 'w') as zentry:
                            zentry.write(str(te).encode('utf-8'))

                if missing:
                    with zf.open('missing_tables.txt', 'w') as zentry:
                        zentry.write(('\n'.join(missing)).encode('utf-8'))

            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"sync_export_{'local' if is_local else 'neon'}_{ts}.zip"
            resp = send_file(
                tmp_path,
                mimetype='application/zip',
                as_attachment=True,
                download_name=filename,
            )

            try:
                @resp.call_on_close
                def _cleanup_tmp():
                    try:
                        if tmp_path and os.path.exists(tmp_path):
                            os.remove(tmp_path)
                    except Exception:
                        pass
            except Exception:
                pass

            return resp
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.exception('export_database_tables_csv_zip failed')
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/database-sync/upload', methods=['POST'])
def database_sync_upload():
    """將本地資料庫上傳到 Neon"""
    local_conn = None
    neon_conn = None
    try:
        # 獲取要上傳的表格列表
        data = request.get_json() or {}
        selected_tables = data.get('tables', [])  # 如果沒有指定，則上傳所有表格
        
        logger.info(f"🚀 開始資料庫同步... 選擇的表格: {selected_tables if selected_tables else '全部'}")
        push_sse(
            'db_sync',
            'start',
            '開始同步：本機 → Neon',
            direction='upload',
            selected_tables=selected_tables,
        )
        
        neon_url = (
            os.environ.get('DATABASE_URL')
            or os.environ.get('NEON_DATABASE_URL')
        )
        if not neon_url:
            return jsonify({
                'success': False,
                'error': 'NEON_DATABASE_URL not configured'
            }), 400
        
        logger.info("📡 連接本地資料庫...")
        # 連接本地資料庫
        local_config = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': os.environ.get('DB_PORT', '5432'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', ''),
            'database': os.environ.get('DB_NAME', 'postgres')
        }
        
        try:
            local_conn = psycopg2.connect(**local_config, cursor_factory=RealDictCursor)
            logger.info("✅ 本地資料庫連接成功")
            push_sse('db_sync', 'local_connected', '本地資料庫連接成功', direction='upload')
        except Exception as e:
            logger.error(f"❌ 本地資料庫連接失敗: {e}")
            push_sse('db_sync', 'error', f'本地資料庫連接失敗: {e}', direction='upload')
            raise
        
        logger.info("☁️ 連接 Neon 資料庫...")
        try:
            neon_conn = psycopg2.connect(neon_url, cursor_factory=RealDictCursor)
            logger.info("✅ Neon 資料庫連接成功")
            push_sse('db_sync', 'neon_connected', 'Neon 資料庫連接成功', direction='upload')
        except Exception as e:
            logger.error(f"❌ Neon 資料庫連接失敗: {e}")
            push_sse('db_sync', 'error', f'Neon 資料庫連接失敗: {e}', direction='upload')
            if local_conn:
                local_conn.close()
            raise
        
        local_cursor = local_conn.cursor()
        neon_cursor = neon_conn.cursor()
        
        # 獲取所有表格
        local_cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        all_tables = [row['tablename'] for row in local_cursor.fetchall()]
        
        # 如果有指定表格，只處理選中的表格
        if selected_tables:
            tables = [t for t in all_tables if t in selected_tables]
            logger.info(f"📋 將上傳 {len(tables)} 個選中的表格（共 {len(all_tables)} 個表格）")
        else:
            tables = all_tables
            logger.info(f"📋 將上傳所有 {len(tables)} 個表格")
        
        results = {
            'success': True,
            'tables': [],
            'totalTables': len(tables),
            'totalRows': 0,
            'errors': []
        }
        
        for table_name in tables:
            try:
                logger.info(f"Processing table: {table_name}")
                push_sse('db_sync', 'table_start', f'開始上傳表格: {table_name}', direction='upload', table=table_name)
                
                # 獲取表格結構
                local_cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        character_maximum_length,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public' 
                      AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                columns = local_cursor.fetchall()
                
                # 獲取主鍵
                local_cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                """, (table_name,))
                primary_keys = [row['attname'] for row in local_cursor.fetchall()]
                
                # 創建表格 SQL
                column_defs = []
                for col in columns:
                    col_def = f'"{col["column_name"]}" {col["data_type"]}'
                    if col['character_maximum_length']:
                        col_def += f'({col["character_maximum_length"]})'
                    if col['is_nullable'] == 'NO':
                        col_def += ' NOT NULL'
                    if col['column_default']:
                        col_def += f' DEFAULT {col["column_default"]}'
                    column_defs.append(col_def)
                
                create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(column_defs)}'
                if primary_keys:
                    pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                    create_table_sql += f', PRIMARY KEY ({pk_cols})'
                create_table_sql += ')'
                
                # 在 Neon 創建表格
                neon_cursor.execute(create_table_sql)
                neon_conn.commit()
                
                # 先檢查數據量
                local_cursor.execute(f'SELECT COUNT(*) as count FROM "{table_name}"')
                row_count = local_cursor.fetchone()['count']
                logger.info(f"📊 {table_name} 有 {row_count} 行數據")
                push_sse(
                    'db_sync',
                    'table_info',
                    f'{table_name} 行數: {row_count}',
                    direction='upload',
                    table=table_name,
                    row_count=row_count,
                )
                
                col_names = [col['column_name'] for col in columns]
                inserted_count = 0
                
                if row_count == 0:
                    logger.info(f"⚠️ {table_name} 是空表格，跳過")
                    push_sse('db_sync', 'table_skip', f'{table_name} 是空表格，跳過', direction='upload', table=table_name)
                elif row_count > 10000:
                    # 大表格：使用批次處理 + execute_values（更快）
                    logger.info(f"🚀 使用批次處理上傳大表格 {table_name}...")
                    
                    batch_size = 5000  # 每批 5000 行
                    offset = 0
                    
                    while offset < row_count:
                        # 分批讀取
                        local_cursor.execute(f'SELECT * FROM "{table_name}" LIMIT {batch_size} OFFSET {offset}')
                        batch_rows = local_cursor.fetchall()
                        
                        if not batch_rows:
                            break
                        
                        # 準備數據
                        values_list = []
                        for row in batch_rows:
                            values = tuple(row[col] for col in col_names)
                            values_list.append(values)
                        
                        # 使用 execute_values 批量插入（比逐行快很多）
                        cols_str = ', '.join([f'"{col}"' for col in col_names])
                        insert_sql = f'INSERT INTO "{table_name}" ({cols_str}) VALUES %s ON CONFLICT DO NOTHING'
                        
                        try:
                            execute_values(neon_cursor, insert_sql, values_list, page_size=1000)
                            neon_conn.commit()
                            inserted_count += len(batch_rows)
                            
                            # 顯示進度
                            progress = min(100, int((offset + len(batch_rows)) / row_count * 100))
                            logger.info(f"  進度: {progress}% ({inserted_count}/{row_count})")
                            push_sse(
                                'db_sync',
                                'batch_progress',
                                f'{table_name} 進度 {progress}% ({inserted_count}/{row_count})',
                                direction='upload',
                                table=table_name,
                                progress=progress,
                                inserted=inserted_count,
                                row_count=row_count,
                            )
                            
                        except Exception as e:
                            logger.error(f"Error batch inserting into {table_name}: {e}")
                            neon_conn.rollback()
                            push_sse('db_sync', 'error', f'{table_name} 批次寫入失敗: {e}', direction='upload', table=table_name)
                        
                        offset += batch_size
                else:
                    # 小表格：一次讀取全部
                    local_cursor.execute(f'SELECT * FROM "{table_name}"')
                    rows = local_cursor.fetchall()
                    
                    if rows:
                        # 批次插入
                        batch_size = 100
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i + batch_size]
                            
                            for row in batch:
                                values = [row[col] for col in col_names]
                                placeholders = ', '.join(['%s'] * len(values))
                                cols_str = ', '.join([f'"{col}"' for col in col_names])
                                
                                insert_sql = f'''
                                    INSERT INTO "{table_name}" ({cols_str})
                                    VALUES ({placeholders})
                                    ON CONFLICT DO NOTHING
                                '''
                                
                                try:
                                    neon_cursor.execute(insert_sql, values)
                                    inserted_count += 1
                                except Exception as e:
                                    logger.error(f"Error inserting row into {table_name}: {e}")
                                    push_sse('db_sync', 'error', f'{table_name} 寫入失敗: {e}', direction='upload', table=table_name)
                            
                            neon_conn.commit()
                
                results['tables'].append({
                    'name': table_name,
                    'rowCount': row_count,
                    'insertedCount': inserted_count,
                    'success': True
                })
                
                results['totalRows'] += inserted_count
                logger.info(f"✓ {table_name}: {inserted_count}/{row_count} rows uploaded")
                push_sse(
                    'db_sync',
                    'table_done',
                    f'完成上傳表格: {table_name} ({inserted_count}/{row_count})',
                    direction='upload',
                    table=table_name,
                    inserted=inserted_count,
                    row_count=row_count,
                )
                
            except Exception as e:
                logger.error(f"Error processing table {table_name}: {e}")
                push_sse('db_sync', 'error', f'表格 {table_name} 處理失敗: {e}', direction='upload', table=table_name)
                results['errors'].append({
                    'table': table_name,
                    'error': str(e)
                })
                results['tables'].append({
                    'name': table_name,
                    'success': False,
                    'error': str(e)
                })
                neon_conn.rollback()
        
        # 關閉連接
        local_cursor.close()
        neon_cursor.close()
        local_conn.close()
        neon_conn.close()

        push_sse(
            'db_sync',
            'done',
            f'同步完成（上傳：本機 → Neon），共 {results["totalTables"]} 表 / {results["totalRows"]} 行',
            direction='upload',
            total_tables=results['totalTables'],
            total_rows=results['totalRows'],
        )
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Database sync error: {e}")
        push_sse('db_sync', 'error', f'同步失敗（上傳）：{e}', direction='upload')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/database-sync/download', methods=['POST'])
def database_sync_download():
    """將 Neon 資料庫下載回本地"""
    local_conn = None
    neon_conn = None
    lock_acquired = False
    try:
        data = request.get_json() or {}
        selected_tables = data.get('tables', [])
        truncate_local = bool(data.get('truncateLocal', False))

        lock_acquired = db_sync_lock.acquire(blocking=False)
        if not lock_acquired:
            push_sse('db_sync', 'error', '已有同步作業正在進行中，請稍後再試', direction='download')
            return jsonify({
                'success': False,
                'error': '已有同步作業正在進行中，請稍後再試'
            }), 409

        push_sse(
            'db_sync',
            'start',
            '開始同步：Neon → 本機',
            direction='download',
            selected_tables=selected_tables,
            truncate_local=truncate_local,
        )

        logger.info(
            "⬇️ 開始從 Neon 同步回本地... 選擇的表格: %s | truncateLocal=%s",
            selected_tables if selected_tables else '全部',
            truncate_local,
        )

        neon_url = (
            os.environ.get('DATABASE_URL')
            or os.environ.get('NEON_DATABASE_URL')
        )
        if not neon_url:
            return jsonify({
                'success': False,
                'error': 'NEON_DATABASE_URL not configured'
            }), 400

        local_config = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': os.environ.get('DB_PORT', '5432'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', ''),
            'database': os.environ.get('DB_NAME', 'postgres')
        }

        logger.info("📡 連接 Neon 資料庫...")
        neon_conn = psycopg2.connect(neon_url, cursor_factory=RealDictCursor)
        neon_conn.autocommit = True
        logger.info("✅ Neon 資料庫連接成功")
        push_sse('db_sync', 'neon_connected', 'Neon 資料庫連接成功', direction='download')

        logger.info("🗄️ 連接本地資料庫...")
        local_conn = psycopg2.connect(**local_config, cursor_factory=RealDictCursor)
        logger.info("✅ 本地資料庫連接成功")
        push_sse('db_sync', 'local_connected', '本地資料庫連接成功', direction='download')

        neon_cursor = neon_conn.cursor()
        local_cursor = local_conn.cursor()

        neon_cursor.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        all_neon_tables = [row['tablename'] for row in neon_cursor.fetchall()]

        if selected_tables:
            tables = [t for t in all_neon_tables if t in selected_tables]
            logger.info(f"📋 將下載 {len(tables)} 個選中的表格（Neon 共 {len(all_neon_tables)} 個表格）")
        else:
            tables = all_neon_tables
            logger.info(f"📋 將下載 Neon 所有 {len(tables)} 個表格")

        results = {
            'success': True,
            'direction': 'download',
            'tables': [],
            'totalTables': len(tables),
            'totalRows': 0,
            'errors': []
        }

        max_table_retries = 3
        base_retry_delay = 0.5

        for table_name in tables:
            attempt = 0
            while True:
                try:
                    logger.info(f"Processing table (download): {table_name}")
                    push_sse('db_sync', 'table_start', f'開始下載表格: {table_name}', direction='download', table=table_name)

                    neon_cursor.execute("""
                        SELECT
                            column_name,
                            data_type,
                            character_maximum_length,
                            is_nullable,
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))
                    columns = neon_cursor.fetchall()
                    if not columns:
                        logger.info(f"⚠️ Neon 表格 {table_name} 無欄位或不存在，跳過")
                        results['tables'].append({
                            'name': table_name,
                            'rowCount': 0,
                            'insertedCount': 0,
                            'success': True
                        })
                        continue

                    neon_cursor.execute("""
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = 'public'
                          AND tc.table_name = %s
                          AND tc.constraint_type = 'PRIMARY KEY'
                        ORDER BY kcu.ordinal_position
                    """, (table_name,))
                    primary_keys = [row['column_name'] for row in neon_cursor.fetchall()]

                    if not primary_keys:
                        try:
                            neon_cursor.execute(
                                """
                                SELECT a.attname AS column_name
                                FROM pg_index i
                                JOIN pg_attribute a
                                  ON a.attrelid = i.indrelid
                                 AND a.attnum = ANY(i.indkey)
                                WHERE i.indrelid = %s::regclass
                                  AND i.indisprimary
                                ORDER BY array_position(i.indkey, a.attnum)
                                """,
                                (table_name,),
                            )
                            primary_keys = [row['column_name'] for row in neon_cursor.fetchall()]
                        except Exception:
                            primary_keys = []

                    column_defs = []
                    seq_to_create: set[tuple[str | None, str]] = set()
                    for col in columns:
                        col_def = f'"{col["column_name"]}" {col["data_type"]}'
                        if col.get('character_maximum_length'):
                            col_def += f'({col["character_maximum_length"]})'
                        if col.get('is_nullable') == 'NO':
                            col_def += ' NOT NULL'
                        if col.get('column_default'):
                            default_expr = str(col['column_default'])
                            m = re.search(r"nextval\('([^']+)'", default_expr)
                            if m:
                                seq_full = m.group(1)
                                if '.' in seq_full:
                                    seq_schema, seq_name = seq_full.split('.', 1)
                                    seq_to_create.add((seq_schema, seq_name))
                                else:
                                    seq_to_create.add((None, seq_full))
                            col_def += f' DEFAULT {col["column_default"]}'
                        column_defs.append(col_def)

                    local_cursor.execute(
                        """
                        SELECT c.relkind
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public'
                          AND c.relname = %s
                        """,
                        (table_name,),
                    )
                    existing_rel = local_cursor.fetchone()
                    if existing_rel:
                        relkind = existing_rel.get('relkind')
                        if relkind not in ('r', 'p'):
                            drop_stmt = None
                            if relkind == 'v':
                                drop_stmt = f'DROP VIEW IF EXISTS "{table_name}" CASCADE'
                            elif relkind == 'm':
                                drop_stmt = f'DROP MATERIALIZED VIEW IF EXISTS "{table_name}" CASCADE'
                            elif relkind == 'f':
                                drop_stmt = f'DROP FOREIGN TABLE IF EXISTS "{table_name}" CASCADE'
                            else:
                                drop_stmt = f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
                            local_cursor.execute(drop_stmt)
                            local_conn.commit()

                    # 若使用 truncateLocal，代表希望本機 schema 完全跟 Neon 對齊：直接 drop/recreate
                    if truncate_local:
                        local_cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                        local_conn.commit()
                        push_sse('db_sync', 'table_truncate', f'已清空本機表格: {table_name}', direction='download', table=table_name)

                    # 先建立 CREATE TABLE 會用到的 sequences（避免 nextval(...) 指到不存在的 *_id_seq）
                    for seq_schema, seq_name in sorted(seq_to_create):
                        if seq_schema:
                            local_cursor.execute(
                                sql.SQL('CREATE SEQUENCE IF NOT EXISTS {}.{}').format(
                                    sql.Identifier(seq_schema),
                                    sql.Identifier(seq_name),
                                )
                            )
                        else:
                            local_cursor.execute(
                                sql.SQL('CREATE SEQUENCE IF NOT EXISTS {}').format(
                                    sql.Identifier(seq_name),
                                )
                            )
                    if seq_to_create:
                        local_conn.commit()

                    create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(column_defs)}'
                    if primary_keys:
                        pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                        create_table_sql += f', PRIMARY KEY ({pk_cols})'
                    create_table_sql += ')'

                    local_cursor.execute(create_table_sql)
                    local_conn.commit()

                    if truncate_local:
                        # drop/recreate 後保險再做一次 truncate + reset identity
                        local_cursor.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')
                        local_conn.commit()

                    neon_cursor.execute(f'SELECT COUNT(*) as count FROM "{table_name}"')
                    row_count = neon_cursor.fetchone()['count']
                    logger.info(f"📊 Neon {table_name} 有 {row_count} 行數據")
                    push_sse(
                        'db_sync',
                        'table_info',
                        f'{table_name} 行數: {row_count}',
                        direction='download',
                        table=table_name,
                        row_count=row_count,
                    )

                    col_names = [col['column_name'] for col in columns]
                    col_types = {col['column_name']: (col.get('data_type') or '').lower() for col in columns}
                    inserted_count = 0

                    if row_count == 0:
                        logger.info(f"⚠️ {table_name} 是空表格，跳過")
                    else:
                        batch_size = 5000 if row_count > 10000 else 1000
                        offset = 0

                        cols_str = ', '.join([f'"{col}"' for col in col_names])
                        placeholders_sql = f'INSERT INTO "{table_name}" ({cols_str}) VALUES %s'
                        if primary_keys:
                            pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                            placeholders_sql += f' ON CONFLICT ({pk_cols}) DO NOTHING'

                        while offset < row_count:
                            neon_cursor.execute(
                                f'SELECT * FROM "{table_name}" LIMIT {batch_size} OFFSET {offset}'
                            )
                            batch_rows = neon_cursor.fetchall()
                            if not batch_rows:
                                break

                            values_list = []
                            for r in batch_rows:
                                row_vals = []
                                for col in col_names:
                                    v = r.get(col)
                                    t = col_types.get(col, '')
                                    if t in ('json', 'jsonb') and v is not None:
                                        row_vals.append(Json(v))
                                    else:
                                        row_vals.append(v)
                                values_list.append(tuple(row_vals))
                            try:
                                execute_values(local_cursor, placeholders_sql, values_list, page_size=1000)
                                local_conn.commit()
                                inserted_count += len(batch_rows)

                                if row_count > 0:
                                    progress = min(100, int((offset + len(batch_rows)) / row_count * 100))
                                    logger.info(f"  進度: {progress}% ({inserted_count}/{row_count})")
                                    push_sse(
                                        'db_sync',
                                        'batch_progress',
                                        f'{table_name} 進度 {progress}% ({inserted_count}/{row_count})',
                                        direction='download',
                                        table=table_name,
                                        progress=progress,
                                        inserted=inserted_count,
                                        row_count=row_count,
                                    )
                            except Exception as e:
                                logger.error(f"Error batch inserting into local {table_name}: {e}")
                                try:
                                    local_conn.rollback()
                                except Exception:
                                    pass
                                push_sse('db_sync', 'error', f'{table_name} 批次寫入失敗: {e}', direction='download', table=table_name)
                                raise

                            offset += batch_size

                    results['tables'].append({
                        'name': table_name,
                        'rowCount': row_count,
                        'insertedCount': inserted_count,
                        'success': True
                    })
                    results['totalRows'] += inserted_count
                    logger.info(f"✓ {table_name}: {inserted_count}/{row_count} rows downloaded")
                    push_sse(
                        'db_sync',
                        'table_done',
                        f'完成下載表格: {table_name} ({inserted_count}/{row_count})',
                        direction='download',
                        table=table_name,
                        inserted=inserted_count,
                        row_count=row_count,
                    )
                    break

                except Exception as e:
                    logger.error(f"Error processing table {table_name} (download): {e}")
                    try:
                        if local_conn:
                            local_conn.rollback()
                    except Exception:
                        pass
                    try:
                        if neon_conn and not getattr(neon_conn, 'autocommit', False):
                            neon_conn.rollback()
                    except Exception:
                        pass

                    msg = str(e)
                    pgcode = getattr(e, 'pgcode', None)
                    is_deadlock = (pgcode == '40P01') or ('deadlock detected' in msg.lower())
                    if is_deadlock and attempt < (max_table_retries - 1):
                        delay = base_retry_delay * (2 ** attempt)
                        attempt += 1
                        push_sse(
                            'db_sync',
                            'warning',
                            f'表格 {table_name} 發生死鎖，{delay:.1f}s 後重試（第 {attempt + 1}/{max_table_retries} 次）',
                            direction='download',
                            table=table_name,
                        )
                        time.sleep(delay)
                        continue

                    push_sse('db_sync', 'error', f'表格 {table_name} 處理失敗: {e}', direction='download', table=table_name)
                    results['errors'].append({
                        'table': table_name,
                        'error': str(e)
                    })
                    results['tables'].append({
                        'name': table_name,
                        'success': False,
                        'error': str(e)
                    })
                    break

        try:
            local_cursor.close()
            neon_cursor.close()
        except Exception:
            pass
        try:
            local_conn.close()
        except Exception:
            pass
        try:
            neon_conn.close()
        except Exception:
            pass

        push_sse(
            'db_sync',
            'done',
            f'同步完成（下載：Neon → 本機），共 {results["totalTables"]} 表 / {results["totalRows"]} 行',
            direction='download',
            total_tables=results['totalTables'],
            total_rows=results['totalRows'],
        )

        return jsonify(results)

    except Exception as e:
        logger.error(f"Database download sync error: {e}")
        push_sse('db_sync', 'error', f'同步失敗（下載）：{e}', direction='download')
        try:
            if local_conn:
                local_conn.close()
        except Exception:
            pass
        try:
            if neon_conn:
                neon_conn.close()
        except Exception:
            pass
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

    finally:
        if lock_acquired:
            try:
                db_sync_lock.release()
            except Exception:
                pass

def run_t86_job(start_date=None, end_date=None, market='both', sleep_seconds=0.6, persist=True, use_local=False):
    today = datetime.now().date().isoformat()
    start_value = start_date or today
    end_value = end_date or start_value

    logger.info(
        "[t86-job] start=%s end=%s market=%s persist=%s use_local=%s",
        start_value,
        end_value,
        market,
        persist,
        use_local,
    )

    records, summary, daily_stats = stock_api.fetch_t86_range(
        start_value,
        end_value,
        market=market,
        sleep_seconds=sleep_seconds,
    )

    inserted = 0
    if persist and records:
        db_manager = DatabaseManager(use_local=use_local)
        try:
            inserted = stock_api.upsert_t86_records(records, db_manager=db_manager)
        finally:
            try:
                db_manager.disconnect()
            except Exception:
                pass

    result = {
        'success': True,
        'job': 't86-daily',
        'summary': summary,
        'daily_stats': daily_stats,
        'count': len(records),
        'persisted': inserted,
        'persist_enabled': persist,
        'start': start_value,
        'end': end_value,
        'market': market,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result

def parse_cli_args(argv=None):
    parser = argparse.ArgumentParser(description='Taiwan stock data service jobs')
    parser.add_argument('--job', choices=['t86-daily'], help='Run a background job instead of starting the web server')
    parser.add_argument('--start', help='Start date for the job, format YYYY-MM-DD')
    parser.add_argument('--end', help='End date for the job, format YYYY-MM-DD')
    parser.add_argument('--market', default='both', choices=['twse', 'tpex', 'both'], help='Market scope for T86 job')
    parser.add_argument('--sleep', type=float, default=0.6, help='Sleep seconds between date fetches')
    parser.add_argument('--no-persist', action='store_true', help='Fetch records without writing into the database')
    parser.add_argument('--use-local-db', action='store_true', help='Use local database settings instead of Neon/DATABASE_URL')
    return parser.parse_args(argv)

if __name__ == '__main__':
    args = parse_cli_args()

    if args.job == 't86-daily':
        try:
            run_t86_job(
                start_date=args.start,
                end_date=args.end,
                market=args.market,
                sleep_seconds=args.sleep,
                persist=not args.no_persist,
                use_local=args.use_local_db,
            )
            sys.exit(0)
        except Exception as exc:
            logger.exception('T86 job failed')
            print(json.dumps({'success': False, 'job': 't86-daily', 'error': str(exc)}, ensure_ascii=False))
            sys.exit(1)

    # 支援以環境變數 PORT 指定埠號，預設 5003
    try:
        port = int(os.getenv('PORT', '5003'))
    except Exception:
        port = 5003

    if port == 3000:
        port = 5003

    def _is_port_available(p: int) -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('0.0.0.0', p))
            return True
        except OSError:
            return False
        finally:
            try:
                s.close()
            except Exception:
                pass

    original_port = port
    for _ in range(0, 50):
        if _is_port_available(port):
            break
        port += 1

    print("Taiwan Stock Data API Server Starting...")
    print("API Endpoints:")
    print("   GET  /api/symbols - Get all stock symbols")
    print("   GET  /api/stock/<symbol>/prices - Get stock price data")
    print("   GET  /api/stock/<symbol>/returns - Get return data")
    print("   POST /api/update - Batch update stock data")
    print("   GET  /api/health - Health check")
    print("   GET  /api/income-statement?year=YYYY&season=S - Income statement wide data for all stocks")
    if original_port != port:
        print(f"[Info] Port {original_port} 已被占用，改用 {port}")
    print(f"Server address: http://localhost:{port}")

    try:
        app.run(host='0.0.0.0', port=port, debug=True, threaded=True, use_reloader=False)
    except OSError as e:
        # 常見：Errno 48 (Address already in use)
        print(f"[Error] 無法啟動伺服器：{e}")
        print("提示：")
        print(f"  - 可能已有其他進程使用埠號 {port}")
        print("  - 你可以關閉舊進程，或以不同埠號啟動：例如 'PORT=5004 python3 server.py'")
        sys.exit(1)
        app.run(host='0.0.0.0', port=port, debug=True, threaded=True, use_reloader=False)
    except OSError as e:
        # 常見：Errno 48 (Address already in use)
        print(f"[Error] 無法啟動伺服器：{e}")
        print("提示：")
        print(f"  - 可能已有其他進程使用埠號 {port}")
        print("  - 你可以關閉舊進程，或以不同埠號啟動：例如 'PORT=5004 python3 server.py'")
        sys.exit(1)
