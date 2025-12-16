#!/usr/bin/env python3
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
from flask import Flask, jsonify, request, send_from_directory, send_file, Response
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values, Json
from psycopg2.extensions import register_adapter
import json as json_lib

# 註冊 dict 類型適配器
register_adapter(dict, Json)
from urllib.parse import urlparse
import os
import math
import threading  # 添加线程模块
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import zipfile
from functools import partial
from typing import Optional

from income_statement_service import fetch_all_incomes, fetch_income_row, TARGET_ORDER
from balance_sheet_service import (
    fetch_all_balance_sheets,
    fetch_balance_sheet_row,
    MopsBlockedError as BalanceMopsBlockedError,
    TARGET_ORDER as BALANCE_TARGET_ORDER,
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
)

from returns_calc import compute_returns as compute_returns_task

# 配置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_START_DATE = '2010-01-01'

# 全局数据库表锁（防止并发修改表结构导致死锁）
db_table_lock = threading.Lock()

# 获取当前目录作为静态文件目录
current_dir = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, static_folder=current_dir, static_url_path='')
CORS(app)  # 允許跨域請求

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
                or 'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require'
            )
        )
        ssl_default = 'require' if self.db_url else 'prefer'
        self.db_config = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': os.environ.get('DB_PORT', '5432'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', 's8304021'),
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
        self._cursor_factory = partial(
            TableNameAwareCursor,
            table_prices=self.table_prices,
            table_returns=self.table_returns,
            table_institutional=self.table_institutional,
        )

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
            return False
        
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
            cursor.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {self.table_prices}_symbol_date_idx
                ON {self.table_prices}(symbol, date);
                """
            )

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

            # 確保既有 tw_balance_sheets 表若是舊版，也會補齊所有目標欄位
            for col in BALANCE_TARGET_ORDER:
                try:
                    cursor.execute(
                        f'ALTER TABLE {self.table_balance} ADD COLUMN IF NOT EXISTS "{col}" NUMERIC(20,4);'
                    )
                except Exception as e:
                    logger.warning("balance table add column %s warning: %s", col, e)
            
            self.connection.commit()
            cursor.close()
            logger.info("资料库表创建成功")
            return True
        except Exception as e:
            logger.error(f"创建表失败: {e}")
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
                return float(value)
            except Exception:
                return None
        s = str(value).replace(',', '').strip()
        if s in {'', '-', '--', '---', '----', 'NaN', 'null', 'None'}:
            return None
        try:
            return float(s)
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
            
            # 檢查是否為台灣加權指數，優先使用 yfinance 抓取
            if symbol == '^TWII':
                logger.info(f"檢測到台灣加權指數 {symbol}，使用 yfinance 抓取歷史數據")
                yf_result = self.fetch_twii_with_yfinance(start_date, end_date)
                if yf_result:
                    return yf_result
                logger.warning("yfinance 抓取 ^TWII 失敗，改用證交所指數API作為備援")
                return self.fetch_twse_index_data('^TWII', start_date, end_date)
            
            # 解析股票代碼
            if '.TW' in symbol or '.TWO' in symbol:
                stock_code = symbol.split('.')[0]
                market_suffix = symbol.split('.')[1]
            else:
                stock_code = symbol
                market_suffix = None
            
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
            
            # 台灣API獲取失敗且禁用 Yahoo Finance 備援
            logger.error(f"台灣API獲取失敗，且已禁用 Yahoo Finance 備援")
            return None
            
        except Exception as e:
            logger.error(f"下載 {symbol} 股價失敗: {e}")
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
            
            logger.info(f"批量抓取 {target_dt.strftime('%Y-%m-%d')} 成功，共 {len(result)} 檔股票")
            return result
            
        except Exception as e:
            logger.error(f"批量抓取 {target_date} 失敗: {e}")
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

    def fetch_twse_t86_by_date(self, target_date):
        dt = self._ensure_date(target_date)
        params = {
            'response': 'json',
            'date': dt.strftime('%Y%m%d'),
            'selectType': 'ALLBUT0999'
        }

        resp = self.twse_session.get(self.TWSE_T86_URL, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get('stat') != 'OK':
            logger.info(f"TWSE T86 {dt} stat={payload.get('stat')}, 無資料")
            return []

        data_rows = payload.get('data') or []
        results = []
        for row in data_rows:
            if len(row) < 19:
                continue
            stock_no = (row[0] or '').strip()
            stock_name = (row[1] or '').strip()
            fb = self._t86_parse_int(row[2])
            fs = self._t86_parse_int(row[3])
            fn = self._t86_parse_int(row[4])
            fdb = self._t86_parse_int(row[5])
            fds = self._t86_parse_int(row[6])
            fdn = self._t86_parse_int(row[7])
            itb = self._t86_parse_int(row[8])
            its = self._t86_parse_int(row[9])
            itn = self._t86_parse_int(row[10])
            dealer_total_net = self._t86_parse_int(row[11])
            dsb = self._t86_parse_int(row[12])
            dss = self._t86_parse_int(row[13])
            dsn = self._t86_parse_int(row[14])
            dhb = self._t86_parse_int(row[15])
            dhs = self._t86_parse_int(row[16])
            dhn = self._t86_parse_int(row[17])
            overall = self._t86_parse_int(row[18])

            results.append({
                'date': dt.isoformat(),
                'market': 'TWSE',
                'stock_no': stock_no,
                'stock_name': stock_name,
                'foreign_buy': fb,
                'foreign_sell': fs,
                'foreign_net': fn,
                'foreign_dealer_buy': fdb,
                'foreign_dealer_sell': fds,
                'foreign_dealer_net': fdn,
                'foreign_total_buy': fb + fdb,
                'foreign_total_sell': fs + fds,
                'foreign_total_net': fn + fdn,
                'investment_trust_buy': itb,
                'investment_trust_sell': its,
                'investment_trust_net': itn,
                'dealer_self_buy': dsb,
                'dealer_self_sell': dss,
                'dealer_self_net': dsn,
                'dealer_hedge_buy': dhb,
                'dealer_hedge_sell': dhs,
                'dealer_hedge_net': dhn,
                'dealer_total_buy': dsb + dhb,
                'dealer_total_sell': dss + dhs,
                'dealer_total_net': dealer_total_net if dealer_total_net else dsn + dhn,
                'overall_net': overall,
            })
        logger.info(f"TWSE T86 {dt} 抓取 {len(results)} 筆")
        return results

    def fetch_tpex_t86_by_date(self, target_date):
        dt = self._ensure_date(target_date)
        roc_date = f"{dt.year - 1911:03d}/{dt.month:02d}/{dt.day:02d}"
        params = {
            'l': 'zh-tw',
            'date': roc_date,
            'json': '1'
        }

        resp = self.tpex_session.get(self.TPEX_T86_URL, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get('stat', '').lower() != 'ok':
            logger.info(f"TPEX T86 {dt} stat={payload.get('stat')}, 無資料")
            return []

        tables = payload.get('tables') or []
        if not tables:
            return []

        data_rows = tables[0].get('data') or []
        results = []
        for row in data_rows:
            if len(row) < 24:
                continue
            stock_no = (row[0] or '').strip()
            stock_name = (row[1] or '').strip()
            fb = self._t86_parse_int(row[2])
            fs = self._t86_parse_int(row[3])
            fn = self._t86_parse_int(row[4])
            fdb = self._t86_parse_int(row[5])
            fds = self._t86_parse_int(row[6])
            fdn = self._t86_parse_int(row[7])
            ftb = self._t86_parse_int(row[8])
            fts = self._t86_parse_int(row[9])
            ftn = self._t86_parse_int(row[10])
            itb = self._t86_parse_int(row[11])
            its = self._t86_parse_int(row[12])
            itn = self._t86_parse_int(row[13])
            dsb = self._t86_parse_int(row[14])
            dss = self._t86_parse_int(row[15])
            dsn = self._t86_parse_int(row[16])
            dhb = self._t86_parse_int(row[17])
            dhs = self._t86_parse_int(row[18])
            dhn = self._t86_parse_int(row[19])
            dtb = self._t86_parse_int(row[20])
            dts = self._t86_parse_int(row[21])
            dtn = self._t86_parse_int(row[22])
            overall = self._t86_parse_int(row[23])

            results.append({
                'date': dt.isoformat(),
                'market': 'TPEX',
                'stock_no': stock_no,
                'stock_name': stock_name,
                'foreign_buy': fb,
                'foreign_sell': fs,
                'foreign_net': fn,
                'foreign_dealer_buy': fdb,
                'foreign_dealer_sell': fds,
                'foreign_dealer_net': fdn,
                'foreign_total_buy': ftb,
                'foreign_total_sell': fts,
                'foreign_total_net': ftn,
                'investment_trust_buy': itb,
                'investment_trust_sell': its,
                'investment_trust_net': itn,
                'dealer_self_buy': dsb,
                'dealer_self_sell': dss,
                'dealer_self_net': dsn,
                'dealer_hedge_buy': dhb,
                'dealer_hedge_sell': dhs,
                'dealer_hedge_net': dhn,
                'dealer_total_buy': dtb,
                'dealer_total_sell': dts,
                'dealer_total_net': dtn,
                'overall_net': overall,
            })
        logger.info(f"TPEX T86 {dt} 抓取 {len(results)} 筆")
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

        # 找出標題列（第一欄為 '公司代號'）
        first_col = df.iloc[:, 0].astype(str).str.strip()
        header_idx_list = df.index[first_col == '公司代號'].tolist()
        if not header_idx_list:
            logger.info("TWSE 月營收 HTML 找不到 '公司代號' 標題列")
            return []
        header_idx = header_idx_list[0]
        df.columns = df.iloc[header_idx]
        df = df[header_idx + 1:]

        if '公司代號' not in df.columns or '當月營收' not in df.columns:
            logger.info("TWSE 月營收 HTML 欄位名稱不符合預期")
            return []

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

        first_col = df.iloc[:, 0].astype(str).str.strip()
        header_idx_list = df.index[first_col == '公司代號'].tolist()
        if not header_idx_list:
            logger.info("TPEX 月營收 HTML 找不到 '公司代號' 標題列")
            return []
        header_idx = header_idx_list[0]
        df.columns = df.iloc[header_idx]
        df = df[header_idx + 1:]

        if '公司代號' not in df.columns or '當月營收' not in df.columns:
            logger.info("TPEX 月營收 HTML 欄位名稱不符合預期")
            return []

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
            download_dir = os.path.join(home_dir, "Downloads", "mops_csv")
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
            
            result = []
            current_date = start_dt
            
            # 使用櫃買中心傳統API（提供正確歷史數據）
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://www.tpex.org.tw/'
            }
            
            logger.info(f"使用櫃買中心傳統API抓取 {stock_code}，日期範圍: {start_date} ~ {end_date}")
            
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
                
                # 轉換為民國年格式 YYY/MM/DD（傳統API使用斜線）
                roc_date = f"{current_date.year - 1911:03d}/{current_date.month:02d}/{current_date.day:02d}"
                
                url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
                params = {
                    'l': 'zh-tw',
                    'd': roc_date,
                    'se': 'AL'  # 全部股票
                }
                
                # 添加重試機制
                max_retries = 3
                retry_count = 0
                success = False
                
                while retry_count < max_retries and not success:
                    try:
                        response = requests.get(url, params=params, headers=headers, timeout=15)
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            if data.get('stat') == 'ok' and 'tables' in data:
                                tables = data['tables']
                                found_stock = False
                                
                                # 搜尋所有表格中的目標股票
                                for table in tables:
                                    if 'data' in table:
                                        table_data = table['data']
                                        
                                        # 查找目標股票
                                        for row in table_data:
                                            if (isinstance(row, list) and len(row) >= 8 and 
                                                row[0] == stock_code):
                                                
                                                found_stock = True
                                                date_str = current_date.strftime('%Y-%m-%d')
                                                
                                                try:
                                                    # 解析價格數據（傳統API格式）
                                                    # row[0]=代碼, row[1]=名稱, row[2]=收盤, row[3]=漲跌
                                                    # row[4]=開盤, row[5]=最高, row[6]=最低, row[7]=成交量
                                                    
                                                    close_str = str(row[2]).replace(',', '').strip()
                                                    open_str = str(row[4]).replace(',', '').strip()
                                                    high_str = str(row[5]).replace(',', '').strip()
                                                    low_str = str(row[6]).replace(',', '').strip()
                                                    volume_str = str(row[7]).replace(',', '').strip()
                                                    
                                                    # 檢查是否為有效數據（避免 "----" 等無效值）
                                                    if (close_str not in ['----', '---', '', '0'] and 
                                                        volume_str not in ['0', '', '----']):
                                                        
                                                        close_price = float(close_str)
                                                        open_price = float(open_str) if open_str not in ['----', '---', ''] else None
                                                        high_price = float(high_str) if high_str not in ['----', '---', ''] else None
                                                        low_price = float(low_str) if low_str not in ['----', '---', ''] else None
                                                        volume = int(volume_str)
                                                        
                                                        if close_price > 0 and volume > 0:
                                                            result.append({
                                                                'ticker': f"{stock_code}.TWO",
                                                                'Date': date_str,
                                                                'Open': round(open_price, 2) if open_price else None,
                                                                'High': round(high_price, 2) if high_price else None,
                                                                'Low': round(low_price, 2) if low_price else None,
                                                                'Close': round(close_price, 2),
                                                                'Volume': volume
                                                            })
                                                            success_count += 1
                                                        
                                                except (ValueError, IndexError) as e:
                                                    logger.warning(f"解析 {stock_code} {date_str} 數據失敗: {e}")
                                                
                                                break  # 找到股票後跳出內層循環
                                        
                                        if found_stock:
                                            break  # 找到股票後跳出表格循環
                                
                                success = True
                            else:
                                logger.warning(f"櫃買中心傳統API 回應狀態異常: {data.get('stat')}")
                                success = True  # 避免重試
                        else:
                            logger.warning(f"櫃買中心傳統API HTTP {response.status_code}，日期: {current_date.strftime('%Y-%m-%d')}")
                            success = True  # 跳過此日期
                            
                    except requests.exceptions.Timeout:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"櫃買中心傳統API 超時，第 {retry_count} 次重試: {current_date.strftime('%Y-%m-%d')}")
                            time.sleep(2)
                        else:
                            logger.error(f"櫃買中心傳統API 超時，已達最大重試次數: {current_date.strftime('%Y-%m-%d')}")
                            break
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"櫃買中心傳統API 請求失敗: {e}，第 {retry_count} 次重試")
                            time.sleep(2)
                        else:
                            logger.error(f"櫃買中心傳統API 請求失敗: {e}，已達最大重試次數")
                            break
                
                # 移動到下一天
                current_date = current_date + timedelta(days=1)
                processed_days += 1
                
                # 進度提示（每20天，減少日誌噪音）
                if processed_days % 20 == 0:
                    logger.info(f"櫃買中心 {stock_code} 進度: {processed_days}/{total_days} 天，成功 {success_count} 筆")
                
                # 避免請求過於頻繁（每秒最多2次）
                time.sleep(0.5)
            
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

            # 移除時區資訊，確保 index 為 naive datetime
            if getattr(df.index, "tz", None) is not None:
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
    return send_file(os.path.join(current_dir, 'index.html'))

@app.route('/<path:filename>')
def static_files(filename):
    """提供靜態文件"""
    return send_from_directory(current_dir, filename)

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


def _upsert_prices(cursor, symbol, price_records):
    """將價格資料批量 upsert 進 tw_stock_prices。price_records: list[dict] with keys Date/Open/High/Low/Close/Volume 或對應小寫欄位。
    """
    if not price_records:
        logger.warning(f"_upsert_prices: {symbol} 收到空資料，跳過")
        return 0
    logger.info(f"_upsert_prices: 準備寫入 {symbol} 的 {len(price_records)} 筆資料")
    values = []
    for pr in price_records:
        record_date = pr.get('date') or pr.get('Date')
        values.append(
            (
                symbol,
                record_date,
                pr.get('open_price') or pr.get('Open'),
                pr.get('high_price') or pr.get('High'),
                pr.get('low_price') or pr.get('Low'),
                pr.get('close_price') or pr.get('Close'),
                pr.get('volume') or pr.get('Volume')
            )
        )
    if not values:
        return 0
    upsert_sql = """
        INSERT INTO tw_stock_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
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
        try:
            db.create_tables()
            records = stock_api.fetch_twii_with_yfinance(start_str, end_str)
            if records:
                count = len(records)
                cur = db.connection.cursor()
                inserted = _upsert_prices(cur, '^TWII', records)
                db.connection.commit()
            return jsonify({
                'success': True,
                'symbol': '^TWII',
                'start': start_str,
                'end': end_str,
                'fetched': count,
                'inserted': inserted
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
                    symbol_inserted = _upsert_prices(cur, sym, recs_filtered)
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
            db = DatabaseManager.from_request_args(request.args)
            if not db.connect():
                yield sse_format({'type': 'error', 'message': '資料庫連線失敗'})
                return
            cur = db.connection.cursor()
            try:
                try:
                    db.create_tables()
                except Exception:
                    pass

                yield sse_format({'type': 'start', 'symbol': symbol, 'start': start_date, 'end': end_date, 'threshold': threshold})

                anomalies = _detect_price_anomalies(cur, symbol, start_date, end_date, threshold)
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
                        recs_filtered, recs_skipped = _validate_refetched_records(cur, sym, recs, threshold=rv_thresh)
                        symbol_inserted = _upsert_prices(cur, sym, recs_filtered)
                        total_refetched += symbol_inserted

                    # 稽核（以refetch-only記錄）
                    cur.execute(
                        """
                            INSERT INTO stock_anomaly_audit
                                (symbol, start_date, end_date, deleted_count, refetched_count, rule_version, threshold)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        [sym, start_date or rs_str, end_date or re_str, 0, total_refetched, 'rules_v1_pct', threshold]
                    )

                    # 每檔提交，讓資料在過程中即時生效
                    db.connection.commit()

                    info = {
                        'symbol': sym,
                        'inserted': int(symbol_inserted),
                        'preview': preview,
                        'refetch_range': {'start': rs_str, 'end': re_str}
                    }
                    details.append(info)
                    yield sse_format({'type': 'symbol_done', **info})

                # 最終提交（多數情況已於每檔提交，這裡作為保險）
                db.connection.commit()
                yield sse_format({'type': 'done', 'success': True, 'count': len(anomalies), 'refetched': total_refetched, 'details': details})
            except Exception as e:
                db.connection.rollback()
                yield sse_format({'type': 'error', 'message': str(e)})
            finally:
                db.disconnect()

        headers = {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
        }
        return Response(generate(), headers=headers)
    except Exception as e:
        logger.error(f"fix_anomalies_stream 錯誤: {e}")
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
        logger.error(f"獲取股票代碼失敗: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.get('/api/income-statement')
def api_income_statement():
    """Return wide-format income statement data for all stocks for a given year/season.

    Query params:
        year:   e.g. 2025
        season: 1-4
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
        df = fetch_all_incomes(
            str(year),
            str(season),
            progress_cb=_income_progress_cb,
            row_cb=row_cb,
            code_from=code_from,
            code_to=code_to,
            pause_every=pause_every,
            pause_seconds=pause_seconds,
        )
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
        logger.error(f"income-statement fetch failed: {e}")
        income_fetch_status['running'] = False
        income_fetch_status['finishedAt'] = _dt.utcnow().isoformat()
        income_fetch_status['error'] = str(e)
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

    data_json = df.to_json(orient='records', force_ascii=False)
    return Response(data_json, mimetype='application/json; charset=utf-8')


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


@app.route('/api/warrants/dates', methods=['GET'])
def get_warrant_dates():
    """取得 tw_warrant_trade 中可用的交易日期清單。"""
    try:
        limit = request.args.get('limit', default=60, type=int)
        if not isinstance(limit, int) or limit <= 0:
            limit = 60
        limit = min(limit, 365)

        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({'success': False, 'error': '資料庫連接失敗'}), 500

        try:
            cursor = db_manager.connection.cursor()
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
    """依日期與關鍵字查詢權證資料。

    Query parameters:
        date: 交易日期 (YYYY-MM-DD)，若未提供則使用資料表中最新日期
        keyword: 權證代號或名稱關鍵字（模糊查詢）
        page: 第幾頁（預設 1）
        pageSize: 每頁筆數（預設 50，區間 10~200）
    """
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
            cursor = db_manager.connection.cursor()

            target_date = date_str
            if not target_date:
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
                where_clauses.append("(warrant_code ILIKE %s OR warrant_name ILIKE %s)")
                pattern = f"%{keyword}%"
                params.extend([pattern, pattern])

            where_sql = " WHERE " + " AND ".join(where_clauses)

            # 統計總筆數
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM tw_warrant_trade{where_sql}",
                params,
            )
            row = cursor.fetchone()
            if isinstance(row, dict):
                total = row.get('cnt', 0)
            else:
                total = row[0] if row else 0
            total = int(total or 0)

            # 查詢實際資料
            params_with_paging = list(params) + [page_size, offset]
            cursor.execute(
                f"""
                SELECT
                    trade_date,
                    warrant_code,
                    warrant_name,
                    turnover,
                    volume
                FROM tw_warrant_trade
                {where_sql}
                ORDER BY turnover DESC NULLS LAST,
                         volume DESC NULLS LAST,
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
                tdate = row.get('trade_date')
                code = row.get('warrant_code')
                name = row.get('warrant_name')
                turnover = row.get('turnover')
                volume = row.get('volume')
            else:
                tdate, code, name, turnover, volume = row

            if isinstance(tdate, (datetime, date)):
                tdate_str = tdate.strftime('%Y-%m-%d')
            elif isinstance(tdate, str):
                tdate_str = tdate[:10]
            else:
                tdate_str = None

            try:
                turnover_val = float(turnover) if turnover is not None else None
            except Exception:
                turnover_val = None

            try:
                volume_val = int(volume) if volume is not None else None
            except Exception:
                volume_val = None

            data.append({
                'trade_date': tdate_str,
                'warrant_code': code,
                'warrant_name': name,
                'turnover': turnover_val,
                'volume': volume_val,
            })

        return jsonify({
            'success': True,
            'data': data,
            'total': total,
            'page': page,
            'pageSize': page_size,
            'date': target_date,
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

        persist_flag = (request.args.get('persist', 'true').lower() != 'false')

        total_inserted = 0
        total_records = 0
        monthly_stats = []

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

                total_inserted += month_inserted
                total_records += len(records)

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
                'monthsProcessed': len(months),
                'totalInserted': total_inserted,
                'persist_enabled': persist_flag,
            }

            return jsonify({
                'success': True,
                'summary': summary_out,
                'monthly_stats': monthly_stats,
                'count': total_records,
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
        db_manager = DatabaseManager.from_request_payload(data)
        if not db_manager.connect():
            return jsonify({
                'success': False,
                'error': '資料庫連接失敗'
            }), 500
        
        try:
            cursor = db_manager.connection.cursor()
            prices_table = db_manager.table_prices

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
                query = f"""
                    SELECT date, open_price, high_price, low_price, close_price, volume
                    FROM {prices_table} 
                    WHERE symbol = %s
                """
            else:
                # 如果沒有 open_price 等欄位，可能是舊的表結構
                query = f"""
                    SELECT date, close_price, volume
                    FROM {prices_table} 
                    WHERE symbol = %s
                """
            
            # 支援多種股票代碼格式查詢
            # 如果輸入的是純數字代碼，嘗試匹配完整格式
            if symbol.isdigit():
                # 先嘗試直接查詢，如果沒有結果，再嘗試添加後綴
                cursor.execute(
                    f"SELECT COUNT(*) FROM {prices_table} WHERE symbol = %s",
                    [symbol]
                )
                result = cursor.fetchone()
                count = result[0] if isinstance(result, (list, tuple)) else result.get('count', 0)
                
                if count == 0:
                    # 嘗試查找帶有 .TW 或 .TWO 後綴的股票
                    cursor.execute(
                        f"""
                        SELECT symbol FROM {prices_table} 
                        WHERE symbol IN (%s, %s) 
                        LIMIT 1
                        """,
                        [f"{symbol}.TW", f"{symbol}.TWO"]
                    )
                    
                    result = cursor.fetchone()
                    if result:
                        found_symbol = result[0] if isinstance(result, (list, tuple)) else result.get('symbol')
                        if found_symbol:
                            symbol = found_symbol  # 使用找到的完整格式
            
            params = [symbol]
            
            if start_date:
                query += " AND date >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= %s"
                params.append(end_date)
                
            query += " ORDER BY date ASC"
            
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
        
        # 連接資料庫
        db_manager = DatabaseManager.from_request_args(request.args)
        if not db_manager.connect():
            return jsonify({
                'success': False,
                'error': '資料庫連接失敗'
            }), 500
        
        try:
            cursor = db_manager.connection.cursor()
            
            # 構建查詢語句
            query = """
                SELECT date, daily_return, weekly_return, monthly_return, cumulative_return
                FROM tw_stock_returns 
                WHERE symbol = %s
            """
            
            # 支援多種股票代碼格式查詢
            # 如果輸入的是純數字代碼，嘗試匹配完整格式
            if symbol.isdigit():
                # 先嘗試直接查詢，如果沒有結果，再嘗試添加後綴
                cursor.execute("SELECT COUNT(*) FROM tw_stock_returns WHERE symbol = %s", [symbol])
                result = cursor.fetchone()
                count = result[0] if isinstance(result, (list, tuple)) else result.get('count', 0)
                
                if count == 0:
                    # 嘗試查找帶有 .TW 或 .TWO 後綴的股票
                    cursor.execute("""
                        SELECT symbol FROM tw_stock_returns 
                        WHERE symbol IN (%s, %s) 
                        LIMIT 1
                    """, [f"{symbol}.TW", f"{symbol}.TWO"])
                    
                    result = cursor.fetchone()
                    if result:
                        found_symbol = result[0] if isinstance(result, (list, tuple)) else result.get('symbol')
                        if found_symbol:
                            symbol = found_symbol  # 使用找到的完整格式
            
            params = [symbol]
            
            if start_date:
                query += " AND date >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= %s"
                params.append(end_date)
                
            query += " ORDER BY date ASC"
            
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
        symbol = body.get('symbol')
        start = body.get('start')
        end = body.get('end')
        all_flag = bool(body.get('all', False))
        limit = body.get('limit')
        fill_missing = bool(body.get('fillMissing', body.get('fill_missing', False)))
        use_local_db = bool(body.get('use_local_db', False))
        upload_to_neon = bool(body.get('upload_to_neon', False))

        # 若未提供 symbol 且未指定 all，就預設 all=true
        if not symbol and not all_flag:
            all_flag = True

        # use_local_db=True 表示使用本地資料庫，use_neon=False
        # use_local_db=False 表示使用 Neon 資料庫，use_neon=True
        use_neon = not use_local_db

        result = compute_returns_task(
            symbol=symbol,
            start=start,
            end=end,
            all=all_flag,
            limit=limit,
            fill_missing=fill_missing,
            use_neon=use_neon,
            upload_to_neon=upload_to_neon,
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

        symbol = params.get('symbol')
        start = params.get('start')
        end = params.get('end')
        all_flag = _to_bool(params.get('all'), False)
        limit = params.get('limit')
        limit = int(limit) if limit not in (None, '', 'null') else None
        fill_missing = _to_bool(params.get('fillMissing') or params.get('fill_missing'), False)
        use_local_db = _to_bool(params.get('use_local_db'), False)
        upload_to_neon = _to_bool(params.get('upload_to_neon'), False)

        if not symbol and not all_flag:
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
                    symbol=symbol,
                    start=start,
                    end=end,
                    all=all_flag,
                    limit=limit,
                    fill_missing=fill_missing,
                    use_neon=use_neon,
                    upload_to_neon=upload_to_neon,
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
            # 先送出握手訊息
            yield f"data: {json.dumps({'event': 'connected'})}\n\n"
            while True:
                item = progress_queue.get()
                if item is None:
                    break
                try:
                    payload = json.dumps(item, ensure_ascii=False)
                except TypeError as encode_err:
                    logger.exception('progress encode 失敗: %s', encode_err)
                    payload = json.dumps({'event': 'error', 'error': 'encode_failed'})
                yield f"data: {payload}\n\n"

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
    try:
        index_symbol = '^TWII'

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
        # 僅抓取股價資料，停用報酬率計算
        update_returns = False
        # ⚠️ 預設不再同步加權指數，僅在明確指定 fetch_market_index=true 時才執行
        fetch_market_index = bool(data.get('fetch_market_index', False))
        
        if not symbols:
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
                # 使用單次查詢獲取所有 symbols 的最新日期
                placeholders = ','.join(['%s'] * len(symbols))
                cursor.execute(f"""
                    SELECT symbol, MAX(date) AS max_date
                    FROM tw_stock_prices
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                """, symbols)
                for row in cursor.fetchall():
                    # row 是 RealDictCursor，鍵為 'symbol', 'max_date'
                    latest_price_date_map[row['symbol']] = row['max_date']
            except Exception as e:
                logger.warning(f"查詢最新股價日期失敗，將以請求日期為準: {e}")
                latest_price_date_map = {}

            # 同步加權指數的資料範圍
            index_sync_summary = None
            if update_prices and fetch_market_index:
                try:
                    cursor.execute(
                        "SELECT MAX(date) AS max_date FROM tw_stock_prices WHERE symbol = %s",
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
                            f"同步加權指數 {index_symbol}，日期範圍: {effective_index_start} ~ {requested_end}"
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
                                rows.append((
                                    index_symbol,
                                    date_val,
                                    _to_float(rec.get('open_price') or rec.get('Open') or rec.get('open')),
                                    _to_float(rec.get('high_price') or rec.get('High') or rec.get('high')),
                                    _to_float(rec.get('low_price') or rec.get('Low') or rec.get('low')),
                                    _to_float(rec.get('close_price') or rec.get('Close') or rec.get('close')),
                                    _to_int(rec.get('volume') or rec.get('Volume'))
                                ))

                            if rows:
                                execute_values(
                                    cursor,
                                    f"""
                                    INSERT INTO {prices_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
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
                                index_sync_summary = {
                                    'symbol': index_symbol,
                                    'status': 'success',
                                    'prices_updated': len(rows),
                                    'mode': 'index'
                                }
                                logger.info(f"加權指數同步完成，寫入 {len(rows)} 筆資料")
                            else:
                                logger.info("加權指數資料為空，略過寫入")
                        else:
                            logger.warning("加權指數抓取結果為 None，略過寫入")
                    else:
                        logger.info("加權指數無需更新（起始日期晚於結束日期）")
                except Exception as index_exc:
                    logger.exception(f"同步加權指數失敗: {index_exc}")
                    errors.append({'symbol': index_symbol, 'error': str(index_exc)})

            # 🚀 批量抓取模式：一次性抓取所有股票
            if update_prices and use_batch_mode and len(symbols) > 1:
                logger.info(f"🚀 啟用批量抓取模式，準備抓取 {len(symbols)} 檔股票")
                
                # 過濾出上市股票代碼（去除 .TW 後綴）
                twse_codes = []
                for sym in symbols:
                    if '.TW' in sym:
                        code = sym.split('.')[0]
                        if code.isdigit():
                            twse_codes.append(code)
                
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
                                values = []
                                for pr in price_records:
                                    values.append((
                                        symbol,
                                        pr.get('Date'),
                                        pr.get('Open'),
                                        pr.get('High'),
                                        pr.get('Low'),
                                        pr.get('Close'),
                                        pr.get('Volume')
                                    ))
                                
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
                                    execute_values(cursor, upsert_sql, values, page_size=1000)
                                    db_manager.connection.commit()
                                    
                                    results.append({
                                        'symbol': symbol,
                                        'status': 'success',
                                        'prices_updated': len(values),
                                        'mode': 'batch'
                                    })
                                    logger.info(f"✅ {symbol} 批量寫入 {len(values)} 筆")
                        except Exception as e:
                            logger.error(f"批量寫入 {symbol} 失敗: {e}")
                            errors.append({'symbol': symbol, 'error': str(e)})
                    
                    logger.info(f"🎉 批量抓取完成，成功處理 {len(results)} 檔股票")
            
            # 🔄 逐檔抓取模式（備用或非批量模式）
            else:
                for i, symbol in enumerate(symbols):
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
                                for pr in price_records:
                                    record_date = pr.get('date') or pr.get('Date')
                                    dates.append(record_date)
                                    values.append((
                                        symbol,
                                        record_date,
                                        pr.get('open_price') or pr.get('Open'),
                                        pr.get('high_price') or pr.get('High'),
                                        pr.get('low_price') or pr.get('Low'),
                                        pr.get('close_price') or pr.get('Close'),
                                        pr.get('volume') or pr.get('Volume')
                                    ))

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
                                        logger.warning(f"統計 {symbol} 既有日期失敗，略過重複統計: {e}")
                                        duplicate_count = 0

                                    upsert_sql = """
                                        INSERT INTO tw_stock_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
                                        VALUES %s
                                        ON CONFLICT (symbol, date) DO UPDATE SET
                                            open_price = EXCLUDED.open_price,
                                            high_price = EXCLUDED.high_price,
                                            low_price = EXCLUDED.low_price,
                                            close_price = EXCLUDED.close_price,
                                            volume = EXCLUDED.volume
                                    """
                                    try:
                                        execute_values(cursor, upsert_sql, values, page_size=1000)
                                        db_manager.connection.commit()
                                    except Exception as e:
                                        logger.warning(f"批量寫入 {symbol} 價格數據失敗，將嘗試較小批次: {e}")
                                        # 回退為小批次
                                        batch = 200
                                        for idx in range(0, len(values), batch):
                                            sub = values[idx:idx+batch]
                                            execute_values(cursor, upsert_sql, sub, page_size=len(sub))
                                        db_manager.connection.commit()
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
                                if existing_total is not None:
                                    result['existing_records'] = existing_total
                        
                        # 報酬率計算已停用（僅處理股價）
                        # 如需啟用，請使用 /api/returns/compute 端點
                        pass  # 僅處理股價，不計算報酬率
                        
                        if False:  # 報酬率計算區塊已完全停用
                            pass  # 以下代碼不會執行
                            if weekly_returns:
                                for wr in weekly_returns:
                                    week_end_date = pd.to_datetime(wr['Date'])
                                    # 找到該週的所有交易日
                                    for dr in daily_returns:
                                        daily_date = pd.to_datetime(dr['Date'])
                                        # 檢查是否在同一週（週日為一週開始）
                                        if daily_date.isocalendar()[1] == week_end_date.isocalendar()[1] and daily_date.year == week_end_date.year:
                                            weekly_dict[dr['Date']] = wr['return']
                            
                            # 為月報酬率建立映射 - 將月報酬率分配給該月的所有交易日
                            if monthly_returns:
                                for mr in monthly_returns:
                                    month_end_date = pd.to_datetime(mr['Date'])
                                    # 找到該月的所有交易日
                                    for dr in daily_returns:
                                        daily_date = pd.to_datetime(dr['Date'])
                                        # 檢查是否在同一月
                                        if daily_date.year == month_end_date.year and daily_date.month == month_end_date.month:
                                            monthly_dict[dr['Date']] = mr['return']
                            for return_record in daily_returns:
                                try:
                                    date_str = return_record.get('Date')
                                    weekly_return = None
                                    monthly_return = None
                                    
                                    if weekly_returns:
                                        for wr in weekly_returns:
                                            week_end_date = pd.to_datetime(wr['Date'])
                                            # 找到該週的所有交易日
                                            daily_date = pd.to_datetime(date_str)
                                            # 檢查是否在同一週（週日為一週開始）
                                            if daily_date.isocalendar()[1] == week_end_date.isocalendar()[1] and daily_date.year == week_end_date.year:
                                                weekly_return = wr['return']
                                                break
                                    
                                    if monthly_returns:
                                        for mr in monthly_returns:
                                            month_end_date = pd.to_datetime(mr['Date'])
                                            # 找到該月的所有交易日
                                            daily_date = pd.to_datetime(date_str)
                                            # 檢查是否在同一月
                                            if daily_date.year == month_end_date.year and daily_date.month == month_end_date.month:
                                                monthly_return = mr['return']
                                                break
                                    
                                    daily_return = return_record.get('return')
                                    cumulative_return = return_record.get('cumulative_return')

                                    if daily_return is not None and (math.isinf(daily_return) or math.isnan(daily_return)):
                                        daily_return = None
                                    if weekly_return is not None and (math.isinf(weekly_return) or math.isnan(weekly_return)):
                                        weekly_return = None
                                    if monthly_return is not None and (math.isinf(monthly_return) or math.isnan(monthly_return)):
                                        monthly_return = None
                                    if cumulative_return is not None and (math.isinf(cumulative_return) or math.isnan(cumulative_return)):
                                        cumulative_return = None

                                    return_values.append((
                                        symbol,
                                        date_str,
                                        daily_return,
                                        weekly_return,
                                        monthly_return,
                                        cumulative_return
                                    ))
                                    return_dates.append(date_str)
                                except Exception as e:
                                    logger.warning(f"準備 {symbol} 報酬率數據失敗: {e}")

                            if return_values:
                                returns_upsert_sql = """
                                    INSERT INTO tw_stock_returns (symbol, date, daily_return, weekly_return, monthly_return, cumulative_return)
                                    VALUES %s
                                    ON CONFLICT (symbol, date)
                                    DO UPDATE SET
                                        daily_return = EXCLUDED.daily_return,
                                        weekly_return = EXCLUDED.weekly_return,
                                        monthly_return = EXCLUDED.monthly_return,
                                        cumulative_return = EXCLUDED.cumulative_return
                                """
                                try:
                                    execute_values(cursor, returns_upsert_sql, return_values, page_size=2000)
                                    db_manager.connection.commit()
                                except Exception as e:
                                    logger.warning(f"批量寫入報酬率失敗，改用小批次: {e}")
                                    batch = 500
                                    for idx in range(0, len(return_values), batch):
                                        sub = return_values[idx:idx+batch]
                                        execute_values(cursor, returns_upsert_sql, sub, page_size=len(sub))
                                    db_manager.connection.commit()

                            result['return_records'] = len(return_values)
                            
                            # 添加報酬率日期範圍資訊
                            if return_dates:
                                return_dates.sort()
                                result['return_date_range'] = {
                                    'start': return_dates[0],
                                    'end': return_dates[-1],
                                    'requested_start': start_date,
                                    'requested_end': end_date,
                                    'trading_days_count': len(return_dates)
                                }
                    
                        results.append(result)
                    
                    except Exception as e:
                        errors.append({'symbol': symbol, 'error': str(e)})
                        logger.error(f"更新 {symbol} 失敗: {e}")
            
            db_manager.connection.commit()
        except Exception as batch_error:
            db_manager.connection.rollback()
            logger.error(f"批次更新失敗: {batch_error}")
            errors.append({'symbol': 'batch', 'error': str(batch_error)})
        finally:
            db_manager.disconnect()
        
        return jsonify({
            'success': len(errors) == 0,
            'results': results,
            'errors': errors,
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
            except:
                pass
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

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
            or 'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require'
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
    """獲取本地資料庫的所有表格列表"""
    try:
        local_config = {
            'host': os.environ.get('DB_HOST', 'localhost'),
            'port': os.environ.get('DB_PORT', '5432'),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', 's8304021'),
            'database': os.environ.get('DB_NAME', 'postgres')
        }
        
        conn = psycopg2.connect(**local_config, cursor_factory=RealDictCursor)
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
                cursor.execute(f'SELECT COUNT(*) as count FROM "{table_name}"')
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
        
        neon_url = (
            os.environ.get('DATABASE_URL')
            or os.environ.get('NEON_DATABASE_URL')
            or 'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require'
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
            'password': os.environ.get('DB_PASSWORD', 's8304021'),
            'database': os.environ.get('DB_NAME', 'postgres')
        }
        
        try:
            local_conn = psycopg2.connect(**local_config, cursor_factory=RealDictCursor)
            logger.info("✅ 本地資料庫連接成功")
        except Exception as e:
            logger.error(f"❌ 本地資料庫連接失敗: {e}")
            raise
        
        logger.info("☁️ 連接 Neon 資料庫...")
        try:
            neon_conn = psycopg2.connect(neon_url, cursor_factory=RealDictCursor)
            logger.info("✅ Neon 資料庫連接成功")
        except Exception as e:
            logger.error(f"❌ Neon 資料庫連接失敗: {e}")
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
                
                col_names = [col['column_name'] for col in columns]
                inserted_count = 0
                
                if row_count == 0:
                    logger.info(f"⚠️ {table_name} 是空表格，跳過")
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
                            
                        except Exception as e:
                            logger.error(f"Error batch inserting into {table_name}: {e}")
                            neon_conn.rollback()
                        
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
                            
                            neon_conn.commit()
                
                results['tables'].append({
                    'name': table_name,
                    'rowCount': row_count,
                    'insertedCount': inserted_count,
                    'success': True
                })
                
                results['totalRows'] += inserted_count
                logger.info(f"✓ {table_name}: {inserted_count}/{row_count} rows uploaded")
                
            except Exception as e:
                logger.error(f"Error processing table {table_name}: {e}")
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
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Database sync error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    import sys
    # 支援以環境變數 PORT 指定埠號，預設 5003
    try:
        port = int(os.getenv('PORT', '5003'))
    except Exception:
        port = 5003

    print("Taiwan Stock Data API Server Starting...")
    print("API Endpoints:")
    print("   GET  /api/symbols - Get all stock symbols")
    print("   GET  /api/stock/<symbol>/prices - Get stock price data")
    print("   GET  /api/stock/<symbol>/returns - Get return data")
    print("   POST /api/update - Batch update stock data")
    print("   GET  /api/health - Health check")
    print("   GET  /api/income-statement?year=YYYY&season=S - Income statement wide data for all stocks")
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
