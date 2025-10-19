#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import logging
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, send_from_directory, send_file, Response
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from urllib.parse import urlparse
import os
import math
import threading  # 添加线程模块
from queue import Queue

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

class DatabaseManager:
    def __init__(self, use_local: bool = False):
        self.use_local = use_local
        self.db_url = (
            None if use_local else (
                os.environ.get('DATABASE_URL')
                or os.environ.get('NEON_DATABASE_URL')
                or 'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
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
                conn_args = {
                    'cursor_factory': RealDictCursor,
                    'sslmode': self.db_config.get('sslmode', 'require')
                }
                try:
                    self.connection = psycopg2.connect(self.db_url, **conn_args)
                except psycopg2.OperationalError as exc:
                    if 'channel binding' in str(exc).lower() and 'channel_binding=require' in self.db_url:
                        safe_url = self.db_url.replace('channel_binding=require', 'channel_binding=disable')
                        logger.warning("channel_binding=require 不支援，改為 disable")
                        self.connection = psycopg2.connect(safe_url, **conn_args)
                    else:
                        raise
            else:
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['database'],
                    cursor_factory=RealDictCursor,
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
                CREATE TABLE IF NOT EXISTS stock_symbols (
                    symbol VARCHAR(20) PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    market VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 創建股價數據表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
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
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS stock_prices_symbol_date_idx
                ON stock_prices(symbol, date);
            """)
            
            # 創建報酬率數據表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_returns (
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
            """)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS stock_returns_symbol_date_idx
                ON stock_returns(symbol, date);
            """)
            
            # 為現有表添加新欄位（如果不存在）
            try:
                cursor.execute("""
                    ALTER TABLE stock_returns 
                    ADD COLUMN IF NOT EXISTS weekly_return DECIMAL(10,6),
                    ADD COLUMN IF NOT EXISTS monthly_return DECIMAL(10,6);
                """)
            except Exception as e:
                logger.warning(f"添加新欄位時出現警告: {e}")
                # 嘗試單獨添加每個欄位
                try:
                    cursor.execute("ALTER TABLE stock_returns ADD COLUMN IF NOT EXISTS weekly_return DECIMAL(10,6);")
                    cursor.execute("ALTER TABLE stock_returns ADD COLUMN IF NOT EXISTS monthly_return DECIMAL(10,6);")
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

class StockDataAPI:
    def __init__(self):
        self.symbols_cache = None
        self.cache_time = None
        self.db_manager = DatabaseManager()
        
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
            
            # 檢查是否為台灣加權指數，使用證交所指數API
            if symbol == '^TWII':
                logger.info(f"檢測到台灣加權指數 {symbol}，使用證交所指數API抓取")
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
stock_api = StockDataAPI()

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

def _upsert_prices(cursor, symbol, price_records):
    """將價格資料批量 upsert 進 stock_prices。price_records: list[dict] with keys Date/Open/High/Low/Close/Volume 或對應小寫欄位。
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
        INSERT INTO stock_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
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
        cur.execute("SELECT close_price FROM stock_prices WHERE symbol=%s AND date < %s ORDER BY date DESC LIMIT 1", [symbol, first_date])
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
            FROM stock_prices
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
    """偵測 stock_prices 異常跳動。query: symbol, start, end, threshold=0.2"""
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
                            FROM stock_prices
                            WHERE symbol = %s AND date IN ({placeholders})
                        """,
                        [rule_ver, threshold, sym] + date_list
                    )
                    cur.execute(
                        f"DELETE FROM stock_prices WHERE symbol = %s AND date IN ({placeholders})",
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
        stock_api = StockDataAPI()
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
                            """
                            INSERT INTO stock_prices
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
            
            # 先檢查表是否存在並獲取欄位資訊
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'stock_prices' AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
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
                logger.info("stock_prices 表不存在，嘗試創建...")
                db_manager.create_tables()
                return jsonify({
                    'success': False,
                    'error': '資料庫表不存在，已嘗試創建，請重新查詢'
                }), 500
            
            # 檢查是否有必要的欄位，如果沒有則重新創建表
            required_columns = ['symbol', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            missing_columns = [col for col in required_columns if col not in columns]
            
            if missing_columns:
                logger.warning(f"stock_prices 表缺少欄位: {missing_columns}")
                # 刪除舊表並重新創建
                cursor.execute("DROP TABLE IF EXISTS stock_prices CASCADE")
                cursor.execute("DROP TABLE IF EXISTS stock_returns CASCADE")
                db_manager.connection.commit()
                logger.info("已刪除舊表，重新創建...")
                db_manager.create_tables()
                return jsonify({
                    'success': False,
                    'error': '資料庫表結構不完整，已重新創建，請重新查詢'
                }), 500
            
            logger.info(f"stock_prices 表欄位: {columns}")
            
            # 根據實際欄位構建查詢
            if 'open_price' in columns:
                query = """
                    SELECT date, open_price, high_price, low_price, close_price, volume
                    FROM stock_prices 
                    WHERE symbol = %s
                """
            else:
                # 如果沒有 open_price 等欄位，可能是舊的表結構
                query = """
                    SELECT date, close_price, volume
                    FROM stock_prices 
                    WHERE symbol = %s
                """
            
            # 支援多種股票代碼格式查詢
            # 如果輸入的是純數字代碼，嘗試匹配完整格式
            if symbol.isdigit():
                # 先嘗試直接查詢，如果沒有結果，再嘗試添加後綴
                cursor.execute("SELECT COUNT(*) FROM stock_prices WHERE symbol = %s", [symbol])
                result = cursor.fetchone()
                count = result[0] if isinstance(result, (list, tuple)) else result.get('count', 0)
                
                if count == 0:
                    # 嘗試查找帶有 .TW 或 .TWO 後綴的股票
                    cursor.execute("""
                        SELECT symbol FROM stock_prices 
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
                FROM stock_returns 
                WHERE symbol = %s
            """
            
            # 支援多種股票代碼格式查詢
            # 如果輸入的是純數字代碼，嘗試匹配完整格式
            if symbol.isdigit():
                # 先嘗試直接查詢，如果沒有結果，再嘗試添加後綴
                cursor.execute("SELECT COUNT(*) FROM stock_returns WHERE symbol = %s", [symbol])
                result = cursor.fetchone()
                count = result[0] if isinstance(result, (list, tuple)) else result.get('count', 0)
                
                if count == 0:
                    # 嘗試查找帶有 .TW 或 .TWO 後綴的股票
                    cursor.execute("""
                        SELECT symbol FROM stock_returns 
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
    """觸發計算 stock_returns 的 API。
    JSON body 支援參數：
      - symbol: 指定單一股票代碼（例如 2330.TW）；若未提供且 all=false，會自動以 all=true。
      - start: 起始日期 YYYY-MM-DD（可選）
      - end: 結束日期 YYYY-MM-DD（可選）
      - all: 是否處理所有在 stock_prices 出現過的股票（預設 false）
      - limit: 當 all=true 時限制處理檔數（可選）
      - fillMissing/fill_missing: 僅計算尚未存在於 stock_returns 的日期（布林，可選）
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
        # 僅抓取股價資料，停用報酬率計算
        update_returns = False
        
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

            # 預先查詢每個 symbol 在 prices/returns 的最新日期，用於增量更新
            latest_price_date_map = {}
            try:
                # 使用單次查詢獲取所有 symbols 的最新日期
                placeholders = ','.join(['%s'] * len(symbols))
                cursor.execute(f"""
                    SELECT symbol, MAX(date) AS max_date
                    FROM stock_prices
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                """, symbols)
                for row in cursor.fetchall():
                    # row 是 RealDictCursor，鍵為 'symbol', 'max_date'
                    latest_price_date_map[row['symbol']] = row['max_date']
            except Exception as e:
                logger.warning(f"查詢最新股價日期失敗，將以請求日期為準: {e}")
                latest_price_date_map = {}

            for i, symbol in enumerate(symbols):
                try:
                    result = {'symbol': symbol, 'status': 'success'}
                    
                    if update_prices:
                        existing_total = None
                        try:
                            cursor.execute(
                                "SELECT COUNT(*) AS total FROM stock_prices WHERE symbol = %s",
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
                                                SELECT date FROM stock_prices
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
                                    INSERT INTO stock_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
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
                    
                    # 已停用報酬率計算（僅處理股價）
                    if False and price_data is not None and (
                        (isinstance(price_data, pd.DataFrame) and not price_data.empty and len(price_data) > 1) or
                        (isinstance(price_data, list) and len(price_data) > 1)
                    ):
                        # 計算各種頻率的報酬率
                        daily_returns = stock_api.calculate_returns(price_data, 'daily')
                        weekly_returns = stock_api.calculate_returns(price_data, 'weekly')
                        monthly_returns = stock_api.calculate_returns(price_data, 'monthly')
                        
                        # 除錯：記錄計算結果
                        logger.info(f"Daily returns count: {len(daily_returns) if daily_returns else 0}")
                        logger.info(f"Weekly returns count: {len(weekly_returns) if weekly_returns else 0}")
                        logger.info(f"Monthly returns count: {len(monthly_returns) if monthly_returns else 0}")
                        if weekly_returns:
                            logger.info(f"Weekly returns sample: {weekly_returns[:2]}")
                        if monthly_returns:
                            logger.info(f"Monthly returns sample: {monthly_returns[:2]}")
                        
                        if daily_returns is not None and len(daily_returns) > 0:
                            # 儲存報酬率數據到資料庫
                            stored_returns = 0
                            return_dates = []
                            
                            # 建立週報酬率和月報酬率的查找字典
                            weekly_dict = {}
                            monthly_dict = {}
                            
                            # 為週報酬率建立映射 - 將週報酬率分配給該週的所有交易日
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
                                    INSERT INTO stock_returns (symbol, date, daily_return, weekly_return, monthly_return, cumulative_return)
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
        
        finally:
            if db_manager:
                db_manager.disconnect()
        
        return jsonify({
            'success': True,
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
                        FROM stock_prices;
                    """)
                    price_stats = cursor.fetchone()
                    
                    # 查詢報酬率數據統計
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_records, 
                            COUNT(DISTINCT symbol) as unique_stocks,
                            MIN(date) as earliest_date,
                            MAX(date) as latest_date
                        FROM stock_returns;
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
                            'stock_prices': {
                                'total_records': price_stats['total_records'],
                                'unique_stocks': price_stats['unique_stocks'],
                                'date_range': {
                                    'earliest': price_stats['earliest_date'].isoformat() if price_stats['earliest_date'] else None,
                                    'latest': price_stats['latest_date'].isoformat() if price_stats['latest_date'] else None
                                }
                            },
                            'stock_returns': {
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
                FROM stock_prices
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
                FROM stock_prices
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
                WHERE table_name = 'stock_prices'
            """)
            if cursor.fetchone()[0] > 0:
                cursor.execute("SELECT COUNT(*) FROM stock_prices")
                total_records = cursor.fetchone()[0]
                
                # 獲取日期範圍
                cursor.execute("""
                    SELECT MIN(date) as start_date, MAX(date) as end_date 
                    FROM stock_prices 
                    WHERE date IS NOT NULL
                """)
                date_range_result = cursor.fetchone()
                
                # 獲取最後更新時間
                cursor.execute("""
                    SELECT MAX(updated_at) as last_update 
                    FROM stock_prices 
                    WHERE updated_at IS NOT NULL
                """)
                last_update_result = cursor.fetchone()
        except Exception as e:
            logger.warning(f"stock_prices 表查詢錯誤: {e}")
        
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
