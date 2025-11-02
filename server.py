#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BWIBBU 歷史資料回朔工具 - 後端
支援日期範圍選擇、資料庫切換、進度追蹤
"""

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from time import sleep
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import execute_values

# ============ 日誌設定 ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============ Flask 應用 ============
app = Flask(__name__)
CORS(app)

# ============ 資料庫管理 ============
class DatabaseManager:
    def __init__(self, use_local=False):
        self.use_local = use_local
        self.connection = None
        self.db_url = None if use_local else (
            os.environ.get('DATABASE_URL')
            or os.environ.get('NEON_DATABASE_URL')
            or 'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
        )
        ssl_default = 'require' if self.db_url else 'prefer'
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 's8304021'),
            'sslmode': os.getenv('DB_SSLMODE', ssl_default)
        }
    
    def connect(self):
        try:
            if self.db_url:
                conn_args = {'sslmode': self.config.get('sslmode', 'require')}
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
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password'],
                    sslmode=self.config.get('sslmode', 'prefer')
                )
            logger.info(f"資料庫連接成功 ({'本地' if self.use_local else 'Neon'})")
            return True
        except Exception as e:
            logger.error(f"資料庫連接失敗: {e}")
            return False
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("資料庫連接已關閉")
    
    def create_tables(self):
        if not self.connection:
            return False
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tw_stock_bwibbu (
                    id SERIAL PRIMARY KEY,
                    code VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    name VARCHAR(100),
                    pe_ratio FLOAT,
                    dividend_yield FLOAT,
                    pb_ratio FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(code, date)
                )
            """)
            self.connection.commit()
            logger.info("表格已建立或已存在")
            return True
        except Exception as e:
            logger.error(f"建立表格失敗: {e}")
            return False

# ============ BWIBBU 資料抓取 ============
class BWIBBUFetcher:
    def __init__(self):
        self.url = "https://www.twse.com.tw/exchangeReport/BWIBBU_d"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json,text/html',
            'Referer': 'https://www.twse.com.tw/zh/trading/historical/bwibbu-day.html',
            'Accept-Language': 'zh-TW,zh;q=0.9'
        })
        self.tpex_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
        self.tpex_session = requests.Session()
        self.tpex_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json,text/html',
            'Referer': 'https://www.tpex.org.tw'
        })
        # 預熱 historical 頁面
        try:
            self.session.get('https://www.twse.com.tw/zh/trading/historical/bwibbu-day.html', timeout=10)
            logger.info("TWSE Session 預熱成功")
        except Exception as e:
            logger.warning(f"TWSE Session 預熱失敗: {e}")

    @staticmethod
    def _to_num(value):
        if value in ('', 'NaN', 'null', 'None', '--', '---', 'N/A'):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_roc_date(dt):
        return f"{dt.year - 1911:03d}{dt.month:02d}{dt.day:02d}"

    def fetch_date(self, target_date, retries=3, pause=0.8):
        """抓取單一日期的 BWIBBU 資料"""
        if isinstance(target_date, str):
            try:
                dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            except Exception:
                return None
        else:
            dt = target_date
        
        ymd = dt.strftime('%Y%m%d')
        params = {'response': 'json', 'date': ymd, 'selectType': 'ALL'}
        
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(self.url, params=params, timeout=15)
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}")

                
                js = resp.json()
                if js.get('stat') and '沒有符合' in js['stat']:
                    return None
                
                rows = js.get('data') or []
                if not rows:
                    return None
                
                records = []
                for r in rows:
                    try:
                        code = (r[0] or '').strip() if len(r) > 0 else ''
                        name = (r[1] or '').strip() if len(r) > 1 else ''
                        dy_raw = (r[3] or '').strip() if len(r) > 3 else ''
                        pe_raw = (r[5] or '').strip() if len(r) > 5 else ''
                        pb_raw = (r[6] or '').strip() if len(r) > 6 else ''

                        records.append({
                            'code': code,
                            'name': name,
                            'date': dt.strftime('%Y-%m-%d'),
                            'dividend_yield': self._to_num(dy_raw),
                            'pe_ratio': self._to_num(pe_raw),
                            'pb_ratio': self._to_num(pb_raw)
                        })
                    except Exception:
                        continue
                
                return records if records else None
            
            except Exception as e:
                logger.warning(f"抓取 {ymd} 失敗 (attempt {attempt}/{retries}): {e}")
                if attempt < retries:
                    sleep(pause)
        
        return None
    
    def fetch_range(self, start_date, end_date):
        """抓取日期範圍內的所有工作日"""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = pd.bdate_range(start=start_dt, end=end_dt, freq='C')
        
        all_records = []
        for d in dates[::-1]:  # 由近往遠
            date_str = d.strftime('%Y%m%d')
            records = self.fetch_date(d)
            if records:
                all_records.extend(records)
            tpex_records = self.fetch_tpex_date(d)
            if tpex_records:
                all_records.extend(tpex_records)
            sleep(0.2)  # 禮貌延遲
        
        return all_records

    def fetch_range_stats(self, start_date, end_date):
        """抓取日期範圍，並回傳每日上市/上櫃筆數與公司數的統計"""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        dates = pd.bdate_range(start=start_dt, end=end_dt, freq='C')

        all_records = []
        daily_stats = {}

        for d in dates[::-1]:  # 由近往遠
            dt = d.date()
            iso = dt.strftime('%Y-%m-%d')

            twse = self.fetch_date(d) or []
            tpex = self.fetch_tpex_date(d) or []

            all_records.extend(twse)
            all_records.extend(tpex)

            daily_stats[iso] = {
                'twse_count': len(twse),
                'tpex_count': len(tpex),
                'twse_companies': len({r['code'] for r in twse}),
                'tpex_companies': len({r['code'] for r in tpex}),
                'total_count': len(twse) + len(tpex),
                'total_companies': len({r['code'] for r in (twse + tpex)}),
            }

            sleep(0.2)

        return all_records, daily_stats

    def fetch_tpex_date(self, target_date, retries=3, pause=0.8):
        if isinstance(target_date, str):
            try:
                dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            except Exception:
                return None
        else:
            dt = target_date if isinstance(target_date, date) else target_date.date()

        roc_date = self._to_roc_date(dt)
        # 使用 TPEx 歷史查詢頁面 JSON 介面，支援 ROC 日期（YYY/MM/DD）
        tpex_hist_url = "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php"
        params = {
            'l': 'zh-tw',
            'o': 'json',
            'd': f"{roc_date[:3]}/{roc_date[3:5]}/{roc_date[5:]}"
        }

        for attempt in range(1, retries + 1):
            try:
                resp = self.tpex_session.get(tpex_hist_url, params=params, timeout=20)
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}")

                js = resp.json()
                # 結構：{"tables":[{"data":[[header...],[code,name,PE,DividendPerShare,ROCYear,Yield,PBR,EPSQ]...]}],"date":"YYYYMMDD","stat":"ok"}
                tables = js.get('tables') or []
                if not tables:
                    return None
                data_rows = tables[0].get('data') or []
                if not data_rows or len(data_rows) <= 1:
                    return None
                # 第一列為表頭，從第二列起是資料
                rows = data_rows[1:]

                records = []
                for r in rows:
                    try:
                        code = (r[0] or '').strip()
                        name = (r[1] or '').strip()
                        pe_raw = (r[2] or '').strip() if len(r) > 2 else ''
                        div_raw = (r[3] or '').strip() if len(r) > 3 else ''  # 每股股利
                        # r[4] 可能是 ROC 年別，略過
                        dy_raw = (r[5] or '').strip() if len(r) > 5 else ''
                        pb_raw = (r[6] or '').strip() if len(r) > 6 else ''

                        records.append({
                            'code': code,
                            'name': name,
                            'date': dt.strftime('%Y-%m-%d'),
                            'dividend_yield': self._to_num(dy_raw),
                            'pe_ratio': self._to_num(pe_raw),
                            'pb_ratio': self._to_num(pb_raw)
                        })
                    except Exception:
                        continue

                return records if records else None

            except Exception as e:
                logger.warning(f"TPEx 抓取 {roc_date} 失敗 (attempt {attempt}/{retries}): {e}")
                if attempt < retries:
                    sleep(pause)

        return None

# ============ 全域實例 ============
fetcher = BWIBBUFetcher()

# ============ API 端點 ============
@app.route('/api/backfill', methods=['POST'])
def backfill_bwibbu():
    """回朔 BWIBBU 資料到指定資料庫"""
    try:
        payload = request.get_json() or {}
        start_date = payload.get('start')
        end_date = payload.get('end')
        use_local_db = payload.get('use_local_db', False)
        skip_existing = payload.get('skip_existing', False)
        
        if not start_date or not end_date:
            return jsonify({'success': False, 'error': '缺少 start 或 end'}), 400
        
        # 驗證日期格式
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except:
            return jsonify({'success': False, 'error': '日期格式錯誤，需 YYYY-MM-DD'}), 400
        
        # 連接資料庫
        db = DatabaseManager(use_local=use_local_db)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
        
        try:
            db.create_tables()
            
            # 抓取資料
            logger.info(f"開始回朔 {start_date} 到 {end_date}")
            records, daily_stats = fetcher.fetch_range_stats(start_date, end_date)
            
            if not records:
                return jsonify({
                    'success': True,
                    'message': '指定期間無有效資料',
                    'total_records': 0,
                    'available_dates': [],
                    'daily_stats': {}
                })
            
            # 寫入資料庫
            cursor = db.connection.cursor()
            values = []
            available_dates = set()
            
            for rec in records:
                try:
                    rec_date = datetime.strptime(rec['date'], '%Y-%m-%d').date()
                    values.append((
                        rec['code'],
                        rec_date,
                        rec['name'],
                        rec['pe_ratio'],
                        rec['dividend_yield'],
                        rec['pb_ratio']
                    ))
                    available_dates.add(rec['date'])
                except Exception as e:
                    logger.warning(f"記錄解析失敗: {e}")
                    continue
            
            if values:
                if skip_existing:
                    upsert_sql = (
                        """
                        INSERT INTO tw_stock_bwibbu (code, date, name, pe_ratio, dividend_yield, pb_ratio)
                        VALUES %s
                        ON CONFLICT (code, date) DO NOTHING
                        """
                    )
                else:
                    upsert_sql = (
                        """
                        INSERT INTO tw_stock_bwibbu (code, date, name, pe_ratio, dividend_yield, pb_ratio)
                        VALUES %s
                        ON CONFLICT (code, date) DO UPDATE SET
                            name = EXCLUDED.name,
                            pe_ratio = EXCLUDED.pe_ratio,
                            dividend_yield = EXCLUDED.dividend_yield,
                            pb_ratio = EXCLUDED.pb_ratio,
                            updated_at = CURRENT_TIMESTAMP
                        """
                    )

                execute_values(cursor, upsert_sql, values, page_size=500)
                db.connection.commit()
                logger.info(f"寫入 {len(values)} 筆記錄")
            
            return jsonify({
                'success': True,
                'total_records': len(values),
                'available_dates': sorted(list(available_dates)),
                'message': f'成功寫入 {len(values)} 筆記錄',
                'daily_stats': daily_stats,
                'write_mode': 'insert_only' if skip_existing else 'upsert'
            })
        
        finally:
            db.disconnect()
    
    except Exception as e:
        logger.error(f"回朔失敗: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/query', methods=['GET'])
def query_bwibbu():
    """查詢資料庫中的 BWIBBU 資料"""
    try:
        use_local_db = request.args.get('use_local_db', 'false').lower() == 'true'
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        
        db = DatabaseManager(use_local=use_local_db)
        if not db.connect():
            return jsonify({'success': False, 'error': '資料庫連線失敗'}), 500
        
        try:
            cursor = db.connection.cursor()
            
            if start_date and end_date:
                cursor.execute("""
                    SELECT DISTINCT date FROM tw_stock_bwibbu
                    WHERE date BETWEEN %s AND %s
                    ORDER BY date DESC
                """, (start_date, end_date))
            else:
                cursor.execute("""
                    SELECT DISTINCT date FROM tw_stock_bwibbu
                    ORDER BY date DESC
                """)
            
            dates = [row[0].isoformat() for row in cursor.fetchall()]
            
            cursor.execute("SELECT COUNT(*) FROM tw_stock_bwibbu")
            total_count = cursor.fetchone()[0]
            
            return jsonify({
                'success': True,
                'dates': dates,
                'total_count': total_count
            })
        
        finally:
            db.disconnect()
    
    except Exception as e:
        logger.error(f"查詢失敗: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
