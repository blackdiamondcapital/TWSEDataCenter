#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自動排程系統 - 每日抓取股價資料並同步到本地端和 Neon 資料庫
"""

import os
import sys
import logging
from datetime import datetime, time as dt_time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 資料庫配置
LOCAL_DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 's8304021',
    'database': 'postgres'
}

NEON_DB_URL = os.environ.get('NEON_DATABASE_URL') or \
    'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require'

API_BASE = "http://localhost:5003"


class StockDataScheduler:
    """股價資料自動排程器"""
    
    def __init__(self):
        self.scheduler = BlockingScheduler()
        
    def fetch_daily_data(self):
        """抓取當日股價資料"""
        logger.info("=" * 60)
        logger.info("開始執行每日股價資料抓取任務")
        logger.info("=" * 60)
        
        try:
            # 1. 抓取上市股票資料
            logger.info("📊 抓取上市股票資料...")
            self._fetch_twse_data()
            
            # 2. 抓取上櫃股票資料
            logger.info("📊 抓取上櫃股票資料...")
            self._fetch_tpex_data()
            
            # 3. 同步到 Neon 資料庫
            logger.info("☁️  同步資料到 Neon 資料庫...")
            self._sync_to_neon()
            
            # 4. 計算報酬率
            logger.info("📈 計算報酬率...")
            self._calculate_returns()
            
            logger.info("✅ 每日股價資料抓取任務完成！")
            
        except Exception as e:
            logger.error(f"❌ 執行任務時發生錯誤: {e}", exc_info=True)
    
    def _fetch_twse_data(self):
        """抓取上市股票資料"""
        try:
            # 使用 smart_refresh.py 抓取資料
            result = subprocess.run(
                [sys.executable, 'smart_refresh.py', '--market', 'twse'],
                capture_output=True,
                text=True,
                timeout=3600  # 1小時超時
            )
            
            if result.returncode == 0:
                logger.info("✅ 上市股票資料抓取成功")
                logger.debug(result.stdout)
            else:
                logger.error(f"❌ 上市股票資料抓取失敗: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("❌ 上市股票資料抓取超時")
        except Exception as e:
            logger.error(f"❌ 抓取上市股票資料時發生錯誤: {e}")
    
    def _fetch_tpex_data(self):
        """抓取上櫃股票資料"""
        try:
            result = subprocess.run(
                [sys.executable, 'smart_refresh.py', '--market', 'tpex'],
                capture_output=True,
                text=True,
                timeout=3600
            )
            
            if result.returncode == 0:
                logger.info("✅ 上櫃股票資料抓取成功")
                logger.debug(result.stdout)
            else:
                logger.error(f"❌ 上櫃股票資料抓取失敗: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("❌ 上櫃股票資料抓取超時")
        except Exception as e:
            logger.error(f"❌ 抓取上櫃股票資料時發生錯誤: {e}")
    
    def _sync_to_neon(self):
        """同步資料到 Neon 資料庫"""
        try:
            # 連接本地資料庫
            local_conn = psycopg2.connect(**LOCAL_DB_CONFIG)
            local_cur = local_conn.cursor(cursor_factory=RealDictCursor)
            
            # 連接 Neon 資料庫
            neon_conn = psycopg2.connect(NEON_DB_URL)
            neon_cur = neon_conn.cursor()
            
            # 取得今日資料
            today = datetime.now().date()
            local_cur.execute("""
                SELECT symbol, date, open_price, high_price, low_price, 
                       close_price, volume, transaction_count
                FROM tw_stock_prices
                WHERE date = %s
            """, (today,))
            
            rows = local_cur.fetchall()
            logger.info(f"📦 準備同步 {len(rows)} 筆資料到 Neon")
            
            if rows:
                # 批次插入到 Neon（使用 ON CONFLICT 避免重複）
                from psycopg2.extras import execute_values
                
                execute_values(
                    neon_cur,
                    """
                    INSERT INTO tw_stock_prices 
                    (symbol, date, open_price, high_price, low_price, 
                     close_price, volume, transaction_count)
                    VALUES %s
                    ON CONFLICT (symbol, date) 
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        transaction_count = EXCLUDED.transaction_count
                    """,
                    [(r['symbol'], r['date'], r['open_price'], r['high_price'],
                      r['low_price'], r['close_price'], r['volume'], 
                      r['transaction_count']) for r in rows]
                )
                
                neon_conn.commit()
                logger.info(f"✅ 成功同步 {len(rows)} 筆資料到 Neon")
            else:
                logger.warning("⚠️  沒有今日資料需要同步")
            
            # 關閉連接
            local_cur.close()
            local_conn.close()
            neon_cur.close()
            neon_conn.close()
            
        except Exception as e:
            logger.error(f"❌ 同步到 Neon 時發生錯誤: {e}", exc_info=True)
    
    def _calculate_returns(self):
        """計算報酬率"""
        try:
            # 呼叫 API 計算報酬率
            response = requests.post(
                f"{API_BASE}/api/compute-returns",
                json={"force": False},
                timeout=600
            )
            
            if response.status_code == 200:
                logger.info("✅ 報酬率計算成功")
            else:
                logger.error(f"❌ 報酬率計算失敗: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ 計算報酬率時發生錯誤: {e}")
    
    def weekly_full_refresh(self):
        """每週完整資料回補"""
        logger.info("=" * 60)
        logger.info("開始執行每週完整資料回補任務")
        logger.info("=" * 60)
        
        try:
            result = subprocess.run(
                [sys.executable, 'smart_refresh.py', '--full'],
                capture_output=True,
                text=True,
                timeout=7200  # 2小時超時
            )
            
            if result.returncode == 0:
                logger.info("✅ 每週完整資料回補成功")
                logger.debug(result.stdout)
            else:
                logger.error(f"❌ 每週完整資料回補失敗: {result.stderr}")
            
            # 同步到 Neon
            self._sync_to_neon()
            
        except Exception as e:
            logger.error(f"❌ 每週回補時發生錯誤: {e}", exc_info=True)
    
    def start(self):
        """啟動排程器"""
        logger.info("🚀 啟動股價資料自動排程系統")
        
        # 每日排程：週一到週五 15:30 執行（收盤後）
        self.scheduler.add_job(
            self.fetch_daily_data,
            CronTrigger(day_of_week='mon-fri', hour=15, minute=30),
            id='daily_fetch',
            name='每日股價資料抓取',
            replace_existing=True
        )
        logger.info("📅 已設定每日排程：週一至週五 15:30")
        
        # 每週排程：週日 02:00 執行完整回補
        self.scheduler.add_job(
            self.weekly_full_refresh,
            CronTrigger(day_of_week='sun', hour=2, minute=0),
            id='weekly_refresh',
            name='每週完整資料回補',
            replace_existing=True
        )
        logger.info("📅 已設定每週排程：週日 02:00")
        
        # 顯示所有排程任務
        logger.info("\n排程任務列表：")
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.name}: {job.trigger}")
        
        logger.info("\n✅ 排程器已啟動，等待執行任務...")
        logger.info("按 Ctrl+C 停止排程器\n")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("\n👋 排程器已停止")


def main():
    """主程式"""
    import argparse
    
    parser = argparse.ArgumentParser(description='股價資料自動排程系統')
    parser.add_argument('--test', action='store_true', help='測試執行一次抓取任務')
    parser.add_argument('--weekly', action='store_true', help='測試執行每週回補任務')
    
    args = parser.parse_args()
    
    scheduler = StockDataScheduler()
    
    if args.test:
        logger.info("🧪 測試模式：執行一次抓取任務")
        scheduler.fetch_daily_data()
    elif args.weekly:
        logger.info("🧪 測試模式：執行每週回補任務")
        scheduler.weekly_full_refresh()
    else:
        scheduler.start()


if __name__ == '__main__':
    main()
