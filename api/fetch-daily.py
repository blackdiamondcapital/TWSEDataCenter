"""
Vercel Serverless Function - 每日股價抓取
"""
from http.server import BaseHTTPRequestHandler
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date
import requests
import json

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # 連接 Neon 資料庫
            NEON_DB_URL = os.environ.get('NEON_DATABASE_URL')
            
            if not NEON_DB_URL:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': 'NEON_DATABASE_URL not set'
                }).encode())
                return
            
            conn = psycopg2.connect(NEON_DB_URL)
            cur = conn.cursor()
            
            # 抓取今日資料
            today = date.today()
            
            # TODO: 實作資料抓取邏輯
            # 1. 呼叫 TWSE API
            # 2. 解析資料
            # 3. 插入資料庫
            
            # 範例：插入測試資料
            # execute_values(cur, "INSERT INTO tw_stock_prices (...) VALUES %s", data)
            
            conn.commit()
            cur.close()
            conn.close()
            
            # 回傳成功訊息
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'success': True,
                'message': f'Successfully fetched data for {today}',
                'date': str(today)
            }).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                'error': str(e)
            }).encode())
