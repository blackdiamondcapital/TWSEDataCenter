#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""清空資料庫並重新從 2010 年開始完整抓取所有股價數據"""

import argparse
import sys
import psycopg2
from backfill_full_history import main as backfill_main

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 's8304021',
    'database': 'postgres'
}


def confirm_action():
    """確認用戶是否要清空資料庫"""
    print("=" * 80)
    print("警告：此操作將清空 stock_prices 和 stock_returns 表中的所有數據！")
    print("=" * 80)
    response = input("確定要繼續嗎？(輸入 YES 確認): ")
    return response.strip() == "YES"


def truncate_tables():
    """清空股價和報酬率表"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\n正在清空資料表...")
        
        # 清空表格但保留結構
        cursor.execute("TRUNCATE TABLE stock_prices CASCADE;")
        cursor.execute("TRUNCATE TABLE stock_returns CASCADE;")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✓ 資料表已清空\n")
        return True
        
    except Exception as e:
        print(f"✗ 清空資料表失敗: {e}")
        return False


def main(argv):
    parser = argparse.ArgumentParser(
        description="清空資料庫並重新從 2010 年開始完整抓取"
    )
    parser.add_argument(
        "--skip-confirm",
        action="store_true",
        help="跳過確認提示（危險！）"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=15,
        help="每批處理的股票數"
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=2.5,
        help="批次間隔秒數"
    )
    parser.add_argument(
        "--market",
        choices=["tw", "two", "all"],
        default="all",
        help="選擇市場"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制處理前 N 檔（測試用）"
    )
    
    args = parser.parse_args(argv)
    
    # 確認操作
    if not args.skip_confirm:
        if not confirm_action():
            print("操作已取消")
            return 0
    
    # 清空資料表
    if not truncate_tables():
        return 1
    
    # 構建 backfill 參數
    backfill_args = [
        "--start", "2010-01-01",
        "--chunk-size", str(args.chunk_size),
        "--pause", str(args.pause),
        "--timeout", "900",
        "--market", args.market
    ]
    
    if args.limit:
        backfill_args.extend(["--limit", str(args.limit)])
    
    print("開始從 2010-01-01 完整抓取股價數據...\n")
    
    # 呼叫 backfill 函數
    return backfill_main(backfill_args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
