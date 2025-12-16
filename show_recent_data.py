#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
显示最近的数据记录，检查是否有重复
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

# 从环境变量获取数据库连接
db_url = os.getenv('DATABASE_URL')

if not db_url:
    print("❌ 未设置 DATABASE_URL 环境变量")
    print("\n请设置环境变量或手动修改脚本中的连接信息")
    exit(1)

print("🔍 检查最近的数据记录")
print("=" * 80)

try:
    # 连接数据库
    conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    
    # 1. 检查重复记录
    print("\n📊 步骤 1: 检查重复的 (symbol, date) 组合")
    print("-" * 80)
    
    cursor.execute("""
        SELECT symbol, date, COUNT(*) as count
        FROM tw_stock_prices
        GROUP BY symbol, date
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10;
    """)
    
    duplicates = cursor.fetchall()
    
    if duplicates:
        print(f"❌ 发现 {len(duplicates)} 组重复数据：\n")
        print(f"{'股票代码':<15} {'日期':<12} {'重复次数':<8}")
        print("-" * 40)
        for row in duplicates:
            print(f"{row['symbol']:<15} {row['date']} {row['count']:<8}")
        
        # 显示第一组重复的详细信息
        if duplicates:
            first_dup = duplicates[0]
            print(f"\n📋 第一组重复的详细记录 ({first_dup['symbol']}, {first_dup['date']})：")
            print("-" * 80)
            
            cursor.execute("""
                SELECT id, symbol, date, created_at, open_price, close_price, volume
                FROM tw_stock_prices
                WHERE symbol = %s AND date = %s
                ORDER BY id;
            """, [first_dup['symbol'], first_dup['date']])
            
            details = cursor.fetchall()
            print(f"\n{'ID':<8} {'创建时间':<20} {'开盘':<10} {'收盘':<10} {'成交量':<15}")
            print("-" * 70)
            for row in details:
                print(f"{row['id']:<8} {row['created_at'].strftime('%Y-%m-%d %H:%M:%S'):<20} "
                      f"{row['open_price'] or 0:<10.2f} {row['close_price'] or 0:<10.2f} "
                      f"{row['volume'] or 0:>14,}")
    else:
        print("✅ 没有发现重复的 (symbol, date) 组合")
    
    # 2. 显示最近创建的记录
    print("\n📅 步骤 2: 最近创建的 20 条记录")
    print("-" * 80)
    
    cursor.execute("""
        SELECT symbol, date, created_at, open_price, close_price, volume
        FROM tw_stock_prices
        ORDER BY created_at DESC
        LIMIT 20;
    """)
    
    recent = cursor.fetchall()
    
    if recent:
        print(f"\n{'股票代码':<15} {'日期':<12} {'创建时间':<20} {'开盘':<10} {'收盘':<10} {'成交量':<15}")
        print("-" * 100)
        
        last_symbol = None
        last_date = None
        
        for row in recent:
            # 标记重复的记录
            is_duplicate = (row['symbol'] == last_symbol and row['date'] == last_date)
            marker = "⚠️ " if is_duplicate else "   "
            
            print(f"{marker}{row['symbol']:<15} {row['date']} "
                  f"{row['created_at'].strftime('%Y-%m-%d %H:%M:%S'):<20} "
                  f"{row['open_price'] or 0:<10.2f} {row['close_price'] or 0:<10.2f} "
                  f"{row['volume'] or 0:>14,}")
            
            last_symbol = row['symbol']
            last_date = row['date']
    
    # 3. 统计信息
    print("\n📈 步骤 3: 数据库统计")
    print("-" * 80)
    
    cursor.execute("SELECT COUNT(*) as total FROM tw_stock_prices;")
    total = cursor.fetchone()['total']
    print(f"总记录数: {total:,} 条")
    
    cursor.execute("SELECT COUNT(DISTINCT symbol) as total FROM tw_stock_prices;")
    symbols = cursor.fetchone()['total']
    print(f"股票数量: {symbols} 支")
    
    cursor.execute("SELECT COUNT(DISTINCT date) as total FROM tw_stock_prices;")
    dates = cursor.fetchone()['total']
    print(f"日期数量: {dates} 天")
    
    # 4. 检查特定股票
    print("\n🔎 步骤 4: 检查特定股票（6488.TWO）")
    print("-" * 80)
    
    cursor.execute("""
        SELECT date, COUNT(*) as count
        FROM tw_stock_prices
        WHERE symbol = '6488.TWO'
        GROUP BY date
        HAVING COUNT(*) > 1
        ORDER BY date DESC;
    """)
    
    stock_dups = cursor.fetchall()
    
    if stock_dups:
        print(f"❌ 6488.TWO 有 {len(stock_dups)} 个日期重复：")
        for row in stock_dups:
            print(f"  {row['date']}: {row['count']} 条记录")
    else:
        print("✅ 6488.TWO 没有重复记录")
        
        # 显示最近10条
        cursor.execute("""
            SELECT date, open_price, close_price, volume, created_at
            FROM tw_stock_prices
            WHERE symbol = '6488.TWO'
            ORDER BY date DESC
            LIMIT 10;
        """)
        
        stock_data = cursor.fetchall()
        if stock_data:
            print("\n最近 10 条记录：")
            print(f"{'日期':<12} {'开盘':<10} {'收盘':<10} {'成交量':<15} {'创建时间':<20}")
            print("-" * 70)
            for row in stock_data:
                print(f"{row['date']} {row['open_price'] or 0:<10.2f} "
                      f"{row['close_price'] or 0:<10.2f} {row['volume'] or 0:>14,} "
                      f"{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ 检查完成")
    print("=" * 80)
    
except Exception as e:
    print(f"\n❌ 错误: {e}")
    import traceback
    traceback.print_exc()
