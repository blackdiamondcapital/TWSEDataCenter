#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键清理数据库重复数据
自动备份 → 检查 → 清理 → 验证
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime

print("🔧 数据库重复数据清理工具")
print("=" * 80)

# 从环境变量获取数据库连接
db_url = os.getenv('DATABASE_URL')

if not db_url:
    print("❌ 未设置 DATABASE_URL 环境变量")
    print("\n方法 1: 在终端设置")
    print("  export DATABASE_URL='postgresql://user:pass@host:port/dbname'")
    print("\n方法 2: 在代码中设置")
    print("  修改此脚本，在开头添加:")
    print("  db_url = 'postgresql://...'")
    exit(1)

try:
    conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    print("✅ 数据库连接成功\n")
    
    # 步骤 1: 检查重复
    print("📊 步骤 1/5: 检查重复数据")
    print("-" * 80)
    
    cursor.execute("""
        SELECT symbol, date, COUNT(*) as count
        FROM stock_prices
        GROUP BY symbol, date
        HAVING COUNT(*) > 1
        ORDER BY count DESC;
    """)
    
    duplicates = cursor.fetchall()
    
    if not duplicates:
        print("✅ 没有发现重复数据！数据库状态良好。")
        cursor.close()
        conn.close()
        exit(0)
    
    print(f"⚠️ 发现 {len(duplicates)} 组重复数据")
    
    # 计算总共有多少条重复记录
    total_duplicates = sum(row['count'] - 1 for row in duplicates)
    print(f"⚠️ 需要删除 {total_duplicates} 条重复记录\n")
    
    # 显示前10组
    print("前 10 组重复数据：")
    print(f"{'股票代码':<15} {'日期':<12} {'重复次数':<8}")
    print("-" * 40)
    for i, row in enumerate(duplicates[:10]):
        print(f"{row['symbol']:<15} {row['date']} {row['count']:<8}")
    
    if len(duplicates) > 10:
        print(f"... 还有 {len(duplicates) - 10} 组\n")
    
    # 步骤 2: 用户确认
    print("\n⚠️ 步骤 2/5: 确认操作")
    print("-" * 80)
    print("即将执行以下操作：")
    print(f"  1. 备份当前数据到 stock_prices_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    print(f"  2. 删除 {total_duplicates} 条重复记录（保留最新的）")
    print("  3. 验证清理结果")
    print("\n")
    
    confirm = input("确认执行？输入 'yes' 继续，其他键取消: ").strip().lower()
    
    if confirm != 'yes':
        print("\n❌ 操作已取消")
        cursor.close()
        conn.close()
        exit(0)
    
    # 步骤 3: 备份
    print("\n💾 步骤 3/5: 备份数据")
    print("-" * 80)
    
    backup_table_name = f"stock_prices_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        cursor.execute(f"""
            CREATE TABLE {backup_table_name} AS 
            SELECT * FROM stock_prices;
        """)
        conn.commit()
        
        cursor.execute(f"SELECT COUNT(*) as count FROM {backup_table_name};")
        backup_count = cursor.fetchone()['count']
        print(f"✅ 备份成功: {backup_table_name} ({backup_count:,} 条记录)\n")
    except Exception as e:
        print(f"❌ 备份失败: {e}")
        print("为安全起见，操作终止")
        conn.rollback()
        cursor.close()
        conn.close()
        exit(1)
    
    # 步骤 4: 删除重复
    print("🗑️ 步骤 4/5: 删除重复记录")
    print("-" * 80)
    
    try:
        # 删除重复，保留 ID 最大的（通常是最新创建的）
        cursor.execute("""
            DELETE FROM stock_prices
            WHERE id NOT IN (
                SELECT MAX(id)
                FROM stock_prices
                GROUP BY symbol, date
            );
        """)
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"✅ 成功删除 {deleted_count:,} 条重复记录\n")
        
    except Exception as e:
        print(f"❌ 删除失败: {e}")
        print("正在回滚...")
        conn.rollback()
        
        # 尝试从备份恢复
        print("\n尝试从备份恢复...")
        try:
            cursor.execute("DELETE FROM stock_prices;")
            cursor.execute(f"INSERT INTO stock_prices SELECT * FROM {backup_table_name};")
            conn.commit()
            print("✅ 已从备份恢复")
        except Exception as e2:
            print(f"❌ 恢复失败: {e2}")
            print(f"⚠️ 请手动从 {backup_table_name} 恢复数据")
        
        cursor.close()
        conn.close()
        exit(1)
    
    # 步骤 5: 验证
    print("✔️ 步骤 5/5: 验证结果")
    print("-" * 80)
    
    # 检查是否还有重复
    cursor.execute("""
        SELECT symbol, date, COUNT(*) as count
        FROM stock_prices
        GROUP BY symbol, date
        HAVING COUNT(*) > 1;
    """)
    
    remaining_duplicates = cursor.fetchall()
    
    if remaining_duplicates:
        print(f"⚠️ 仍有 {len(remaining_duplicates)} 组重复数据未清理")
        print("可能需要手动处理或再次运行此脚本")
    else:
        print("✅ 验证通过：没有重复数据了！")
    
    # 统计
    cursor.execute("SELECT COUNT(*) as count FROM stock_prices;")
    final_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(DISTINCT symbol) as count FROM stock_prices;")
    symbol_count = cursor.fetchone()['count']
    
    print(f"\n📊 清理后的数据：")
    print(f"  - 总记录数: {final_count:,} 条")
    print(f"  - 股票数量: {symbol_count} 支")
    print(f"  - 删除记录: {deleted_count:,} 条")
    print(f"  - 备份位置: {backup_table_name}")
    
    # 重建索引（可选）
    print("\n🔨 重建唯一索引...")
    try:
        cursor.execute("DROP INDEX IF EXISTS stock_prices_symbol_date_idx;")
        cursor.execute("""
            CREATE UNIQUE INDEX stock_prices_symbol_date_idx
            ON stock_prices(symbol, date);
        """)
        conn.commit()
        print("✅ 唯一索引已重建")
    except Exception as e:
        print(f"⚠️ 索引重建失败: {e}")
        print("   不影响数据完整性，可以手动重建")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("🎉 清理完成！")
    print("=" * 80)
    print("\n💡 后续建议：")
    print("  1. 验证数据正确性：python show_recent_data.py")
    print("  2. 测试前端功能：更新一支股票试试")
    print("  3. 如果需要恢复：")
    print(f"     INSERT INTO stock_prices SELECT * FROM {backup_table_name};")
    print("=" * 80)
    
except psycopg2.Error as e:
    print(f"\n❌ 数据库错误: {e}")
    print("请检查数据库连接字符串是否正确")
except Exception as e:
    print(f"\n❌ 未知错误: {e}")
    import traceback
    traceback.print_exc()
