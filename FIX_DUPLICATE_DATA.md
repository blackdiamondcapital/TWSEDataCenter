# 🔧 修复数据重复问题指南

## 🎯 问题分析

根据代码检查，系统已经有防止重复的机制：
- ✅ 数据库约束：`UNIQUE(symbol, date)`
- ✅ 插入逻辑：`ON CONFLICT (symbol, date) DO UPDATE`

**如果仍然看到重复数据，可能的原因：**

### 1. 数据库中真的有重复记录
   - 旧数据在约束添加前就存在
   - 约束未正确创建
   - 数据库迁移问题

### 2. 显示重复（实际没重复）
   - 前端查询多次
   - 缓存问题
   - 相同日期不同时间

### 3. 日志显示重复抓取
   - 批量更新时重复抓取相同股票
   - 并发请求导致

---

## 🔍 步骤 1：诊断问题

### 方法 A：使用 SQL 直接查询（推荐）

```sql
-- 在数据库管理工具（如 DBeaver, pgAdmin）中执行

-- 1. 检查是否有重复
SELECT symbol, date, COUNT(*) as count
FROM stock_prices
GROUP BY symbol, date
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 20;
```

**预期结果：**
- 如果返回**空结果** → 没有重复，是显示问题
- 如果返回**有数据** → 确实有重复，需要清理

### 方法 B：使用 Python 脚本

```bash
# 设置数据库连接（如果还没设置）
export DATABASE_URL="你的数据库连接字符串"

# 运行检查脚本
python show_recent_data.py
```

### 方法 C：查看特定股票

```sql
-- 查看某支股票的所有记录
SELECT id, date, created_at, open_price, close_price, volume
FROM stock_prices
WHERE symbol = '6488.TWO'  -- 改成你要检查的股票
ORDER BY date DESC, id DESC
LIMIT 50;
```

**检查要点：**
- 是否有相同的 `date` 出现多次？
- `created_at` 时间是否不同？

---

## 🛠️ 步骤 2：解决方案

### 情况 A：确实有重复记录

#### 解决方案 1：删除重复数据（保留最新的）⭐ 推荐

```sql
-- ⚠️ 警告：执行前请先备份数据库！

-- 1. 备份（可选但强烈建议）
CREATE TABLE stock_prices_backup AS SELECT * FROM stock_prices;

-- 2. 删除重复记录，保留 ID 最大的（通常是最新的）
DELETE FROM stock_prices
WHERE id NOT IN (
    SELECT MAX(id)
    FROM stock_prices
    GROUP BY symbol, date
);

-- 3. 验证结果
SELECT symbol, date, COUNT(*) as count
FROM stock_prices
GROUP BY symbol, date
HAVING COUNT(*) > 1;
-- 应该返回 0 条记录

-- 4. 查看删除了多少条
SELECT 
    (SELECT COUNT(*) FROM stock_prices_backup) as before_count,
    (SELECT COUNT(*) FROM stock_prices) as after_count,
    (SELECT COUNT(*) FROM stock_prices_backup) - 
    (SELECT COUNT(*) FROM stock_prices) as deleted_count;
```

#### 解决方案 2：重建唯一索引

```sql
-- 如果约束没有正确创建

-- 1. 先删除旧索引
DROP INDEX IF EXISTS stock_prices_symbol_date_idx;

-- 2. 重新创建唯一索引
CREATE UNIQUE INDEX stock_prices_symbol_date_idx
ON stock_prices(symbol, date);

-- 如果这一步失败，说明有重复数据，先执行解决方案 1
```

#### 解决方案 3：使用 Python 脚本清理

```python
# clean_duplicates.py
import psycopg2
import os

db_url = os.getenv('DATABASE_URL')
conn = psycopg2.connect(db_url)
cursor = conn.cursor()

# 删除重复，保留最新的
cursor.execute("""
    DELETE FROM stock_prices
    WHERE id NOT IN (
        SELECT MAX(id)
        FROM stock_prices
        GROUP BY symbol, date
    );
""")

affected_rows = cursor.rowcount
conn.commit()

print(f"✅ 删除了 {affected_rows} 条重复记录")

cursor.close()
conn.close()
```

---

### 情况 B：没有重复，但显示重复

#### 可能原因 1：前端多次请求

检查前端代码（如 `index.html` 或 `main.js`）：

```javascript
// 不好的做法：
function updateStock() {
    fetchData();  // 第一次
    fetchData();  // 重复！
}

// 好的做法：
function updateStock() {
    if (isLoading) return;  // 防止重复请求
    isLoading = true;
    fetchData().finally(() => {
        isLoading = false;
    });
}
```

#### 可能原因 2：浏览器缓存

```bash
# 解决方案：强制刷新
# 在浏览器中按：
Cmd + Shift + R  (Mac)
Ctrl + Shift + R (Windows)
```

#### 可能原因 3：日志重复

如果是看到日志中"抓取数据"的重复，这是正常的：

```python
# 批量更新时：
for symbol in symbols:
    fetch_data(symbol)  # 每支股票都会有日志
    
# 看起来像重复，其实是不同股票
INFO: 抓取 6488.TWO 2025-10-18
INFO: 抓取 3529.TWO 2025-10-18  # 不同股票，相同日期
```

---

## 🔧 步骤 3：预防重复

### 1. 确保数据库约束存在

```sql
-- 检查约束
SELECT conname, contype, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'stock_prices'::regclass;

-- 应该看到：
-- stock_prices_symbol_date_key | u | UNIQUE (symbol, date)
```

### 2. 修改插入逻辑（已完成）

代码中已经使用了正确的 UPSERT 逻辑：

```python
INSERT INTO stock_prices (symbol, date, ...)
VALUES %s
ON CONFLICT (symbol, date) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    ...
```

这确保了相同 `(symbol, date)` 会被更新而不是插入新记录。

### 3. 添加日志去重

如果是日志看起来重复，可以添加：

```python
# 在 server.py 中添加
logged_combinations = set()

def log_fetch(symbol, date):
    key = f"{symbol}_{date}"
    if key not in logged_combinations:
        logger.info(f"抓取 {symbol} {date}")
        logged_combinations.add(key)
```

---

## 📊 步骤 4：验证修复

### 1. 检查重复

```sql
SELECT symbol, date, COUNT(*) 
FROM stock_prices 
GROUP BY symbol, date 
HAVING COUNT(*) > 1;
```

**应该返回 0 条记录**

### 2. 检查约束

```sql
\d stock_prices  -- PostgreSQL
-- 应该看到 UNIQUE 约束
```

### 3. 测试插入

```python
# 测试相同数据插入两次
from server import DatabaseManager

db = DatabaseManager()
db.connect()
cursor = db.connection.cursor()

# 第一次插入
cursor.execute("""
    INSERT INTO stock_prices (symbol, date, close_price, volume)
    VALUES ('TEST.TW', '2025-10-18', 100.0, 1000)
    ON CONFLICT (symbol, date) DO UPDATE SET
        close_price = EXCLUDED.close_price;
""")

# 第二次插入（应该更新，不重复）
cursor.execute("""
    INSERT INTO stock_prices (symbol, date, close_price, volume)
    VALUES ('TEST.TW', '2025-10-18', 105.0, 2000)
    ON CONFLICT (symbol, date) DO UPDATE SET
        close_price = EXCLUDED.close_price;
""")

db.connection.commit()

# 检查
cursor.execute("""
    SELECT * FROM stock_prices WHERE symbol = 'TEST.TW';
""")
result = cursor.fetchall()

print(f"记录数: {len(result)}")  # 应该是 1
print(f"收盘价: {result[0]['close_price']}")  # 应该是 105.0
```

---

## ❓ 常见问题

### Q1: 删除重复后，数据还会重复吗？

A: 不会。只要约束存在，`ON CONFLICT` 会自动处理。

### Q2: 如何知道删除了哪些数据？

A: 在删除前先备份：
```sql
CREATE TABLE stock_prices_deleted AS
SELECT * FROM stock_prices
WHERE id NOT IN (
    SELECT MAX(id) FROM stock_prices GROUP BY symbol, date
);
```

### Q3: 误删了怎么办？

A: 如果有备份表：
```sql
INSERT INTO stock_prices 
SELECT * FROM stock_prices_backup
ON CONFLICT (symbol, date) DO NOTHING;
```

### Q4: 为什么会产生重复？

A: 可能原因：
- 旧版本代码没有约束
- 数据库迁移时约束丢失
- 直接使用 SQL 插入绕过了约束

---

## 🚀 快速修复（一键执行）

如果您确认有重复且想快速修复：

```bash
# 1. 连接数据库
psql "你的数据库连接字符串"

# 2. 执行清理
DELETE FROM stock_prices
WHERE id NOT IN (
    SELECT MAX(id)
    FROM stock_prices
    GROUP BY symbol, date
);

# 3. 验证
SELECT COUNT(*) FROM (
    SELECT symbol, date, COUNT(*) as c
    FROM stock_prices
    GROUP BY symbol, date
    HAVING COUNT(*) > 1
) t;
-- 应该返回 0

# 4. 重建索引（如果需要）
DROP INDEX IF EXISTS stock_prices_symbol_date_idx;
CREATE UNIQUE INDEX stock_prices_symbol_date_idx
ON stock_prices(symbol, date);
```

---

## 📞 需要帮助？

1. **查看实际重复数据**：
   ```sql
   SELECT * FROM stock_prices 
   WHERE (symbol, date) IN (
       SELECT symbol, date FROM stock_prices 
       GROUP BY symbol, date HAVING COUNT(*) > 1
   )
   ORDER BY symbol, date, id;
   ```

2. **统计重复数量**：
   ```sql
   SELECT COUNT(*) as duplicate_groups
   FROM (
       SELECT symbol, date
       FROM stock_prices
       GROUP BY symbol, date
       HAVING COUNT(*) > 1
   ) t;
   ```

3. **查看特定日期的所有重复**：
   ```sql
   SELECT * FROM stock_prices
   WHERE date = '2025-10-18'
   ORDER BY symbol, created_at;
   ```

---

**最后更新**: 2025-10-19  
**建议操作**: 先运行诊断，确认问题类型，再执行相应的解决方案
