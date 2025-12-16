import logging
import os
import threading
from collections import defaultdict
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import SimpleConnectionPool

DEFAULTS = {
    "DB_HOST": os.getenv("DB_HOST", "localhost"),
    "DB_PORT": os.getenv("DB_PORT", "5432"),
    "DB_USER": os.getenv("DB_USER", "postgres"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD", "s8304021"),
    "DB_NAME": os.getenv("DB_NAME", "postgres"),
}

# Neon 雲端資料庫配置
NEON_DATABASE_URL = os.environ.get('DATABASE_URL') or os.environ.get('NEON_DATABASE_URL') or 'postgresql://neondb_owner:npg_6vuayEsIl4Qb@ep-wispy-sky-adgltyd1-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require'


logger = logging.getLogger(__name__)

_POOL_LOCK = threading.Lock()
_LOCAL_POOL: SimpleConnectionPool | None = None
_NEON_POOL: SimpleConnectionPool | None = None


def _pool_limits(use_neon: bool) -> tuple[int, int]:
    min_key = 'NEON_POOL_MIN' if use_neon else 'DB_POOL_MIN'
    max_key = 'NEON_POOL_MAX' if use_neon else 'DB_POOL_MAX'
    default_min = os.getenv('DB_POOL_MIN', '1')
    default_max = os.getenv('DB_POOL_MAX', '5')
    min_conn = int(os.getenv(min_key, default_min))
    max_conn = int(os.getenv(max_key, default_max))
    min_conn = max(1, min_conn)
    max_conn = max(min_conn, max_conn)
    return min_conn, max_conn


def _get_pool(use_neon: bool = False) -> SimpleConnectionPool:
    global _LOCAL_POOL, _NEON_POOL
    pool = _NEON_POOL if use_neon else _LOCAL_POOL
    if pool is not None:
        return pool

    with _POOL_LOCK:
        pool = _NEON_POOL if use_neon else _LOCAL_POOL
        if pool is not None:
            return pool

        min_conn, max_conn = _pool_limits(use_neon)

        if use_neon:
            try:
                pool = SimpleConnectionPool(
                    min_conn,
                    max_conn,
                    NEON_DATABASE_URL,
                    cursor_factory=RealDictCursor,
                    sslmode='require',
                )
            except psycopg2.OperationalError as exc:
                if 'channel binding' in str(exc).lower() and 'channel_binding=require' in NEON_DATABASE_URL:
                    safe_url = NEON_DATABASE_URL.replace('channel_binding=require', 'channel_binding=disable')
                    logger.warning("channel_binding=require 不支援，改為 disable")
                    pool = SimpleConnectionPool(
                        min_conn,
                        max_conn,
                        safe_url,
                        cursor_factory=RealDictCursor,
                        sslmode='require',
                    )
                else:
                    raise
            _NEON_POOL = pool
        else:
            pool = SimpleConnectionPool(
                min_conn,
                max_conn,
                host=DEFAULTS["DB_HOST"],
                port=DEFAULTS["DB_PORT"],
                user=DEFAULTS["DB_USER"],
                password=DEFAULTS["DB_PASSWORD"],
                database=DEFAULTS["DB_NAME"],
                cursor_factory=RealDictCursor,
            )
            _LOCAL_POOL = pool

        return pool

def get_conn(use_neon: bool = False):
    """獲取資料庫連接
    
    Args:
        use_neon: True 使用 Neon 雲端資料庫，False 使用本地資料庫
    """
    pool = _get_pool(use_neon=use_neon)
    return pool.getconn()


def release_conn(conn, use_neon: bool = False):
    """將連線歸還連線池"""
    if conn is None:
        return
    pool = _get_pool(use_neon=use_neon)
    try:
        if getattr(conn, 'closed', 0):
            pool.putconn(conn, close=True)
        else:
            pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        finally:
            logger.exception("歸還資料庫連線失敗")


def get_neon_conn():
    """快捷方式：取得 Neon 雲端資料庫連線"""
    return get_conn(use_neon=True)

@contextmanager
def db_cursor(commit: bool = False, use_neon: bool = False):
    """資料庫游標上下文管理器
    
    Args:
        commit: 是否自動提交
        use_neon: 是否使用 Neon 雲端資料庫
    """
    conn = get_conn(use_neon=use_neon)
    cur = conn.cursor()
    try:
        yield cur
        if commit:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        finally:
            release_conn(conn, use_neon=use_neon)

def ensure_tables(use_neon: bool = False):
    """
    Ensure `tw_stock_returns` table exists with required columns.
    This function will also add missing columns if the table already exists.
    
    Args:
        use_neon: 是否使用 Neon 雲端資料庫
    """
    with db_cursor(commit=True, use_neon=use_neon) as cur:
        # Create table if not exists
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tw_stock_returns (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                date DATE NOT NULL,
                daily_return DECIMAL(10,6),
                weekly_return DECIMAL(10,6),
                monthly_return DECIMAL(10,6),
                quarterly_return DECIMAL(10,6),
                yearly_return DECIMAL(10,6),
                cumulative_return DECIMAL(10,6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date)
            );
            """
        )
        # Add columns if missing
        columns = [
            ("daily_return", "DECIMAL(10,6)"),
            ("weekly_return", "DECIMAL(10,6)"),
            ("monthly_return", "DECIMAL(10,6)"),
            ("quarterly_return", "DECIMAL(10,6)"),
            ("yearly_return", "DECIMAL(10,6)"),
            ("cumulative_return", "DECIMAL(10,6)"),
        ]
        for name, typ in columns:
            cur.execute(
                f"ALTER TABLE tw_stock_returns ADD COLUMN IF NOT EXISTS {name} {typ};"
            )


def fetch_symbols(limit: int | None = None, use_neon: bool = False):
    """獲取股票代碼列表
    
    Args:
        limit: 限制數量
        use_neon: 是否使用 Neon 雲端資料庫
    """
    with db_cursor(use_neon=use_neon) as cur:
        sql = "SELECT DISTINCT symbol FROM tw_stock_prices ORDER BY symbol"
        if limit:
            sql += " LIMIT %s"
            cur.execute(sql, [limit])
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        return [r["symbol"] if isinstance(r, dict) else r[0] for r in rows]


def batch_fetch_prices(symbols: list[str], start: str | None, end: str | None, use_neon: bool = False):
    """批次獲取多支股票的股價資料"""
    if not symbols:
        return {}

    params: list = [symbols]
    sql = "SELECT symbol, date, close_price FROM tw_stock_prices WHERE symbol = ANY(%s)"
    if start:
        sql += " AND date >= %s"
        params.append(start)
    if end:
        sql += " AND date <= %s"
        params.append(end)
    sql += " ORDER BY symbol, date ASC"

    result: dict[str, list] = defaultdict(list)
    with db_cursor(use_neon=use_neon) as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            symbol = row["symbol"] if isinstance(row, dict) else row[0]
            result[symbol].append(row)
    return result


def fetch_prices(symbol: str, start: str | None, end: str | None, use_neon: bool = False):
    """獲取股價數據
    
    Args:
        symbol: 股票代碼
        start: 開始日期
        end: 結束日期
        use_neon: 是否使用 Neon 雲端資料庫
    """
    params = [symbol]
    sql = "SELECT date, close_price FROM tw_stock_prices WHERE symbol = %s"
    if start:
        sql += " AND date >= %s"
        params.append(start)
    if end:
        sql += " AND date <= %s"
        params.append(end)
    sql += " ORDER BY date ASC"
    with db_cursor(use_neon=use_neon) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def batch_fetch_existing_return_dates(symbols: list[str], start: str | None, end: str | None, use_neon: bool = False):
    """批次獲取多支股票已存在的報酬率日期"""
    if not symbols:
        return {}

    params: list = [symbols]
    sql = "SELECT symbol, date FROM tw_stock_returns WHERE symbol = ANY(%s)"
    if start:
        sql += " AND date >= %s"
        params.append(start)
    if end:
        sql += " AND date <= %s"
        params.append(end)
    sql += " ORDER BY symbol, date ASC"

    result: dict[str, set] = defaultdict(set)
    with db_cursor(use_neon=use_neon) as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            symbol = row["symbol"] if isinstance(row, dict) else row[0]
            date_value = row["date"] if isinstance(row, dict) else row[1]
            result[symbol].add(date_value)
    return result


def fetch_existing_return_dates(symbol: str, start: str | None, end: str | None, use_neon: bool = False):
    """獲取已存在的報酬率日期
    
    Args:
        symbol: 股票代碼
        start: 開始日期
        end: 結束日期
        use_neon: 是否使用 Neon 雲端資料庫
    """
    params = [symbol]
    sql = "SELECT date FROM tw_stock_returns WHERE symbol = %s"
    if start:
        sql += " AND date >= %s"
        params.append(start)
    if end:
        sql += " AND date <= %s"
        params.append(end)
    sql += " ORDER BY date ASC"
    with db_cursor(use_neon=use_neon) as cur:
        cur.execute(sql, params)
        return {row["date"] if isinstance(row, dict) else row[0] for row in cur.fetchall()}


def upsert_returns(records: list[dict], use_neon: bool = False):
    """將報酬率寫入資料庫
    
    Args:
        records: 報酬率記錄列表
        use_neon: 是否使用 Neon 雲端資料庫
    """
    if not records:
        return 0
    cols = [
        "symbol",
        "date",
        "daily_return",
        "weekly_return",
        "monthly_return",
        "quarterly_return",
        "yearly_return",
        "cumulative_return",
    ]
    values = [
        [
            r.get("symbol"),
            r.get("date"),
            r.get("daily_return"),
            r.get("weekly_return"),
            r.get("monthly_return"),
            r.get("quarterly_return"),
            r.get("yearly_return"),
            r.get("cumulative_return"),
        ]
        for r in records
    ]
    with db_cursor(commit=True, use_neon=use_neon) as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO tw_stock_returns ({', '.join(cols)})
            VALUES %s
            ON CONFLICT (symbol, date) DO UPDATE SET
              daily_return = EXCLUDED.daily_return,
              weekly_return = EXCLUDED.weekly_return,
              monthly_return = EXCLUDED.monthly_return,
              quarterly_return = EXCLUDED.quarterly_return,
              yearly_return = EXCLUDED.yearly_return,
              cumulative_return = EXCLUDED.cumulative_return
            """,
            values,
        )
    return len(values)


def upsert_returns_neon(records: list[dict]):
    """專用於 Neon 雲端資料庫的 upsert 包裝函式"""
    return upsert_returns(records, use_neon=True)
