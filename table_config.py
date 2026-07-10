import os
from typing import Optional


def resolve_use_neon(*, use_local: bool = False, db_url: Optional[str] = None) -> bool:
    if use_local:
        return False
    if db_url is None:
        db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    return bool(db_url)


def _table_env(local_key: str, neon_key: str, default: str, use_neon: bool) -> str:
    if use_neon:
        return os.environ.get(neon_key) or os.environ.get(local_key) or default
    return os.environ.get(local_key) or default


def stock_prices_table(*, use_neon: bool = False) -> str:
    return _table_env("STOCK_PRICES_TABLE", "NEON_STOCK_PRICES_TABLE", "tw_stock_prices", use_neon)


def stock_returns_table(*, use_neon: bool = False) -> str:
    return _table_env("STOCK_RETURNS_TABLE", "NEON_STOCK_RETURNS_TABLE", "tw_stock_returns", use_neon)


def institutional_trades_table(*, use_neon: bool = False) -> str:
    return _table_env(
        "INSTITUTIONAL_TRADES_TABLE",
        "NEON_INSTITUTIONAL_TRADES_TABLE",
        "tw_institutional_trades",
        use_neon,
    )


def margin_trades_table(*, use_neon: bool = False) -> str:
    return _table_env("MARGIN_TRADES_TABLE", "NEON_MARGIN_TRADES_TABLE", "tw_margin_trades", use_neon)


def monthly_revenue_table(*, use_neon: bool = False) -> str:
    return _table_env("MONTHLY_REVENUE_TABLE", "NEON_MONTHLY_REVENUE_TABLE", "tw_monthly_revenue", use_neon)


def income_statement_table(*, use_neon: bool = False) -> str:
    return _table_env("INCOME_STATEMENT_TABLE", "NEON_INCOME_STATEMENT_TABLE", "tw_income_statement", use_neon)


def balance_sheet_table(*, use_neon: bool = False) -> str:
    return _table_env("BALANCE_SHEET_TABLE", "NEON_BALANCE_SHEET_TABLE", "tw_balance_sheet", use_neon)


def cash_flow_table(*, use_neon: bool = False) -> str:
    return _table_env(
        "CASH_FLOW_TABLE",
        "NEON_CASH_FLOW_TABLE",
        "tw_cash_flow_statement",
        use_neon,
    )


def financial_ratios_table(*, use_neon: bool = False) -> str:
    return _table_env(
        "FINANCIAL_RATIOS_TABLE",
        "NEON_FINANCIAL_RATIOS_TABLE",
        "tw_financial_ratios",
        use_neon,
    )
