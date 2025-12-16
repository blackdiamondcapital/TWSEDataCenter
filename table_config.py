import os
from typing import Optional


NEON_HOST_KEYWORDS = ("neon.tech", "neon.build")


def _env_provider_is_neon() -> bool:
    provider = os.getenv("DB_PROVIDER", "").strip().lower()
    return provider == "neon"


def _url_is_neon(url: Optional[str]) -> bool:
    if not url:
        return False
    url = url.lower()
    return any(keyword in url for keyword in NEON_HOST_KEYWORDS)


def resolve_use_neon(
    *,
    use_neon: Optional[bool] = None,
    use_local: Optional[bool] = None,
    db_url: Optional[str] = None,
) -> bool:
    """Resolve whether the current context should be treated as Neon.

    Priority order:
    1. Explicit ``use_neon`` flag
    2. Explicit ``use_local`` flag (negated)
    3. ``DB_PROVIDER=neon``
    4. Presence of Neon host keywords in database URL env vars
    """

    if use_neon is not None:
        return use_neon
    if use_local is not None:
        return not bool(use_local)

    if _env_provider_is_neon():
        return True

    if db_url:
        if _url_is_neon(db_url):
            return True

    env_url = (
        os.getenv("DATABASE_URL")
        or os.getenv("NEON_DATABASE_URL")
        or os.getenv("SUPABASE_DB_URL")
    )
    return _url_is_neon(env_url)


def stock_prices_table(*, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None) -> str:
    return "stock_prices" if resolve_use_neon(use_neon=use_neon, use_local=use_local, db_url=db_url) else "tw_stock_prices"


def stock_returns_table(*, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None) -> str:
    return "stock_returns" if resolve_use_neon(use_neon=use_neon, use_local=use_local, db_url=db_url) else "tw_stock_returns"


def institutional_trades_table(
    *, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None
) -> str:
    return "tw_institutional_tradea"


def margin_trades_table(
    *, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None
) -> str:
    """Return table name for TW margin trading data.

    Neon 環境使用簡短名稱 ``margin_trades``，本地則使用 ``tw_margin_trades``。
    """
    return "margin_trades" if resolve_use_neon(use_neon=use_neon, use_local=use_local, db_url=db_url) else "tw_margin_trades"


def monthly_revenue_table(
    *, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None
) -> str:
    """Return table name for monthly revenue data.

    Neon 環境使用簡短名稱 ``stock_monthly_revenue``，本地則使用 ``tw_stock_monthly_revenue``。
    """
    return "stock_monthly_revenue" if resolve_use_neon(use_neon=use_neon, use_local=use_local, db_url=db_url) else "tw_stock_monthly_revenue"


def income_statement_table(
    *, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None
) -> str:
    """Return table name for income statement data.

    目前統一使用 ``tw_income_statements``，不區分 Neon 與本地，方便共用相同 schema。
    """
    return "tw_income_statements"


def balance_sheet_table(
    *, use_neon: Optional[bool] = None, use_local: Optional[bool] = None, db_url: Optional[str] = None
) -> str:
    """Return table name for balance sheet data.

    目前統一使用 ``tw_balance_sheets``，不區分 Neon 與本地，方便共用相同 schema。
    """
    return "tw_balance_sheets"
