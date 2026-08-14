"""TTM and cross-period financial-ratio calculations.

Income rows produced by ``income_statement_service`` are single-quarter values.
Cash-flow rows are MOPS year-to-date values and are de-accumulated here before
TTM aggregation.  Callers importing a different income source can explicitly
set ``income_cumulative=True``.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


LEGACY_RATIO_COLS = [
    "assets", "equity", "revenue", "gross_profit", "op_profit", "net_profit",
    "gross_margin", "op_margin", "net_margin", "roa", "roe", "debt_ratio",
    "current_ratio", "quick_ratio",
]

EXTENDED_RATIO_COLS = [
    "operating_cash_flow", "free_cash_flow", "operating_cash_to_net_income",
    "free_cash_flow_margin", "roic", "interest_coverage", "asset_turnover",
    "inventory_turnover", "receivable_turnover", "payable_turnover",
    "inventory_days", "receivable_days", "payable_days",
    "cash_conversion_cycle", "revenue_yoy", "op_profit_yoy", "eps_yoy",
    "revenue_cagr_3y", "eps_cagr_3y", "book_value_per_share",
    "free_cash_flow_per_share", "dupont_net_margin",
    "dupont_asset_turnover", "dupont_equity_multiplier",
]

FINANCIAL_RATIO_COLS = LEGACY_RATIO_COLS + EXTENDED_RATIO_COLS
AMOUNT_RATIO_COLS = {"operating_cash_flow", "free_cash_flow"}

INCOME_FLOW_FIELDS = [
    "Revenue", "GrossProfitFromOperations", "ProfitLossFromOperatingActivities",
    "ProfitLoss", "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossBeforeTax", "OperatingCosts", "FinanceCosts",
    "BasicEarningsLossPerShareTotal",
]
CASH_FLOW_FIELDS = [
    "NetCashFlowsFromUsedInOperatingActivities",
    "AcquisitionOfPropertyPlantAndEquipment",
]

FINANCIAL_BS_FIELDS = {
    "DueFromTheCentralBankAndCallLoansToBanks", "LoansDiscountedNet",
    "DepositsFromBanks", "DueToTheCentralBankAndBanks", "DepositsFromCustomers",
    "InsuranceContractAndReinsuranceAssetsNet",
    "InsuranceContractAndReinsuranceContractLiabilities",
}
FINANCIAL_NAME_WORDS = ("金融", "金控", "銀行", "保險", "證券", "票券", "bank", "insurance")
EPS_FIELDS = ("BasicEarningsLossPerShareTotal", "BasicEarningsLossPerShare")
BPS_FIELDS = (
    "BookValuePerShare", "BookValuePerShareOfCommonStock",
    "NetAssetValuePerShare", "NetWorthPerShare",
)
EPSILON = 1e-12


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def safe_div(numerator: Any, denominator: Any) -> float | None:
    n, d = _number(numerator), _number(denominator)
    if n is None or d is None or abs(d) <= EPSILON:
        return None
    return n / d


def parse_period(period: Any) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})(?:Q|q)?0?([1-4])", str(period).strip())
    if not match:
        raise ValueError(f"invalid quarterly period: {period!r}")
    return int(match.group(1)), int(match.group(2))


def period_ordinal(period: Any) -> int:
    year, quarter = parse_period(period)
    return year * 4 + quarter - 1


def format_period(ordinal: int) -> str:
    return f"{ordinal // 4}{ordinal % 4 + 1:02d}"


def prior_period(period: Any, quarters: int) -> str:
    return format_period(period_ordinal(period) - quarters)


def required_periods(period: Any, count: int = 16) -> list[str]:
    end = period_ordinal(period)
    return [format_period(value) for value in range(end - count + 1, end + 1)]


def _rows_by_period(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        try:
            label = format_period(period_ordinal(row.get("period")))
        except (TypeError, ValueError):
            continue
        output[label] = dict(row)
    return output


def quarterly_flows(
    rows: Iterable[Mapping[str, Any]],
    fields: Sequence[str],
    *,
    cumulative: bool,
) -> dict[str, dict[str, float | None]]:
    """Normalize rows to single-quarter flows without guessing their basis."""
    source = _rows_by_period(rows)
    result: dict[str, dict[str, float | None]] = {}
    for period in sorted(source, key=period_ordinal):
        year, quarter = parse_period(period)
        current = source[period]
        previous = source.get(f"{year}{quarter - 1:02d}") if quarter > 1 else None
        normalized: dict[str, float | None] = {}
        for field in fields:
            value = _number(current.get(field))
            if not cumulative or quarter == 1:
                normalized[field] = value
            else:
                previous_value = _number(previous.get(field)) if previous else None
                normalized[field] = (
                    value - previous_value
                    if value is not None and previous_value is not None
                    else None
                )
        result[period] = normalized
    return result


def quarterly_income_flows(
    rows: Iterable[Mapping[str, Any]],
    fields: Sequence[str],
    *,
    cumulative: bool = False,
) -> dict[str, dict[str, float | None]]:
    """Normalize MOPS income rows to standalone quarters.

    The current ingestion stores Q1-Q3 as standalone quarters, while Q4 is the
    full-year amount. Alternate cumulative sources can opt into ordinary YTD
    de-accumulation.
    """
    if cumulative:
        return quarterly_flows(rows, fields, cumulative=True)

    source = _rows_by_period(rows)
    result: dict[str, dict[str, float | None]] = {}
    for period in sorted(source, key=period_ordinal):
        year, quarter = parse_period(period)
        current = source[period]
        normalized: dict[str, float | None] = {}
        for field in fields:
            value = _number(current.get(field))
            if quarter != 4:
                normalized[field] = value
                continue
            prior_values = [
                _number(source.get(f"{year}{prior_quarter:02d}", {}).get(field))
                for prior_quarter in (1, 2, 3)
            ]
            normalized[field] = (
                value - sum(prior_values)
                if value is not None and all(item is not None for item in prior_values)
                else None
            )
        result[period] = normalized
    return result


def _ttm(
    quarterly: Mapping[str, Mapping[str, float | None]],
    period: str,
    field: str,
) -> float | None:
    periods = [prior_period(period, offset) for offset in range(3, -1, -1)]
    values = [_number(quarterly.get(label, {}).get(field)) for label in periods]
    return sum(values) if all(value is not None for value in values) else None


def _sum_fields(row: Mapping[str, Any] | None, fields: Sequence[str]) -> float | None:
    if not row:
        return None
    values = [_number(row.get(field)) for field in fields]
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _average(start: Any, end: Any) -> float | None:
    left, right = _number(start), _number(end)
    return (left + right) / 2 if left is not None and right is not None else None


def _growth(current: Any, previous: Any) -> float | None:
    current_value, previous_value = _number(current), _number(previous)
    if current_value is None or previous_value is None or abs(previous_value) <= EPSILON:
        return None
    return (current_value - previous_value) / abs(previous_value)


def _cagr(current: Any, previous: Any, years: float = 3.0) -> float | None:
    current_value, previous_value = _number(current), _number(previous)
    if (
        current_value is None or previous_value is None
        or current_value <= 0 or previous_value <= 0
    ):
        return None
    return (current_value / previous_value) ** (1 / years) - 1


def is_financial_company(
    metadata: Mapping[str, Any] | None,
    balance_rows: Iterable[Mapping[str, Any]],
) -> bool:
    metadata_text = " ".join(
        str(value or "") for key, value in (metadata or {}).items()
        if key.lower() in {"name", "short_name", "industry", "industry_name", "category"}
    ).lower()
    if any(word in metadata_text for word in FINANCIAL_NAME_WORDS):
        return True
    return any(
        _number(row.get(field)) is not None
        for row in balance_rows
        for field in FINANCIAL_BS_FIELDS
    )


def compute_financial_ratios(
    period: Any,
    income_rows: Iterable[Mapping[str, Any]],
    balance_rows: Iterable[Mapping[str, Any]],
    cash_flow_rows: Iterable[Mapping[str, Any]],
    metadata: Mapping[str, Any] | None = None,
    *,
    income_cumulative: bool = False,
) -> dict[str, float | None]:
    """Compute one period's complete ratio contract from at least 16 quarters."""
    end_period = format_period(period_ordinal(period))
    income_list = list(income_rows)
    balance_list = list(balance_rows)
    cash_list = list(cash_flow_rows)
    income = quarterly_income_flows(
        income_list, INCOME_FLOW_FIELDS, cumulative=income_cumulative
    )
    cash = quarterly_flows(cash_list, CASH_FLOW_FIELDS, cumulative=True)
    balance = _rows_by_period(balance_list)
    end_bs = balance.get(end_period)
    start_bs = balance.get(prior_period(end_period, 4))
    if not end_bs or end_period not in income:
        raise ValueError("missing current income statement or balance sheet")

    def ttm(field: str, at: str = end_period) -> float | None:
        return _ttm(income, at, field)

    revenue = ttm("Revenue")
    gross_profit = ttm("GrossProfitFromOperations")
    op_profit = ttm("ProfitLossFromOperatingActivities")
    net_profit = ttm("ProfitLoss")
    parent_net_profit = ttm("ProfitLossAttributableToOwnersOfParent")
    if parent_net_profit is None:
        parent_net_profit = net_profit
    pretax = ttm("ProfitLossBeforeTax")
    operating_costs = ttm("OperatingCosts")
    finance_costs = ttm("FinanceCosts")
    eps = ttm("BasicEarningsLossPerShareTotal")

    cfo = _ttm(cash, end_period, "NetCashFlowsFromUsedInOperatingActivities")
    capex = _ttm(cash, end_period, "AcquisitionOfPropertyPlantAndEquipment")
    free_cash_flow = cfo - abs(capex) if cfo is not None and capex is not None else None

    assets = _number(end_bs.get("Assets"))
    liabilities = _number(end_bs.get("Liabilities"))
    equity = _number(end_bs.get("EquityAttributableToOwnersOfParent"))
    if equity is None:
        equity = _number(end_bs.get("Equity"))
    avg_assets = _average(
        start_bs.get("Assets") if start_bs else None, assets
    )
    start_equity = None
    if start_bs:
        start_equity = _number(start_bs.get("EquityAttributableToOwnersOfParent"))
        if start_equity is None:
            start_equity = _number(start_bs.get("Equity"))
    avg_equity = _average(start_equity, equity)

    inventory_end = _number(end_bs.get("Inventories"))
    receivable_end = _sum_fields(
        end_bs,
        ("AccountsReceivableNet", "AccountsReceivableDueFromRelatedPartiesNet"),
    )
    payable_end = _sum_fields(
        end_bs,
        ("TradeAndOtherCurrentPayablesToTradeSuppliers",
         "TradeAndOtherCurrentPayablesToRelatedParties"),
    )
    avg_inventory = _average(
        start_bs.get("Inventories") if start_bs else None, inventory_end
    )
    avg_receivable = _average(
        _sum_fields(
            start_bs,
            ("AccountsReceivableNet", "AccountsReceivableDueFromRelatedPartiesNet"),
        ),
        receivable_end,
    )
    avg_payable = _average(
        _sum_fields(
            start_bs,
            ("TradeAndOtherCurrentPayablesToTradeSuppliers",
             "TradeAndOtherCurrentPayablesToRelatedParties"),
        ),
        payable_end,
    )

    inventory_turnover = safe_div(abs(operating_costs), avg_inventory) if operating_costs is not None else None
    receivable_turnover = safe_div(revenue, avg_receivable)
    payable_turnover = safe_div(abs(operating_costs), avg_payable) if operating_costs is not None else None
    inventory_days = safe_div(365, inventory_turnover)
    receivable_days = safe_div(365, receivable_turnover)
    payable_days = safe_div(365, payable_turnover)
    cash_conversion_cycle = (
        inventory_days + receivable_days - payable_days
        if all(value is not None for value in (inventory_days, receivable_days, payable_days))
        else None
    )

    tax_rate = None
    if pretax is not None and net_profit is not None and abs(pretax) > EPSILON:
        candidate = (pretax - net_profit) / abs(pretax)
        if math.isfinite(candidate):
            tax_rate = min(0.35, max(0.0, candidate))
    if tax_rate is None:
        tax_rate = 0.20

    short_debt_fields = (
        "ShorttermBorrowings",
        "CurrentCommercialPapersIssuedAndCurrentPortionOfNoncurrentCommercialPapersIssued",
        "LongtermLiabilitiesCurrentPortion",
    )
    long_debt_fields = ("LongtermBorrowings", "NoncurrentPortionOfNoncurrentBondsIssued")

    def invested_capital(row: Mapping[str, Any] | None) -> float | None:
        if not row:
            return None
        parent_equity = _number(row.get("EquityAttributableToOwnersOfParent"))
        if parent_equity is None:
            parent_equity = _number(row.get("Equity"))
        short_debt = _sum_fields(row, short_debt_fields)
        long_debt = _sum_fields(row, long_debt_fields)
        cash_value = _number(row.get("CashAndCashEquivalents"))
        if parent_equity is None or cash_value is None:
            return None
        return parent_equity + (short_debt or 0.0) + (long_debt or 0.0) - cash_value

    avg_invested_capital = _average(invested_capital(start_bs), invested_capital(end_bs))
    roic = safe_div(
        op_profit * (1 - tax_rate) if op_profit is not None else None,
        avg_invested_capital,
    )
    interest_coverage = safe_div(
        op_profit, abs(finance_costs) if finance_costs is not None else None
    )

    direct_bps = next(
        (_number(end_bs.get(field)) for field in BPS_FIELDS if _number(end_bs.get(field)) is not None),
        None,
    )
    shares = safe_div(parent_net_profit, eps)
    book_value_per_share = direct_bps if direct_bps is not None else safe_div(equity, shares)
    free_cash_flow_per_share = safe_div(free_cash_flow, shares)

    quick_assets = _sum_fields(
        end_bs,
        (
            "CashAndCashEquivalents", "AccountsReceivableNet",
            "OtherCurrentReceivables", "CurrentFinancialAssetsAtAmortisedCost",
            "CurrentFinancialAssetsAtFairValueThroughProfitOrLoss",
            "CurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome",
            "OtherCurrentFinancialAssets",
        ),
    )
    current_assets = _number(end_bs.get("CurrentAssets"))
    current_liabilities = _number(end_bs.get("CurrentLiabilities"))
    avg_asset_turnover = safe_div(revenue, avg_assets)
    dupont_net_margin = safe_div(parent_net_profit, revenue)
    dupont_equity_multiplier = safe_div(avg_assets, avg_equity)

    result = {
        "assets": assets,
        "equity": equity,
        "revenue": revenue,
        "gross_profit": gross_profit,
        "op_profit": op_profit,
        "net_profit": net_profit,
        "gross_margin": safe_div(gross_profit, revenue),
        "op_margin": safe_div(op_profit, revenue),
        "net_margin": safe_div(net_profit, revenue),
        "roa": safe_div(net_profit, avg_assets),
        "roe": safe_div(parent_net_profit, avg_equity),
        "debt_ratio": safe_div(liabilities, assets),
        "current_ratio": safe_div(current_assets, current_liabilities),
        "quick_ratio": safe_div(quick_assets, current_liabilities),
        "operating_cash_flow": cfo,
        "free_cash_flow": free_cash_flow,
        "operating_cash_to_net_income": safe_div(cfo, parent_net_profit),
        "free_cash_flow_margin": safe_div(free_cash_flow, revenue),
        "roic": roic,
        "interest_coverage": interest_coverage,
        "asset_turnover": avg_asset_turnover,
        "inventory_turnover": inventory_turnover,
        "receivable_turnover": receivable_turnover,
        "payable_turnover": payable_turnover,
        "inventory_days": inventory_days,
        "receivable_days": receivable_days,
        "payable_days": payable_days,
        "cash_conversion_cycle": cash_conversion_cycle,
        "revenue_yoy": _growth(revenue, ttm("Revenue", prior_period(end_period, 4))),
        "op_profit_yoy": _growth(op_profit, ttm("ProfitLossFromOperatingActivities", prior_period(end_period, 4))),
        "eps_yoy": _growth(eps, ttm("BasicEarningsLossPerShareTotal", prior_period(end_period, 4))),
        "revenue_cagr_3y": _cagr(revenue, ttm("Revenue", prior_period(end_period, 12))),
        "eps_cagr_3y": _cagr(eps, ttm("BasicEarningsLossPerShareTotal", prior_period(end_period, 12))),
        "book_value_per_share": book_value_per_share,
        "free_cash_flow_per_share": free_cash_flow_per_share,
        "dupont_net_margin": dupont_net_margin,
        "dupont_asset_turnover": avg_asset_turnover,
        "dupont_equity_multiplier": dupont_equity_multiplier,
    }

    if is_financial_company(metadata, balance_list):
        for field in (
            "roic", "asset_turnover", "inventory_turnover", "receivable_turnover",
            "payable_turnover", "inventory_days", "receivable_days", "payable_days",
            "cash_conversion_cycle", "dupont_asset_turnover",
            "dupont_equity_multiplier",
        ):
            result[field] = None
    return {field: result.get(field) for field in FINANCIAL_RATIO_COLS}


def _validated_table(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", name):
        raise ValueError(f"unsafe table name: {name!r}")
    return ".".join(f'"{part}"' for part in name.split("."))


def _fetch_dicts(connection: Any, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_symbol_codes(
    connection: Any,
    *,
    period: Any,
    income_table: str = "tw_income_statement",
    code_from: str | None = None,
    code_to: str | None = None,
) -> list[str]:
    clauses = ["period = %s"]
    params: list[Any] = [format_period(period_ordinal(period))]
    if code_from:
        clauses.append('"股票代號" >= %s')
        params.append(code_from)
    if code_to:
        clauses.append('"股票代號" <= %s')
        params.append(code_to)
    rows = _fetch_dicts(
        connection,
        f'SELECT DISTINCT "股票代號" FROM {_validated_table(income_table)} '
        f'WHERE {" AND ".join(clauses)} ORDER BY "股票代號"',
        params,
    )
    return [str(row["股票代號"]).strip() for row in rows]


def compute_records_from_connection(
    connection: Any,
    *,
    period: Any,
    codes: Sequence[str],
    income_table: str = "tw_income_statement",
    balance_table: str = "tw_balance_sheet",
    cash_flow_table: str = "tw_cash_flow_statement",
    symbols_table: str = "tw_stock_symbols",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Load 16 quarters for a code batch and return upsert-ready records."""
    if not codes:
        return [], {"success": 0, "missing": 0, "not_applicable": 0, "errors": 0}
    periods = required_periods(period, 16)
    params = [list(codes), periods]

    def statement_rows(table: str) -> list[dict[str, Any]]:
        return _fetch_dicts(
            connection,
            f'SELECT * FROM {_validated_table(table)} '
            'WHERE "股票代號" = ANY(%s) AND period = ANY(%s)',
            params,
        )

    income_rows = statement_rows(income_table)
    balance_rows = statement_rows(balance_table)
    cash_rows = statement_rows(cash_flow_table)
    try:
        metadata_rows = _fetch_dicts(
            connection,
            f"SELECT * FROM {_validated_table(symbols_table)} "
            "WHERE REPLACE(REPLACE(symbol, '.TWO', ''), '.TW', '') = ANY(%s)",
            [list(codes)],
        )
    except Exception:
        connection.rollback()
        metadata_rows = []

    def group(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            code = str(row.get("股票代號") or "").strip()
            grouped.setdefault(code, []).append(dict(row))
        return grouped

    income_by_code, balance_by_code, cash_by_code = (
        group(income_rows), group(balance_rows), group(cash_rows)
    )
    metadata_by_code = {
        str(row.get("symbol") or "").replace(".TWO", "").replace(".TW", ""): row
        for row in metadata_rows
    }
    stats = {"success": 0, "missing": 0, "not_applicable": 0, "errors": 0}
    records: list[dict[str, Any]] = []
    for code in codes:
        metadata = metadata_by_code.get(code, {})
        financial = is_financial_company(metadata, balance_by_code.get(code, []))
        try:
            ratios = compute_financial_ratios(
                period,
                income_by_code.get(code, []),
                balance_by_code.get(code, []),
                cash_by_code.get(code, []),
                metadata,
            )
            raw_symbol = str(metadata.get("symbol") or "")
            market = str(metadata.get("market") or "").strip().lower()
            suffix = ".TWO" if raw_symbol.endswith(".TWO") or market in {"otc", "tpex"} else ".TW"
            records.append({
                "symbol": f"{code}{suffix}",
                "period": format_period(period_ordinal(period)),
                **ratios,
            })
            stats["success"] += 1
            if financial:
                stats["not_applicable"] += 1
        except ValueError:
            stats["missing"] += 1
        except Exception:
            stats["errors"] += 1
    return records, stats
