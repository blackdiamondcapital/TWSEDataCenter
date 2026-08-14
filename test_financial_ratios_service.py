import math

import pytest

from financial_ratios_service import (
    compute_financial_ratios,
    quarterly_flows,
    quarterly_income_flows,
    required_periods,
)


def _history(*, financial=False, negative_base=False):
    income, balance, cash = [], [], []
    annual_revenue = {2021: 400.0, 2022: 500.0, 2023: 600.0, 2024: 800.0}
    for index, period in enumerate(required_periods("202404", 16)):
        year, quarter = int(period[:4]), int(period[-2:])
        standalone_revenue = annual_revenue[year] / 4
        revenue = annual_revenue[year] if quarter == 4 else standalone_revenue
        standalone_eps = 1.0 if year != 2021 or not negative_base else -1.0
        eps = standalone_eps * 4 if quarter == 4 else standalone_eps
        finance_costs = -20.0 if quarter == 4 else -5.0
        income.append(
            {
                "period": period,
                "Revenue": revenue,
                "GrossProfitFromOperations": revenue * 0.4,
                "ProfitLossFromOperatingActivities": revenue * 0.2,
                "ProfitLoss": revenue * 0.12,
                "ProfitLossAttributableToOwnersOfParent": revenue * 0.1,
                "ProfitLossBeforeTax": revenue * 0.15,
                "OperatingCosts": -revenue * 0.6,
                "FinanceCosts": finance_costs,
                "BasicEarningsLossPerShareTotal": eps,
            }
        )
        assets = 900 + index * 10
        row = {
            "period": period,
            "Assets": assets,
            "Liabilities": 400,
            "Equity": 500,
            "EquityAttributableToOwnersOfParent": 500,
            "CurrentAssets": 300,
            "CurrentLiabilities": 150,
            "CashAndCashEquivalents": 50,
            "Inventories": 60 + index,
            "AccountsReceivableNet": 80 + index,
            "TradeAndOtherCurrentPayablesToTradeSuppliers": 40 + index,
            "ShorttermBorrowings": 30,
            "LongtermBorrowings": 70,
        }
        if financial:
            row["DepositsFromCustomers"] = 1000
        balance.append(row)

        single_cfo = 10.0 * quarter
        single_capex = -1.0 * quarter
        cash.append(
            {
                "period": period,
                "NetCashFlowsFromUsedInOperatingActivities": 10.0 * sum(range(1, quarter + 1)),
                "AcquisitionOfPropertyPlantAndEquipment": -1.0 * sum(range(1, quarter + 1)),
            }
        )
    return income, balance, cash


def test_cash_flow_ytd_is_restored_to_single_quarters_and_q4():
    _, _, cash = _history()
    quarterly = quarterly_flows(
        cash,
        [
            "NetCashFlowsFromUsedInOperatingActivities",
            "AcquisitionOfPropertyPlantAndEquipment",
        ],
        cumulative=True,
    )
    assert quarterly["202404"]["NetCashFlowsFromUsedInOperatingActivities"] == 40
    assert quarterly["202404"]["AcquisitionOfPropertyPlantAndEquipment"] == -4


def test_income_defaults_to_existing_single_quarter_ingestion():
    rows = [
        {"period": "202401", "Revenue": 100},
        {"period": "202402", "Revenue": 120},
    ]
    single = quarterly_flows(rows, ["Revenue"], cumulative=False)
    cumulative = quarterly_flows(rows, ["Revenue"], cumulative=True)
    assert single["202402"]["Revenue"] == 120
    assert cumulative["202402"]["Revenue"] == 20


def test_income_q4_full_year_is_restored_to_standalone_quarter():
    rows = [
        {"period": "202401", "Revenue": 100},
        {"period": "202402", "Revenue": 120},
        {"period": "202403", "Revenue": 130},
        {"period": "202404", "Revenue": 500},
    ]
    quarterly = quarterly_income_flows(rows, ["Revenue"])
    assert quarterly["202404"]["Revenue"] == 150


def test_core_ttm_average_balance_and_dupont_identity():
    income, balance, cash = _history()
    result = compute_financial_ratios("202404", income, balance, cash)

    assert result["revenue"] == 800
    assert result["operating_cash_flow"] == 100
    assert result["free_cash_flow"] == 90
    assert result["free_cash_flow_margin"] == pytest.approx(90 / 800)
    assert result["asset_turnover"] == pytest.approx(800 / ((1010 + 1050) / 2))
    assert result["revenue_yoy"] == pytest.approx((800 - 600) / 600)
    assert result["revenue_cagr_3y"] == pytest.approx((800 / 400) ** (1 / 3) - 1)
    dupont_roe = (
        result["dupont_net_margin"]
        * result["dupont_asset_turnover"]
        * result["dupont_equity_multiplier"]
    )
    assert dupont_roe == pytest.approx(result["roe"])
    assert result["inventory_days"] == pytest.approx(365 / result["inventory_turnover"])
    assert result["cash_conversion_cycle"] == pytest.approx(
        result["inventory_days"] + result["receivable_days"] - result["payable_days"]
    )


def test_roic_tax_clamp_and_per_share_fallback():
    income, balance, cash = _history()
    result = compute_financial_ratios("202404", income, balance, cash)
    # TTM parent net income=80, TTM EPS=4 -> 20 shares.
    assert result["book_value_per_share"] == pytest.approx(25)
    assert result["free_cash_flow_per_share"] == pytest.approx(4.5)
    # Tax rate=(120-96)/120=20%; avg invested capital=(500+30+70-50)=550.
    assert result["roic"] == pytest.approx(160 * 0.8 / 550)
    assert result["interest_coverage"] == pytest.approx(160 / 20)


def test_direct_bps_wins_over_derived_value():
    income, balance, cash = _history()
    balance[-1]["BookValuePerShare"] = 88.5
    result = compute_financial_ratios("202404", income, balance, cash)
    assert result["book_value_per_share"] == 88.5


def test_nonpositive_cagr_base_is_null():
    income, balance, cash = _history(negative_base=True)
    result = compute_financial_ratios("202404", income, balance, cash)
    assert result["eps_cagr_3y"] is None


def test_zero_growth_denominator_and_missing_cash_predecessor_are_null():
    income, balance, cash = _history()
    for row in income:
        if row["period"].startswith("2023"):
            row["Revenue"] = 0
    cash = [row for row in cash if row["period"] != "202402"]
    result = compute_financial_ratios("202404", income, balance, cash)
    assert result["revenue_yoy"] is None
    assert result["operating_cash_flow"] is None
    assert result["free_cash_flow"] is None


def test_financial_company_guard_nulls_not_applicable_metrics():
    income, balance, cash = _history(financial=True)
    result = compute_financial_ratios(
        "202404", income, balance, cash, {"name": "範例商業銀行"}
    )
    guarded = (
        "roic", "asset_turnover", "inventory_turnover", "receivable_turnover",
        "payable_turnover", "inventory_days", "receivable_days", "payable_days",
        "cash_conversion_cycle", "dupont_asset_turnover",
        "dupont_equity_multiplier",
    )
    assert all(result[field] is None for field in guarded)
    assert result["free_cash_flow"] == 90


def test_missing_current_statement_is_reported():
    income, balance, cash = _history()
    with pytest.raises(ValueError, match="missing current"):
        compute_financial_ratios("202404", income[:-1], balance, cash)
