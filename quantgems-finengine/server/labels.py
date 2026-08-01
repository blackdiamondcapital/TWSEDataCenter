# -*- coding: utf-8 -*-
"""中英欄位標籤與報表分組（財報引擎顯示用）。"""

INCOME_GROUPS = [
    {
        "id": "ops",
        "title": "營運表現",
        "keys": [
            "Revenue",
            "OperatingCosts",
            "GrossProfitFromOperations",
            "SellingExpense",
            "AdministrativeExpense",
            "ResearchAndDevelopmentExpense",
            "NetOtherIncomeExpenses",
            "ProfitLossFromOperatingActivities",
        ],
    },
    {
        "id": "below",
        "title": "營業外與稅後",
        "keys": [
            "FinanceCosts",
            "NonoperatingIncomeAndExpenses",
            "ProfitLossBeforeTax",
            "ProfitLoss",
            "ProfitLossAttributableToOwnersOfParent",
            "BasicEarningsLossPerShareTotal",
            "DilutedEarningsLossPerShareTotal",
        ],
    },
    {
        "id": "oci",
        "title": "其他綜合損益",
        "keys": [
            "OCIEquityInstruments",
            "OCINotReclassifiedTotal",
            "OCIReclassifiedTotal",
            "OtherComprehensiveIncomeTotal",
            "ComprehensiveIncome",
        ],
    },
]

BALANCE_GROUPS = [
    {
        "id": "assets",
        "title": "資產",
        "keys": [
            "CashAndCashEquivalents",
            "AccountsReceivableNet",
            "Inventories",
            "CurrentAssets",
            "PropertyPlantAndEquipment",
            "InvestmentAccountedForUsingEquityMethod",
            "IntangibleAssetsAndGoodwill",
            "NoncurrentAssets",
            "Assets",
        ],
    },
    {
        "id": "liab",
        "title": "負債",
        "keys": [
            "ShorttermBorrowings",
            "CurrentLiabilities",
            "LongtermBorrowings",
            "OtherNoncurrentLiabilities",
            "Liabilities",
        ],
    },
    {
        "id": "equity",
        "title": "權益",
        "keys": [
            "OrdinaryShare",
            "EquityAttributableToOwnersOfParent",
            "Equity",
        ],
    },
]

CASHFLOW_GROUPS = [
    {
        "id": "op",
        "title": "營業活動",
        "keys": [
            "ProfitLossBeforeTax",
            "DepreciationExpense",
            "AmortisationExpense",
            "CashFlowsFromUsedInOperations",
            "NetCashFlowsFromUsedInOperatingActivities",
        ],
    },
    {
        "id": "inv",
        "title": "投資活動",
        "keys": [
            "AcquisitionOfPropertyPlantAndEquipment",
            "ProceedsFromDisposalOfPropertyPlantAndEquipment",
            "AcquisitionOfIntangibleAssets",
            "NetCashFlowsFromUsedInInvestingActivities",
        ],
    },
    {
        "id": "fin",
        "title": "籌資活動",
        "keys": [
            "DividendsPaid",
            "NetCashFlowsFromUsedInFinancingActivities",
            "NetIncreaseDecreaseInCashAndCashEquivalents",
            "CashAndCashEquivalentsAtEndOfPeriod",
        ],
    },
]

LABELS_ZH = {
    # Income
    "Revenue": "營業收入",
    "OperatingCosts": "營業成本",
    "GrossProfitFromOperations": "營業毛利",
    "SellingExpense": "推銷費用",
    "AdministrativeExpense": "管理費用",
    "ResearchAndDevelopmentExpense": "研究發展費用",
    "NetOtherIncomeExpenses": "其他收益及費損淨額",
    "ProfitLossFromOperatingActivities": "營業利益",
    "FinanceCosts": "財務成本",
    "NonoperatingIncomeAndExpenses": "營業外收入及支出",
    "ProfitLossBeforeTax": "稅前淨利",
    "ProfitLoss": "本期淨利",
    "ProfitLossAttributableToOwnersOfParent": "歸屬母公司淨利",
    "BasicEarningsLossPerShareTotal": "基本每股盈餘",
    "DilutedEarningsLossPerShareTotal": "稀釋每股盈餘",
    "OCIEquityInstruments": "權益工具未實現損益",
    "OCINotReclassifiedTotal": "不重分類之綜合損益",
    "OCIReclassifiedTotal": "後續可能重分類之綜合損益",
    "OtherComprehensiveIncomeTotal": "其他綜合損益合計",
    "ComprehensiveIncome": "綜合損益總額",
    "RevenueFromInterest": "利息收入",
    "OtherRevenue": "其他收入",
    "OtherGainsLossesNet": "其他利益及損失",
    # Balance
    "CashAndCashEquivalents": "現金及約當現金",
    "AccountsReceivableNet": "應收帳款淨額",
    "Inventories": "存貨",
    "CurrentAssets": "流動資產",
    "PropertyPlantAndEquipment": "不動產廠房及設備",
    "InvestmentAccountedForUsingEquityMethod": "採用權益法之投資",
    "IntangibleAssetsAndGoodwill": "無形資產及商譽",
    "NoncurrentAssets": "非流動資產",
    "Assets": "資產總計",
    "ShorttermBorrowings": "短期借款",
    "CurrentLiabilities": "流動負債",
    "LongtermBorrowings": "長期借款",
    "OtherNoncurrentLiabilities": "其他非流動負債",
    "Liabilities": "負債總計",
    "OrdinaryShare": "普通股股本",
    "EquityAttributableToOwnersOfParent": "歸屬母公司權益",
    "Equity": "權益總計",
    # Cash flow
    "DepreciationExpense": "折舊費用",
    "AmortisationExpense": "攤銷費用",
    "CashFlowsFromUsedInOperations": "營運產生之現金流量",
    "NetCashFlowsFromUsedInOperatingActivities": "營業活動淨現金流",
    "AcquisitionOfPropertyPlantAndEquipment": "取得不動產廠房設備",
    "ProceedsFromDisposalOfPropertyPlantAndEquipment": "處分不動產廠房設備",
    "AcquisitionOfIntangibleAssets": "取得無形資產",
    "NetCashFlowsFromUsedInInvestingActivities": "投資活動淨現金流",
    "DividendsPaid": "發放現金股利",
    "NetCashFlowsFromUsedInFinancingActivities": "籌資活動淨現金流",
    "NetIncreaseDecreaseInCashAndCashEquivalents": "現金淨增減",
    "CashAndCashEquivalentsAtEndOfPeriod": "期末現金及約當現金",
    # Ratios
    "gross_margin": "毛利率",
    "op_margin": "營業利益率",
    "net_margin": "淨利率",
    "roa": "ROA",
    "roe": "ROE",
    "debt_ratio": "負債比率",
    "current_ratio": "流動比率",
    "quick_ratio": "速動比率",
}

EMPHASIS_KEYS = {
    "Revenue",
    "GrossProfitFromOperations",
    "ProfitLossFromOperatingActivities",
    "ProfitLoss",
    "BasicEarningsLossPerShareTotal",
    "Assets",
    "Liabilities",
    "Equity",
    "CurrentAssets",
    "CurrentLiabilities",
    "NetCashFlowsFromUsedInOperatingActivities",
    "NetCashFlowsFromUsedInInvestingActivities",
    "NetCashFlowsFromUsedInFinancingActivities",
    "CashAndCashEquivalentsAtEndOfPeriod",
}

EPS_KEYS = {
    "BasicEarningsLossPerShareTotal",
    "DilutedEarningsLossPerShareTotal",
}

RATIO_KEYS = [
    "gross_margin",
    "op_margin",
    "net_margin",
    "roa",
    "roe",
    "debt_ratio",
    "current_ratio",
    "quick_ratio",
]


def groups_for(kind: str):
    if kind == "income":
        return INCOME_GROUPS
    if kind == "balance":
        return BALANCE_GROUPS
    if kind == "cashflow":
        return CASHFLOW_GROUPS
    return []
