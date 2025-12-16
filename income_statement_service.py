# -*- coding: utf-8 -*-
"""Service functions for fetching Taiwan stock income statement data from MOPS.

This module exposes two main entry points:

- fetch_income_row(co_id: str, year: str, season: str) -> pd.DataFrame
    Fetch a single company's income statement for a given year/season and
    return a single-row wide-format DataFrame.

- fetch_all_incomes(year: str, season: str) -> pd.DataFrame
    Fetch income statements for all TWSE/TPEX listed companies and return
    a wide-format DataFrame with one row per (stock, period).

The core scraping/parsing logic is largely adapted from the standalone
script `comprehensive_income_statement.py`.
"""

import time
from io import BytesIO
from typing import List, Callable, Optional

import logging
import pandas as pd
import requests
from lxml import etree


TARGET_KEYWORDS = {
    "RevenueFromInterest": [
        "ifrs-full:RevenueFromInterest",
        "Total interest income",
        "利息收入合計",
        "利息收入",
    ],
    "OtherRevenue": [
        "ifrs-full:OtherRevenue",
        "Total other income",
        "其他收入合計",
        "其他收入",
    ],
    "OtherGainsLossesNet": [
        "ifrs-full:OtherGainsLossesNet",
        "ifrs-full:OtherGainsLosses",
        "tifrs-bsci-ci:OtherGainsLossesNet",
        "Other gains and losses, net",
        "Other gains and losses",
        "其他利益及損失淨額",
    ],
    "Revenue": ["Revenue", "ifrs-full:Revenue", "營業收入"],
    "OperatingCosts": [
        "OperatingCosts",
        "tifrs-bsci-ci:OperatingCosts",
        "營業成本",
        "Total operating costs",
    ],
    "GrossProfitFromOperations": [
        "tifrs-bsci-ci:GrossProfitLossFromOperations",
        "Gross profit (loss) from operations",
        "營業毛利",
        "營業毛利 (營業利益)",
        "營業毛利 (營業利益) (營業收入-營業成本)",
    ],
    "SellingExpense": [
        "ifrs-full:SellingExpense",
        "Selling expenses",
        "推銷費用",
    ],
    "AdministrativeExpense": [
        "ifrs-full:AdministrativeExpense",
        "Administrative expenses",
        "管理費用",
    ],
    "ResearchAndDevelopmentExpense": [
        "ifrs-full:ResearchAndDevelopmentExpense",
        "Research and development expenses",
        "研究發展費用",
    ],
    "NetOtherIncomeExpenses": [
        "tifrs-bsci-ci:NetOtherIncomeExpenses",
        "Net other income (expenses)",
        "其他收益及費損淨額",
    ],
    "FinanceCosts": [
        "ifrs-full:FinanceCosts",
        "Finance costs, net",
        "Finance costs",
        "財務成本淨額",
        "財務成本",
    ],
    "NonoperatingIncomeAndExpenses": [
        "tifrs-bsci-ci:NonoperatingIncomeAndExpenses",
        "Total non-operating income and expenses",
        "Non-operating income and expenses",
        "營業外收入及支出合計",
        "營業外收入及支出",
    ],
    "OCIEquityInstruments": [
        "ifrs-full:OtherComprehensiveIncomeBeforeTaxGainsLossesFromInvestmentsInEquityInstruments",
        "Unrealised gains (losses) from investments in equity instruments measured at fair value through other comprehensive income",
        "Unrealized gains (losses) from investments in equity instruments measured at fair value through other comprehensive income",
        "未實現損益",
        "透過其他綜合損益按公允價值衡量之權益工具投資未實現損益",
    ],
    "OCINotReclassifiedTotal": [
        "ifrs-full:OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLossNetOfTax",
        "Other comprehensive income that will not be reclassified to profit or loss, net of tax",
        "Other comprehensive income that will not be reclassified to profit or loss",
        "不重分類至損益之項目總額",
        "不重分類至損益之項目",
    ],
    "OCIReclassifiedTotal": [
        "ifrs-full:OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLossNetOfTax",
        "Other comprehensive income that will be reclassified to profit or loss, net of tax",
        "Other comprehensive income that will be reclassified to profit or loss",
        "後續可能重分類至損益之項目總額",
        "後續可能重分類至損益之項目",
    ],
    "OtherComprehensiveIncomeTotal": [
        "ifrs-full:OtherComprehensiveIncome",
        "Total other comprehensive income",
        "Other comprehensive income, net",
        "其他綜合損益（淨額）",
        "其他綜合損益淨額",
        "其他綜合損益",
    ],
    "ComprehensiveIncome": [
        "ifrs-full:ComprehensiveIncome",
        "Total comprehensive income",
        "Total comprehensive income for the period",
        "本期綜合損益總額",
        "本期綜合損益",
    ],
    "ComprehensiveIncomeAttributableToOwnersOfParent": [
        "ifrs-full:ComprehensiveIncomeAttributableToOwnersOfParent",
        "Comprehensive income, attributable to owners of parent",
        "Total comprehensive income attributable to owners of parent",
        "Comprehensive income for the period attributable to owners of parent",
        "母公司業主（綜合損益）",
        "母公司業主綜合損益",
    ],
    "ComprehensiveIncomeAttributableToNoncontrollingInterests": [
        "ifrs-full:ComprehensiveIncomeAttributableToNoncontrollingInterests",
        "Comprehensive income, attributable to non-controlling interests",
        "Total comprehensive income attributable to non-controlling interests",
        "Comprehensive income for the period attributable to non-controlling interests",
        "非控制權益（綜合損益）",
        "非控制權益綜合損益",
    ],
    "BasicEarningsLossPerShareTotal": [
        "ifrs-full:BasicEarningsLossPerShare",
        "Total basic earnings per share",
        "Basic earnings (loss) per share",
        "Basic earnings per share",
        "基本每股盈餘合計",
        "基本每股盈餘",
    ],
    "DilutedEarningsLossPerShareTotal": [
        "ifrs-full:DilutedEarningsLossPerShare",
        "Total diluted earnings per share",
        "Diluted earnings (loss) per share",
        "Diluted earnings per share",
        "稀釋每股盈餘合計",
        "稀釋每股盈餘",
    ],
    "ExchangeDifferencesOnTranslation": [
        "ifrs-full:ExchangeDifferencesOnTranslation",
        "ifrs-full:ExchangeDifferencesOnTranslationOfForeignOperations",
        "ifrs-full:OtherComprehensiveIncomeBeforeTaxExchangeDifferencesOnTranslation",
        "Exchange differences on translation",
        "Exchange differences on translation of foreign operations",
        "國外營運機構財務報表換算之兌換差額",
    ],
    "EquityMethodOCIReclassified": [
        "ifrs-full:ShareOfOtherComprehensiveIncomeOfAssociatesAndJointVenturesAccountedForUsingEquityMethodThatWillBeReclassifiedToProfitOrLossBeforeTax",
        "Share of other comprehensive income of associates and joint ventures accounted for using equity method that will be reclassified to profit or loss before tax",
        "採用權益法認列之關聯企業及合資之其他綜合損益之份額－可能重分類至損益之項目",
        "採用權益法認列之關聯企業及合資之其他綜合損益之份額可能重分類至損益",
    ],
    "EquityMethodOCINotReclassified": [
        "ifrs-full:ShareOfOtherComprehensiveIncomeOfAssociatesAndJointVenturesAccountedForUsingEquityMethodThatWillNotBeReclassifiedToProfitOrLossBeforeTax",
        "Share of other comprehensive income of associates and joint ventures accounted for using equity method that will not be reclassified to profit or loss before tax",
        "採用權益法認列之關聯企業及合資之其他綜合損益－不重分類至損益之項目",
        "採用權益法認列之關聯企業及合資之其他綜合損益不重分類至損益",
    ],
    "EquityMethodShareOfProfitLoss": [
        "ifrs-full:ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod",
        "Share of profit (loss) of associates and joint ventures accounted for using equity method",
        "Share of profit of associates and joint ventures accounted for using equity method",
        "採用權益法認列之關聯企業及合資損益之份額淨額",
        "採用權益法認列之關聯企業及合資損益之份額",
    ],
    "ProfitLossBeforeTax": [
        "ifrs-full:ProfitLossBeforeTax",
        "Profit (loss) from continuing operations before tax",
        "Profit before tax",
        "Profit (loss) before tax",
        "繼續營業單位稅前淨利（淨損）",
        "繼續營業單位稅前淨利",
    ],
    "ProfitLoss": [
        "ifrs-full:ProfitLoss",
        "Profit (loss)",
        "Profit (loss) for the period",
        "Profit for the period",
        "本期淨利（淨損）",
        "本期淨利",
    ],
    "ProfitLossAttributableToOwnersOfParent": [
        "ifrs-full:ProfitLossAttributableToOwnersOfParent",
        "Profit (loss) attributable to owners of parent",
        "Profit attributable to owners of parent",
        "Profit (loss) for the period attributable to owners of parent",
        "Profit for the period attributable to owners of parent",
        "母公司業主（損）益",
        "母公司業主損益",
    ],
    "ProfitLossAttributableToNoncontrollingInterests": [
        "ifrs-full:ProfitLossAttributableToNoncontrollingInterests",
        "Profit (loss) attributable to non-controlling interests",
        "Profit attributable to non-controlling interests",
        "Profit (loss) for the period attributable to non-controlling interests",
        "Profit for the period attributable to non-controlling interests",
        "非控制權益（損）益",
        "非控制權益損益",
    ],
    "ProfitLossFromOperatingActivities": [
        "ifrs-full:ProfitLossFromOperatingActivities",
        "Net operating income (loss)",
        "營業利益（損失）",
        "營業利益(損失)",
        "營業利益",
    ],
}


MAIN_CONCEPTS = {
    "Revenue": ["ifrs-full:Revenue"],
    "OtherRevenue": ["ifrs-full:OtherRevenue"],
    "OtherGainsLossesNet": [
        "ifrs-full:OtherGainsLossesNet",
        "ifrs-full:OtherGainsLosses",
        "tifrs-bsci-ci:OtherGainsLossesNet",
    ],
    "OperatingCosts": ["tifrs-bsci-ci:OperatingCosts"],
    "GrossProfitFromOperations": ["tifrs-bsci-ci:GrossProfitLossFromOperations"],
    "ProfitLossFromOperatingActivities": ["ifrs-full:ProfitLossFromOperatingActivities"],
    "SellingExpense": ["ifrs-full:SellingExpense"],
    "AdministrativeExpense": ["ifrs-full:AdministrativeExpense"],
    "ResearchAndDevelopmentExpense": ["ifrs-full:ResearchAndDevelopmentExpense"],
    "NetOtherIncomeExpenses": ["tifrs-bsci-ci:NetOtherIncomeExpenses"],
    "FinanceCosts": ["ifrs-full:FinanceCosts"],
    "NonoperatingIncomeAndExpenses": ["tifrs-bsci-ci:NonoperatingIncomeAndExpenses"],
    "EquityMethodShareOfProfitLoss": [
        "ifrs-full:ShareOfProfitLossOfAssociatesAndJointVenturesAccountedForUsingEquityMethod",
    ],
    "ProfitLossBeforeTax": ["ifrs-full:ProfitLossBeforeTax"],
    "ProfitLoss": ["ifrs-full:ProfitLoss"],
    "ProfitLossAttributableToOwnersOfParent": [
        "ifrs-full:ProfitLossAttributableToOwnersOfParent",
    ],
    "ProfitLossAttributableToNoncontrollingInterests": [
        "ifrs-full:ProfitLossAttributableToNoncontrollingInterests",
    ],
    "OCIEquityInstruments": [
        "ifrs-full:OtherComprehensiveIncomeBeforeTaxGainsLossesFromInvestmentsInEquityInstruments",
    ],
    "OCINotReclassifiedTotal": [
        "ifrs-full:OtherComprehensiveIncomeThatWillNotBeReclassifiedToProfitOrLossNetOfTax",
    ],
    "OCIReclassifiedTotal": [
        "ifrs-full:OtherComprehensiveIncomeThatWillBeReclassifiedToProfitOrLossNetOfTax",
    ],
    "OtherComprehensiveIncomeTotal": ["ifrs-full:OtherComprehensiveIncome"],
    "ComprehensiveIncome": ["ifrs-full:ComprehensiveIncome"],
    "ComprehensiveIncomeAttributableToOwnersOfParent": [
        "ifrs-full:ComprehensiveIncomeAttributableToOwnersOfParent",
    ],
    "ComprehensiveIncomeAttributableToNoncontrollingInterests": [
        "ifrs-full:ComprehensiveIncomeAttributableToNoncontrollingInterests",
    ],
    "BasicEarningsLossPerShareTotal": ["ifrs-full:BasicEarningsLossPerShare"],
    "DilutedEarningsLossPerShareTotal": ["ifrs-full:DilutedEarningsLossPerShare"],
    "ExchangeDifferencesOnTranslation": ["ifrs-full:ExchangeDifferencesOnTranslation"],
    "EquityMethodOCIReclassified": [
        "ifrs-full:ShareOfOtherComprehensiveIncomeOfAssociatesAndJointVenturesAccountedForUsingEquityMethodThatWillBeReclassifiedToProfitOrLossBeforeTax",
    ],
    "EquityMethodOCINotReclassified": [
        "ifrs-full:ShareOfOtherComprehensiveIncomeOfAssociatesAndJointVenturesAccountedForUsingEquityMethodThatWillNotBeReclassifiedToProfitOrLossBeforeTax",
    ],
    "RevenueFromInterest": ["ifrs-full:RevenueFromInterest"],
}


TARGET_ORDER = [
    "Revenue",
    "RevenueFromInterest",
    "OtherRevenue",
    "OtherGainsLossesNet",
    "OperatingCosts",
    "GrossProfitFromOperations",
    "SellingExpense",
    "AdministrativeExpense",
    "ResearchAndDevelopmentExpense",
    "NetOtherIncomeExpenses",
    "ProfitLossFromOperatingActivities",
    "FinanceCosts",
    "EquityMethodShareOfProfitLoss",
    "NonoperatingIncomeAndExpenses",
    "ProfitLossBeforeTax",
    "ProfitLoss",
    "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossAttributableToNoncontrollingInterests",
    "OCIEquityInstruments",
    "EquityMethodOCINotReclassified",
    "OCINotReclassifiedTotal",
    "ExchangeDifferencesOnTranslation",
    "EquityMethodOCIReclassified",
    "OCIReclassifiedTotal",
    "OtherComprehensiveIncomeTotal",
    "ComprehensiveIncome",
    "ComprehensiveIncomeAttributableToOwnersOfParent",
    "ComprehensiveIncomeAttributableToNoncontrollingInterests",
    "BasicEarningsLossPerShareTotal",
    "DilutedEarningsLossPerShareTotal",
]


logger = logging.getLogger(__name__)


class MopsBlockedError(Exception):
    pass


def _build_mops_url(co_id: str, year: str, season: str) -> str:
    """Construct the MOPS URL for a given company/year/season (C-report)."""

    return (
        "https://mopsov.twse.com.tw/server-java/t164sb01?step=1"
        f"&CO_ID={co_id}&SYEAR={year}&SSEASON={season}&REPORT_ID=C"
    )


def _parse_number(s: str):
    if not s:
        return None
    s2 = s.replace(",", "").replace("(", "-").replace(")", "").strip()
    try:
        if "." in s2:
            return float(s2)
        else:
            return int(s2)
    except Exception:
        try:
            return float(s2)
        except Exception:
            return None


def _apply_sign(row):
    v = row["numeric_value"]
    s = row.get("sign")
    if v is None:
        return None
    if s == "-":
        return -abs(v)
    return v


def _apply_scale(row):
    v = row["numeric_value"]
    sc = row["scale"]
    if v is None or sc is None:
        return v
    try:
        s = int(sc)
        return v * (10 ** s)
    except Exception:
        return v


def fetch_income_row(co_id: str, year: str, season: str) -> pd.DataFrame:
    """Fetch a single company's income-statement row in wide format.

    Returns a 1-row DataFrame with columns:
    股票代號, period, Revenue, RevenueFromInterest, ..., DilutedEarningsLossPerShareTotal
    """

    url = _build_mops_url(co_id, year, season)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    resp = session.get(url, timeout=20)
    resp.raise_for_status()

    text = ""
    try:
        text = resp.text or ""
    except Exception:
        try:
            text = resp.content.decode("utf-8", errors="ignore")
        except Exception:
            text = ""
    upper_text = text.upper()
    if (
        "FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED!" in upper_text
        or "THE PAGE CANNOT BE ACCESSED!" in upper_text
        or "因為安全性考量，您所執行的頁面無法呈現" in text
    ):
        logger.warning(
            "[income][blocked] MOPS security page for stock=%s year=%s season=%s",
            co_id,
            year,
            season,
        )
        raise MopsBlockedError("MOPS/TWSE security page encountered")

    parser = etree.HTMLParser()
    tree = etree.parse(BytesIO(resp.content), parser)

    nonfraction_nodes = tree.xpath("//*[local-name() = 'nonfraction']")

    results = []
    for node in nonfraction_nodes:
        name_attr = node.get("name") or ""
        text_content = (node.text or "").strip()

        matched_key = None
        for key, kws in TARGET_KEYWORDS.items():
            for kw in kws:
                if kw.startswith(("ifrs-full:", "tifrs-bsci-ci:")):
                    if name_attr == kw:
                        matched_key = key
                        break
                else:
                    if kw.lower() in name_attr.lower() or kw in text_content:
                        matched_key = key
                        break
            if matched_key:
                break

        if not matched_key:
            continue

        contextref = node.get("contextref")
        unitref = node.get("unitref")
        scale = node.get("scale")
        decimals = node.get("decimals")
        sign = node.get("sign")
        value_text = text_content

        period_text = None
        if contextref:
            ctx = tree.xpath(
                f"//*[local-name() = 'context' and @id='{contextref}']"
            )
            if ctx:
                ctx = ctx[0]
                start = ctx.xpath(".//*[local-name()='startDate']/text()")
                end = ctx.xpath(".//*[local-name()='endDate']/text()")
                instant = ctx.xpath(".//*[local-name()='instant']/text()")
                if start and end:
                    period_text = f"{start[0]} to {end[0]}"
                elif instant:
                    period_text = instant[0]
                else:
                    period_text = contextref

        results.append(
            {
                "target": matched_key,
                "name_attr": name_attr,
                "value_text": value_text,
                "contextref": contextref,
                "period": period_text,
                "unitref": unitref,
                "scale": scale,
                "decimals": decimals,
                "sign": sign,
            }
        )

    df = pd.DataFrame(results)
    if df.empty:
        try:
            logger.info(
                "[income][no-match] stock=%s year=%s season=%s (no TARGET_KEYWORDS matched)",
                co_id,
                year,
                season,
            )
        except Exception:
            pass
        return pd.DataFrame()

    df = df[~df["contextref"].astype(str).str.contains("_")].reset_index(drop=True)

    df["numeric_value"] = df["value_text"].apply(_parse_number)
    df["numeric_value"] = df.apply(_apply_sign, axis=1)
    df["scaled_value"] = df.apply(_apply_scale, axis=1)

    # period：這裡直接用 year + season 兩位，例如 2025 + 3 -> 202503
    period_label = f"{year}{int(season):02d}"
    df["period"] = period_label

    df["target"] = pd.Categorical(df["target"], categories=TARGET_ORDER, ordered=True)
    df = df.sort_values(["target", "period"]).reset_index(drop=True)
    # observed=False 保持與目前預設行為一致，並避免 pandas FutureWarning
    df = df.groupby("target", observed=False).head(1).reset_index(drop=True)

    # wide format
    stock_code = co_id
    pivot = df.pivot_table(
        index=["period"], columns="target", values="scaled_value", aggfunc="first"
    ).reset_index()
    pivot.columns.name = None
    pivot.insert(0, "股票代號", stock_code)

    # 依 TARGET_ORDER 排欄位
    ordered_cols = ["股票代號", "period"] + [
        t for t in TARGET_ORDER if t in pivot.columns
    ]
    pivot = pivot[ordered_cols]
    try:
        if pivot.empty:
            logger.info(
                "[income][parsed-empty] stock=%s period=%s (no fields parsed)",
                stock_code,
                period_label,
            )
        else:
            record = pivot.iloc[0].to_dict()
            logger.info(
                "[income][parsed] stock=%s period=%s fields_values=%s",
                stock_code,
                period_label,
                record,
            )
    except Exception:
        pass
    return pivot


def fetch_all_stock_codes() -> List[str]:
    """Fetch all TWSE + TPEX stock codes from ISIN pages."""

    urls = [
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",  # 上市
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",  # 上櫃
    ]

    codes: set[str] = set()
    for url in urls:
        # 這兩個 ISIN 頁面使用 Big5 編碼，若直接給 pandas URL 會以 UTF-8 解碼而失敗
        resp = requests.get(url, timeout=20)
        # 明確指定 Big5（cp950）；若解析失敗則退回 apparent_encoding 或忽略錯誤
        try:
            resp.encoding = "cp950"  # Big5 / 繁體中文編碼
            html = resp.text
        except Exception:
            try:
                enc = resp.apparent_encoding or "cp950"
                resp.encoding = enc
                html = resp.text
            except Exception:
                html = resp.content.decode("cp950", errors="ignore")

        tables = pd.read_html(html)
        if not tables:
            continue
        df = tables[0]
        col = df.columns[0]
        for val in df[col]:
            if not isinstance(val, str):
                continue
            parts = val.split()
            if len(parts) >= 1 and parts[0].isdigit() and len(parts[0]) == 4:
                codes.add(parts[0])

    return sorted(codes)


def fetch_all_incomes(
    year: str,
    season: str,
    delay: float = 0.5,
    progress_cb: Optional[Callable[[int, int, str, str, Optional[str]], None]] = None,
    row_cb: Optional[Callable[[str, pd.DataFrame], None]] = None,
    code_from: Optional[str] = None,
    code_to: Optional[str] = None,
    pause_every: Optional[int] = None,
    pause_seconds: float = 0.0,
) -> pd.DataFrame:
    """Fetch income statement wide-rows for all stocks for a given year/season.

    If provided, ``progress_cb`` will be called為每一檔股票回報進度：
        progress_cb(idx, total, code, status, detail)
    """

    codes = fetch_all_stock_codes()
    if code_from is not None or code_to is not None:
        cf = str(code_from).strip() if code_from else None
        ct = str(code_to).strip() if code_to else None
        if cf and ct and cf > ct:
            cf, ct = ct, cf
        filtered_codes: list[str] = []
        for c in codes:
            if cf and c < cf:
                continue
            if ct and c > ct:
                continue
            filtered_codes.append(c)
        logger.info(
            "[income] applying code range filter from=%s to=%s -> %d codes (original=%d)",
            cf,
            ct,
            len(filtered_codes),
            len(codes),
        )
        codes = filtered_codes

    rows: list[pd.DataFrame] = []

    total = len(codes)
    for idx, co_id in enumerate(codes, 1):
        try:
            logger.info(
                "[income] fetching %s/%s for stock %s (year=%s, season=%s)",
                idx,
                total,
                co_id,
                year,
                season,
            )
            if progress_cb is not None:
                progress_cb(idx, total, co_id, "start", None)

            row_df = fetch_income_row(co_id, year, season)
            if not row_df.empty:
                rows.append(row_df)
                try:
                    recs = row_df.to_dict(orient="records")
                    if recs:
                        logger.info(
                            "[income][row] stock=%s period=%s fields_values=%s",
                            co_id,
                            recs[0].get("period"),
                            recs[0],
                        )
                except Exception:
                    pass
                if row_cb is not None:
                    row_cb(co_id, row_df)
                if progress_cb is not None:
                    progress_cb(idx, total, co_id, "success", None)
            else:
                if progress_cb is not None:
                    progress_cb(idx, total, co_id, "empty", None)
        except MopsBlockedError as e:
            logger.error(
                "[income] MOPS blocked at stock %s (%s/%s): %s",
                co_id,
                idx,
                total,
                e,
            )
            if progress_cb is not None:
                progress_cb(idx, total, co_id, "error", str(e))
            break
        except Exception as e:
            logger.exception("[income] error fetching stock %s: %s", co_id, e)
            if progress_cb is not None:
                progress_cb(idx, total, co_id, "error", str(e))
            # swallow individual errors, continue with others
            continue
        time.sleep(delay)

        if (
            pause_every
            and pause_every > 0
            and pause_seconds > 0
            and idx < total
            and (idx % pause_every == 0)
        ):
            logger.info(
                "[income][throttle] reached %s/%s stocks, sleeping %.1f seconds",
                idx,
                total,
                pause_seconds,
            )
            time.sleep(pause_seconds)

    if progress_cb is not None:
        progress_cb(total, total, "", "done", None)

    if not rows:
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)
