# -*- coding: utf-8 -*-
"""Fetch Taiwan listed companies' cash-flow statements from MOPS iXBRL."""

import logging
import time
from typing import Callable, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class MopsBlockedError(Exception):
    """Raised when MOPS returns its security blocking page."""


# Stable API/database column names mapped to XBRL concepts and label fallbacks.
# The label fallbacks are needed because issuers occasionally use extension concepts.
TARGET_KEYWORDS = {
    "ProfitLossBeforeTax": [
        "ifrs-full:ProfitLossBeforeTax",
        "繼續營業單位稅前淨利（淨損）",
        "稅前淨利（淨損）",
    ],
    "AdjustmentsToReconcileProfitLoss": [
        "ifrs-full:AdjustmentsToReconcileProfitLoss",
        "收益費損項目合計",
        "調整項目合計",
    ],
    "DepreciationExpense": [
        "ifrs-full:DepreciationExpense",
        "折舊費用",
    ],
    "AmortisationExpense": [
        "ifrs-full:AmortisationExpense",
        "攤銷費用",
    ],
    "InterestExpense": [
        "ifrs-full:InterestExpense",
        "利息費用",
    ],
    "InterestIncome": [
        "ifrs-full:InterestIncome",
        "利息收入",
    ],
    "CashFlowsFromUsedInOperations": [
        "ifrs-full:CashFlowsFromUsedInOperations",
        "營運產生之現金流入（流出）",
        "營運產生之現金流量",
    ],
    "InterestReceived": [
        "ifrs-full:InterestReceived",
        "收取之利息",
    ],
    "DividendsReceived": [
        "ifrs-full:DividendsReceived",
        "收取之股利",
    ],
    "InterestPaid": [
        "ifrs-full:InterestPaid",
        "支付之利息",
    ],
    "IncomeTaxesPaid": [
        "ifrs-full:IncomeTaxesPaidRefund",
        "ifrs-full:IncomeTaxesPaid",
        "支付之所得稅",
        "退還之所得稅",
    ],
    "NetCashFlowsFromUsedInOperatingActivities": [
        "ifrs-full:CashFlowsFromUsedInOperatingActivities",
        "ifrs-full:NetCashFlowsFromUsedInOperatingActivities",
        "營業活動之淨現金流入（流出）",
        "營業活動之淨現金流量",
    ],
    "AcquisitionOfPropertyPlantAndEquipment": [
        "ifrs-full:PurchaseOfPropertyPlantAndEquipment",
        "ifrs-full:AcquisitionOfPropertyPlantAndEquipment",
        "取得不動產、廠房及設備",
    ],
    "ProceedsFromDisposalOfPropertyPlantAndEquipment": [
        "ifrs-full:ProceedsFromSalesOfPropertyPlantAndEquipment",
        "ifrs-full:ProceedsFromDisposalOfPropertyPlantAndEquipment",
        "處分不動產、廠房及設備",
    ],
    "AcquisitionOfIntangibleAssets": [
        "ifrs-full:PurchaseOfIntangibleAssets",
        "ifrs-full:AcquisitionOfIntangibleAssets",
        "取得無形資產",
    ],
    "AcquisitionOfInvestments": [
        "ifrs-full:PurchaseOfInvestments",
        "ifrs-full:AcquisitionOfInvestments",
        "取得投資",
        "取得金融資產",
    ],
    "ProceedsFromDisposalOfInvestments": [
        "ifrs-full:ProceedsFromSalesOfInvestments",
        "ifrs-full:ProceedsFromDisposalOfInvestments",
        "處分投資",
        "處分金融資產",
    ],
    "NetCashFlowsFromUsedInInvestingActivities": [
        "ifrs-full:CashFlowsFromUsedInInvestingActivities",
        "ifrs-full:NetCashFlowsFromUsedInInvestingActivities",
        "tifrs-SCF:NetCashFlowsFromUsedInInvestingActivities",
        "投資活動之淨現金流入（流出）",
        "投資活動之淨現金流量",
    ],
    "ProceedsFromIssuingShares": [
        "ifrs-full:ProceedsFromIssuingShares",
        "現金增資",
        "發行股份",
    ],
    "PaymentsToAcquireTreasuryShares": [
        "ifrs-full:PaymentsToAcquireTreasuryShares",
        "取得庫藏股票",
    ],
    "ProceedsFromBorrowings": [
        "ifrs-full:ProceedsFromBorrowings",
        "舉借借款",
        "借款增加",
    ],
    "RepaymentsOfBorrowings": [
        "ifrs-full:RepaymentsOfBorrowings",
        "償還借款",
    ],
    "DividendsPaid": [
        "ifrs-full:DividendsPaid",
        "發放現金股利",
        "支付之股利",
    ],
    "PaymentsOfLeaseLiabilities": [
        "ifrs-full:PaymentsOfLeaseLiabilities",
        "租賃負債本金償還",
        "償還租賃負債",
    ],
    "NetCashFlowsFromUsedInFinancingActivities": [
        "ifrs-full:CashFlowsFromUsedInFinancingActivities",
        "ifrs-full:NetCashFlowsFromUsedInFinancingActivities",
        "tifrs-SCF:CashFlowsFromUsedInFinancingActivities",
        "tifrs-SCF:NetCashFlowsFromUsedInFinancingActivities",
        "籌資活動之淨現金流入（流出）",
        "籌資活動之淨現金流量",
    ],
    "EffectOfExchangeRateChangesOnCashAndCashEquivalents": [
        "ifrs-full:EffectOfExchangeRateChangesOnCashAndCashEquivalents",
        "匯率變動對現金及約當現金之影響",
    ],
    "NetIncreaseDecreaseInCashAndCashEquivalents": [
        "ifrs-full:IncreaseDecreaseInCashAndCashEquivalents",
        "ifrs-full:NetIncreaseDecreaseInCashAndCashEquivalents",
        "tifrs-SCF:IncreaseDecreaseInCashAndCashEquivalents",
        "本期現金及約當現金增加（減少）數",
        "現金及約當現金淨增加（減少）",
    ],
    "CashAndCashEquivalentsAtBeginningOfPeriod": [
        "ifrs-full:CashAndCashEquivalentsAtBeginningOfPeriod",
        "期初現金及約當現金餘額",
    ],
    "CashAndCashEquivalentsAtEndOfPeriod": [
        "ifrs-full:CashAndCashEquivalentsAtEndOfPeriod",
        "期末現金及約當現金餘額",
    ],
}

TARGET_ORDER = list(TARGET_KEYWORDS.keys())
_CONCEPT_PREFIXES = (
    "ifrs-full:",
    "tifrs-bsci-ci:",
    "tifrs-cash-flows:",
    "tifrs-SCF:",
)


def _build_mops_url(co_id: str, year: str, season: str, *, host: str) -> str:
    return (
        f"https://{host}/server-java/t164sb01?step=1"
        f"&CO_ID={co_id}&SYEAR={year}&SSEASON={season}&REPORT_ID=C"
    )


def _is_connection_issue(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "failed to resolve",
            "nodatename nor servname",
            "name resolution",
            "gaierror",
            "connection reset",
            "connection aborted",
            "max retries exceeded",
            "timed out",
        )
    )


def _parse_number(value: str):
    if not value:
        return None
    normalized = (
        value.replace(",", "")
        .replace("(", "-")
        .replace(")", "")
        .replace("\u2212", "-")
        .strip()
    )
    try:
        return float(normalized) if "." in normalized else int(normalized)
    except (TypeError, ValueError):
        return None


def _scaled_signed_value(value, sign, scale):
    if value is None:
        return None
    if sign == "-":
        value = -abs(value)
    try:
        return value * (10 ** int(scale)) if scale is not None else value
    except (TypeError, ValueError):
        return value


def _match_target(name_attr: str, row_text: str, text_content: str) -> Optional[str]:
    lowered_name = name_attr.lower()
    for key, keywords in TARGET_KEYWORDS.items():
        for keyword in keywords:
            if keyword.startswith(_CONCEPT_PREFIXES):
                lowered_keyword = keyword.lower()
                if lowered_name == lowered_keyword or lowered_name.startswith(f"{lowered_keyword}-"):
                    return key
            elif keyword in row_text or keyword in text_content:
                return key
    return None


def fetch_cash_flow_row(co_id: str, year: str, season: str) -> pd.DataFrame:
    """Return one company's cash-flow statement as a one-row wide DataFrame."""
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

    response = None
    last_error: Optional[Exception] = None
    for host in ("mopsov.twse.com.tw", "mops.twse.com.tw"):
        try:
            response = session.get(_build_mops_url(co_id, year, season, host=host), timeout=20)
            response.raise_for_status()
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            if not _is_connection_issue(exc):
                raise
            logger.warning(
                "[cash-flow][mops] connection issue host=%s stock=%s year=%s season=%s err=%s",
                host,
                co_id,
                year,
                season,
                exc,
            )

    if response is None:
        raise last_error or RuntimeError("MOPS request failed")

    text = response.text or ""
    upper_text = text.upper()
    if (
        "FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED!" in upper_text
        or "THE PAGE CANNOT BE ACCESSED!" in upper_text
        or "因為安全性考量，您所執行的頁面無法呈現" in text
    ):
        raise MopsBlockedError("MOPS/TWSE security page encountered")

    # MOPS emits a second nested <html> document. lxml's HTML parser stops before
    # the later cash-flow section, while BeautifulSoup preserves all iXBRL nodes.
    soup = BeautifulSoup(response.content, "html.parser")
    nodes = soup.find_all(
        lambda tag: bool(tag.name and tag.name.lower().endswith("nonfraction"))
    )
    results = []
    for node in nodes:
        name_attr = node.get("name") or ""
        text_content = node.get_text("", strip=True)
        try:
            parent_row = node.find_parent("tr")
            row_text = parent_row.get_text(" ", strip=True) if parent_row else ""
        except Exception:
            row_text = ""
        target = _match_target(name_attr, row_text, text_content)
        if not target:
            continue
        contextref = node.get("contextref") or node.get("contextRef") or ""
        # MOPS suffixes dimension/member contexts with underscores. Those rows are
        # breakdowns rather than the consolidated headline amount.
        if "_" in contextref:
            continue
        numeric_value = _parse_number(text_content)
        results.append(
            {
                "target": target,
                "contextref": contextref,
                "scaled_value": _scaled_signed_value(
                    numeric_value, node.get("sign"), node.get("scale")
                ),
            }
        )

    if not results:
        logger.info(
            "[cash-flow][no-match] stock=%s year=%s season=%s nodes=%s",
            co_id,
            year,
            season,
            len(nodes),
        )
        return pd.DataFrame()

    period = f"{year}{int(season):02d}"
    frame = pd.DataFrame(results)
    frame["period"] = period
    requested_year = str(year)
    frame["context_priority"] = frame["contextref"].apply(
        lambda context: 0
        if requested_year in str(context)
        else 1
    )
    frame["target"] = pd.Categorical(frame["target"], categories=TARGET_ORDER, ordered=True)
    frame = (
        frame.sort_values(["target", "context_priority"], kind="stable")
        .groupby("target", observed=False)
        .head(1)
    )
    pivot = frame.pivot_table(
        index="period",
        columns="target",
        values="scaled_value",
        aggfunc="first",
        observed=False,
    ).reset_index()
    pivot.columns.name = None
    pivot.insert(0, "股票代號", co_id)
    return pivot[
        ["股票代號", "period"] + [column for column in TARGET_ORDER if column in pivot.columns]
    ]


def fetch_all_cash_flows(
    year: str,
    season: str,
    delay: float = 0.5,
    progress_cb: Optional[Callable[[int, int, str, str, Optional[str]], None]] = None,
    row_cb: Optional[Callable[[str, pd.DataFrame], None]] = None,
    code_from: Optional[str] = None,
    code_to: Optional[str] = None,
    resume_after: bool = False,
    pause_every: Optional[int] = None,
    pause_seconds: float = 0.0,
) -> pd.DataFrame:
    """Fetch cash-flow statements for all or a range of listed companies."""
    from income_statement_service import fetch_all_stock_codes

    codes = fetch_all_stock_codes()
    lower = str(code_from).strip() if code_from else None
    upper = str(code_to).strip() if code_to else None
    if lower and upper and lower > upper:
        lower, upper = upper, lower
    codes = [
        code
        for code in codes
        if (not lower or (code > lower if resume_after else code >= lower))
        and (not upper or code <= upper)
    ]

    rows: list[pd.DataFrame] = []
    total = len(codes)
    for index, co_id in enumerate(codes, 1):
        try:
            if progress_cb:
                progress_cb(index, total, co_id, "start", None)
            row = fetch_cash_flow_row(co_id, year, season)
            if row.empty:
                if progress_cb:
                    progress_cb(index, total, co_id, "empty", None)
            else:
                rows.append(row)
                if row_cb:
                    row_cb(co_id, row)
                if progress_cb:
                    progress_cb(index, total, co_id, "success", None)
        except MopsBlockedError as exc:
            if progress_cb:
                progress_cb(index, total, co_id, "error", str(exc))
            raise
        except Exception as exc:
            logger.warning("[cash-flow] stock=%s failed: %s", co_id, exc)
            if progress_cb:
                progress_cb(index, total, co_id, "error", str(exc))

        time.sleep(delay)
        if (
            pause_every
            and pause_every > 0
            and pause_seconds > 0
            and index < total
            and index % pause_every == 0
        ):
            time.sleep(pause_seconds)

    if progress_cb:
        progress_cb(total, total, "", "done", None)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
