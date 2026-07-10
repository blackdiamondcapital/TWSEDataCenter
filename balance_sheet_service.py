# -*- coding: utf-8 -*-
"""Service functions for fetching Taiwan stock balance sheet data from MOPS.

This module mirrors the structure of `income_statement_service.py`.

Main entry points:

- fetch_balance_sheet_row(co_id: str, year: str, season: str) -> pd.DataFrame
- fetch_all_balance_sheets(year: str, season: str, ...) -> pd.DataFrame
"""

import logging
import time
from io import BytesIO
from typing import Callable, Optional

import pandas as pd
import requests
from lxml import etree

logger = logging.getLogger(__name__)


class MopsBlockedError(Exception):
    pass


# Balance sheet mapping (extracted from the standalone comprehensive_balance_sheet.py)
TARGET_KEYWORDS = {
    "CashAndCashEquivalents": ["ifrs-full:CashAndCashEquivalents"],
    "CurrentFinancialAssetsAtFairValueThroughProfitOrLoss": [
        "ifrs-full:CurrentFinancialAssetsAtFairValueThroughProfitOrLoss"
    ],
    "CurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome": [
        "ifrs-full:CurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome"
    ],
    "CurrentFinancialAssetsAtAmortisedCost": [
        "ifrs-full:CurrentFinancialAssetsAtAmortisedCost"
    ],
    "OtherCurrentFinancialAssets": ["ifrs-full:OtherCurrentFinancialAssets"],
    "CurrentContractAssets": ["ifrs-full:CurrentContractAssets"],
    "AccountsReceivableNet": ["ifrs-full:TradeAndOtherCurrentReceivables"],
    "AccountsReceivableDueFromRelatedPartiesNet": [
        "ifrs-full:TradeAndOtherCurrentReceivablesDueFromRelatedParties"
    ],
    "OtherCurrentReceivables": ["ifrs-full:OtherCurrentReceivables"],
    "Inventories": ["ifrs-full:Inventories"],
    "CurrentPrepayments": ["ifrs-full:CurrentPrepayments"],
    "CurrentDeferredTaxAssets": ["ifrs-full:CurrentDeferredTaxAssets"],
    "NoncurrentAssetsHeldForSaleNet": ["ifrs-full:NoncurrentAssetsHeldForSale"],
    "OtherCurrentAssets": ["ifrs-full:OtherCurrentAssets"],
    "OtherCurrentAssetsOthers": ["tifrs-bsci-ci:OtherCurrentAssetsOthers"],
    "CurrentAssets": ["ifrs-full:CurrentAssets"],
    "CurrentLiabilities": ["ifrs-full:CurrentLiabilities"],
    "NoncurrentFinancialAssetsAtFairValueThroughProfitOrLoss": [
        "ifrs-full:NoncurrentFinancialAssetsAtFairValueThroughProfitOrLoss"
    ],
    "NoncurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome": [
        "ifrs-full:NoncurrentFinancialAssetsAtFairValueThroughOtherComprehensiveIncome"
    ],
    "NoncurrentFinancialAssetsAtAmortisedCost": [
        "ifrs-full:NoncurrentFinancialAssetsAtAmortisedCost"
    ],
    "NoncurrentContractAssets": ["ifrs-full:NoncurrentContractAssets"],
    "InvestmentAccountedForUsingEquityMethod": [
        "ifrs-full:InvestmentsAccountedForUsingEquityMethod"
    ],
    "PropertyPlantAndEquipment": ["ifrs-full:PropertyPlantAndEquipment"],
    "RightofuseAssets": ["ifrs-full:RightofuseAssets"],
    "InvestmentProperty": ["ifrs-full:InvestmentProperty"],
    "IntangibleAssetsAndGoodwill": ["ifrs-full:IntangibleAssets"],
    "LicencesAndFranchises": ["tifrs-bsci-ci:LicencesAndFranchises"],
    "Goodwill": ["ifrs-full:Goodwill"],
    "OtherIntangibleAssets": ["ifrs-full:OtherIntangibleAssets"],
    "DeferredTaxAssets": ["ifrs-full:DeferredTaxAssets"],
    "OtherNoncurrentAssets": ["ifrs-full:OtherNoncurrentAssets"],
    "NoncurrentAssetsRecognisedAsIncrementalCostsToObtainContractWithCustomers": [
        "ifrs-full:NoncurrentAssetsRecognisedAsIncrementalCostsToObtainContractWithCustomers"
    ],
    "RecognisedAssetsDefinedBenefitPlan": [
        "ifrs-full:RecognisedAssetsDefinedBenefitPlan"
    ],
    "OtherNoncurrentFinancialAssets": ["ifrs-full:OtherNoncurrentFinancialAssets"],
    "OtherNoncurrentAssetsOthers": ["tifrs-bsci-ci:OtherNoncurrentAssetsOthers"],
    "NoncurrentAssets": ["ifrs-full:NoncurrentAssets"],
    "Assets": ["ifrs-full:Assets"],
    "Liabilities": ["ifrs-full:Liabilities"],
    "ShorttermBorrowings": ["ifrs-full:ShorttermBorrowings"],
    "CurrentCommercialPapersIssuedAndCurrentPortionOfNoncurrentCommercialPapersIssued": [
        "ifrs-full:CurrentCommercialPapersIssuedAndCurrentPortionOfNoncurrentCommercialPapersIssued"
    ],
    "CurrentContractLiabilities": ["ifrs-full:CurrentContractLiabilities"],
    "TradeAndOtherCurrentPayablesToTradeSuppliers": [
        "ifrs-full:TradeAndOtherCurrentPayablesToTradeSuppliers"
    ],
    "TradeAndOtherCurrentPayablesToRelatedParties": [
        "ifrs-full:TradeAndOtherCurrentPayablesToRelatedParties"
    ],
    "OtherCurrentPayables": ["ifrs-full:OtherCurrentPayables"],
    "OtherPayablesOthers": ["tifrs-bsci-ci:OtherPayablesOthers"],
    "CurrentTaxLiabilities": ["ifrs-full:CurrentTaxLiabilities"],
    "CurrentProvisions": ["ifrs-full:CurrentProvisions"],
    "CurrentLeaseLiabilities": ["tifrs-bsci-ci:CurrentLeaseLiabilities"],
    "OtherCurrentLiabilities": ["ifrs-full:OtherCurrentLiabilities"],
    "CurrentAdvances": ["ifrs-full:CurrentAdvances"],
    "LongtermLiabilitiesCurrentPortion": [
        "tifrs-bsci-ci:LongtermLiabilitiesCurrentPortion"
    ],
    "OtherCurrentLiabilitiesOthers": ["tifrs-bsci-ci:OtherCurrentLiabilitiesOthers"],
    "NoncurrentFinancialLiabilitiesAtFairValueThroughProfitOrLoss": [
        "ifrs-full:NoncurrentFinancialLiabilitiesAtFairValueThroughProfitOrLoss"
    ],
    "NoncurrentContractLiabilities": ["ifrs-full:NoncurrentContractLiabilities"],
    "NoncurrentPortionOfNoncurrentBondsIssued": [
        "ifrs-full:NoncurrentPortionOfNoncurrentBondsIssued"
    ],
    "LongtermBorrowings": ["ifrs-full:LongtermBorrowings"],
    "NoncurrentProvisions": ["ifrs-full:NoncurrentProvisions"],
    "DeferredTaxLiabilities": ["ifrs-full:DeferredTaxLiabilities"],
    "NoncurrentFinanceLeaseLiabilities": [
        "ifrs-full:NoncurrentFinanceLeaseLiabilities"
    ],
    "OtherNoncurrentLiabilities": ["ifrs-full:OtherNoncurrentLiabilities"],
    "NoncurrentRecognisedLiabilitiesDefinedBenefitPlan": [
        "ifrs-full:NoncurrentRecognisedLiabilitiesDefinedBenefitPlan"
    ],
    "GuaranteeDepositsReceived": ["tifrs-bsci-ci:GuaranteeDepositsReceived"],
    "OtherNoncurrentLiabilitiesOthers": [
        "tifrs-bsci-ci:OtherNoncurrentLiabilitiesOthers"
    ],
    "OrdinaryShare": ["tifrs-bsci-ci:OrdinaryShare"],
    "CapitalReserve": ["ifrs-full:CapitalReserve"],
    "StatutoryReserve": ["ifrs-full:StatutoryReserve"],
    "SpecialReserve": ["tifrs-bsci-ci:SpecialReserve"],
    "UnappropriatedRetainedEarningsAaccumulatedDeficit": [
        "tifrs-bsci-ci:UnappropriatedRetainedEarningsAaccumulatedDeficit"
    ],
    "RetainedEarnings": ["ifrs-full:RetainedEarnings"],
    "OtherEquityInterest": ["ifrs-full:OtherEquityInterest"],
    "TreasuryShares": ["ifrs-full:TreasuryShares"],
    "EquityAttributableToOwnersOfParent": [
        "ifrs-full:EquityAttributableToOwnersOfParent"
    ],
    "NoncontrollingInterests": ["ifrs-full:NoncontrollingInterests"],
    "EquityAndLiabilities": ["ifrs-full:EquityAndLiabilities"],
}


MAIN_CONCEPTS = {k: v for k, v in TARGET_KEYWORDS.items()}


TARGET_ORDER = list(TARGET_KEYWORDS.keys())


def _build_mops_url(co_id: str, year: str, season: str, *, host: str) -> str:
    return (
        f"https://{host}/server-java/t164sb01?step=1"
        f"&CO_ID={co_id}&SYEAR={year}&SSEASON={season}&REPORT_ID=C"
    )


def _is_connection_issue(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "failed to resolve" in msg
        or "nodatename nor servname" in msg
        or "name resolution" in msg
        or "gaierror" in msg
        or "connection reset" in msg
        or "connection aborted" in msg
        or "max retries exceeded" in msg
        or "timed out" in msg
    )


def _parse_number(s: str):
    if not s:
        return None
    s2 = s.replace(",", "").replace("(", "-").replace(")", "").strip()
    try:
        if "." in s2:
            return float(s2)
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
        return v * (10**s)
    except Exception:
        return v


def _is_main_concept(row) -> bool:
    concepts = MAIN_CONCEPTS.get(row["target"], [])
    name_attr = row["name_attr"]
    for c in concepts:
        if name_attr == c or name_attr.startswith(f"{c}-"):
            return True
    return False


def fetch_balance_sheet_row(co_id: str, year: str, season: str) -> pd.DataFrame:
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

    resp = None
    last_exc: Exception | None = None
    for host in ("mopsov.twse.com.tw", "mops.twse.com.tw"):
        url = _build_mops_url(co_id, year, season, host=host)
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
            last_exc = None
            break
        except Exception as e:
            last_exc = e
            if not _is_connection_issue(e):
                raise
            logger.warning(
                "[balance][mops] connection issue host=%s stock=%s year=%s season=%s err=%s",
                host,
                co_id,
                year,
                season,
                e,
            )
            continue

    if resp is None:
        raise last_exc if last_exc is not None else RuntimeError("MOPS request failed")

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
            "[balance][blocked] MOPS security page for stock=%s year=%s season=%s",
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
                if name_attr == kw:
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

        results.append(
            {
                "target": matched_key,
                "name_attr": name_attr,
                "value_text": value_text,
                "contextref": contextref,
                "unitref": unitref,
                "scale": scale,
                "decimals": decimals,
                "sign": sign,
            }
        )

    df = pd.DataFrame(results)
    if df.empty:
        return pd.DataFrame()

    df = df[~df["contextref"].astype(str).str.contains("_")].reset_index(drop=True)

    df["numeric_value"] = df["value_text"].apply(_parse_number)
    df["numeric_value"] = df.apply(_apply_sign, axis=1)
    df["scaled_value"] = df.apply(_apply_scale, axis=1)

    df = df[df.apply(_is_main_concept, axis=1)].reset_index(drop=True)
    if df.empty:
        return pd.DataFrame()

    period_label = f"{year}{int(season):02d}"
    df["period"] = period_label

    df["target"] = pd.Categorical(df["target"], categories=TARGET_ORDER, ordered=True)
    df = df.sort_values(["target"]).reset_index(drop=True)
    df = df.groupby("target", observed=False).head(1).reset_index(drop=True)

    pivot = df.pivot_table(
        index=["period"], columns="target", values="scaled_value", aggfunc="first"
    ).reset_index()
    pivot.columns.name = None
    pivot.insert(0, "股票代號", co_id)

    ordered_cols = ["股票代號", "period"] + [t for t in TARGET_ORDER if t in pivot.columns]
    pivot = pivot[ordered_cols]
    return pivot


def fetch_all_balance_sheets(
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
    # Reuse ISIN scraping implementation from income_statement_service.
    from income_statement_service import fetch_all_stock_codes

    codes = fetch_all_stock_codes()

    if code_from is not None or code_to is not None:
        cf = str(code_from).strip() if code_from else None
        ct = str(code_to).strip() if code_to else None
        if cf and ct and cf > ct:
            cf, ct = ct, cf
        filtered_codes: list[str] = []
        for c in codes:
            if cf:
                if resume_after:
                    if c <= cf:
                        continue
                else:
                    if c < cf:
                        continue
            if ct and c > ct:
                continue
            filtered_codes.append(c)
        codes = filtered_codes

    rows: list[pd.DataFrame] = []
    total = len(codes)

    for idx, co_id in enumerate(codes, 1):
        try:
            if progress_cb is not None:
                progress_cb(idx, total, co_id, "start", None)

            row_df = fetch_balance_sheet_row(co_id, year, season)
            if not row_df.empty:
                rows.append(row_df)
                if row_cb is not None:
                    try:
                        row_cb(co_id, row_df)
                    except Exception:
                        pass
                if progress_cb is not None:
                    progress_cb(idx, total, co_id, "success", None)
            else:
                if progress_cb is not None:
                    progress_cb(idx, total, co_id, "empty", None)
        except MopsBlockedError as e:
            if progress_cb is not None:
                progress_cb(idx, total, co_id, "error", str(e))
            raise
        except Exception as e:
            if progress_cb is not None:
                progress_cb(idx, total, co_id, "error", str(e))
            continue

        time.sleep(delay)

        if (
            pause_every
            and pause_every > 0
            and pause_seconds > 0
            and idx < total
            and (idx % pause_every == 0)
        ):
            time.sleep(pause_seconds)

    if progress_cb is not None:
        progress_cb(total, total, "", "done", None)

    if not rows:
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)
