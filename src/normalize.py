"""
Phase 2 — Row classification and amount normalization.
No LLM dependency — uses rules from Phase 1 (table_detect) instead.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from .io_readers import parse_number

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class RowClassificationRules(BaseModel):
    """Rules for classifying rows as ACCOUNT, TOTAL, HEADER, or NOISE."""
    total_keywords: List[str] = Field(default_factory=lambda: [
        "summe", "gesamt", "total", "bilanzsumme", "zwischensumme",
        "jahresüberschuss", "ergebnis", "result", "subtotal", "grand total",
        "total sume", "sold final", "profit", "loss", "net income",
    ])
    header_keywords: List[str] = Field(default_factory=lambda: [
        "kontenklasse", "class", "gruppe", "group", "section",
    ])
    noise_patterns: List[str] = Field(default_factory=lambda: [
        r"^\s*$", r"^-+$", r"^=+$", r"^\*+$",
    ])
    account_number_pattern: str = r"^\d{3,6}$"
    amount_strategy: str = "end_balance"
    sign_column_rule: str = "none"
    language: str = "de"


def rules_from_detection(detection: Any) -> RowClassificationRules:
    """Build RowClassificationRules from a Phase 1 TableDetection result.

    This replaces the old Phase 2 LLM call — we reuse the hints
    that Phase 1 already provides.
    """
    hints = detection.row_type_hints if hasattr(detection, "row_type_hints") else {}
    defaults = RowClassificationRules()

    return RowClassificationRules(
        total_keywords=hints.get("total_keywords", defaults.total_keywords),
        header_keywords=hints.get("header_keywords", defaults.header_keywords),
        noise_patterns=hints.get("noise_patterns", defaults.noise_patterns),
        account_number_pattern=defaults.account_number_pattern,
        amount_strategy=getattr(detection, "amount_strategy", "end_balance"),
        sign_column_rule="none",
        language=getattr(detection, "language_guess", "de"),
    )



def apply_classification(
    df: pd.DataFrame,
    rules: RowClassificationRules,
) -> pd.DataFrame:
    """Apply row classification rules to the extracted DataFrame."""
    df = df.copy()
    row_types = []

    konto_nr_col = "konto_nr" if "konto_nr" in df.columns else None
    konto_name_col = "konto_name" if "konto_name" in df.columns else None

    for idx, row in df.iterrows():
        konto_nr = str(row.get("konto_nr", "")).strip() if konto_nr_col else ""
        konto_name = str(row.get("konto_name", "")).strip() if konto_name_col else ""
        combined_text = f"{konto_nr} {konto_name}".lower()

        # Check noise first
        is_noise = False
        for pattern in rules.noise_patterns:
            try:
                if re.match(pattern, combined_text.strip()):
                    is_noise = True
                    break
            except re.error:
                pass
        if is_noise or (not konto_nr and not konto_name):
            row_types.append("NOISE")
            continue

        # PRIORITY RULE: If the row has a valid account number, it IS an account.
        # Keywords in the account name (like "Soll", "Haben", "Summe" etc.)
        # should NOT override the account classification.
        has_valid_acct_nr = False
        try:
            if konto_nr and re.match(rules.account_number_pattern, konto_nr):
                has_valid_acct_nr = True
            elif konto_nr and re.match(r"^\d+", konto_nr):
                has_valid_acct_nr = True
        except re.error:
            if konto_nr and konto_nr.isdigit():
                has_valid_acct_nr = True

        if has_valid_acct_nr:
            row_types.append("ACCOUNT")
            continue

        # --- Only for rows WITHOUT a valid account number: check keywords ---

        # Check totals
        is_total = False
        for kw in rules.total_keywords:
            if kw.lower() in combined_text:
                is_total = True
                break
        if is_total:
            # Distinguish TOTAL from CALCULATED_RESULT
            result_keywords = [
                "jahresüberschuss", "jahresfehlbetrag", "ergebnis", "net income",
                "profit", "loss", "result", "gewinn", "verlust",
            ]
            is_result = any(rk in combined_text for rk in result_keywords)
            row_types.append("CALCULATED_RESULT" if is_result else "TOTAL")
            continue

        # Check headers
        is_header = False
        for kw in rules.header_keywords:
            if kw.lower() in combined_text:
                is_header = True
                break
        if is_header:
            row_types.append("HEADER")
            continue

        # No account number and no keyword match → treat as HEADER
        row_types.append("HEADER")

    df["row_type"] = row_types
    return df


def normalize_amounts(
    df: pd.DataFrame,
    strategy: str,
    period: Optional[str] = None,
    language_hint: str = "de",
) -> pd.DataFrame:
    """Compute amount_normalized based on strategy."""
    df = df.copy()
    locale = "de" if language_hint in ("de", "nl", "ro") else "en"

    amount_normalized = []
    amount_basis_list = []

    for idx, row in df.iterrows():
        val = None
        basis = strategy

        if strategy.startswith("use_column:"):
            col_name = strategy.replace("use_column:", "")
            # Try exact match
            if col_name in row.index:
                val = parse_number(str(row[col_name]), locale)
                basis = f"column: {col_name}"
            else:
                # Try amount_ prefix
                prefixed = f"amount_{col_name}"
                if prefixed in row.index:
                    val = parse_number(str(row[prefixed]), locale)
                    basis = f"column: {prefixed}"

        elif strategy.startswith("computed:"):
            formula = strategy.replace("computed:", "")
            if "begin+debit-credit" in formula:
                begin = _get_amount(row, ["amount_begin_balance", "amount_begin"], locale)
                debit = _get_amount(row, ["amount_debit", "amount_soll"], locale)
                credit = _get_amount(row, ["amount_credit", "amount_haben"], locale)
                begin = begin or 0.0
                debit = debit or 0.0
                credit = credit or 0.0
                val = begin + debit - credit
                basis = "computed: begin + debit - credit"
            elif "debit-credit" in formula:
                debit = _get_amount(row, ["amount_debit", "amount_soll"], locale)
                credit = _get_amount(row, ["amount_credit", "amount_haben"], locale)
                debit = debit or 0.0
                credit = credit or 0.0
                val = debit - credit
                basis = "computed: debit - credit"

        # Fallback: try common amount columns
        if val is None:
            for candidate in [
                "amount_end_balance", "amount_closing_balance", "amount_saldo",
                "amount_kum_saldo", "amount_endsaldo", "amount_balance",
            ]:
                if candidate in row.index:
                    val = parse_number(str(row[candidate]), locale)
                    if val is not None:
                        basis = f"column: {candidate}"
                        break

        # Last resort: try any amount_ column
        if val is None:
            for col in row.index:
                if col.startswith("amount_") and col != "amount_raw":
                    val = parse_number(str(row[col]), locale)
                    if val is not None:
                        basis = f"column: {col}"
                        break

        # Handle side indicator
        if val is not None and "side_indicator" in row.index:
            indicator = str(row.get("side_indicator", "")).strip().upper()
            if indicator in ("H", "HABEN", "C", "CREDIT", "CR"):
                val = -abs(val)
            elif indicator in ("S", "SOLL", "D", "DEBIT", "DR"):
                val = abs(val)

        amount_normalized.append(val)
        amount_basis_list.append(basis)

    df["amount_normalized"] = amount_normalized
    df["amount_basis"] = amount_basis_list
    return df


def _get_amount(row: pd.Series, candidates: List[str], locale: str) -> Optional[float]:
    """Try to get a parsed amount from multiple candidate column names."""
    for col in candidates:
        if col in row.index:
            val = parse_number(str(row[col]), locale)
            if val is not None:
                return val
    return None


def deduplicate_accounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Disabled deduplication: returns all detected account rows as separate entries.
    Flags each row with an empty flag list.
    """
    df = df.copy()
    if "flags" not in df.columns:
        df["flags"] = "[]"
    return df
