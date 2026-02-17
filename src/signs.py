"""
Phase 5 — Sign convention normalization (deterministic).

No LLM dependency — uses sign_convention from Phase 1 TableDetection.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict

import pandas as pd

logger = logging.getLogger(__name__)


def normalize_signs(
    df: pd.DataFrame,
    sign_convention: str = "standard",
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """Apply sign normalization based on convention from Phase 1.

    Target: all amounts should be positive absolute values.

    Args:
        df: DataFrame with row_type, target_class, amount_normalized columns.
        sign_convention: One of:
            - "standard": amounts already positive, no flip needed
            - "assets_negative": flip sign of AKTIVA accounts
            - "liabilities_negative": flip sign of PASSIVA accounts
            - "debit_positive_credit_negative": no flip (debit=asset convention)

    Returns:
        (normalized_df, sign_info_dict)
    """
    df = df.copy()
    sign_info = {"convention": sign_convention, "flips_applied": 0}

    if sign_convention == "standard":
        sign_info["notes"] = "No sign corrections needed"
        return df, sign_info

    accounts_mask = df["row_type"] == "ACCOUNT"

    # Map convention to which classes need sign flip
    flip_classes = {
        "assets_negative": ["AKTIVA"],
        "liabilities_negative": ["PASSIVA"],
        "debit_positive_credit_negative": [],  # no flip needed
    }.get(sign_convention, [])

    if not flip_classes:
        sign_info["notes"] = f"Convention '{sign_convention}' — no flips required"
        return df, sign_info

    flips = 0
    for idx, row in df[accounts_mask].iterrows():
        tc = str(row.get("target_class", ""))
        amt = row.get("amount_normalized")
        if tc in flip_classes and amt is not None:
            df.at[idx, "amount_normalized"] = -amt
            flips += 1

    sign_info["flips_applied"] = flips
    sign_info["notes"] = f"Flipped {flips} accounts in classes {flip_classes}"
    logger.info("Sign normalization: %s", sign_info["notes"])

    return df, sign_info
