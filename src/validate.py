"""
Phase 6 — Validation checks and LLM-based repair.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

PROMPT_REPAIR_MAPPER = """You are fixing account mappings in a trial balance.

The current mapping has validation issues (see below). Review the suspect accounts and suggest corrections.

RULES:
- Only change mappings you are confident are wrong
- Use target_ids from the provided whitelist
- Return ONLY changes, not the full list

Respond with JSON:
{"repairs": [{"konto_key": "...", "new_target_id": "...", "new_target_name": "...", "new_target_class": "...", "reason": "..."}]}"""

REPAIR_SCHEMA = {
    "type": "object",
    "properties": {
        "repairs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "konto_key": {"type": "string"},
                    "new_target_id": {"type": "string"},
                    "new_target_name": {"type": "string"},
                    "new_target_class": {"type": "string"},
                    "reason": {"type": "string"},
                },
            },
        }
    },
}


def run_checks(df: pd.DataFrame) -> Dict[str, Any]:
    """Run plausibility checks on the mapped data."""
    accounts = df[df["row_type"] == "ACCOUNT"].copy()
    checks: Dict[str, Any] = {}

    # Balance check: Aktiva vs Passiva
    has_amounts = "amount_normalized" in accounts.columns
    
    if has_amounts:
        aktiva = accounts[accounts["target_class"] == "AKTIVA"]["amount_normalized"].sum()
        passiva = accounts[accounts["target_class"] == "PASSIVA"]["amount_normalized"].sum()
        checks["aktiva_sum"] = float(aktiva) if pd.notna(aktiva) else 0
        checks["passiva_sum"] = float(passiva) if pd.notna(passiva) else 0
        checks["balance_diff"] = checks["aktiva_sum"] - checks["passiva_sum"]
        checks["balance_diff_pct"] = (
            abs(checks["balance_diff"]) / max(abs(checks["aktiva_sum"]), 1) * 100
        )
    else:
        checks["aktiva_sum"] = 0
        checks["passiva_sum"] = 0
        checks["balance_diff"] = 0
        checks["balance_diff_pct"] = 0

    # GuV check
    if has_amounts:
        ertrag = accounts[accounts["target_class"] == "ERTRAG"]["amount_normalized"].sum()
        aufwand = accounts[accounts["target_class"] == "AUFWAND"]["amount_normalized"].sum()
        checks["ertrag_sum"] = float(ertrag) if pd.notna(ertrag) else 0
        checks["aufwand_sum"] = float(aufwand) if pd.notna(aufwand) else 0
        checks["guv_result"] = checks["ertrag_sum"] - checks["aufwand_sum"]
    else:
        checks["ertrag_sum"] = 0
        checks["aufwand_sum"] = 0
        checks["guv_result"] = 0

    # Unmapped / low confidence
    unmapped = accounts[accounts.get("target_overpos_id", pd.Series()) == "UNMAPPED"]
    checks["unmapped_count"] = len(unmapped)
    if has_amounts:
        checks["unmapped_total"] = float(unmapped["amount_normalized"].sum()) if len(unmapped) > 0 else 0
    else:
        checks["unmapped_total"] = 0

    low_conf = accounts[accounts.get("confidence", pd.Series(dtype=float)) < 0.5]
    checks["low_confidence_count"] = len(low_conf)

    checks["total_accounts"] = len(accounts)
    checks["has_issues"] = (checks["balance_diff_pct"] > 10 if has_amounts else False) or checks["unmapped_count"] > 0

    return checks


def repair_mappings(
    llm_client: Any,
    df: pd.DataFrame,
    targets_whitelist: List[Dict],
    checks: Dict[str, Any],
    max_rounds: int = 2,
) -> pd.DataFrame:
    """Iteratively repair mappings using LLM."""
    df = df.copy()

    for round_num in range(1, max_rounds + 1):
        if not checks.get("has_issues"):
            logger.info("No issues found, skipping repair round %d", round_num)
            break

        # Identify suspects: unmapped + low confidence + large amounts
        accounts = df[df["row_type"] == "ACCOUNT"]
        suspects = accounts[
            (accounts.get("target_overpos_id", pd.Series()) == "UNMAPPED") |
            (accounts.get("confidence", pd.Series(dtype=float)) < 0.5)
        ].head(50)

        if suspects.empty:
            break

        suspect_items = []
        for _, row in suspects.iterrows():
            suspect_items.append({
                "konto_key": str(row.get("konto_nr", "")),
                "konto_name": str(row.get("konto_name", "")),
                "current_target": str(row.get("target_overpos_id", "")),
                "amount": row.get("amount_normalized"),
            })

        prompt = (
            f"## Validation Issues:\n{json.dumps(checks, default=str)}\n\n"
            f"## Suspect Accounts:\n{json.dumps(suspect_items, ensure_ascii=False, default=str)}\n\n"
            f"## Target Whitelist:\n{json.dumps(targets_whitelist[:200], ensure_ascii=False)}\n\n"
            "Suggest repairs."
        )

        result = llm_client.call(
            prompt=prompt,
            system_prompt=PROMPT_REPAIR_MAPPER,
            json_schema=REPAIR_SCHEMA,
            temperature=0.1,
            schema_version="repair_v1",
        )

        repairs = result.get("repairs", []) if isinstance(result, dict) else []
        if not repairs:
            logger.info("No repairs suggested in round %d", round_num)
            break

        # Apply repairs
        for repair in repairs:
            key = repair.get("konto_key", "")
            mask = df["konto_nr"].astype(str) == key
            if mask.any():
                df.loc[mask, "target_overpos_id"] = repair.get("new_target_id", "UNMAPPED")
                df.loc[mask, "target_overpos_name"] = repair.get("new_target_name", "")
                df.loc[mask, "target_class"] = repair.get("new_target_class", "")
                df.loc[mask, "confidence"] = 0.6
                df.loc[mask, "rationale_short"] = f"Repaired R{round_num}: {repair.get('reason', '')}"

        logger.info("Applied %d repairs in round %d", len(repairs), round_num)
        checks = run_checks(df)

    return df
