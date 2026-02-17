"""
Phase 4 — LLM-based account mapping to target positions.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import pandas as pd

from .targets import TargetPosition, targets_to_whitelist

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

PROMPT_ACCOUNT_MAPPER = """You are an expert accountant mapping trial balance accounts to a standardized chart of accounts (LucaNet positions).

For each account, assign the best matching target position from the whitelist.

RULES:
- output MUST use a target_id from the whitelist, or "UNMAPPED"
- confidence: 0.0-1.0 (1.0 = certain, <0.5 = unsure)
- flags: array of strings like "NEEDS_REVIEW", "FOREIGN_LANGUAGE", "SIGN_CONVENTION_UNCERTAIN"
- rationale_short: one sentence explaining the mapping

Respond with JSON: {"results": [{"konto_key": ..., "target_id": ..., "target_name": ..., "target_class": ..., "confidence": ..., "rationale_short": ..., "flags": [...]}]}"""

MAPPING_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "konto_key": {"type": "string"},
                    "target_id": {"type": "string"},
                    "target_name": {"type": "string"},
                    "target_class": {"type": "string"},
                    "confidence": {"type": "number"},
                    "rationale_short": {"type": "string"},
                    "flags": {"type": "array", "items": {"type": "string"}},
                },
            },
        }
    },
}


def map_accounts(
    llm_client: Any,
    accounts_df: pd.DataFrame,
    targets: List[TargetPosition],
    batch_size: int = 50,
    reasoning_effort: Optional[str] = None,
) -> pd.DataFrame:
    """Map accounts to target positions using LLM."""
    df = accounts_df.copy()
    accounts_only = df[df["row_type"] == "ACCOUNT"]

    if accounts_only.empty:
        logger.warning("No ACCOUNT rows to map")
        for col in ["target_overpos_id", "target_overpos_name", "target_class",
                     "confidence", "rationale_short"]:
            df[col] = ""
        return df

    whitelist = targets_to_whitelist(targets)

    # Build account items for batching
    items = []
    for _, row in accounts_only.iterrows():
        items.append({
            "konto_key": f"{row.get('konto_nr', '')}",
            "konto_nr": str(row.get("konto_nr", "")),
            "konto_name": str(row.get("konto_name", "")),
            "amount": row.get("amount_normalized"),
        })

    # Batch map
    all_results: List[Dict] = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        prompt = (
            f"## Target Positions (whitelist):\n{json.dumps(whitelist, ensure_ascii=False)}\n\n"
            f"## Accounts to map:\n{json.dumps(batch, ensure_ascii=False, default=str)}\n\n"
            "Map each account to the best target. Respond with JSON."
        )
        # Progress logging
        n_batches = (len(accounts_only) + batch_size - 1) // batch_size
        logger.info(f"Mapping batch {i//batch_size + 1}/{n_batches} ({len(batch)} accounts)...")

        result = llm_client.call(
            prompt=prompt,
            system_prompt=PROMPT_ACCOUNT_MAPPER,
            json_schema=MAPPING_SCHEMA,
            temperature=0.1,
            schema_version="mapping_v2",
            reasoning_effort=reasoning_effort,
        )
        if isinstance(result, dict) and "results" in result:
            all_results.extend(result["results"])

    # Merge results into DataFrame
    result_map = {r["konto_key"]: r for r in all_results}

    mapping_cols = {
        "target_overpos_id": [], "target_overpos_name": [], "target_class": [],
        "confidence": [], "rationale_short": [], "mapping_flags": [],
    }

    for _, row in df.iterrows():
        key = str(row.get("konto_nr", ""))
        r = result_map.get(key, {})
        mapping_cols["target_overpos_id"].append(r.get("target_id", "UNMAPPED"))
        mapping_cols["target_overpos_name"].append(r.get("target_name", ""))
        mapping_cols["target_class"].append(r.get("target_class", ""))
        mapping_cols["confidence"].append(r.get("confidence", 0.0))
        mapping_cols["rationale_short"].append(r.get("rationale_short", ""))
        mapping_cols["mapping_flags"].append(json.dumps(r.get("flags", [])))

    for col, vals in mapping_cols.items():
        df[col] = vals

    return df
