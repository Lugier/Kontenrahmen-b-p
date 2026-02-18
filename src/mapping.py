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
# Prompts
# ---------------------------------------------------------------------------

PROMPT_ACCOUNT_MAPPER = """You are a Senior Technical Accountant and LucaNet Implementation Specialist.
Your task is to map trial balance accounts (Summen- und Saldenlisten) to the specific target positions of a customer's LucaNet chart of accounts.

CONTEXT:
- You are looking at a batch of accounts.
- Neighboring accounts are provided to help you understand the grouping logic (e.g., if many accounts in a range are 'Material expenses', others in that range likely are too).
- Account numbers (Konto-Nr) often follow a logic (e.g., SKR03/04 in Germany: 0=Anlagevermögen, 1=Bank/Kasse/Umlauf, 3=Verbindlichkeiten, 4=Umsatz, 6=Kosten).

INSTRUCTIONS:
1. For each account, analyze the `konto_name` and `konto_nr`.
2. Look at the `hierarchy_path` of potential targets in the whitelist to find the best fit.
3. Assign a `target_id` from the provided whitelist.
4. If no good match exists, use "UNMAPPED".
5. Provide a `confidence` score (0.0 to 1.0).
6. Provide a `rationale_short` explaining WHY you chose this mapping (e.g., "Standard bank account range", "Keyword 'Miete' matches 'Rent expenses'").
7. Use `flags` for warnings: "UNCERTAIN_MAPPING", "AMBIGUOUS_NAME", "RANGE_OUTLIER".

CRITICAL RULE:
- ONLY use `target_id` values that are present in the whitelist.
- If the account number logic seems to change, note it in the flags.

Respond ONLY with valid JSON."""

MAPPING_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "konto_key": {"type": "string", "description": "The unique key of the account to map"},
                    "target_id": {"type": "string", "description": "The target_id from the whitelist"},
                    "target_name": {"type": "string"},
                    "target_class": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "rationale_short": {"type": "string"},
                    "flags": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["konto_key", "target_id", "confidence"],
            },
        }
    },
    "required": ["results"],
}


def _guess_class_from_nr(konto_nr: str) -> str:
    """Heuristic for German SKR03/04 account classes."""
    if not konto_nr or not konto_nr[0].isdigit():
        return "UNKNOWN"
    first = konto_nr[0]
    mapping = {
        "0": "AKTIVA (Anlagevermögen)",
        "1": "AKTIVA (Umlaufvermögen / Bank)",
        "2": "PASSIVA (Eigenkapital / Rückstellungen)",
        "3": "PASSIVA (Verbindlichkeiten)",
        "4": "ERTRAG (Umsatzerlöse)",
        "5": "AUFWAND (Material/Wareneinkauf)",
        "6": "AUFWAND (Betriebliche Kosten)",
        "7": "AUFWAND/ERTRAG (Sonstige)",
        "8": "ERTRAG (Erlöse)",
        "9": "STATISTISCH",
    }
    return mapping.get(first, "UNKNOWN")


def map_accounts(
    llm_client: Any,
    accounts_df: pd.DataFrame,
    targets: List[TargetPosition],
    batch_size: int = 50,
    reasoning_effort: Optional[str] = None,
) -> pd.DataFrame:
    """Map accounts to target positions using LLM with internal deduplication."""
    df = accounts_df.copy()
    accounts_only = df[df["row_type"] == "ACCOUNT"]

    if accounts_only.empty:
        logger.warning("No ACCOUNT rows to map")
        for col in ["target_overpos_id", "target_overpos_name", "target_class",
                     "confidence", "rationale_short", "mapping_flags"]:
            df[col] = ""
        return df

    whitelist = targets_to_whitelist(targets)

    # Internal Deduplication: Only map unique (konto_nr, konto_name) pairs
    # This saves tokens and ensures consistency across sheets/periods
    unique_accounts = accounts_only.drop_duplicates(subset=["konto_nr", "konto_name"]).copy()
    unique_accounts = unique_accounts.sort_values(by="konto_nr") # Sort helps LLM see ranges better
    
    logger.info(f"Deduplicated {len(accounts_only)} accounts to {len(unique_accounts)} unique accounts for mapping.")

    # Build account items for batching with neighbor context
    full_items = []
    accounts_list = unique_accounts.to_dict("records")
    for idx, row in enumerate(accounts_list):
        # Get neighbors (previous and next) as context from the UNIQUE list
        prev_names = [accounts_list[i].get("konto_name") for i in range(max(0, idx-3), idx)]
        next_names = [accounts_list[i].get("konto_name") for i in range(idx+1, min(len(accounts_list), idx+4))]

        full_items.append({
            "konto_key": f"{row.get('konto_nr', idx)}", # Simplified key
            "konto_nr": str(row.get("konto_nr", "")),
            "konto_name": str(row.get("konto_name", "")),
            "amount": row.get("amount_normalized"),
            "class_heuristic": _guess_class_from_nr(str(row.get("konto_nr", ""))),
            "_context": {
                "prev_accounts": prev_names,
                "next_accounts": next_names
            }
        })

    # Batch map
    all_results: List[Dict] = []
    n_batches = (len(full_items) + batch_size - 1) // batch_size

    for i in range(0, len(full_items), batch_size):
        batch = full_items[i:i + batch_size]
        curr_batch_idx = i // batch_size + 1

        prompt = (
            f"## LucaNet Target Positions (Whitelist):\n"
            f"{json.dumps(whitelist, ensure_ascii=False, indent=1)}\n\n"
            f"## Unique Accounts to Map (Batch {curr_batch_idx}/{n_batches}):\n"
            f"{json.dumps(batch, ensure_ascii=False, indent=1)}\n\n"
            "Task: Map each account in the list to the most appropriate target_id from the whitelist.\n"
            "Use the neighbor context ('_context') to ensure consistency in mapping account ranges."
        )

        logger.info(f"Mapping unique batch {curr_batch_idx}/{n_batches} ({len(batch)} accounts)...")

        result = llm_client.call(
            prompt=prompt,
            system_prompt=PROMPT_ACCOUNT_MAPPER,
            json_schema=MAPPING_SCHEMA,
            temperature=0.0,
            schema_version="mapping_v4_dedup",
            reasoning_effort=reasoning_effort,
        )
        if isinstance(result, dict) and "results" in result:
            all_results.extend(result["results"])
        else:
            logger.error(f"Failed to get results for batch {curr_batch_idx}")

    # Broadcast results: Create a lookup map for unique pairs
    # Key is (konto_nr, konto_name)
    lookup = {}
    for r in all_results:
        # We need to find which unique account this result belongs to
        # Since we used konto_nr as konto_key in the items
        lookup[str(r.get("konto_key"))] = r

    # Apply to the FULL dataframe
    mapping_cols = {
        "target_overpos_id": [], "target_overpos_name": [], "target_class": [],
        "confidence": [], "rationale_short": [], "mapping_flags": [],
    }

    for _, row in df.iterrows():
        if row["row_type"] != "ACCOUNT":
            for col in mapping_cols: mapping_cols[col].append("")
            continue
            
        nr_key = str(row.get("konto_nr", ""))
        r = lookup.get(nr_key, {})
        
        mapping_cols["target_overpos_id"].append(r.get("target_id", "UNMAPPED"))
        mapping_cols["target_overpos_name"].append(r.get("target_name", ""))
        mapping_cols["target_class"].append(r.get("target_class", ""))
        mapping_cols["confidence"].append(r.get("confidence", 0.0))
        mapping_cols["rationale_short"].append(r.get("rationale_short", ""))
        mapping_cols["mapping_flags"].append(json.dumps(r.get("flags", [])))

    for col, vals in mapping_cols.items():
        df[col] = vals

    return df
