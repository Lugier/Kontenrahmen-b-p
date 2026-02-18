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

PROMPT_ACCOUNT_MAPPER = """You are a Lead Financial Auditor and LucaNet Implementation Expert.
You are tasked with mapping a client's "Summen- und Saldenliste" (Trial Balance) to their standardized Group Chart of Accounts (LucaNet).

### INPUT CONTEXT
- You will receive a **batch of accounts** from the trial balance.
- The accounts are usually **sorted by Account Number**.
- **Chart of Accounts Logic**: 
  - The client might use a standard frame (like SKR03/04) OR a **custom/proprietary** chart of accounts.
  - Do NOT assume standard SKR logic unless the pattern clearly matches.
  - Instead, **analyze the batch for internal patterns**: identifying blocks of similar accounts (e.g., a range of valid IDs followed by a range of expense accounts).

### YOUR TASK
1. **Analyze the Batch Context**: Look at the sequence. If you see a block of accounts (e.g. 4000-4050) that are all "Sales/Revenue", then a new account "4025 diff" in between is likely also Revenue.
2. **Map to Target**: Select the *most specific* `target_id` from the provided **Whitelist**.
3. **Handle Uncertainty**:
   - If the account is ambiguous (e.g., "Verrechnungskonto"), look at neighbors in the batch to guess the context (Assets vs Expenses).
   - If absolutely no fit is found, use "UNMAPPED".
4. **Validation**:
   - Do NOT invent target IDs. Use ONLY keys from the whitelist.

### OUTPUT FORMAT
Respond with a JSON object containing a "results" array.
Each result MUST include:
- `konto_key`: The ID provided in the input.
- `target_id`: The chosen ID from the whitelist.
- `confidence`: 1.0 (Certain) to 0.0 (Guess).
- `rationale_short`: Brief professional reasoning (e.g. "Context implies personnel expense block").

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
                     "confidence", "rationale_short", "mapping_flags"]:
            df[col] = ""
        return df

    whitelist = targets_to_whitelist(targets)

    # Prepare items for batching
    # We send the accounts in their original order (sorted by extraction)
    # This preserves the natural "Block" context for the LLM.
    items = []
    for idx, row in accounts_only.iterrows():
        items.append({
            "konto_key": f"{row.get('konto_nr', idx)}", 
            "konto_nr": str(row.get("konto_nr", "")),
            "konto_name": str(row.get("konto_name", "")),
            "amount": row.get("amount_normalized"),
            # No manual context/heuristics - rely on LLM reading the batch natively
        })

    # Batch map
    all_results: List[Dict] = []
    n_batches = (len(items) + batch_size - 1) // batch_size

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        curr_batch_idx = i // batch_size + 1

        prompt = (
            f"## LucaNet Target Positions (Whitelist):\n"
            f"{json.dumps(whitelist, ensure_ascii=False, indent=1)}\n\n"
            f"## Trial Balance Accounts (Batch {curr_batch_idx}/{n_batches}):\n"
            f"{json.dumps(batch, ensure_ascii=False, indent=1)}\n\n"
            "Task: Map the accounts above to the Target Positions."
        )

        logger.info(f"Mapping batch {curr_batch_idx}/{n_batches} ({len(batch)} accounts)...")

        result = llm_client.call(
            prompt=prompt,
            system_prompt=PROMPT_ACCOUNT_MAPPER,
            json_schema=MAPPING_SCHEMA,
            temperature=0.0,
            schema_version="mapping_v5_pro_prompt",
            reasoning_effort=reasoning_effort,
        )
        if isinstance(result, dict) and "results" in result:
            all_results.extend(result["results"])
        else:
            logger.error(f"Failed to get results for batch {curr_batch_idx}")

    # Merge results into DataFrame
    result_map = {r["konto_key"]: r for r in all_results}

    mapping_cols = {
        "target_overpos_id": [], "target_overpos_name": [], "target_class": [],
        "confidence": [], "rationale_short": [], "mapping_flags": [],
    }

    for idx, row in df.iterrows():
        if row["row_type"] != "ACCOUNT":
            for col in mapping_cols: mapping_cols[col].append("")
            continue
            
        # Use the same key generation logic as above
        nr_as_key = str(row.get("konto_nr", idx))
        
        # Try finding by explicit key first, fallback to Nr if implicit
        r = result_map.get(nr_as_key, {})
        if not r and "konto_nr" in row:
             r = result_map.get(str(row["konto_nr"]), {})

        mapping_cols["target_overpos_id"].append(r.get("target_id", "UNMAPPED"))
        mapping_cols["target_overpos_name"].append(r.get("target_name", ""))
        mapping_cols["target_class"].append(r.get("target_class", ""))
        mapping_cols["confidence"].append(r.get("confidence", 0.0))
        mapping_cols["rationale_short"].append(r.get("rationale_short", ""))
        mapping_cols["mapping_flags"].append(json.dumps(r.get("flags", [])))

    for col, vals in mapping_cols.items():
        df[col] = vals

    return df
