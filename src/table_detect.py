"""
Phase 1 — LLM-based table and header detection in SuSa sheets.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field

from .io_readers import make_sheet_snapshot, column_profile

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ColumnRoles(BaseModel):
    account_number_col: Optional[int] = None
    account_name_col: Optional[int] = None
    amount_cols: Dict[str, Any] = Field(default_factory=dict)
    side_indicator_col: Optional[int] = None


class TableDetection(BaseModel):
    table_id: str = "main"
    header_rows: List[int] = Field(default_factory=list)
    start_row: int = 0
    end_row: int = 0
    start_col: int = 0
    end_col: int = 0
    column_roles: ColumnRoles = Field(default_factory=ColumnRoles)
    amount_strategy: str = "end_balance"
    row_type_hints: Dict[str, Any] = Field(default_factory=dict)
    sign_convention: str = "standard"  # standard | assets_negative | liabilities_negative
    language_guess: str = "de"
    confidence: float = 0.5


# ---------------------------------------------------------------------------
# JSON Schema for LLM
# ---------------------------------------------------------------------------

TABLE_DETECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "tables": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "header_rows": {"type": "array", "items": {"type": "integer"}},
                    "start_row": {"type": "integer"},
                    "end_row": {"type": "integer"},
                    "start_col": {"type": "integer"},
                    "end_col": {"type": "integer"},
                    "column_roles": {
                        "type": "object",
                        "properties": {
                            "account_number_col": {"type": ["integer", "null"]},
                            "account_name_col": {"type": ["integer", "null"]},
                            "amount_cols": {"type": "object"},
                            "side_indicator_col": {"type": ["integer", "null"]},
                        },
                    },
                    "amount_strategy": {"type": "string"},
                    "row_type_hints": {"type": "object"},
                    "sign_convention": {"type": "string"},
                    "language_guess": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": ["table_id", "header_rows", "start_row", "end_row",
                             "start_col", "end_col", "column_roles"],
            },
        }
    },
    "required": ["tables"],
}

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

PROMPT_TABLE_DETECTOR = """You are a specialist at analyzing trial balance (SuSa / Summen- und Saldenliste) spreadsheets.

Given a snapshot of a spreadsheet sheet (in CSV format) and column statistics, identify the accounting data table(s).
The snapshot likely contains the first 1000 rows of the sheet, covering the headers and a significant portion of the data.

CRITICAL:
- "Summen- und Saldenlisten" often consist of **multiple sections** (e.g., Asset Accounts, Liability Accounts, Expense Accounts, Revenue Accounts).
- These sections are often separated by "Summe" (Total) rows, empty lines, and new section headers (e.g. "Klasse 5...").
- **You must define the 'main' table to encompass ALL these sections.** 
- Do **NOT** stop at the first "Summe" row if the data continues afterwards with the same column structure.
- The `end_row` should be the very last row of accounting data in the file (or the snapshot limit).

TASK:
1. Find the start of the accounting data.
2. Find the END of the accounting data (look past intermediate totals!).
3. Identify column roles.

For each table found, provide:
- table_id: unique identifier (e.g., "main")
- header_rows: list of 0-based row indices that form the header
- start_row, end_row: 0-based row range for DATA (excluding headers). 
  - **start_row**: The first row containing an actual account (after headers).
  - **end_row**: The LAST row containing an actual account. 
  - Do NOT set end_row to an intermediate total line. Look for the final total or the end of the file.
- start_col, end_col: 0-based column range
- column_roles:
  - account_number_col: column index with account numbers (3-6 digit codes)
  - account_name_col: column index with account names/descriptions
  - amount_cols: dict mapping role to column index. Possible roles:
    "end_balance" (prefer this one), "closing_balance", "saldo", "begin_balance",
    "debit", "credit", "kum_saldo"
  - side_indicator_col: column with S/H or Debit/Credit indicators (null if none)
- amount_strategy: prioritized rule to compute the normalized amount.
- row_type_hints: keywords to identify total/header rows.
- sign_convention: e.g. "standard", "assets_negative", etc.
- language_guess: e.g. "de"

Respond with valid JSON only. The CSV uses ';' as separator."""


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------

def detect_tables(
    llm_client: Any,
    sheet_name: str,
    df: pd.DataFrame,
) -> List[TableDetection]:
    """Use LLM to detect accounting tables in a sheet."""
    # New logic: CSV format, up to 1000 rows
    snapshot = make_sheet_snapshot(df, max_rows=1000, max_cols=25, format="csv")
    profile = column_profile(df)

    prompt = f"""## Sheet: "{sheet_name}"

### Snapshot (CSV format, first ~1000 rows):
Note: Empty values are consecutive delimiters (;;).
{snapshot}

### Column Profiles:
{json.dumps(profile, indent=2, ensure_ascii=False)}

### Sheet dimensions: {df.shape[0]} rows × {df.shape[1]} columns

Analyze this sheet and return the table detection result."""

    result = llm_client.call(
        prompt=prompt,
        system_prompt=PROMPT_TABLE_DETECTOR,
        json_schema=TABLE_DETECTION_SCHEMA,
        temperature=0.1,
        schema_version="table_detect_v2",
    )

    if isinstance(result, dict) and "_parse_error" in result:
        logger.warning("Failed to parse table detection result for sheet '%s'", sheet_name)
        return []

    tables = []
    for t in result.get("tables", []):
        try:
            cr = t.get("column_roles", {})
            col_roles = ColumnRoles(
                account_number_col=cr.get("account_number_col"),
                account_name_col=cr.get("account_name_col"),
                amount_cols=cr.get("amount_cols", {}),
                side_indicator_col=cr.get("side_indicator_col"),
            )
            det = TableDetection(
                table_id=t.get("table_id", "main"),
                header_rows=t.get("header_rows", []),
                start_row=t.get("start_row", 0),
                end_row=min(t.get("end_row", df.shape[0] - 1), df.shape[0] - 1),
                start_col=t.get("start_col", 0),
                end_col=min(t.get("end_col", df.shape[1] - 1), df.shape[1] - 1),
                column_roles=col_roles,
                amount_strategy=t.get("amount_strategy", "end_balance"),
                row_type_hints=t.get("row_type_hints", {}),
                sign_convention=t.get("sign_convention", "standard"),
                language_guess=t.get("language_guess", "de"),
                confidence=t.get("confidence", 0.5),
            )
            tables.append(det)
        except Exception as e:
            logger.warning("Error parsing table detection entry: %s", e)

    return tables


def extract_by_detection(
    df: pd.DataFrame,
    detection: TableDetection,
) -> pd.DataFrame:
    """Extract and standardize data from a DataFrame using table detection results."""
    # Slice the table area
    data = df.iloc[
        detection.start_row:detection.end_row + 1,
        detection.start_col:detection.end_col + 1,
    ].copy()
    data = data.reset_index(drop=True)

    # Adjust column indices relative to start_col
    offset = detection.start_col
    roles = detection.column_roles

    result = pd.DataFrame()
    result["_original_row"] = range(
        detection.start_row, detection.start_row + len(data)
    )

    if roles.account_number_col is not None:
        adj = roles.account_number_col - offset
        if 0 <= adj < data.shape[1]:
            result["konto_nr"] = data.iloc[:, adj].astype(str).str.strip()

    if roles.account_name_col is not None:
        adj = roles.account_name_col - offset
        if 0 <= adj < data.shape[1]:
            result["konto_name"] = data.iloc[:, adj].astype(str).str.strip()

    # Amount columns
    for role, col_idx in roles.amount_cols.items():
        if isinstance(col_idx, int):
            adj = col_idx - offset
            if 0 <= adj < data.shape[1]:
                result[f"amount_{role}"] = data.iloc[:, adj]
        elif isinstance(col_idx, list):
            for i, ci in enumerate(col_idx):
                adj = ci - offset
                if 0 <= adj < data.shape[1]:
                    result[f"amount_{role}_{i}"] = data.iloc[:, adj]

    if roles.side_indicator_col is not None:
        adj = roles.side_indicator_col - offset
        if 0 <= adj < data.shape[1]:
            result["side_indicator"] = data.iloc[:, adj].astype(str).str.strip()

    return result
