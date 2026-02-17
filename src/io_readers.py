"""
I/O Readers — Excel/CSV reading, sheet snapshots, locale-aware number parsing.
"""
from __future__ import annotations

import re
import math
from pathlib import Path
from typing import Dict, Optional, List, Any

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def read_excel(path: str | Path) -> Dict[str, pd.DataFrame]:
    """Read an Excel file and return {sheet_name: DataFrame}.
    Supports .xlsx (openpyxl) and .xls (xlrd).
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    ext = path.suffix.lower()
    if ext == ".xlsx":
        engine = "openpyxl"
    elif ext == ".xls":
        engine = "xlrd"
    elif ext == ".csv":
        df = pd.read_csv(path, dtype=str)
        return {path.stem: df}
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    xls = pd.ExcelFile(path, engine=engine)
    sheets: Dict[str, pd.DataFrame] = {}
    for name in xls.sheet_names:
        df = xls.parse(name, header=None, dtype=str)
        sheets[name] = df
    return sheets


# ---------------------------------------------------------------------------
# Sheet snapshot for LLM
# ---------------------------------------------------------------------------

def make_sheet_snapshot(
    df: pd.DataFrame,
    max_rows: int = 1000,
    max_cols: int = 25,
    format: str = "csv"
) -> str:
    """Create a text representation of the first rows/cols for LLM consumption.
    
    Args:
        df: The DataFrame to snapshot.
        max_rows: Maximum number of rows to include (default 1000).
        max_cols: Maximum number of columns to include (default 25).
        format: "csv" for comma-separated values (efficient), "markdown" for visual grid.
    """
    # Slice the dataframe
    sub = df.iloc[:max_rows, :max_cols].copy()
    
    # If format is CSV, use pandas to_csv
    if format == "csv":
        return sub.to_csv(index=False, header=False, sep=";", lineterminator="\n")
    
    # Fallback to "markdown" / visual grid
    # Replace NaN with empty string for readability
    sub = sub.fillna("")

    lines: list[str] = []
    # Column letters
    col_labels = [_col_letter(i) for i in range(sub.shape[1])]
    lines.append("     | " + " | ".join(f"{c:>12s}" for c in col_labels))
    lines.append("-----+" + "-+-".join("-" * 12 for _ in col_labels))

    for row_idx in range(sub.shape[0]):
        row_num = row_idx + 1  # 1-based like Excel
        vals = [str(sub.iat[row_idx, c])[:12].rjust(12) for c in range(sub.shape[1])]
        lines.append(f"{row_num:4d} | " + " | ".join(vals))

    return "\n".join(lines)


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to Excel column letter(s)."""
    result = ""
    while True:
        result = chr(65 + idx % 26) + result
        idx = idx // 26 - 1
        if idx < 0:
            break
    return result


# ---------------------------------------------------------------------------
# Column profiling
# ---------------------------------------------------------------------------

def column_profile(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Generate per-column statistics useful for LLM context."""
    profiles = []
    for col_idx in range(df.shape[1]):
        col = df.iloc[:, col_idx]
        total = len(col)
        non_empty = col.dropna().astype(str).str.strip().replace("", pd.NA).dropna()
        n_non_empty = len(non_empty)

        # Numeric detection
        n_numeric = 0
        for v in non_empty:
            if _looks_numeric(str(v)):
                n_numeric += 1

        # Pattern detection
        patterns: list[str] = []
        sample_vals = non_empty.head(20).tolist()

        # Account number pattern (digits, possibly with dots)
        acct_pattern = sum(1 for v in sample_vals if re.match(r"^\d{3,6}$", str(v).strip()))
        if acct_pattern > 3:
            patterns.append("ACCOUNT_NUMBER")

        # S/H indicator
        sh_vals = {str(v).strip().upper() for v in sample_vals}
        if sh_vals <= {"S", "H", "D", "C", "SOLL", "HABEN", "DEBIT", "CREDIT"}:
            if len(sh_vals) >= 2:
                patterns.append("SIDE_INDICATOR")

        # Date-like
        date_count = sum(1 for v in sample_vals if re.search(r"\d{2}[./]\d{2}[./]\d{2,4}", str(v)))
        if date_count > 3:
            patterns.append("DATE")

        # Keywords
        keywords_found = []
        all_text = " ".join(str(v).lower() for v in sample_vals)
        for kw in ["summe", "gesamt", "total", "saldo", "endsaldo", "balance", "debit",
                    "credit", "soll", "haben", "beginning", "anfang", "end", "sold",
                    "bilanz", "result", "ergebnis"]:
            if kw in all_text:
                keywords_found.append(kw)

        profiles.append({
            "col_index": col_idx,
            "col_letter": _col_letter(col_idx),
            "total_rows": total,
            "non_empty": n_non_empty,
            "empty_ratio": round(1 - n_non_empty / max(total, 1), 3),
            "numeric_ratio": round(n_numeric / max(n_non_empty, 1), 3),
            "patterns": patterns,
            "keywords": keywords_found,
            "sample_values": [str(v)[:50] for v in sample_vals[:10]],
        })
    return profiles


def _looks_numeric(s: str) -> bool:
    """Check if a string looks like a number (various locales)."""
    s = s.strip()
    if not s:
        return False
    # Remove currency symbols and whitespace
    s = re.sub(r"[€$£¥₹\s]", "", s)
    # Remove parentheses (negative)
    s = re.sub(r"[()]", "", s)
    # Try common patterns
    # German: 1.234,56  or  1234,56
    if re.match(r"^-?\d{1,3}(\.\d{3})*(,\d+)?$", s):
        return True
    # English: 1,234.56  or  1234.56
    if re.match(r"^-?\d{1,3}(,\d{3})*(\.\d+)?$", s):
        return True
    # Plain number
    if re.match(r"^-?\d+([.,]\d+)?$", s):
        return True
    return False


# ---------------------------------------------------------------------------
# Number parsing
# ---------------------------------------------------------------------------

def parse_number(text: str, locale_hint: str = "auto") -> Optional[float]:
    """Parse a number string from various locales into a float.

    Handles:
    - German: 1.234,56
    - English: 1,234.56
    - Parentheses for negatives: (1.234,56)
    - Currency symbols: €, $, etc.
    - Whitespace/apostrophe as thousand separator: 1 234,56 or 1'234.56
    """
    if text is None:
        return None
    if isinstance(text, (int, float)):
        if math.isnan(text):
            return None
        return float(text)

    s = str(text).strip()
    if not s:
        return None

    # Detect negative via parentheses
    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1].strip()
    elif s.startswith("-"):
        is_negative = True
        s = s[1:].strip()

    # Remove currency symbols
    s = re.sub(r"[€$£¥₹CHF\s]", "", s)
    # Remove apostrophe thousand separator
    s = s.replace("'", "")

    if not s:
        return None

    # Determine locale
    if locale_hint == "auto":
        locale_hint = _detect_number_locale(s)

    if locale_hint == "de":
        # German: dots are thousands, comma is decimal
        s = s.replace(".", "")
        s = s.replace(",", ".")
    elif locale_hint == "en":
        # English: commas are thousands, dot is decimal
        s = s.replace(",", "")
    else:
        # Try to be smart
        s = s.replace(",", "")

    try:
        val = float(s)
        return -val if is_negative else val
    except ValueError:
        return None


def _detect_number_locale(s: str) -> str:
    """Heuristic: detect if a number string is German or English format."""
    # If there's a comma after a dot → German (1.234,56)
    dot_pos = s.rfind(".")
    comma_pos = s.rfind(",")

    if dot_pos >= 0 and comma_pos >= 0:
        if comma_pos > dot_pos:
            return "de"  # 1.234,56
        else:
            return "en"  # 1,234.56

    # Only comma: could be German decimal or English thousand
    if comma_pos >= 0 and dot_pos < 0:
        # If comma is followed by exactly 2-3 digits at end → likely decimal
        after_comma = s[comma_pos + 1:]
        if len(after_comma) <= 3 and after_comma.isdigit() and len(after_comma) >= 1:
            # Check if it's 3 digits — could be thousand separator
            if len(after_comma) == 3 and comma_pos > 0:
                before_comma = s[:comma_pos].replace(".", "").replace(",", "")
                if len(before_comma) <= 3:
                    return "de"  # Ambiguous, but treat as German decimal
                return "en"  # Likely English thousand: 1,234
            return "de"  # 1234,56 → German

    # Only dot
    if dot_pos >= 0 and comma_pos < 0:
        after_dot = s[dot_pos + 1:]
        if len(after_dot) == 3:
            # Could be German thousand separator: 1.234
            before_dot = s[:dot_pos]
            if before_dot.isdigit() and len(before_dot) <= 3:
                return "de"
        return "en"

    return "en"  # Default
