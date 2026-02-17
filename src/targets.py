"""
Phase 3 — Target positions (Überpositionen) from LucaNet Zuordnung XLS.
"""
from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TargetPosition(BaseModel):
    target_id: str
    target_name: str
    target_class: str  # AKTIVA, PASSIVA, AUFWAND, ERTRAG
    hierarchy_path: str = ""
    is_leaf: bool = True
    level: int = 0
    sheet: str = ""


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def load_targets(xls_path: str | Path) -> List[TargetPosition]:
    """Load target positions from Unsere_Lucanet_Zuordnung.xls.

    Expects sheets 'Bilanz' and 'GuV', each with a hierarchy column.
    """
    xls_path = Path(xls_path)
    if not xls_path.exists():
        raise FileNotFoundError(f"Targets file not found: {xls_path}")

    targets: List[TargetPosition] = []

    try:
        xls = pd.ExcelFile(xls_path, engine="xlrd")
    except Exception:
        # Fallback for newer format
        xls = pd.ExcelFile(xls_path, engine="openpyxl")

    for sheet_name in xls.sheet_names:
        sheet_lower = sheet_name.strip().lower()
        if "bilanz" in sheet_lower:
            sheet_targets = _parse_bilanz_sheet(xls, sheet_name)
            targets.extend(sheet_targets)
        elif "guv" in sheet_lower or "g+v" in sheet_lower or "g&v" in sheet_lower:
            sheet_targets = _parse_guv_sheet(xls, sheet_name)
            targets.extend(sheet_targets)

    if not targets:
        logger.warning("No targets found in %s. Available sheets: %s",
                        xls_path, xls.sheet_names)

    logger.info("Loaded %d target positions from %s", len(targets), xls_path)
    return targets


def _parse_bilanz_sheet(xls: pd.ExcelFile, sheet_name: str) -> List[TargetPosition]:
    """Parse a Bilanz sheet into target positions."""
    df = xls.parse(sheet_name, header=None, dtype=str)
    return _parse_hierarchy_sheet(df, sheet_name, "Bilanz")


def _parse_guv_sheet(xls: pd.ExcelFile, sheet_name: str) -> List[TargetPosition]:
    """Parse a GuV sheet into target positions."""
    df = xls.parse(sheet_name, header=None, dtype=str)
    return _parse_hierarchy_sheet(df, sheet_name, "GuV")


def _parse_hierarchy_sheet(
    df: pd.DataFrame, sheet_name: str, section: str
) -> List[TargetPosition]:
    """Parse a hierarchical position sheet.

    Detects the main text column and builds hierarchy from indentation/numbering.
    """
    # Find the main text column (the one with most non-empty text values)
    best_col = 0
    best_count = 0
    for col_idx in range(min(df.shape[1], 10)):
        col = df.iloc[:, col_idx].dropna().astype(str)
        text_vals = col[col.str.len() > 2]
        if len(text_vals) > best_count:
            best_count = len(text_vals)
            best_col = col_idx

    targets: List[TargetPosition] = []
    hierarchy_stack: List[Tuple[int, str]] = []  # (level, name)
    current_class = ""  # AKTIVA, PASSIVA, AUFWAND, ERTRAG

    for row_idx in range(df.shape[0]):
        cell = df.iat[row_idx, best_col]
        if pd.isna(cell):
            continue
        text = str(cell).strip()
        if not text or len(text) < 2:
            continue

        # Determine class from section markers
        text_lower = text.lower()
        if section == "Bilanz":
            if any(k in text_lower for k in ["aktivseite", "aktiva", "assets"]):
                current_class = "AKTIVA"
                continue
            elif any(k in text_lower for k in ["passivseite", "passiva", "liabilities", "equity"]):
                current_class = "PASSIVA"
                continue
            elif text_lower in ["bilanz", "balance sheet", "bilanţ"]:
                continue
        elif section == "GuV":
            if not current_class:
                current_class = "ERTRAG"  # Start with Ertrag

        # Detect if this is a section header or a leaf position
        level = _detect_level(text)

        # Check if it's a pure section header (Roman numerals, letters, etc.)
        is_header = _is_section_header(text)

        # Update hierarchy stack
        while hierarchy_stack and hierarchy_stack[-1][0] >= level:
            hierarchy_stack.pop()
        hierarchy_stack.append((level, text))

        hierarchy_path = " > ".join(name for _, name in hierarchy_stack)

        # Determine target class for GuV
        if section == "GuV":
            current_class = _guess_guv_class(text, current_class)

        if not current_class:
            current_class = "AKTIVA" if section == "Bilanz" else "AUFWAND"

        target_id = _make_target_id(text, row_idx, section)

        targets.append(TargetPosition(
            target_id=target_id,
            target_name=text,
            target_class=current_class,
            hierarchy_path=hierarchy_path,
            is_leaf=not is_header,
            level=level,
            sheet=sheet_name,
        ))

    return targets


def _detect_level(text: str) -> int:
    """Detect hierarchical level from text formatting."""
    # Count leading spaces
    leading_spaces = len(text) - len(text.lstrip())
    base_level = leading_spaces // 2

    text = text.strip()

    # Roman numeral sections: I., II., III., IV., etc.
    if re.match(r"^[IVX]+\.\s", text):
        return base_level + 1

    # Letter sections: A., B., C., a), b)
    if re.match(r"^[A-Z]\.\s", text):
        return base_level

    # Numbered: 1., 2., 3.
    if re.match(r"^\d+\.\s", text):
        return base_level + 2

    return base_level + 3  # Default: leaf level


def _is_section_header(text: str) -> bool:
    """Check if a line is a section header rather than a leaf position."""
    text = text.strip()
    # Very short entries or single-word uppercase
    if re.match(r"^[A-Z]\.\s", text):
        return True
    if re.match(r"^[IVX]+\.\s", text):
        return True
    if text.isupper() and len(text.split()) <= 3:
        return True
    return False


def _guess_guv_class(text: str, current: str) -> str:
    """Guess whether a GuV position is AUFWAND or ERTRAG."""
    text_lower = text.lower()

    ertrag_keywords = [
        "erlös", "ertrag", "erträg", "umsatz", "revenue", "income", "gain",
        "bestandsveränder", "eigenleistung", "sonstige betriebliche erträge",
        "zinserträge", "beteiligungserträge", "venituri",
    ]
    aufwand_keywords = [
        "aufwand", "aufwendung", "kosten", "abschreib", "material",
        "personal", "miete", "expense", "cost", "depreciation",
        "zinsaufwend", "steuer", "cheltuieli",
    ]

    for kw in ertrag_keywords:
        if kw in text_lower:
            return "ERTRAG"
    for kw in aufwand_keywords:
        if kw in text_lower:
            return "AUFWAND"

    return current


def _make_target_id(text: str, row_idx: int, section: str = "") -> str:
    """Create a stable target ID from text."""
    # Slugify
    slug = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß]", "_", text.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    slug = slug[:60]
    # Add hash for uniqueness (includes section to avoid cross-sheet collisions)
    h = hashlib.md5(f"{section}_{text}_{row_idx}".encode()).hexdigest()[:10]
    return f"{slug}_{h}"


def targets_to_whitelist(targets: List[TargetPosition]) -> List[Dict]:
    """Convert targets to a whitelist format suitable for LLM mapping."""
    return [
        {
            "target_id": t.target_id,
            "target_name": t.target_name,
            "target_class": t.target_class,
            "hierarchy_path": t.hierarchy_path,
            "is_leaf": t.is_leaf,
        }
        for t in targets
    ]


def get_targets_by_class(
    targets: List[TargetPosition], target_class: str
) -> List[TargetPosition]:
    """Filter targets by class."""
    return [t for t in targets if t.target_class == target_class]
