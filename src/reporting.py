"""
Reporting — Generate report.md, report.json, and optional review.csv.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def generate_report(
    mapping_df: pd.DataFrame,
    checks: Dict[str, Any],
    sign_info: Dict[str, Any],
    output_dir: str | Path,
) -> None:
    """Write report.md, report.json, and review.csv to output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    accounts = mapping_df[mapping_df["row_type"] == "ACCOUNT"]
    totals_removed = mapping_df[mapping_df["row_type"].isin(["TOTAL", "SUBTOTAL", "CALCULATED_RESULT"])]

    # --- report.json ---
    report_data = {
        "total_accounts": len(accounts),
        "total_rows_removed": len(totals_removed),
        "unmapped_count": checks.get("unmapped_count", 0),
        "low_confidence_count": checks.get("low_confidence_count", 0),
        "balance_check": {
            "aktiva": checks.get("aktiva_sum", 0),
            "passiva": checks.get("passiva_sum", 0),
            "diff": checks.get("balance_diff", 0),
            "diff_pct": checks.get("balance_diff_pct", 0),
        },
        "guv_check": {
            "ertrag": checks.get("ertrag_sum", 0),
            "aufwand": checks.get("aufwand_sum", 0),
            "result": checks.get("guv_result", 0),
        },
        "sign_convention": sign_info,
    }
    (output_dir / "report.json").write_text(
        json.dumps(report_data, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )

    # --- report.md ---
    md = [
        "# SuSa → LucaNet Mapping Report\n",
        f"## Summary\n",
        f"- **Accounts mapped**: {len(accounts)}",
        f"- **Total/Subtotal rows removed**: {len(totals_removed)}",
        f"- **Unmapped accounts**: {checks.get('unmapped_count', 0)}",
        f"- **Low confidence (< 0.5)**: {checks.get('low_confidence_count', 0)}\n",
        f"## Balance Check (Bilanz)\n",
        f"| | Amount |",
        f"|---|---:|",
        f"| Aktiva | {checks.get('aktiva_sum', 0):,.2f} |",
        f"| Passiva | {checks.get('passiva_sum', 0):,.2f} |",
        f"| **Differenz** | **{checks.get('balance_diff', 0):,.2f}** ({checks.get('balance_diff_pct', 0):.1f}%) |\n",
        f"## GuV Check\n",
        f"| | Amount |",
        f"|---|---:|",
        f"| Ertrag | {checks.get('ertrag_sum', 0):,.2f} |",
        f"| Aufwand | {checks.get('aufwand_sum', 0):,.2f} |",
        f"| **Ergebnis** | **{checks.get('guv_result', 0):,.2f}** |\n",
        f"## Sign Convention\n",
        f"- Confidence: {sign_info.get('confidence', 'N/A')}",
        f"- Convention: {json.dumps(sign_info.get('convention', {}), ensure_ascii=False)}",
        f"- Notes: {sign_info.get('notes', 'N/A')}\n",
    ]

    # Top risk accounts
    if not accounts.empty and "confidence" in accounts.columns:
        risky = accounts.nsmallest(10, "confidence")
        if not risky.empty:
            md.append("## Top Risk Accounts (lowest confidence)\n")
            md.append("| Konto | Name | Target | Confidence |")
            md.append("|---|---|---|---:|")
            for _, r in risky.iterrows():
                md.append(
                    f"| {r.get('konto_nr', '')} | {str(r.get('konto_name', ''))[:40]} "
                    f"| {str(r.get('target_overpos_name', ''))[:30]} | {r.get('confidence', 0):.2f} |"
                )
            md.append("")

    (output_dir / "report.md").write_text("\n".join(md), encoding="utf-8")

    # --- review.csv (optional) ---
    if not accounts.empty:
        review_cols = ["konto_nr", "konto_name", "target_overpos_id",
                       "target_overpos_name", "target_class", "confidence",
                       "rationale_short", "amount_normalized"]
        available = [c for c in review_cols if c in accounts.columns]
        accounts[available].to_csv(output_dir / "review.csv", index=False, encoding="utf-8-sig")
