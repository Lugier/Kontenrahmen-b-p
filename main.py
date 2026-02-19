"""
SuSa → LLM-Mapping → LucaNet Kontenrahmen XML Pipeline

CLI entry point. Simplified flow:
  1. LLM Call (per sheet): table detection + row rules + sign convention
  2. Python: extract, classify, normalize amounts, deduplicate
  3. LLM Call (batched): map accounts to target positions
  4. Python: sign normalization, validation, (optional) LLM repair
  5. Python: XML export + reporting
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="SuSa → LucaNet Kontenrahmen XML Pipeline"
    )
    parser.add_argument("--susa", required=True, help="Path to SuSa file (xlsx/xls/csv)")
    parser.add_argument("--targets", required=True, help="Path to LucaNet Zuordnung file (xls/xlsx)")
    parser.add_argument("--out", default="./output", help="Output directory")
    parser.add_argument("--template_xml", default=None, help="Path to template AccountFramework XML")
    parser.add_argument("--period", default=None, help="Period to use (YYYY-MM, YYYY, or 'last')")
    parser.add_argument("--model", default="gpt-5-mini-2025-08-07", help="OpenAI model")
    parser.add_argument("--max_repair_rounds", type=int, default=2)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log = logging.getLogger("pipeline")

    # Imports (after arg parsing so --help is fast)
    from src.io_readers import read_excel
    from src.llm_client import LLMClient
    from src.table_detect import detect_tables, extract_by_detection
    from src.normalize import rules_from_detection, apply_classification, normalize_amounts, deduplicate_accounts
    from src.targets import load_targets, targets_to_whitelist
    from src.mapping import map_accounts
    from src.validate import run_checks, repair_mappings
    from src.reporting import generate_report

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Init ───────────────────────────────────────────────────────────
    log.info("Starting pipeline (model: %s)", args.model)
    llm = LLMClient(model=args.model, cache_db_path=out_dir / "llm_cache.db")

    # ── Load targets ───────────────────────────────────────────────────
    log.info("Loading target positions from %s", args.targets)
    targets = load_targets(args.targets)
    log.info("Loaded %d target positions", len(targets))

    # ── Read SuSa ──────────────────────────────────────────────────────
    log.info("Reading SuSa file %s", args.susa)
    sheets = read_excel(args.susa)
    log.info("Found %d sheets: %s", len(sheets), list(sheets.keys()))

    all_accounts = []

    for sheet_name, df in sheets.items():
        log.info("━━━ Sheet: %s (%d×%d) ━━━", sheet_name, *df.shape)

        # ── LLM Call 1: detect table + rules + sign convention ─────────
        detections = detect_tables(llm, sheet_name, df)
        if not detections:
            log.warning("No tables detected in '%s', skipping", sheet_name)
            continue

        for det in detections:
            log.info("  Table '%s': rows %d-%d, confidence %.2f, signs=%s",
                     det.table_id, det.start_row, det.end_row,
                     det.confidence, det.sign_convention)

            # ── Python: extract → classify → normalize ─────────────────
            extracted = extract_by_detection(df, det)
            if extracted.empty or "konto_nr" not in extracted.columns:
                log.warning("  No account data extracted, skipping")
                continue

            rules = rules_from_detection(det)
            classified = apply_classification(extracted, rules)
            classified = normalize_amounts(
                classified, rules.amount_strategy,
                period=args.period, language_hint=det.language_guess,
            )
            classified = deduplicate_accounts(classified)

            # Add source info
            classified["source_file"] = str(args.susa)
            classified["sheet"] = sheet_name
            classified["_sign_convention"] = det.sign_convention

            account_count = len(classified[classified["row_type"] == "ACCOUNT"])
            log.info("  → %d accounts extracted", account_count)
            all_accounts.append(classified)

    if not all_accounts:
        log.error("No accounts extracted from any sheet!")
        sys.exit(1)

    # Combine all sheets
    full_df = pd.concat(all_accounts, ignore_index=True)
    log.info("Total: %d rows (%d accounts)",
             len(full_df), len(full_df[full_df["row_type"] == "ACCOUNT"]))

    # ── LLM Call 2: map accounts to targets ────────────────────────────
    log.info("Mapping accounts to target positions...")
    full_df = map_accounts(llm, full_df, targets)

    # ── Python: validate + optional LLM repair ─────────────────────────
    log.info("Running validation checks...")
    checks = run_checks(full_df)
    # log.info("Balance diff: %.2f (%.1f%%), Unmapped: %d",
    #          checks["balance_diff"], checks["balance_diff_pct"], checks["unmapped_count"])
    log.info("Unmapped: %d", checks["unmapped_count"])

    if checks["has_issues"]:
        log.info("Issues detected, running repair (max %d rounds)...", args.max_repair_rounds)
        whitelist = targets_to_whitelist(targets)
        full_df = repair_mappings(llm, full_df, whitelist, checks, args.max_repair_rounds)
        checks = run_checks(full_df)

    # ── Python: outputs ────────────────────────────────────────────────
    log.info("Writing outputs...")
    full_df.to_csv(out_dir / "mapping.csv", index=False, encoding="utf-8-sig")

    generate_report(full_df, checks, {}, out_dir)

    # Done
    log.info("━━━ Pipeline complete ━━━")
    log.info("  Outputs in: %s", out_dir)
    log.info("  LLM stats: %s", json.dumps(llm.stats))
    llm.close()


if __name__ == "__main__":
    main()
