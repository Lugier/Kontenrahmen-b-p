"""
End-to-end smoke test — runs the pipeline on SuSa_Sammlung.xlsx.

Requires OPENAI_API_KEY to be set. Skipped otherwise.
Only checks that the pipeline runs without errors and produces outputs.
"""
import os
import pytest
from pathlib import Path


SUSA_PATH = Path(__file__).parent.parent / "Examples" / "Lucanet Einlesen Automation" / "SuSa_Sammlung" / "SuSa_Sammlung.xlsx"
TARGETS_PATH = Path(__file__).parent.parent / "Examples" / "Lucanet Einlesen Automation" / "Unsere_Lucanet_Zuordnung.xls"

needs_api_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY not set"
)
needs_files = pytest.mark.skipif(
    not SUSA_PATH.exists() or not TARGETS_PATH.exists(),
    reason="Test fixture files not found"
)


@needs_api_key
@needs_files
def test_smoke_pipeline(tmp_path):
    """Run full pipeline and check outputs exist."""
    from src.io_readers import read_excel
    from src.llm_client import LLMClient
    from src.table_detect import detect_tables, extract_by_detection
    from src.normalize import rules_from_detection, apply_classification, normalize_amounts, deduplicate_accounts
    from src.targets import load_targets, targets_to_whitelist
    from src.mapping import map_accounts
    from src.signs import normalize_signs
    from src.validate import run_checks
    from src.xml_export import generate_xml
    from src.reporting import generate_report

    import pandas as pd

    llm = LLMClient(cache_db_path=tmp_path / "cache.db")
    targets = load_targets(TARGETS_PATH)
    sheets = read_excel(SUSA_PATH)

    # Process just the first sheet for speed
    first_sheet = list(sheets.keys())[0]
    df = sheets[first_sheet]

    detections = detect_tables(llm, first_sheet, df)
    assert len(detections) > 0, "Should detect at least one table"

    det = detections[0]
    extracted = extract_by_detection(df, det)
    assert not extracted.empty

    rules = rules_from_detection(det)
    classified = apply_classification(extracted, rules)
    classified = normalize_amounts(classified, rules.amount_strategy)
    classified = deduplicate_accounts(classified)

    # Add required columns for mapping
    classified["source_file"] = str(SUSA_PATH)
    classified["sheet"] = first_sheet
    classified["source_range"] = f"{det.start_row}:{det.end_row}"

    # Map
    mapped = map_accounts(llm, classified, targets)
    mapped, sign_info = normalize_signs(mapped, det.sign_convention)
    checks = run_checks(mapped)

    # Outputs
    mapped.to_csv(tmp_path / "mapping.csv", index=False)
    generate_xml(mapped, output_path=tmp_path / "kontenrahmen.xml")
    generate_report(mapped, checks, sign_info, tmp_path)

    # Assertions: outputs exist
    assert (tmp_path / "mapping.csv").exists()
    assert (tmp_path / "kontenrahmen.xml").exists()
    assert (tmp_path / "report.md").exists()
    assert (tmp_path / "report.json").exists()

    llm.close()
