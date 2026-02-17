"""
Debug script to verify Step 2 (LLM Interpretation) and Step 3 (Python Extraction).
Does NOT overwrite any files. Writes results to output/debug_result.txt.
"""
import os
import sys
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.io_readers import read_excel
from src.llm_client import LLMClient
from src.table_detect import detect_tables, extract_by_detection
from src.normalize import rules_from_detection, apply_classification, normalize_amounts, deduplicate_accounts
from src.signs import normalize_signs

# Force load .env explicitly
load_dotenv(Path(__file__).parent.parent / ".env")

SUSA_PATH = Path(r"Examples/Lucanet Einlesen Automation/SuSa_Sammlung/SuSa_Sammlung.xlsx")
OUT_FILE = Path("output/debug_result.txt")
OUT_FILE.parent.mkdir(exist_ok=True)

def run_debug():
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        def log(msg):
            print(msg)
            f.write(str(msg) + "\n")

        if not SUSA_PATH.exists():
            log(f"❌ File not found: {SUSA_PATH}")
            return

        log(f"📂 Reading {SUSA_PATH}...")
        sheets = read_excel(SUSA_PATH)
        sheet_name = list(sheets.keys())[0] if sheets else None
        
        if not sheet_name:
            log("❌ No sheets found!")
            return

        df = sheets[sheet_name]
        log(f"📄 Sheet '{sheet_name}': {df.shape[0]} rows, {df.shape[1]} cols")

        # 1. LLM Call
        log("\n🤖 Step 2: Running LLM Sheet-Interpreter (gpt-5-mini)...")
        # Use api_key strictly from env loaded above
        llm = LLMClient(model="gpt-5-mini-2025-08-07", api_key=os.getenv("OPENAI_API_KEY")) 
        
        detections = detect_tables(llm, sheet_name, df)
        
        if not detections:
            log("❌ No tables detected!")
            return

        det = detections[0]
        log("\n✅ LLM Result (The 'Regel-Paket'):")
        log(f"  • Table Bounds: Rows {det.start_row}-{det.end_row}, Cols {det.start_col}-{det.end_col}")
        log(f"  • Columns: KontoNr={det.column_roles.account_number_col}, Name={det.column_roles.account_name_col}, AmountCols={det.column_roles.amount_cols}")
        log(f"  • Rules: Total Keywords={json.dumps(det.row_type_hints.get('total_keywords', []))}")
        log(f"  • Strategy: {det.amount_strategy}")
        log(f"  • Sign Convention: {det.sign_convention}")
        log(f"  • Confidence: {det.confidence}")

        # 2. Python Extraction
        log("\n⚙️ Step 3: Python Extraction & Cleaning...")
        extracted = extract_by_detection(df, det)
        log(f"  → Extracted {len(extracted)} raw rows from table area")

        rules = rules_from_detection(det)
        classified = apply_classification(extracted, rules)
        
        counts = classified['row_type'].value_counts().to_dict()
        log(f"  → Classified rows: {counts}")

        normalized = normalize_amounts(classified, det.amount_strategy)
        deduped = deduplicate_accounts(normalized)
        
        # Sign normalization
        final_df, sign_info = normalize_signs(deduped, det.sign_convention)

        # Show result
        accounts = final_df[final_df["row_type"] == "ACCOUNT"]
        log(f"\n🎉 Result: {len(accounts)} clean accounts extracted.")
        
        log("\n🔎 First 10 Extracted Accounts:")
        view_cols = ["konto_nr", "konto_name", "amount_normalized", "row_type"]
        if not accounts.empty:
            log(accounts[view_cols].head(10).to_string(index=False))
        else:
            log("  (No accounts found)")

        log(f"\nℹ️ Sign Logic Applied: {sign_info.get('notes', '')}")

if __name__ == "__main__":
    try:
        run_debug()
    except Exception as e:
        with open(OUT_FILE, "a", encoding="utf-8") as f:
            f.write(f"\n❌ Error: {e}\n")
        print(f"\n❌ Error: {e}")
