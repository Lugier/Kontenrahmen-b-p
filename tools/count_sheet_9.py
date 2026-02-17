import pandas as pd
import logging
from pathlib import Path
from src.llm_client import LLMClient
from src.table_detect import detect_tables, extract_by_detection
from src.normalize import rules_from_detection, apply_classification, normalize_amounts, deduplicate_accounts

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("sheet_9_count")

def count_sheet_9():
    susa_path = "Examples/Lucanet Einlesen Automation/SuSa_Sammlung/SuSa_Sammlung.xlsx"
    sheet_name = "9"
    
    # 2. Read SuSa (Sheet 9 only)
    temp_susa = Path(f"temp_susa_{sheet_name}.xlsx")
    if not temp_susa.exists():
        import shutil
        shutil.copy2(susa_path, temp_susa)
        log.info(f"Copied {susa_path} to {temp_susa}")

    log.info(f"Reading SuSa sheet '{sheet_name}'")
    sheets = pd.read_excel(temp_susa, sheet_name=[sheet_name])
    df = sheets[sheet_name]
    log.info(f"Sheet 9: {df.shape[0]} rows x {df.shape[1]} cols")
    
    # 3. Detect & Extract
    log.info("Detecting table and rules...")
    model = "gpt-5-mini-2025-08-07"
    llm = LLMClient(model=model, cache_db_path="debug_llm_cache.db")
    
    detections = detect_tables(llm, sheet_name, df)
    if not detections:
        log.error("No table detected!")
        return

    det = detections[0]
    log.info(f"Table detected: rows {det.start_row}-{det.end_row}, sign={det.sign_convention}")
    
    extracted = extract_by_detection(df, det)
    rules = rules_from_detection(det)
    classified = apply_classification(extracted, rules)
    classified = normalize_amounts(classified, rules.amount_strategy, language_hint=det.language_guess)
    classified = deduplicate_accounts(classified)
    
    account_count = len(classified[classified["row_type"] == "ACCOUNT"])
    log.info(f"Extracted {account_count} accounts")
    
    print("\n" + "="*40)
    print(f"FINAL ACCOUNT COUNT: {account_count}")
    print("="*40 + "\n")
    
    llm.close()

if __name__ == "__main__":
    count_sheet_9()
