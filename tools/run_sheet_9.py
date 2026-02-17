import os
import pandas as pd
import logging
from pathlib import Path
from src.llm_client import LLMClient
from src.io_readers import read_excel
from src.table_detect import detect_tables, extract_by_detection
from src.normalize import rules_from_detection, apply_classification, normalize_amounts, deduplicate_accounts
from src.targets import load_targets
from src.mapping import map_accounts

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("sheet_9_run")

def run_sheet_9():
    susa_path = "Examples/Lucanet Einlesen Automation/SuSa_Sammlung/SuSa_Sammlung.xlsx"
    targets_path = "Examples/Lucanet Einlesen Automation/Unsere_Lucanet_Zuordnung.xls"
    out_dir = Path("./output_sheet_9")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    model = "gpt-5-mini-2025-08-07"
    llm = LLMClient(model=model, cache_db_path=out_dir / "llm_cache.db")
    
    # 1. Load targets
    log.info(f"Loading targets from {targets_path}")
    targets = load_targets(targets_path)
    log.info(f"Loaded {len(targets)} target positions")
    
    # 2. Read SuSa (Sheet 9 only)
    temp_susa = Path("temp_susa.xlsx")
    import shutil
    shutil.copy2(susa_path, temp_susa)
    log.info("Reading SuSa sheet '9' from temp copy")
    sheets = pd.read_excel(temp_susa, sheet_name=['9'])
    df = sheets['9']
    log.info(f"Sheet 9: {df.shape[0]} rows x {df.shape[1]} cols")
    
    # 3. Detect & Extract
    log.info("Detecting table and rules...")
    detections = detect_tables(llm, '9', df)
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
    
    # 4. Map — batch_size=100 (smaller batches for better progress tracking and token limits), reasoning_effort=medium
    log.info(f"Mapping all {account_count} accounts in batches of 100 (reasoning_effort='medium')...")
    mapped_df = map_accounts(llm, classified, targets, batch_size=100, reasoning_effort='medium')
    
    # 5. Prepare Output — Excel + TXT
    account_rows = mapped_df[mapped_df['row_type'] == 'ACCOUNT'].copy()
    
    output_df = pd.DataFrame()
    output_df['Konto-Nr'] = account_rows['konto_nr'].values
    output_df['Konto-Name'] = account_rows['konto_name'].values
    output_df['Kontonr & Bezeichnung'] = account_rows.apply(
        lambda row: f"{str(row.get('konto_nr','')).strip()} {str(row.get('konto_name','')).strip()}", axis=1
    ).values
    output_df['Unsere Zuordnung (LucaNet)'] = account_rows['target_overpos_name'].values
    output_df['Target ID'] = account_rows['target_overpos_id'].values
    output_df['Confidence'] = account_rows['confidence'].values
    output_df['Begründung'] = account_rows['rationale_short'].values
    
    # Save Excel
    xlsx_path = out_dir / "Mapping_Blatt_9.xlsx"
    output_df.to_excel(xlsx_path, index=False, sheet_name="Mapping Blatt 9")
    log.info(f"Excel saved: {xlsx_path}")
    
    # Save TXT
    txt_path = out_dir / "Mapping_Blatt_9.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"Mapping Blatt 9 — {account_count} Konten\n")
        f.write("=" * 80 + "\n\n")
        for _, row in output_df.iterrows():
            f.write(f"{row['Kontonr & Bezeichnung']:<50} → {row['Unsere Zuordnung (LucaNet)']}\n")
    log.info(f"TXT saved: {txt_path}")
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"DONE! {account_count} Konten gemapped.")
    print(f"  Excel: {xlsx_path}")
    print(f"  TXT:   {txt_path}")
    print(f"{'='*80}\n")
    
    # Show first 10 mappings
    print("Erste 10 Mappings:")
    print("-" * 80)
    for i, (_, row) in enumerate(output_df.head(10).iterrows()):
        print(f"  {row['Kontonr & Bezeichnung']:<45} → {row['Unsere Zuordnung (LucaNet)']}")
    
    llm.close()

if __name__ == "__main__":
    run_sheet_9()
