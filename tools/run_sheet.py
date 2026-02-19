import os
import sys
import logging
import pandas as pd
from pathlib import Path

# Add project root to sys.path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.llm_client import LLMClient
from src.io_readers import read_excel
from src.table_detect import detect_tables, extract_by_detection
from src.normalize import rules_from_detection, apply_classification, deduplicate_accounts
from src.targets import load_targets
from src.mapping import map_accounts
from src.dummy_mapper import load_dummy_pool, assign_dummy_ids, build_lucanet_df, save_lucanet_xlsx

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("sheet_run")

def run_sheet(sheet_name):
    susa_path = "Examples/Lucanet Einlesen Automation/SuSa_Sammlung/SuSa_Sammlung.xlsx"
    targets_path = "Examples/Lucanet Einlesen Automation/Unsere_Lucanet_Zuordnung.xls"
    dummy_pool_path = "Examples/Dummykonten_Zuordnung_hart.xlsx"
    out_dir = Path(f"./output/sheet_{sheet_name}")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    model = "gpt-5-mini-2025-08-07"
    llm = LLMClient(model=model, cache_db_path=out_dir / "llm_cache.db")
    
    # 1. Load targets
    log.info(f"Loading targets from {targets_path}")
    targets = load_targets(targets_path)
    log.info(f"Loaded {len(targets)} target positions")
    
    # 2. Read SuSa (Specific sheet)
    temp_susa = Path(f"temp_susa_{sheet_name}.xlsx")
    import shutil
    shutil.copy2(susa_path, temp_susa)
    log.info(f"Reading SuSa sheet '{sheet_name}' from temp copy")
    sheets = pd.read_excel(temp_susa, sheet_name=[sheet_name])
    df = sheets[sheet_name]
    log.info(f"Sheet {sheet_name}: {df.shape[0]} rows x {df.shape[1]} cols")
    
    # 3. Detect & Extract
    log.info("Detecting table and rules...")
    detections = detect_tables(llm, sheet_name, df)
    if not detections:
        log.error("No table detected!")
        return

    det = detections[0]
    log.info(f"Table detected: rows {det.start_row}-{det.end_row}, sign={det.sign_convention}")
    
    extracted = extract_by_detection(df, det)
    rules = rules_from_detection(det)
    classified = apply_classification(extracted, rules)
    classified = deduplicate_accounts(classified)
    
    account_count = len(classified[classified["row_type"] == "ACCOUNT"])
    log.info(f"Extracted {account_count} accounts")
    
    # 4. Map — batch_size=50 (optimal for reasoning models to finish within timeouts)
    log.info(f"Mapping all {account_count} accounts in batches of 50 (reasoning_effort='medium')...")
    mapped_df = map_accounts(llm, classified, targets, batch_size=50, reasoning_effort='medium')
    
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
    xlsx_path = out_dir / f"Mapping_Blatt_{sheet_name}.xlsx"
    output_df.to_excel(xlsx_path, index=False, sheet_name=f"Mapping Blatt {sheet_name}")
    log.info(f"Excel saved: {xlsx_path}")
    
    # Save TXT
    txt_path = out_dir / f"Mapping_Blatt_{sheet_name}.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"Mapping Blatt {sheet_name} — {account_count} Konten\n")
        f.write("=" * 80 + "\n\n")
        for _, row in output_df.iterrows():
            f.write(f"{row['Kontonr & Bezeichnung']:<50} → [{row['Target ID']}] {row['Unsere Zuordnung (LucaNet)']}\n")
    log.info(f"TXT saved: {txt_path}")

    # 6. Lucanet Dummy-OID Zuordnung
    log.info("Lade Dummy-Pool und weise OIDs zu...")
    dummy_pool = load_dummy_pool(dummy_pool_path)
    mapped_with_dummies = assign_dummy_ids(mapped_df, dummy_pool)
    lucanet_df = build_lucanet_df(mapped_with_dummies)
    lucanet_path = out_dir / f"Lucanet_Mapping_Blatt_{sheet_name}.xlsx"
    save_lucanet_xlsx(lucanet_df, lucanet_path)

    print(f"\nDONE! {account_count} Konten gemapped.")
    print(f"  Excel:         {xlsx_path}")
    print(f"  TXT:           {txt_path}")
    print(f"  Lucanet-Export:{lucanet_path} ({len(lucanet_df)} Zeilen mit Dummy-OID)")

    llm.close()

if __name__ == "__main__":
    import sys
    sheet = sys.argv[1] if len(sys.argv) > 1 else '5'
    run_sheet(sheet)
