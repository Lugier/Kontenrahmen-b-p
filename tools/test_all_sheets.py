"""
Comprehensive test: Compare manual vs pipeline account extraction for ALL sheets.
Verify that the system works universally across different SuSa formats.
"""
import pandas as pd
import re
import logging
import shutil
from pathlib import Path
from src.llm_client import LLMClient
from src.table_detect import detect_tables, extract_by_detection
from src.normalize import rules_from_detection, apply_classification, normalize_amounts, deduplicate_accounts

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("all_sheets_test")

def count_manual(df):
    """Count rows where col 0 looks like an account number (4-8 digits)."""
    accounts = []
    for idx, row in df.iterrows():
        val = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
        name = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
        if re.match(r'^\d{4,8}$', val):
            accounts.append((idx, val, name))
    return accounts

def count_pipeline(llm, sheet_name, df):
    """Run full pipeline and return extracted accounts."""
    try:
        detections = detect_tables(llm, sheet_name, df)
    except Exception as e:
        log.error(f"Sheet {sheet_name}: Detection failed: {e}")
        return [], 0, str(e)
    
    if not detections:
        return [], 0, "No tables detected"
    
    all_accounts = pd.DataFrame()
    for det in detections:
        try:
            extracted = extract_by_detection(df, det)
            if extracted.empty or 'konto_nr' not in extracted.columns:
                continue
            rules = rules_from_detection(det)
            classified = apply_classification(extracted, rules)
            classified = normalize_amounts(classified, rules.amount_strategy, language_hint=det.language_guess)
            classified = deduplicate_accounts(classified)
            accts = classified[classified['row_type'] == 'ACCOUNT']
            all_accounts = pd.concat([all_accounts, accts], ignore_index=True)
        except Exception as e:
            log.error(f"Sheet {sheet_name}, table {det.table_id}: Processing failed: {e}")
    
    return all_accounts, len(detections), None

def main():
    susa_path = "Examples/Lucanet Einlesen Automation/SuSa_Sammlung/SuSa_Sammlung.xlsx"
    shutil.copy2(susa_path, "temp_susa.xlsx")
    
    cache_path = Path("output/all_sheets_cache.db")
    llm = LLMClient(model="gpt-5-mini-2025-08-07", cache_db_path=cache_path)
    
    sheets = pd.read_excel("temp_susa.xlsx", sheet_name=None, header=None)
    
    # Skip non-data sheets
    skip_sheets = ['Mapping Beispiel']
    
    results = []
    report_lines = []
    report_lines.append("=" * 100)
    report_lines.append("FULL SHEET ANALYSIS — All SuSa Sheets")
    report_lines.append("=" * 100)
    
    for sheet_name in sheets.keys():
        if sheet_name in skip_sheets:
            continue
        
        df = sheets[sheet_name]
        log.info(f"--- Processing Sheet '{sheet_name}' ({len(df)} rows) ---")
        
        # Manual count
        manual_accts = count_manual(df)
        manual_count = len(manual_accts)
        manual_unique = len(set((nr, name) for _, nr, name in manual_accts))
        
        # Pipeline count
        pipeline_accts, num_tables, error = count_pipeline(llm, sheet_name, df)
        
        if error:
            pipeline_count = 0
            pipeline_nrs = set()
            status = f"ERROR: {error}"
        elif isinstance(pipeline_accts, pd.DataFrame) and not pipeline_accts.empty:
            pipeline_count = len(pipeline_accts)
            pipeline_nrs = set(
                (str(r['konto_nr']).strip(), str(r['konto_name']).strip()) 
                for _, r in pipeline_accts.iterrows()
            )
            
            # Find missing
            manual_set = set((nr, name) for _, nr, name in manual_accts)
            # Compare by konto_nr only (names may differ slightly)
            manual_nr_set = set(nr for _, nr, _ in manual_accts)
            pipeline_nr_set = set(str(r['konto_nr']).strip() for _, r in pipeline_accts.iterrows())
            missing_nrs = manual_nr_set - pipeline_nr_set
            extra_nrs = pipeline_nr_set - manual_nr_set
            
            diff = manual_unique - pipeline_count
            if diff == 0 and not missing_nrs:
                status = "✅ PERFECT"
            elif not missing_nrs and diff <= 0:
                status = f"✅ OK (pipeline has {-diff} more due to split entries)"
            elif missing_nrs:
                status = f"⚠️ MISSING {len(missing_nrs)} account numbers"
            else:
                status = f"⚠️ DIFF={diff}"
        else:
            pipeline_count = 0
            pipeline_nrs = set()
            missing_nrs = set()
            extra_nrs = set()
            status = "⚠️ No accounts extracted"
        
        results.append({
            'sheet': sheet_name,
            'rows': len(df),
            'manual_total': manual_count,
            'manual_unique': manual_unique,
            'pipeline': pipeline_count,
            'tables': num_tables,
            'status': status,
        })
        
        # Detailed report
        report_lines.append(f"\n{'='*80}")
        report_lines.append(f"Sheet: {sheet_name}")
        report_lines.append(f"{'='*80}")
        report_lines.append(f"  Raw rows: {len(df)}")
        report_lines.append(f"  Manual accounts (raw): {manual_count}")
        report_lines.append(f"  Manual accounts (unique nr+name): {manual_unique}")
        report_lines.append(f"  Pipeline accounts: {pipeline_count}")
        report_lines.append(f"  Tables detected: {num_tables}")
        report_lines.append(f"  Status: {status}")
        
        if error:
            report_lines.append(f"  Error: {error}")
        elif isinstance(pipeline_accts, pd.DataFrame) and not pipeline_accts.empty and missing_nrs:
            report_lines.append(f"  Missing account numbers:")
            for nr in sorted(missing_nrs):
                names = [name for _, mnr, name in manual_accts if mnr == nr]
                report_lines.append(f"    {nr} {names[0] if names else ''}")
    
    # Summary table
    print("\n" + "=" * 100)
    print(f"{'Sheet':<8} {'Rows':>6} {'Manual':>8} {'Unique':>8} {'Pipeline':>10} {'Tables':>7} {'Status'}")
    print("-" * 100)
    total_manual = 0
    total_pipeline = 0
    all_ok = True
    for r in results:
        print(f"{r['sheet']:<8} {r['rows']:>6} {r['manual_total']:>8} {r['manual_unique']:>8} {r['pipeline']:>10} {r['tables']:>7}   {r['status']}")
        total_manual += r['manual_unique']
        total_pipeline += r['pipeline']
        if '⚠️' in r['status'] or 'ERROR' in r['status']:
            all_ok = False
    print("-" * 100)
    print(f"{'TOTAL':<8} {'':>6} {total_manual:>8} {'':>8} {total_pipeline:>10}")
    print("=" * 100)
    
    if all_ok:
        print("\n🎉 ALL SHEETS PASS! Pipeline correctly extracts all accounts.")
    else:
        print("\n⚠️ Some sheets have issues. See details above.")
    
    # Save report
    report_path = Path("output/all_sheets_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"\nDetailed report: {report_path}")
    
    llm.close()

if __name__ == "__main__":
    main()
