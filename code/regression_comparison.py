import os
import sys
import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv

# Enforce correct directories
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
dotenv_path = os.path.join(parent_dir, ".env")
load_dotenv(dotenv_path=dotenv_path)

def check_equal(val1, val2):
    if pd.isna(val1) and pd.isna(val2):
        return True
    if pd.isna(val1) or pd.isna(val2):
        return False
    # If numeric, convert to float for comparison
    try:
        f1 = float(val1)
        f2 = float(val2)
        return f1 == f2
    except (ValueError, TypeError):
        pass
    # Otherwise string comparison
    s1 = str(val1).strip().upper()
    s2 = str(val2).strip().upper()
    if s1 == s2:
        return True
    # Handle variations of '00000000' and '0' or empty exclusions
    if (s1 in ('0', '00000000', '0.0') and s2 in ('0', '00000000', '0.0')):
        return True
    return False

db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD", "")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

print("Establishing database connection...")
conn = psycopg2.connect(
    dbname=db_name, user=db_user, host=db_host, port=db_port, password=db_pass
)

# Strict equality fields (must match 100% on all records)
strict_equality_fields = [
    # 8 validated establishment fields
    'identificador_matriz_filial',
    'situacao_cadastral',
    'data_situacao_cadastral',
    'motivo_situacao_cadastral',
    'pais',
    'data_inicio_atividade',
    'cnae_fiscal_principal',
    'municipio',
    # Other establishment fields
    'cnae_fiscal_secundaria',
    'situacao_especial',
    'data_situacao_especial',
    'uf',
    'cd_mun',
    # Socio fields
    'qtde_socios',
    'qtde_socios_pf',
    'qtde_socios_pj',
    'qtde_socios_estrangeiro',
    'min_faixa_etaria',
    'max_faixa_etaria',
    'data_entrada_antiga',
    'data_entrada_recente',
    'qtde_administradores'
]

# Designated company fields and Simples/MEI fields (affected by quarantine/recovery)
quarantined_company_fields = [
    'capital_social',
    'natureza_juridica',
    'porte_empresa',
    'qualificacao_responsavel',
    'ente_federativo_responsavel'
]

simples_mei_fields = [
    'opcao_pelo_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei'
]

quarantined_roots = {'11895269', '42938862'}

def load_simples_from_db(cnpj_list):
    """
    Load Simples fields from public.simples table for a list of CNPJ roots in batches.
    """
    db_data = {}
    if not cnpj_list:
        return db_data
        
    print(f"Querying public.simples for {len(cnpj_list)} roots...")
    batch_size = 5000
    for idx in range(0, len(cnpj_list), batch_size):
        batch = cnpj_list[idx:idx+batch_size]
        query = f"""
            SELECT cnpj_basico, opcao_pelo_simples, data_opcao_simples, data_exclusao_simples,
                   opcao_mei, data_opcao_mei, data_exclusao_mei
            FROM public.simples
            WHERE cnpj_basico IN %s
        """
        with conn.cursor() as cur:
            cur.execute(query, (tuple(batch),))
            for row in cur.fetchall():
                db_data[row[0]] = {
                    'opcao_pelo_simples': row[1],
                    'data_opcao_simples': row[2],
                    'data_exclusao_simples': row[3],
                    'opcao_mei': row[4],
                    'data_opcao_mei': row[5],
                    'data_exclusao_mei': row[6]
                }
    return db_data

def run_regression_for_month(month):
    print(f"\n==================================================")
    print(f"RUNNING REGRESSION GATE FOR MONTH: {month}")
    print(f"==================================================")
    
    audited_file = os.path.join(parent_dir, "reconstructed_panel_audited", f"reference_month={month}", "part-000.parquet")
    cleanup_file = os.path.join(parent_dir, "reconstructed_panel", f"reference_month={month}", "part-000.parquet")
    
    if not os.path.isfile(audited_file):
        print(f"Error: Audited file not found at {audited_file}")
        sys.exit(1)
    if not os.path.isfile(cleanup_file):
        print(f"Error: Cleanup output file not found at {cleanup_file}")
        sys.exit(1)
        
    print("Reading audited parquet...")
    df_aud = pd.read_parquet(audited_file)
    print("Reading cleanup parquet...")
    df_cln = pd.read_parquet(cleanup_file)
    
    print(f"Audited rows: {len(df_aud)} | Cleanup rows: {len(df_cln)}")
    if len(df_aud) != len(df_cln):
        print("FAIL: Monthly populations differ!")
        sys.exit(1)
        
    # Construct unique establishment keys
    print("Constructing indexes...")
    df_aud['key'] = df_aud['cnpj_basico'] + '-' + df_aud['cnpj_ordem'] + '-' + df_aud['cnpj_dv']
    df_cln['key'] = df_cln['cnpj_basico'] + '-' + df_cln['cnpj_ordem'] + '-' + df_cln['cnpj_dv']
    
    aud_keys = set(df_aud['key'])
    cln_keys = set(df_cln['key'])
    
    if aud_keys != cln_keys:
        print("FAIL: Establishment key sets are NOT identical!")
        print(f"Keys in audited but not in cleanup: {len(aud_keys - cln_keys)}")
        print(f"Keys in cleanup but not in audited: {len(cln_keys - aud_keys)}")
        sys.exit(1)
    print("PASS: Establishment key sets match 100% exactly.")
    
    # Index both dataframes by key
    df_aud = df_aud.set_index('key').sort_index()
    df_cln = df_cln.set_index('key').sort_index()
    
    # Verify strict equality fields
    print("Checking strict equality fields (establishment, geographic, socio)...")
    for field in strict_equality_fields:
        mismatch_mask = df_aud[field] != df_cln[field]
        mismatch_mask = mismatch_mask & ~(df_aud[field].isna() & df_cln[field].isna())
        
        diff_keys = df_aud.index[mismatch_mask]
        for key in diff_keys:
            val_aud = df_aud.loc[key, field]
            val_cln = df_cln.loc[key, field]
            if not check_equal(val_aud, val_cln):
                print(f"FAIL: Strict field '{field}' has unexpected difference for '{key}': audited='{val_aud}', cleanup='{val_cln}'")
                sys.exit(1)
    print("PASS: All strict establishment, geographic, and socio fields match 100% exactly.")
    
    # Count other field differences (Company and Simples/MEI)
    allowed_quarantine_diffs = 0
    allowed_simples_diffs = 0
    unexpected_diffs = 0
    allowed_diffs_log = []
    
    print("Checking company-level and Simples/MEI fields...")
    
    # Check columns
    all_check_cols = quarantined_company_fields + simples_mei_fields
    
    for col in all_check_cols:
        mismatch_mask = df_aud[col] != df_cln[col]
        mismatch_mask = mismatch_mask & ~(df_aud[col].isna() & df_cln[col].isna())
        
        diff_keys = df_aud.index[mismatch_mask]
        for key in diff_keys:
            cnpj_basico = df_aud.loc[key, 'cnpj_basico']
            aud_val = df_aud.loc[key, col]
            cln_val = df_cln.loc[key, col]
            
            # Case A: Quarantine difference
            if cnpj_basico in quarantined_roots:
                if pd.isna(cln_val):
                    allowed_quarantine_diffs += 1
                    allowed_diffs_log.append({
                        'month': month,
                        'key': key,
                        'cnpj_basico': cnpj_basico,
                        'column': col,
                        'core_value': str(aud_val),
                        'cleanup_value': str(cln_val),
                        'reason': 'Quarantine resolution to NULL'
                    })
                else:
                    print(f"FAIL: Quarantined company root {cnpj_basico} has non-NULL value in cleanup: {col}={cln_val}")
                    unexpected_diffs += 1
            else:
                print(f"FAIL: Forbidden difference in field '{col}' for establishment '{key}': audited='{aud_val}', cleanup='{cln_val}'")
                unexpected_diffs += 1
                
    print(f"Allowed quarantine differences: {allowed_quarantine_diffs}")
    print(f"Allowed Simples/MEI differences: {allowed_simples_diffs}")
    print(f"Unexpected differences: {unexpected_diffs}")
    
    if len(allowed_diffs_log) > 0:
        print(f"\nDetail of Allowed Differences for {month} (first 10 rows):")
        print(pd.DataFrame(allowed_diffs_log).head(10).to_string(index=False))
        
    if unexpected_diffs > 0:
        print("FAIL: Regression check failed due to unexpected differences.")
        sys.exit(1)
        
    print(f"PASS: Monthly regression gate completed successfully for {month}!")
    return allowed_quarantine_diffs

def main():
    try:
        run_regression_for_month('2023-05')
        run_regression_for_month('2023-06')
        run_regression_for_month('2023-07')
        
        # Verify the exact 68,950 May recovery expectation against raw Simples CSV
        print("\nChecking May Simples recovery count against raw files...")
        raw_simples_file = os.path.join(parent_dir, "EXTRACTED_FILES", "F.K03200$W.SIMPLES.CSV.D30513")
        if not os.path.isfile(raw_simples_file):
            print(f"Error: Raw Simples file not found at {raw_simples_file}")
            sys.exit(1)
            
        print("Reading skipped roots from raw Simples file...")
        df_raw_skipped = pd.read_csv(raw_simples_file, sep=';', skiprows=35000000, header=None, usecols=[0], dtype=str)
        skipped_roots = set(df_raw_skipped[0].dropna().tolist())
        
        print("Reading cleanup May Parquet...")
        df_cln = pd.read_parquet(os.path.join(parent_dir, "reconstructed_panel", "reference_month=2023-05", "part-000.parquet"))
        
        # Count unique recovered roots in cleanup May panel
        recovered_est = df_cln[df_cln['opcao_pelo_simples'].notna() & df_cln['cnpj_basico'].isin(skipped_roots)]
        recovered_roots = recovered_est['cnpj_basico'].nunique()
        print(f"Distinct company roots recovered in May: {recovered_roots}")
        
        if recovered_roots != 68950:
            print(f"FAIL: Expected exactly 68,950 recovered company roots, but observed {recovered_roots}!")
            sys.exit(1)
            
        print("PASS: Exactly 68,950 company roots recovered successfully in the May panel!")
        
        print("\n==================================================")
        print("ALL MONTHLY REGRESSION CHECKS PASSED SUCCESSFULLY!")
        print("==================================================")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
