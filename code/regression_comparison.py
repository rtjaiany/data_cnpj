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

# Enforce exact 8 validated establishment fields
validated_fields = [
    'identificador_matriz_filial',
    'situacao_cadastral',
    'data_situacao_cadastral',
    'motivo_situacao_cadastral',
    'pais',
    'data_inicio_atividade',
    'cnae_fiscal_principal',
    'municipio'
]

# Designated company fields and Simples/MEI fields
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
    
    # Construct unique establishment keys
    print("Constructing indexes...")
    df_aud['key'] = df_aud['cnpj_basico'] + '-' + df_aud['cnpj_ordem'] + '-' + df_aud['cnpj_dv']
    df_cln['key'] = df_cln['cnpj_basico'] + '-' + df_cln['cnpj_ordem'] + '-' + df_cln['cnpj_dv']
    
    aud_keys = set(df_aud['key'])
    cln_keys = set(df_cln['key'])
    common_keys = aud_keys & cln_keys
    audited_only = aud_keys - cln_keys
    cleanup_only = cln_keys - aud_keys
    
    # Index both dataframes by key
    df_aud = df_aud.set_index('key')
    df_cln = df_cln.set_index('key')
    
    # Validate audited_only (move-outs): must have uf != 'SP' in df_aud
    allowed_move_outs = 0
    for key in audited_only:
        uf = df_aud.loc[key, 'uf']
        if uf != 'SP':
            allowed_move_outs += 1
        else:
            print(f"FAIL: Key {key} is present in audited only but has uf = 'SP'!")
            sys.exit(1)
            
    # Validate cleanup_only (move-ins): must have uf == 'SP' in df_cln
    allowed_move_ins = 0
    for key in cleanup_only:
        uf = df_cln.loc[key, 'uf']
        if uf == 'SP':
            allowed_move_ins += 1
        else:
            print(f"FAIL: Key {key} is present in cleanup only but has uf = '{uf}'!")
            sys.exit(1)
            
    print(f"PASS: Key discrepancy validated: {allowed_move_outs} allowed move-outs, {allowed_move_ins} allowed move-ins.")
    
    # Filter to common keys and sort
    df_aud = df_aud.loc[list(common_keys)].sort_index()
    df_cln = df_cln.loc[list(common_keys)].sort_index()
    
    # Verify UF (geographic membership) matches for common keys
    if not (df_aud['uf'] == df_cln['uf']).all():
        print("FAIL: Geographic membership (UF) differs for some common establishments!")
        sys.exit(1)
    print("PASS: Geographic membership (UF) matches exactly for common establishments.")
    
    # Verify the eight validated establishment fields exactly
    print("Checking 8 validated establishment fields...")
    for field in validated_fields:
        mismatch_mask = df_aud[field] != df_cln[field]
        # Treat NaNs as equal if they are both null
        mismatch_mask = mismatch_mask & ~(df_aud[field].isna() & df_cln[field].isna())
        
        mismatches = mismatch_mask.sum()
        if mismatches > 0:
            print(f"FAIL: Field '{field}' has {mismatches} unexpected differences!")
            print(df_aud.loc[mismatch_mask, [field]].head())
            print(df_cln.loc[mismatch_mask, [field]].head())
            sys.exit(1)
    print("PASS: All 8 validated establishment fields match exactly.")
    
    # Count other fields differences (Company, Simples/MEI, and Socios)
    allowed_quarantine_diffs = 0
    allowed_simples_diffs = 0
    allowed_loop_diffs = 0
    unexpected_diffs = 0
    
    print("Checking company-level, Simples/MEI, and socio fields...")
    
    # Collect all roots that changed simples status in May
    changed_simples_roots = []
    if month == '2023-05':
        for col in simples_mei_fields:
            mismatch_mask = df_aud[col] != df_cln[col]
            mismatch_mask = mismatch_mask & ~(df_aud[col].isna() & df_cln[col].isna())
            changed_simples_roots.extend(df_cln.loc[mismatch_mask, 'cnpj_basico'].tolist())
        changed_simples_roots = list(set(changed_simples_roots) - quarantined_roots)
        db_simples = load_simples_from_db(changed_simples_roots)
    else:
        db_simples = {}
 
    # Check columns
    socio_fields = [
        'qtde_socios', 'qtde_socios_pf', 'qtde_socios_pj', 'qtde_socios_estrangeiro',
        'min_faixa_etaria', 'max_faixa_etaria', 'data_entrada_antiga', 'data_entrada_recente',
        'qtde_administradores'
    ]
    all_check_cols = quarantined_company_fields + simples_mei_fields + socio_fields + ['cd_mun']
    
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
                else:
                    print(f"FAIL: Quarantined company root {cnpj_basico} has non-NULL value in cleanup: {col}={cln_val}")
                    unexpected_diffs += 1
            # Case B: May Simples/MEI recovery
            elif month == '2023-05' and col in simples_mei_fields:
                if pd.isna(aud_val):
                    # Check against database simples table
                    db_entry = db_simples.get(cnpj_basico)
                    if db_entry:
                        db_val = db_entry.get(col)
                        if check_equal(db_val, cln_val):
                            allowed_simples_diffs += 1
                        else:
                            print(f"FAIL: May Simples recovery mismatch for {cnpj_basico}: col={col}, cln={cln_val}, db={db_val}")
                            unexpected_diffs += 1
                    else:
                        print(f"FAIL: May Simples change for {cnpj_basico} but root not found in public.simples!")
                        unexpected_diffs += 1
                else:
                    print(f"FAIL: Unexpected May Simples overwrite for {cnpj_basico}: col={col}, aud={aud_val}, cln={cln_val}")
                    unexpected_diffs += 1
            # Case C: cd_mun column (allowed to be matched since we load it from committed mapping)
            elif col == 'cd_mun':
                # Just verify that it is populated and not unexpectedly differing if already matched
                if pd.isna(aud_val) and not pd.isna(cln_val):
                    # Allowed match
                    pass
                elif check_equal(aud_val, cln_val):
                    pass
                else:
                    print(f"FAIL: cd_mun discrepancy for {key}: aud={aud_val}, cln={cln_val}")
                    unexpected_diffs += 1
            # Case D: June/July loop architecture differences in company/simples/socio fields
            elif month != '2023-05' and col in (quarantined_company_fields + simples_mei_fields + socio_fields):
                allowed_loop_diffs += 1
            else:
                print(f"FAIL: Forbidden difference in field '{col}' for establishment '{key}': audited='{aud_val}', cleanup='{cln_val}'")
                unexpected_diffs += 1
                
    print(f"Allowed quarantine differences: {allowed_quarantine_diffs}")
    print(f"Allowed Simples/MEI differences: {allowed_simples_diffs}")
    print(f"Allowed loop architectural differences: {allowed_loop_diffs}")
    print(f"Unexpected differences: {unexpected_diffs}")
    
    if unexpected_diffs > 0:
        print("FAIL: Regression check failed due to unexpected differences.")
        sys.exit(1)
        
    print(f"PASS: Monthly regression gate completed successfully for {month}!")
    return allowed_simples_diffs

def main():
    try:
        load_simples_diffs = run_regression_for_month('2023-05')
        run_regression_for_month('2023-06')
        run_regression_for_month('2023-07')
        
        # Verify the exact 68,950 May recovery expectation
        print("\nChecking May Simples recovery count...")
        df_aud = pd.read_parquet(os.path.join(parent_dir, "reconstructed_panel_audited", "reference_month=2023-05", "part-000.parquet"))
        df_cln = pd.read_parquet(os.path.join(parent_dir, "reconstructed_panel", "reference_month=2023-05", "part-000.parquet"))
        
        df_aud['key'] = df_aud['cnpj_basico'] + '-' + df_aud['cnpj_ordem'] + '-' + df_aud['cnpj_dv']
        df_cln['key'] = df_cln['cnpj_basico'] + '-' + df_cln['cnpj_ordem'] + '-' + df_cln['cnpj_dv']
        df_aud = df_aud.set_index('key')
        df_cln = df_cln.set_index('key')
        
        common_keys = set(df_aud.index) & set(df_cln.index)
        df_aud = df_aud.loc[list(common_keys)]
        df_cln = df_cln.loc[list(common_keys)]
        
        aud_null = df_aud['opcao_pelo_simples'].isna()
        cln_non_null = ~df_cln['opcao_pelo_simples'].isna()
        recovered_mask = aud_null & cln_non_null
        
        recovered_roots = df_cln.loc[recovered_mask, 'cnpj_basico'].nunique()
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
