import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv('/Users/rtjaiany/Library/CloudStorage/OneDrive-Personal/01_Documentos/01 - In Progress/03 - Dissertation/data_cnpj/.env')

db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "")
db_name = os.getenv("DB_NAME", "cnpj")

output_dir = '/Users/rtjaiany/Library/CloudStorage/OneDrive-Personal/01_Documentos/01 - In Progress/03 - Dissertation/data_cnpj/OUTPUT_FILES'

try:
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cur = conn.cursor()
    
    views = [
        ('analytics.v_reconstructed_establishments', 'v_reconstructed_establishments.csv'),
        ('analytics.v_reconstructed_companies', 'v_reconstructed_companies.csv'),
        ('analytics.v_reconstructed_simples', 'v_reconstructed_simples.csv'),
        ('analytics.v_reconstructed_partners', 'v_reconstructed_partners.csv'),
        ('analytics.v_reconstructed_partner_summaries', 'v_reconstructed_partner_summaries.csv'),
        ('analytics.longitudinal_establishment_intervals', 'longitudinal_establishment_intervals.csv'),
        ('analytics.establishment_transitions', 'establishment_transitions.csv')
    ]
    
    # Check if a limit argument was passed
    limit = None
    if len(sys.argv) > 1:
        if sys.argv[1].startswith('--limit='):
            limit = int(sys.argv[1].split('=')[1])
            print(f"Exporting sample of {limit} rows per table...")
        else:
            print("Usage: python export_csv.py [--limit=10000]")
            sys.exit(1)
            
    for view, filename in views:
        filepath = os.path.join(output_dir, filename)
        print(f"Exporting {view} to {filepath}...")
        
        if limit:
            if 'establishments' in view:
                query = f"SELECT * FROM {view} ORDER BY reference_month, cnpj_basic, cnpj_order, cnpj_dv LIMIT {limit}"
            elif 'companies' in view or 'simples' in view or 'partners' in view or 'summaries' in view:
                query = f"SELECT * FROM {view} ORDER BY reference_month, cnpj_basic LIMIT {limit}"
            elif 'transitions' in view:
                query = f"SELECT * FROM {view} ORDER BY cnpj_basic, cnpj_order, cnpj_dv, reference_month, variable_name LIMIT {limit}"
            elif 'longitudinal' in view:
                query = f"SELECT * FROM {view} ORDER BY cnpj_basic, cnpj_order, cnpj_dv, valid_from_month LIMIT {limit}"
            else:
                query = f"SELECT * FROM {view} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {view}"
            
        copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
        with open(filepath, 'w', encoding='utf-8') as f:
            cur.copy_expert(copy_sql, f)
        print(f"Completed export of {filename}!")
        
    # Free space if a full export was performed (limit is None)
    if limit is None:
        print("Freeing database space by truncating analytical tables...")
        tables_to_truncate = [
            'reconstructed_establishments',
            'reconstructed_companies',
            'reconstructed_simples',
            'reconstructed_partners',
            'reconstructed_partner_summaries',
            'longitudinal_establishment_intervals',
            'establishment_transitions'
        ]
        for tbl in tables_to_truncate:
            try:
                cur.execute(f"TRUNCATE TABLE analytics.{tbl} CASCADE;")
            except Exception as e:
                print(f"Warning: Failed to truncate {tbl}: {e}")
        conn.commit()
        print("Database space freed successfully!")
        
    cur.close()
    conn.close()
    print("All exports completed successfully!")
except Exception as e:
    print("Error:", e)
