import os
import sys
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

def main():
    # 1. Load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    dotenv_path = os.path.join(parent_dir, '.env')
    
    if not os.path.isfile(dotenv_path):
        print(f"Error: .env file not found at {dotenv_path}")
        sys.exit(1)
        
    print(f"Loading environment from: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)
    
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASSWORD', '')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    
    csv_path = os.path.join(parent_dir, 'municipios.csv')
    if not os.path.isfile(csv_path):
        print(f"Error: municipios.csv not found at {csv_path}")
        sys.exit(1)
        
    # 2. Load and prepare mapping from CSV
    print(f"Reading CSV file: {csv_path}...")
    df_csv = pd.read_csv(csv_path, sep=';', encoding='latin1')
    df_csv.columns = [col.strip() for col in df_csv.columns]
    
    # Select mapping columns and drop duplicates if any
    mapping = df_csv[['CÓDIGO DO MUNICÍPIO - TOM', 'CÓDIGO DO MUNICÍPIO - IBGE']].copy()
    mapping.columns = ['codigo', 'cd_mun']
    mapping = mapping.drop_duplicates(subset=['codigo'])
    
    # 3. Add manual entry for newly created/installed municipality
    # Boa Esperança do Norte (MT) has TOM code 1182 and IBGE code 5101837
    if 1182 not in mapping['codigo'].values:
        print("Adding manual entry for 'BOA ESPERANCA DO NORTE' (TOM: 1182, IBGE: 5101837)")
        new_row = pd.DataFrame([{'codigo': 1182, 'cd_mun': 5101837}])
        mapping = pd.concat([mapping, new_row], ignore_index=True)
        
    # Ensure columns are integer
    mapping['codigo'] = mapping['codigo'].astype(int)
    mapping['cd_mun'] = mapping['cd_mun'].astype(int)
    
    print(f"Mapping size: {len(mapping)} records.")
    
    # 4. Connect to PostgreSQL database
    print(f"Connecting to database '{db_name}' on '{db_host}'...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    
    try:
        with conn.cursor() as cur:
            # Check row count in public.munic before proceeding
            cur.execute('SELECT COUNT(*) FROM public.munic;')
            row_count_before = cur.fetchone()[0]
            print(f"Current row count in public.munic: {row_count_before}")
            
            # Step 1: Add the column if it doesn't exist
            print("Altering table to add 'cd_mun' column...")
            cur.execute('ALTER TABLE public.munic ADD COLUMN IF NOT EXISTS cd_mun integer;')
            
            # Step 2: Create temporary table for mapping
            print("Creating temporary staging table...")
            cur.execute('CREATE TEMP TABLE temp_muni_map (codigo integer PRIMARY KEY, cd_mun integer);')
            
            # Step 3: Insert mapping into temporary table
            print("Inserting mapping into temporary table...")
            insert_query = "INSERT INTO temp_muni_map (codigo, cd_mun) VALUES %s"
            data_tuples = list(mapping.itertuples(index=False, name=None))
            execute_values(cur, insert_query, data_tuples)
            
            # Step 4: Perform the bulk update
            print("Updating public.munic with IBGE codes...")
            update_query = """
                UPDATE public.munic m
                SET cd_mun = tm.cd_mun
                FROM temp_muni_map tm
                WHERE m.codigo = tm.codigo;
            """
            cur.execute(update_query)
            updated_rows = cur.rowcount
            print(f"Updated {updated_rows} rows.")
            
            # Step 5: Verify the results
            print("Verifying database changes...")
            
            # Check if there are any NULL values remaining in cd_mun
            cur.execute('SELECT COUNT(*), string_agg(descricao || \' (\' || codigo || \')\', \', \') FROM public.munic WHERE cd_mun IS NULL;')
            null_count, null_list = cur.fetchone()
            
            if null_count > 0:
                print(f"WARNING: {null_count} rows still have NULL cd_mun value!")
                print(f"Unmapped rows: {null_list}")
            else:
                print("Success: All rows have been mapped to non-null IBGE codes.")
                
            # Print sample rows
            cur.execute('SELECT codigo, descricao, cd_mun FROM public.munic ORDER BY codigo LIMIT 10;')
            print("\nSample rows after update:")
            print("codigo | descricao | cd_mun")
            print("-------+-----------+-------")
            for row in cur.fetchall():
                print(f"{row[0]:<6} | {row[1]:<25} | {row[2]}")
                
        # Commit transaction
        conn.commit()
        print("\nTransaction committed successfully.")
        
    except Exception as e:
        conn.rollback()
        print(f"\nError occurred: {e}")
        print("Transaction rolled back.")
        sys.exit(1)
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    main()
