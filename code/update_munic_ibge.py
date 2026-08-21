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
    
    csv_path = os.path.join(parent_dir, 'munic.csv')
    if not os.path.isfile(csv_path):
        print(f"Error: munic.csv not found at {csv_path}")
        sys.exit(1)
        
    # 2. Load and prepare data from CSV
    print(f"Reading CSV file: {csv_path}...")
    df_csv = pd.read_csv(csv_path)
    df_csv.columns = [col.strip() for col in df_csv.columns]
    
    # Ensure correct data types
    df_csv['codigo'] = df_csv['codigo'].astype(int)
    df_csv['cd_mun'] = df_csv['cd_mun'].astype(int)
    df_csv['descricao'] = df_csv['descricao'].astype(str)
    
    print(f"Loaded {len(df_csv)} records from CSV.")
    
    # 3. Connect to PostgreSQL database
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
            
            # Step 1: Truncate table
            print("Truncating table public.munic...")
            cur.execute('TRUNCATE TABLE public.munic;')
            
            # Step 2: Insert data into public.munic
            print("Inserting records from CSV into public.munic...")
            insert_query = "INSERT INTO public.munic (codigo, descricao, cd_mun) VALUES %s"
            data_tuples = list(df_csv.itertuples(index=False, name=None))
            execute_values(cur, insert_query, data_tuples)
            
            # Step 3: Verify the results
            print("Verifying database changes...")
            cur.execute('SELECT COUNT(*) FROM public.munic;')
            row_count_after = cur.fetchone()[0]
            print(f"Row count in public.munic after update: {row_count_after}")
            
            cur.execute('SELECT COUNT(*) FROM public.munic WHERE cd_mun IS NULL;')
            null_count = cur.fetchone()[0]
            if null_count > 0:
                print(f"WARNING: {null_count} rows still have NULL cd_mun value!")
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

