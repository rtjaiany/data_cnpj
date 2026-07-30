import os
import sys
import time
import csv
import psycopg2
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
    
    file_summary = os.path.join(parent_dir, "business_sp_by_city.csv")
    
    print(f"Connecting to database '{db_name}' on '{db_host}'...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    
    start_time = time.time()
    
    query = """
        SELECT 
            m.cd_mun AS city_code,
            m.descricao AS city,
            COUNT(CASE WHEN e.situacao_cadastral = 2 THEN 1 END) AS active,
            COUNT(CASE WHEN e.situacao_cadastral = 8 THEN 1 END) AS failed,
            COUNT(CASE WHEN e.situacao_cadastral = 2 AND e.identificador_matriz_filial = 1 THEN 1 END) AS active_head_offices,
            COUNT(CASE WHEN e.situacao_cadastral = 8 AND e.identificador_matriz_filial = 1 THEN 1 END) AS failed_head_offices,
            COUNT(CASE WHEN e.situacao_cadastral = 2 AND e.identificador_matriz_filial = 2 THEN 1 END) AS active_branches,
            COUNT(CASE WHEN e.situacao_cadastral = 8 AND e.identificador_matriz_filial = 2 THEN 1 END) AS failed_branches
        FROM public.estabelecimento e
        LEFT JOIN public.munic m ON e.municipio = m.codigo
        WHERE e.uf = 'SP'
        GROUP BY m.cd_mun, m.descricao
        ORDER BY m.descricao;
    """
    
    headers = [
        "city_code", "city", "active", "failed", 
        "active_head_offices", "failed_head_offices", 
        "active_branches", "failed_branches"
    ]
    
    try:
        with conn.cursor() as cur:
            # Optimize join behavior
            cur.execute("SET work_mem = '256MB';")
            cur.execute("SET enable_nestloop = off;")
            
            print("Executing city aggregation query...")
            cur.execute(query)
            
            rows = cur.fetchall()
            print(f"Aggregation complete. Writing {len(rows)} rows to business_sp_by_city.csv...")
            
            with open(file_summary, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
                
        end_time = time.time()
        print(f"\nExport complete! Time taken: {end_time - start_time:.2f} seconds.")
        print(f"File created: {file_summary}")
        print(f"File size: {os.path.getsize(file_summary) / 1024:.2f} KB")
        
    except Exception as e:
        print(f"\nError occurred: {e}")
        sys.exit(1)
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    main()
