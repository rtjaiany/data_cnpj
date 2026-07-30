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
    
    file_poi = os.path.join(parent_dir, "business_poi_sp.csv")
    file_sp = os.path.join(parent_dir, "business_sp.csv")
    
    print(f"Connecting to database '{db_name}' on '{db_host}'...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    
    # Target headers in English with secao and desc_secao_en added after primary_cnae
    headers_poi = [
        "cnpj_basic", "cnpj_order", "cnpj_dv", "simples_option", "simples_option_date",
        "mei_option", "mei_option_date", "mei_exclusion_date", "headquarters_branch_identifier",
        "trade_name", "registration_status", "registration_status_date", "registration_status_reason",
        "activity_start_date", "primary_cnae", "secao", "desc_secao_en", "company_size", "legal_name",
        "legal_nature", "responsible_qualification", "share_capital", "city_code", "city",
        "street_type", "street_name", "number", "neighborhood", "zip_code", "latitude", "longitude"
    ]
    
    headers_sp = headers_poi[:-2]  # Omit latitude and longitude
    
    try:
        with conn.cursor() as setup_cur:
            # Increase work_mem to speed up large join and hash operations
            setup_cur.execute("SET work_mem = '256MB';")
            setup_cur.execute("SET temp_buffers = '256MB';")
            setup_cur.execute("SET enable_nestloop = off;")
            
        # ==========================================
        # STEP 1: Export business_poi_sp.csv (SP with lat/long)
        # ==========================================
        print("\n--- STEP 1: Exporting business_poi_sp.csv (geocoded only) ---")
        start_poi = time.time()
        
        query_poi = """
            SELECT 
                e.cnpj_basico, 
                e.cnpj_ordem, 
                e.cnpj_dv, 
                s.opcao_pelo_simples, 
                s.data_opcao_simples, 
                s.opcao_mei, 
                s.data_opcao_mei, 
                s.data_exclusao_mei, 
                e.identificador_matriz_filial, 
                e.nome_fantasia,  
                e.situacao_cadastral, 
                e.data_situacao_cadastral, 
                e.motivo_situacao_cadastral, 
                e.data_inicio_atividade, 
                e.cnae_fiscal_principal, 
                c.secao_cnae AS secao,
                c.desc_secao_en AS desc_secao_en,
                em.porte_empresa, 
                em.razao_social, 
                em.natureza_juridica, 
                em.qualificacao_responsavel, 
                em.capital_social, 
                m.cd_mun, 
                m.descricao AS city, 
                e.tipo_logradouro, 
                e.logradouro, 
                e.numero, 
                e.bairro, 
                e.cep, 
                g.latitude, 
                g.longitude
            FROM public.estabelecimento e
            LEFT JOIN public.empresa em ON e.cnpj_basico = em.cnpj_basico
            LEFT JOIN public.simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN public.munic m ON e.municipio = m.codigo
            LEFT JOIN public.cnae c ON e.cnae_fiscal_principal = c.codigo::integer
            JOIN public.geoloc g ON e.cnpj_basico = g.cnpj_basico AND e.cep = g.cep
            WHERE e.uf = 'SP'
              AND g.latitude IS NOT NULL
              AND g.longitude IS NOT NULL;
        """
        
        cur_poi = conn.cursor('poi_stream_cursor')
        cur_poi.itersize = 50000
        cur_poi.execute(query_poi)
        
        row_count_poi = 0
        with open(file_poi, 'w', newline='', encoding='utf-8') as f_poi:
            writer_poi = csv.writer(f_poi)
            writer_poi.writerow(headers_poi)
            
            for row in cur_poi:
                writer_poi.writerow(row)
                row_count_poi += 1
                if row_count_poi % 1000000 == 0:
                    print(f"Exported {row_count_poi} rows to business_poi_sp.csv...")
                    
        cur_poi.close()
        end_poi = time.time()
        print(f"business_poi_sp.csv export complete!")
        print(f"Rows exported: {row_count_poi}")
        print(f"Time taken: {end_poi - start_poi:.2f} seconds.")
        print(f"File size: {os.path.getsize(file_poi) / (1024*1024):.1f} MB")
        
        # ==========================================
        # STEP 2: Export business_sp.csv (Complete SP, no lat/long filter/columns)
        # ==========================================
        print("\n--- STEP 2: Exporting business_sp.csv (complete SP base) ---")
        start_sp = time.time()
        
        query_sp = """
            SELECT 
                e.cnpj_basico, 
                e.cnpj_ordem, 
                e.cnpj_dv, 
                s.opcao_pelo_simples, 
                s.data_opcao_simples, 
                s.opcao_mei, 
                s.data_opcao_mei, 
                s.data_exclusao_mei, 
                e.identificador_matriz_filial, 
                e.nome_fantasia,  
                e.situacao_cadastral, 
                e.data_situacao_cadastral, 
                e.motivo_situacao_cadastral, 
                e.data_inicio_atividade, 
                e.cnae_fiscal_principal, 
                c.secao_cnae AS secao,
                c.desc_secao_en AS desc_secao_en,
                em.porte_empresa, 
                em.razao_social, 
                em.natureza_juridica, 
                em.qualificacao_responsavel, 
                em.capital_social, 
                m.cd_mun, 
                m.descricao AS city, 
                e.tipo_logradouro, 
                e.logradouro, 
                e.numero, 
                e.bairro, 
                e.cep
            FROM public.estabelecimento e
            LEFT JOIN public.empresa em ON e.cnpj_basico = em.cnpj_basico
            LEFT JOIN public.simples s ON e.cnpj_basico = s.cnpj_basico
            LEFT JOIN public.munic m ON e.municipio = m.codigo
            LEFT JOIN public.cnae c ON e.cnae_fiscal_principal = c.codigo::integer
            WHERE e.uf = 'SP';
        """
        
        cur_sp = conn.cursor('sp_stream_cursor')
        cur_sp.itersize = 50000
        cur_sp.execute(query_sp)
        
        row_count_sp = 0
        with open(file_sp, 'w', newline='', encoding='utf-8') as f_sp:
            writer_sp = csv.writer(f_sp)
            writer_sp.writerow(headers_sp)
            
            for row in cur_sp:
                writer_sp.writerow(row)
                row_count_sp += 1
                if row_count_sp % 1000000 == 0:
                    print(f"Exported {row_count_sp} rows to business_sp.csv...")
                    
        cur_sp.close()
        end_sp = time.time()
        print(f"business_sp.csv export complete!")
        print(f"Rows exported: {row_count_sp}")
        print(f"Time taken: {end_sp - start_sp:.2f} seconds.")
        print(f"File size: {os.path.getsize(file_sp) / (1024*1024):.1f} MB")
        
        print("\nAll exports completed successfully!")
        
    except Exception as e:
        print(f"\nError occurred during export: {e}")
        sys.exit(1)
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == '__main__':
    main()
