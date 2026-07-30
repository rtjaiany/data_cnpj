import os
import sys
import time
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
    
    print(f"Connecting to database '{db_name}' on '{db_host}'...")
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    
    start_time = time.time()
    
    try:
        with conn.cursor() as cur:
            # Step 1: Alter table to add columns
            print("Adding columns (secao_cnae, desc_secao, desc_secao_en) if they do not exist...")
            cur.execute('ALTER TABLE public.cnae ADD COLUMN IF NOT EXISTS secao_cnae VARCHAR(2);')
            cur.execute('ALTER TABLE public.cnae ADD COLUMN IF NOT EXISTS desc_secao VARCHAR(255);')
            cur.execute('ALTER TABLE public.cnae ADD COLUMN IF NOT EXISTS desc_secao_en VARCHAR(255);')
            conn.commit()
            
            # Step 2: Populate division code
            print("Populating secao_cnae column from first 2 digits of codigo...")
            cur.execute('UPDATE public.cnae SET secao_cnae = SUBSTRING(codigo, 1, 2);')
            print(f"Updated division codes for {cur.rowcount} rows.")
            conn.commit()
            
            # Step 3: Populate Portuguese descriptions
            print("Populating desc_secao (PT) based on division ranges...")
            query_pt = """
                UPDATE public.cnae
                SET desc_secao = CASE
                    WHEN secao_cnae BETWEEN '01' AND '03' THEN 'Agricultura, pecuaria, producao florestal, pesca e aquicultura'
                    WHEN secao_cnae BETWEEN '05' AND '09' THEN 'Industrias extrativas'
                    WHEN secao_cnae BETWEEN '10' AND '33' THEN 'Industrias de transformacao'
                    WHEN secao_cnae = '35' THEN 'Eletricidade e gas'
                    WHEN secao_cnae BETWEEN '36' AND '39' THEN 'Agua, esgoto, atividades de gestao de residuos e descontaminacao'
                    WHEN secao_cnae BETWEEN '41' AND '43' THEN 'Construcao'
                    WHEN secao_cnae BETWEEN '45' AND '47' THEN 'Comercio; reparacao de veiculos automotores e motocicletas'
                    WHEN secao_cnae BETWEEN '49' AND '53' THEN 'Transporte, armazenagem e correio'
                    WHEN secao_cnae BETWEEN '55' AND '56' THEN 'Alojamento e alimentacao'
                    WHEN secao_cnae BETWEEN '58' AND '63' THEN 'Informacao e comunicacao'
                    WHEN secao_cnae BETWEEN '64' AND '66' THEN 'Atividades financeiras, de seguros e servicos relacionados'
                    WHEN secao_cnae = '68' THEN 'Atividades imobiliarias'
                    WHEN secao_cnae BETWEEN '69' AND '75' THEN 'Atividades profissionais, cientificas e tecnicas'
                    WHEN secao_cnae BETWEEN '77' AND '82' THEN 'Atividades administrativas e serviços complementares'
                    WHEN secao_cnae = '84' THEN 'Administracao publica, defesa e seguridade social'
                    WHEN secao_cnae = '85' THEN 'Educacao'
                    WHEN secao_cnae BETWEEN '86' AND '88' THEN 'Saude humana e servicos sociais'
                    WHEN secao_cnae BETWEEN '90' AND '93' THEN 'Artes, cultura, esporte e recreacao'
                    WHEN secao_cnae BETWEEN '94' AND '96' THEN 'Outras atividades de servicos'
                    WHEN secao_cnae = '97' THEN 'Servicos domesticos'
                    WHEN secao_cnae = '99' THEN 'Organismos internacionais e outras instituicoes extraterritoriais'
                    ELSE 'Outras' 
                END;
            """
            cur.execute(query_pt)
            print(f"Updated PT descriptions for {cur.rowcount} rows.")
            conn.commit()
            
            # Step 4: Populate English descriptions
            print("Populating desc_secao_en (EN) based on division ranges...")
            query_en = """
                UPDATE public.cnae
                SET desc_secao_en = CASE
                    WHEN secao_cnae BETWEEN '01' AND '03' THEN 'Agriculture, forestry, fishing and aquaculture'
                    WHEN secao_cnae BETWEEN '05' AND '09' THEN 'Mining and quarrying'
                    WHEN secao_cnae BETWEEN '10' AND '33' THEN 'Manufacturing'
                    WHEN secao_cnae = '35' THEN 'Electricity, gas, steam and air conditioning supply'
                    WHEN secao_cnae BETWEEN '36' AND '39' THEN 'Water supply; sewerage, waste management and remediation activities'
                    WHEN secao_cnae BETWEEN '41' AND '43' THEN 'Construction'
                    WHEN secao_cnae BETWEEN '45' AND '47' THEN 'Wholesale and retail trade; repair of motor vehicles and motorcycles'
                    WHEN secao_cnae BETWEEN '49' AND '53' THEN 'Transportation and storage'
                    WHEN secao_cnae BETWEEN '55' AND '56' THEN 'Accommodation and food service activities'
                    WHEN secao_cnae BETWEEN '58' AND '63' THEN 'Information and communication'
                    WHEN secao_cnae BETWEEN '64' AND '66' THEN 'Financial and insurance activities'
                    WHEN secao_cnae = '68' THEN 'Real estate activities'
                    WHEN secao_cnae BETWEEN '69' AND '75' THEN 'Professional, scientific and technical activities'
                    WHEN secao_cnae BETWEEN '77' AND '82' THEN 'Administrative and support service activities'
                    WHEN secao_cnae = '84' THEN 'Public administration and defence; compulsory social security'
                    WHEN secao_cnae = '85' THEN 'Education'
                    WHEN secao_cnae BETWEEN '86' AND '88' THEN 'Human health and social work activities'
                    WHEN secao_cnae BETWEEN '90' AND '93' THEN 'Arts, entertainment and recreation'
                    WHEN secao_cnae BETWEEN '94' AND '96' THEN 'Other service activities'
                    WHEN secao_cnae = '97' THEN 'Domestic services'
                    WHEN secao_cnae = '99' THEN 'Activities of extraterritorial organizations and bodies'
                    ELSE 'Others' 
                END;
            """
            cur.execute(query_en)
            print(f"Updated EN descriptions for {cur.rowcount} rows.")
            conn.commit()
            
            # Step 5: Verification check
            print("\nRunning verification query (first 10 rows)...")
            cur.execute('SELECT codigo, secao_cnae, desc_secao, desc_secao_en FROM public.cnae LIMIT 10;')
            print("codigo  | secao | desc_secao (PT) | desc_secao_en (EN)")
            print("--------+-------+-----------------+-------------------")
            for row in cur.fetchall():
                # Truncate descriptions slightly for printing clarity
                pt_desc = row[2][:30] + "..." if row[2] and len(row[2]) > 30 else row[2]
                en_desc = row[3][:30] + "..." if row[3] and len(row[3]) > 30 else row[3]
                print(f"{row[0]:<7} | {row[1]:<5} | {pt_desc:<33} | {en_desc}")
                
        end_time = time.time()
        print(f"\nUpdate complete! Time taken: {end_time - start_time:.2f} seconds.")
        
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
