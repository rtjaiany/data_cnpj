import os
import sys
import time
import pathlib
import psycopg2
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq

# Define PyArrow schema matching database types
schema_panel = pa.schema([
    ('cnpj_basico', pa.string()),
    ('cnpj_ordem', pa.string()),
    ('cnpj_dv', pa.string()),
    ('identificador_matriz_filial', pa.int32()),
    ('situacao_cadastral', pa.int32()),
    ('data_situacao_cadastral', pa.int32()),
    ('motivo_situacao_cadastral', pa.int32()),
    ('pais', pa.string()),
    ('data_inicio_atividade', pa.int32()),
    ('cnae_fiscal_principal', pa.int32()),
    ('cnae_fiscal_secundaria', pa.string()),
    ('uf', pa.string()),
    ('municipio', pa.int32()),
    ('situacao_especial', pa.string()),
    ('data_situacao_especial', pa.int32()),
    ('capital_social', pa.float64()),
    ('natureza_juridica', pa.int32()),
    ('porte_empresa', pa.int32()),
    ('qualificacao_responsavel', pa.int32()),
    ('ente_federativo_responsavel', pa.string()),
    ('opcao_pelo_simples', pa.string()),
    ('data_opcao_simples', pa.int32()),
    ('data_exclusao_simples', pa.int32()),
    ('opcao_mei', pa.string()),
    ('data_opcao_mei', pa.int32()),
    ('data_exclusao_mei', pa.int32()),
    ('qtde_socios', pa.int32()),
    ('qtde_socios_pf', pa.int32()),
    ('qtde_socios_pj', pa.int32()),
    ('qtde_socios_estrangeiro', pa.int32()),
    ('min_faixa_etaria', pa.int32()),
    ('max_faixa_etaria', pa.int32()),
    ('data_entrada_antiga', pa.int32()),
    ('data_entrada_recente', pa.int32()),
    ('qtde_administradores', pa.int32()),
    ('cd_mun', pa.int32())
])

def export_month_to_parquet(conn, ref_month, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    parquet_file = os.path.join(output_dir, "part-000.parquet")
    
    query = """
        SELECT 
            cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, situacao_cadastral,
            data_situacao_cadastral, motivo_situacao_cadastral, pais, data_inicio_atividade,
            cnae_fiscal_principal, cnae_fiscal_secundaria, uf, municipio, situacao_especial,
            data_situacao_especial, capital_social, natureza_juridica, porte_empresa,
            qualificacao_responsavel, ente_federativo_responsavel, opcao_pelo_simples,
            data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei,
            data_exclusao_mei, qtde_socios, qtde_socios_pf, qtde_socios_pj,
            qtde_socios_estrangeiro, min_faixa_etaria, max_faixa_etaria,
            data_entrada_antiga, data_entrada_recente, qtde_administradores, cd_mun
        FROM analytics.establishment_panel
        WHERE reference_month = %s
        ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv
    """
    
    cursor_name = "stream_cur_" + str(int(time.time() * 1000))
    chunk_size = 100000
    
    print(f"Exporting {ref_month} to {parquet_file}...")
    start_time = time.time()
    
    row_count = 0
    writer = None
    
    try:
        with conn.cursor(name=cursor_name) as stream_cur:
            stream_cur.itersize = chunk_size
            stream_cur.execute(query, (ref_month,))
            
            while True:
                rows = stream_cur.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Format each column data list to match schema types
                cols_data = {col: [row[idx] for row in rows] for idx, col in enumerate(schema_panel.names)}
                
                batch = pa.RecordBatch.from_pydict(cols_data, schema=schema_panel)
                table = pa.Table.from_batches([batch])
                
                if writer is None:
                    writer = pq.ParquetWriter(parquet_file, schema_panel, compression='snappy')
                
                writer.write_table(table)
                row_count += len(rows)
                sys.stdout.write(f"\rExported {row_count} rows for {ref_month}...")
                sys.stdout.flush()
                
        print(f"\nFinished {ref_month} in {time.time() - start_time:.2f} seconds. Total rows: {row_count}")
    finally:
        if writer is not None:
            writer.close()

def main():
    # Load configuration
    current_path = pathlib.Path().resolve()
    dotenv_path = os.path.join(current_path, ".env")
    if not os.path.isfile(dotenv_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dotenv_path = os.path.join(os.path.dirname(script_dir), ".env")
        
    if not os.path.isfile(dotenv_path):
        print("Error: .env file not found!")
        sys.exit(1)
        
    print("Using .env file at:", dotenv_path)
    load_dotenv(dotenv_path=dotenv_path)
    
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, host=db_host, port=db_port, password=db_pass
    )
    
    try:
        with conn.cursor() as cur:
            # Query distinct reference months present in the panel
            cur.execute("SELECT DISTINCT reference_month FROM analytics.establishment_panel ORDER BY reference_month")
            months = [row[0] for row in cur.fetchall()]
            
        if not months:
            print("No reconstructed panel data found in table analytics.establishment_panel!")
            sys.exit(0)
            
        print(f"Found panel data for months: {months}")
        
        output_base_dir = os.path.join(os.path.dirname(dotenv_path), "reconstructed_panel")
        print(f"Target base directory: {output_base_dir}")
        
        for month in months:
            month_dir = os.path.join(output_base_dir, f"reference_month={month}")
            export_month_to_parquet(conn, month, month_dir)
            
        print("\nAll months successfully exported to partitioned Parquet dataset!")
    except Exception as e:
        print(f"\nError occurred: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
