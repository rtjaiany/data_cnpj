import os
import sys
import time
import gc
import pathlib
import zipfile
import csv
from io import StringIO
import xml.etree.ElementTree as ET
import requests
import psycopg2
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq

# Helpers to create directories
def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Custom stream writer to stream files chunk-by-chunk using PostgreSQL COPY
def psql_insert_copy(cur, table_name, df_iter, keys):
    s_buf = StringIO()
    writer = csv.writer(s_buf)
    writer.writerows(df_iter)
    s_buf.seek(0)
    columns = ", ".join([f'"{k}"' for k in keys])
    sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV NULL ' + r"'\N'"
    cur.copy_expert(sql=sql, file=s_buf)

# Chunked loading from CSV directly into staging
def load_csv_to_staging(cur, file_path, table_name, dtypes, columns, delimiter=";"):
    import pandas as pd
    chunksize = 200000
    print(f"Loading {file_path} into staging_{table_name}...")
    
    count = 0
    for chunk in pd.read_csv(
        file_path,
        sep=delimiter,
        header=None,
        dtype=dtypes,
        encoding="latin-1",
        chunksize=chunksize,
        on_bad_lines='skip'
    ):
        chunk.columns = columns
        
        # Specific cleanup
        if table_name == "empresa":
            # Normalize capital_social
            chunk["capital_social"] = chunk["capital_social"].apply(
                lambda x: str(x).replace(",", ".") if pd.notnull(x) else x
            )
            chunk["capital_social"] = chunk["capital_social"].astype(float)
        elif table_name == "socios":
            # Ensure nome_socio_razao_social is never NaN/NULL to prevent primary key constraint violations
            chunk["nome_socio_razao_social"] = chunk["nome_socio_razao_social"].fillna("")
            
        # Convert to object and replace nulls with '\N' to output correct empty strings for NULLs
        chunk = chunk.astype(object).where(pd.notnull(chunk), r"\N")

        # Convert to list of tuples for COPY
        data_iter = [tuple(x) for x in chunk.itertuples(index=False)]
        psql_insert_copy(cur, f"staging_{table_name}", data_iter, columns)
        
        count += len(chunk)
        sys.stdout.write(f"\rImported {count} rows into staging_{table_name}...")
        sys.stdout.flush()
        
        del chunk
        gc.collect()
    print(f"\nFinished importing {count} rows.")
    return count

# PyArrow schemas for ignored fields
schema_empresa = pa.schema([
    ('cnpj_basico', pa.string()),
    ('reference_month', pa.string()),
    ('razao_social', pa.string())
])

schema_estabelecimento = pa.schema([
    ('cnpj_basico', pa.string()),
    ('cnpj_ordem', pa.string()),
    ('cnpj_dv', pa.string()),
    ('reference_month', pa.string()),
    ('nome_fantasia', pa.string()),
    ('ddd_1', pa.string()),
    ('ddd_2', pa.string()),
    ('ddd_fax', pa.string()),
    ('telefone_1', pa.string()),
    ('telefone_2', pa.string()),
    ('fax', pa.string()),
    ('correio_eletronico', pa.string())
])

schema_estabelecimento_enderecos = pa.schema([
    ('cnpj_basico', pa.string()),
    ('cnpj_ordem', pa.string()),
    ('cnpj_dv', pa.string()),
    ('reference_month', pa.string()),
    ('tipo_logradouro', pa.string()),
    ('logradouro', pa.string()),
    ('numero', pa.string()),
    ('complemento', pa.string()),
    ('bairro', pa.string()),
    ('cep', pa.string()),
    ('nome_cidade_exterior', pa.string())
])

def export_ignored_fields_parquet(cur, conn, query, schema, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    parquet_file = os.path.join(output_dir, "part-000.parquet")
    
    cursor_name = "cur_" + str(int(time.time() * 1000))
    with conn.cursor(name=cursor_name) as stream_cur:
        stream_cur.itersize = 50000
        stream_cur.execute(query)
        
        writer = None
        try:
            while True:
                rows = stream_cur.fetchmany(50000)
                if not rows:
                    break
                
                cols_data = {col: [row[idx] for row in rows] for idx, col in enumerate(schema.names)}
                batch = pa.RecordBatch.from_pydict(cols_data, schema=schema)
                table = pa.Table.from_batches([batch])
                
                if writer is None:
                    writer = pq.ParquetWriter(parquet_file, schema, compression='snappy')
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()

def main():
    try:
        # 1. Load configuration
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
    
        output_files = os.getenv("OUTPUT_FILES_PATH")
        extracted_files = os.getenv("EXTRACTED_FILES_PATH")
        makedirs(output_files)
        makedirs(extracted_files)
    
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASSWORD", "")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
    
        # 2. Get target month from CLI arguments or .env
        if len(sys.argv) > 1:
            data_folder = sys.argv[1].strip()
            print(f"Reference month provided via argument: {data_folder}")
        else:
            data_folder = os.getenv("NEXTCLOUD_FOLDER", "2023-06")
            print(f"Reference month fallback to config: {data_folder}")
        
        # Check if download month changed and clean up old ZIPs to prevent corrupt resumes
        download_month_file = os.path.join(output_files, ".download_month")
        current_download_month = None
        if os.path.exists(download_month_file):
            try:
                with open(download_month_file, "r") as f:
                    current_download_month = f.read().strip()
            except Exception:
                pass

        if current_download_month != data_folder:
            print(f"Target month changed ({current_download_month} -> {data_folder}). Cleaning up old zip files from {output_files}...")
            for f in os.listdir(output_files):
                if f.lower().endswith(".zip"):
                    try:
                        os.remove(os.path.join(output_files, f))
                    except Exception as e:
                        print(f"Error removing old ZIP file {f}: {e}")

        # Reference month representation in YYYY-MM format
        ref_month_date = data_folder
        
        # Collection date represents the download start time
        collection_date = datetime_now = time.strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()
    
        # 3. Connect to database
        conn = psycopg2.connect(
            dbname=db_name, user=db_user, host=db_host, port=db_port, password=db_pass
        )
        cur = conn.cursor()
    
        # Check if this reference month has already been processed successfully
        cur.execute(
            "SELECT status FROM snapshots_metadata WHERE reference_month = %s",
            (ref_month_date,)
        )
        res = cur.fetchone()
        if res and res[0] == 'SUCCESS':
            print(f"Snapshot month {data_folder} has already been successfully processed. Exiting.")
            cur.close()
            conn.close()
            sys.exit(0)
        
        # 4. WebDAV connection details
        share_url = os.getenv(
            "NEXTCLOUD_SHARE_URL",
            "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9"
        )
        token = share_url.rstrip("/").split("/")[-1]
        webdav_base = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
        webdav_url = f"{webdav_base}{data_folder}/"
    
        print(f"Listing WebDAV files for {data_folder} from: {webdav_url}")
        headers = {"X-Requested-With": "XMLHttpRequest", "Depth": "1"}
        response = requests.request("PROPFIND", webdav_url, auth=(token, ""), headers=headers)
        if response.status_code != 207:
            print(f"Error: Cannot access folder {data_folder} on Nextcloud server! Status: {response.status_code}")
            cur.close()
            conn.close()
            sys.exit(1)
        
        root = ET.fromstring(response.content)
        ns = {"d": "DAV:"}
        files_to_download = []
        for resp in root.findall(".//d:response", ns):
            href = resp.find("d:href", ns)
            if href is not None:
                path = href.text
                if path.endswith(".zip"):
                    files_to_download.append(os.path.basename(path))
                
        files_to_download.sort()
        print(f"Found {len(files_to_download)} files in Nextcloud WebDAV directory.")
    
        # Save checkpoint month info
        download_month_file = os.path.join(output_files, ".download_month")
        with open(download_month_file, "w") as f:
            f.write(data_folder)
        
        # 5. Incremental Download and Load Loop
        # We will ensure staging tables exist matching targets
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        existing_tables = [t[0] for t in cur.fetchall()]
    
        tables_to_create = ["empresa", "estabelecimento", "socios", "simples"]
        for t in tables_to_create:
            if f"staging_{t}" not in existing_tables:
                print(f"Creating staging_{t} matching table structure...")
                cur.execute(f"CREATE TABLE staging_{t} (LIKE {t});")
                conn.commit()

        # Truncate staging tables once at the start of a fresh month (to accumulate multi-part files)
        cur.execute("SELECT COUNT(*) FROM processed_files WHERE file_path LIKE %s", (f"{data_folder}/%",))
        checkpoint_cnt = cur.fetchone()[0]
        if checkpoint_cnt == 0:
            print(f"Fresh run for {data_folder}. Truncating staging tables...")
            for t in tables_to_create:
                cur.execute(f"TRUNCATE TABLE staging_{t};")
            conn.commit()
            
        for idx, zip_name in enumerate(files_to_download, 1):
            checkpoint_key = f"{data_folder}/{zip_name}"
        
            # Check if already processed
            cur.execute("SELECT EXISTS(SELECT 1 FROM processed_files WHERE file_path = %s)", (checkpoint_key,))
            if cur.fetchone()[0]:
                print(f"File {zip_name} already processed. Skipping.")
                continue
            
            print(f"\nProcessing file ({idx}/{len(files_to_download)}): {zip_name}")
            local_zip_path = os.path.join(output_files, zip_name)
        
            # Resumable download check
            url = f"{webdav_url}{zip_name}"
            headers_prop = {"X-Requested-With": "XMLHttpRequest"}
            try:
                head_res = requests.head(url, auth=(token, ""), headers=headers_prop)
                server_size = int(head_res.headers.get("content-length", 0))
            except Exception as e:
                print(f"Warning: Cannot head server file {zip_name}: {e}")
                server_size = 0
            
            download_needed = True
            resume_header = {}
            downloaded_bytes = 0
            write_mode = "wb"
        
            if server_size > 0 and os.path.exists(local_zip_path):
                local_size = os.path.getsize(local_zip_path)
                if local_size == server_size:
                    print(f"File {zip_name} already downloaded and matches server size.")
                    download_needed = False
                elif local_size < server_size:
                    print(f"Resuming download from {local_size / (1024*1024):.1f} MB...")
                    resume_header = {"Range": f"bytes={local_size}-"}
                    downloaded_bytes = local_size
                    write_mode = "ab"
                else:
                    print(f"Local file size exceeds server size. Restarting download.")
                    try:
                        os.remove(local_zip_path)
                    except:
                        pass
                    
            if download_needed:
                req_headers = {**headers_prop, **resume_header}
                max_retries = 5
                success = False
                for attempt in range(1, max_retries + 1):
                    try:
                        with requests.get(url, auth=(token, ""), headers=req_headers, stream=True) as r:
                            if r.status_code not in [200, 206]:
                                raise Exception(f"HTTP status code {r.status_code}")
                            
                            if r.status_code == 200:
                                write_mode = "wb"
                                downloaded_bytes = 0
                            
                            block_size = 1024 * 1024
                            with open(local_zip_path, write_mode) as f:
                                for chunk in r.iter_content(chunk_size=block_size):
                                    if chunk:
                                        f.write(chunk)
                                        downloaded_bytes += len(chunk)
                                        if server_size > 0:
                                            percent = (downloaded_bytes / server_size) * 100
                                            sys.stdout.write(f"\rDownloading: {percent:.2f}% [{downloaded_bytes/(1024*1024):.1f}MB/{server_size/(1024*1024):.1f}MB]")
                                        else:
                                            sys.stdout.write(f"\rDownloading: {downloaded_bytes/(1024*1024):.1f}MB")
                                        sys.stdout.flush()
                            print()
                            success = True
                            break
                    except Exception as e:
                        print(f"\nDownload attempt {attempt} failed: {e}")
                        if attempt == max_retries:
                            print("Max download retries reached. Exiting.")
                            sys.exit(1)
                        time.sleep(5)
                if not success:
                    sys.exit(1)
                
            # Validate downloaded ZIP
            print(f"Validating ZIP archive: {zip_name}")
            try:
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    bad_file = zip_ref.testzip()
                    if bad_file:
                        raise zipfile.BadZipFile(f"CRC check failed for {bad_file}")
                
                    # Extraction
                    extracted_files_list = zip_ref.namelist()
                    zip_ref.extractall(extracted_files)
            except Exception as e:
                print(f"Error: ZIP file {zip_name} is corrupted or invalid: {e}")
                try:
                    os.remove(local_zip_path)
                except:
                    pass
                sys.exit(1)
            
            # Import extracted files
            for extracted_name in extracted_files_list:
                file_path = os.path.join(extracted_files, extracted_name)
            
                # Map file to table schema
                table_name = None
                dtypes = None
                columns = None
            
                upper_name = extracted_name.upper()
                if "EMPRE" in upper_name:
                    table_name = "empresa"
                    dtypes = {0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: object}
                    columns = ["cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel", "capital_social", "porte_empresa", "ente_federativo_responsavel"]
                elif "ESTABELE" in upper_name:
                    table_name = "estabelecimento"
                    dtypes = {
                        0: object, 1: object, 2: object, 3: "Int32", 4: object, 5: "Int32", 6: "Int32", 7: "Int32", 8: object, 9: object,
                        10: "Int32", 11: "Int32", 12: object, 13: object, 14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                        20: "Int32", 21: object, 22: object, 23: object, 24: object, 25: object, 26: object, 27: object, 28: object, 29: "Int32"
                    }
                    columns = [
                        "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia", "situacao_cadastral",
                        "data_situacao_cadastral", "motivo_situacao_cadastral", "nome_cidade_exterior", "pais", "data_inicio_atividade",
                        "cnae_fiscal_principal", "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero", "complemento",
                        "bairro", "cep", "uf", "municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax",
                        "correio_eletronico", "situacao_especial", "data_situacao_especial"
                    ]
                elif "SOCIO" in upper_name:
                    table_name = "socios"
                    dtypes = {0: object, 1: "Int32", 2: object, 3: object, 4: "Int32", 5: "Int32", 6: "Int32", 7: object, 8: object, 9: "Int32", 10: "Int32"}
                    columns = ["cnpj_basico", "identificador_socio", "nome_socio_razao_social", "cpf_cnpj_socio", "qualificacao_socio", "data_entrada_sociedade", "pais", "representante_legal", "nome_do_representante", "qualificacao_representante_legal", "faixa_etaria"]
                elif "SIMPLES" in upper_name:
                    table_name = "simples"
                    dtypes = {0: object, 1: object, 2: "Int32", 3: "Int32", 4: object, 5: "Int32", 6: "Int32"}
                    columns = ["cnpj_basico", "opcao_pelo_simples", "data_opcao_simples", "data_exclusao_simples", "opcao_mei", "data_opcao_mei", "data_exclusao_mei"]
            
                if table_name:
                    # Load CSV using COPY chunk-by-chunk directly into staging table
                    load_csv_to_staging(cur, file_path, table_name, dtypes, columns)
                
                # Clean up unzipped CSV file
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Could not remove extracted file {file_path}: {e}")
                
            # Clean up local ZIP file
            try:
                os.remove(local_zip_path)
            except Exception as e:
                print(f"Could not remove ZIP file {local_zip_path}: {e}")
            
            # Commit staging load and file checkpoint
            cur.execute(
                "INSERT INTO processed_files (file_path) VALUES (%s) ON CONFLICT (file_path) DO NOTHING",
                (checkpoint_key,)
            )
            conn.commit()
            print(f"File {zip_name} successfully imported and committed.")

        # 6. Change Detection and Final Merge
        print("\nStarting set-based change detection and snapshots generation...")
    
        # Staging Validation Checks
        print("Validating snapshot data in staging tables...")
        for t in ["empresa", "estabelecimento", "socios", "simples"]:
            cur.execute(f"SELECT COUNT(*) FROM staging_{t};")
            cnt = cur.fetchone()[0]
            print(f"Staging table staging_{t} has {cnt} rows.")
            if cnt == 0:
                raise ValueError(f"Snapshot validation failed: staging_{t} is empty for reference month {data_folder}")

        total_inserts = 0
        total_updates = 0
        total_deletes = 0

        # ----------------------------------------------------
        # Table: empresa
        # ----------------------------------------------------
        print("\nProcessing table: empresa")
    
        # A. Deduplicate staging_empresa using non-NULL count and tie-breaker ordering, with conflict detection & quarantine
        print("Deduplicating staging_empresa with conflict checking...")
        cur.execute("DROP TABLE IF EXISTS staging_empresa_dedup;")
        cur.execute(f"""
            CREATE TABLE staging_empresa_dedup AS
            WITH company_stats AS (
                SELECT
                    cnpj_basico,
                    COUNT(DISTINCT capital_social) as dc_cap,
                    COUNT(DISTINCT natureza_juridica) as dc_nat,
                    COUNT(DISTINCT porte_empresa) as dc_por,
                    COUNT(DISTINCT qualificacao_responsavel) as dc_qua,
                    COUNT(DISTINCT ente_federativo_responsavel) as dc_ent
                FROM staging_empresa
                GROUP BY cnpj_basico
            ),
            conflicts AS (
                SELECT cnpj_basico
                FROM company_stats
                WHERE dc_cap > 1 OR dc_nat > 1 OR dc_por > 1 OR dc_qua > 1 OR dc_ent > 1
            ),
            -- Insert conflicts into quarantine table
            quarantine_log AS (
                INSERT INTO analytics.quarantine_empresa (cnpj_basico, motivo, reference_month)
                SELECT 
                    s.cnpj_basico,
                    'Conflict in staging company fields: ' || 
                    CASE WHEN s.dc_cap > 1 THEN 'capital_social (' || s.dc_cap || ') ' ELSE '' END ||
                    CASE WHEN s.dc_nat > 1 THEN 'natureza_juridica (' || s.dc_nat || ') ' ELSE '' END ||
                    CASE WHEN s.dc_por > 1 THEN 'porte_empresa (' || s.dc_por || ') ' ELSE '' END ||
                    CASE WHEN s.dc_qua > 1 THEN 'qualificacao_responsavel (' || s.dc_qua || ') ' ELSE '' END ||
                    CASE WHEN s.dc_ent > 1 THEN 'ente_federativo_responsavel (' || s.dc_ent || ') ' ELSE '' END,
                    '{data_folder}'
                FROM company_stats s
                JOIN conflicts c ON s.cnpj_basico = c.cnpj_basico
                ON CONFLICT (cnpj_basico) DO UPDATE 
                SET motivo = EXCLUDED.motivo, reference_month = EXCLUDED.reference_month
                RETURNING cnpj_basico
            ),
            -- Resolve companies: for non-conflicting ones, take MAX/fewest NULLs. For conflicting ones, resolve to NULL (except name).
            resolved AS (
                SELECT
                    e.cnpj_basico,
                    MAX(e.razao_social) AS razao_social,
                    MAX(e.natureza_juridica) AS natureza_juridica,
                    MAX(e.qualificacao_responsavel) AS qualificacao_responsavel,
                    MAX(e.capital_social) AS capital_social,
                    MAX(e.porte_empresa) AS porte_empresa,
                    MAX(e.ente_federativo_responsavel) AS ente_federativo_responsavel
                FROM staging_empresa e
                LEFT JOIN conflicts c ON e.cnpj_basico = c.cnpj_basico
                WHERE c.cnpj_basico IS NULL
                GROUP BY e.cnpj_basico
                
                UNION ALL
                
                SELECT
                    c.cnpj_basico,
                    MAX(e.razao_social) AS razao_social,
                    NULL::integer AS natureza_juridica,
                    NULL::integer AS qualificacao_responsavel,
                    NULL::double precision AS capital_social,
                    NULL::integer AS porte_empresa,
                    NULL::text AS ente_federativo_responsavel
                FROM conflicts c
                JOIN staging_empresa e ON e.cnpj_basico = c.cnpj_basico
                GROUP BY c.cnpj_basico
            )
            SELECT cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel FROM resolved;
        """)
        cur.execute("DROP TABLE staging_empresa;")
        cur.execute("ALTER TABLE staging_empresa_dedup RENAME TO staging_empresa;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_empresa_cnpj ON staging_empresa(cnpj_basico);")
        conn.commit()

        # Export ignored fields to partitioned Parquet for the current month
        print("Exporting ignored fields for empresa to Parquet...")
        query_emp = f"SELECT cnpj_basico, '{data_folder}'::varchar as reference_month, razao_social FROM staging_empresa"
        output_dir_emp = os.path.join(current_path, "ignored_fields", "empresas", f"reference_month={data_folder}")
        export_ignored_fields_parquet(cur, conn, query_emp, schema_empresa, output_dir_emp)
    
        # Check and generate baseline 2023-05 ignored fields if missing
        baseline_empresa_dir = os.path.join(current_path, "ignored_fields", "empresas", "reference_month=2023-05")
        if not os.path.exists(os.path.join(baseline_empresa_dir, "part-000.parquet")):
            print("Extracting baseline 2023-05 ignored fields for empresa...")
            query_baseline_emp = "SELECT cnpj_basico, '2023-05'::varchar as reference_month, razao_social FROM empresa"
            export_ignored_fields_parquet(cur, conn, query_baseline_emp, schema_empresa, baseline_empresa_dir)

        # B. Insert INSERTs and UPDATEs into snapshots
        print("Detecting INSERT and UPDATE events for empresa...")
        cur.execute("""
            INSERT INTO snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'empresa',
                stg.cnpj_basico,
                jsonb_build_object('cnpj_basico', stg.cnpj_basico),
                p.old_row,
                jsonb_build_object(
                    'cnpj_basico', stg.cnpj_basico,
                    'natureza_juridica', stg.natureza_juridica,
                    'qualificacao_responsavel', stg.qualificacao_responsavel,
                    'capital_social', stg.capital_social,
                    'porte_empresa', stg.porte_empresa,
                    'ente_federativo_responsavel', stg.ente_federativo_responsavel
                ),
                CASE WHEN p.is_new THEN 'INSERT' ELSE 'UPDATE' END,
                %s::varchar,
                %s::timestamp
            FROM staging_empresa stg
            LEFT JOIN latest_state_empresa l ON l.cnpj_basico = stg.cnpj_basico
            LEFT JOIN empresa b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    CASE
                        WHEN l.cnpj_basico IS NOT NULL THEN
                            jsonb_build_object(
                                'cnpj_basico', l.cnpj_basico,
                                'natureza_juridica', l.natureza_juridica,
                                'qualificacao_responsavel', l.qualificacao_responsavel,
                                'capital_social', l.capital_social,
                                'porte_empresa', l.porte_empresa,
                                'ente_federativo_responsavel', l.ente_federativo_responsavel
                            )
                        ELSE
                            jsonb_build_object(
                                'cnpj_basico', b.cnpj_basico,
                                'natureza_juridica', b.natureza_juridica,
                                'qualificacao_responsavel', b.qualificacao_responsavel,
                                'capital_social', b.capital_social,
                                'porte_empresa', b.porte_empresa,
                                'ente_federativo_responsavel', b.ente_federativo_responsavel
                            )
                    END as old_row,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.natureza_juridica, b.natureza_juridica) as prev_natureza_juridica,
                    COALESCE(l.qualificacao_responsavel, b.qualificacao_responsavel) as prev_qualificacao_responsavel,
                    COALESCE(l.capital_social, b.capital_social) as prev_capital_social,
                    COALESCE(l.porte_empresa, b.porte_empresa) as prev_porte_empresa,
                    COALESCE(l.ente_federativo_responsavel, b.ente_federativo_responsavel) as prev_ente_federativo_responsavel
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.natureza_juridica IS DISTINCT FROM p.prev_natureza_juridica OR
                        stg.qualificacao_responsavel IS DISTINCT FROM p.prev_qualificacao_responsavel OR
                        stg.capital_social IS DISTINCT FROM p.prev_capital_social OR
                        stg.porte_empresa IS DISTINCT FROM p.prev_porte_empresa OR
                        stg.ente_federativo_responsavel IS DISTINCT FROM p.prev_ente_federativo_responsavel
                    )
                );
        """, (ref_month_date, collection_date))
        emp_snap_count = cur.rowcount
    
        # Query count of inserts vs updates
        cur.execute("""
            SELECT
                COUNT(case when tipo_alteracao = 'INSERT' then 1 end),
                COUNT(case when tipo_alteracao = 'UPDATE' then 1 end)
            FROM snapshots
            WHERE tabela = 'empresa' AND mes_referencia = %s::varchar
        """, (ref_month_date,))
        emp_inserts, emp_updates = cur.fetchone()
        total_inserts += emp_inserts
        total_updates += emp_updates
    
        # C. Upsert changes to latest_state_empresa
        print("Updating latest_state_empresa lookup table...")
        cur.execute("""
            INSERT INTO latest_state_empresa (cnpj_basico, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel, is_deleted, last_updated_month, data_coleta)
            SELECT
                stg.cnpj_basico,
                stg.natureza_juridica,
                stg.qualificacao_responsavel,
                stg.capital_social,
                stg.porte_empresa,
                stg.ente_federativo_responsavel,
                FALSE,
                %s::varchar,
                %s::timestamp
            FROM staging_empresa stg
            LEFT JOIN latest_state_empresa l ON l.cnpj_basico = stg.cnpj_basico
            LEFT JOIN empresa b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.natureza_juridica, b.natureza_juridica) as prev_natureza_juridica,
                    COALESCE(l.qualificacao_responsavel, b.qualificacao_responsavel) as prev_qualificacao_responsavel,
                    COALESCE(l.capital_social, b.capital_social) as prev_capital_social,
                    COALESCE(l.porte_empresa, b.porte_empresa) as prev_porte_empresa,
                    COALESCE(l.ente_federativo_responsavel, b.ente_federativo_responsavel) as prev_ente_federativo_responsavel
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.natureza_juridica IS DISTINCT FROM p.prev_natureza_juridica OR
                        stg.qualificacao_responsavel IS DISTINCT FROM p.prev_qualificacao_responsavel OR
                        stg.capital_social IS DISTINCT FROM p.prev_capital_social OR
                        stg.porte_empresa IS DISTINCT FROM p.prev_porte_empresa OR
                        stg.ente_federativo_responsavel IS DISTINCT FROM p.prev_ente_federativo_responsavel
                    )
                )
            ON CONFLICT (cnpj_basico) DO UPDATE SET
                natureza_juridica = EXCLUDED.natureza_juridica,
                qualificacao_responsavel = EXCLUDED.qualificacao_responsavel,
                capital_social = EXCLUDED.capital_social,
                porte_empresa = EXCLUDED.porte_empresa,
                ente_federativo_responsavel = EXCLUDED.ente_federativo_responsavel,
                is_deleted = FALSE,
                last_updated_month = EXCLUDED.last_updated_month,
                data_coleta = EXCLUDED.data_coleta;
        """, (ref_month_date, collection_date))

        # D. Deletions
        print("Detecting DELETEs for empresa...")
        cur.execute("""
            INSERT INTO snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'empresa',
                prev.cnpj_basico,
                jsonb_build_object('cnpj_basico', prev.cnpj_basico),
                prev.old_row,
                NULL,
                'DELETE',
                %s::varchar,
                %s::timestamp
            FROM (
                SELECT l.cnpj_basico,
                    jsonb_build_object(
                        'cnpj_basico', l.cnpj_basico,
                        'natureza_juridica', l.natureza_juridica,
                        'qualificacao_responsavel', l.qualificacao_responsavel,
                        'capital_social', l.capital_social,
                        'porte_empresa', l.porte_empresa,
                        'ente_federativo_responsavel', l.ente_federativo_responsavel
                    ) as old_row
                FROM latest_state_empresa l
                LEFT JOIN staging_empresa stg ON stg.cnpj_basico = l.cnpj_basico
                WHERE l.is_deleted = FALSE
                  AND stg.cnpj_basico IS NULL
                UNION ALL
                SELECT b.cnpj_basico,
                    jsonb_build_object(
                        'cnpj_basico', b.cnpj_basico,
                        'natureza_juridica', b.natureza_juridica,
                        'qualificacao_responsavel', b.qualificacao_responsavel,
                        'capital_social', b.capital_social,
                        'porte_empresa', b.porte_empresa,
                        'ente_federativo_responsavel', b.ente_federativo_responsavel
                    ) as old_row
                FROM empresa b
                LEFT JOIN latest_state_empresa l ON l.cnpj_basico = b.cnpj_basico
                LEFT JOIN staging_empresa stg ON stg.cnpj_basico = b.cnpj_basico
                WHERE l.cnpj_basico IS NULL
                  AND stg.cnpj_basico IS NULL
            ) prev;
        """, (ref_month_date, collection_date))
        emp_deletes = cur.rowcount
        total_deletes += emp_deletes
    
        # Mark deleted records in latest_state_empresa
        if emp_deletes > 0:
            cur.execute("""
                INSERT INTO latest_state_empresa (cnpj_basico, is_deleted, last_updated_month, data_coleta)
                SELECT
                    prev.cnpj_basico,
                    TRUE,
                    %s::varchar,
                    %s::timestamp
                FROM (
                    SELECT l.cnpj_basico
                    FROM latest_state_empresa l
                    LEFT JOIN staging_empresa stg ON stg.cnpj_basico = l.cnpj_basico
                    WHERE l.is_deleted = FALSE
                      AND stg.cnpj_basico IS NULL
                    UNION
                    SELECT DISTINCT b.cnpj_basico
                    FROM empresa b
                    LEFT JOIN latest_state_empresa l ON l.cnpj_basico = b.cnpj_basico
                    LEFT JOIN staging_empresa stg ON stg.cnpj_basico = b.cnpj_basico
                    WHERE l.cnpj_basico IS NULL
                      AND stg.cnpj_basico IS NULL
                ) prev
                ON CONFLICT (cnpj_basico) DO UPDATE SET
                    is_deleted = TRUE,
                    last_updated_month = EXCLUDED.last_updated_month,
                    data_coleta = EXCLUDED.data_coleta;
            """, (ref_month_date, collection_date))

        # ----------------------------------------------------
        # Table: estabelecimento
        # ----------------------------------------------------
        print("\nProcessing table: estabelecimento")
        
        # A. Deduplicate staging_estabelecimento using non-NULL count and tie-breaker ordering
        print("Deduplicating staging_estabelecimento...")
        cur.execute("DROP TABLE IF EXISTS staging_estabelecimento_dedup;")
        cur.execute("""
            CREATE TABLE staging_estabelecimento_dedup AS
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY cnpj_basico, cnpj_ordem, cnpj_dv
                           ORDER BY
                               (
                                   (situacao_cadastral IS NOT NULL)::int +
                                   (data_situacao_cadastral IS NOT NULL AND data_situacao_cadastral != 0)::int +
                                   (cnae_fiscal_principal IS NOT NULL)::int +
                                   (uf IS NOT NULL)::int +
                                   (municipio IS NOT NULL)::int
                               ) DESC,
                               situacao_cadastral DESC NULLS LAST,
                               data_inicio_atividade DESC NULLS LAST
                       ) as rn
                FROM staging_estabelecimento
            )
            SELECT cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial
            FROM ranked
            WHERE rn = 1;
        """)
        cur.execute("DROP TABLE staging_estabelecimento;")
        cur.execute("ALTER TABLE staging_estabelecimento_dedup RENAME TO staging_estabelecimento;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_estabelecimento_pk ON staging_estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);")
        conn.commit()

        # Export ignored fields to partitioned Parquet for the current month
        print("Exporting ignored fields for estabelecimento to Parquet...")
        query_est = f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '{data_folder}'::varchar as reference_month, nome_fantasia, ddd_1, ddd_2, ddd_fax, telefone_1, telefone_2, fax, correio_eletronico FROM staging_estabelecimento"
        output_dir_est = os.path.join(current_path, "ignored_fields", "estabelecimento", f"reference_month={data_folder}")
        export_ignored_fields_parquet(cur, conn, query_est, schema_estabelecimento, output_dir_est)
    
        print("Exporting address fields for estabelecimento to Parquet...")
        query_est_addr = f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '{data_folder}'::varchar as reference_month, tipo_logradouro, logradouro, numero, complemento, bairro, cep, nome_cidade_exterior FROM staging_estabelecimento"
        output_dir_est_addr = os.path.join(current_path, "ignored_fields", "estabelecimento_enderecos", f"reference_month={data_folder}")
        export_ignored_fields_parquet(cur, conn, query_est_addr, schema_estabelecimento_enderecos, output_dir_est_addr)

        # Check and generate baseline 2023-05 ignored fields if missing
        baseline_estabelecimento_dir = os.path.join(current_path, "ignored_fields", "estabelecimento", "reference_month=2023-05")
        if not os.path.exists(os.path.join(baseline_estabelecimento_dir, "part-000.parquet")):
            print("Extracting baseline 2023-05 ignored fields for estabelecimento...")
            query_baseline_est = "SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '2023-05'::varchar as reference_month, nome_fantasia, ddd_1, ddd_2, ddd_fax, telefone_1, telefone_2, fax, correio_eletronico FROM estabelecimento"
            export_ignored_fields_parquet(cur, conn, query_baseline_est, schema_estabelecimento, baseline_estabelecimento_dir)

        # Check and generate baseline 2023-05 address fields if missing
        baseline_estabelecimento_addr_dir = os.path.join(current_path, "ignored_fields", "estabelecimento_enderecos", "reference_month=2023-05")
        if not os.path.exists(os.path.join(baseline_estabelecimento_addr_dir, "part-000.parquet")):
            print("Extracting baseline 2023-05 address fields for estabelecimento...")
            query_baseline_est_addr = "SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '2023-05'::varchar as reference_month, tipo_logradouro, logradouro, numero, complemento, bairro, cep, nome_cidade_exterior FROM estabelecimento"
            export_ignored_fields_parquet(cur, conn, query_baseline_est_addr, schema_estabelecimento_enderecos, baseline_estabelecimento_addr_dir)

        # B. Insert INSERTs and UPDATEs into snapshots
        print("Detecting INSERT and UPDATE events for estabelecimento...")
        cur.execute("""
            INSERT INTO snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'estabelecimento',
                stg.cnpj_basico,
                stg.cnpj_ordem,
                stg.cnpj_dv,
                jsonb_build_object('cnpj_basico', stg.cnpj_basico, 'cnpj_ordem', stg.cnpj_ordem, 'cnpj_dv', stg.cnpj_dv),
                p.old_row,
                jsonb_build_object(
                    'cnpj_basico', stg.cnpj_basico,
                    'cnpj_ordem', stg.cnpj_ordem,
                    'cnpj_dv', stg.cnpj_dv,
                    'identificador_matriz_filial', stg.identificador_matriz_filial,
                    'situacao_cadastral', stg.situacao_cadastral,
                    'data_situacao_cadastral', stg.data_situacao_cadastral,
                    'motivo_situacao_cadastral', stg.motivo_situacao_cadastral,
                    'pais', stg.pais,
                    'data_inicio_atividade', stg.data_inicio_atividade,
                    'cnae_fiscal_principal', stg.cnae_fiscal_principal,
                    'cnae_fiscal_secundaria', stg.cnae_fiscal_secundaria,
                    'uf', stg.uf,
                    'municipio', stg.municipio,
                    'situacao_especial', stg.situacao_especial,
                    'data_situacao_especial', stg.data_situacao_especial
                ),
                CASE WHEN p.is_new THEN 'INSERT' ELSE 'UPDATE' END,
                %s::varchar,
                %s::timestamp
            FROM staging_estabelecimento stg
            LEFT JOIN latest_state_estabelecimento l ON l.cnpj_basico = stg.cnpj_basico AND l.cnpj_ordem = stg.cnpj_ordem AND l.cnpj_dv = stg.cnpj_dv
            LEFT JOIN estabelecimento b ON b.cnpj_basico = stg.cnpj_basico AND b.cnpj_ordem = stg.cnpj_ordem AND b.cnpj_dv = stg.cnpj_dv AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    CASE
                        WHEN l.cnpj_basico IS NOT NULL THEN
                            jsonb_build_object(
                                'cnpj_basico', l.cnpj_basico,
                                'cnpj_ordem', l.cnpj_ordem,
                                'cnpj_dv', l.cnpj_dv,
                                'identificador_matriz_filial', l.identificador_matriz_filial,
                                'situacao_cadastral', l.situacao_cadastral,
                                'data_situacao_cadastral', l.data_situacao_cadastral,
                                'motivo_situacao_cadastral', l.motivo_situacao_cadastral,
                                'pais', l.pais,
                                'data_inicio_atividade', l.data_inicio_atividade,
                                'cnae_fiscal_principal', l.cnae_fiscal_principal,
                                'cnae_fiscal_secundaria', l.cnae_fiscal_secundaria,
                                'uf', l.uf,
                                'municipio', l.municipio,
                                'situacao_especial', l.situacao_especial,
                                'data_situacao_especial', l.data_situacao_especial
                            )
                        ELSE
                            jsonb_build_object(
                                'cnpj_basico', b.cnpj_basico,
                                'cnpj_ordem', b.cnpj_ordem,
                                'cnpj_dv', b.cnpj_dv,
                                'identificador_matriz_filial', b.identificador_matriz_filial,
                                'situacao_cadastral', b.situacao_cadastral,
                                'data_situacao_cadastral', b.data_situacao_cadastral,
                                'motivo_situacao_cadastral', b.motivo_situacao_cadastral,
                                'pais', b.pais,
                                'data_inicio_atividade', b.data_inicio_atividade,
                                'cnae_fiscal_principal', b.cnae_fiscal_principal,
                                'cnae_fiscal_secundaria', b.cnae_fiscal_secundaria,
                                'uf', b.uf,
                                'municipio', b.municipio,
                                'situacao_especial', b.situacao_especial,
                                'data_situacao_especial', b.data_situacao_especial
                            )
                    END as old_row,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.identificador_matriz_filial, b.identificador_matriz_filial) as prev_identificador_matriz_filial,
                    COALESCE(l.situacao_cadastral, b.situacao_cadastral) as prev_situacao_cadastral,
                    COALESCE(l.data_situacao_cadastral, b.data_situacao_cadastral) as prev_data_situacao_cadastral,
                    COALESCE(l.motivo_situacao_cadastral, b.motivo_situacao_cadastral) as prev_motivo_situacao_cadastral,
                    COALESCE(l.pais, b.pais) as prev_pais,
                    COALESCE(l.data_inicio_atividade, b.data_inicio_atividade) as prev_data_inicio_atividade,
                    COALESCE(l.cnae_fiscal_principal, b.cnae_fiscal_principal) as prev_cnae_fiscal_principal,
                    COALESCE(l.cnae_fiscal_secundaria, b.cnae_fiscal_secundaria) as prev_cnae_fiscal_secundaria,
                    COALESCE(l.uf, b.uf) as prev_uf,
                    COALESCE(l.municipio, b.municipio) as prev_municipio,
                    COALESCE(l.situacao_especial, b.situacao_especial) as prev_situacao_especial,
                    COALESCE(l.data_situacao_especial, b.data_situacao_especial) as prev_data_situacao_especial
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.identificador_matriz_filial IS DISTINCT FROM p.prev_identificador_matriz_filial OR
                        stg.situacao_cadastral IS DISTINCT FROM p.prev_situacao_cadastral OR
                        stg.data_situacao_cadastral IS DISTINCT FROM p.prev_data_situacao_cadastral OR
                        stg.motivo_situacao_cadastral IS DISTINCT FROM p.prev_motivo_situacao_cadastral OR
                        stg.pais IS DISTINCT FROM p.prev_pais OR
                        stg.data_inicio_atividade IS DISTINCT FROM p.prev_data_inicio_atividade OR
                        stg.cnae_fiscal_principal IS DISTINCT FROM p.prev_cnae_fiscal_principal OR
                        stg.cnae_fiscal_secundaria IS DISTINCT FROM p.prev_cnae_fiscal_secundaria OR
                        stg.uf IS DISTINCT FROM p.prev_uf OR
                        stg.municipio IS DISTINCT FROM p.prev_municipio OR
                        stg.situacao_especial IS DISTINCT FROM p.prev_situacao_especial OR
                        stg.data_situacao_especial IS DISTINCT FROM p.prev_data_situacao_especial
                    )
                );
        """, (ref_month_date, collection_date))
        est_snap_count = cur.rowcount
    
        cur.execute("""
            SELECT
                COUNT(case when tipo_alteracao = 'INSERT' then 1 end),
                COUNT(case when tipo_alteracao = 'UPDATE' then 1 end)
            FROM snapshots
            WHERE tabela = 'estabelecimento' AND mes_referencia = %s::varchar
        """, (ref_month_date,))
        est_inserts, est_updates = cur.fetchone()
        total_inserts += est_inserts
        total_updates += est_updates
 
        # C. Upsert changes to latest_state_estabelecimento
        print("Updating latest_state_estabelecimento lookup table...")
        cur.execute("""
            INSERT INTO latest_state_estabelecimento (
                cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, situacao_cadastral,
                data_situacao_cadastral, motivo_situacao_cadastral, pais, data_inicio_atividade,
                cnae_fiscal_principal, cnae_fiscal_secundaria, uf, municipio, situacao_especial, data_situacao_especial, is_deleted, last_updated_month, data_coleta
            )
            SELECT
                stg.cnpj_basico, stg.cnpj_ordem, stg.cnpj_dv, stg.identificador_matriz_filial, stg.situacao_cadastral,
                stg.data_situacao_cadastral, stg.motivo_situacao_cadastral, stg.pais, stg.data_inicio_atividade,
                stg.cnae_fiscal_principal, stg.cnae_fiscal_secundaria, stg.uf, stg.municipio, stg.situacao_especial, stg.data_situacao_especial, FALSE, %s::varchar, %s::timestamp
            FROM staging_estabelecimento stg
            LEFT JOIN latest_state_estabelecimento l ON l.cnpj_basico = stg.cnpj_basico AND l.cnpj_ordem = stg.cnpj_ordem AND l.cnpj_dv = stg.cnpj_dv
            LEFT JOIN estabelecimento b ON b.cnpj_basico = stg.cnpj_basico AND b.cnpj_ordem = stg.cnpj_ordem AND b.cnpj_dv = stg.cnpj_dv AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.identificador_matriz_filial, b.identificador_matriz_filial) as prev_identificador_matriz_filial,
                    COALESCE(l.situacao_cadastral, b.situacao_cadastral) as prev_situacao_cadastral,
                    COALESCE(l.data_situacao_cadastral, b.data_situacao_cadastral) as prev_data_situacao_cadastral,
                    COALESCE(l.motivo_situacao_cadastral, b.motivo_situacao_cadastral) as prev_motivo_situacao_cadastral,
                    COALESCE(l.pais, b.pais) as prev_pais,
                    COALESCE(l.data_inicio_atividade, b.data_inicio_atividade) as prev_data_inicio_atividade,
                    COALESCE(l.cnae_fiscal_principal, b.cnae_fiscal_principal) as prev_cnae_fiscal_principal,
                    COALESCE(l.cnae_fiscal_secundaria, b.cnae_fiscal_secundaria) as prev_cnae_fiscal_secundaria,
                    COALESCE(l.uf, b.uf) as prev_uf,
                    COALESCE(l.municipio, b.municipio) as prev_municipio,
                    COALESCE(l.situacao_especial, b.situacao_especial) as prev_situacao_especial,
                    COALESCE(l.data_situacao_especial, b.data_situacao_especial) as prev_data_situacao_especial
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.identificador_matriz_filial IS DISTINCT FROM p.prev_identificador_matriz_filial OR
                        stg.situacao_cadastral IS DISTINCT FROM p.prev_situacao_cadastral OR
                        stg.data_situacao_cadastral IS DISTINCT FROM p.prev_data_situacao_cadastral OR
                        stg.motivo_situacao_cadastral IS DISTINCT FROM p.prev_motivo_situacao_cadastral OR
                        stg.pais IS DISTINCT FROM p.prev_pais OR
                        stg.data_inicio_atividade IS DISTINCT FROM p.prev_data_inicio_atividade OR
                        stg.cnae_fiscal_principal IS DISTINCT FROM p.prev_cnae_fiscal_principal OR
                        stg.cnae_fiscal_secundaria IS DISTINCT FROM p.prev_cnae_fiscal_secundaria OR
                        stg.uf IS DISTINCT FROM p.prev_uf OR
                        stg.municipio IS DISTINCT FROM p.prev_municipio OR
                        stg.situacao_especial IS DISTINCT FROM p.prev_situacao_especial OR
                        stg.data_situacao_especial IS DISTINCT FROM p.prev_data_situacao_especial
                    )
                )
            ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE SET
                identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
                situacao_cadastral = EXCLUDED.situacao_cadastral,
                data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
                motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
                pais = EXCLUDED.pais,
                data_inicio_atividade = EXCLUDED.data_inicio_atividade,
                cnae_fiscal_principal = EXCLUDED.cnae_fiscal_principal,
                cnae_fiscal_secundaria = EXCLUDED.cnae_fiscal_secundaria,
                uf = EXCLUDED.uf,
                municipio = EXCLUDED.municipio,
                situacao_especial = EXCLUDED.situacao_especial,
                data_situacao_especial = EXCLUDED.data_situacao_especial,
                is_deleted = FALSE,
                last_updated_month = EXCLUDED.last_updated_month,
                data_coleta = EXCLUDED.data_coleta;
        """, (ref_month_date, collection_date))

        # D. Deletions
        print("Detecting DELETEs for estabelecimento...")
        cur.execute("""
            INSERT INTO snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'estabelecimento',
                prev.cnpj_basico,
                prev.cnpj_ordem,
                prev.cnpj_dv,
                jsonb_build_object('cnpj_basico', prev.cnpj_basico, 'cnpj_ordem', prev.cnpj_ordem, 'cnpj_dv', prev.cnpj_dv),
                prev.old_row,
                NULL,
                'DELETE',
                %s::varchar,
                %s::timestamp
            FROM (
                SELECT l.cnpj_basico, l.cnpj_ordem, l.cnpj_dv,
                    jsonb_build_object(
                        'cnpj_basico', l.cnpj_basico,
                        'cnpj_ordem', l.cnpj_ordem,
                        'cnpj_dv', l.cnpj_dv,
                        'identificador_matriz_filial', l.identificador_matriz_filial,
                        'situacao_cadastral', l.situacao_cadastral,
                        'data_situacao_cadastral', l.data_situacao_cadastral,
                        'motivo_situacao_cadastral', l.motivo_situacao_cadastral,
                        'pais', l.pais,
                        'data_inicio_atividade', l.data_inicio_atividade,
                        'cnae_fiscal_principal', l.cnae_fiscal_principal,
                        'cnae_fiscal_secundaria', l.cnae_fiscal_secundaria,
                        'uf', l.uf,
                        'municipio', l.municipio,
                        'situacao_especial', l.situacao_especial,
                        'data_situacao_especial', l.data_situacao_especial
                    ) as old_row
                FROM latest_state_estabelecimento l
                LEFT JOIN staging_estabelecimento stg ON stg.cnpj_basico = l.cnpj_basico AND stg.cnpj_ordem = l.cnpj_ordem AND stg.cnpj_dv = l.cnpj_dv
                WHERE l.is_deleted = FALSE
                  AND stg.cnpj_basico IS NULL
                UNION ALL
                SELECT b.cnpj_basico, b.cnpj_ordem, b.cnpj_dv,
                    jsonb_build_object(
                        'cnpj_basico', b.cnpj_basico,
                        'cnpj_ordem', b.cnpj_ordem,
                        'cnpj_dv', b.cnpj_dv,
                        'identificador_matriz_filial', b.identificador_matriz_filial,
                        'situacao_cadastral', b.situacao_cadastral,
                        'data_situacao_cadastral', b.data_situacao_cadastral,
                        'motivo_situacao_cadastral', b.motivo_situacao_cadastral,
                        'pais', b.pais,
                        'data_inicio_atividade', b.data_inicio_atividade,
                        'cnae_fiscal_principal', b.cnae_fiscal_principal,
                        'cnae_fiscal_secundaria', b.cnae_fiscal_secundaria,
                        'uf', b.uf,
                        'municipio', b.municipio,
                        'situacao_especial', b.situacao_especial,
                        'data_situacao_especial', b.data_situacao_especial
                    ) as old_row
                FROM estabelecimento b
                LEFT JOIN latest_state_estabelecimento l ON l.cnpj_basico = b.cnpj_basico AND l.cnpj_ordem = b.cnpj_ordem AND l.cnpj_dv = b.cnpj_dv
                LEFT JOIN staging_estabelecimento stg ON stg.cnpj_basico = b.cnpj_basico AND stg.cnpj_ordem = b.cnpj_ordem AND stg.cnpj_dv = b.cnpj_dv
                WHERE l.cnpj_basico IS NULL
                  AND stg.cnpj_basico IS NULL
            ) prev;
        """, (ref_month_date, collection_date))
        est_deletes = cur.rowcount
        total_deletes += est_deletes
    
        # Mark deleted records in latest_state_estabelecimento
        if est_deletes > 0:
            cur.execute("""
                INSERT INTO latest_state_estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, is_deleted, last_updated_month, data_coleta)
                SELECT
                    prev.cnpj_basico, prev.cnpj_ordem, prev.cnpj_dv, TRUE, %s::varchar, %s::timestamp
                FROM (
                    SELECT l.cnpj_basico, l.cnpj_ordem, l.cnpj_dv
                    FROM latest_state_estabelecimento l
                    LEFT JOIN staging_estabelecimento stg ON stg.cnpj_basico = l.cnpj_basico AND stg.cnpj_ordem = l.cnpj_ordem AND stg.cnpj_dv = l.cnpj_dv
                    WHERE l.is_deleted = FALSE
                      AND stg.cnpj_basico IS NULL
                    UNION
                    SELECT DISTINCT b.cnpj_basico, b.cnpj_ordem, b.cnpj_dv
                    FROM estabelecimento b
                    LEFT JOIN latest_state_estabelecimento l ON l.cnpj_basico = b.cnpj_basico AND l.cnpj_ordem = b.cnpj_ordem AND l.cnpj_dv = b.cnpj_dv
                    LEFT JOIN staging_estabelecimento stg ON stg.cnpj_basico = b.cnpj_basico AND stg.cnpj_ordem = b.cnpj_ordem AND stg.cnpj_dv = b.cnpj_dv
                    WHERE l.cnpj_basico IS NULL
                      AND stg.cnpj_basico IS NULL
                ) prev
                ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE SET
                    is_deleted = TRUE,
                    last_updated_month = EXCLUDED.last_updated_month,
                    data_coleta = EXCLUDED.data_coleta;
            """, (ref_month_date, collection_date))

        # ----------------------------------------------------
        # Table: socios
        # ----------------------------------------------------
        print("\nProcessing table: socios")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_socios_cnpj ON staging_socios(cnpj_basico);")
        conn.commit()

        # B. Insert INSERTs and UPDATEs into snapshots
        print("Detecting INSERT and UPDATE events for socios...")
        cur.execute("""
            WITH stg_agg AS (
                SELECT
                    cnpj_basico,
                    COUNT(*)::int as qtde_socios,
                    COUNT(CASE WHEN identificador_socio = 2 THEN 1 END)::int as qtde_socios_pf,
                    COUNT(CASE WHEN identificador_socio = 1 THEN 1 END)::int as qtde_socios_pj,
                    COUNT(CASE WHEN identificador_socio = 3 THEN 1 END)::int as qtde_socios_estrangeiro,
                    MIN(NULLIF(faixa_etaria, 0))::int as min_faixa_etaria,
                    MAX(NULLIF(faixa_etaria, 0))::int as max_faixa_etaria,
                    MIN(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_antiga,
                    MAX(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_recente,
                    COUNT(CASE WHEN qualificacao_socio IN (5, 10, 16, 49) THEN 1 END)::int as qtde_administradores
                FROM staging_socios
                GROUP BY cnpj_basico
            ),
            baseline_agg AS (
                SELECT
                    cnpj_basico,
                    COUNT(*)::int as qtde_socios,
                    COUNT(CASE WHEN identificador_socio = 2 THEN 1 END)::int as qtde_socios_pf,
                    COUNT(CASE WHEN identificador_socio = 1 THEN 1 END)::int as qtde_socios_pj,
                    COUNT(CASE WHEN identificador_socio = 3 THEN 1 END)::int as qtde_socios_estrangeiro,
                    MIN(NULLIF(faixa_etaria, 0))::int as min_faixa_etaria,
                    MAX(NULLIF(faixa_etaria, 0))::int as max_faixa_etaria,
                    MIN(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_antiga,
                    MAX(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_recente,
                    COUNT(CASE WHEN qualificacao_socio IN (5, 10, 16, 49) THEN 1 END)::int as qtde_administradores
                FROM socios
                GROUP BY cnpj_basico
            )
            INSERT INTO snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'socios',
                stg.cnpj_basico,
                jsonb_build_object('cnpj_basico', stg.cnpj_basico),
                p.old_row,
                jsonb_build_object(
                    'cnpj_basico', stg.cnpj_basico,
                    'qtde_socios', stg.qtde_socios,
                    'qtde_socios_pf', stg.qtde_socios_pf,
                    'qtde_socios_pj', stg.qtde_socios_pj,
                    'qtde_socios_estrangeiro', stg.qtde_socios_estrangeiro,
                    'min_faixa_etaria', stg.min_faixa_etaria,
                    'max_faixa_etaria', stg.max_faixa_etaria,
                    'data_entrada_antiga', stg.data_entrada_antiga,
                    'data_entrada_recente', stg.data_entrada_recente,
                    'qtde_administradores', stg.qtde_administradores
                ),
                CASE WHEN p.is_new THEN 'INSERT' ELSE 'UPDATE' END,
                %s::varchar,
                %s::timestamp
            FROM stg_agg stg
            LEFT JOIN latest_state_socios l ON l.cnpj_basico = stg.cnpj_basico
            LEFT JOIN baseline_agg b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    CASE
                        WHEN l.cnpj_basico IS NOT NULL THEN
                            jsonb_build_object(
                                'cnpj_basico', l.cnpj_basico,
                                'qtde_socios', l.qtde_socios,
                                'qtde_socios_pf', l.qtde_socios_pf,
                                'qtde_socios_pj', l.qtde_socios_pj,
                                'qtde_socios_estrangeiro', l.qtde_socios_estrangeiro,
                                'min_faixa_etaria', l.min_faixa_etaria,
                                'max_faixa_etaria', l.max_faixa_etaria,
                                'data_entrada_antiga', l.data_entrada_antiga,
                                'data_entrada_recente', l.data_entrada_recente,
                                'qtde_administradores', l.qtde_administradores
                            )
                        ELSE
                            jsonb_build_object(
                                'cnpj_basico', b.cnpj_basico,
                                'qtde_socios', b.qtde_socios,
                                'qtde_socios_pf', b.qtde_socios_pf,
                                'qtde_socios_pj', b.qtde_socios_pj,
                                'qtde_socios_estrangeiro', b.qtde_socios_estrangeiro,
                                'min_faixa_etaria', b.min_faixa_etaria,
                                'max_faixa_etaria', b.max_faixa_etaria,
                                'data_entrada_antiga', b.data_entrada_antiga,
                                'data_entrada_recente', b.data_entrada_recente,
                                'qtde_administradores', b.qtde_administradores
                            )
                    END as old_row,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.qtde_socios, b.qtde_socios) as prev_qtde_socios,
                    COALESCE(l.qtde_socios_pf, b.qtde_socios_pf) as prev_qtde_socios_pf,
                    COALESCE(l.qtde_socios_pj, b.qtde_socios_pj) as prev_qtde_socios_pj,
                    COALESCE(l.qtde_socios_estrangeiro, b.qtde_socios_estrangeiro) as prev_qtde_socios_estrangeiro,
                    COALESCE(l.min_faixa_etaria, b.min_faixa_etaria) as prev_min_faixa_etaria,
                    COALESCE(l.max_faixa_etaria, b.max_faixa_etaria) as prev_max_faixa_etaria,
                    COALESCE(l.data_entrada_antiga, b.data_entrada_antiga) as prev_data_entrada_antiga,
                    COALESCE(l.data_entrada_recente, b.data_entrada_recente) as prev_data_entrada_recente,
                    COALESCE(l.qtde_administradores, b.qtde_administradores) as prev_qtde_administradores
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.qtde_socios IS DISTINCT FROM p.prev_qtde_socios OR
                        stg.qtde_socios_pf IS DISTINCT FROM p.prev_qtde_socios_pf OR
                        stg.qtde_socios_pj IS DISTINCT FROM p.prev_qtde_socios_pj OR
                        stg.qtde_socios_estrangeiro IS DISTINCT FROM p.prev_qtde_socios_estrangeiro OR
                        stg.min_faixa_etaria IS DISTINCT FROM p.prev_min_faixa_etaria OR
                        stg.max_faixa_etaria IS DISTINCT FROM p.prev_max_faixa_etaria OR
                        stg.data_entrada_antiga IS DISTINCT FROM p.prev_data_entrada_antiga OR
                        stg.data_entrada_recente IS DISTINCT FROM p.prev_data_entrada_recente OR
                        stg.qtde_administradores IS DISTINCT FROM p.prev_qtde_administradores
                    )
                );
        """, (ref_month_date, collection_date))
        soc_snap_count = cur.rowcount
    
        cur.execute("""
            SELECT
                COUNT(case when tipo_alteracao = 'INSERT' then 1 end),
                COUNT(case when tipo_alteracao = 'UPDATE' then 1 end)
            FROM snapshots
            WHERE tabela = 'socios' AND mes_referencia = %s::varchar
        """, (ref_month_date,))
        soc_inserts, soc_updates = cur.fetchone()
        total_inserts += soc_inserts
        total_updates += soc_updates

        # C. Upsert changes to latest_state_socios
        print("Updating latest_state_socios lookup table...")
        cur.execute("""
            WITH stg_agg AS (
                SELECT
                    cnpj_basico,
                    COUNT(*)::int as qtde_socios,
                    COUNT(CASE WHEN identificador_socio = 2 THEN 1 END)::int as qtde_socios_pf,
                    COUNT(CASE WHEN identificador_socio = 1 THEN 1 END)::int as qtde_socios_pj,
                    COUNT(CASE WHEN identificador_socio = 3 THEN 1 END)::int as qtde_socios_estrangeiro,
                    MIN(NULLIF(faixa_etaria, 0))::int as min_faixa_etaria,
                    MAX(NULLIF(faixa_etaria, 0))::int as max_faixa_etaria,
                    MIN(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_antiga,
                    MAX(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_recente,
                    COUNT(CASE WHEN qualificacao_socio IN (5, 10, 16, 49) THEN 1 END)::int as qtde_administradores
                FROM staging_socios
                GROUP BY cnpj_basico
            ),
            baseline_agg AS (
                SELECT
                    cnpj_basico,
                    COUNT(*)::int as qtde_socios,
                    COUNT(CASE WHEN identificador_socio = 2 THEN 1 END)::int as qtde_socios_pf,
                    COUNT(CASE WHEN identificador_socio = 1 THEN 1 END)::int as qtde_socios_pj,
                    COUNT(CASE WHEN identificador_socio = 3 THEN 1 END)::int as qtde_socios_estrangeiro,
                    MIN(NULLIF(faixa_etaria, 0))::int as min_faixa_etaria,
                    MAX(NULLIF(faixa_etaria, 0))::int as max_faixa_etaria,
                    MIN(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_antiga,
                    MAX(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_recente,
                    COUNT(CASE WHEN qualificacao_socio IN (5, 10, 16, 49) THEN 1 END)::int as qtde_administradores
                FROM socios
                GROUP BY cnpj_basico
            )
            INSERT INTO latest_state_socios (
                cnpj_basico, qtde_socios, qtde_socios_pf, qtde_socios_pj, qtde_socios_estrangeiro,
                min_faixa_etaria, max_faixa_etaria, data_entrada_antiga, data_entrada_recente,
                qtde_administradores, is_deleted, last_updated_month, data_coleta
            )
            SELECT
                stg.cnpj_basico, stg.qtde_socios, stg.qtde_socios_pf, stg.qtde_socios_pj, stg.qtde_socios_estrangeiro,
                stg.min_faixa_etaria, stg.max_faixa_etaria, stg.data_entrada_antiga, stg.data_entrada_recente,
                stg.qtde_administradores, FALSE, %s::varchar, %s::timestamp
            FROM stg_agg stg
            LEFT JOIN latest_state_socios l ON l.cnpj_basico = stg.cnpj_basico
            LEFT JOIN baseline_agg b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.qtde_socios, b.qtde_socios) as prev_qtde_socios,
                    COALESCE(l.qtde_socios_pf, b.qtde_socios_pf) as prev_qtde_socios_pf,
                    COALESCE(l.qtde_socios_pj, b.qtde_socios_pj) as prev_qtde_socios_pj,
                    COALESCE(l.qtde_socios_estrangeiro, b.qtde_socios_estrangeiro) as prev_qtde_socios_estrangeiro,
                    COALESCE(l.min_faixa_etaria, b.min_faixa_etaria) as prev_min_faixa_etaria,
                    COALESCE(l.max_faixa_etaria, b.max_faixa_etaria) as prev_max_faixa_etaria,
                    COALESCE(l.data_entrada_antiga, b.data_entrada_antiga) as prev_data_entrada_antiga,
                    COALESCE(l.data_entrada_recente, b.data_entrada_recente) as prev_data_entrada_recente,
                    COALESCE(l.qtde_administradores, b.qtde_administradores) as prev_qtde_administradores
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.qtde_socios IS DISTINCT FROM p.prev_qtde_socios OR
                        stg.qtde_socios_pf IS DISTINCT FROM p.prev_qtde_socios_pf OR
                        stg.qtde_socios_pj IS DISTINCT FROM p.prev_qtde_socios_pj OR
                        stg.qtde_socios_estrangeiro IS DISTINCT FROM p.prev_qtde_socios_estrangeiro OR
                        stg.min_faixa_etaria IS DISTINCT FROM p.prev_min_faixa_etaria OR
                        stg.max_faixa_etaria IS DISTINCT FROM p.prev_max_faixa_etaria OR
                        stg.data_entrada_antiga IS DISTINCT FROM p.prev_data_entrada_antiga OR
                        stg.data_entrada_recente IS DISTINCT FROM p.prev_data_entrada_recente OR
                        stg.qtde_administradores IS DISTINCT FROM p.prev_qtde_administradores
                    )
                )
            ON CONFLICT (cnpj_basico) DO UPDATE SET
                qtde_socios = EXCLUDED.qtde_socios,
                qtde_socios_pf = EXCLUDED.qtde_socios_pf,
                qtde_socios_pj = EXCLUDED.qtde_socios_pj,
                qtde_socios_estrangeiro = EXCLUDED.qtde_socios_estrangeiro,
                min_faixa_etaria = EXCLUDED.min_faixa_etaria,
                max_faixa_etaria = EXCLUDED.max_faixa_etaria,
                data_entrada_antiga = EXCLUDED.data_entrada_antiga,
                data_entrada_recente = EXCLUDED.data_entrada_recente,
                qtde_administradores = EXCLUDED.qtde_administradores,
                is_deleted = FALSE,
                last_updated_month = EXCLUDED.last_updated_month,
                data_coleta = EXCLUDED.data_coleta;
        """, (ref_month_date, collection_date))

        # D. Deletions
        print("Detecting DELETEs for socios...")
        cur.execute("""
            WITH stg_agg AS (
                SELECT cnpj_basico
                FROM staging_socios
                GROUP BY cnpj_basico
            ),
            baseline_agg AS (
                SELECT
                    cnpj_basico,
                    COUNT(*)::int as qtde_socios,
                    COUNT(CASE WHEN identificador_socio = 2 THEN 1 END)::int as qtde_socios_pf,
                    COUNT(CASE WHEN identificador_socio = 1 THEN 1 END)::int as qtde_socios_pj,
                    COUNT(CASE WHEN identificador_socio = 3 THEN 1 END)::int as qtde_socios_estrangeiro,
                    MIN(NULLIF(faixa_etaria, 0))::int as min_faixa_etaria,
                    MAX(NULLIF(faixa_etaria, 0))::int as max_faixa_etaria,
                    MIN(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_antiga,
                    MAX(NULLIF(data_entrada_sociedade, 0))::int as data_entrada_recente,
                    COUNT(CASE WHEN qualificacao_socio IN (5, 10, 16, 49) THEN 1 END)::int as qtde_administradores
                FROM socios
                GROUP BY cnpj_basico
            )
            INSERT INTO snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'socios',
                prev.cnpj_basico,
                jsonb_build_object('cnpj_basico', prev.cnpj_basico),
                prev.old_row,
                NULL,
                'DELETE',
                %s::varchar,
                %s::timestamp
            FROM (
                SELECT l.cnpj_basico,
                    jsonb_build_object(
                        'cnpj_basico', l.cnpj_basico,
                        'qtde_socios', l.qtde_socios,
                        'qtde_socios_pf', l.qtde_socios_pf,
                        'qtde_socios_pj', l.qtde_socios_pj,
                        'qtde_socios_estrangeiro', l.qtde_socios_estrangeiro,
                        'min_faixa_etaria', l.min_faixa_etaria,
                        'max_faixa_etaria', l.max_faixa_etaria,
                        'data_entrada_antiga', l.data_entrada_antiga,
                        'data_entrada_recente', l.data_entrada_recente,
                        'qtde_administradores', l.qtde_administradores
                    ) as old_row
                FROM latest_state_socios l
                LEFT JOIN stg_agg stg ON stg.cnpj_basico = l.cnpj_basico
                WHERE l.is_deleted = FALSE
                  AND stg.cnpj_basico IS NULL
                UNION ALL
                SELECT b.cnpj_basico,
                    jsonb_build_object(
                        'cnpj_basico', b.cnpj_basico,
                        'qtde_socios', b.qtde_socios,
                        'qtde_socios_pf', b.qtde_socios_pf,
                        'qtde_socios_pj', b.qtde_socios_pj,
                        'qtde_socios_estrangeiro', b.qtde_socios_estrangeiro,
                        'min_faixa_etaria', b.min_faixa_etaria,
                        'max_faixa_etaria', b.max_faixa_etaria,
                        'data_entrada_antiga', b.data_entrada_antiga,
                        'data_entrada_recente', b.data_entrada_recente,
                        'qtde_administradores', b.qtde_administradores
                    ) as old_row
                FROM baseline_agg b
                LEFT JOIN latest_state_socios l ON l.cnpj_basico = b.cnpj_basico
                LEFT JOIN stg_agg stg ON stg.cnpj_basico = b.cnpj_basico
                WHERE l.cnpj_basico IS NULL
                  AND stg.cnpj_basico IS NULL
            ) prev;
        """, (ref_month_date, collection_date))
        soc_deletes = cur.rowcount
        total_deletes += soc_deletes
    
        # Mark deleted records in latest_state_socios
        if soc_deletes > 0:
            cur.execute("""
                WITH stg_agg AS (
                    SELECT cnpj_basico
                    FROM staging_socios
                    GROUP BY cnpj_basico
                ),
                baseline_agg AS (
                    SELECT cnpj_basico
                    FROM socios
                    GROUP BY cnpj_basico
                )
                INSERT INTO latest_state_socios (cnpj_basico, is_deleted, last_updated_month, data_coleta)
                SELECT
                    prev.cnpj_basico, TRUE, %s::varchar, %s::timestamp
                FROM (
                    SELECT l.cnpj_basico
                    FROM latest_state_socios l
                    LEFT JOIN stg_agg stg ON stg.cnpj_basico = l.cnpj_basico
                    WHERE l.is_deleted = FALSE
                      AND stg.cnpj_basico IS NULL
                    UNION
                    SELECT DISTINCT b.cnpj_basico
                    FROM baseline_agg b
                    LEFT JOIN latest_state_socios l ON l.cnpj_basico = b.cnpj_basico
                    LEFT JOIN stg_agg stg ON stg.cnpj_basico = b.cnpj_basico
                    WHERE l.cnpj_basico IS NULL
                      AND stg.cnpj_basico IS NULL
                ) prev
                ON CONFLICT (cnpj_basico) DO UPDATE SET
                    is_deleted = TRUE,
                    last_updated_month = EXCLUDED.last_updated_month,
                    data_coleta = EXCLUDED.data_coleta;
            """, (ref_month_date, collection_date))

        # ----------------------------------------------------
        # Table: simples
        # ----------------------------------------------------
        print("\nProcessing table: simples")
        
        # A. Deduplicate staging_simples using non-NULL count and tie-breaker ordering
        print("Deduplicating staging_simples...")
        cur.execute("DROP TABLE IF EXISTS staging_simples_dedup;")
        cur.execute("""
            CREATE TABLE staging_simples_dedup AS
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY cnpj_basico
                           ORDER BY
                               (
                                   (opcao_pelo_simples IS NOT NULL)::int +
                                   (data_opcao_simples IS NOT NULL AND data_opcao_simples != 0)::int +
                                   (opcao_mei IS NOT NULL)::int +
                                   (data_opcao_mei IS NOT NULL AND data_opcao_mei != 0)::int
                               ) DESC,
                               opcao_pelo_simples DESC NULLS LAST,
                               opcao_mei DESC NULLS LAST
                       ) as rn
                FROM staging_simples
            )
            SELECT cnpj_basico, opcao_pelo_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei
            FROM ranked
            WHERE rn = 1;
        """)
        cur.execute("DROP TABLE staging_simples;")
        cur.execute("ALTER TABLE staging_simples_dedup RENAME TO staging_simples;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_simples_pk ON staging_simples(cnpj_basico);")
        conn.commit()

        # B. Insert INSERTs and UPDATEs into snapshots
        print("Detecting INSERT and UPDATE events for simples...")
        cur.execute("""
            INSERT INTO snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'simples',
                stg.cnpj_basico,
                jsonb_build_object('cnpj_basico', stg.cnpj_basico),
                p.old_row,
                row_to_json(stg)::jsonb,
                CASE WHEN p.is_new THEN 'INSERT' ELSE 'UPDATE' END,
                %s::varchar,
                %s::timestamp
            FROM staging_simples stg
            LEFT JOIN latest_state_simples l ON l.cnpj_basico = stg.cnpj_basico
            LEFT JOIN simples b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    CASE
                        WHEN l.cnpj_basico IS NOT NULL THEN (row_to_json(l)::jsonb - 'is_deleted' - 'last_updated_month' - 'data_coleta')
                        ELSE row_to_json(b)::jsonb
                    END as old_row,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.opcao_pelo_simples, b.opcao_pelo_simples) as prev_opcao_pelo_simples,
                    COALESCE(l.data_opcao_simples, b.data_opcao_simples) as prev_data_opcao_simples,
                    COALESCE(l.data_exclusao_simples, b.data_exclusao_simples) as prev_data_exclusao_simples,
                    COALESCE(l.opcao_mei, b.opcao_mei) as prev_opcao_mei,
                    COALESCE(l.data_opcao_mei, b.data_opcao_mei) as prev_data_opcao_mei,
                    COALESCE(l.data_exclusao_mei, b.data_exclusao_mei) as prev_data_exclusao_mei
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.opcao_pelo_simples IS DISTINCT FROM p.prev_opcao_pelo_simples OR
                        stg.data_opcao_simples IS DISTINCT FROM p.prev_data_opcao_simples OR
                        stg.data_exclusao_simples IS DISTINCT FROM p.prev_data_exclusao_simples OR
                        stg.opcao_mei IS DISTINCT FROM p.prev_opcao_mei OR
                        stg.data_opcao_mei IS DISTINCT FROM p.prev_data_opcao_mei OR
                        stg.data_exclusao_mei IS DISTINCT FROM p.prev_data_exclusao_mei
                    )
                );
        """, (ref_month_date, collection_date))
        sim_snap_count = cur.rowcount
    
        cur.execute("""
            SELECT
                COUNT(case when tipo_alteracao = 'INSERT' then 1 end),
                COUNT(case when tipo_alteracao = 'UPDATE' then 1 end)
            FROM snapshots
            WHERE tabela = 'simples' AND mes_referencia = %s::varchar
        """, (ref_month_date,))
        sim_inserts, sim_updates = cur.fetchone()
        total_inserts += sim_inserts
        total_updates += sim_updates

        # C. Upsert changes to latest_state_simples
        print("Updating latest_state_simples lookup table...")
        cur.execute("""
            INSERT INTO latest_state_simples (
                cnpj_basico, opcao_pelo_simples, data_opcao_simples, data_exclusao_simples, opcao_mei,
                data_opcao_mei, data_exclusao_mei, is_deleted, last_updated_month, data_coleta
            )
            SELECT
                stg.cnpj_basico, stg.opcao_pelo_simples, stg.data_opcao_simples, stg.data_exclusao_simples, stg.opcao_mei,
                stg.data_opcao_mei, stg.data_exclusao_mei, FALSE, %s::varchar, %s::timestamp
            FROM staging_simples stg
            LEFT JOIN latest_state_simples l ON l.cnpj_basico = stg.cnpj_basico
            LEFT JOIN simples b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
            CROSS JOIN LATERAL (
                SELECT
                    ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                    COALESCE(l.is_deleted, FALSE) as prev_deleted,
                    COALESCE(l.opcao_pelo_simples, b.opcao_pelo_simples) as prev_opcao_pelo_simples,
                    COALESCE(l.data_opcao_simples, b.data_opcao_simples) as prev_data_opcao_simples,
                    COALESCE(l.data_exclusao_simples, b.data_exclusao_simples) as prev_data_exclusao_simples,
                    COALESCE(l.opcao_mei, b.opcao_mei) as prev_opcao_mei,
                    COALESCE(l.data_opcao_mei, b.data_opcao_mei) as prev_data_opcao_mei,
                    COALESCE(l.data_exclusao_mei, b.data_exclusao_mei) as prev_data_exclusao_mei
            ) p
            WHERE
                p.is_new
                OR (
                    NOT p.prev_deleted
                    AND (
                        stg.opcao_pelo_simples IS DISTINCT FROM p.prev_opcao_pelo_simples OR
                        stg.data_opcao_simples IS DISTINCT FROM p.prev_data_opcao_simples OR
                        stg.data_exclusao_simples IS DISTINCT FROM p.prev_data_exclusao_simples OR
                        stg.opcao_mei IS DISTINCT FROM p.prev_opcao_mei OR
                        stg.data_opcao_mei IS DISTINCT FROM p.prev_data_opcao_mei OR
                        stg.data_exclusao_mei IS DISTINCT FROM p.prev_data_exclusao_mei
                    )
                )
            ON CONFLICT (cnpj_basico) DO UPDATE SET
                opcao_pelo_simples = EXCLUDED.opcao_pelo_simples,
                data_opcao_simples = EXCLUDED.data_opcao_simples,
                data_exclusao_simples = EXCLUDED.data_exclusao_simples,
                opcao_mei = EXCLUDED.opcao_mei,
                data_opcao_mei = EXCLUDED.data_opcao_mei,
                data_exclusao_mei = EXCLUDED.data_exclusao_mei,
                is_deleted = FALSE,
                last_updated_month = EXCLUDED.last_updated_month,
                data_coleta = EXCLUDED.data_coleta;
        """, (ref_month_date, collection_date))

        # D. Deletions
        print("Detecting DELETEs for simples...")
        cur.execute("""
            INSERT INTO snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
            SELECT
                'simples',
                prev.cnpj_basico,
                jsonb_build_object('cnpj_basico', prev.cnpj_basico),
                prev.old_row,
                NULL,
                'DELETE',
                %s::varchar,
                %s::timestamp
            FROM (
                SELECT l.cnpj_basico, (row_to_json(l)::jsonb - 'is_deleted' - 'last_updated_month' - 'data_coleta') as old_row
                FROM latest_state_simples l
                LEFT JOIN staging_simples stg ON stg.cnpj_basico = l.cnpj_basico
                WHERE l.is_deleted = FALSE
                  AND stg.cnpj_basico IS NULL
                UNION ALL
                SELECT b.cnpj_basico, row_to_json(b)::jsonb as old_row
                FROM simples b
                LEFT JOIN latest_state_simples l ON l.cnpj_basico = b.cnpj_basico
                LEFT JOIN staging_simples stg ON stg.cnpj_basico = b.cnpj_basico
                WHERE l.cnpj_basico IS NULL
                  AND stg.cnpj_basico IS NULL
            ) prev;
        """, (ref_month_date, collection_date))
        sim_deletes = cur.rowcount
        total_deletes += sim_deletes
    
        # Mark deleted records in latest_state_simples
        if sim_deletes > 0:
            cur.execute("""
                INSERT INTO latest_state_simples (cnpj_basico, is_deleted, last_updated_month, data_coleta)
                SELECT
                    prev.cnpj_basico, TRUE, %s::varchar, %s::timestamp
                FROM (
                    SELECT l.cnpj_basico
                    FROM latest_state_simples l
                    LEFT JOIN staging_simples stg ON stg.cnpj_basico = l.cnpj_basico
                    WHERE l.is_deleted = FALSE
                      AND stg.cnpj_basico IS NULL
                    UNION
                    SELECT DISTINCT b.cnpj_basico
                    FROM simples b
                    LEFT JOIN latest_state_simples l ON l.cnpj_basico = b.cnpj_basico
                    LEFT JOIN staging_simples stg ON stg.cnpj_basico = b.cnpj_basico
                    WHERE l.cnpj_basico IS NULL
                      AND stg.cnpj_basico IS NULL
                ) prev
                ON CONFLICT (cnpj_basico) DO UPDATE SET
                    is_deleted = TRUE,
                    last_updated_month = EXCLUDED.last_updated_month,
                    data_coleta = EXCLUDED.data_coleta;
            """, (ref_month_date, collection_date))

        # 7. Finalization and Metadata log
        duration = int(time.time() - start_time)
        print(f"\nFinalizing reference month {data_folder}...")
        print(f"Total time elapsed: {duration} seconds.")
        print(f"Detected Changes: {total_inserts} INSERTs, {total_updates} UPDATEs, {total_deletes} DELETEs.")
    
        # Record metadata log
        cur.execute("""
            INSERT INTO snapshots_metadata (reference_month, collection_date, status, duration_seconds, num_inserts, num_updates, num_deletes)
            VALUES (%s::varchar, %s::timestamp, 'SUCCESS', %s, %s, %s, %s)
            ON CONFLICT (reference_month) DO UPDATE SET
                collection_date = EXCLUDED.collection_date,
                status = EXCLUDED.status,
                duration_seconds = EXCLUDED.duration_seconds,
                num_inserts = EXCLUDED.num_inserts,
                num_updates = EXCLUDED.num_updates,
                num_deletes = EXCLUDED.num_deletes,
                processed_at = CURRENT_TIMESTAMP;
        """, (ref_month_date, collection_date, duration, total_inserts, total_updates, total_deletes))
    
        # Cleanup staging tables
        for t in tables_to_create:
            print(f"Dropping staging_{t} table...")
            cur.execute(f"DROP TABLE IF EXISTS staging_{t};")
        
        conn.commit()
        print(f"\nReference month {data_folder} processed successfully and committed!")
    
        cur.close()
        conn.close()

    except Exception as e:
        print(f"\nError occurred during processing of reference month {data_folder}: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            try:
                with conn.cursor() as err_cur:
                    err_cur.execute("""
                        INSERT INTO snapshots_metadata (reference_month, collection_date, status, duration_seconds, num_inserts, num_updates, num_deletes, processed_at)
                        VALUES (%s::varchar, %s::timestamp, 'FAILED', 0, 0, 0, 0, CURRENT_TIMESTAMP)
                        ON CONFLICT (reference_month) DO UPDATE SET
                            collection_date = EXCLUDED.collection_date,
                            status = EXCLUDED.status,
                            processed_at = CURRENT_TIMESTAMP;
                    """, (ref_month_date, collection_date))
                conn.commit()
                print("Successfully recorded failure status.")
            except Exception as log_err:
                print(f"Could not record failure status: {log_err}")
        sys.exit(1)

if __name__ == "__main__":
    main()
