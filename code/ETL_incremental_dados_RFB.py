import datetime
import gc
import pathlib

# pyrefly: ignore [missing-import]
from dotenv import load_dotenv

# pyrefly: ignore [missing-import]
from sqlalchemy import create_engine
import os
import pandas as pd
import psycopg2
import re
import sys
import time
import requests
import zipfile
import xml.etree.ElementTree as ET
import csv
from io import StringIO


def check_diff(url, file_name, token):
    """
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    """
    if not os.path.isfile(file_name):
        return True  # ainda nao foi baixado

    headers = {
        "X-Requested-With": "XMLHttpRequest",
    }
    response = requests.head(url, auth=(token, ""), headers=headers)
    new_size = int(response.headers.get("content-length", 0))
    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True  # tamanho diferente

    return False  # arquivos sao iguais


def makedirs(path):
    """
    Cria o diretório se não existir
    """
    if not os.path.exists(path):
        os.makedirs(path)


# Importar usando COPY
def psql_insert_copy(table, conn, keys, data_iter):
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = "{}.{}".format(table.schema, table.name)
        else:
            table_name = table.name

        sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def to_staging(dataframe, table_name, engine, size=100000):
    """
    Insere dados na tabela temporária (staging) usando COPY em blocos
    """
    total = len(dataframe)

    def chunker(df):
        return (df[i : i + size] for i in range(0, len(df), size))

    for i, df in enumerate(chunker(dataframe)):
        df.to_sql(
            name=f"staging_{table_name}",
            con=engine,
            method=psql_insert_copy,
            if_exists="append",
            index=False,
        )
        index = (i + 1) * size
        if index > total:
            index = total
        percent = (index * 100) / total
        sys.stdout.write(
            f"\rCarga Staging {table_name}: {percent:.2f}% [{index}/{total}]"
        )
    sys.stdout.write("\n")


def merge_staging_to_target(table_name, cur, conn, data_folder):
    """
    Mescla a tabela staging na tabela de produção deletando registros com chaves coincidentes e inserindo os novos
    """
    merge_start = time.time()

    # Configurar condição de chave primária para a junção
    if table_name == "empresa":
        join_cond = "target.cnpj_basico = staging.cnpj_basico"
    elif table_name == "estabelecimento":
        join_cond = "target.cnpj_basico = staging.cnpj_basico AND target.cnpj_ordem = staging.cnpj_ordem AND target.cnpj_dv = staging.cnpj_dv"
    elif table_name == "socios":
        join_cond = "target.cnpj_basico = staging.cnpj_basico"  # Substituir todos os sócios da empresa alterada
    elif table_name == "simples":
        join_cond = "target.cnpj_basico = staging.cnpj_basico"
    else:
        join_cond = (
            "target.codigo = staging.codigo"  # Tabelas de lookup (cnae, pais, etc)
        )

    print(f"\nMesclando dados da staging_{table_name} na tabela {table_name}...")

    # Se a tabela de destino não existir, criar com a mesma estrutura da staging
    cur.execute(
        f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
    )
    target_exists = cur.fetchone()[0]
    if not target_exists:
        print(f"Tabela de destino '{table_name}' não existe. Criando...")
        cur.execute(
            f"CREATE TABLE {table_name} (LIKE staging_{table_name} INCLUDING ALL);"
        )
        conn.commit()
        if table_name in ["empresa", "estabelecimento", "socios", "simples"]:
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {table_name}_cnpj ON {table_name}(cnpj_basico);"
            )
            conn.commit()

    # 0. Criar tabela de snapshots se não existir
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id SERIAL PRIMARY KEY,
            tabela VARCHAR(50) NOT NULL,
            cnpj_basico VARCHAR(8) NOT NULL,
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            chave JSONB NOT NULL,
            conteudo_anterior JSONB,
            conteudo_novo JSONB,
            tipo_alteracao VARCHAR(10) NOT NULL,
            mes_referencia VARCHAR(20) NOT NULL,
            data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS snapshots_tabela_mes ON snapshots(tabela, mes_referencia);
        CREATE INDEX IF NOT EXISTS snapshots_cnpj_busca ON snapshots(cnpj_basico, cnpj_ordem, cnpj_dv);
        CREATE INDEX IF NOT EXISTS snapshots_chave ON snapshots USING gin (chave);
    """
    )
    conn.commit()

    # Mapeamento para obter as partes do CNPJ explicitamente para cada tabela
    CNPJ_COLS = {
        "empresa": ("target.cnpj_basico", "NULL", "NULL"),
        "estabelecimento": (
            "target.cnpj_basico",
            "target.cnpj_ordem",
            "target.cnpj_dv",
        ),
        "socios": ("target.cnpj_basico", "NULL", "NULL"),
        "simples": ("target.cnpj_basico", "NULL", "NULL"),
    }
    CNPJ_COLS_STAGING = {
        "empresa": ("staging.cnpj_basico", "NULL", "NULL"),
        "estabelecimento": (
            "staging.cnpj_basico",
            "staging.cnpj_ordem",
            "staging.cnpj_dv",
        ),
        "socios": ("staging.cnpj_basico", "NULL", "NULL"),
        "simples": ("staging.cnpj_basico", "NULL", "NULL"),
    }
    COMPARE_KEYS = {
        "empresa": ["cnpj_basico"],
        "estabelecimento": ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
        "simples": ["cnpj_basico"],
        "socios": ["cnpj_basico", "nome_socio_razao_social"],
    }

    # Só registra snapshots das tabelas principais e se o target já existia
    if target_exists and table_name in COMPARE_KEYS:
        compare_cols = COMPARE_KEYS[table_name]
        compare_join = " AND ".join(
            [f"target.{col} = staging.{col}" for col in compare_cols]
        )
        key_json_target = (
            "jsonb_build_object("
            + ", ".join([f"'{col}', target.{col}" for col in compare_cols])
            + ")"
        )
        key_json_staging = (
            "jsonb_build_object("
            + ", ".join([f"'{col}', staging.{col}" for col in compare_cols])
            + ")"
        )

        cnpj_basico_expr, cnpj_ordem_expr, cnpj_dv_expr = CNPJ_COLS[table_name]
        cnpj_basico_expr_stg, cnpj_ordem_expr_stg, cnpj_dv_expr_stg = CNPJ_COLS_STAGING[
            table_name
        ]

        # Condições de junção lateral para obter o último snapshot a partir de target
        lateral_join_conds = [
            f"s.tabela = '{table_name}'",
            "s.cnpj_basico = target.cnpj_basico",
        ]
        if table_name == "estabelecimento":
            lateral_join_conds.append("s.cnpj_ordem = target.cnpj_ordem")
            lateral_join_conds.append("s.cnpj_dv = target.cnpj_dv")
        elif table_name == "socios":
            lateral_join_conds.append(
                "(s.chave->>'nome_socio_razao_social') = target.nome_socio_razao_social"
            )
        lateral_where = " AND ".join(lateral_join_conds)

        # Condições de junção lateral para obter o último snapshot a partir de staging
        stg_lateral_join_conds = [
            f"s.tabela = '{table_name}'",
            "s.cnpj_basico = staging.cnpj_basico",
        ]
        if table_name == "estabelecimento":
            stg_lateral_join_conds.append("s.cnpj_ordem = staging.cnpj_ordem")
            stg_lateral_join_conds.append("s.cnpj_dv = staging.cnpj_dv")
        elif table_name == "socios":
            stg_lateral_join_conds.append(
                "(s.chave->>'nome_socio_razao_social') = staging.nome_socio_razao_social"
            )
        stg_lateral_where = " AND ".join(stg_lateral_join_conds)

        # Se for a tabela empresa, desconsideramos alterações apenas na 'razao_social'
        if table_name == "empresa":
            where_update = "(row_to_json(target)::jsonb - 'razao_social') IS DISTINCT FROM (row_to_json(staging)::jsonb - 'razao_social')"
            diff_filter = "(row_to_json(staging)::jsonb - 'razao_social') IS DISTINCT FROM (COALESCE(latest_snap.conteudo_novo, row_to_json(target)::jsonb) - 'razao_social')"
            insert_filter = "(row_to_json(staging)::jsonb - 'razao_social') IS DISTINCT FROM (latest_snap.conteudo_novo - 'razao_social')"
        elif table_name == "estabelecimento":
            where_update = "(row_to_json(target)::jsonb - 'nome_fantasia') IS DISTINCT FROM (row_to_json(staging)::jsonb - 'nome_fantasia')"
            diff_filter = "(row_to_json(staging)::jsonb - 'nome_fantasia') IS DISTINCT FROM (COALESCE(latest_snap.conteudo_novo, row_to_json(target)::jsonb) - 'nome_fantasia')"
            insert_filter = "(row_to_json(staging)::jsonb - 'nome_fantasia') IS DISTINCT FROM (latest_snap.conteudo_novo - 'nome_fantasia')"
        else:
            where_update = "target IS DISTINCT FROM staging"
            diff_filter = "row_to_json(staging)::jsonb IS DISTINCT FROM COALESCE(latest_snap.conteudo_novo, row_to_json(target)::jsonb)"
            insert_filter = (
                "row_to_json(staging)::jsonb IS DISTINCT FROM latest_snap.conteudo_novo"
            )

        # 1. Registrar UPDATES (registros que mudaram)
        cur.execute(
            f"""
            INSERT INTO snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia)
            SELECT
                '{table_name}',
                {cnpj_basico_expr},
                {cnpj_ordem_expr},
                {cnpj_dv_expr},
                {key_json_target},
                COALESCE(latest_snap.conteudo_novo, row_to_json(target)::jsonb),
                row_to_json(staging)::jsonb,
                'UPDATE',
                '{data_folder}'
            FROM {table_name} target
            JOIN staging_{table_name} staging ON {compare_join}
            LEFT JOIN LATERAL (
                SELECT s.conteudo_novo
                FROM snapshots s
                WHERE {lateral_where}
                ORDER BY s.id DESC
                LIMIT 1
            ) latest_snap ON TRUE
            WHERE {where_update}
              AND {diff_filter};
        """
        )
        updates_logged = cur.rowcount
        conn.commit()

        # 2. Registrar DELETES (ex: sócios removidos de uma empresa)
        deletes_logged = 0
        if table_name == "socios":
            cur.execute(
                f"""
                INSERT INTO snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia)
                SELECT
                    '{table_name}',
                    target.cnpj_basico,
                    NULL,
                    NULL,
                    {key_json_target},
                    row_to_json(target)::jsonb,
                    NULL,
                    'DELETE',
                    '{data_folder}'
                FROM {table_name} target
                JOIN (SELECT DISTINCT cnpj_basico FROM staging_{table_name}) staging_keys 
                  ON target.cnpj_basico = staging_keys.cnpj_basico
                LEFT JOIN staging_{table_name} staging ON {compare_join}
                LEFT JOIN LATERAL (
                    SELECT s.tipo_alteracao
                    FROM snapshots s
                    WHERE s.tabela = 'socios'
                      AND s.cnpj_basico = target.cnpj_basico
                      AND (s.chave->>'nome_socio_razao_social') = target.nome_socio_razao_social
                    ORDER BY s.id DESC
                    LIMIT 1
                ) latest_snap ON TRUE
                WHERE staging.cnpj_basico IS NULL
                  AND (
                      latest_snap.tipo_alteracao IS NULL
                      OR latest_snap.tipo_alteracao != 'DELETE'
                  );
            """
            )
            deletes_logged = cur.rowcount
            conn.commit()

        # 3. Registrar INSERTS (novos registros)
        null_cond = " AND ".join([f"target.{col} IS NULL" for col in compare_cols])
        cur.execute(
            f"""
            INSERT INTO snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia)
            SELECT
                '{table_name}',
                {cnpj_basico_expr_stg},
                {cnpj_ordem_expr_stg},
                {cnpj_dv_expr_stg},
                {key_json_staging},
                NULL,
                row_to_json(staging)::jsonb,
                'INSERT',
                '{data_folder}'
            FROM staging_{table_name} staging
            LEFT JOIN {table_name} target ON {compare_join}
            LEFT JOIN LATERAL (
                SELECT s.conteudo_novo, s.tipo_alteracao
                FROM snapshots s
                WHERE {stg_lateral_where}
                ORDER BY s.id DESC
                LIMIT 1
            ) latest_snap ON TRUE
            WHERE {null_cond}
              AND (
                  latest_snap.tipo_alteracao IS NULL
                  OR latest_snap.tipo_alteracao = 'DELETE'
                  OR {insert_filter}
              );
        """
        )
        inserts_logged = cur.rowcount
        conn.commit()

        print(
            f"Histórico ({table_name}): {updates_logged} UPDATES, {inserts_logged} INSERTS, {deletes_logged} DELETES salvos."
        )

    # 1. Deletar do destino e 2. Inserir (Mesclar) apenas para o mês 2026-07
    deleted_count = 0
    inserted_count = 0
    if (
        False
    ):  # Desativado por solicitação: mesmo em 2026-07 as tabelas de produção não serão modificadas
        print(f"Modificando tabela de produção '{table_name}' (Mês: {data_folder})...")
        cur.execute(
            f"""
            DELETE FROM {table_name} target
            USING staging_{table_name} staging
            WHERE {join_cond};
        """
        )
        deleted_count = cur.rowcount
        conn.commit()

        cur.execute(
            f"""
            INSERT INTO {table_name}
            SELECT * FROM staging_{table_name};
        """
        )
        inserted_count = cur.rowcount
        conn.commit()
        print(
            f"Tabela de produção '{table_name}' atualizada com sucesso: {deleted_count} deletados, {inserted_count} inseridos."
        )
    else:
        print(
            f"Tabela de produção '{table_name}' mantida inalterada (Mês: {data_folder})."
        )

    # 3. Limpar a tabela staging
    cur.execute(f"DROP TABLE IF EXISTS staging_{table_name};")
    conn.commit()

    print(
        f"Concluído em {time.time() - merge_start:.1f}s | Alterações gravadas em snapshots para {data_folder}."
    )


# 1. Ler arquivo .env
current_path = pathlib.Path().resolve()
dotenv_path = os.path.join(current_path, ".env")
if not os.path.isfile(dotenv_path):
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv_path = os.path.join(script_dir, ".env")
if not os.path.isfile(dotenv_path):
    print("Arquivo .env não encontrado!")
    sys.exit(1)
print("Using .env file at:", dotenv_path)
load_dotenv(dotenv_path=dotenv_path)

# Obter diretórios e configurações
output_files = os.getenv("OUTPUT_FILES_PATH")
extracted_files = os.getenv("EXTRACTED_FILES_PATH")
makedirs(output_files)
makedirs(extracted_files)

user = os.getenv("DB_USER")
passw = os.getenv("DB_PASSWORD", "")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

# Solicitar qual mês deseja processar (ou pegar do env ou argumento de linha de comando)
if len(sys.argv) > 1:
    data_folder = sys.argv[1].strip()
    print(f"Mês fornecido via argumento: {data_folder}")
else:
    default_folder = os.getenv("NEXTCLOUD_FOLDER", "2026-06")
    print(f"Mês padrão configurado no .env: {default_folder}")
    print(
        "Digite o mês/pasta que deseja processar (pressione Enter para usar o padrão):"
    )
    data_folder = input().strip()
    if not data_folder:
        data_folder = default_folder

# Validar se o mês do diretório de downloads é o mesmo
download_month_file = os.path.join(output_files, ".download_month")
current_download_month = None
if os.path.exists(download_month_file):
    try:
        with open(download_month_file, "r") as f:
            current_download_month = f.read().strip()
    except Exception:
        pass

if current_download_month != data_folder:
    print(
        f"Mês de download alterado ({current_download_month} -> {data_folder}). Limpando zip files antigos de {output_files}..."
    )
    for f in os.listdir(output_files):
        if f.lower().endswith(".zip"):
            try:
                os.remove(os.path.join(output_files, f))
            except Exception as e:
                print(f"Erro ao remover arquivo zip antigo {f}: {e}")
    try:
        with open(download_month_file, "w") as f:
            f.write(data_folder)
    except Exception as e:
        print(f"Erro ao gravar metadados de download: {e}")

# Configurar WebDAV
share_url = os.getenv(
    "NEXTCLOUD_SHARE_URL",
    "https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9",
)
token = share_url.rstrip("/").split("/")[-1]
webdav_base = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
webdav_url = f"{webdav_base}{data_folder}/"

print(f"Conectando ao WebDAV: {webdav_url}")
headers = {"X-Requested-With": "XMLHttpRequest", "Depth": "1"}
response = requests.request("PROPFIND", webdav_url, auth=(token, ""), headers=headers)
if response.status_code != 207:
    print(
        f"Erro ao acessar a pasta {data_folder} no Nextcloud! Status: {response.status_code}"
    )
    sys.exit(1)

root = ET.fromstring(response.content)
ns = {"d": "DAV:"}
Files = []
for resp in root.findall(".//d:response", ns):
    href = resp.find("d:href", ns)
    if href is not None:
        path = href.text
        if path.endswith(".zip"):
            filename = os.path.basename(path)
            Files.append(filename)

Files.sort()

# Conectar ao Postgres
engine = create_engine(f"postgresql://{user}:{passw}@{host}:{port}/{database}")
conn = psycopg2.connect(
    f"dbname={database} user={user} host={host} port={port} password={passw}"
)
cur = conn.cursor()

# 2. Iniciar downloads e mesclagem
for file_idx, zip_name in enumerate(Files, 1):
    checkpoint_key = f"{data_folder}/{zip_name}"
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM processed_files WHERE file_path = %s)",
        (checkpoint_key,),
    )
    if cur.fetchone()[0]:
        print(f"\n==========================================")
        print(f"PULANDO ARQUIVO ({file_idx}/{len(Files)}) [JÁ PROCESSADO]: {zip_name}")
        print(f"==========================================")
        continue

    print(f"\n==========================================")
    print(f"PROCESSANDO ARQUIVO ({file_idx}/{len(Files)}): {zip_name}")
    print(f"==========================================")

    url = f"{webdav_url}{zip_name}"
    local_zip_path = os.path.join(output_files, zip_name)

    # Baixar ou retomar download
    download_needed = True
    resume_header = {}
    downloaded = 0
    write_mode = "wb"
    skip_file = False

    headers_prop = {"X-Requested-With": "XMLHttpRequest"}
    try:
        response = requests.head(url, auth=(token, ""), headers=headers_prop)
        server_size = int(response.headers.get("content-length", 0))
    except Exception as e:
        print(f"Erro ao obter informações do servidor para {zip_name}: {e}")
        server_size = 0

    if server_size > 0 and os.path.isfile(local_zip_path):
        local_size = os.path.getsize(local_zip_path)
        if local_size == server_size:
            print(f"Arquivo {zip_name} já está totalmente baixado localmente.")
            download_needed = False
        elif local_size < server_size:
            print(
                f"Retomando download de {zip_name} a partir de {local_size / (1024*1024):.1f}MB..."
            )
            resume_header = {"Range": f"bytes={local_size}-"}
            downloaded = local_size
            write_mode = "ab"
        else:
            print(
                f"Arquivo local {zip_name} é maior que o servidor. Reiniciando download..."
            )
            try:
                os.remove(local_zip_path)
            except:
                pass

    if download_needed:
        req_headers = {**headers_prop, **resume_header}
        max_retries = 8
        backoff_factor = 2
        download_success = False

        for attempt in range(1, max_retries + 1):
            try:
                with requests.get(
                    url, auth=(token, ""), headers=req_headers, stream=True
                ) as r:
                    if r.status_code not in [200, 206]:
                        print(f"\nErro ao baixar arquivo {zip_name}. Status: {r.status_code} (Tentativa {attempt}/{max_retries})")
                        if attempt == max_retries:
                            break
                        sleep_time = min(backoff_factor ** attempt, 180)
                        time.sleep(sleep_time)
                        continue

                    if r.status_code == 200:
                        # Se o servidor não aceitou o Range, recomeça do zero
                        write_mode = "wb"
                        downloaded = 0

                    block_size = 1024 * 1024
                    with open(local_zip_path, write_mode) as f:
                        for chunk in r.iter_content(chunk_size=block_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if server_size > 0:
                                    percent = (downloaded / server_size) * 100
                                    sys.stdout.write(
                                        f"\rProgresso: {percent:.2f}% [{downloaded / (1024*1024):.1f}MB / {server_size / (1024*1024):.1f}MB]"
                                    )
                                else:
                                    sys.stdout.write(
                                        f"\rProgresso: {downloaded / (1024*1024):.1f}MB"
                                    )
                                sys.stdout.flush()
                    print()
                    download_success = True
                    break
            except Exception as e:
                print(f"\nErro de conexão durante o download de {zip_name}: {e} (Tentativa {attempt}/{max_retries})")
                if attempt == max_retries:
                    break
                sleep_time = min(backoff_factor ** attempt, 180)
                time.sleep(sleep_time)

        if not download_success:
            is_lookup = any(name in zip_name.upper() for name in ["PAIS", "MUNIC", "CNAE", "NATJU", "QUALS", "MOTI"])
            if is_lookup:
                print(f"\n[AVISO] Falha definitiva no download do arquivo auxiliar {zip_name}. Pulando e continuando...")
                skip_file = True
            else:
                print(f"\n[ERRO] Falha definitiva no download do arquivo essencial {zip_name}. Abortando execução.")
                sys.exit(1)

    if skip_file:
        continue

    # Extrair arquivo
    print(f"Extraindo {zip_name}...")
    try:
        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            new_files = zip_ref.namelist()
            # Verificar se os arquivos já existem para evitar re-extração desnecessária
            all_exist = all(
                os.path.exists(os.path.join(extracted_files, f)) for f in new_files
            )
            if not all_exist:
                zip_ref.extractall(extracted_files)
            else:
                print("Arquivos já extraídos. Pulando extração...")
    except zipfile.BadZipFile as e:
        print(f"\n[AVISO] O arquivo {zip_name} está corrompido ou incompleto no servidor/disco: {e}")
        print(f"Pulando {zip_name} e marcando como processado para evitar interrupções.")
        try:
            os.remove(local_zip_path)
        except:
            pass
        # Salva checkpoint do arquivo corrompido
        try:
            cur.execute(
                "INSERT INTO processed_files (file_path) VALUES (%s) ON CONFLICT (file_path) DO NOTHING",
                (checkpoint_key,),
            )
            conn.commit()
        except Exception as db_err:
            print(f"Erro ao salvar checkpoint para arquivo corrompido: {db_err}")
        continue

    # Processar cada arquivo extraído correspondente
    for extracted_file in new_files:
        extracted_file_path = os.path.join(extracted_files, extracted_file)

        # Identificar tabela correspondente
        table_name = None
        dtypes = None
        columns = None

        if "EMPRE" in extracted_file.upper():
            table_name = "empresa"
            dtypes = {
                0: object,
                1: object,
                2: "Int32",
                3: "Int32",
                4: object,
                5: "Int32",
                6: object,
            }
            columns = [
                "cnpj_basico",
                "razao_social",
                "natureza_juridica",
                "qualificacao_responsavel",
                "capital_social",
                "porte_empresa",
                "ente_federativo_responsavel",
            ]
        elif "ESTABELE" in extracted_file.upper():
            table_name = "estabelecimento"
            dtypes = {
                0: object,
                1: object,
                2: object,
                3: "Int32",
                4: object,
                5: "Int32",
                6: "Int32",
                7: "Int32",
                8: object,
                9: object,
                10: "Int32",
                11: "Int32",
                12: object,
                13: object,
                14: object,
                15: object,
                16: object,
                17: object,
                18: object,
                19: object,
                20: "Int32",
                21: object,
                22: object,
                23: object,
                24: object,
                25: object,
                26: object,
                27: object,
                28: object,
                29: "Int32",
            }
            columns = [
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "identificador_matriz_filial",
                "nome_fantasia",
                "situacao_cadastral",
                "data_situacao_cadastral",
                "motivo_situacao_cadastral",
                "nome_cidade_exterior",
                "pais",
                "data_inicio_atividade",
                "cnae_fiscal_principal",
                "cnae_fiscal_secundaria",
                "tipo_logradouro",
                "logradouro",
                "numero",
                "complemento",
                "bairro",
                "cep",
                "uf",
                "municipio",
                "ddd_1",
                "telefone_1",
                "ddd_2",
                "telefone_2",
                "ddd_fax",
                "fax",
                "correio_eletronico",
                "situacao_especial",
                "data_situacao_especial",
            ]
        elif "SOCIO" in extracted_file.upper():
            table_name = "socios"
            dtypes = {
                0: object,
                1: "Int32",
                2: object,
                3: object,
                4: "Int32",
                5: "Int32",
                6: "Int32",
                7: object,
                8: object,
                9: "Int32",
                10: "Int32",
            }
            columns = [
                "cnpj_basico",
                "identificador_socio",
                "nome_socio_razao_social",
                "cpf_cnpj_socio",
                "qualificacao_socio",
                "data_entrada_sociedade",
                "pais",
                "representante_legal",
                "nome_do_representante",
                "qualificacao_representante_legal",
                "faixa_etaria",
            ]
        elif "SIMPLES" in extracted_file.upper():
            table_name = "simples"
            dtypes = {
                0: object,
                1: object,
                2: "Int32",
                3: "Int32",
                4: object,
                5: "Int32",
                6: "Int32",
            }
            columns = [
                "cnpj_basico",
                "opcao_pelo_simples",
                "data_opcao_simples",
                "data_exclusao_simples",
                "opcao_mei",
                "data_opcao_mei",
                "data_exclusao_mei",
            ]
        elif "CNAE" in extracted_file.upper():
            table_name = "cnae"
            dtypes = "object"
            columns = ["codigo", "descricao"]
        elif "MOTI" in extracted_file.upper():
            table_name = "moti"
            dtypes = {0: "Int32", 1: object}
            columns = ["codigo", "descricao"]
        elif "MUNIC" in extracted_file.upper():
            table_name = "munic"
            dtypes = {0: "Int32", 1: object}
            columns = ["codigo", "descricao"]
        elif "NATJU" in extracted_file.upper():
            table_name = "natju"
            dtypes = {0: "Int32", 1: object}
            columns = ["codigo", "descricao"]
        elif "PAIS" in extracted_file.upper():
            table_name = "pais"
            dtypes = {0: "Int32", 1: object}
            columns = ["codigo", "descricao"]
        elif "QUALS" in extracted_file.upper():
            table_name = "quals"
            dtypes = {0: "Int32", 1: object}
            columns = ["codigo", "descricao"]

        if not table_name:
            continue

        print(f"Lendo e inserindo {extracted_file} na staging para mesclagem...")

        # Assegurar que a staging esteja limpa
        cur.execute(f"DROP TABLE IF EXISTS staging_{table_name};")
        conn.commit()

        # Para arquivos gigantescos, podemos carregar em pedaços na staging usando chunksize (mais eficiente em RAM/CPU)
        NROWS = 1000000
        try:
            for chunk_df in pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=";",
                header=None,
                dtype=dtypes,
                encoding="latin-1",
                chunksize=NROWS,
            ):
                if len(chunk_df) == 0:
                    break

                # Tratamentos específicos
                if table_name == "empresa":
                    chunk_df[4] = chunk_df[4].apply(lambda x: str(x).replace(",", "."))
                    chunk_df[4] = chunk_df[4].astype(float)

                chunk_df.columns = columns

                if table_name == "munic":
                    proj_dir = os.path.dirname(dotenv_path)
                    csv_path = os.path.join(proj_dir, "municipios.csv")
                    if os.path.isfile(csv_path):
                        df_csv = pd.read_csv(csv_path, sep=";", encoding="latin1")
                        df_csv.columns = [col.strip() for col in df_csv.columns]
                        mapping = df_csv[
                            ["CÓDIGO DO MUNICÍPIO - TOM", "CÓDIGO DO MUNICÍPIO - IBGE"]
                        ].copy()
                        mapping.columns = ["codigo", "cd_mun"]
                        mapping = mapping.drop_duplicates(subset=["codigo"])

                        if 1182 not in mapping["codigo"].values:
                            new_row = pd.DataFrame(
                                [{"codigo": 1182, "cd_mun": 5101837}]
                            )
                            mapping = pd.concat([mapping, new_row], ignore_index=True)

                        mapping["codigo"] = mapping["codigo"].astype("Int32")
                        mapping["cd_mun"] = mapping["cd_mun"].astype("Int32")
                        chunk_df["codigo"] = chunk_df["codigo"].astype("Int32")

                        chunk_df = pd.merge(chunk_df, mapping, on="codigo", how="left")
                    else:
                        print(
                            f"Aviso: arquivo {csv_path} não encontrado. Coluna cd_mun ficará nula."
                        )
                        chunk_df["cd_mun"] = pd.Series(
                            [None] * len(chunk_df), dtype="Int32"
                        )

                to_staging(chunk_df, table_name, engine)

                # Liberar memória explicitamente
                del chunk_df
                gc.collect()
        except Exception as e:
            print(f"Erro ao processar arquivo {extracted_file}: {e}")

        # Mesclar staging na tabela final de produção
        merge_staging_to_target(table_name, cur, conn, data_folder)

        # Deletar arquivo físico extraído para economizar espaço em disco
        try:
            os.remove(extracted_file_path)
            print(f"Arquivo extraído {extracted_file} removido para liberar espaço.")
        except Exception as e:
            print(f"Erro ao remover arquivo {extracted_file}: {e}")

    # Deletar o arquivo zip original para economizar espaço em disco
    try:
        if os.path.exists(local_zip_path):
            os.remove(local_zip_path)
            print(f"Arquivo zip {zip_name} removido para liberar espaço.")
    except Exception as e:
        print(f"Erro ao remover arquivo zip {zip_name}: {e}")

    # Salvar checkpoint
    try:
        cur.execute(
            "INSERT INTO processed_files (file_path) VALUES (%s) ON CONFLICT (file_path) DO NOTHING",
            (checkpoint_key,),
        )
        conn.commit()
    except Exception as e:
        print(f"Erro ao salvar checkpoint para {zip_name}: {e}")

print("\nCarga incremental concluída com sucesso!")
cur.close()
conn.close()
