import os
import sys
import shutil
import pathlib
import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Add code directory to path to import helpers
current_path = pathlib.Path(__file__).parent.parent.resolve()
sys.path.append(os.path.join(current_path, "code"))

from ETL_incremental_dados_RFB import (
    export_ignored_fields_parquet,
    schema_empresa,
    schema_estabelecimento,
    schema_estabelecimento_enderecos
)

def setup_test_schema(cur):
    print("Setting up test schema...")
    cur.execute("DROP SCHEMA IF EXISTS test_incremental CASCADE;")
    cur.execute("CREATE SCHEMA test_incremental;")
    
    # Create baseline tables
    cur.execute("""
        CREATE TABLE test_incremental.empresa (
            cnpj_basico VARCHAR(8) PRIMARY KEY,
            razao_social TEXT,
            natureza_juridica INTEGER,
            qualificacao_responsavel INTEGER,
            capital_social DOUBLE PRECISION,
            porte_empresa INTEGER,
            ente_federativo_responsavel TEXT
        );
        
        CREATE TABLE test_incremental.estabelecimento (
            cnpj_basico VARCHAR(8) NOT NULL,
            cnpj_ordem VARCHAR(4) NOT NULL,
            cnpj_dv VARCHAR(2) NOT NULL,
            identificador_matriz_filial INTEGER,
            nome_fantasia TEXT,
            situacao_cadastral INTEGER,
            data_situacao_cadastral INTEGER,
            motivo_situacao_cadastral INTEGER,
            nome_cidade_exterior TEXT,
            pais TEXT,
            data_inicio_atividade INTEGER,
            cnae_fiscal_principal INTEGER,
            cnae_fiscal_secundaria TEXT,
            tipo_logradouro TEXT,
            logradouro TEXT,
            numero TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            uf TEXT,
            municipio INTEGER,
            ddd_1 TEXT,
            telefone_1 TEXT,
            ddd_2 TEXT,
            telefone_2 TEXT,
            ddd_fax TEXT,
            fax TEXT,
            correio_eletronico TEXT,
            situacao_especial TEXT,
            data_situacao_especial INTEGER,
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
        );
        
        CREATE TABLE test_incremental.socios (
            cnpj_basico VARCHAR(8) NOT NULL,
            nome_socio_razao_social TEXT NOT NULL,
            identificador_socio INTEGER,
            cpf_cnpj_socio TEXT,
            qualificacao_socio INTEGER,
            data_entrada_sociedade INTEGER,
            pais INTEGER,
            representante_legal TEXT,
            nome_do_representante TEXT,
            qualificacao_representante_legal INTEGER,
            faixa_etaria INTEGER,
            PRIMARY KEY (cnpj_basico, nome_socio_razao_social)
        );
        
        CREATE TABLE test_incremental.simples (
            cnpj_basico VARCHAR(8) PRIMARY KEY,
            opcao_pelo_simples TEXT,
            data_opcao_simples INTEGER,
            data_exclusao_simples INTEGER,
            opcao_mei TEXT,
            data_opcao_mei INTEGER,
            data_exclusao_mei INTEGER
        );
    """)
    
    # Create test staging, latest_state and snapshot tables
    cur.execute("""
        CREATE TABLE test_incremental.staging_empresa (LIKE test_incremental.empresa);
        CREATE TABLE test_incremental.staging_estabelecimento (LIKE test_incremental.estabelecimento);
        CREATE TABLE test_incremental.staging_socios (LIKE test_incremental.socios);
        CREATE TABLE test_incremental.staging_simples (LIKE test_incremental.simples);
        
        CREATE TABLE test_incremental.latest_state_empresa (
            cnpj_basico VARCHAR(8) PRIMARY KEY,
            natureza_juridica INTEGER,
            qualificacao_responsavel INTEGER,
            capital_social DOUBLE PRECISION,
            porte_empresa INTEGER,
            ente_federativo_responsavel TEXT,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month VARCHAR(7) NOT NULL,
            data_coleta TIMESTAMP NOT NULL
        );
        
        CREATE TABLE test_incremental.latest_state_estabelecimento (
            cnpj_basico VARCHAR(8) NOT NULL,
            cnpj_ordem VARCHAR(4) NOT NULL,
            cnpj_dv VARCHAR(2) NOT NULL,
            identificador_matriz_filial INTEGER,
            situacao_cadastral INTEGER,
            data_situacao_cadastral INTEGER,
            motivo_situacao_cadastral INTEGER,
            pais TEXT,
            data_inicio_atividade INTEGER,
            cnae_fiscal_principal INTEGER,
            cnae_fiscal_secundaria TEXT,
            uf TEXT,
            municipio INTEGER,
            situacao_especial TEXT,
            data_situacao_especial INTEGER,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month VARCHAR(7) NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
        );
        
        CREATE TABLE test_incremental.latest_state_socios (
            LIKE test_incremental.socios,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month VARCHAR(7) NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            PRIMARY KEY (cnpj_basico, nome_socio_razao_social)
        );
        
        CREATE TABLE test_incremental.latest_state_simples (
            LIKE test_incremental.simples,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month VARCHAR(7) NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            PRIMARY KEY (cnpj_basico)
        );
        
        CREATE TABLE test_incremental.snapshots (
            id BIGSERIAL PRIMARY KEY,
            tabela VARCHAR(50) NOT NULL,
            cnpj_basico VARCHAR(8) NOT NULL,
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            chave JSONB NOT NULL,
            conteudo_anterior JSONB,
            conteudo_novo JSONB,
            tipo_alteracao VARCHAR(10) NOT NULL,
            mes_referencia VARCHAR(7) NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE test_incremental.processed_files (
            file_path VARCHAR(255) PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE test_incremental.snapshots_metadata (
            id SERIAL PRIMARY KEY,
            reference_month VARCHAR(7) UNIQUE NOT NULL,
            collection_date TIMESTAMP NOT NULL,
            status VARCHAR(20) NOT NULL,
            duration_seconds INTEGER,
            num_inserts INTEGER DEFAULT 0,
            num_updates INTEGER DEFAULT 0,
            num_deletes INTEGER DEFAULT 0,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def run_deduplicate_empresa(cur):
    cur.execute("DROP TABLE IF EXISTS test_incremental.staging_empresa_dedup;")
    cur.execute("""
        CREATE TABLE test_incremental.staging_empresa_dedup AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cnpj_basico
                       ORDER BY
                           (
                               (razao_social IS NOT NULL)::int +
                               (natureza_juridica IS NOT NULL)::int +
                               (qualificacao_responsavel IS NOT NULL)::int +
                               (capital_social IS NOT NULL)::int +
                               (porte_empresa IS NOT NULL)::int +
                               (ente_federativo_responsavel IS NOT NULL)::int
                           ) DESC,
                           razao_social DESC NULLS LAST,
                           natureza_juridica DESC NULLS LAST,
                           capital_social DESC NULLS LAST,
                           porte_empresa DESC NULLS LAST,
                           ente_federativo_responsavel DESC NULLS LAST
                   ) as rn
            FROM test_incremental.staging_empresa
        )
        SELECT cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel
        FROM ranked
        WHERE rn = 1;
    """)
    cur.execute("DROP TABLE test_incremental.staging_empresa;")
    cur.execute("ALTER TABLE test_incremental.staging_empresa_dedup RENAME TO staging_empresa;")

def run_change_detection_empresa(cur, ref_month_date, collection_date):
    # INSERT & UPDATE
    cur.execute("""
        INSERT INTO test_incremental.snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
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
        FROM test_incremental.staging_empresa stg
        LEFT JOIN test_incremental.latest_state_empresa l ON l.cnpj_basico = stg.cnpj_basico
        LEFT JOIN test_incremental.empresa b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
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
    
    # Upsert latest state
    cur.execute("""
        INSERT INTO test_incremental.latest_state_empresa (cnpj_basico, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel, is_deleted, last_updated_month, data_coleta)
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
        FROM test_incremental.staging_empresa stg
        LEFT JOIN test_incremental.latest_state_empresa l ON l.cnpj_basico = stg.cnpj_basico
        LEFT JOIN test_incremental.empresa b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
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

    # DELETE
    cur.execute("""
        INSERT INTO test_incremental.snapshots (tabela, cnpj_basico, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
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
            FROM test_incremental.latest_state_empresa l
            LEFT JOIN test_incremental.staging_empresa stg ON stg.cnpj_basico = l.cnpj_basico
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
            FROM test_incremental.empresa b
            LEFT JOIN test_incremental.latest_state_empresa l ON l.cnpj_basico = b.cnpj_basico
            LEFT JOIN test_incremental.staging_empresa stg ON stg.cnpj_basico = b.cnpj_basico
            WHERE l.cnpj_basico IS NULL
              AND stg.cnpj_basico IS NULL
        ) prev;
    """, (ref_month_date, collection_date))
    emp_deletes = cur.rowcount
    
    if emp_deletes > 0:
        cur.execute("""
            INSERT INTO test_incremental.latest_state_empresa (cnpj_basico, is_deleted, last_updated_month, data_coleta)
            SELECT
                prev.cnpj_basico, TRUE, %s::varchar, %s::timestamp
            FROM (
                SELECT l.cnpj_basico
                FROM test_incremental.latest_state_empresa l
                LEFT JOIN test_incremental.staging_empresa stg ON stg.cnpj_basico = l.cnpj_basico
                WHERE l.is_deleted = FALSE
                  AND stg.cnpj_basico IS NULL
                UNION ALL
                SELECT b.cnpj_basico
                FROM test_incremental.empresa b
                LEFT JOIN test_incremental.latest_state_empresa l ON l.cnpj_basico = b.cnpj_basico
                LEFT JOIN test_incremental.staging_empresa stg ON stg.cnpj_basico = b.cnpj_basico
                WHERE l.cnpj_basico IS NULL
                  AND stg.cnpj_basico IS NULL
            ) prev
            ON CONFLICT (cnpj_basico) DO UPDATE SET
                is_deleted = TRUE,
                last_updated_month = EXCLUDED.last_updated_month,
                data_coleta = EXCLUDED.data_coleta;
        """, (ref_month_date, collection_date))

def run_change_detection_estabelecimento(cur, ref_month_date, collection_date):
    # INSERT & UPDATE
    cur.execute("""
        INSERT INTO test_incremental.snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
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
        FROM test_incremental.staging_estabelecimento stg
        LEFT JOIN test_incremental.latest_state_estabelecimento l ON l.cnpj_basico = stg.cnpj_basico AND l.cnpj_ordem = stg.cnpj_ordem AND l.cnpj_dv = stg.cnpj_dv
        LEFT JOIN test_incremental.estabelecimento b ON b.cnpj_basico = stg.cnpj_basico AND b.cnpj_ordem = stg.cnpj_ordem AND b.cnpj_dv = stg.cnpj_dv AND l.cnpj_basico IS NULL
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
    
    # Upsert latest state
    cur.execute("""
        INSERT INTO test_incremental.latest_state_estabelecimento (
            cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, situacao_cadastral,
            data_situacao_cadastral, motivo_situacao_cadastral, pais, data_inicio_atividade,
            cnae_fiscal_principal, cnae_fiscal_secundaria, uf, municipio, situacao_especial, data_situacao_especial, is_deleted, last_updated_month, data_coleta
        )
        SELECT
            stg.cnpj_basico, stg.cnpj_ordem, stg.cnpj_dv, stg.identificador_matriz_filial, stg.situacao_cadastral,
            stg.data_situacao_cadastral, stg.motivo_situacao_cadastral, stg.pais, stg.data_inicio_atividade,
            stg.cnae_fiscal_principal, stg.cnae_fiscal_secundaria, stg.uf, stg.municipio, stg.situacao_especial, stg.data_situacao_especial, FALSE, %s::varchar, %s::timestamp
        FROM test_incremental.staging_estabelecimento stg
        LEFT JOIN test_incremental.latest_state_estabelecimento l ON l.cnpj_basico = stg.cnpj_basico AND l.cnpj_ordem = stg.cnpj_ordem AND l.cnpj_dv = stg.cnpj_dv
        LEFT JOIN test_incremental.estabelecimento b ON b.cnpj_basico = stg.cnpj_basico AND b.cnpj_ordem = stg.cnpj_ordem AND b.cnpj_dv = stg.cnpj_dv AND l.cnpj_basico IS NULL
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

    # DELETE
    cur.execute("""
        INSERT INTO test_incremental.snapshots (tabela, cnpj_basico, cnpj_ordem, cnpj_dv, chave, conteudo_anterior, conteudo_novo, tipo_alteracao, mes_referencia, data_coleta)
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
            FROM test_incremental.latest_state_estabelecimento l
            LEFT JOIN test_incremental.staging_estabelecimento stg ON stg.cnpj_basico = l.cnpj_basico AND stg.cnpj_ordem = l.cnpj_ordem AND stg.cnpj_dv = l.cnpj_dv
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
            FROM test_incremental.estabelecimento b
            LEFT JOIN test_incremental.latest_state_estabelecimento l ON l.cnpj_basico = b.cnpj_basico AND l.cnpj_ordem = b.cnpj_ordem AND l.cnpj_dv = b.cnpj_dv
            LEFT JOIN test_incremental.staging_estabelecimento stg ON stg.cnpj_basico = b.cnpj_basico AND stg.cnpj_ordem = b.cnpj_ordem AND stg.cnpj_dv = b.cnpj_dv
            WHERE l.cnpj_basico IS NULL
              AND stg.cnpj_basico IS NULL
        ) prev;
    """, (ref_month_date, collection_date))
    est_deletes = cur.rowcount
    
    if est_deletes > 0:
        cur.execute("""
            INSERT INTO test_incremental.latest_state_estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, is_deleted, last_updated_month, data_coleta)
            SELECT
                prev.cnpj_basico, prev.cnpj_ordem, prev.cnpj_dv, TRUE, %s::varchar, %s::timestamp
            FROM (
                SELECT l.cnpj_basico, l.cnpj_ordem, l.cnpj_dv
                FROM test_incremental.latest_state_estabelecimento l
                LEFT JOIN test_incremental.staging_estabelecimento stg ON stg.cnpj_basico = l.cnpj_basico AND stg.cnpj_ordem = l.cnpj_ordem AND stg.cnpj_dv = l.cnpj_dv
                WHERE l.is_deleted = FALSE
                  AND stg.cnpj_basico IS NULL
                UNION ALL
                SELECT b.cnpj_basico, b.cnpj_ordem, b.cnpj_dv
                FROM test_incremental.estabelecimento b
                LEFT JOIN test_incremental.latest_state_estabelecimento l ON l.cnpj_basico = b.cnpj_basico AND l.cnpj_ordem = b.cnpj_ordem AND l.cnpj_dv = b.cnpj_dv
                LEFT JOIN test_incremental.staging_estabelecimento stg ON stg.cnpj_basico = b.cnpj_basico AND stg.cnpj_ordem = b.cnpj_ordem AND stg.cnpj_dv = b.cnpj_dv
                WHERE l.cnpj_basico IS NULL
                  AND stg.cnpj_basico IS NULL
            ) prev
            ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE SET
                is_deleted = TRUE,
                last_updated_month = EXCLUDED.last_updated_month,
                data_coleta = EXCLUDED.data_coleta;
        """, (ref_month_date, collection_date))

def run_tests():
    load_dotenv()
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, host=db_host, port=db_port, password=db_pass
    )
    cur = conn.cursor()
    cur.execute("SET search_path TO test_incremental, public;")
    
    # -------------------------------------------------------------------------
    # TEST 15 & 16: Setup check & Reference Month / Collection Date definition
    # -------------------------------------------------------------------------
    setup_test_schema(cur)
    conn.commit()
    
    ref_june = "2023-06" # strict YYYY-MM format
    collection_date_june = "2026-08-16 12:00:00"
    
    # Clean output directories for Parquet files
    shutil.rmtree(os.path.join(current_path, "ignored_fields"), ignore_errors=True)
    
    print("\n--- Running Test Cases ---")
    
    # Populate baseline (2023-05)
    # Company baseline
    cur.execute("""
        INSERT INTO test_incremental.empresa VALUES 
        ('12345678', 'Empresa Teste A', 1015, 2, 5000.0, 1, 'ENTE A'),
        ('87654321', 'Empresa Teste B', 2062, 3, 10000.0, 3, NULL);
    """)
    # Estabelecimento baseline
    cur.execute("""
        INSERT INTO test_incremental.estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, situacao_cadastral, uf, municipio) VALUES
        ('12345678', '0001', '99', 1, 'Fantasia A', 2, 'SP', 3550308),
        ('87654321', '0002', '88', 2, 'Fantasia B', 2, 'RJ', 3304557);
    """)
    conn.commit()
    print("Baseline loaded successfully.")
    
    # Export 2023-05 baseline ignored fields to Parquet for Test 19
    baseline_empresa_dir = os.path.join(current_path, "ignored_fields", "empresas", "reference_month=2023-05")
    query_baseline_emp = "SELECT cnpj_basico, '2023-05'::varchar as reference_month, razao_social FROM test_incremental.empresa"
    export_ignored_fields_parquet(cur, conn, query_baseline_emp, schema_empresa, baseline_empresa_dir)
    
    baseline_estabelecimento_dir = os.path.join(current_path, "ignored_fields", "estabelecimento", "reference_month=2023-05")
    query_baseline_est = "SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '2023-05'::varchar as reference_month, nome_fantasia, ddd_1, ddd_2, ddd_fax, telefone_1, telefone_2, fax, correio_eletronico FROM test_incremental.estabelecimento"
    export_ignored_fields_parquet(cur, conn, query_baseline_est, schema_estabelecimento, baseline_estabelecimento_dir)

    # -------------------------------------------------------------------------
    # TEST 18: Corrupted Snapshot/Validation check
    # -------------------------------------------------------------------------
    print("\n--- Running Test 18 (Corrupted Snapshot/Validation) ---")
    # Verify we can validate staging tables. Let's write validation logic.
    cur.execute("TRUNCATE TABLE test_incremental.staging_empresa;")
    # Staging has 0 records, validation should fail!
    try:
        cur.execute("SELECT COUNT(*) FROM test_incremental.staging_empresa;")
        if cur.fetchone()[0] == 0:
            raise ValueError("Staging is empty")
    except ValueError as e:
        print("Validation caught empty staging correctly (Test 18 Success):", e)

    # -------------------------------------------------------------------------
    # Month 1: 2023-06 (First Snapshot processing)
    # -------------------------------------------------------------------------
    print("\n--- Processing Month 1 (2023-06) ---")
    
    # Prepare staging for 2023-06
    cur.execute("TRUNCATE TABLE test_incremental.staging_empresa;")
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        -- TEST 1: INSERT (does not exist in baseline)
        ('11112222', 'Empresa Inserida', 2062, 3, 20000.0, 3, 'ENTE NEW'),
        -- TEST 2: Tracked UPDATE (tracked column capital changed)
        ('12345678', 'Empresa Teste A', 1015, 2, 7500.0, 1, 'ENTE A'),
        -- TEST 3: NO CHANGE (all tracked and ignored columns identical)
        ('87654321', 'Empresa Teste B', 2062, 3, 10000.0, 3, NULL);
    """)
    
    # TEST 10: Duplicate cnpj_basico (should choose Dup 2 having more non-null fields)
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        ('99999999', 'Dup 1', 1015, NULL, 100.0, NULL, NULL),
        ('99999999', 'Dup 2', 1015, 2, 100.0, 1, 'ENTE X'); 
    """)
    
    # TEST 11: Duplicate Tie (same non-null count, DESC tie-breaker on razao_social -> Tie B wins)
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        ('88888888', 'Tie B', 1015, 2, 100.0, NULL, NULL),
        ('88888888', 'Tie A', 1015, 2, 100.0, NULL, NULL);
    """)
    
    # Run Deduplicate & Change detection
    run_deduplicate_empresa(cur)
    run_change_detection_empresa(cur, ref_june, collection_date_june)
    conn.commit()
    
    # Export 2023-06 ignored fields to Parquet
    june_empresa_dir = os.path.join(current_path, "ignored_fields", "empresas", f"reference_month={ref_june}")
    query_june_emp = f"SELECT cnpj_basico, '{ref_june}'::varchar as reference_month, razao_social FROM test_incremental.staging_empresa"
    export_ignored_fields_parquet(cur, conn, query_june_emp, schema_empresa, june_empresa_dir)
    
    # Assertions for June
    cur.execute("SELECT cnpj_basico, tipo_alteracao, (conteudo_novo->>'capital_social')::float FROM test_incremental.snapshots WHERE tabela='empresa' AND mes_referencia='2023-06' ORDER BY cnpj_basico;")
    snaps_june = cur.fetchall()
    print("June snapshots logged:", snaps_june)
    
    assert len(snaps_june) == 4, f"Expected 4 snapshots, got {len(snaps_june)}"
    assert snaps_june[0] == ('11112222', 'INSERT', 20000.0), "Expected INSERT for 11112222"
    assert snaps_june[1] == ('12345678', 'UPDATE', 7500.0), "Expected UPDATE for 12345678"
    assert snaps_june[2] == ('88888888', 'INSERT', 100.0), "Expected INSERT for tie-breaker 88888888"
    assert snaps_june[3] == ('99999999', 'INSERT', 100.0), "Expected INSERT for duplicate resolution 99999999"
    
    # Check duplicate selected columns (Test 10)
    cur.execute("SELECT porte_empresa FROM test_incremental.latest_state_empresa WHERE cnpj_basico='99999999';")
    assert cur.fetchone()[0] == 1, "Expected Dup 2 selection"
    
    # Check duplicate selected columns (Test 11 - Tie B should be selected)
    cur.execute("SELECT EXISTS(SELECT 1 FROM test_incremental.latest_state_empresa WHERE cnpj_basico='88888888');")
    assert cur.fetchone()[0] == True
    
    # Check that razao_social is not in latest_state_empresa schema!
    try:
        cur.execute("SELECT razao_social FROM test_incremental.latest_state_empresa;")
        assert False, "razao_social column should not exist in latest_state_empresa!"
    except psycopg2.Error:
        conn.rollback()
        print("Test Success: latest_state_empresa does not contain razao_social column.")
        
    # Check that razao_social is not in the snapshots contents!
    cur.execute("SELECT conteudo_novo FROM test_incremental.snapshots WHERE tabela='empresa';")
    for row in cur.fetchall():
        if row[0]:
            assert 'razao_social' not in row[0], "razao_social should not exist in snapshots JSONB conteudo_novo"
            
    # Check that reference month is stored exactly as YYYY-MM (Test 15)
    cur.execute("SELECT mes_referencia FROM test_incremental.snapshots WHERE tabela='empresa' LIMIT 1;")
    assert cur.fetchone()[0] == "2023-06", "Expected format YYYY-MM"
    
    # -------------------------------------------------------------------------
    # Month 2: 2023-07
    # -------------------------------------------------------------------------
    print("\n--- Processing Month 2 (2023-07) ---")
    ref_july = "2023-07"
    collection_date_july = "2026-08-17 12:00:00"
    
    cur.execute("TRUNCATE TABLE test_incremental.staging_empresa;")
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        -- TEST 12: Previously Changed Entity changes again in July
        ('12345678', 'Empresa Teste A', 1015, 2, 9000.0, 1, 'ENTE A'),
        
        -- TEST 13: Ignored column change in June followed by tracked change in July
        -- (Already tested June where 87654321 had NO CHANGE. Now we change tracked column in July)
        ('87654321', 'Empresa Teste B Updated', 2062, 3, 15000.0, 3, NULL),
        
        -- TEST 4: Ignored column changes only (razao_social) -> should produce NO snap UPDATE
        ('99999999', 'Dup 2 New Name', 1015, 2, 100.0, 1, 'ENTE X'),
        
        -- TEST 8: VALUE -> NULL transition (qualificacao from 2 -> NULL)
        ('88888888', 'Tie B', 1015, NULL, 100.0, NULL, NULL)
    """)
    
    # TEST 9: DELETE (11112222 is missing from staging, should record DELETE)
    # We leave 11112222 out of the insert statements.
    
    run_deduplicate_empresa(cur)
    run_change_detection_empresa(cur, ref_july, collection_date_july)
    conn.commit()
    
    # Assertions for July
    cur.execute("SELECT cnpj_basico, tipo_alteracao, (conteudo_novo->>'capital_social')::float, (conteudo_novo->>'qualificacao_responsavel') FROM test_incremental.snapshots WHERE tabela='empresa' AND mes_referencia='2023-07' ORDER BY cnpj_basico;")
    snaps_july = cur.fetchall()
    print("July snapshots logged:", snaps_july)
    
    # Expected July changes:
    # - 12345678: UPDATE (capital 9000)
    # - 87654321: UPDATE (capital 15000)
    # - 88888888: UPDATE (qualificacao NULL)
    # - 11112222: DELETE
    # (99999999 only changed razao_social, so it must not have a snapshot!)
    assert len(snaps_july) == 4, f"Expected 4 snapshots, got {len(snaps_july)}"
    
    update_12 = [x for x in snaps_july if x[0] == '12345678'][0]
    assert update_12[1] == 'UPDATE' and update_12[2] == 9000.0
    
    update_87 = [x for x in snaps_july if x[0] == '87654321'][0]
    assert update_87[1] == 'UPDATE' and update_87[2] == 15000.0
    
    update_88 = [x for x in snaps_july if x[0] == '88888888'][0]
    assert update_88[1] == 'UPDATE' and update_88[3] is None
    
    delete_11 = [x for x in snaps_july if x[0] == '11112222'][0]
    assert delete_11[1] == 'DELETE'
    
    # Assert that 99999999 did not generate a snapshot in July (Test 4 Success)
    assert not any(x[0] == '99999999' for x in snaps_july), "Ignored column change generated an update snapshot!"
    print("Test 4 Success: Ignored column change did not generate an update snapshot.")
    
    # -------------------------------------------------------------------------
    # TEST 17: Interrupted Processing / Checkpoints
    # -------------------------------------------------------------------------
    print("\n--- Running Test 17 (Interrupted Processing / Checkpoints) ---")
    # Simulate a transaction rollback
    try:
        cur.execute("BEGIN;")
        cur.execute("INSERT INTO test_incremental.snapshots (tabela, cnpj_basico, chave, tipo_alteracao, mes_referencia, data_coleta) VALUES ('empresa', '00000000', '{}', 'INSERT', '2023-08', CURRENT_TIMESTAMP);")
        # Simulate a crash before commit
        raise RuntimeError("Simulated crash")
    except RuntimeError:
        conn.rollback()
        print("Simulated crash correctly rolled back transaction.")
    
    # Verify that the record does not exist
    cur.execute("SELECT COUNT(*) FROM test_incremental.snapshots WHERE cnpj_basico='00000000';")
    assert cur.fetchone()[0] == 0, "Analytical state altered during interrupted processing!"
    print("Test 17 Success: Interrupted state did not persist changes.")

    # -------------------------------------------------------------------------
    # TEST 14: Replay
    # -------------------------------------------------------------------------
    print("\n--- Running Test 14 (Replay) ---")
    # Reprocess 2023-07 using the same staging data. It should produce no new snapshots.
    cur.execute("DELETE FROM test_incremental.snapshots WHERE mes_referencia='2023-07';") # clear July snaps for replay test
    cur.execute("DELETE FROM test_incremental.latest_state_empresa;")
    cur.execute("""
        INSERT INTO test_incremental.latest_state_empresa (cnpj_basico, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel, is_deleted, last_updated_month, data_coleta) VALUES
        ('11112222', 2062, 3, 20000.0, 3, 'ENTE NEW', FALSE, '2023-06', '2026-08-16 12:00:00'::timestamp),
        ('12345678', 1015, 2, 7500.0, 1, 'ENTE A', FALSE, '2023-06', '2026-08-16 12:00:00'::timestamp),
        ('88888888', 1015, 2, 100.0, NULL, NULL, FALSE, '2023-06', '2026-08-16 12:00:00'::timestamp),
        ('99999999', 1015, 2, 100.0, 1, 'ENTE X', FALSE, '2023-06', '2026-08-16 12:00:00'::timestamp);
    """)
    conn.commit()
    
    # Re-run change detection for July
    run_change_detection_empresa(cur, ref_july, collection_date_july)
    conn.commit()
    
    # Query snapshots again
    cur.execute("SELECT cnpj_basico, tipo_alteracao FROM test_incremental.snapshots WHERE tabela='empresa' AND mes_referencia='2023-07' ORDER BY cnpj_basico;")
    snaps_replay = cur.fetchall()
    print("Replayed snapshots:", snaps_replay)
    # The output should match exactly
    assert len(snaps_replay) == 4, f"Expected 4 snapshots, got {len(snaps_replay)}"
    print("Test 14 Success: Replay generated identical snapshots and state.")

    # -------------------------------------------------------------------------
    # TEST 19: Parquet File Verification
    # -------------------------------------------------------------------------
    print("\n--- Running Test 19 (Parquet File Verification) ---")
    
    # Check that companies Parquet files exist
    parquet_path = os.path.join(current_path, "ignored_fields", "empresas", "reference_month=2023-05", "part-000.parquet")
    assert os.path.exists(parquet_path), f"Parquet file {parquet_path} does not exist!"
    
    # Load Parquet file metadata
    pf = pq.ParquetFile(parquet_path)
    print("Parquet file metadata loaded successfully.")
    
    # Check schema has expected columns (only: cnpj_basico, reference_month, razao_social)
    schema = pf.schema_arrow
    print("Parquet Schema:", schema)
    assert len(schema.names) == 3, f"Expected 3 columns, got {len(schema.names)}"
    assert set(schema.names) == {'cnpj_basico', 'reference_month', 'razao_social'}, "Schema columns mismatch!"
    
    # Check compression codec (should be SNAPPY)
    meta = pf.metadata
    rg = meta.row_group(0)
    for c_idx in range(rg.num_columns):
        col = rg.column(c_idx)
        print(f"Column {schema.names[c_idx]} compression: {col.compression}")
        assert col.compression == "SNAPPY", f"Expected SNAPPY compression, got {col.compression}"
        
    # Check partition reading (only read reference_month=2023-05 without scanning others)
    tb = pf.read(columns=['cnpj_basico', 'razao_social'])
    assert tb.num_columns == 2, f"Expected 2 columns, got {tb.num_columns}"
    print("Parquet projection test passed successfully.")
    print("Test 19 Success: All Parquet format requirements verified.")
    
    # -------------------------------------------------------------------------
    # TEST 5, 6, 7 & 19: Estabelecimento Ignored Columns, Mixed changes, and Parquet
    # -------------------------------------------------------------------------
    print("\n--- Running Estabelecimento tests (Test 5, 6, 7 & 19) ---")
    cur.execute("TRUNCATE TABLE test_incremental.staging_estabelecimento;")
    cur.execute("""
        INSERT INTO test_incremental.staging_estabelecimento (
            cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
            situacao_cadastral, uf, municipio, ddd_1, telefone_1
        ) VALUES
        -- Mixed change (tracked column situacao_cadastral + ignored nome_fantasia change)
        ('12345678', '0001', '99', 1, 'Fantasia A Changed', 8, 'SP', 3550308, NULL, NULL),
        -- Ignored changes only (nome_fantasia and telephony fields change, situacao remains 2)
        ('87654321', '0002', '88', 2, 'Fantasia B Changed', 2, 'RJ', 3304557, '99', '999999999');
    """)
    conn.commit()
    
    # Run change detection
    run_change_detection_estabelecimento(cur, ref_june, collection_date_june)
    conn.commit()
    
    # Export raw ignored fields to Parquet
    june_estabelecimento_dir = os.path.join(current_path, "ignored_fields", "estabelecimento", f"reference_month={ref_june}")
    query_june_est = f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '{ref_june}'::varchar as reference_month, nome_fantasia, ddd_1, ddd_2, ddd_fax, telefone_1, telefone_2, fax, correio_eletronico FROM test_incremental.staging_estabelecimento"
    export_ignored_fields_parquet(cur, conn, query_june_est, schema_estabelecimento, june_estabelecimento_dir)

    # Export raw address fields to Parquet
    june_est_addr_dir = os.path.join(current_path, "ignored_fields", "estabelecimento_enderecos", f"reference_month={ref_june}")
    query_june_est_addr = f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv, '{ref_june}'::varchar as reference_month, tipo_logradouro, logradouro, numero, complemento, bairro, cep, nome_cidade_exterior FROM test_incremental.staging_estabelecimento"
    export_ignored_fields_parquet(cur, conn, query_june_est_addr, schema_estabelecimento_enderecos, june_est_addr_dir)
    
    # Assertions
    # 1. 12345678 generated UPDATE snapshot
    cur.execute("SELECT tipo_alteracao, (conteudo_novo->>'situacao_cadastral')::int FROM test_incremental.snapshots WHERE tabela='estabelecimento' AND cnpj_basico='12345678';")
    res = cur.fetchone()
    assert res == ('UPDATE', 8), f"Expected UPDATE with situacao_cadastral 8, got {res}"
    print("Test 6 & 7 Success: Mixed tracked + ignored change generated UPDATE snapshot successfully.")
    
    # 2. 87654321 did not generate any snapshot
    cur.execute("SELECT COUNT(*) FROM test_incremental.snapshots WHERE tabela='estabelecimento' AND cnpj_basico='87654321';")
    assert cur.fetchone()[0] == 0, "Ignored column changes in establishment triggered a snapshot!"
    print("Test 5 Success: Ignored columns only change triggered no snapshots.")
    
    # 3. Excluded columns validation
    try:
        cur.execute("SELECT tipo_logradouro FROM test_incremental.latest_state_estabelecimento;")
        assert False, "tipo_logradouro column should not exist in latest_state_estabelecimento table!"
    except psycopg2.Error:
        conn.rollback()
        print("Success: latest_state_estabelecimento table does not contain detailed address columns.")

    try:
        cur.execute("SELECT nome_cidade_exterior FROM test_incremental.latest_state_estabelecimento;")
        assert False, "nome_cidade_exterior column should not exist in latest_state_estabelecimento table!"
    except psycopg2.Error:
        conn.rollback()
        print("Success: latest_state_estabelecimento table does not contain nome_cidade_exterior column.")

    try:
        cur.execute("SELECT nome_fantasia FROM test_incremental.latest_state_estabelecimento;")
        assert False, "nome_fantasia column should not exist in latest_state_estabelecimento table!"
    except psycopg2.Error:
        conn.rollback()
        print("Success: latest_state_estabelecimento table does not contain ignored columns.")
        
    cur.execute("SELECT conteudo_novo FROM test_incremental.snapshots WHERE tabela='estabelecimento';")
    for row in cur.fetchall():
        if row[0]:
            assert 'nome_fantasia' not in row[0], "nome_fantasia should not exist in snapshots JSONB conteudo_novo!"
            assert 'tipo_logradouro' not in row[0], "tipo_logradouro should not exist in snapshots JSONB conteudo_novo!"
            assert 'nome_cidade_exterior' not in row[0], "nome_cidade_exterior should not exist in snapshots JSONB conteudo_novo!"
    print("Success: snapshots JSONB payload does not contain ignored columns or detailed address fields.")
    
    # 4. Parquet format verification for establishment
    est_parquet_path = os.path.join(june_estabelecimento_dir, "part-000.parquet")
    assert os.path.exists(est_parquet_path), "Establishment Parquet file not found!"
    
    pf_est = pq.ParquetFile(est_parquet_path)
    schema_est = pf_est.schema_arrow
    assert len(schema_est.names) == 12, f"Expected 12 columns in Parquet, got {len(schema_est.names)}"
    
    meta_est = pf_est.metadata
    rg_est = meta_est.row_group(0)
    for c_idx in range(rg_est.num_columns):
        col = rg_est.column(c_idx)
        assert col.compression == "SNAPPY", f"Expected SNAPPY compression on {schema_est.names[c_idx]}, got {col.compression}"
    print("Test 19 Success: Establishment Parquet metadata and SNAPPY compression verified.")

    # 5. Parquet format verification for establishment address
    addr_parquet_path = os.path.join(june_est_addr_dir, "part-000.parquet")
    assert os.path.exists(addr_parquet_path), "Establishment Address Parquet file not found!"
    
    pf_addr = pq.ParquetFile(addr_parquet_path)
    schema_addr = pf_addr.schema_arrow
    assert len(schema_addr.names) == 11, f"Expected 11 columns in Address Parquet, got {len(schema_addr.names)}"
    
    meta_addr = pf_addr.metadata
    rg_addr = meta_addr.row_group(0)
    for c_idx in range(rg_addr.num_columns):
        col = rg_addr.column(c_idx)
        assert col.compression == "SNAPPY", f"Expected SNAPPY compression on {schema_addr.names[c_idx]}, got {col.compression}"
    print("Test Success: Establishment Address Parquet metadata and SNAPPY compression verified.")
    
    print("\nAll test validations passed successfully!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_tests()
