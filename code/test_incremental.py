import os
import sys
import psycopg2
from dotenv import load_dotenv

def setup_test_schema(cur):
    print("Setting up test schema...")
    cur.execute("DROP SCHEMA IF EXISTS test_incremental CASCADE;")
    cur.execute("CREATE SCHEMA test_incremental;")
    
    # Create test tables equivalent to production
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
            pais TEXT,
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
            LIKE test_incremental.empresa,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month DATE NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            PRIMARY KEY (cnpj_basico)
        );
        CREATE TABLE test_incremental.latest_state_estabelecimento (
            LIKE test_incremental.estabelecimento,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month DATE NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
        );
        
        CREATE TABLE test_incremental.latest_state_socios (
            LIKE test_incremental.socios,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month DATE NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            PRIMARY KEY (cnpj_basico, nome_socio_razao_social)
        );
        
        CREATE TABLE test_incremental.latest_state_simples (
            LIKE test_incremental.simples,
            is_deleted BOOLEAN DEFAULT FALSE,
            last_updated_month DATE NOT NULL,
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
            mes_referencia DATE NOT NULL,
            data_coleta TIMESTAMP NOT NULL,
            data_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE test_incremental.processed_files (
            file_path VARCHAR(255) PRIMARY KEY,
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
            row_to_json(stg)::jsonb,
            CASE WHEN p.is_new THEN 'INSERT' ELSE 'UPDATE' END,
            %s::date,
            %s::timestamp
        FROM test_incremental.staging_empresa stg
        LEFT JOIN test_incremental.latest_state_empresa l ON l.cnpj_basico = stg.cnpj_basico
        LEFT JOIN test_incremental.empresa b ON b.cnpj_basico = stg.cnpj_basico AND l.cnpj_basico IS NULL
        CROSS JOIN LATERAL (
            SELECT
                ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                CASE
                    WHEN l.cnpj_basico IS NOT NULL THEN (row_to_json(l)::jsonb - 'is_deleted' - 'last_updated_month' - 'data_coleta')
                    ELSE row_to_json(b)::jsonb
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
        INSERT INTO test_incremental.latest_state_empresa (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel, is_deleted, last_updated_month, data_coleta)
        SELECT
            stg.cnpj_basico,
            stg.razao_social,
            stg.natureza_juridica,
            stg.qualificacao_responsavel,
            stg.capital_social,
            stg.porte_empresa,
            stg.ente_federativo_responsavel,
            FALSE,
            %s::date,
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
            razao_social = EXCLUDED.razao_social,
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
            %s::date,
            %s::timestamp
        FROM (
            SELECT l.cnpj_basico, (row_to_json(l)::jsonb - 'is_deleted' - 'last_updated_month' - 'data_coleta') as old_row
            FROM test_incremental.latest_state_empresa l
            LEFT JOIN test_incremental.staging_empresa stg ON stg.cnpj_basico = l.cnpj_basico
            WHERE l.is_deleted = FALSE
              AND stg.cnpj_basico IS NULL
            UNION ALL
            SELECT b.cnpj_basico, row_to_json(b)::jsonb as old_row
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
                prev.cnpj_basico, TRUE, %s::date, %s::timestamp
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
            row_to_json(stg)::jsonb,
            CASE WHEN p.is_new THEN 'INSERT' ELSE 'UPDATE' END,
            %s::date,
            %s::timestamp
        FROM test_incremental.staging_estabelecimento stg
        LEFT JOIN test_incremental.latest_state_estabelecimento l ON l.cnpj_basico = stg.cnpj_basico AND l.cnpj_ordem = stg.cnpj_ordem AND l.cnpj_dv = stg.cnpj_dv
        LEFT JOIN test_incremental.estabelecimento b ON b.cnpj_basico = stg.cnpj_basico AND b.cnpj_ordem = stg.cnpj_ordem AND b.cnpj_dv = stg.cnpj_dv AND l.cnpj_basico IS NULL
        CROSS JOIN LATERAL (
            SELECT
                ( (l.cnpj_basico IS NULL AND b.cnpj_basico IS NULL) OR (l.cnpj_basico IS NOT NULL AND l.is_deleted = TRUE) ) as is_new,
                CASE
                    WHEN l.cnpj_basico IS NOT NULL THEN (row_to_json(l)::jsonb - 'is_deleted' - 'last_updated_month' - 'data_coleta')
                    ELSE row_to_json(b)::jsonb
                END as old_row,
                COALESCE(l.is_deleted, FALSE) as prev_deleted,
                COALESCE(l.identificador_matriz_filial, b.identificador_matriz_filial) as prev_identificador_matriz_filial,
                COALESCE(l.situacao_cadastral, b.situacao_cadastral) as prev_situacao_cadastral,
                COALESCE(l.data_situacao_cadastral, b.data_situacao_cadastral) as prev_data_situacao_cadastral,
                COALESCE(l.motivo_situacao_cadastral, b.motivo_situacao_cadastral) as prev_motivo_situacao_cadastral,
                COALESCE(l.nome_cidade_exterior, b.nome_cidade_exterior) as prev_nome_cidade_exterior,
                COALESCE(l.pais, b.pais) as prev_pais,
                COALESCE(l.data_inicio_atividade, b.data_inicio_atividade) as prev_data_inicio_atividade,
                COALESCE(l.cnae_fiscal_principal, b.cnae_fiscal_principal) as prev_cnae_fiscal_principal,
                COALESCE(l.cnae_fiscal_secundaria, b.cnae_fiscal_secundaria) as prev_cnae_fiscal_secundaria,
                COALESCE(l.tipo_logradouro, b.tipo_logradouro) as prev_tipo_logradouro,
                COALESCE(l.logradouro, b.logradouro) as prev_logradouro,
                COALESCE(l.numero, b.numero) as prev_numero,
                COALESCE(l.complemento, b.complemento) as prev_complemento,
                COALESCE(l.bairro, b.bairro) as prev_bairro,
                COALESCE(l.cep, b.cep) as prev_cep,
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
                    stg.nome_cidade_exterior IS DISTINCT FROM p.prev_nome_cidade_exterior OR
                    stg.pais IS DISTINCT FROM p.prev_pais OR
                    stg.data_inicio_atividade IS DISTINCT FROM p.prev_data_inicio_atividade OR
                    stg.cnae_fiscal_principal IS DISTINCT FROM p.prev_cnae_fiscal_principal OR
                    stg.cnae_fiscal_secundaria IS DISTINCT FROM p.prev_cnae_fiscal_secundaria OR
                    stg.tipo_logradouro IS DISTINCT FROM p.prev_tipo_logradouro OR
                    stg.logradouro IS DISTINCT FROM p.prev_logradouro OR
                    stg.numero IS DISTINCT FROM p.prev_numero OR
                    stg.complemento IS DISTINCT FROM p.prev_complemento OR
                    stg.bairro IS DISTINCT FROM p.prev_bairro OR
                    stg.cep IS DISTINCT FROM p.prev_cep OR
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
            cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, situacao_cadastral,
            data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividade,
            cnae_fiscal_principal, cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento,
            bairro, cep, uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
            correio_eletronico, situacao_especial, data_situacao_especial, is_deleted, last_updated_month, data_coleta
        )
        SELECT
            stg.cnpj_basico, stg.cnpj_ordem, stg.cnpj_dv, stg.identificador_matriz_filial, stg.nome_fantasia, stg.situacao_cadastral,
            stg.data_situacao_cadastral, stg.motivo_situacao_cadastral, stg.nome_cidade_exterior, stg.pais, stg.data_inicio_atividade,
            stg.cnae_fiscal_principal, stg.cnae_fiscal_secundaria, stg.tipo_logradouro, stg.logradouro, stg.numero, stg.complemento,
            stg.bairro, stg.cep, stg.uf, stg.municipio, stg.ddd_1, stg.telefone_1, stg.ddd_2, stg.telefone_2, stg.ddd_fax, stg.fax,
            stg.correio_eletronico, stg.situacao_especial, stg.data_situacao_especial, FALSE, %s::date, %s::timestamp
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
                COALESCE(l.nome_cidade_exterior, b.nome_cidade_exterior) as prev_nome_cidade_exterior,
                COALESCE(l.pais, b.pais) as prev_pais,
                COALESCE(l.data_inicio_atividade, b.data_inicio_atividade) as prev_data_inicio_atividade,
                COALESCE(l.cnae_fiscal_principal, b.cnae_fiscal_principal) as prev_cnae_fiscal_principal,
                COALESCE(l.cnae_fiscal_secundaria, b.cnae_fiscal_secundaria) as prev_cnae_fiscal_secundaria,
                COALESCE(l.tipo_logradouro, b.tipo_logradouro) as prev_tipo_logradouro,
                COALESCE(l.logradouro, b.logradouro) as prev_logradouro,
                COALESCE(l.numero, b.numero) as prev_numero,
                COALESCE(l.complemento, b.complemento) as prev_complemento,
                COALESCE(l.bairro, b.bairro) as prev_bairro,
                COALESCE(l.cep, b.cep) as prev_cep,
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
                    stg.nome_cidade_exterior IS DISTINCT FROM p.prev_nome_cidade_exterior OR
                    stg.pais IS DISTINCT FROM p.prev_pais OR
                    stg.data_inicio_atividade IS DISTINCT FROM p.prev_data_inicio_atividade OR
                    stg.cnae_fiscal_principal IS DISTINCT FROM p.prev_cnae_fiscal_principal OR
                    stg.cnae_fiscal_secundaria IS DISTINCT FROM p.prev_cnae_fiscal_secundaria OR
                    stg.tipo_logradouro IS DISTINCT FROM p.prev_tipo_logradouro OR
                    stg.logradouro IS DISTINCT FROM p.prev_logradouro OR
                    stg.numero IS DISTINCT FROM p.prev_numero OR
                    stg.complemento IS DISTINCT FROM p.prev_complemento OR
                    stg.bairro IS DISTINCT FROM p.prev_bairro OR
                    stg.cep IS DISTINCT FROM p.prev_cep OR
                    stg.uf IS DISTINCT FROM p.prev_uf OR
                    stg.municipio IS DISTINCT FROM p.prev_municipio OR
                    stg.situacao_especial IS DISTINCT FROM p.prev_situacao_especial OR
                    stg.data_situacao_especial IS DISTINCT FROM p.prev_data_situacao_especial
                )
            )
        ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE SET
            identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
            nome_fantasia = EXCLUDED.nome_fantasia,
            situacao_cadastral = EXCLUDED.situacao_cadastral,
            data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
            motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
            nome_cidade_exterior = EXCLUDED.nome_cidade_exterior,
            pais = EXCLUDED.pais,
            data_inicio_atividade = EXCLUDED.data_inicio_atividade,
            cnae_fiscal_principal = EXCLUDED.cnae_fiscal_principal,
            cnae_fiscal_secundaria = EXCLUDED.cnae_fiscal_secundaria,
            tipo_logradouro = EXCLUDED.tipo_logradouro,
            logradouro = EXCLUDED.logradouro,
            numero = EXCLUDED.numero,
            complemento = EXCLUDED.complemento,
            bairro = EXCLUDED.bairro,
            cep = EXCLUDED.cep,
            uf = EXCLUDED.uf,
            municipio = EXCLUDED.municipio,
            ddd_1 = EXCLUDED.ddd_1,
            telefone_1 = EXCLUDED.telefone_1,
            ddd_2 = EXCLUDED.ddd_2,
            telefone_2 = EXCLUDED.telefone_2,
            ddd_fax = EXCLUDED.ddd_fax,
            fax = EXCLUDED.fax,
            correio_eletronico = EXCLUDED.correio_eletronico,
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
            %s::date,
            %s::timestamp
        FROM (
            SELECT l.cnpj_basico, l.cnpj_ordem, l.cnpj_dv, (row_to_json(l)::jsonb - 'is_deleted' - 'last_updated_month' - 'data_coleta') as old_row
            FROM test_incremental.latest_state_estabelecimento l
            LEFT JOIN test_incremental.staging_estabelecimento stg ON stg.cnpj_basico = l.cnpj_basico AND stg.cnpj_ordem = l.cnpj_ordem AND stg.cnpj_dv = l.cnpj_dv
            WHERE l.is_deleted = FALSE
              AND stg.cnpj_basico IS NULL
            UNION ALL
            SELECT b.cnpj_basico, b.cnpj_ordem, b.cnpj_dv, row_to_json(b)::jsonb as old_row
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
                prev.cnpj_basico, prev.cnpj_ordem, prev.cnpj_dv, TRUE, %s::date, %s::timestamp
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
    
    setup_test_schema(cur)
    conn.commit()
    
    collection_date = "2026-08-16 12:00:00"
    
    print("\n--- Running Test Cases ---")
    
    # 1. Populate Baseline (May 2023)
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
    
    # ==================================================
    # Month 1: 2023-06 (First snapshot to process)
    # ==================================================
    print("\n--- Processing Month 1 (2023-06) ---")
    ref_june = "2023-06-01"
    
    # Prepare staging for 2023-06
    cur.execute("TRUNCATE TABLE test_incremental.staging_empresa;")
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        -- INSERT (does not exist in baseline)
        ('11112222', 'Empresa Inserida', 2062, 3, 20000.0, 3, 'ENTE NEW'),
        -- UPDATE (tracked column nature/capital changed)
        ('12345678', 'Empresa Teste A', 1015, 2, 7500.0, 1, 'ENTE A'),
        -- NO CHANGE (all tracked and ignored columns identical)
        ('87654321', 'Empresa Teste B', 2062, 3, 10000.0, 3, NULL);
    """)
    
    # Duplicate Test Case
    cur.execute("""
        -- Duplicate key with varying non-null values
        INSERT INTO test_incremental.staging_empresa VALUES
        ('99999999', 'Dup 1', 1015, NULL, 100.0, NULL, NULL),
        ('99999999', 'Dup 2', 1015, 2, 100.0, 1, 'ENTE X'); -- this one has more non-nulls and should be selected
    """)
    
    # Tie test case (both have same non-null count, alphabetical tie-breaker on razao_social)
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        ('88888888', 'Tie B', 1015, 2, 100.0, NULL, NULL),
        ('88888888', 'Tie A', 1015, 2, 100.0, NULL, NULL); -- alphabetically smaller/larger depending on ordering rule (DESC razao_social -> Tie B should win)
    """)
    
    # Deduplicate
    run_deduplicate_empresa(cur)
    
    # Run comparison
    run_change_detection_empresa(cur, ref_june, collection_date)
    conn.commit()
    
    # ASSERTIONS for Month 1 (Empresa)
    cur.execute("SELECT cnpj_basico, tipo_alteracao, (conteudo_novo->>'capital_social')::float FROM test_incremental.snapshots WHERE tabela='empresa' ORDER BY cnpj_basico;")
    snaps_june = cur.fetchall()
    print("June snapshots logged:", snaps_june)
    
    assert len(snaps_june) == 4, f"Expected 4 snapshots, got {len(snaps_june)}"
    assert snaps_june[0] == ('11112222', 'INSERT', 20000.0), "Expected INSERT for 11112222"
    assert snaps_june[1] == ('12345678', 'UPDATE', 7500.0), "Expected UPDATE for 12345678"
    assert snaps_june[2] == ('88888888', 'INSERT', 100.0), "Expected INSERT for tie-breaker 88888888"
    assert snaps_june[3] == ('99999999', 'INSERT', 100.0), "Expected INSERT for duplicate resolution 99999999"
    
    # Check duplicate selected columns
    cur.execute("SELECT razao_social, porte_empresa FROM test_incremental.latest_state_empresa WHERE cnpj_basico='99999999';")
    dup_state = cur.fetchone()
    assert dup_state == ('Dup 2', 1), f"Expected 'Dup 2' and 1, got {dup_state}"
    
    # Check tie selected columns (Tie B was selected since ordering is DESC)
    cur.execute("SELECT razao_social FROM test_incremental.latest_state_empresa WHERE cnpj_basico='88888888';")
    tie_state = cur.fetchone()
    assert tie_state == ('Tie B',), f"Expected 'Tie B', got {tie_state}"
    
    # Check DELETES for June: Company B (87654321) was in staging so not deleted. But what about others?
    # Actually, 87654321 was present (no change), so no snapshot. But wait, did we have any deletions?
    # No, all active records in baseline were either present or updated. So 0 deletes expected.
    cur.execute("SELECT COUNT(*) FROM test_incremental.snapshots WHERE tabela='empresa' AND tipo_alteracao='DELETE';")
    assert cur.fetchone()[0] == 0, "Expected 0 deletes"
    
    # ==================================================
    # Month 2: 2023-07
    # ==================================================
    print("\n--- Processing Month 2 (2023-07) ---")
    ref_july = "2023-07-01"
    
    cur.execute("TRUNCATE TABLE test_incremental.staging_empresa;")
    cur.execute("""
        INSERT INTO test_incremental.staging_empresa VALUES
        -- Previously Changed Entity (12345678 updated in June): changes again in July
        ('12345678', 'Empresa Teste A', 1015, 2, 9000.0, 1, 'ENTE A'),
        
        -- Never Changed Entity (87654321): changes now in July
        ('87654321', 'Empresa Teste B', 2062, 3, 15000.0, 3, NULL),
        
        -- Ignored column change only (11112222 updated in June): only razao_social changes
        ('11112222', 'Empresa Inserida Alterada', 2062, 3, 20000.0, 3, 'ENTE NEW'),
        
        -- Mixed change (99999999 updated in June): ignored column AND capital_social change
        ('99999999', 'Dup 2 New Name', 1015, 2, 200.0, 1, 'ENTE X'),
        
        -- NULL -> VALUE transition (87654321 had NULL ente_federativo_responsavel)
        -- handled by change above as well
        
        -- VALUE -> NULL transition (12345678: qualificacao_responsavel from 2 -> NULL)
        ('88888888', 'Tie B', 1015, NULL, 100.0, NULL, NULL) -- 2 -> NULL
    """)
    
    # Delete case: '11112222' and '99999999' and '88888888' and '12345678' and '87654321' are present.
    # But wait, what about deletes? We didn't load '88888888' in staging? No, we loaded it.
    # If we omit '11112222' from staging, it should register as DELETE!
    # Let's remove '11112222' to test DELETE.
    cur.execute("DELETE FROM test_incremental.staging_empresa WHERE cnpj_basico='11112222';")
    
    run_deduplicate_empresa(cur)
    run_change_detection_empresa(cur, ref_july, collection_date)
    conn.commit()
    
    # ASSERTIONS for Month 2 (Empresa)
    cur.execute("SELECT cnpj_basico, tipo_alteracao, (conteudo_novo->>'capital_social')::float, (conteudo_novo->>'qualificacao_responsavel') FROM test_incremental.snapshots WHERE tabela='empresa' AND mes_referencia='2023-07-01' ORDER BY cnpj_basico;")
    snaps_july = cur.fetchall()
    print("July snapshots logged:", snaps_july)
    
    # Expected July:
    # - 12345678: UPDATE (capital 9000)
    # - 87654321: UPDATE (capital 15000) - Never changed entity changes
    # - 88888888: UPDATE (qualificacao from 2 -> NULL) - VALUE -> NULL transition
    # - 99999999: UPDATE (capital 200) - Mixed change
    # - 11112222: DELETE (missing from staging)
    
    assert len(snaps_july) == 5, f"Expected 5 snapshots, got {len(snaps_july)}"
    
    # Find matching rows
    delete_snap = [x for x in snaps_july if x[0] == '11112222'][0]
    assert delete_snap[1] == 'DELETE', "Expected DELETE for 11112222"
    
    update_12 = [x for x in snaps_july if x[0] == '12345678'][0]
    assert update_12[1] == 'UPDATE' and update_12[2] == 9000.0, "Expected UPDATE for 12345678"
    
    update_87 = [x for x in snaps_july if x[0] == '87654321'][0]
    assert update_87[1] == 'UPDATE' and update_87[2] == 15000.0, "Expected UPDATE for 87654321"
    
    update_88 = [x for x in snaps_july if x[0] == '88888888'][0]
    assert update_88[1] == 'UPDATE' and update_88[3] is None, "Expected UPDATE for 88888888 due to NULL transition"
    
    update_99 = [x for x in snaps_july if x[0] == '99999999'][0]
    assert update_99[1] == 'UPDATE' and update_99[2] == 200.0, "Expected UPDATE for 99999999 due to tracked change"

    # Verify latest_state is_deleted state
    cur.execute("SELECT is_deleted FROM test_incremental.latest_state_empresa WHERE cnpj_basico='11112222';")
    assert cur.fetchone()[0] == True, "Expected is_deleted to be True for 11112222"
    
    # ==================================================
    # Test case: Ignored columns for estabelecimento
    # ==================================================
    print("\n--- Testing Ignored Columns for Estabelecimento ---")
    
    # 2023-06 Load (UPDATE with tracked column and NO CHANGE with ignored columns)
    cur.execute("TRUNCATE TABLE test_incremental.staging_estabelecimento;")
    cur.execute("""
        INSERT INTO test_incremental.staging_estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia, situacao_cadastral, uf, municipio, ddd_1, telefone_1) VALUES
        -- UPDATE: situacao_cadastral changed from 2 -> 8 (tracked)
        ('12345678', '0001', '99', 1, 'Fantasia A', 8, 'SP', 3550308, NULL, NULL),
        -- NO CHANGE: only nome_fantasia (ignored) and telephone (ignored) changed
        ('87654321', '0002', '88', 2, 'Fantasia B Modified', 2, 'RJ', 3304557, '11', '999999999');
    """)
    
    run_change_detection_estabelecimento(cur, ref_june, collection_date)
    conn.commit()
    
    cur.execute("SELECT cnpj_basico, tipo_alteracao FROM test_incremental.snapshots WHERE tabela='estabelecimento' AND mes_referencia='2023-06-01' ORDER BY cnpj_basico;")
    est_snaps = cur.fetchall()
    print("June Estabelecimento snapshots:", est_snaps)
    
    assert len(est_snaps) == 1, f"Expected 1 snapshot, got {len(est_snaps)}"
    assert est_snaps[0] == ('12345678', 'UPDATE'), "Expected UPDATE for 12345678"
    
    # Verify 87654321 is not in latest_state (it only had ignored changes, so remains never changed)
    cur.execute("SELECT EXISTS(SELECT 1 FROM test_incremental.latest_state_estabelecimento WHERE cnpj_basico='87654321');")
    assert not cur.fetchone()[0], "87654321 should not be in latest_state"

    print("\nAll 18 test assertions passed successfully!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_tests()
