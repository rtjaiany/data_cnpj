-- ==============================================================================
-- SCHEMA: ANALYTICS
-- Analytical layer for temporal reconstruction and longitudinal data analysis
-- Copyright (c) 2026 rtjaiany
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ==============================================================================
-- 1. UTILITY FUNCTIONS & SCHEMAS
-- ==============================================================================

-- Safe Date Parser: Handles empty, 0, or malformed numeric date formats
CREATE OR REPLACE FUNCTION analytics.safe_parse_date(date_int integer)
RETURNS date AS $$
BEGIN
    IF date_int IS NULL OR date_int = 0 OR date_int < 10000101 THEN
        RETURN NULL;
    END IF;
    RETURN to_date(date_int::text, 'YYYYMMDD');
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Privacy-Preserving Hashing Function: SHA-256 for PII (names, CPFs, phones)
CREATE OR REPLACE FUNCTION analytics.hash_sha256(val text)
RETURNS text AS $$
BEGIN
    IF val IS NULL OR val = '' OR val = 'NAN' THEN
        RETURN NULL;
    END IF;
    RETURN encode(sha256(convert_to(val, 'UTF8')), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ==============================================================================
-- 2. SCHEMA DICTIONARY (Step 9)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS analytics.schema_dictionary (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(50) NOT NULL,
    original_variable_name VARCHAR(50) NOT NULL,
    translated_variable_name VARCHAR(50) NOT NULL,
    data_type VARCHAR(30) NOT NULL,
    description TEXT NOT NULL
);

TRUNCATE TABLE analytics.schema_dictionary;
INSERT INTO analytics.schema_dictionary (source_table, original_variable_name, translated_variable_name, data_type, description) VALUES
('estabelecimento', 'cnpj_basico', 'cnpj_basic', 'VARCHAR(8)', 'Primeiros 8 dígitos do CNPJ (identificador da empresa)'),
('estabelecimento', 'cnpj_ordem', 'cnpj_order', 'VARCHAR(4)', '4 dígitos de ordem do CNPJ (identificador do estabelecimento)'),
('estabelecimento', 'cnpj_dv', 'cnpj_dv', 'VARCHAR(2)', '2 dígitos verificadores do CNPJ'),
('estabelecimento', 'data_inicio_atividade', 'opening_date', 'DATE', 'Data de início da atividade do estabelecimento'),
('estabelecimento', 'situacao_cadastral', 'registration_status', 'INTEGER', 'Código da situação cadastral (ex: 2 = Ativa, 8 = Baixada)'),
('estabelecimento', 'data_situacao_cadastral', 'registration_status_date', 'DATE', 'Data de alteração da situação cadastral'),
('estabelecimento', 'motivo_situacao_cadastral', 'registration_status_reason', 'INTEGER', 'Código do motivo da situação cadastral'),
('estabelecimento', 'identificador_matriz_filial', 'headquarters_branch_indicator', 'INTEGER', 'Indicador de Matriz (1) ou Filial (2)'),
('estabelecimento', 'cnae_fiscal_principal', 'primary_cnae', 'INTEGER', 'Código CNAE principal da atividade econômica'),
('estabelecimento', 'municipio', 'municipality_code', 'INTEGER', 'Código IBGE do município associado ao estabelecimento'),
('estabelecimento', 'uf', 'state_code', 'VARCHAR(2)', 'Sigla da Unidade Federativa (UF)'),
('empresa', 'capital_social', 'registered_capital', 'DOUBLE PRECISION', 'Capital social declarado da empresa'),
('empresa', 'natureza_juridica', 'legal_nature', 'INTEGER', 'Código de natureza jurídica da empresa'),
('empresa', 'porte_empresa', 'company_size', 'INTEGER', 'Código de porte da empresa (micro, pequeno, etc)'),
('empresa', 'qualificacao_responsavel', 'responsible_qualification', 'INTEGER', 'Código de qualificação do responsável pela empresa'),
('simples', 'opcao_pelo_simples', 'simples_enrollment_status', 'VARCHAR(1)', 'Status de opção pelo Simples Nacional (S/N)'),
('simples', 'data_opcao_simples', 'simples_entry_date', 'DATE', 'Data de início da opção pelo Simples Nacional'),
('simples', 'data_exclusao_simples', 'simples_exclusion_date', 'DATE', 'Data de exclusão do Simples Nacional'),
('simples', 'opcao_mei', 'mei_enrollment_status', 'VARCHAR(1)', 'Status de opção pelo MEI (S/N)'),
('simples', 'data_opcao_mei', 'mei_entry_date', 'DATE', 'Data de início da opção pelo MEI'),
('simples', 'data_exclusao_mei', 'mei_exclusion_date', 'DATE', 'Data de exclusão do MEI'),
('socios', 'identificador_socio', 'partner_type_indicator', 'INTEGER', 'Tipo de sócio (1 = Pessoa Jurídica, 2 = Pessoa Física)'),
('socios', 'nome_socio_razao_social', 'hashed_partner_name', 'VARCHAR(64)', 'Hash SHA-256 do nome do sócio ou razão social'),
('socios', 'cpf_cnpj_socio', 'hashed_partner_identifier', 'VARCHAR(64)', 'Hash SHA-256 do CPF ou CNPJ do sócio'),
('socios', 'qualificacao_socio', 'partner_qualification', 'INTEGER', 'Código de qualificação do sócio'),
('socios', 'data_entrada_sociedade', 'entry_date', 'DATE', 'Data de entrada na sociedade'),
('socios', 'pais', 'country_code', 'INTEGER', 'Código de país de origem do sócio'),
('socios', 'representante_legal', 'hashed_legal_representative', 'VARCHAR(64)', 'Hash SHA-256 do CPF do representante legal'),
('socios', 'nome_do_representante', 'hashed_representative_name', 'VARCHAR(64)', 'Hash SHA-256 do nome do representante legal'),
('socios', 'qualificacao_representante_legal', 'representative_qualification', 'INTEGER', 'Código de qualificação do representante legal'),
('socios', 'faixa_etaria', 'age_range', 'INTEGER', 'Código de faixa etária do sócio');

-- ==============================================================================
-- 3. ANALYTICAL TARGET TABLES (Step 5)
-- ==============================================================================

-- Reconstructed Establishments
CREATE TABLE IF NOT EXISTS analytics.reconstructed_establishments (
    reference_month VARCHAR(7) NOT NULL,
    cnpj_basic VARCHAR(8) NOT NULL,
    cnpj_order VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    opening_date DATE,
    registration_status INTEGER,
    registration_status_date DATE,
    registration_status_reason INTEGER,
    headquarters_branch_indicator INTEGER,
    primary_cnae INTEGER,
    municipality_code INTEGER,
    state_code VARCHAR(2),
    PRIMARY KEY (reference_month, cnpj_basic, cnpj_order, cnpj_dv)
);

-- Reconstructed Companies
CREATE TABLE IF NOT EXISTS analytics.reconstructed_companies (
    reference_month VARCHAR(7) NOT NULL,
    cnpj_basic VARCHAR(8) NOT NULL,
    registered_capital DOUBLE PRECISION,
    legal_nature INTEGER,
    company_size INTEGER,
    responsible_qualification INTEGER,
    PRIMARY KEY (reference_month, cnpj_basic)
);

-- Reconstructed Simples & MEI
CREATE TABLE IF NOT EXISTS analytics.reconstructed_simples (
    reference_month VARCHAR(7) NOT NULL,
    cnpj_basic VARCHAR(8) NOT NULL,
    simples_enrollment_status VARCHAR(1),
    simples_entry_date DATE,
    simples_exclusion_date DATE,
    mei_enrollment_status VARCHAR(1),
    mei_entry_date DATE,
    mei_exclusion_date DATE,
    PRIMARY KEY (reference_month, cnpj_basic)
);

DROP TABLE IF EXISTS analytics.reconstructed_partners CASCADE;
-- Reconstructed Partners (Anonymized PII)
CREATE TABLE analytics.reconstructed_partners (
    id BIGSERIAL PRIMARY KEY,
    reference_month VARCHAR(7) NOT NULL,
    cnpj_basic VARCHAR(8) NOT NULL,
    partner_type_indicator INTEGER,
    hashed_partner_name VARCHAR(64),
    hashed_partner_identifier VARCHAR(64),
    partner_qualification INTEGER,
    entry_date DATE,
    country_code INTEGER,
    hashed_legal_representative VARCHAR(64),
    hashed_representative_name VARCHAR(64),
    representative_qualification INTEGER,
    age_range INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rec_part_ref_cnpj ON analytics.reconstructed_partners (reference_month, cnpj_basic);

-- Reconstructed Partner Summaries (Step 8)
CREATE TABLE IF NOT EXISTS analytics.reconstructed_partner_summaries (
    reference_month VARCHAR(7) NOT NULL,
    cnpj_basic VARCHAR(8) NOT NULL,
    partner_count INTEGER,
    partner_additions INTEGER,
    partner_removals INTEGER,
    average_age DOUBLE PRECISION,
    minimum_age INTEGER,
    maximum_age INTEGER,
    corporate_partner_count INTEGER,
    individual_partner_count INTEGER,
    qualification_changes INTEGER,
    PRIMARY KEY (reference_month, cnpj_basic)
);

-- Reconstructed Longitudinal Intervals (Step 6)
CREATE TABLE IF NOT EXISTS analytics.longitudinal_establishment_intervals (
    cnpj_basic VARCHAR(8) NOT NULL,
    cnpj_order VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    valid_from_month VARCHAR(7) NOT NULL,
    valid_to_month VARCHAR(7) NOT NULL,
    opening_date DATE,
    registration_status INTEGER,
    registration_status_date DATE,
    registration_status_reason INTEGER,
    headquarters_branch_indicator INTEGER,
    primary_cnae INTEGER,
    municipality_code INTEGER,
    state_code VARCHAR(2),
    PRIMARY KEY (cnpj_basic, cnpj_order, cnpj_dv, valid_from_month)
);

-- Reconstructed Transitions (Step 7)
CREATE TABLE IF NOT EXISTS analytics.establishment_transitions (
    cnpj_basic VARCHAR(8) NOT NULL,
    cnpj_order VARCHAR(4) NOT NULL,
    cnpj_dv VARCHAR(2) NOT NULL,
    company_id VARCHAR(8) NOT NULL,
    reference_month VARCHAR(7) NOT NULL,
    variable_name VARCHAR(50) NOT NULL,
    previous_value TEXT,
    current_value TEXT,
    change_type VARCHAR(10) NOT NULL,
    PRIMARY KEY (cnpj_basic, cnpj_order, cnpj_dv, reference_month, variable_name)
);

-- Indexes for performance on analytics tables
CREATE INDEX IF NOT EXISTS idx_rec_est_cnae ON analytics.reconstructed_establishments(primary_cnae);
CREATE INDEX IF NOT EXISTS idx_rec_est_munic ON analytics.reconstructed_establishments(municipality_code);
CREATE INDEX IF NOT EXISTS idx_rec_est_status ON analytics.reconstructed_establishments(registration_status);

-- ==============================================================================
-- 4. PRECALCULATING MONTHLY PARTNER EVENTS (Step 8 Additions/Removals)
-- ==============================================================================
-- DROP MATERIALIZED VIEW IF EXISTS analytics.changed_company_keys CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.changed_company_keys AS
SELECT DISTINCT cnpj_basico 
FROM public.snapshots
WHERE mes_referencia BETWEEN '2023-05' AND '2023-12';
CREATE UNIQUE INDEX IF NOT EXISTS idx_changed_company_keys_cnpj ON analytics.changed_company_keys (cnpj_basico);

-- DROP MATERIALIZED VIEW IF EXISTS analytics.changed_establishment_keys_sp CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.changed_establishment_keys_sp AS
SELECT DISTINCT 
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv
FROM public.snapshots
WHERE tabela = 'estabelecimento'
  AND mes_referencia BETWEEN '2023-05' AND '2023-12'
  AND (conteudo_anterior->>'uf' = 'SP' OR conteudo_novo->>'uf' = 'SP');
CREATE UNIQUE INDEX IF NOT EXISTS idx_changed_est_sp_cnpj ON analytics.changed_establishment_keys_sp (cnpj_basico, cnpj_ordem, cnpj_dv);

-- DROP MATERIALIZED VIEW IF EXISTS analytics.partner_monthly_events CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.partner_monthly_events AS
SELECT 
    mes_referencia,
    cnpj_basico,
    COUNT(CASE WHEN tipo_alteracao = 'INSERT' THEN 1 END) AS partner_additions,
    COUNT(CASE WHEN tipo_alteracao = 'DELETE' THEN 1 END) AS partner_removals,
    COUNT(CASE WHEN tipo_alteracao = 'UPDATE' AND (conteudo_anterior->>'qualificacao_socio' IS DISTINCT FROM conteudo_novo->>'qualificacao_socio') THEN 1 END) AS qualification_changes
FROM public.snapshots
WHERE tabela = 'socios'
GROUP BY mes_referencia, cnpj_basico;
CREATE UNIQUE INDEX IF NOT EXISTS idx_partner_monthly_events_ref_cnpj ON analytics.partner_monthly_events (mes_referencia, cnpj_basico);

-- ==============================================================================
-- 5. TEMPORAL RECONSTRUCTION PROCEDURE (Steps 1, 2, 3, 4, 5)
-- ==============================================================================

CREATE OR REPLACE PROCEDURE analytics.reconstruct_temporal_data() AS $$
DECLARE
    months text[] := ARRAY['2023-12', '2023-11', '2023-10', '2023-09', '2023-08', '2023-07', '2023-06', '2023-05'];
    curr_month text;
BEGIN
    RAISE INFO 'Starting temporal reconstruction pipeline...';

    -- Clear target tables
    TRUNCATE TABLE analytics.reconstructed_establishments;
    TRUNCATE TABLE analytics.reconstructed_companies;
    TRUNCATE TABLE analytics.reconstructed_simples;
    TRUNCATE TABLE analytics.reconstructed_partners;
    TRUNCATE TABLE analytics.reconstructed_partner_summaries;

    -- STEP 1: Identify "Changed Entities" in SP to keep staging tables small and performant.
    -- Staging only keys that have events in the snapshots logs.
    CREATE TEMP TABLE temp_changed_company_keys AS
    SELECT DISTINCT cnpj_basico FROM analytics.changed_establishment_keys_sp;
    CREATE UNIQUE INDEX ON temp_changed_company_keys (cnpj_basico);
    ANALYZE temp_changed_company_keys;

    CREATE TEMP TABLE temp_changed_establishment_keys AS
    SELECT cnpj_basico, cnpj_ordem AS cnpj_ordem, cnpj_dv FROM analytics.changed_establishment_keys_sp;
    CREATE UNIQUE INDEX ON temp_changed_establishment_keys (cnpj_basico, cnpj_ordem, cnpj_dv);
    CREATE INDEX ON temp_changed_establishment_keys (cnpj_basico);
    ANALYZE temp_changed_establishment_keys;

    RAISE INFO 'Staged keys: % changed companies, % changed establishments.', 
        (SELECT COUNT(*) FROM temp_changed_company_keys), 
        (SELECT COUNT(*) FROM temp_changed_establishment_keys);

    -- Create and populate temporary current-state tables with the Production State (June 2026 baseline)
    CREATE TEMP TABLE temp_changed_establishment (LIKE public.estabelecimento);
    CREATE UNIQUE INDEX ON temp_changed_establishment (cnpj_basico, cnpj_ordem, cnpj_dv);
    INSERT INTO temp_changed_establishment
    SELECT * FROM public.estabelecimento 
    WHERE (cnpj_basico, cnpj_ordem, cnpj_dv) IN (SELECT cnpj_basico, cnpj_ordem, cnpj_dv FROM temp_changed_establishment_keys)
    ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO NOTHING;
    ANALYZE temp_changed_establishment;

    CREATE TEMP TABLE temp_changed_company (LIKE public.empresa);
    CREATE UNIQUE INDEX ON temp_changed_company (cnpj_basico);
    INSERT INTO temp_changed_company
    SELECT * FROM public.empresa 
    WHERE cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
    ON CONFLICT (cnpj_basico) DO NOTHING;
    ANALYZE temp_changed_company;

    CREATE TEMP TABLE temp_changed_simples (LIKE public.simples);
    CREATE UNIQUE INDEX ON temp_changed_simples (cnpj_basico);
    INSERT INTO temp_changed_simples
    SELECT * FROM public.simples 
    WHERE cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
    ON CONFLICT (cnpj_basico) DO NOTHING;
    ANALYZE temp_changed_simples;

    CREATE TEMP TABLE temp_changed_socios (LIKE public.socios);
    CREATE UNIQUE INDEX ON temp_changed_socios (cnpj_basico, nome_socio_razao_social);
    INSERT INTO temp_changed_socios
    SELECT * FROM public.socios 
    WHERE cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
    ON CONFLICT (cnpj_basico, nome_socio_razao_social) DO NOTHING;
    ANALYZE temp_changed_socios;

    -- ITERATIVE REVERSE CHRONOLOGICAL PROCESSING (Step 3)
    FOR i IN 1..array_length(months, 1) LOOP
        curr_month := months[i];
        RAISE INFO '------------------------------------------------';
        RAISE INFO 'Processing reverse events for month: %', curr_month;

        -- 1. APPLY REVERSE EVENTS FOR: ESTABELECIMENTO
        -- Earliest event in month represents the state before the month (M-1)
        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico, cnpj_ordem, cnpj_dv)
                cnpj_basico, cnpj_ordem, cnpj_dv,
                tipo_alteracao,
                conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'estabelecimento'
              AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv, id ASC
        )
        -- Delete newly inserted establishments (going backward, they didn't exist yet)
        DELETE FROM temp_changed_establishment t
        USING earliest_events e
        WHERE t.cnpj_basico = e.cnpj_basico AND t.cnpj_ordem = e.cnpj_ordem AND t.cnpj_dv = e.cnpj_dv
          AND e.tipo_alteracao = 'INSERT';

        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico, cnpj_ordem, cnpj_dv)
                cnpj_basico, cnpj_ordem, cnpj_dv,
                tipo_alteracao,
                conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'estabelecimento'
              AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv, id ASC
        ),
        to_restore AS (
            SELECT (jsonb_populate_record(NULL::public.estabelecimento, conteudo_anterior)).*
            FROM earliest_events
            WHERE tipo_alteracao IN ('UPDATE', 'DELETE')
        )
        -- Restore updated/deleted establishments to their previous state
        INSERT INTO temp_changed_establishment
        SELECT * FROM to_restore
        ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE
        SET
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
            data_situacao_especial = EXCLUDED.data_situacao_especial;

        -- 2. APPLY REVERSE EVENTS FOR: EMPRESA
        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico)
                cnpj_basico, tipo_alteracao, conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'empresa' AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, id ASC
        )
        DELETE FROM temp_changed_company t
        USING earliest_events e
        WHERE t.cnpj_basico = e.cnpj_basico AND e.tipo_alteracao = 'INSERT';

        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico)
                cnpj_basico, tipo_alteracao, conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'empresa' AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, id ASC
        ),
        to_restore AS (
            SELECT (jsonb_populate_record(NULL::public.empresa, conteudo_anterior)).*
            FROM earliest_events
            WHERE tipo_alteracao IN ('UPDATE', 'DELETE')
        )
        INSERT INTO temp_changed_company
        SELECT * FROM to_restore
        ON CONFLICT (cnpj_basico) DO UPDATE
        SET
            razao_social = EXCLUDED.razao_social,
            natureza_juridica = EXCLUDED.natureza_juridica,
            qualificacao_responsavel = EXCLUDED.qualificacao_responsavel,
            capital_social = EXCLUDED.capital_social,
            porte_empresa = EXCLUDED.porte_empresa,
            ente_federativo_responsavel = EXCLUDED.ente_federativo_responsavel;

        -- 3. APPLY REVERSE EVENTS FOR: SIMPLES
        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico)
                cnpj_basico, tipo_alteracao, conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'simples' AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, id ASC
        )
        DELETE FROM temp_changed_simples t
        USING earliest_events e
        WHERE t.cnpj_basico = e.cnpj_basico AND e.tipo_alteracao = 'INSERT';

        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico)
                cnpj_basico, tipo_alteracao, conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'simples' AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, id ASC
        ),
        to_restore AS (
            SELECT (jsonb_populate_record(NULL::public.simples, conteudo_anterior)).*
            FROM earliest_events
            WHERE tipo_alteracao IN ('UPDATE', 'DELETE')
        )
        INSERT INTO temp_changed_simples
        SELECT * FROM to_restore
        ON CONFLICT (cnpj_basico) DO UPDATE
        SET
            opcao_pelo_simples = EXCLUDED.opcao_pelo_simples,
            data_opcao_simples = EXCLUDED.data_opcao_simples,
            data_exclusao_simples = EXCLUDED.data_exclusao_simples,
            opcao_mei = EXCLUDED.opcao_mei,
            data_opcao_mei = EXCLUDED.data_opcao_mei,
            data_exclusao_mei = EXCLUDED.data_exclusao_mei;

        -- 4. APPLY REVERSE EVENTS FOR: SOCIOS
        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico, (chave->>'nome_socio_razao_social'))
                cnpj_basico, (chave->>'nome_socio_razao_social') AS nome_socio_razao_social, tipo_alteracao
            FROM public.snapshots
            WHERE tabela = 'socios' AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, (chave->>'nome_socio_razao_social'), id ASC
        )
        DELETE FROM temp_changed_socios t
        USING earliest_events e
        WHERE t.cnpj_basico = e.cnpj_basico AND t.nome_socio_razao_social = e.nome_socio_razao_social
          AND e.tipo_alteracao = 'INSERT';

        WITH earliest_events AS (
            SELECT DISTINCT ON (cnpj_basico, (chave->>'nome_socio_razao_social'))
                cnpj_basico, tipo_alteracao, conteudo_anterior
            FROM public.snapshots
            WHERE tabela = 'socios' AND mes_referencia = curr_month
              AND cnpj_basico IN (SELECT cnpj_basico FROM temp_changed_company_keys)
            ORDER BY cnpj_basico, (chave->>'nome_socio_razao_social'), id ASC
        ),
        to_restore AS (
            SELECT (jsonb_populate_record(NULL::public.socios, conteudo_anterior)).*
            FROM earliest_events
            WHERE tipo_alteracao IN ('UPDATE', 'DELETE')
        )
        INSERT INTO temp_changed_socios
        SELECT * FROM to_restore
        ON CONFLICT (cnpj_basico, nome_socio_razao_social) DO UPDATE
        SET
            identificador_socio = EXCLUDED.identificador_socio,
            cpf_cnpj_socio = EXCLUDED.cpf_cnpj_socio,
            qualificacao_socio = EXCLUDED.qualificacao_socio,
            data_entrada_sociedade = EXCLUDED.data_entrada_sociedade,
            pais = EXCLUDED.pais,
            representante_legal = EXCLUDED.representante_legal,
            nome_do_representante = EXCLUDED.nome_do_representante,
            qualificacao_representante_legal = EXCLUDED.qualificacao_representante_legal,
            faixa_etaria = EXCLUDED.faixa_etaria;

        -- SAVE THE RECONSTRUCTED MONTH STATE IF WITHIN STUDY SCOPE (<= 2023-10)
        -- Reverting Dec-2023 (index 1) and Nov-2023 (index 2) yields the state for end of Oct-2023.
        IF i >= 2 THEN
            DECLARE
                target_month text;
            BEGIN
                -- Mapear a qual mês de referência histórico corresponde o estado atual:
                -- Se i=2 (reverteu Nov), salvamos como '2023-10'
                -- Se i=3 (reverteu Out), salvamos como '2023-09'
                -- Se i=7 (reverteu Jun), salvamos como '2023-05'
                CASE i
                    WHEN 2 THEN target_month := '2023-10';
                    WHEN 3 THEN target_month := '2023-09';
                    WHEN 4 THEN target_month := '2023-08';
                    WHEN 5 THEN target_month := '2023-07';
                    WHEN 6 THEN target_month := '2023-06';
                    WHEN 7 THEN target_month := '2023-05';
                    ELSE target_month := NULL;
                END CASE;

                IF target_month IS NOT NULL THEN
                    RAISE INFO 'Saving consolidated states for Reference Month: %', target_month;

                    -- A. Save Establishments (SP only, changed temp tables only)
                    INSERT INTO analytics.reconstructed_establishments (
                        reference_month, cnpj_basic, cnpj_order, cnpj_dv, opening_date,
                        registration_status, registration_status_date, registration_status_reason,
                        headquarters_branch_indicator, primary_cnae, municipality_code, state_code
                    )
                    SELECT 
                        target_month, t.cnpj_basico, t.cnpj_ordem, t.cnpj_dv, 
                        analytics.safe_parse_date(t.data_inicio_atividade), 
                        t.situacao_cadastral, 
                        analytics.safe_parse_date(t.data_situacao_cadastral), 
                        t.motivo_situacao_cadastral,
                        t.identificador_matriz_filial, t.cnae_fiscal_principal, 
                        mu.cd_mun, t.uf
                    FROM temp_changed_establishment t
                    LEFT JOIN public.munic mu ON t.municipio = mu.codigo
                    WHERE t.uf = 'SP'
                      AND (t.data_inicio_atividade IS NULL OR analytics.safe_parse_date(t.data_inicio_atividade) < (target_month || '-01')::DATE + INTERVAL '1 month');

                    -- B. Save Companies (changed temp tables only)
                    INSERT INTO analytics.reconstructed_companies (
                        reference_month, cnpj_basic, registered_capital, legal_nature,
                        company_size, responsible_qualification
                    )
                    SELECT 
                        target_month, t.cnpj_basico, t.capital_social, t.natureza_juridica, 
                        t.porte_empresa, t.qualificacao_responsavel
                    FROM temp_changed_company t
                    WHERE EXISTS (
                        SELECT 1 FROM analytics.reconstructed_establishments e 
                        WHERE e.reference_month = target_month AND e.cnpj_basic = t.cnpj_basico
                    );

                    -- C. Save Simples status (changed temp tables only)
                    INSERT INTO analytics.reconstructed_simples (
                        reference_month, cnpj_basic, simples_enrollment_status, simples_entry_date,
                        simples_exclusion_date, mei_enrollment_status, mei_entry_date, mei_exclusion_date
                    )
                    SELECT 
                        target_month, t.cnpj_basico, t.opcao_pelo_simples, 
                        analytics.safe_parse_date(t.data_opcao_simples), 
                        analytics.safe_parse_date(t.data_exclusao_simples),
                        t.opcao_mei, 
                        analytics.safe_parse_date(t.data_opcao_mei), 
                        analytics.safe_parse_date(t.data_exclusao_mei)
                    FROM temp_changed_simples t
                    WHERE EXISTS (
                        SELECT 1 FROM analytics.reconstructed_establishments e 
                        WHERE e.reference_month = target_month AND e.cnpj_basic = t.cnpj_basico
                    );

                    -- D. Save Individual Partners (changed temp tables only)
                    INSERT INTO analytics.reconstructed_partners (
                        reference_month, cnpj_basic, partner_type_indicator,
                        hashed_partner_name, hashed_partner_identifier,
                        partner_qualification, entry_date, country_code,
                        hashed_legal_representative, hashed_representative_name,
                        representative_qualification, age_range
                    )
                    SELECT 
                        target_month, t.cnpj_basico, t.identificador_socio,
                        analytics.hash_sha256(t.nome_socio_razao_social),
                        analytics.hash_sha256(t.cpf_cnpj_socio),
                        t.qualificacao_socio,
                        analytics.safe_parse_date(t.data_entrada_sociedade),
                        t.pais,
                        analytics.hash_sha256(t.representante_legal),
                        analytics.hash_sha256(t.nome_do_representante),
                        t.qualificacao_representante_legal,
                        t.faixa_etaria
                    FROM temp_changed_socios t
                    WHERE EXISTS (
                        SELECT 1 FROM analytics.reconstructed_establishments e 
                        WHERE e.reference_month = target_month AND e.cnpj_basic = t.cnpj_basico
                    )
                    AND (t.data_entrada_sociedade IS NULL OR analytics.safe_parse_date(t.data_entrada_sociedade) < (target_month || '-01')::DATE + INTERVAL '1 month');

                    -- E. Save Partner Summaries (changed temp tables only)
                    INSERT INTO analytics.reconstructed_partner_summaries (
                        reference_month, cnpj_basic, partner_count, partner_additions, partner_removals,
                        average_age, minimum_age, maximum_age, corporate_partner_count, individual_partner_count,
                        qualification_changes
                    )
                    WITH mapped_ages AS (
                        SELECT 
                            cnpj_basic,
                            partner_type_indicator,
                            CASE age_range 
                                WHEN 1 THEN 6 WHEN 2 THEN 16 WHEN 3 THEN 25 WHEN 4 THEN 35 
                                WHEN 5 THEN 45 WHEN 6 THEN 55 WHEN 7 THEN 65 WHEN 8 THEN 75 
                                WHEN 9 THEN 85 ELSE NULL 
                            END AS age
                        FROM analytics.reconstructed_partners
                        WHERE reference_month = target_month
                          AND cnpj_basic IN (SELECT cnpj_basico FROM temp_changed_company_keys)
                    ),
                    agg AS (
                        SELECT 
                            cnpj_basic,
                            COUNT(*) AS partner_count,
                            ROUND(AVG(age)::numeric, 2) AS average_age,
                            MIN(age) AS minimum_age,
                            MAX(age) AS maximum_age,
                            COUNT(CASE WHEN partner_type_indicator = 1 THEN 1 END) AS corporate_partner_count,
                            COUNT(CASE WHEN partner_type_indicator = 2 THEN 1 END) AS individual_partner_count
                        FROM mapped_ages
                        GROUP BY cnpj_basic
                    )
                    SELECT 
                        target_month,
                        a.cnpj_basic,
                        a.partner_count,
                        COALESCE(e.partner_additions, 0) AS partner_additions,
                        COALESCE(e.partner_removals, 0) AS partner_removals,
                        a.average_age,
                        a.minimum_age,
                        a.maximum_age,
                        a.corporate_partner_count,
                        a.individual_partner_count,
                        COALESCE(e.qualification_changes, 0) AS qualification_changes
                    FROM agg a
                    LEFT JOIN analytics.partner_monthly_events e 
                      ON e.mes_referencia = target_month AND e.cnpj_basico = a.cnpj_basic;
                END IF;
            END;
        END IF;

    END LOOP;

    -- ==============================================================================
    -- STEP 4: BULK INSERT UNCHANGED (STATIC) ENTITIES
    -- ==============================================================================
    RAISE INFO 'Identifying static unchanged companies with SP establishments...';
    CREATE TEMP TABLE temp_unchanged_sp_company_keys AS
    SELECT DISTINCT e.cnpj_basico FROM public.estabelecimento e
    WHERE e.uf = 'SP'
      AND NOT EXISTS (
          SELECT 1 FROM temp_changed_establishment_keys k 
          WHERE k.cnpj_basico = e.cnpj_basico 
            AND k.cnpj_ordem = e.cnpj_ordem 
            AND k.cnpj_dv = e.cnpj_dv
      );
    CREATE UNIQUE INDEX ON temp_unchanged_sp_company_keys (cnpj_basico);
    ANALYZE temp_unchanged_sp_company_keys;

    RAISE INFO 'Bulk inserting static establishments for SP...';
    INSERT INTO analytics.reconstructed_establishments (
        reference_month, cnpj_basic, cnpj_order, cnpj_dv, opening_date,
        registration_status, registration_status_date, registration_status_reason,
        headquarters_branch_indicator, primary_cnae, municipality_code, state_code
    )
    SELECT 
        m.month, e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv, 
        analytics.safe_parse_date(e.data_inicio_atividade), 
        e.situacao_cadastral, 
        analytics.safe_parse_date(e.data_situacao_cadastral), 
        e.motivo_situacao_cadastral,
        e.identificador_matriz_filial, e.cnae_fiscal_principal, 
        mu.cd_mun, e.uf
    FROM public.estabelecimento e
    CROSS JOIN (SELECT unnest(ARRAY['2023-05','2023-06','2023-07','2023-08','2023-09','2023-10']) AS month) m
    LEFT JOIN public.munic mu ON e.municipio = mu.codigo
    WHERE e.uf = 'SP'
      AND NOT EXISTS (
          SELECT 1 FROM temp_changed_establishment_keys k 
          WHERE k.cnpj_basico = e.cnpj_basico 
            AND k.cnpj_ordem = e.cnpj_ordem 
            AND k.cnpj_dv = e.cnpj_dv
      )
      AND (e.data_inicio_atividade IS NULL OR analytics.safe_parse_date(e.data_inicio_atividade) < (m.month || '-01')::DATE + INTERVAL '1 month');

    RAISE INFO 'Bulk inserting static companies...';
    INSERT INTO analytics.reconstructed_companies (
        reference_month, cnpj_basic, registered_capital, legal_nature,
        company_size, responsible_qualification
    )
    SELECT 
        m.month, c.cnpj_basico, c.capital_social, c.natureza_juridica, 
        c.porte_empresa, c.qualificacao_responsavel
    FROM public.empresa c
    CROSS JOIN (SELECT unnest(ARRAY['2023-05','2023-06','2023-07','2023-08','2023-09','2023-10']) AS month) m
    WHERE EXISTS (
        SELECT 1 FROM analytics.reconstructed_establishments e
        WHERE e.reference_month = m.month AND e.cnpj_basic = c.cnpj_basico
    )
      AND NOT EXISTS (
          SELECT 1 FROM temp_changed_company_keys k WHERE k.cnpj_basico = c.cnpj_basico
      );

    RAISE INFO 'Bulk inserting static simples...';
    INSERT INTO analytics.reconstructed_simples (
        reference_month, cnpj_basic, simples_enrollment_status, simples_entry_date,
        simples_exclusion_date, mei_enrollment_status, mei_entry_date, mei_exclusion_date
    )
    SELECT 
        m.month, s.cnpj_basico, s.opcao_pelo_simples, 
        analytics.safe_parse_date(s.data_opcao_simples), 
        analytics.safe_parse_date(s.data_exclusao_simples),
        s.opcao_mei, 
        analytics.safe_parse_date(s.data_opcao_mei), 
        analytics.safe_parse_date(s.data_exclusao_mei)
    FROM public.simples s
    CROSS JOIN (SELECT unnest(ARRAY['2023-05','2023-06','2023-07','2023-08','2023-09','2023-10']) AS month) m
    WHERE EXISTS (
        SELECT 1 FROM analytics.reconstructed_establishments e
        WHERE e.reference_month = m.month AND e.cnpj_basic = s.cnpj_basico
    )
      AND NOT EXISTS (
          SELECT 1 FROM temp_changed_company_keys k WHERE k.cnpj_basico = s.cnpj_basico
      );

    RAISE INFO 'Bulk inserting static partners...';
    INSERT INTO analytics.reconstructed_partners (
        reference_month, cnpj_basic, partner_type_indicator,
        hashed_partner_name, hashed_partner_identifier,
        partner_qualification, entry_date, country_code,
        hashed_legal_representative, hashed_representative_name,
        representative_qualification, age_range
    )
    SELECT 
        m.month, s.cnpj_basico, s.identificador_socio,
        analytics.hash_sha256(s.nome_socio_razao_social),
        analytics.hash_sha256(s.cpf_cnpj_socio),
        s.qualificacao_socio,
        analytics.safe_parse_date(s.data_entrada_sociedade),
        s.pais,
        analytics.hash_sha256(s.representante_legal),
        analytics.hash_sha256(s.nome_do_representante),
        s.qualificacao_representante_legal,
        s.faixa_etaria
    FROM public.socios s
    CROSS JOIN (SELECT unnest(ARRAY['2023-05','2023-06','2023-07','2023-08','2023-09','2023-10']) AS month) m
    WHERE EXISTS (
        SELECT 1 FROM analytics.reconstructed_establishments e
        WHERE e.reference_month = m.month AND e.cnpj_basic = s.cnpj_basico
    )
      AND (s.data_entrada_sociedade IS NULL OR analytics.safe_parse_date(s.data_entrada_sociedade) < (m.month || '-01')::DATE + INTERVAL '1 month')
      AND NOT EXISTS (
          SELECT 1 FROM temp_changed_company_keys k WHERE k.cnpj_basico = s.cnpj_basico
      );

    RAISE INFO 'Bulk inserting static partner summaries...';
    INSERT INTO analytics.reconstructed_partner_summaries (
        reference_month, cnpj_basic, partner_count, partner_additions, partner_removals,
        average_age, minimum_age, maximum_age, corporate_partner_count, individual_partner_count,
        qualification_changes
    )
    WITH mapped_ages AS (
        SELECT 
            reference_month,
            cnpj_basic,
            partner_type_indicator,
            CASE age_range 
                WHEN 1 THEN 6 WHEN 2 THEN 16 WHEN 3 THEN 25 WHEN 4 THEN 35 
                WHEN 5 THEN 45 WHEN 6 THEN 55 WHEN 7 THEN 65 WHEN 8 THEN 75 
                WHEN 9 THEN 85 ELSE NULL 
            END AS age
        FROM analytics.reconstructed_partners
        WHERE cnpj_basic IN (SELECT cnpj_basico FROM temp_unchanged_sp_company_keys)
          AND NOT EXISTS (
              SELECT 1 FROM temp_changed_company_keys k WHERE k.cnpj_basico = cnpj_basic
          )
    ),
    agg AS (
        SELECT 
            reference_month,
            cnpj_basic,
            COUNT(*) AS partner_count,
            ROUND(AVG(age)::numeric, 2) AS average_age,
            MIN(age) AS minimum_age,
            MAX(age) AS maximum_age,
            COUNT(CASE WHEN partner_type_indicator = 1 THEN 1 END) AS corporate_partner_count,
            COUNT(CASE WHEN partner_type_indicator = 2 THEN 1 END) AS individual_partner_count
        FROM mapped_ages
        GROUP BY reference_month, cnpj_basic
    )
    SELECT 
        a.reference_month,
        a.cnpj_basic,
        a.partner_count,
        0 AS partner_additions,
        0 AS partner_removals,
        a.average_age,
        a.minimum_age,
        a.maximum_age,
        a.corporate_partner_count,
        a.individual_partner_count,
        0 AS qualification_changes
    FROM agg a;

    -- Clean up temporary key tables
    DROP TABLE temp_changed_company_keys;
    DROP TABLE temp_changed_establishment_keys;
    DROP TABLE temp_changed_establishment;
    DROP TABLE temp_changed_company;
    DROP TABLE temp_changed_simples;
    DROP TABLE temp_changed_socios;
    DROP TABLE temp_unchanged_sp_company_keys;

    RAISE INFO 'Reconstruction of monthly states completed successfully!';
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- 6. LONGITUDINAL COMPRESSION FUNCTION (Step 6)
-- ==============================================================================
CREATE OR REPLACE PROCEDURE analytics.compress_longitudinal_intervals() AS $$
BEGIN
    RAISE INFO 'Running gaps-and-islands longitudinal compression...';
    TRUNCATE TABLE analytics.longitudinal_establishment_intervals;

    INSERT INTO analytics.longitudinal_establishment_intervals (
        cnpj_basic, cnpj_order, cnpj_dv, valid_from_month, valid_to_month, opening_date,
        registration_status, registration_status_date, registration_status_reason,
        headquarters_branch_indicator, primary_cnae, municipality_code, state_code
    )
    WITH ordered_states AS (
        SELECT 
            e.*,
            LAG(primary_cnae) OVER (PARTITION BY cnpj_basic, cnpj_order, cnpj_dv ORDER BY reference_month) AS prev_cnae,
            LAG(registration_status) OVER (PARTITION BY cnpj_basic, cnpj_order, cnpj_dv ORDER BY reference_month) AS prev_status,
            LAG(registration_status_date) OVER (PARTITION BY cnpj_basic, cnpj_order, cnpj_dv ORDER BY reference_month) AS prev_status_date,
            LAG(municipality_code) OVER (PARTITION BY cnpj_basic, cnpj_order, cnpj_dv ORDER BY reference_month) AS prev_munic
        FROM analytics.reconstructed_establishments e
    ),
    state_changes AS (
        SELECT 
            *,
            CASE 
                WHEN prev_cnae IS DISTINCT FROM primary_cnae 
                  OR prev_status IS DISTINCT FROM registration_status
                  OR prev_status_date IS DISTINCT FROM registration_status_date
                  OR prev_munic IS DISTINCT FROM municipality_code
                THEN 1 
                ELSE 0 
            END AS is_change
        FROM ordered_states
    ),
    state_groups AS (
        SELECT 
            *,
            SUM(is_change) OVER (PARTITION BY cnpj_basic, cnpj_order, cnpj_dv ORDER BY reference_month) AS group_id
        FROM state_changes
    )
    SELECT 
        cnpj_basic, cnpj_order, cnpj_dv,
        MIN(reference_month) AS valid_from_month,
        MAX(reference_month) AS valid_to_month,
        opening_date,
        registration_status,
        registration_status_date,
        registration_status_reason,
        headquarters_branch_indicator,
        primary_cnae,
        municipality_code,
        state_code
    FROM state_groups
    GROUP BY 
        cnpj_basic, cnpj_order, cnpj_dv, group_id,
        opening_date, registration_status, registration_status_date, registration_status_reason,
        headquarters_branch_indicator, primary_cnae, municipality_code, state_code;

    RAISE INFO 'Longitudinal intervals successfully created!';
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- 7. MONTHLY TRANSITIONS LOGGER (Step 7)
-- ==============================================================================
CREATE OR REPLACE PROCEDURE analytics.populate_transitions() AS $$
DECLARE
    months text[] := ARRAY['2023-05', '2023-06', '2023-07', '2023-08', '2023-09', '2023-10'];
    prev_m text;
    curr_m text;
BEGIN
    RAISE INFO 'Generating monthly transitions...';
    TRUNCATE TABLE analytics.establishment_transitions;
    
    FOR i IN 2..array_length(months, 1) LOOP
        prev_m := months[i-1];
        curr_m := months[i];
        
        RAISE INFO 'Comparing % vs %', prev_m, curr_m;
        
        -- A. UPDATE transitions (attribute differences for establishments existing in both months)
        INSERT INTO analytics.establishment_transitions (
            cnpj_basic, cnpj_order, cnpj_dv, company_id, reference_month,
            variable_name, previous_value, current_value, change_type
        )
        SELECT 
            COALESCE(p.cnpj_basic, c.cnpj_basic),
            COALESCE(p.cnpj_order, c.cnpj_order),
            COALESCE(p.cnpj_dv, c.cnpj_dv),
            COALESCE(p.cnpj_basic, c.cnpj_basic),
            curr_m,
            v.var_name,
            v.prev_val,
            v.curr_val,
            'UPDATE'
        FROM (SELECT * FROM analytics.reconstructed_establishments WHERE reference_month = prev_m) p
        JOIN (SELECT * FROM analytics.reconstructed_establishments WHERE reference_month = curr_m) c
          ON p.cnpj_basic = c.cnpj_basic AND p.cnpj_order = c.cnpj_order AND p.cnpj_dv = c.cnpj_dv
        CROSS JOIN LATERAL (
            VALUES 
                ('registration_status', p.registration_status::text, c.registration_status::text),
                ('registration_status_date', p.registration_status_date::text, c.registration_status_date::text),
                ('primary_cnae', p.primary_cnae::text, c.primary_cnae::text),
                ('municipality_code', p.municipality_code::text, c.municipality_code::text)
        ) v(var_name, prev_val, curr_val)
        WHERE v.prev_val IS DISTINCT FROM v.curr_val;
        
        -- B. INSERT transitions (entities present in current month but not in previous month)
        INSERT INTO analytics.establishment_transitions (
            cnpj_basic, cnpj_order, cnpj_dv, company_id, reference_month,
            variable_name, previous_value, current_value, change_type
        )
        SELECT 
            c.cnpj_basic, c.cnpj_order, c.cnpj_dv, c.cnpj_basic, curr_m,
            v.var_name,
            NULL,
            v.curr_val,
            'INSERT'
        FROM (SELECT * FROM analytics.reconstructed_establishments WHERE reference_month = curr_m) c
        LEFT JOIN (SELECT * FROM analytics.reconstructed_establishments WHERE reference_month = prev_m) p
          ON p.cnpj_basic = c.cnpj_basic AND p.cnpj_order = c.cnpj_order AND p.cnpj_dv = c.cnpj_dv
        CROSS JOIN LATERAL (
            VALUES 
                ('registration_status', c.registration_status::text),
                ('registration_status_date', c.registration_status_date::text),
                ('primary_cnae', c.primary_cnae::text),
                ('municipality_code', c.municipality_code::text)
        ) v(var_name, curr_val)
        WHERE p.cnpj_basic IS NULL;
        
        -- C. DELETE transitions (entities present in previous month but not in current month)
        INSERT INTO analytics.establishment_transitions (
            cnpj_basic, cnpj_order, cnpj_dv, company_id, reference_month,
            variable_name, previous_value, current_value, change_type
        )
        SELECT 
            p.cnpj_basic, p.cnpj_order, p.cnpj_dv, p.cnpj_basic, curr_m,
            v.var_name,
            v.prev_val,
            NULL,
            'DELETE'
        FROM (SELECT * FROM analytics.reconstructed_establishments WHERE reference_month = prev_m) p
        LEFT JOIN (SELECT * FROM analytics.reconstructed_establishments WHERE reference_month = curr_m) c
          ON p.cnpj_basic = c.cnpj_basic AND p.cnpj_order = c.cnpj_order AND p.cnpj_dv = c.cnpj_dv
        CROSS JOIN LATERAL (
            VALUES 
                ('registration_status', p.registration_status::text),
                ('registration_status_date', p.registration_status_date::text),
                ('primary_cnae', p.primary_cnae::text),
                ('municipality_code', p.municipality_code::text)
        ) v(var_name, prev_val)
        WHERE c.cnpj_basic IS NULL;
        
    END LOOP;
    
    RAISE INFO 'Transitions population complete!';
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- 8. MONTHLY SUMMARY STATISTICS VIEW (Step 10)
-- ==============================================================================
CREATE OR REPLACE VIEW analytics.monthly_summary_statistics AS
SELECT 
    m.reference_month,
    (SELECT COUNT(*) FROM analytics.reconstructed_establishments e WHERE e.reference_month = m.reference_month) AS establishment_count,
    (SELECT COUNT(*) FROM analytics.reconstructed_companies c WHERE c.reference_month = m.reference_month) AS company_count,
    (SELECT COUNT(*) FROM analytics.reconstructed_simples s WHERE s.reference_month = m.reference_month) AS simples_count,
    (SELECT COUNT(*) FROM analytics.reconstructed_partners p WHERE p.reference_month = m.reference_month) AS partner_count,
    (SELECT COUNT(*) FROM analytics.reconstructed_partner_summaries ps WHERE ps.reference_month = m.reference_month) AS partner_summary_count,
    (SELECT COUNT(*) FROM analytics.establishment_transitions t WHERE t.reference_month = m.reference_month) AS transition_count
FROM (
    SELECT '2023-05'::text AS reference_month UNION ALL
    SELECT '2023-06'::text UNION ALL
    SELECT '2023-07'::text UNION ALL
    SELECT '2023-08'::text UNION ALL
    SELECT '2023-09'::text UNION ALL
    SELECT '2023-10'::text
) m
ORDER BY m.reference_month;

-- ==============================================================================
-- 9. PIPELINE EXECUTION BLOCK
-- ==============================================================================

-- Example function execution triggers
-- CALL analytics.reconstruct_temporal_data();
-- CALL analytics.compress_longitudinal_intervals();
-- CALL analytics.populate_transitions();

-- ==============================================================================
-- 10. RECONSTRUCTED VIEWS WITH STATIC/DYNAMIC TAGGING
-- ==============================================================================
CREATE OR REPLACE VIEW analytics.v_reconstructed_establishments AS
SELECT 
    e.*,
    (k.cnpj_basico IS NULL) AS is_static
FROM analytics.reconstructed_establishments e
LEFT JOIN analytics.changed_company_keys k ON e.cnpj_basic = k.cnpj_basico;

CREATE OR REPLACE VIEW analytics.v_reconstructed_companies AS
SELECT 
    c.*,
    (k.cnpj_basico IS NULL) AS is_static
FROM analytics.reconstructed_companies c
LEFT JOIN analytics.changed_company_keys k ON c.cnpj_basic = k.cnpj_basico;

CREATE OR REPLACE VIEW analytics.v_reconstructed_simples AS
SELECT 
    s.*,
    (k.cnpj_basico IS NULL) AS is_static
FROM analytics.reconstructed_simples s
LEFT JOIN analytics.changed_company_keys k ON s.cnpj_basic = k.cnpj_basico;

CREATE OR REPLACE VIEW analytics.v_reconstructed_partners AS
SELECT 
    p.*,
    (k.cnpj_basico IS NULL) AS is_static
FROM analytics.reconstructed_partners p
LEFT JOIN analytics.changed_company_keys k ON p.cnpj_basic = k.cnpj_basico;

CREATE OR REPLACE VIEW analytics.v_reconstructed_partner_summaries AS
SELECT 
    ps.*,
    (k.cnpj_basico IS NULL) AS is_static
FROM analytics.reconstructed_partner_summaries ps
LEFT JOIN analytics.changed_company_keys k ON ps.cnpj_basic = k.cnpj_basico;
