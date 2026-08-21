-- ==============================================================================
-- SCHEMA: ANALYTICS
-- Analytical layer for temporal reconstruction and longitudinal data analysis
-- Copyright (c) 2026 rtjaiany
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ------------------------------------------------------------------------------
-- Table for target longitudinal establishment panel
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.establishment_panel (
    reference_month VARCHAR(7) NOT NULL,
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
    uf VARCHAR(2),
    municipio INTEGER,
    situacao_especial TEXT,
    data_situacao_especial INTEGER,
    capital_social DOUBLE PRECISION,
    natureza_juridica INTEGER,
    porte_empresa INTEGER,
    qualificacao_responsavel INTEGER,
    ente_federativo_responsavel TEXT,
    opcao_pelo_simples TEXT,
    data_opcao_simples INTEGER,
    data_exclusao_simples INTEGER,
    opcao_mei TEXT,
    data_opcao_mei INTEGER,
    data_exclusao_mei INTEGER,
    qtde_socios INTEGER DEFAULT 0,
    qtde_socios_pf INTEGER DEFAULT 0,
    qtde_socios_pj INTEGER DEFAULT 0,
    qtde_socios_estrangeiro INTEGER DEFAULT 0,
    min_faixa_etaria INTEGER,
    max_faixa_etaria INTEGER,
    data_entrada_antiga INTEGER,
    data_entrada_recente INTEGER,
    qtde_administradores INTEGER DEFAULT 0,
    cd_mun INTEGER,
    PRIMARY KEY (reference_month, cnpj_basico, cnpj_ordem, cnpj_dv)
);

-- Optimize panel queries
CREATE INDEX IF NOT EXISTS idx_panel_ref_uf ON analytics.establishment_panel (reference_month, uf);
CREATE INDEX IF NOT EXISTS idx_panel_cnpj ON analytics.establishment_panel (cnpj_basico, cnpj_ordem, cnpj_dv);
CREATE INDEX IF NOT EXISTS idx_panel_cnae ON analytics.establishment_panel (cnae_fiscal_principal);

-- ------------------------------------------------------------------------------
-- Forward Longitudinal Panel Reconstruction Procedure
-- ------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE analytics.reconstruct_panel(
    start_month VARCHAR,
    end_month VARCHAR,
    state_filter VARCHAR DEFAULT NULL
) AS $$
DECLARE
    months text[];
    curr_month text;
BEGIN
    RAISE INFO 'Starting temporal panel reconstruction from % to % (Filter: %)...', start_month, end_month, COALESCE(state_filter, 'ALL');

    -- 1. Clear target month range
    DELETE FROM analytics.establishment_panel 
    WHERE reference_month BETWEEN start_month AND end_month;

    -- 2. Build temporal array of months
    WITH RECURSIVE month_list AS (
        SELECT TO_DATE(start_month || '-01', 'YYYY-MM-DD') AS m_date
        UNION ALL
        SELECT (m_date + INTERVAL '1 month')::date
        FROM month_list
        WHERE m_date < TO_DATE(end_month || '-01', 'YYYY-MM-DD')
    )
    SELECT ARRAY_AGG(TO_CHAR(m_date, 'YYYY-MM')) INTO months FROM month_list;

    -- 2.5 Create quarantine table if not exists and clear records for the start_month
    CREATE TABLE IF NOT EXISTS analytics.quarantine_empresa (
        cnpj_basico VARCHAR(8) PRIMARY KEY,
        motivo TEXT,
        reference_month VARCHAR(7),
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    DELETE FROM analytics.quarantine_empresa WHERE reference_month = start_month;

    -- Identify conflicting company roots in public.empresa
    INSERT INTO analytics.quarantine_empresa (cnpj_basico, motivo, reference_month)
    SELECT 
        cnpj_basico,
        'Conflict in retained company fields: ' || 
        CASE WHEN dc_cap > 1 THEN 'capital_social (' || dc_cap || ') ' ELSE '' END ||
        CASE WHEN dc_nat > 1 THEN 'natureza_juridica (' || dc_nat || ') ' ELSE '' END ||
        CASE WHEN dc_por > 1 THEN 'porte_empresa (' || dc_por || ') ' ELSE '' END ||
        CASE WHEN dc_qua > 1 THEN 'qualificacao_responsavel (' || dc_qua || ') ' ELSE '' END ||
        CASE WHEN dc_ent > 1 THEN 'ente_federativo_responsavel (' || dc_ent || ') ' ELSE '' END,
        start_month
    FROM (
        SELECT
            cnpj_basico,
            COUNT(DISTINCT capital_social) as dc_cap,
            COUNT(DISTINCT natureza_juridica) as dc_nat,
            COUNT(DISTINCT porte_empresa) as dc_por,
            COUNT(DISTINCT qualificacao_responsavel) as dc_qua,
            COUNT(DISTINCT ente_federativo_responsavel) as dc_ent
        FROM public.empresa
        GROUP BY cnpj_basico
        HAVING COUNT(*) > 1
    ) stats
    WHERE dc_cap > 1 OR dc_nat > 1 OR dc_por > 1 OR dc_qua > 1 OR dc_ent > 1
    ON CONFLICT (cnpj_basico) DO UPDATE 
    SET motivo = EXCLUDED.motivo, reference_month = EXCLUDED.reference_month;

    -- Deduplicate public.empresa into a temporary table temp_resolved_empresa
    DROP TABLE IF EXISTS temp_resolved_empresa;
    CREATE TEMP TABLE temp_resolved_empresa AS
    SELECT
        e.cnpj_basico,
        MAX(e.capital_social) AS capital_social,
        MAX(e.natureza_juridica) AS natureza_juridica,
        MAX(e.porte_empresa) AS porte_empresa,
        MAX(e.qualificacao_responsavel) AS qualificacao_responsavel,
        MAX(e.ente_federativo_responsavel) AS ente_federativo_responsavel
    FROM public.empresa e
    LEFT JOIN analytics.quarantine_empresa q ON e.cnpj_basico = q.cnpj_basico
    WHERE q.cnpj_basico IS NULL
    GROUP BY e.cnpj_basico;

    CREATE UNIQUE INDEX ON temp_resolved_empresa (cnpj_basico);

    -- 3. Initialize current-state temp table with baseline (May 2023) - NATIONAL STATE
    DROP TABLE IF EXISTS temp_current_state;
    
    CREATE TEMP TABLE temp_current_state AS
    SELECT DISTINCT ON (est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv)
        est.cnpj_basico,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.identificador_matriz_filial,
        est.situacao_cadastral,
        est.data_situacao_cadastral,
        est.motivo_situacao_cadastral,
        est.pais,
        est.data_inicio_atividade,
        est.cnae_fiscal_principal,
        est.cnae_fiscal_secundaria,
        est.uf,
        est.municipio,
        est.situacao_especial,
        est.data_situacao_especial,
        emp.capital_social,
        emp.natureza_juridica,
        emp.porte_empresa,
        emp.qualificacao_responsavel,
        emp.ente_federativo_responsavel,
        smp.opcao_pelo_simples,
        smp.data_opcao_simples,
        smp.data_exclusao_simples,
        smp.opcao_mei,
        smp.data_opcao_mei,
        smp.data_exclusao_mei,
        COALESCE(soc.qtde_socios, 0)::int as qtde_socios,
        COALESCE(soc.qtde_socios_pf, 0)::int as qtde_socios_pf,
        COALESCE(soc.qtde_socios_pj, 0)::int as qtde_socios_pj,
        COALESCE(soc.qtde_socios_estrangeiro, 0)::int as qtde_socios_estrangeiro,
        soc.min_faixa_etaria::int as min_faixa_etaria,
        soc.max_faixa_etaria::int as max_faixa_etaria,
        soc.data_entrada_antiga::int as data_entrada_antiga,
        soc.data_entrada_recente::int as data_entrada_recente,
        COALESCE(soc.qtde_administradores, 0)::int as qtde_administradores,
        NULL::integer as cd_mun
    FROM public.estabelecimento est
    LEFT JOIN temp_resolved_empresa emp ON emp.cnpj_basico = est.cnpj_basico
    LEFT JOIN public.simples smp ON smp.cnpj_basico = est.cnpj_basico
    LEFT JOIN (
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
        FROM public.socios
        GROUP BY cnpj_basico
    ) soc ON soc.cnpj_basico = est.cnpj_basico
    ORDER BY est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv, est.situacao_cadastral DESC NULLS LAST;

    CREATE UNIQUE INDEX ON temp_current_state (cnpj_basico, cnpj_ordem, cnpj_dv);
    CREATE INDEX ON temp_current_state (cnpj_basico);
    ANALYZE temp_current_state;

    -- 4. Iteratively process month by month forward
    FOR i IN 1..array_length(months, 1) LOOP
        curr_month := months[i];
        RAISE INFO 'Processing month: %', curr_month;

        IF curr_month = start_month THEN
            -- First month is the initial state (May 2023 baseline) - apply geographic filter on output layer
            INSERT INTO analytics.establishment_panel
            SELECT curr_month, * FROM temp_current_state
            WHERE (state_filter IS NULL OR uf = ANY(string_to_array(state_filter, ',')));
        ELSE
            -- Apply snapshots of the current month to temp_current_state

            -- A. ESTABELECIMENTO Deletions
            DELETE FROM temp_current_state t
            USING public.snapshots s
            WHERE s.tabela = 'estabelecimento'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao = 'DELETE'
              AND t.cnpj_basico = s.cnpj_basico
              AND t.cnpj_ordem = s.cnpj_ordem
              AND t.cnpj_dv = s.cnpj_dv;

            -- B. ESTABELECIMENTO Updates
            UPDATE temp_current_state t
            SET
                identificador_matriz_filial = (s.conteudo_novo->>'identificador_matriz_filial')::int,
                situacao_cadastral = (s.conteudo_novo->>'situacao_cadastral')::int,
                data_situacao_cadastral = (s.conteudo_novo->>'data_situacao_cadastral')::int,
                motivo_situacao_cadastral = (s.conteudo_novo->>'motivo_situacao_cadastral')::int,
                pais = s.conteudo_novo->>'pais',
                data_inicio_atividade = (s.conteudo_novo->>'data_inicio_atividade')::int,
                cnae_fiscal_principal = (s.conteudo_novo->>'cnae_fiscal_principal')::int,
                cnae_fiscal_secundaria = s.conteudo_novo->>'cnae_fiscal_secundaria',
                uf = s.conteudo_novo->>'uf',
                municipio = (s.conteudo_novo->>'municipio')::int,
                situacao_especial = s.conteudo_novo->>'situacao_especial',
                data_situacao_especial = (s.conteudo_novo->>'data_situacao_especial')::int
            FROM public.snapshots s
            WHERE s.tabela = 'estabelecimento'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao = 'UPDATE'
              AND t.cnpj_basico = s.cnpj_basico
              AND t.cnpj_ordem = s.cnpj_ordem
              AND t.cnpj_dv = s.cnpj_dv;

            -- C. ESTABELECIMENTO Inserts
            INSERT INTO temp_current_state (
                cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, situacao_cadastral,
                data_situacao_cadastral, motivo_situacao_cadastral, pais, data_inicio_atividade,
                cnae_fiscal_principal, cnae_fiscal_secundaria, uf, municipio, situacao_especial, data_situacao_especial,
                capital_social, natureza_juridica, porte_empresa, qualificacao_responsavel, ente_federativo_responsavel,
                opcao_pelo_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei,
                qtde_socios, qtde_socios_pf, qtde_socios_pj, qtde_socios_estrangeiro, min_faixa_etaria, max_faixa_etaria,
                data_entrada_antiga, data_entrada_recente, qtde_administradores, cd_mun
            )
            SELECT
                s.cnpj_basico,
                s.cnpj_ordem,
                s.cnpj_dv,
                (s.conteudo_novo->>'identificador_matriz_filial')::int,
                (s.conteudo_novo->>'situacao_cadastral')::int,
                (s.conteudo_novo->>'data_situacao_cadastral')::int,
                (s.conteudo_novo->>'motivo_situacao_cadastral')::int,
                s.conteudo_novo->>'pais',
                (s.conteudo_novo->>'data_inicio_atividade')::int,
                (s.conteudo_novo->>'cnae_fiscal_principal')::int,
                s.conteudo_novo->>'cnae_fiscal_secundaria',
                s.conteudo_novo->>'uf',
                (s.conteudo_novo->>'municipio')::int,
                s.conteudo_novo->>'situacao_especial',
                (s.conteudo_novo->>'data_situacao_especial')::int,
                COALESCE(c.capital_social, b_emp.capital_social),
                COALESCE(c.natureza_juridica, b_emp.natureza_juridica),
                COALESCE(c.porte_empresa, b_emp.porte_empresa),
                COALESCE(c.qualificacao_responsavel, b_emp.qualificacao_responsavel),
                COALESCE(c.ente_federativo_responsavel, b_emp.ente_federativo_responsavel),
                COALESCE(c.opcao_pelo_simples, b_smp.opcao_pelo_simples),
                COALESCE(c.data_opcao_simples, b_smp.data_opcao_simples),
                COALESCE(c.data_exclusao_simples, b_smp.data_exclusao_simples),
                COALESCE(c.opcao_mei, b_smp.opcao_mei),
                COALESCE(c.data_opcao_mei, b_smp.data_opcao_mei),
                COALESCE(c.data_exclusao_mei, b_smp.data_exclusao_mei),
                COALESCE(c.qtde_socios, b_soc.qtde_socios, 0),
                COALESCE(c.qtde_socios_pf, b_soc.qtde_socios_pf, 0),
                COALESCE(c.qtde_socios_pj, b_soc.qtde_socios_pj, 0),
                COALESCE(c.qtde_socios_estrangeiro, b_soc.qtde_socios_estrangeiro, 0),
                COALESCE(c.min_faixa_etaria, b_soc.min_faixa_etaria),
                COALESCE(c.max_faixa_etaria, b_soc.max_faixa_etaria),
                COALESCE(c.data_entrada_antiga, b_soc.data_entrada_antiga),
                COALESCE(c.data_entrada_recente, b_soc.data_entrada_recente),
                COALESCE(c.qtde_administradores, b_soc.qtde_administradores, 0),
                NULL::integer
            FROM public.snapshots s
            LEFT JOIN (
                SELECT DISTINCT ON (cnpj_basico) *
                FROM temp_current_state
            ) c ON c.cnpj_basico = s.cnpj_basico
            LEFT JOIN temp_resolved_empresa b_emp ON b_emp.cnpj_basico = s.cnpj_basico AND c.cnpj_basico IS NULL
            LEFT JOIN public.simples b_smp ON b_smp.cnpj_basico = s.cnpj_basico AND c.cnpj_basico IS NULL
            LEFT JOIN (
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
                FROM public.socios
                GROUP BY cnpj_basico
            ) b_soc ON b_soc.cnpj_basico = s.cnpj_basico AND c.cnpj_basico IS NULL
            WHERE s.tabela = 'estabelecimento'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao = 'INSERT'
            ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO NOTHING;

            -- D. EMPRESA Updates & Inserts
            UPDATE temp_current_state t
            SET
                capital_social = (s.conteudo_novo->>'capital_social')::double precision,
                natureza_juridica = (s.conteudo_novo->>'natureza_juridica')::int,
                porte_empresa = (s.conteudo_novo->>'porte_empresa')::int,
                qualificacao_responsavel = (s.conteudo_novo->>'qualificacao_responsavel')::int,
                ente_federativo_responsavel = s.conteudo_novo->>'ente_federativo_responsavel'
            FROM public.snapshots s
            WHERE s.tabela = 'empresa'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao IN ('INSERT', 'UPDATE')
              AND t.cnpj_basico = s.cnpj_basico;

            -- E. EMPRESA Deletions
            DELETE FROM temp_current_state t
            USING public.snapshots s
            WHERE s.tabela = 'empresa'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao = 'DELETE'
              AND t.cnpj_basico = s.cnpj_basico;

            -- F. SIMPLES Updates & Inserts
            UPDATE temp_current_state t
            SET
                opcao_pelo_simples = s.conteudo_novo->>'opcao_pelo_simples',
                data_opcao_simples = (s.conteudo_novo->>'data_opcao_simples')::int,
                data_exclusao_simples = (s.conteudo_novo->>'data_exclusao_simples')::int,
                opcao_mei = s.conteudo_novo->>'opcao_mei',
                data_opcao_mei = (s.conteudo_novo->>'data_opcao_mei')::int,
                data_exclusao_mei = (s.conteudo_novo->>'data_exclusao_mei')::int
            FROM public.snapshots s
            WHERE s.tabela = 'simples'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao IN ('INSERT', 'UPDATE')
              AND t.cnpj_basico = s.cnpj_basico;

            -- G. SIMPLES Deletions
            UPDATE temp_current_state t
            SET
                opcao_pelo_simples = NULL,
                data_opcao_simples = NULL,
                data_exclusao_simples = NULL,
                opcao_mei = NULL,
                data_opcao_mei = NULL,
                data_exclusao_mei = NULL
            FROM public.snapshots s
            WHERE s.tabela = 'simples'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao = 'DELETE'
              AND t.cnpj_basico = s.cnpj_basico;

            -- H. SOCIOS (Partners) Updates & Inserts
            UPDATE temp_current_state t
            SET
                qtde_socios = (s.conteudo_novo->>'qtde_socios')::int,
                qtde_socios_pf = (s.conteudo_novo->>'qtde_socios_pf')::int,
                qtde_socios_pj = (s.conteudo_novo->>'qtde_socios_pj')::int,
                qtde_socios_estrangeiro = (s.conteudo_novo->>'qtde_socios_estrangeiro')::int,
                min_faixa_etaria = (s.conteudo_novo->>'min_faixa_etaria')::int,
                max_faixa_etaria = (s.conteudo_novo->>'max_faixa_etaria')::int,
                data_entrada_antiga = (s.conteudo_novo->>'data_entrada_antiga')::int,
                data_entrada_recente = (s.conteudo_novo->>'data_entrada_recente')::int,
                qtde_administradores = (s.conteudo_novo->>'qtde_administradores')::int
            FROM public.snapshots s
            WHERE s.tabela = 'socios'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao IN ('INSERT', 'UPDATE')
              AND t.cnpj_basico = s.cnpj_basico;

            -- I. SOCIOS (Partners) Deletions
            UPDATE temp_current_state t
            SET
                qtde_socios = 0,
                qtde_socios_pf = 0,
                qtde_socios_pj = 0,
                qtde_socios_estrangeiro = 0,
                min_faixa_etaria = NULL,
                max_faixa_etaria = NULL,
                data_entrada_antiga = NULL,
                data_entrada_recente = NULL,
                qtde_administradores = 0
            FROM public.snapshots s
            WHERE s.tabela = 'socios'
              AND s.mes_referencia = curr_month
              AND s.tipo_alteracao = 'DELETE'
              AND t.cnpj_basico = s.cnpj_basico;

            -- Clean and optimize temp table state
            ANALYZE temp_current_state;

            -- Append current state to final panel table - apply geographic filter on output layer
            INSERT INTO analytics.establishment_panel
            SELECT curr_month, * FROM temp_current_state
            WHERE (state_filter IS NULL OR uf = ANY(string_to_array(state_filter, ',')));
        END IF;
    END LOOP;

    -- 5. Post-processing: Bulk update cd_mun from public.munic to keep loop lightweight
    RAISE INFO 'Post-processing: updating cd_mun in analytics.establishment_panel...';
    UPDATE analytics.establishment_panel p
    SET cd_mun = m.cd_mun
    FROM public.munic m
    WHERE p.municipio = m.codigo
      AND p.reference_month BETWEEN start_month AND end_month;

    -- 6. Post-processing: Force quarantined company fields (including Simples/MEI) to NULL across all months in target range
    RAISE INFO 'Post-processing: applying persistent quarantine cleanup...';
    UPDATE analytics.establishment_panel
    SET capital_social = NULL,
        natureza_juridica = NULL,
        porte_empresa = NULL,
        qualificacao_responsavel = NULL,
        ente_federativo_responsavel = NULL,
        opcao_pelo_simples = NULL,
        data_opcao_simples = NULL,
        data_exclusao_simples = NULL,
        opcao_mei = NULL,
        data_opcao_mei = NULL,
        data_exclusao_mei = NULL
    WHERE reference_month BETWEEN start_month AND end_month
      AND cnpj_basico IN (SELECT cnpj_basico FROM analytics.quarantine_empresa);

    -- Clean up temporary table
    DROP TABLE IF EXISTS temp_current_state;
    RAISE INFO 'Longitudinal panel reconstruction completed successfully!';
END;
$$ LANGUAGE plpgsql;
