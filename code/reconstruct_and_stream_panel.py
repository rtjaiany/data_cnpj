#!/usr/bin/env python3
"""
==============================================================================
reconstruct_and_stream_panel.py
Integrated Longitudinal Panel Reconstruction & Streaming Pipeline to Parquet
==============================================================================

Features:
- National Current-State Architecture: Tracks all Brazilian establishments in a
  persistent state table (analytics.panel_checkpoint_state) to correctly handle
  interstate migration without false deletion.
- Streaming to Parquet: Streams records chunk-by-chunk directly into partitioned
  Snappy-compressed Parquet files, keeping disk usage under ~15-20 GB.
- Resumability & Checkpointing: Tracks completed months in analytics.panel_checkpoint_metadata.
  Can be safely interrupted and resumed from the exact next month without restarting from scratch.
- Portões de Qualidade: Verifies key uniqueness, schema conformity, and IBGE cd_mun mapping
  for each partition, and integrates automated regression checks (regression_comparison.py).
- Multi-Scope Support: Configurable start/end months, geographic filters ('SP', 'RJ', 'SP,RJ',
  or 'ALL' for national scale), and support for future months.
"""

import os
import sys
import time
import argparse
import datetime
import pathlib
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Define strict PyArrow schema for output Parquet matching dissertation data dictionary
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

def get_db_connection(parent_dir):
    """Establishes database connection using .env configuration."""
    dotenv_path = os.path.join(parent_dir, ".env")
    if not os.path.isfile(dotenv_path):
        dotenv_path = os.path.join(os.getcwd(), ".env")
    load_dotenv(dotenv_path=dotenv_path)

    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "cnpj")

    conn = psycopg2.connect(
        dbname=db_name, user=db_user, host=db_host, port=db_port, password=db_pass
    )
    return conn

def generate_months(start_str, end_str):
    """Generates chronological list of months in YYYY-MM format."""
    start = datetime.datetime.strptime(start_str, "%Y-%m")
    end = datetime.datetime.strptime(end_str, "%Y-%m")
    months = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months

def init_checkpoint_tables(cur):
    """Creates metadata and state tracking tables if they do not exist."""
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    
    # Metadata table recording completed monthly partitions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.panel_checkpoint_metadata (
            reference_month VARCHAR(7) NOT NULL,
            state_filter VARCHAR(50) NOT NULL,
            output_path TEXT NOT NULL,
            row_count BIGINT NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (reference_month, state_filter)
        );
    """)

    # State tracker recording the reference month represented by panel_checkpoint_state
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.panel_checkpoint_tracker (
            id INTEGER PRIMARY KEY DEFAULT 1,
            last_state_month VARCHAR(7) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT single_row CHECK (id = 1)
        );
    """)

def get_current_state_month(cur):
    """Returns the month currently loaded in analytics.panel_checkpoint_state, or None."""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'analytics' AND table_name = 'panel_checkpoint_state'
        );
    """)
    table_exists = cur.fetchone()[0]
    if not table_exists:
        return None

    cur.execute("SELECT last_state_month FROM analytics.panel_checkpoint_tracker WHERE id = 1;")
    row = cur.fetchone()
    return row[0] if row else None

def init_baseline_state(cur, conn, baseline_month):
    """
    Initializes the national current state baseline in analytics.panel_checkpoint_state.
    Implements:
    - Quarantine resolution of conflicting company fields into analytics.quarantine_empresa.
    - Company resolution via temp_resolved_empresa (quarantined roots resolved to NULL).
    - Deterministic establishment deduplication prioritizing active status.
    - Aggregated socio information and Simples option flags.
    - Initial cd_mun mapping from public.munic.
    """
    print(f"\n[INIT] Initializing National Baseline State for {baseline_month}...")
    t0 = time.time()

    # 1. Quarantine conflicting company roots
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.quarantine_empresa (
            cnpj_basico VARCHAR(8) PRIMARY KEY,
            motivo TEXT,
            reference_month VARCHAR(7),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("DELETE FROM analytics.quarantine_empresa WHERE reference_month = %s;", (baseline_month,))

    cur.execute("""
        INSERT INTO analytics.quarantine_empresa (cnpj_basico, motivo, reference_month)
        SELECT 
            cnpj_basico,
            'Conflict in retained company fields: ' || 
            CASE WHEN dc_cap > 1 THEN 'capital_social (' || dc_cap || ') ' ELSE '' END ||
            CASE WHEN dc_nat > 1 THEN 'natureza_juridica (' || dc_nat || ') ' ELSE '' END ||
            CASE WHEN dc_por > 1 THEN 'porte_empresa (' || dc_por || ') ' ELSE '' END ||
            CASE WHEN dc_qua > 1 THEN 'qualificacao_responsavel (' || dc_qua || ') ' ELSE '' END ||
            CASE WHEN dc_ent > 1 THEN 'ente_federativo_responsavel (' || dc_ent || ') ' ELSE '' END,
            %s
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
    """, (baseline_month,))
    conn.commit()

    # 2. Build deduplicated resolved company temporary table
    print("Building temporary resolved company table...")
    cur.execute("DROP TABLE IF EXISTS temp_resolved_empresa;")
    cur.execute("""
        CREATE TEMP TABLE temp_resolved_empresa AS
        SELECT
            e.cnpj_basico,
            MAX(e.capital_social) AS capital_social,
            MAX(e.natureza_juridica) AS natureza_juridica,
            MAX(e.porte_empresa) AS porte_empresa,
            MAX(e.qualificacao_responsavel) AS qualificacao_responsavel,
            MAX(e.ente_federativo_responsavel) AS ente_federativo_responsavel
        FROM public.empresa e
        LEFT JOIN analytics.quarantine_empresa q ON e.cnpj_basico = q.cnpj_basico AND q.reference_month = %s
        WHERE q.cnpj_basico IS NULL
        GROUP BY e.cnpj_basico;
    """, (baseline_month,))
    cur.execute("CREATE UNIQUE INDEX ON temp_resolved_empresa (cnpj_basico);")
    conn.commit()

    # 3. Create persistent national state table
    print("Creating analytics.panel_checkpoint_state table...")
    cur.execute("DROP TABLE IF EXISTS analytics.panel_checkpoint_state CASCADE;")
    cur.execute("""
        CREATE TABLE analytics.panel_checkpoint_state (
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
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
        );
    """)

    # Populate baseline state with deterministic selection and cd_mun mapping
    print("Populating baseline national establishments (May 2023)...")
    cur.execute("""
        INSERT INTO analytics.panel_checkpoint_state
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
            mun.cd_mun
        FROM public.estabelecimento est
        LEFT JOIN temp_resolved_empresa emp ON emp.cnpj_basico = est.cnpj_basico
        LEFT JOIN public.simples smp ON smp.cnpj_basico = est.cnpj_basico
        LEFT JOIN public.munic mun ON mun.codigo = est.municipio
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
    """)
    conn.commit()

    # Index for fast state filtering during streaming
    print("Building indexing on analytics.panel_checkpoint_state (cnpj_basico, uf)...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chk_cnpj_basico ON analytics.panel_checkpoint_state (cnpj_basico);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_chk_uf ON analytics.panel_checkpoint_state (uf);")
    cur.execute("ANALYZE analytics.panel_checkpoint_state;")
    conn.commit()

    # Record state tracker
    cur.execute("""
        INSERT INTO analytics.panel_checkpoint_tracker (id, last_state_month, updated_at)
        VALUES (1, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE 
        SET last_state_month = EXCLUDED.last_state_month, updated_at = CURRENT_TIMESTAMP;
    """, (baseline_month,))
    conn.commit()

    print(f"[INIT] Baseline {baseline_month} successfully initialized in {time.time() - t0:.2f}s.")

def apply_month_deltas(cur, conn, curr_month):
    """
    Applies incremental deltas from public.snapshots for curr_month to analytics.panel_checkpoint_state.
    Tracks interstate migration properly as UPDATE (changing uf) rather than DELETE.
    """
    print(f"\n[DELTA] Applying monthly deltas for {curr_month} to National State...")
    t0 = time.time()

    # Build temp resolved company table if needed for fallback company attributes
    cur.execute("DROP TABLE IF EXISTS temp_resolved_empresa;")
    cur.execute("""
        CREATE TEMP TABLE temp_resolved_empresa AS
        SELECT
            e.cnpj_basico,
            MAX(e.capital_social) AS capital_social,
            MAX(e.natureza_juridica) AS natureza_juridica,
            MAX(e.porte_empresa) AS porte_empresa,
            MAX(e.qualificacao_responsavel) AS qualificacao_responsavel,
            MAX(e.ente_federativo_responsavel) AS ente_federativo_responsavel
        FROM public.empresa e
        LEFT JOIN analytics.quarantine_empresa q ON e.cnpj_basico = q.cnpj_basico AND q.reference_month <= %s
        WHERE q.cnpj_basico IS NULL
        GROUP BY e.cnpj_basico;
    """, (curr_month,))
    cur.execute("CREATE UNIQUE INDEX ON temp_resolved_empresa (cnpj_basico);")

    # A. ESTABELECIMENTO Deletions
    cur.execute("""
        DELETE FROM analytics.panel_checkpoint_state t
        USING public.snapshots s
        WHERE s.tabela = 'estabelecimento'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao = 'DELETE'
          AND t.cnpj_basico = s.cnpj_basico
          AND t.cnpj_ordem = s.cnpj_ordem
          AND t.cnpj_dv = s.cnpj_dv;
    """, (curr_month,))

    # B. ESTABELECIMENTO Updates (including UF changes and IBGE cd_mun mapping)
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
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
            data_situacao_especial = (s.conteudo_novo->>'data_situacao_especial')::int,
            cd_mun = m.cd_mun
        FROM public.snapshots s
        LEFT JOIN public.munic m ON m.codigo = (s.conteudo_novo->>'municipio')::int
        WHERE s.tabela = 'estabelecimento'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao = 'UPDATE'
          AND t.cnpj_basico = s.cnpj_basico
          AND t.cnpj_ordem = s.cnpj_ordem
          AND t.cnpj_dv = s.cnpj_dv;
    """, (curr_month,))

    # C. ESTABELECIMENTO Inserts
    cur.execute("""
        INSERT INTO analytics.panel_checkpoint_state (
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
            m.cd_mun
        FROM public.snapshots s
        LEFT JOIN LATERAL (
            SELECT 
                capital_social, natureza_juridica, porte_empresa, qualificacao_responsavel,
                ente_federativo_responsavel, opcao_pelo_simples, data_opcao_simples,
                data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei,
                qtde_socios, qtde_socios_pf, qtde_socios_pj, qtde_socios_estrangeiro,
                min_faixa_etaria, max_faixa_etaria, data_entrada_antiga, data_entrada_recente,
                qtde_administradores
            FROM analytics.panel_checkpoint_state c
            WHERE c.cnpj_basico = s.cnpj_basico
            LIMIT 1
        ) c ON true
        LEFT JOIN temp_resolved_empresa b_emp ON b_emp.cnpj_basico = s.cnpj_basico AND c.capital_social IS NULL
        LEFT JOIN public.simples b_smp ON b_smp.cnpj_basico = s.cnpj_basico AND c.capital_social IS NULL
        LEFT JOIN public.munic m ON m.codigo = (s.conteudo_novo->>'municipio')::int
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
            WHERE cnpj_basico IN (
                SELECT cnpj_basico FROM public.snapshots 
                WHERE tabela = 'estabelecimento' AND mes_referencia = %s AND tipo_alteracao = 'INSERT'
            )
            GROUP BY cnpj_basico
        ) b_soc ON b_soc.cnpj_basico = s.cnpj_basico AND c.capital_social IS NULL
        WHERE s.tabela = 'estabelecimento'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao = 'INSERT'
        ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO NOTHING;
    """, (curr_month, curr_month))

    # D. EMPRESA Updates & Inserts
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
        SET
            capital_social = (s.conteudo_novo->>'capital_social')::double precision,
            natureza_juridica = (s.conteudo_novo->>'natureza_juridica')::int,
            porte_empresa = (s.conteudo_novo->>'porte_empresa')::int,
            qualificacao_responsavel = (s.conteudo_novo->>'qualificacao_responsavel')::int,
            ente_federativo_responsavel = s.conteudo_novo->>'ente_federativo_responsavel'
        FROM public.snapshots s
        WHERE s.tabela = 'empresa'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao IN ('INSERT', 'UPDATE')
          AND t.cnpj_basico = s.cnpj_basico;
    """, (curr_month,))

    # E. EMPRESA Deletions
    cur.execute("""
        DELETE FROM analytics.panel_checkpoint_state t
        USING public.snapshots s
        WHERE s.tabela = 'empresa'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao = 'DELETE'
          AND t.cnpj_basico = s.cnpj_basico;
    """, (curr_month,))

    # F. SIMPLES Updates & Inserts
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
        SET
            opcao_pelo_simples = s.conteudo_novo->>'opcao_pelo_simples',
            data_opcao_simples = (s.conteudo_novo->>'data_opcao_simples')::int,
            data_exclusao_simples = (s.conteudo_novo->>'data_exclusao_simples')::int,
            opcao_mei = s.conteudo_novo->>'opcao_mei',
            data_opcao_mei = (s.conteudo_novo->>'data_opcao_mei')::int,
            data_exclusao_mei = (s.conteudo_novo->>'data_exclusao_mei')::int
        FROM public.snapshots s
        WHERE s.tabela = 'simples'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao IN ('INSERT', 'UPDATE')
          AND t.cnpj_basico = s.cnpj_basico;
    """, (curr_month,))

    # G. SIMPLES Deletions
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
        SET
            opcao_pelo_simples = NULL,
            data_opcao_simples = NULL,
            data_exclusao_simples = NULL,
            opcao_mei = NULL,
            data_opcao_mei = NULL,
            data_exclusao_mei = NULL
        FROM public.snapshots s
        WHERE s.tabela = 'simples'
          AND s.mes_referencia = %s
          AND s.tipo_alteracao = 'DELETE'
          AND t.cnpj_basico = s.cnpj_basico;
    """, (curr_month,))

    # H. SOCIOS (Partners) Updates & Inserts
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
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
          AND s.mes_referencia = %s
          AND s.tipo_alteracao IN ('INSERT', 'UPDATE')
          AND t.cnpj_basico = s.cnpj_basico;
    """, (curr_month,))

    # I. SOCIOS Deletions
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
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
          AND s.mes_referencia = %s
          AND s.tipo_alteracao = 'DELETE'
          AND t.cnpj_basico = s.cnpj_basico;
    """, (curr_month,))

    # J. QUARANTINE: Nullify company fields for roots quarantined in curr_month
    cur.execute("""
        UPDATE analytics.panel_checkpoint_state t
        SET
            capital_social = NULL,
            natureza_juridica = NULL,
            porte_empresa = NULL,
            qualificacao_responsavel = NULL,
            ente_federativo_responsavel = NULL
        FROM analytics.quarantine_empresa q
        WHERE q.cnpj_basico = t.cnpj_basico
          AND q.reference_month = %s;
    """, (curr_month,));

    # Update state tracker
    cur.execute("""
        UPDATE analytics.panel_checkpoint_tracker
        SET last_state_month = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = 1;
    """, (curr_month,))
    conn.commit()

    print(f"[DELTA] Finished applying deltas for {curr_month} in {time.time() - t0:.2f}s.")

def stream_month_to_parquet(conn, ref_month, state_filter, output_base_dir):
    """
    Streams the current state of analytics.panel_checkpoint_state with geographic filter
    directly into a Snappy compressed Parquet file without large RAM consumption.
    """
    output_dir = os.path.join(output_base_dir, f"reference_month={ref_month}")
    os.makedirs(output_dir, exist_ok=True)
    parquet_file = os.path.join(output_dir, "part-000.parquet")
    temp_file = os.path.join(output_dir, "part-000.parquet.tmp")

    where_clause = ""
    params = []
    if state_filter and state_filter.upper() != "ALL":
        states = [s.strip().upper() for s in state_filter.split(",")]
        where_clause = "WHERE uf = ANY(%s)"
        params.append(states)

    query = f"""
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
        FROM analytics.panel_checkpoint_state
        {where_clause}
        ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv
    """

    cursor_name = f"stream_cur_{ref_month.replace('-', '_')}_{int(time.time())}"
    chunk_size = 100000

    print(f"[STREAM] Exporting {ref_month} (Filter: {state_filter or 'ALL'}) to {parquet_file}...")
    t0 = time.time()
    row_count = 0
    writer = None

    try:
        with conn.cursor(name=cursor_name) as stream_cur:
            stream_cur.itersize = chunk_size
            stream_cur.execute(query, tuple(params) if params else None)

            while True:
                rows = stream_cur.fetchmany(chunk_size)
                if not rows:
                    break

                cols_data = {col: [row[idx] for row in rows] for idx, col in enumerate(schema_panel.names)}
                batch = pa.RecordBatch.from_pydict(cols_data, schema=schema_panel)
                table = pa.Table.from_batches([batch])

                if writer is None:
                    writer = pq.ParquetWriter(temp_file, schema_panel, compression='snappy')

                writer.write_table(table)
                row_count += len(rows)
                sys.stdout.write(f"\r  Exported {row_count:,} rows for {ref_month}...")
                sys.stdout.flush()

        print(f"\n[STREAM] Finished streaming {ref_month} ({row_count:,} rows) in {time.time() - t0:.2f}s.")
    finally:
        if writer is not None:
            writer.close()

    # Atomically rename temp file to final destination
    if os.path.exists(temp_file):
        os.replace(temp_file, parquet_file)

    file_size = os.path.getsize(parquet_file)
    return parquet_file, row_count, file_size

def validate_partition_quality(parquet_file, expected_rows=None):
    """
    Quality gate validation for an exported Parquet file:
    1. Verify file exists and is not empty.
    2. Verify strict schema equality (37 columns).
    3. Verify Primary Key uniqueness (no duplicates in cnpj_basico, cnpj_ordem, cnpj_dv).
    """
    print(f"[QUALITY] Running partition quality gate on {parquet_file}...")
    if not os.path.isfile(parquet_file):
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

    file_size = os.path.getsize(parquet_file)
    if file_size == 0:
        raise ValueError(f"Parquet file is empty (0 bytes): {parquet_file}")

    # Read Parquet file metadata
    pf = pq.ParquetFile(parquet_file)
    actual_rows = pf.metadata.num_rows

    if expected_rows is not None and actual_rows != expected_rows:
        raise ValueError(f"Row count mismatch! Expected {expected_rows:,}, but got {actual_rows:,}")

    # Schema validation
    actual_schema = pf.schema_arrow
    if len(actual_schema) != len(schema_panel):
        raise ValueError(f"Column count mismatch! Expected {len(schema_panel)}, got {len(actual_schema)}")

    for expected_field in schema_panel:
        field_idx = actual_schema.get_field_index(expected_field.name)
        if field_idx == -1:
            raise ValueError(f"Missing expected column in Parquet: {expected_field.name}")
        actual_type = actual_schema.field(field_idx).type
        if actual_type != expected_field.type:
            raise ValueError(f"Column type mismatch for {expected_field.name}: expected {expected_field.type}, got {actual_type}")

    # Primary key uniqueness check using PyArrow column projection
    print("  Checking establishment primary key uniqueness...")
    table_keys = pf.read(columns=['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'])
    df_keys = table_keys.to_pandas()
    num_unique = len(df_keys.drop_duplicates())
    if num_unique != actual_rows:
        duplicates = actual_rows - num_unique
        raise ValueError(f"Integrity Violation! Found {duplicates} duplicate establishment primary keys in {parquet_file}!")

    print(f"  [PASS] Quality gate passed: {actual_rows:,} rows, 100% unique primary keys, valid schema ({file_size/(1024*1024):.1f} MB).")
    return True

def run_regression_comparison(parent_dir):
    """Executes code/regression_comparison.py to verify parity against audited benchmarks."""
    import subprocess
    print("\n" + "=" * 70)
    print("RUNNING BENCHMARK REGRESSION COMPARISON (code/regression_comparison.py)")
    print("=" * 70)
    script_path = os.path.join(parent_dir, "code", "regression_comparison.py")
    python_bin = sys.executable
    result = subprocess.run([python_bin, script_path], check=False)
    if result.returncode != 0:
        print("\n[FAIL] Regression comparison checks failed!")
        return False
    print("\n[PASS] All benchmark regression comparison checks passed successfully!")
    return True

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(
        description="Integrated Longitudinal Panel Reconstruction & Streaming Pipeline to Parquet with Checkpointing."
    )
    parser.add_argument("--start-month", default="2023-05", help="Starting baseline reference month (default: 2023-05)")
    parser.add_argument("--end-month", default=None, help="Ending reference month (default: auto-detect latest processed in DB)")
    parser.add_argument("--state", default="SP", help="Geographic filter: state code (e.g. 'SP', 'RJ', 'SP,RJ' or 'ALL' for national; default: 'SP')")
    parser.add_argument("--output-dir", default="reconstructed_panel", help="Output directory for partitioned Parquet files (default: reconstructed_panel)")
    parser.add_argument("--restart", action="store_true", help="Force restart from the baseline month, clearing existing checkpoints")
    parser.add_argument("--verify", action="store_true", help="Run benchmark regression comparison checks after completion")

    args = parser.parse_args()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    output_base_dir = os.path.join(parent_dir, args.output_dir)
    os.makedirs(output_base_dir, exist_ok=True)

    print("=" * 75)
    print("PANEL RECONSTRUCTION & STREAMING PIPELINE")
    print(f"Start Month:  {args.start_month}")
    print(f"State Filter: {args.state}")
    print(f"Output Dir:   {output_base_dir}")
    print(f"Restart:      {args.restart}")
    print("=" * 75)

    conn = get_db_connection(parent_dir)
    cur = conn.cursor()

    try:
        init_checkpoint_tables(cur)
        conn.commit()

        # Determine end month
        if args.end_month:
            end_month = args.end_month
        else:
            cur.execute("SELECT MAX(reference_month) FROM snapshots_metadata WHERE status = 'SUCCESS';")
            row = cur.fetchone()
            end_month = row[0] if row and row[0] else "2024-11"
            print(f"Auto-detected latest successfully processed month in DB: {end_month}")

        months = generate_months(args.start_month, end_month)
        print(f"Target chronological timeline ({len(months)} months): {', '.join(months)}")

        # Handle --restart flag
        if args.restart:
            print("\n[FLAG] --restart specified. Clearing previous checkpoints for this scope...")
            cur.execute("DELETE FROM analytics.panel_checkpoint_metadata WHERE state_filter = %s;", (args.state,))
            conn.commit()

        # Inspect current state in DB and completed partitions
        current_state_month = get_current_state_month(cur)
        print(f"Current national state in database represents month: {current_state_month or 'None (not initialized)'}")

        # Check existing metadata
        cur.execute("""
            SELECT reference_month, row_count, file_size_bytes 
            FROM analytics.panel_checkpoint_metadata 
            WHERE state_filter = %s;
        """, (args.state,))
        completed_meta = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

        # ---------------------------------------------------------------------
        # Execution loop
        # ---------------------------------------------------------------------
        for idx, month in enumerate(months):
            print("\n" + "-" * 70)
            print(f"PROCESSING MONTH ({idx+1}/{len(months)}): {month}")
            print("-" * 70)

            month_parquet = os.path.join(output_base_dir, f"reference_month={month}", "part-000.parquet")
            
            # Check if this month is already exported, validated, and recorded in metadata
            already_done = False
            if month in completed_meta and os.path.isfile(month_parquet):
                exp_rows, exp_size = completed_meta[month]
                actual_size = os.path.getsize(month_parquet)
                if actual_size == exp_size and not args.restart:
                    print(f"[CHECKPOINT] Partition for {month} already exported and verified ({exp_rows:,} rows). Skipping export.")
                    already_done = True

            # If state table is behind, advance it chronologically
            if current_state_month is None or args.restart:
                # Must initialize baseline state
                init_baseline_state(cur, conn, args.start_month)
                current_state_month = args.start_month
                args.restart = False # only restart once

            # If current month is after current_state_month, we must apply deltas
            if current_state_month and month > current_state_month:
                # Ensure state is caught up
                months_to_advance = generate_months(current_state_month, month)[1:]
                for adv_m in months_to_advance:
                    apply_month_deltas(cur, conn, adv_m)
                    current_state_month = adv_m

            # If export is needed for this month
            if not already_done:
                # 1. Stream to Parquet
                parquet_file, row_count, file_size = stream_month_to_parquet(
                    conn, month, args.state, output_base_dir
                )

                # 2. Validate partition quality gate
                validate_partition_quality(parquet_file, expected_rows=row_count)

                # 3. Commit checkpoint metadata
                cur.execute("""
                    INSERT INTO analytics.panel_checkpoint_metadata (
                        reference_month, state_filter, output_path, row_count, file_size_bytes, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (reference_month, state_filter) DO UPDATE 
                    SET output_path = EXCLUDED.output_path,
                        row_count = EXCLUDED.row_count,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        completed_at = CURRENT_TIMESTAMP;
                """, (month, args.state, parquet_file, row_count, file_size))
                conn.commit()
                print(f"[CHECKPOINT] Month {month} successfully saved and registered.")

        print("\n" + "=" * 75)
        print("LONGITUDINAL PANEL RECONSTRUCTION SUCCESSFULLY COMPLETED!")
        print(f"Total months processed: {len(months)} ({months[0]} to {months[-1]})")
        print(f"Destination: {output_base_dir}")
        print("=" * 75)

        # Optional regression gate validation
        if args.verify:
            print("\n[VERIFY] Running automated benchmark quality regression comparison...")
            pass_gate = run_regression_comparison(parent_dir)
            if not pass_gate:
                print("\n[WARNING] Benchmark regression checks failed! Please review log outputs.")
                sys.exit(1)

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
