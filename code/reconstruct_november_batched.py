"""Reconstruct the official November 2024 state in an isolated table.

The script copies the validated October state, applies November changes, and
processes establishment inserts in resumable hash buckets. Production state,
snapshots, and existing Parquet outputs are never modified.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

SOURCE_TABLE = "analytics.panel_checkpoint_state"
TARGET_TABLE = "analytics.panel_checkpoint_state_canonical_2024_11"
CHECKPOINT_TABLE = "analytics.panel_checkpoint_batch_2024_11"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an isolated canonical November 2024 state.")
    parser.add_argument("--batches", type=int, default=64, help="Hash buckets for establishment inserts.")
    parser.add_argument("--work-mem", default="256MB", help="Session-local PostgreSQL work_mem.")
    parser.add_argument("--restart", action="store_true", help="Drop and recreate the isolated target table.")
    parser.add_argument("--dry-run", action="store_true", help="Create no table; report source counts only.")
    return parser.parse_args()


def connect():
    load_dotenv(".env")
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "cnpj"), user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""), host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )


def setup_target(cur, conn, restart: bool) -> None:
    if restart:
        cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE} CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {CHECKPOINT_TABLE}")
        conn.commit()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} AS TABLE {SOURCE_TABLE} WITH NO DATA")
    cur.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
    if cur.fetchone()[0] == 0:
        print("[COPY] Copying the validated October state into the isolated target...", flush=True)
        cur.execute(f"INSERT INTO {TARGET_TABLE} SELECT * FROM {SOURCE_TABLE}")
        conn.commit()
    cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_batched_state_key ON {TARGET_TABLE} (cnpj_basico, cnpj_ordem, cnpj_dv)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_batched_state_cnpj ON {TARGET_TABLE} (cnpj_basico)")
    cur.execute(f"CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (bucket integer PRIMARY KEY, completed_at timestamp default current_timestamp)")
    conn.commit()


def setup_insert_staging(cur, conn, batches: int) -> None:
    cur.execute("DROP TABLE IF EXISTS tmp_november_establishment_inserts")
    cur.execute("""
        CREATE TEMP TABLE tmp_november_establishment_inserts AS
        SELECT
            s.cnpj_basico, s.cnpj_ordem, s.cnpj_dv,
            (s.conteudo_novo->>'identificador_matriz_filial')::int AS identificador_matriz_filial,
            (s.conteudo_novo->>'situacao_cadastral')::int AS situacao_cadastral,
            (s.conteudo_novo->>'data_situacao_cadastral')::int AS data_situacao_cadastral,
            (s.conteudo_novo->>'motivo_situacao_cadastral')::int AS motivo_situacao_cadastral,
            s.conteudo_novo->>'pais' AS pais,
            (s.conteudo_novo->>'data_inicio_atividade')::int AS data_inicio_atividade,
            (s.conteudo_novo->>'cnae_fiscal_principal')::int AS cnae_fiscal_principal,
            s.conteudo_novo->>'cnae_fiscal_secundaria' AS cnae_fiscal_secundaria,
            s.conteudo_novo->>'uf' AS uf,
            (s.conteudo_novo->>'municipio')::int AS municipio,
            s.conteudo_novo->>'situacao_especial' AS situacao_especial,
            (s.conteudo_novo->>'data_situacao_especial')::int AS data_situacao_especial,
            mod(abs(hashtext(s.cnpj_basico)), %s)::int AS bucket
        FROM public.snapshots s
        WHERE s.tabela = 'estabelecimento'
          AND s.mes_referencia = '2024-11'
          AND s.tipo_alteracao = 'INSERT'
    """, (batches,))
    cur.execute("CREATE INDEX tmp_november_insert_bucket_key ON tmp_november_establishment_inserts (bucket, cnpj_basico, cnpj_ordem, cnpj_dv)")
    cur.execute("ANALYZE tmp_november_establishment_inserts")
    conn.commit()


def apply_non_insert_deltas(cur, conn) -> None:
    statements = [
        f"""
        UPDATE {TARGET_TABLE} t SET
            identificador_matriz_filial=(s.conteudo_novo->>'identificador_matriz_filial')::int,
            situacao_cadastral=(s.conteudo_novo->>'situacao_cadastral')::int,
            data_situacao_cadastral=(s.conteudo_novo->>'data_situacao_cadastral')::int,
            motivo_situacao_cadastral=(s.conteudo_novo->>'motivo_situacao_cadastral')::int,
            pais=s.conteudo_novo->>'pais', data_inicio_atividade=(s.conteudo_novo->>'data_inicio_atividade')::int,
            cnae_fiscal_principal=(s.conteudo_novo->>'cnae_fiscal_principal')::int,
            cnae_fiscal_secundaria=s.conteudo_novo->>'cnae_fiscal_secundaria', uf=s.conteudo_novo->>'uf',
            municipio=(s.conteudo_novo->>'municipio')::int, situacao_especial=s.conteudo_novo->>'situacao_especial',
            data_situacao_especial=(s.conteudo_novo->>'data_situacao_especial')::int
        FROM public.snapshots s WHERE s.tabela='estabelecimento' AND s.mes_referencia='2024-11' AND s.tipo_alteracao='UPDATE'
          AND t.cnpj_basico=s.cnpj_basico AND t.cnpj_ordem=s.cnpj_ordem AND t.cnpj_dv=s.cnpj_dv
        """,
        f"""
        UPDATE {TARGET_TABLE} t SET capital_social=(s.conteudo_novo->>'capital_social')::double precision,
          natureza_juridica=(s.conteudo_novo->>'natureza_juridica')::int, porte_empresa=(s.conteudo_novo->>'porte_empresa')::int,
          qualificacao_responsavel=(s.conteudo_novo->>'qualificacao_responsavel')::int,
          ente_federativo_responsavel=s.conteudo_novo->>'ente_federativo_responsavel'
        FROM public.snapshots s WHERE s.tabela='empresa' AND s.mes_referencia='2024-11' AND s.tipo_alteracao IN ('INSERT','UPDATE')
          AND t.cnpj_basico=s.cnpj_basico
        """,
        f"""
        UPDATE {TARGET_TABLE} t SET opcao_pelo_simples=s.conteudo_novo->>'opcao_pelo_simples',
          data_opcao_simples=(s.conteudo_novo->>'data_opcao_simples')::int, data_exclusao_simples=(s.conteudo_novo->>'data_exclusao_simples')::int,
          opcao_mei=s.conteudo_novo->>'opcao_mei', data_opcao_mei=(s.conteudo_novo->>'data_opcao_mei')::int,
          data_exclusao_mei=(s.conteudo_novo->>'data_exclusao_mei')::int
        FROM public.snapshots s WHERE s.tabela='simples' AND s.mes_referencia='2024-11' AND s.tipo_alteracao IN ('INSERT','UPDATE')
          AND t.cnpj_basico=s.cnpj_basico
        """,
        f"""
        UPDATE {TARGET_TABLE} t SET qtde_socios=(s.conteudo_novo->>'qtde_socios')::int,
          qtde_socios_pf=(s.conteudo_novo->>'qtde_socios_pf')::int, qtde_socios_pj=(s.conteudo_novo->>'qtde_socios_pj')::int,
          qtde_socios_estrangeiro=(s.conteudo_novo->>'qtde_socios_estrangeiro')::int,
          min_faixa_etaria=(s.conteudo_novo->>'min_faixa_etaria')::int, max_faixa_etaria=(s.conteudo_novo->>'max_faixa_etaria')::int,
          data_entrada_antiga=(s.conteudo_novo->>'data_entrada_antiga')::int, data_entrada_recente=(s.conteudo_novo->>'data_entrada_recente')::int,
          qtde_administradores=(s.conteudo_novo->>'qtde_administradores')::int
        FROM public.snapshots s WHERE s.tabela='socios' AND s.mes_referencia='2024-11' AND s.tipo_alteracao IN ('INSERT','UPDATE')
          AND t.cnpj_basico=s.cnpj_basico
        """,
    ]
    for index, statement in enumerate(statements, 1):
        print(f"[DELTA] Applying non-insert statement {index}/{len(statements)}...", flush=True)
        cur.execute(statement)
        conn.commit()


def insert_bucket(cur, conn, bucket: int) -> int:
    cur.execute(f"""
        INSERT INTO {TARGET_TABLE} (
          cnpj_basico,cnpj_ordem,cnpj_dv,identificador_matriz_filial,situacao_cadastral,data_situacao_cadastral,
          motivo_situacao_cadastral,pais,data_inicio_atividade,cnae_fiscal_principal,cnae_fiscal_secundaria,uf,municipio,
          situacao_especial,data_situacao_especial,capital_social,natureza_juridica,porte_empresa,qualificacao_responsavel,
          ente_federativo_responsavel,opcao_pelo_simples,data_opcao_simples,data_exclusao_simples,opcao_mei,data_opcao_mei,
          data_exclusao_mei,qtde_socios,qtde_socios_pf,qtde_socios_pj,qtde_socios_estrangeiro,min_faixa_etaria,max_faixa_etaria,
          data_entrada_antiga,data_entrada_recente,qtde_administradores,cd_mun)
        SELECT s.cnpj_basico,s.cnpj_ordem,s.cnpj_dv,s.identificador_matriz_filial,s.situacao_cadastral,s.data_situacao_cadastral,
          s.motivo_situacao_cadastral,s.pais,s.data_inicio_atividade,s.cnae_fiscal_principal,s.cnae_fiscal_secundaria,s.uf,s.municipio,
          s.situacao_especial,s.data_situacao_especial,COALESCE(c.capital_social,e.capital_social),COALESCE(c.natureza_juridica,e.natureza_juridica),
          COALESCE(c.porte_empresa,e.porte_empresa),COALESCE(c.qualificacao_responsavel,e.qualificacao_responsavel),COALESCE(c.ente_federativo_responsavel,e.ente_federativo_responsavel),
          smp.opcao_pelo_simples,smp.data_opcao_simples,smp.data_exclusao_simples,smp.opcao_mei,smp.data_opcao_mei,smp.data_exclusao_mei,
          COALESCE(soc.qtde_socios,0),COALESCE(soc.qtde_socios_pf,0),COALESCE(soc.qtde_socios_pj,0),COALESCE(soc.qtde_socios_estrangeiro,0),
          soc.min_faixa_etaria,soc.max_faixa_etaria,soc.data_entrada_antiga,soc.data_entrada_recente,COALESCE(soc.qtde_administradores,0),m.cd_mun
        FROM tmp_november_establishment_inserts s
        LEFT JOIN LATERAL (SELECT capital_social,natureza_juridica,porte_empresa,qualificacao_responsavel,ente_federativo_responsavel
          FROM {TARGET_TABLE} c0 WHERE c0.cnpj_basico=s.cnpj_basico LIMIT 1) c ON true
        LEFT JOIN public.empresa e ON e.cnpj_basico=s.cnpj_basico
        LEFT JOIN public.simples smp ON smp.cnpj_basico=s.cnpj_basico
        LEFT JOIN public.munic m ON m.codigo=s.municipio
        LEFT JOIN (SELECT so.cnpj_basico,COUNT(*)::int qtde_socios,
          COUNT(CASE WHEN identificador_socio=2 THEN 1 END)::int qtde_socios_pf,
          COUNT(CASE WHEN identificador_socio=1 THEN 1 END)::int qtde_socios_pj,
          COUNT(CASE WHEN identificador_socio=3 THEN 1 END)::int qtde_socios_estrangeiro,
          MIN(NULLIF(faixa_etaria,0))::int min_faixa_etaria,MAX(NULLIF(faixa_etaria,0))::int max_faixa_etaria,
          MIN(NULLIF(data_entrada_sociedade,0))::int data_entrada_antiga,MAX(NULLIF(data_entrada_sociedade,0))::int data_entrada_recente,
          COUNT(CASE WHEN qualificacao_socio IN (5,10,16,49) THEN 1 END)::int qtde_administradores
          FROM public.socios so JOIN (SELECT DISTINCT cnpj_basico FROM tmp_november_establishment_inserts WHERE bucket=%s) k USING(cnpj_basico)
          GROUP BY so.cnpj_basico) soc ON soc.cnpj_basico=s.cnpj_basico
        WHERE s.bucket=%s
        ON CONFLICT (cnpj_basico,cnpj_ordem,cnpj_dv) DO NOTHING
    """, (bucket, bucket))
    inserted = cur.rowcount
    conn.commit()
    return inserted


def main() -> int:
    args = parse_args()
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f"SET work_mem = '{args.work_mem}'")
        cur.execute("SET synchronous_commit = on")
        conn.commit()
        cur.execute("SELECT count(*) FROM public.snapshots WHERE tabela='estabelecimento' AND mes_referencia='2024-11' AND tipo_alteracao='INSERT'")
        expected = cur.fetchone()[0]
        print(f"November establishment inserts: {expected:,}", flush=True)
        if args.dry_run:
            return 0
        setup_target(cur, conn, args.restart)
        setup_insert_staging(cur, conn, args.batches)
        apply_non_insert_deltas(cur, conn)
        cur.execute(f"SELECT bucket FROM {CHECKPOINT_TABLE}")
        completed = {row[0] for row in cur.fetchall()}
        total_inserted = 0
        for bucket in range(args.batches):
            if bucket in completed:
                print(f"[RESUME] Bucket {bucket + 1}/{args.batches} already complete", flush=True)
                continue
            started = time.time()
            inserted = insert_bucket(cur, conn, bucket)
            cur.execute(f"INSERT INTO {CHECKPOINT_TABLE}(bucket) VALUES(%s) ON CONFLICT DO NOTHING", (bucket,))
            conn.commit()
            total_inserted += inserted
            print(f"[BATCH] Bucket {bucket + 1}/{args.batches}: inserted={inserted:,} elapsed={time.time()-started:.1f}s", flush=True)
        cur.execute(f"ANALYZE {TARGET_TABLE}")
        conn.commit()
        cur.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
        final_rows = cur.fetchone()[0]
        print(json.dumps({"target_table": TARGET_TABLE, "expected_inserts": expected, "final_rows": final_rows, "completed_buckets": args.batches}, indent=2))
    finally:
        cur.close()
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
