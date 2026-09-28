"""Build an explicitly tagged sensitivity panel for October 2024, Sao Paulo.

The canonical October Parquet is copied to a new output with two audit columns.
Candidates that were active in September, disappeared in October, and reappeared
in November are appended using their observed November state. This is an
imputation for sensitivity analysis only and never modifies canonical outputs.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from reconstruct_and_stream_panel import get_db_connection, schema_panel

CANONICAL_OCTOBER = Path("reconstructed_panel/reference_month=2024-10/part-000.parquet")
OUTPUT = Path("reconstructed_panel_sensitivity/reference_month=2024-10/part-000.parquet")
TAGGED_CANDIDATES = Path("audit_source_verification/official_disappearances/disappeared_establishments_tagged.tsv")
TARGET_TABLE = "analytics.panel_checkpoint_state_canonical_2024_11"

SENSITIVITY_FIELDS = [
    pa.field("source_gap_candidate", pa.bool_()),
    pa.field("source_gap_basis", pa.string()),
]
SENSITIVITY_SCHEMA = pa.schema(list(schema_panel) + SENSITIVITY_FIELDS)


def add_tags(table: pa.Table, candidate: bool, basis: str | None) -> pa.Table:
    count = table.num_rows
    table = table.append_column("source_gap_candidate", pa.array([candidate] * count, type=pa.bool_()))
    table = table.append_column("source_gap_basis", pa.array([basis] * count, type=pa.string()))
    return table


def main() -> int:
    output = OUTPUT.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".parquet.tmp")
    if temporary.exists():
        temporary.unlink()
    if not CANONICAL_OCTOBER.exists():
        raise FileNotFoundError(CANONICAL_OCTOBER)
    if not TAGGED_CANDIDATES.exists():
        raise FileNotFoundError(TAGGED_CANDIDATES)

    conn = get_db_connection(Path(__file__).parent.parent)
    cur = conn.cursor()
    writer = None
    canonical_rows = 0
    candidate_rows = 0
    try:
        cur.execute("""
            CREATE TEMP TABLE sensitivity_candidates (
                cnpj_basico text, cnpj_ordem text, cnpj_dv text,
                opening_date text, september_status text, tag text
            ) ON COMMIT PRESERVE ROWS
        """)
        with TAGGED_CANDIDATES.open("r", encoding="utf-8") as source:
            cur.copy_expert("COPY sensitivity_candidates FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER true)", source)
        cur.execute("CREATE INDEX sensitivity_candidates_key ON sensitivity_candidates(cnpj_basico,cnpj_ordem,cnpj_dv)")
        cur.execute("ANALYZE sensitivity_candidates")
        conn.commit()

        with pq.ParquetFile(CANONICAL_OCTOBER) as parquet:
            for batch in parquet.iter_batches(batch_size=100_000):
                table = add_tags(pa.Table.from_batches([batch]), False, None)
                if writer is None:
                    writer = pq.ParquetWriter(str(temporary), SENSITIVITY_SCHEMA, compression="snappy")
                writer.write_table(table)
                canonical_rows += batch.num_rows
                print(f"[SENSITIVITY] Canonical October rows: {canonical_rows:,}", flush=True)

        query = f"""
            SELECT t.cnpj_basico, t.cnpj_ordem, t.cnpj_dv, t.identificador_matriz_filial, t.situacao_cadastral,
              t.data_situacao_cadastral, t.motivo_situacao_cadastral, t.pais, t.data_inicio_atividade,
              t.cnae_fiscal_principal, t.cnae_fiscal_secundaria, t.uf, t.municipio, t.situacao_especial,
              t.data_situacao_especial, t.capital_social, t.natureza_juridica, t.porte_empresa,
              t.qualificacao_responsavel, t.ente_federativo_responsavel, t.opcao_pelo_simples,
              t.data_opcao_simples, t.data_exclusao_simples, t.opcao_mei, t.data_opcao_mei,
              t.data_exclusao_mei, t.qtde_socios, t.qtde_socios_pf, t.qtde_socios_pj,
              t.qtde_socios_estrangeiro, t.min_faixa_etaria, t.max_faixa_etaria,
              t.data_entrada_antiga, t.data_entrada_recente, t.qtde_administradores, t.cd_mun
            FROM {TARGET_TABLE} t
            JOIN sensitivity_candidates c USING (cnpj_basico, cnpj_ordem, cnpj_dv)
            WHERE t.uf = 'SP' AND c.tag = 'pre_existing_active_in_september'
            ORDER BY t.cnpj_basico, t.cnpj_ordem, t.cnpj_dv
        """
        with conn.cursor(name="sensitivity_candidate_export") as cursor:
            cursor.itersize = 100_000
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(100_000)
                if not rows:
                    break
                columns = {name: [row[index] for row in rows] for index, name in enumerate(schema_panel.names)}
                table = add_tags(pa.Table.from_pydict(columns, schema=schema_panel), True, "active_sep_reappeared_nov")
                if writer is None:
                    writer = pq.ParquetWriter(str(temporary), SENSITIVITY_SCHEMA, compression="snappy")
                writer.write_table(table)
                candidate_rows += len(rows)
                print(f"[SENSITIVITY] Imputed candidate rows: {candidate_rows:,}", flush=True)
        conn.commit()
    finally:
        if writer is not None:
            writer.close()
        cur.close()
        conn.close()
    os.replace(temporary, output)
    print({"output": str(output), "canonical_rows": canonical_rows, "candidate_rows": candidate_rows, "total_rows": canonical_rows + candidate_rows, "columns": len(SENSITIVITY_SCHEMA)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
