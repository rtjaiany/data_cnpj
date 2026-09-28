"""Build the tagged sensitivity panel for canonical November Sao Paulo data.

This creates a separate November sensitivity Parquet with explicit source-gap
flags. It does not modify the canonical November output or production tables.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from reconstruct_and_stream_panel import get_db_connection, schema_panel

TARGET_TABLE = "analytics.panel_checkpoint_state_canonical_2024_11"
TAGGED_CANDIDATES = Path("audit_source_verification/official_disappearances/disappeared_establishments_tagged.tsv")
OUTPUT = Path("reconstructed_panel_sensitivity/reference_month=2024-11/part-000.parquet")
SENSITIVITY_SCHEMA = pa.schema(list(schema_panel) + [
    pa.field("source_gap_candidate", pa.bool_()),
    pa.field("source_gap_basis", pa.string()),
])


def add_tags(table: pa.Table, candidate: list[bool], basis: list[str | None]) -> pa.Table:
    return table.append_column("source_gap_candidate", pa.array(candidate, type=pa.bool_())).append_column(
        "source_gap_basis", pa.array(basis, type=pa.string())
    )


def main() -> int:
    output = OUTPUT.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".parquet.tmp")
    if temporary.exists():
        temporary.unlink()
    conn = get_db_connection(Path(__file__).parent.parent)
    cur = conn.cursor()
    writer = None
    rows_written = 0
    try:
        cur.execute("""
            CREATE TEMP TABLE sensitivity_candidates (
                cnpj_basico text, cnpj_ordem text, cnpj_dv text,
                opening_date text, september_status text, tag text
            ) ON COMMIT PRESERVE ROWS
        """)
        with TAGGED_CANDIDATES.open("r", encoding="utf-8") as source:
            cur.copy_expert("COPY sensitivity_candidates FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', HEADER true)", source)
        cur.execute("CREATE INDEX sensitivity_candidates_key_nov ON sensitivity_candidates(cnpj_basico,cnpj_ordem,cnpj_dv)")
        cur.execute("ANALYZE sensitivity_candidates")
        conn.commit()

        query = f"""
            SELECT t.cnpj_basico, t.cnpj_ordem, t.cnpj_dv, t.identificador_matriz_filial, t.situacao_cadastral,
              t.data_situacao_cadastral, t.motivo_situacao_cadastral, t.pais, t.data_inicio_atividade,
              t.cnae_fiscal_principal, t.cnae_fiscal_secundaria, t.uf, t.municipio, t.situacao_especial,
              t.data_situacao_especial, t.capital_social, t.natureza_juridica, t.porte_empresa,
              t.qualificacao_responsavel, t.ente_federativo_responsavel, t.opcao_pelo_simples,
              t.data_opcao_simples, t.data_exclusao_simples, t.opcao_mei, t.data_opcao_mei,
              t.data_exclusao_mei, t.qtde_socios, t.qtde_socios_pf, t.qtde_socios_pj,
              t.qtde_socios_estrangeiro, t.min_faixa_etaria, t.max_faixa_etaria,
              t.data_entrada_antiga, t.data_entrada_recente, t.qtde_administradores, t.cd_mun,
              (c.tag = 'pre_existing_active_in_september') AS source_gap_candidate,
              CASE WHEN c.tag = 'pre_existing_active_in_september' THEN 'active_sep_reappeared_nov' END AS source_gap_basis
            FROM {TARGET_TABLE} t
            LEFT JOIN sensitivity_candidates c USING (cnpj_basico, cnpj_ordem, cnpj_dv)
            WHERE t.uf = 'SP'
            ORDER BY t.cnpj_basico, t.cnpj_ordem, t.cnpj_dv
        """
        with conn.cursor(name="sensitivity_november_sp_export") as cursor:
            cursor.itersize = 100_000
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(100_000)
                if not rows:
                    break
                base = rows[0]
                data = {name: [row[index] for row in rows] for index, name in enumerate(schema_panel.names)}
                tags = [bool(row[36]) for row in rows]
                bases = [row[37] for row in rows]
                table = add_tags(pa.Table.from_pydict(data, schema=schema_panel), tags, bases)
                if writer is None:
                    writer = pq.ParquetWriter(str(temporary), SENSITIVITY_SCHEMA, compression="snappy")
                writer.write_table(table)
                rows_written += len(rows)
                print(f"[SENSITIVITY] November SP rows: {rows_written:,}", flush=True)
        conn.commit()
    finally:
        if writer is not None:
            writer.close()
        cur.close()
        conn.close()
    os.replace(temporary, output)
    print({"output": str(output), "rows": rows_written, "columns": len(SENSITIVITY_SCHEMA), "bytes": output.stat().st_size})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
