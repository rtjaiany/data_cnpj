"""Export the isolated canonical November state for Sao Paulo.

The output is written atomically to a new directory and does not modify existing
Parquet partitions or the original reconstruction pipeline.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from reconstruct_and_stream_panel import get_db_connection, schema_panel

TARGET_TABLE = "analytics.panel_checkpoint_state_canonical_2024_11"
OUTPUT_FILE = Path("reconstructed_panel_canonical/reference_month=2024-11/part-000.parquet")


def main() -> int:
    output_file = OUTPUT_FILE.resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = output_file.with_suffix(".parquet.tmp")
    if temporary_file.exists():
        temporary_file.unlink()
    conn = get_db_connection(Path(__file__).parent.parent)
    writer = None
    row_count = 0
    started = time.time()
    query = f"""
        SELECT cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, situacao_cadastral,
          data_situacao_cadastral, motivo_situacao_cadastral, pais, data_inicio_atividade,
          cnae_fiscal_principal, cnae_fiscal_secundaria, uf, municipio, situacao_especial,
          data_situacao_especial, capital_social, natureza_juridica, porte_empresa,
          qualificacao_responsavel, ente_federativo_responsavel, opcao_pelo_simples,
          data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei,
          data_exclusao_mei, qtde_socios, qtde_socios_pf, qtde_socios_pj,
          qtde_socios_estrangeiro, min_faixa_etaria, max_faixa_etaria,
          data_entrada_antiga, data_entrada_recente, qtde_administradores, cd_mun
        FROM {TARGET_TABLE}
        WHERE uf = 'SP'
        ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv
    """
    try:
        with conn.cursor(name="canonical_november_sp_export") as cursor:
            cursor.itersize = 100_000
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(100_000)
                if not rows:
                    break
                columns = {name: [row[index] for row in rows] for index, name in enumerate(schema_panel.names)}
                batch = pa.RecordBatch.from_pydict(columns, schema=schema_panel)
                if writer is None:
                    writer = pq.ParquetWriter(str(temporary_file), schema_panel, compression="snappy")
                writer.write_batch(batch)
                row_count += len(rows)
                print(f"[EXPORT] {row_count:,} rows", flush=True)
    finally:
        if writer is not None:
            writer.close()
        conn.close()
    os.replace(temporary_file, output_file)
    metadata = pq.ParquetFile(output_file).metadata
    print({"output": str(output_file), "rows": row_count, "row_groups": metadata.num_row_groups, "bytes": output_file.stat().st_size, "elapsed_seconds": round(time.time() - started, 1)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
