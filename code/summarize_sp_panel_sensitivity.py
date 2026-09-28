"""Summarize canonical versus sensitivity Sao Paulo panel partitions."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

PARTITIONS = {
    "canonical_2024-10": Path("reconstructed_panel/reference_month=2024-10/part-000.parquet"),
    "sensitivity_2024-10": Path("reconstructed_panel_sensitivity/reference_month=2024-10/part-000.parquet"),
    "canonical_2024-11": Path("reconstructed_panel_canonical/reference_month=2024-11/part-000.parquet"),
    "sensitivity_2024-11": Path("reconstructed_panel_sensitivity/reference_month=2024-11/part-000.parquet"),
}


def summarize(path: Path) -> dict[str, object]:
    parquet = pq.ParquetFile(path)
    columns = ["situacao_cadastral", "municipio", "cnae_fiscal_principal"]
    has_tags = "source_gap_candidate" in parquet.schema_arrow.names
    if has_tags:
        columns.append("source_gap_candidate")
    status = Counter()
    municipalities = Counter()
    cnaes = Counter()
    candidates = 0
    rows = 0
    for batch in parquet.iter_batches(batch_size=250_000, columns=columns):
        rows += batch.num_rows
        status.update(str(value) for value in batch.column("situacao_cadastral").to_pylist())
        municipalities.update(str(value) for value in batch.column("municipio").to_pylist() if value is not None)
        cnaes.update(str(value) for value in batch.column("cnae_fiscal_principal").to_pylist() if value is not None)
        if has_tags:
            candidates += sum(bool(value) for value in batch.column("source_gap_candidate").to_pylist())
    return {
        "rows": rows,
        "candidate_rows": candidates,
        "status_counts": dict(sorted(status.items())),
        "distinct_municipios": len(municipalities),
        "distinct_cnaes": len(cnaes),
        "top_municipios": municipalities.most_common(10),
        "top_cnaes": cnaes.most_common(10),
    }


def main() -> int:
    summaries = {name: summarize(path) for name, path in PARTITIONS.items()}
    comparisons = {}
    for month in ("2024-10", "2024-11"):
        canonical = summaries[f"canonical_{month}"]
        sensitivity = summaries[f"sensitivity_{month}"]
        status_delta = {
            code: sensitivity["status_counts"].get(code, 0) - canonical["status_counts"].get(code, 0)
            for code in set(canonical["status_counts"]) | set(sensitivity["status_counts"])
        }
        comparisons[month] = {
            "added_rows": sensitivity["rows"] - canonical["rows"],
            "candidate_rows": sensitivity["candidate_rows"],
            "distinct_municipio_delta": sensitivity["distinct_municipios"] - canonical["distinct_municipios"],
            "distinct_cnae_delta": sensitivity["distinct_cnaes"] - canonical["distinct_cnaes"],
            "status_count_delta": dict(sorted(status_delta.items())),
        }
    report = {"partitions": summaries, "comparisons": comparisons}
    output = Path("audit_source_verification/sp_panel_sensitivity_summary.json")
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
