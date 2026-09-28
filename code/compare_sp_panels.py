"""Compare canonical and sensitivity São Paulo panel partitions for 2024-10/11."""

from __future__ import annotations

import json
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
    columns = parquet.schema_arrow.names
    keys = parquet.read(columns=["cnpj_basico", "cnpj_ordem", "cnpj_dv"])
    key_values = list(zip(*(keys[name].to_pylist() for name in ("cnpj_basico", "cnpj_ordem", "cnpj_dv"))))
    result = {
        "path": str(path),
        "rows": parquet.metadata.num_rows,
        "unique_keys": len(set(key_values)),
        "columns": len(columns),
        "bytes": path.stat().st_size,
        "status": "PASS" if parquet.metadata.num_rows == len(set(key_values)) else "FAIL",
    }
    if "source_gap_candidate" in columns:
        flags = parquet.read(columns=["source_gap_candidate", "source_gap_basis"])
        candidates = flags["source_gap_candidate"].to_pylist()
        bases = flags["source_gap_basis"].to_pylist()
        result["candidate_rows"] = sum(candidates)
        result["basis_counts"] = {value: bases.count(value) for value in set(bases)}
    else:
        result["candidate_rows"] = 0
    return result


def main() -> int:
    summaries = {name: summarize(path) for name, path in PARTITIONS.items()}
    comparison = {
        "october_added_sensitivity_rows": summaries["sensitivity_2024-10"]["rows"] - summaries["canonical_2024-10"]["rows"],
        "november_sensitivity_flagged_rows": summaries["sensitivity_2024-11"]["candidate_rows"],
        "canonical_row_change_october_to_november": summaries["canonical_2024-11"]["rows"] - summaries["canonical_2024-10"]["rows"],
    }
    report = {"partitions": summaries, "comparison": comparison}
    output = Path("audit_source_verification/sp_panel_comparison.json")
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {output}")
    return 0 if all(item["status"] == "PASS" for item in summaries.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
