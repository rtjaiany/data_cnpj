"""Compare establishment keys from alternative ZIP archives and local Parquet files.

The comparison is external-memory based: keys are written to temporary text files,
then sorted and deduplicated by the operating system. No production data or source
archives are modified.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare CNPJ establishment keys using external sorting."
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=["2024-10", "2024-11"],
        help="Reference months to compare (default: 2024-10 2024-11).",
    )
    parser.add_argument(
        "--source-root",
        default="audit_source_verification/downloads",
        help="Directory containing source ZIPs grouped by source directory.",
    )
    parser.add_argument(
        "--reference-root",
        default="ignored_fields/estabelecimento",
        help="Directory containing local reference Parquets.",
    )
    parser.add_argument(
        "--work-root",
        default="audit_source_verification/key_compare",
        help="Isolated temporary and result directory.",
    )
    return parser.parse_args()


def source_directory(source_root: Path, month: str) -> Path:
    matches = sorted(source_root.glob(f"{month}-*/"))
    if len(matches) != 1:
        raise RuntimeError(f"Expected one source directory for {month}, found {matches}")
    return matches[0]


def write_source_keys(source_dir: Path, output: Path) -> int:
    rows = 0
    with output.open("w", encoding="ascii", newline="") as target:
        writer = target.write
        for archive_path in sorted(source_dir.glob("Estabelecimentos*.zip")):
            with zipfile.ZipFile(archive_path) as archive:
                members = archive.infolist()
                if len(members) != 1:
                    raise RuntimeError(f"Expected one CSV member in {archive_path}")
                with archive.open(members[0]) as binary_member:
                    text_member = (line.decode("latin-1") for line in binary_member)
                    for fields in csv.reader(text_member, delimiter=";", quotechar='"'):
                        if len(fields) < 3:
                            raise ValueError(f"Malformed row in {archive_path}: {fields!r}")
                        writer(f"{fields[0]}|{fields[1]}|{fields[2]}\n")
                        rows += 1
            print(f"Wrote source keys from {archive_path.name}: {rows:,} total")
    return rows


def write_reference_keys(reference_path: Path, output: Path) -> int:
    rows = 0
    with output.open("w", encoding="ascii", newline="") as target:
        writer = target.write
        parquet = pq.ParquetFile(reference_path)
        for batch in parquet.iter_batches(
            batch_size=250_000, columns=["cnpj_basico", "cnpj_ordem", "cnpj_dv"]
        ):
            columns = [batch.column(index).to_pylist() for index in range(3)]
            for key_parts in zip(*columns):
                writer(f"{key_parts[0]}|{key_parts[1]}|{key_parts[2]}\n")
                rows += 1
    return rows


def sort_unique(input_path: Path, output_path: Path) -> None:
    with output_path.open("w", encoding="ascii") as output:
        subprocess.run(
            ["sort", "-u", str(input_path)],
            check=True,
            stdout=output,
        )


def line_count(path: Path) -> int:
    with path.open("rb") as source:
        return sum(1 for _ in source)


def compare_month(month: str, source_root: Path, reference_root: Path, work_root: Path) -> dict[str, object]:
    source_dir = source_directory(source_root, month)
    reference_path = reference_root / f"reference_month={month}" / "part-000.parquet"
    if not reference_path.exists():
        raise FileNotFoundError(reference_path)

    month_dir = work_root / month
    month_dir.mkdir(parents=True, exist_ok=True)
    source_raw = month_dir / "source.keys"
    reference_raw = month_dir / "reference.keys"
    source_unique = month_dir / "source.unique.keys"
    reference_unique = month_dir / "reference.unique.keys"
    source_only = month_dir / "source_only.keys"
    reference_only = month_dir / "reference_only.keys"

    source_rows = write_source_keys(source_dir, source_raw)
    reference_rows = write_reference_keys(reference_path, reference_raw)
    sort_unique(source_raw, source_unique)
    sort_unique(reference_raw, reference_unique)

    with source_only.open("w", encoding="ascii") as output:
        subprocess.run(["comm", "-23", str(source_unique), str(reference_unique)], check=True, stdout=output)
    with reference_only.open("w", encoding="ascii") as output:
        subprocess.run(["comm", "-13", str(source_unique), str(reference_unique)], check=True, stdout=output)

    source_unique_rows = line_count(source_unique)
    reference_unique_rows = line_count(reference_unique)
    source_only_rows = line_count(source_only)
    reference_only_rows = line_count(reference_only)
    result = {
        "month": month,
        "source_directory": str(source_dir),
        "source_rows": source_rows,
        "reference_rows": reference_rows,
        "source_unique_keys": source_unique_rows,
        "reference_unique_keys": reference_unique_rows,
        "source_only_keys": source_only_rows,
        "reference_only_keys": reference_only_rows,
        "status": "MATCH" if source_only_rows == 0 and reference_only_rows == 0 else "MISMATCH",
        "source_only_file": str(source_only),
        "reference_only_file": str(reference_only),
    }
    return result


def main() -> int:
    args = parse_args()
    source_root = Path(args.source_root).resolve()
    reference_root = Path(args.reference_root).resolve()
    work_root = Path(args.work_root).resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    results = []
    for month in args.months:
        print(f"\nComparing establishment keys for {month}")
        result = compare_month(month, source_root, reference_root, work_root)
        results.append(result)
        print(json.dumps(result, indent=2))
    report_path = work_root / "key_comparison_report.json"
    report_path.write_text(json.dumps({"months": results}, indent=2), encoding="utf-8")
    print(f"\nReport written to {report_path}")
    return 0 if all(result["status"] == "MATCH" for result in results) else 2


if __name__ == "__main__":
    sys.exit(main())
