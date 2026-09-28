"""Compare official establishment key sets across September-November 2024.

This read-only analysis uses keys extracted from official Receita Federal archives
and compares them with local derived Parquet partitions. It never writes to
PostgreSQL, source archives, or existing Parquet files.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare official establishment transitions by key.")
    parser.add_argument("--months", nargs="+", default=["2024-09", "2024-10", "2024-11"])
    parser.add_argument("--official-key-root", default="audit_source_verification/official_recovery/keys")
    parser.add_argument("--reference-root", default="ignored_fields/estabelecimento")
    parser.add_argument("--work-root", default="audit_source_verification/official_transition")
    return parser.parse_args()


def count_lines(path: Path) -> int:
    with path.open("rb") as source:
        return sum(1 for _ in source)


def merge_official_keys(month: str, key_root: Path, output: Path) -> None:
    inputs = sorted((key_root / month).glob("*.keys"))
    if len(inputs) != 10:
        raise RuntimeError(f"Expected 10 extracted key files for {month}, found {len(inputs)}")
    with output.open("w", encoding="ascii") as merged:
        subprocess.run(["sort", "-u", *map(str, inputs)], check=True, stdout=merged)


def extract_reference_keys(month: str, reference_root: Path, output: Path) -> None:
    reference_path = reference_root / f"reference_month={month}" / "part-000.parquet"
    if not reference_path.exists():
        raise FileNotFoundError(reference_path)
    raw_path = output.with_suffix(".raw")
    with raw_path.open("w", encoding="ascii") as raw:
        for batch in pq.ParquetFile(reference_path).iter_batches(
            batch_size=250_000, columns=["cnpj_basico", "cnpj_ordem", "cnpj_dv"]
        ):
            columns = [batch.column(index).to_pylist() for index in range(3)]
            for key in zip(*columns):
                raw.write(f"{key[0]}|{key[1]}|{key[2]}\n")
    with output.open("w", encoding="ascii") as sorted_output:
        subprocess.run(["sort", "-u", str(raw_path)], check=True, stdout=sorted_output)
    raw_path.unlink()


def compare_sets(left: Path, right: Path, output: Path, command: str) -> int:
    with output.open("w", encoding="ascii") as result:
        subprocess.run(["comm", command, str(left), str(right)], check=True, stdout=result)
    return count_lines(output)


def main() -> int:
    args = parse_args()
    key_root = Path(args.official_key_root).resolve()
    reference_root = Path(args.reference_root).resolve()
    work_root = Path(args.work_root).resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    official_sets: dict[str, Path] = {}
    reference_sets: dict[str, Path] = {}
    results: dict[str, object] = {"months": {}, "transitions": {}}

    for month in args.months:
        official_path = work_root / f"official_{month}.keys"
        reference_path = work_root / f"reference_{month}.keys"
        merge_official_keys(month, key_root, official_path)
        extract_reference_keys(month, reference_root, reference_path)
        source_only = work_root / f"official_only_{month}.keys"
        reference_only = work_root / f"reference_only_{month}.keys"
        source_only_count = compare_sets(official_path, reference_path, source_only, "-23")
        reference_only_count = compare_sets(official_path, reference_path, reference_only, "-13")
        official_sets[month] = official_path
        reference_sets[month] = reference_path
        results["months"][month] = {
            "official_unique_keys": count_lines(official_path),
            "reference_unique_keys": count_lines(reference_path),
            "official_only_keys": source_only_count,
            "reference_only_keys": reference_only_count,
            "status": "MATCH" if source_only_count == 0 and reference_only_count == 0 else "MISMATCH",
        }
        print(f"{month}: official={count_lines(official_path):,} reference={count_lines(reference_path):,} official_only={source_only_count:,} reference_only={reference_only_count:,}")

    for previous, current in zip(args.months, args.months[1:]):
        only_current = work_root / f"transition_{previous}_to_{current}_only_current.keys"
        only_previous = work_root / f"transition_{previous}_to_{current}_only_previous.keys"
        current_only_count = compare_sets(official_sets[previous], official_sets[current], only_current, "-13")
        previous_only_count = compare_sets(official_sets[previous], official_sets[current], only_previous, "-23")
        results["transitions"][f"{previous}_to_{current}"] = {
            "previous_only_keys": previous_only_count,
            "current_only_keys": current_only_count,
            "shared_keys": count_lines(official_sets[current]) - current_only_count,
        }
        print(f"transition {previous}->{current}: previous_only={previous_only_count:,} current_only={current_only_count:,}")

    report_path = work_root / "official_transition_report.json"
    report_path.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
