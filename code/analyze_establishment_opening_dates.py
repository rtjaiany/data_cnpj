"""Analyze establishment opening dates for source-only October keys.

This read-only audit checks whether establishments missing from the local October
Parquet have plausible opening dates. It does not modify source archives, local
Parquets, production tables, or the original ETL code.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import zipfile
from collections import Counter
from pathlib import Path

DATE_FIELD_INDEX = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze data_inicio_atividade for source-only establishment keys."
    )
    parser.add_argument(
        "--source-only",
        default="audit_source_verification/key_compare/2024-10/source_only.keys",
        help="Sorted establishment keys present in the alternative source but absent locally.",
    )
    parser.add_argument(
        "--source-root",
        default="audit_source_verification/downloads",
        help="Directory containing source ZIPs grouped by source directory.",
    )
    parser.add_argument(
        "--work-dir",
        default="audit_source_verification/opening_date_analysis",
        help="Isolated working and output directory.",
    )
    return parser.parse_args()


def source_directory(source_root: Path, month: str) -> Path:
    matches = sorted(source_root.glob(f"{month}-*/"))
    if len(matches) != 1:
        raise RuntimeError(f"Expected one source directory for {month}, found {matches}")
    return matches[0]


def write_source_key_dates(source_dir: Path, output: Path) -> int:
    rows = 0
    with output.open("w", encoding="ascii") as target:
        for archive_path in sorted(source_dir.glob("Estabelecimentos*.zip")):
            with zipfile.ZipFile(archive_path) as archive:
                members = archive.infolist()
                if len(members) != 1:
                    raise RuntimeError(f"Expected one CSV member in {archive_path}")
                with archive.open(members[0]) as binary_member:
                    text_member = (line.decode("latin-1") for line in binary_member)
                    for fields in csv.reader(text_member, delimiter=";", quotechar='"'):
                        if len(fields) <= DATE_FIELD_INDEX:
                            raise ValueError(f"Malformed row in {archive_path}")
                        key = "|".join(fields[:3])
                        opening_date = fields[DATE_FIELD_INDEX]
                        target.write(f"{key}\t{opening_date}\n")
                        rows += 1
            print(f"Extracted opening dates from {archive_path.name}: {rows:,} total")
    return rows


def sort_key_dates(input_path: Path, output_path: Path) -> None:
    with output_path.open("w", encoding="ascii") as target:
        subprocess.run(
            ["sort", "-t", "\t", "-k1,1", str(input_path)],
            check=True,
            stdout=target,
        )


def join_source_only_dates(source_only: Path, key_dates: Path, output: Path) -> None:
    with output.open("w", encoding="ascii") as target:
        subprocess.run(
            ["join", "-t", "\t", "-1", "1", "-2", "1", str(source_only), str(key_dates)],
            check=True,
            stdout=target,
        )


def summarize_dates(path: Path) -> dict[str, object]:
    dates = Counter()
    rows = 0
    invalid = 0
    with path.open(encoding="ascii") as source:
        for line in source:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 2:
                invalid += 1
                continue
            dates[fields[1] or "NULL"] += 1
            rows += 1
    return {
        "matched_keys": rows,
        "invalid_join_rows": invalid,
        "distinct_opening_dates": len(dates),
        "date_counts": dict(sorted(dates.items())),
    }


def main() -> int:
    args = parse_args()
    source_only = Path(args.source_only).resolve()
    source_root = Path(args.source_root).resolve()
    work_dir = Path(args.work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    october_raw = work_dir / "october.key_dates"
    november_raw = work_dir / "november.key_dates"
    october_sorted = work_dir / "october.key_dates.sorted"
    november_sorted = work_dir / "november.key_dates.sorted"
    october_matches = work_dir / "october.source_only.opening_dates"
    november_matches = work_dir / "november.source_only.opening_dates"

    october_rows = write_source_key_dates(source_directory(source_root, "2024-10"), october_raw)
    november_rows = write_source_key_dates(source_directory(source_root, "2024-11"), november_raw)
    sort_key_dates(october_raw, october_sorted)
    sort_key_dates(november_raw, november_sorted)
    join_source_only_dates(source_only, october_sorted, october_matches)
    join_source_only_dates(source_only, november_sorted, november_matches)

    report = {
        "source_only_keys": sum(1 for _ in source_only.open("rb")),
        "october_source_rows": october_rows,
        "november_source_rows": november_rows,
        "october": summarize_dates(october_matches),
        "november": summarize_dates(november_matches),
        "files": {
            "october_matches": str(october_matches),
            "november_matches": str(november_matches),
        },
    }
    report_path = work_dir / "opening_date_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
