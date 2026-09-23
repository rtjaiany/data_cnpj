"""Analyze opening dates for the official October-to-November transition.

This read-only audit uses the official-equivalent November archives and the
official November-only key set. It does not modify production data, Parquets,
source archives, or the original ETL code.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import zipfile
from collections import Counter
from pathlib import Path

OPENING_DATE_INDEX = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze opening dates for official November-only establishments."
    )
    parser.add_argument(
        "--transition-keys",
        default="audit_source_verification/official_validation/november_only_vs_october.keys",
        help="Sorted keys present in official November but absent in official October.",
    )
    parser.add_argument(
        "--source-dir",
        default="audit_source_verification/official_downloads/2024-11",
        help="Official November archive directory.",
    )
    parser.add_argument(
        "--work-dir",
        default="audit_source_verification/official_validation/opening_dates",
        help="Isolated output directory.",
    )
    return parser.parse_args()


def extract_key_dates(source_dir: Path, output: Path) -> int:
    rows = 0
    with output.open("w", encoding="ascii") as target:
        for archive_path in sorted(source_dir.glob("Estabelecimentos*.zip")):
            with zipfile.ZipFile(archive_path) as archive:
                member = archive.infolist()[0]
                with archive.open(member) as binary_member:
                    text_member = (line.decode("latin-1") for line in binary_member)
                    for fields in csv.reader(text_member, delimiter=";", quotechar='"'):
                        if len(fields) <= OPENING_DATE_INDEX:
                            raise ValueError(f"Malformed row in {archive_path}")
                        target.write(
                            f"{'|'.join(fields[:3])}\t{fields[OPENING_DATE_INDEX]}\n"
                        )
                        rows += 1
    return rows


def sort_key_dates(input_path: Path, output_path: Path) -> None:
    with output_path.open("w", encoding="ascii") as target:
        subprocess.run(
            ["sort", "-t", "\t", "-k1,1", str(input_path)],
            check=True,
            stdout=target,
        )


def summarize(joined_path: Path) -> dict[str, object]:
    counts: Counter[str] = Counter()
    matched = 0
    with joined_path.open(encoding="ascii") as source:
        for line in source:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 2:
                continue
            counts[fields[1] or "NULL"] += 1
            matched += 1
    before_2024 = sum(value for date, value in counts.items() if date.isdigit() and int(date) < 20240101)
    during_2024 = sum(value for date, value in counts.items() if date.startswith("2024"))
    during_2024_11 = sum(value for date, value in counts.items() if date.startswith("202411"))
    return {
        "matched_keys": matched,
        "before_2024": before_2024,
        "during_2024": during_2024,
        "during_2024_11": during_2024_11,
        "null_dates": counts.get("NULL", 0),
        "distinct_dates": len(counts),
        "date_counts": dict(sorted(counts.items())),
    }


def main() -> int:
    args = parse_args()
    transition_keys = Path(args.transition_keys).resolve()
    source_dir = Path(args.source_dir).resolve()
    work_dir = Path(args.work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    raw_dates = work_dir / "november.key_dates"
    sorted_dates = work_dir / "november.key_dates.sorted"
    joined = work_dir / "november_only.opening_dates"

    source_rows = extract_key_dates(source_dir, raw_dates)
    sort_key_dates(raw_dates, sorted_dates)
    raw_dates.unlink()
    with joined.open("w", encoding="ascii") as output:
        subprocess.run(
            ["join", "-t", "\t", "-1", "1", "-2", "1", str(transition_keys), str(sorted_dates)],
            check=True,
            stdout=output,
        )
    result = {
        "transition_keys": sum(1 for _ in transition_keys.open("rb")),
        "source_rows": source_rows,
        "analysis": summarize(joined),
        "joined_file": str(joined),
    }
    report_path = work_dir / "official_transition_opening_date_report.json"
    report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
