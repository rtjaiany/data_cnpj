"""Analyze official establishment disappearances from September to October 2024.

The analysis is read-only. It uses the official September archives and the
official transition key set, then writes an isolated tagged candidate list. It
does not change PostgreSQL, snapshots, Parquet outputs, or reconstruction logic.
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
STATUS_INDEX = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze official September-to-October establishment disappearances."
    )
    parser.add_argument(
        "--disappearance-keys",
        default="audit_source_verification/official_transition/transition_2024-09_to_2024-10_only_previous.keys",
    )
    parser.add_argument(
        "--source-dir",
        default="audit_source_verification/official_downloads/2024-09",
    )
    parser.add_argument(
        "--work-dir",
        default="audit_source_verification/official_disappearances",
    )
    return parser.parse_args()


def extract_metadata(source_dir: Path, raw_path: Path) -> int:
    rows = 0
    with raw_path.open("w", encoding="ascii") as output:
        for archive_path in sorted(source_dir.glob("Estabelecimentos*.zip")):
            with zipfile.ZipFile(archive_path) as archive:
                member = archive.infolist()[0]
                with archive.open(member) as binary_member:
                    text_member = (line.decode("latin-1") for line in binary_member)
                    for fields in csv.reader(text_member, delimiter=";", quotechar='"'):
                        if len(fields) <= OPENING_DATE_INDEX:
                            raise ValueError(f"Malformed row in {archive_path}")
                        key = "|".join(fields[:3])
                        output.write(f"{key}\t{fields[OPENING_DATE_INDEX]}\t{fields[STATUS_INDEX]}\n")
                        rows += 1
    return rows


def sort_unique_metadata(raw_path: Path, sorted_path: Path) -> None:
    with sorted_path.open("w", encoding="ascii") as output:
        subprocess.run(
            ["sort", "-t", "\t", "-k1,1", "-u", str(raw_path)],
            check=True,
            stdout=output,
        )


def classify(opening_date: str, status: str) -> str:
    if opening_date.isdigit() and int(opening_date) < 20241001:
        if status in {"2", "02"}:
            return "pre_existing_active_in_september"
        return "pre_existing_non_active_in_september"
    if opening_date.startswith("202410"):
        return "opened_in_october_2024"
    if opening_date.startswith("2024"):
        return "opened_earlier_in_2024"
    if not opening_date or opening_date == "0":
        return "missing_opening_date"
    return "opened_before_or_outside_2024"


def main() -> int:
    args = parse_args()
    disappearance_keys = Path(args.disappearance_keys).resolve()
    source_dir = Path(args.source_dir).resolve()
    work_dir = Path(args.work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    raw_metadata = work_dir / "september_establishment_metadata.raw"
    sorted_metadata = work_dir / "september_establishment_metadata.sorted"
    normalized_keys = work_dir / "disappearance_keys.normalized"
    tagged_output = work_dir / "disappeared_establishments_tagged.tsv"

    source_rows = extract_metadata(source_dir, raw_metadata)
    sort_unique_metadata(raw_metadata, sorted_metadata)
    raw_metadata.unlink()

    with disappearance_keys.open(encoding="ascii") as source, normalized_keys.open("w", encoding="ascii") as target:
        for line in source:
            parts = line.rstrip("\n").split("|")
            if len(parts) != 3:
                raise ValueError(f"Malformed disappearance key: {line!r}")
            target.write(f"{'|'.join(parts)}\t\n")

    counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    matched = 0
    with tagged_output.open("w", encoding="utf-8") as output:
        output.write("cnpj_basico\tcnpj_ordem\tcnpj_dv\topening_date\tstatus\ttag\n")
        join_process = subprocess.Popen(
            ["join", "-t", "\t", "-1", "1", "-2", "1", str(normalized_keys), str(sorted_metadata)],
            stdout=subprocess.PIPE,
            text=True,
            encoding="ascii",
        )
        assert join_process.stdout is not None
        for line in join_process.stdout:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 4:
                continue
            key_parts = fields[0].split("|")
            opening_date, status = fields[2], fields[3]
            tag = classify(opening_date, status)
            counts[tag] += 1
            status_counts[status or "NULL"] += 1
            output.write("\t".join([*key_parts, opening_date, status, tag]) + "\n")
            matched += 1
        return_code = join_process.wait()
        if return_code != 0:
            raise RuntimeError(f"join failed with exit code {return_code}")

    result = {
        "source_directory": str(source_dir),
        "disappearance_keys": str(disappearance_keys),
        "source_rows": source_rows,
        "disappearance_key_count": sum(1 for _ in normalized_keys.open("rb")),
        "matched_metadata_count": matched,
        "unmatched_metadata_count": sum(1 for _ in normalized_keys.open("rb")) - matched,
        "tag_counts": dict(sorted(counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "tagged_output": str(tagged_output),
        "warning": "Tags are audit candidates only; no record is automatically restored in the panel.",
    }
    report_path = work_dir / "official_disappearance_opening_date_report.json"
    report_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
