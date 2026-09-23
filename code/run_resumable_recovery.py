"""Run the alternative-source recovery preparation in resumable stages.

This orchestrator is intentionally independent from the original collection and
reconstruction code. It only reads source ZIPs and writes isolated manifests,
validation records, and per-archive key files. It never writes to PostgreSQL or
overwrites existing Parquet files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import time
import zipfile
from pathlib import Path
from typing import Any


DEFAULT_SOURCE_ROOT = "audit_source_verification/official_downloads"
DEFAULT_WORK_ROOT = "audit_source_verification/resumable_recovery"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run resumable, isolated recovery preparation stages."
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=["2024-10", "2024-11"],
        help="Reference months to process (default: 2024-10 2024-11).",
    )
    parser.add_argument(
        "--source-root",
        default=DEFAULT_SOURCE_ROOT,
        help="Directory containing downloaded source archives.",
    )
    parser.add_argument(
        "--work-root",
        default=DEFAULT_WORK_ROOT,
        help="Isolated directory for manifest, checkpoints, and derived keys.",
    )
    parser.add_argument(
        "--stage",
        choices=("manifest", "validate", "keys", "all"),
        default="all",
        help="Stage to run; later stages require earlier checkpoints.",
    )
    parser.add_argument(
        "--stop-after",
        choices=("manifest", "validate", "keys"),
        help="Stop after the selected stage, useful for controlled execution.",
    )
    return parser.parse_args()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_directory(source_root: Path, month: str) -> Path:
    matches = sorted(source_root.glob(f"{month}-*/"))
    if len(matches) != 1:
        raise RuntimeError(f"Expected one source directory for {month}, found {matches}")
    return matches[0]


def discover_archives(source_root: Path, months: list[str]) -> dict[str, list[dict[str, Any]]]:
    discovered: dict[str, list[dict[str, Any]]] = {}
    for month in months:
        directory = source_directory(source_root, month)
        archives = sorted(directory.glob("Estabelecimentos*.zip"))
        if len(archives) != 10:
            raise RuntimeError(f"Expected 10 establishment archives for {month}, found {len(archives)}")
        discovered[month] = [
            {
                "name": archive.name,
                "path": str(archive.resolve()),
                "bytes": archive.stat().st_size,
                "status": "discovered",
            }
            for archive in archives
        ]
    return discovered


def validate_archive(archive_path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(archive_path) as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise zipfile.BadZipFile(f"CRC check failed for {bad_member}")
        members = archive.infolist()
        if len(members) != 1:
            raise RuntimeError(f"Expected one CSV member in {archive_path}, found {len(members)}")
        row_count = 0
        with archive.open(members[0]) as binary_member:
            for _ in binary_member:
                row_count += 1
        return {
            "status": "validated",
            "sha256": sha256_file(archive_path),
            "bytes": archive_path.stat().st_size,
            "member": members[0].filename,
            "member_bytes": members[0].file_size,
            "rows": row_count,
            "validated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }


def extract_sorted_keys(archive_path: Path, output_path: Path) -> int:
    raw_path = output_path.with_suffix(".raw")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with zipfile.ZipFile(archive_path) as archive:
        member = archive.infolist()[0]
        with archive.open(member) as binary_member, raw_path.open("w", encoding="ascii") as raw_output:
            text_member = (line.decode("latin-1") for line in binary_member)
            for fields in csv.reader(text_member, delimiter=";", quotechar='"'):
                if len(fields) < 3:
                    raise ValueError(f"Malformed row in {archive_path}")
                raw_output.write("|".join(fields[:3]) + "\n")
                row_count += 1
    with output_path.open("w", encoding="ascii") as sorted_output:
        subprocess.run(["sort", "-u", str(raw_path)], check=True, stdout=sorted_output)
    raw_path.unlink()
    return row_count


def run() -> int:
    args = parse_args()
    source_root = Path(args.source_root).resolve()
    work_root = Path(args.work_root).resolve()
    manifest_path = work_root / "recovery_manifest.json"
    key_root = work_root / "keys"
    work_root.mkdir(parents=True, exist_ok=True)

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("months") != args.months:
            raise RuntimeError("Existing manifest uses different months; choose another --work-root")
    else:
        manifest = {
            "version": 1,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "source_root": str(source_root),
            "months": args.months,
            "archives": discover_archives(source_root, args.months),
        }
        atomic_write_json(manifest_path, manifest)
        print(f"Manifest checkpoint written to {manifest_path}")

    if args.stage in ("manifest", "all"):
        if args.stop_after == "manifest" or args.stage == "manifest":
            return 0

    if args.stage in ("validate", "keys", "all"):
        for month in args.months:
            for archive_record in manifest["archives"][month]:
                archive_path = Path(archive_record["path"])
                if archive_record.get("status") == "validated" and archive_record.get("bytes") == archive_path.stat().st_size:
                    print(f"[RESUME] Validation already complete: {month}/{archive_record['name']}")
                else:
                    print(f"[VALIDATE] {month}/{archive_record['name']}")
                    archive_record.update(validate_archive(archive_path))
                    atomic_write_json(manifest_path, manifest)
        if args.stop_after == "validate" or args.stage == "validate":
            return 0

    if args.stage in ("keys", "all"):
        for month in args.months:
            for archive_record in manifest["archives"][month]:
                if archive_record.get("status") != "validated":
                    raise RuntimeError(f"Archive is not validated: {month}/{archive_record['name']}")
                key_path = key_root / month / f"{Path(archive_record['name']).stem}.keys"
                archive_record["key_file"] = str(key_path)
                if key_path.exists() and archive_record.get("key_status") == "complete":
                    print(f"[RESUME] Keys already complete: {month}/{archive_record['name']}")
                    continue
                print(f"[KEYS] {month}/{archive_record['name']}")
                extracted_rows = extract_sorted_keys(Path(archive_record["path"]), key_path)
                archive_record.update({"key_status": "complete", "key_rows": extracted_rows})
                atomic_write_json(manifest_path, manifest)
        if args.stop_after == "keys" or args.stage == "keys":
            return 0

    manifest["status"] = "complete"
    manifest["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    atomic_write_json(manifest_path, manifest)
    print(f"Recovery preparation complete. Resume manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
