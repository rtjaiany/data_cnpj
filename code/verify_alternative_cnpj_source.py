"""Verify alternative CNPJ establishment archives without touching production data.

This script intentionally does not import or modify the original ETL code. It
uses the Casa dos Dados mirror only as an independent source comparison.
"""

from __future__ import annotations

import argparse
import hashlib
from html.parser import HTMLParser
import json
import re
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests
import pyarrow.parquet as pq

SOURCE_ROOT = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
PART_PATTERN = re.compile(r"^Estabelecimentos\d+\.zip$", re.IGNORECASE)


class DirectoryListingParser(HTMLParser):
    """Extract archive links from the simple HTTP directory listing."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and verify alternative CNPJ establishment archives."
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=["2024-10-16", "2024-11-13"],
        help="Source directories to audit (default: 2024-10-16 2024-11-13).",
    )
    parser.add_argument(
        "--download-dir",
        default="audit_source_verification/downloads",
        help="Isolated download directory; existing project files are never used for downloads.",
    )
    parser.add_argument(
        "--reference-dir",
        default="ignored_fields/estabelecimento",
        help="Directory containing the current derived Parquet files.",
    )
    parser.add_argument(
        "--report",
        default="audit_source_verification/alternative_source_report.json",
        help="JSON report path.",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Only verify archives already present in the isolated download directory.",
    )
    return parser.parse_args()


def list_source_files(session: requests.Session, month: str) -> list[dict[str, str | int]]:
    directory_url = urljoin(SOURCE_ROOT, f"{month}/")
    response = session.get(directory_url, timeout=(30, 120))
    response.raise_for_status()
    parser = DirectoryListingParser()
    parser.feed(response.text)
    files = []
    for href in parser.links:
        name = href.split("/")[-1]
        if not PART_PATTERN.match(name):
            continue
        files.append({"name": name, "url": urljoin(directory_url, name), "listing": ""})
    if not files:
        raise RuntimeError(f"No establishment archives found at {directory_url}")
    return sorted(files, key=lambda item: str(item["name"]))


def download_file(session: requests.Session, url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    existing_size = destination.stat().st_size if destination.exists() else 0
    headers = {"Range": f"bytes={existing_size}-"} if existing_size else {}
    with session.get(url, headers=headers, stream=True, timeout=(30, 300)) as response:
        if existing_size and response.status_code == 200:
            existing_size = 0
            response.close()
            destination.unlink()
            return download_file(session, url, destination)
        response.raise_for_status()
        mode = "ab" if existing_size and response.status_code == 206 else "wb"
        with destination.open(mode) as output:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    output.write(chunk)


def count_member_rows(member: zipfile.ZipExtFile) -> int:
    return sum(1 for _ in member)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_archive(path: Path) -> dict[str, object]:
    result: dict[str, object] = {
        "file": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "members": [],
        "rows": 0,
    }
    with zipfile.ZipFile(path) as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise zipfile.BadZipFile(f"CRC check failed for {bad_member}")
        member_results = []
        for info in archive.infolist():
            with archive.open(info) as member:
                rows = count_member_rows(member)
            member_results.append(
                {"name": info.filename, "compressed_bytes": info.compress_size, "rows": rows}
            )
            result["rows"] = int(result["rows"]) + rows
        result["members"] = member_results
    return result


def reference_rows(reference_dir: Path, month: str) -> int:
    path = reference_dir / f"reference_month={month}" / "part-000.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Reference Parquet not found: {path}")
    return pq.ParquetFile(path).metadata.num_rows


def main() -> int:
    args = parse_args()
    download_dir = Path(args.download_dir).resolve()
    reference_dir = Path(args.reference_dir)
    report_path = Path(args.report).resolve()
    download_dir.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "cnpj-source-audit/1.0"})
    report: dict[str, object] = {
        "source_root": SOURCE_ROOT,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "download_dir": str(download_dir),
        "reference_dir": str(reference_dir.resolve()),
        "months": [],
    }

    for month in args.months:
        print(f"\nAuditing source directory: {month}")
        files = list_source_files(session, month) if not args.no_download else [
            {"name": f.name, "url": "", "listing": ""}
            for f in sorted((download_dir / month).glob("Estabelecimentos*.zip"))
        ]
        if not files:
            raise RuntimeError(f"No local archives found for {month}")

        month_result: dict[str, object] = {"source_directory": month, "archives": []}
        for file_info in files:
            name = str(file_info["name"])
            destination = download_dir / month / name
            if not args.no_download:
                print(f"Downloading or resuming {name}...")
                download_file(session, str(file_info["url"]), destination)
            print(f"Validating {destination}...")
            archive_result = verify_archive(destination)
            archive_result["listing"] = file_info.get("listing", "")
            month_result["archives"].append(archive_result)

        source_rows = sum(int(item["rows"]) for item in month_result["archives"])
        reference_month = month[:7]
        current_rows = reference_rows(reference_dir, reference_month)
        month_result["source_rows"] = source_rows
        month_result["reference_parquet_rows"] = current_rows
        month_result["row_difference_source_minus_reference"] = source_rows - current_rows
        month_result["status"] = "MATCH" if source_rows == current_rows else "MISMATCH"
        report["months"].append(month_result)
        print(f"{reference_month}: source_rows={source_rows:,}, reference_rows={current_rows:,}, status={month_result['status']}")

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nReport written to {report_path}")
    return 0 if all(item["status"] == "MATCH" for item in report["months"]) else 2


if __name__ == "__main__":
    sys.exit(main())
