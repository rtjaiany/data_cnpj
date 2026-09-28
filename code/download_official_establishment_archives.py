"""Download official Receita Federal establishment archives conservatively.

This standalone downloader is intentionally separate from the production ETL.
It downloads one file at a time into an isolated directory, supports resuming,
and uses cooldowns and exponential backoff to reduce request pressure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv

WEBDAV_ROOT = "https://arquivos.receitafederal.gov.br/public.php/webdav/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download official CNPJ establishment archives with throttling."
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=["2024-09", "2024-10", "2024-11"],
        help="Reference months to download (default: 2024-09 2024-10 2024-11).",
    )
    parser.add_argument(
        "--output-root",
        default="audit_source_verification/official_downloads",
        help="Isolated output directory; never writes to production ETL paths.",
    )
    parser.add_argument("--pause-seconds", type=float, default=5.0, help="Pause between requests.")
    parser.add_argument("--timeout-seconds", type=float, default=60.0, help="Connect/read timeout.")
    parser.add_argument("--max-retries", type=int, default=5, help="Retries for transient failures.")
    parser.add_argument("--chunk-mb", type=int, default=8, help="Download chunk size in MiB.")
    parser.add_argument("--dry-run", action="store_true", help="List files without downloading.")
    return parser.parse_args()


def share_token() -> str:
    load_dotenv(".env")
    share_url = os.getenv("NEXTCLOUD_SHARE_URL", "")
    if not share_url:
        raise RuntimeError("NEXTCLOUD_SHARE_URL is not configured")
    return share_url.rstrip("/").split("/")[-1]


def request_with_backoff(session: requests.Session, method: str, url: str, token: str, args: argparse.Namespace, **kwargs: object) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(args.max_retries + 1):
        if attempt:
            delay = max(args.pause_seconds, 2 ** (attempt - 1) * args.pause_seconds)
            print(f"Transient failure; waiting {delay:.1f}s before retry {attempt}/{args.max_retries}...")
            time.sleep(delay)
        try:
            response = session.request(
                method,
                url,
                auth=(token, ""),
                timeout=(args.timeout_seconds, args.timeout_seconds),
                **kwargs,
            )
            if response.status_code == 429 or response.status_code >= 500:
                response.close()
                raise requests.HTTPError(f"transient HTTP status {response.status_code}")
            response.raise_for_status()
            return response
        except (requests.RequestException, OSError) as exc:
            last_error = exc
    raise RuntimeError(f"Request failed after retries: {url}: {last_error}")


def list_establishment_archives(session: requests.Session, month: str, token: str, args: argparse.Namespace) -> list[dict[str, object]]:
    url = f"{WEBDAV_ROOT}{quote(month)}/"
    response = request_with_backoff(session, "PROPFIND", url, token, args, headers={"Depth": "1"})
    try:
        root = ET.fromstring(response.content)
    finally:
        response.close()
    archives = []
    for item in root.findall("{DAV:}response"):
        href = item.findtext("{DAV:}href", "")
        name = href.rstrip("/").split("/")[-1]
        if not name.lower().startswith("estabelecimentos") or not name.lower().endswith(".zip"):
            continue
        size_text = item.findtext(".//{DAV:}getcontentlength", "0")
        archives.append({"name": name, "url": f"{WEBDAV_ROOT}{quote(month)}/{quote(name)}", "bytes": int(size_text or 0)})
    if len(archives) != 10:
        raise RuntimeError(f"Expected 10 establishment archives for {month}, found {len(archives)}")
    return sorted(archives, key=lambda item: str(item["name"]))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(path: Path, manifest: dict[str, object]) -> None:
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(path)


def download_archive(session: requests.Session, archive: dict[str, object], destination: Path, token: str, args: argparse.Namespace) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    expected_size = int(archive["bytes"])
    existing_size = destination.stat().st_size if destination.exists() else 0
    if expected_size and existing_size == expected_size:
        print(f"[SKIP] {destination} already has the expected size")
        return
    if expected_size and existing_size > expected_size:
        destination.unlink()
        existing_size = 0

    headers = {"Range": f"bytes={existing_size}-"} if existing_size else {}
    print(
        f"[DOWNLOAD] {destination.name}: starting at {existing_size / 1024**2:.1f} MiB "
        f"of {expected_size / 1024**2:.1f} MiB",
        flush=True,
    )
    response = request_with_backoff(session, "GET", str(archive["url"]), token, args, headers=headers, stream=True)
    try:
        if existing_size and response.status_code != 206:
            response.close()
            destination.unlink()
            existing_size = 0
            response = request_with_backoff(session, "GET", str(archive["url"]), token, args, headers={}, stream=True)
        mode = "ab" if existing_size else "wb"
        downloaded_size = existing_size
        next_report = downloaded_size + 100 * 1024 * 1024
        with destination.open(mode) as output:
            for chunk in response.iter_content(chunk_size=args.chunk_mb * 1024 * 1024):
                if chunk:
                    output.write(chunk)
                    downloaded_size += len(chunk)
                    if downloaded_size >= next_report or (expected_size and downloaded_size == expected_size):
                        if expected_size:
                            percent = downloaded_size / expected_size * 100
                            print(
                                f"[PROGRESS] {destination.name}: {percent:.1f}% "
                                f"({downloaded_size / 1024**2:.1f}/{expected_size / 1024**2:.1f} MiB)",
                                flush=True,
                            )
                        next_report += 100 * 1024 * 1024
    finally:
        response.close()
    if expected_size and destination.stat().st_size != expected_size:
        raise RuntimeError(f"Size mismatch for {destination}: expected {expected_size}, got {destination.stat().st_size}")


def main() -> int:
    args = parse_args()
    token = share_token()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = output_root.parent / "official_download_manifest.json"
    session = requests.Session()
    session.headers.update({"User-Agent": "cnpj-official-audit-downloader/1.0"})
    manifest: dict[str, object] = {"source": WEBDAV_ROOT, "months": {}, "status": "running"}

    try:
        for month in args.months:
            print(f"\nListing official archives for {month} (single sequential session)")
            archives = list_establishment_archives(session, month, token, args)
            manifest["months"][month] = []
            write_manifest(manifest_path, manifest)
            for archive in archives:
                destination = output_root / month / str(archive["name"])
                record = dict(archive)
                record["path"] = str(destination)
                record["status"] = "in_progress"
                manifest["months"][month].append(record)
                write_manifest(manifest_path, manifest)
                if not args.dry_run:
                    download_archive(session, archive, destination, token, args)
                    record["local_bytes"] = destination.stat().st_size
                    record["sha256"] = sha256_file(destination)
                record["status"] = "completed"
                write_manifest(manifest_path, manifest)
                time.sleep(args.pause_seconds)
                print(f"[READY] {month}/{archive['name']}", flush=True)
    except KeyboardInterrupt:
        manifest["status"] = "interrupted"
        manifest["interrupted_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        write_manifest(manifest_path, manifest)
        print(f"\nDownload interrupted. Partial files were preserved. Resume with the same command. Manifest: {manifest_path}")
        return 130

    manifest["status"] = "completed"
    manifest["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    write_manifest(manifest_path, manifest)
    print(f"\nManifest written to {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
