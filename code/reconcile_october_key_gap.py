"""Reconcile October source-only establishment keys with database snapshots.

This is a read-only audit. It does not modify production tables, Parquet files,
or the original collection/reconstruction code.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile source-only October establishment keys with snapshots."
    )
    parser.add_argument(
        "--source-only",
        default="audit_source_verification/key_compare/2024-10/source_only.keys",
        help="Sorted keys present in the alternative source but absent locally.",
    )
    parser.add_argument(
        "--work-dir",
        default="audit_source_verification/key_reconcile",
        help="Isolated directory for exported snapshot keys and reports.",
    )
    parser.add_argument(
        "--report",
        default="audit_source_verification/key_reconcile/reconciliation_report.json",
        help="Output JSON report path.",
    )
    return parser.parse_args()


def database_connection():
    load_dotenv(".env")
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        password=os.environ.get("DB_PASSWORD", ""),
    )


def export_snapshot_keys(connection, output: Path, month: str, change_type: str) -> int:
    query = """
        COPY (
            SELECT cnpj_basico || '|' || cnpj_ordem || '|' || cnpj_dv
            FROM public.snapshots
            WHERE mes_referencia = %s
              AND tabela = 'estabelecimento'
              AND tipo_alteracao = %s
            ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv
        ) TO STDOUT WITH (FORMAT csv, HEADER false)
    """
    with output.open("w", encoding="ascii") as target:
        with connection.cursor() as cursor:
            cursor.copy_expert(cursor.mogrify(query, (month, change_type)).decode(), target)
    with output.open("rb") as source:
        return sum(1 for _ in source)


def run_comm(left: Path, right: Path, output: Path, flag: str) -> int:
    with output.open("w", encoding="ascii") as target:
        subprocess.run(["comm", flag, str(left), str(right)], check=True, stdout=target)
    with output.open("rb") as source:
        return sum(1 for _ in source)


def main() -> int:
    args = parse_args()
    source_only = Path(args.source_only).resolve()
    work_dir = Path(args.work_dir).resolve()
    report_path = Path(args.report).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    deletion_keys = work_dir / "october_deletion.keys"
    november_insert_keys = work_dir / "november_insert.keys"
    source_only_not_deleted = work_dir / "source_only_not_october_deletion.keys"
    october_deletion_not_source_only = work_dir / "october_deletion_not_source_only.keys"
    source_only_not_november_insert = work_dir / "source_only_not_november_insert.keys"
    november_insert_not_source_only = work_dir / "november_insert_not_source_only.keys"

    connection = database_connection()
    try:
        october_deletions = export_snapshot_keys(connection, deletion_keys, "2024-10", "DELETE")
        november_inserts = export_snapshot_keys(connection, november_insert_keys, "2024-11", "INSERT")
    finally:
        connection.close()

    report = {
        "source_only_keys": sum(1 for _ in source_only.open("rb")),
        "october_deletion_keys": october_deletions,
        "november_insert_keys": november_inserts,
        "source_only_not_october_deletion": run_comm(
            source_only, deletion_keys, source_only_not_deleted, "-23"
        ),
        "october_deletion_not_source_only": run_comm(
            deletion_keys, source_only, october_deletion_not_source_only, "-23"
        ),
        "source_only_not_november_insert": run_comm(
            source_only, november_insert_keys, source_only_not_november_insert, "-23"
        ),
        "november_insert_not_source_only": run_comm(
            november_insert_keys, source_only, november_insert_not_source_only, "-23"
        ),
        "source_only_not_october_deletion_file": str(source_only_not_deleted),
        "october_deletion_not_source_only_file": str(october_deletion_not_source_only),
        "source_only_not_november_insert_file": str(source_only_not_november_insert),
        "november_insert_not_source_only_file": str(november_insert_not_source_only),
    }
    if report["source_only_not_november_insert"] == 0:
        report["status"] = "SOURCE_ONLY_KEYS_ACCOUNTED_IN_NOVEMBER"
    else:
        report["status"] = "SOURCE_ONLY_KEYS_MISSING_FROM_NOVEMBER"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {report_path}")
    return 0 if report["status"] == "SOURCE_ONLY_KEYS_ACCOUNTED_IN_NOVEMBER" else 2


if __name__ == "__main__":
    raise SystemExit(main())
