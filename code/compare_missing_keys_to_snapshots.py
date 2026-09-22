"""Compare alternative-source-only establishment keys with October deletes.

The database operation uses a temporary table inside a transaction that is
rolled back before the connection closes. It does not modify persistent tables,
snapshots, Parquet files, or source ZIPs.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare source-only establishment keys with snapshot deletes."
    )
    parser.add_argument(
        "--keys",
        default="audit_source_verification/key_compare/2024-10/source_only.keys",
        help="File containing pipe-separated source-only establishment keys.",
    )
    parser.add_argument(
        "--month",
        default="2024-10",
        help="Snapshot reference month to inspect.",
    )
    parser.add_argument(
        "--report",
        default="audit_source_verification/key_compare/missing_key_snapshot_report.json",
        help="Output JSON report path.",
    )
    return parser.parse_args()


def database_connection() -> psycopg2.extensions.connection:
    load_dotenv()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "cnpj"),
        user=os.getenv("DB_USER", "postgres"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def main() -> int:
    args = parse_args()
    keys_path = Path(args.keys).resolve()
    report_path = Path(args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if not keys_path.exists():
        raise FileNotFoundError(keys_path)

    with database_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE source_only_keys (
                    cnpj_basico VARCHAR(8) NOT NULL,
                    cnpj_ordem VARCHAR(4) NOT NULL,
                    cnpj_dv VARCHAR(2) NOT NULL
                ) ON COMMIT DROP;
                """
            )
            with keys_path.open("r", encoding="ascii") as keys_file:
                cursor.copy_expert(
                    "COPY source_only_keys (cnpj_basico, cnpj_ordem, cnpj_dv) FROM STDIN WITH (FORMAT csv, DELIMITER '|')",
                    keys_file,
                )
            cursor.execute(
                """
                SELECT
                    COUNT(*)::bigint AS source_only_keys,
                    COUNT(s.cnpj_basico)::bigint AS matching_delete_keys,
                    COUNT(*) - COUNT(s.cnpj_basico)::bigint AS unmatched_source_only_keys
                FROM source_only_keys k
                LEFT JOIN public.snapshots s
                  ON s.tabela = 'estabelecimento'
                 AND s.mes_referencia = %s
                 AND s.tipo_alteracao = 'DELETE'
                 AND s.cnpj_basico = k.cnpj_basico
                 AND s.cnpj_ordem = k.cnpj_ordem
                 AND s.cnpj_dv = k.cnpj_dv;
                """,
                (args.month,),
            )
            source_only_keys, matching_delete_keys, unmatched_source_only_keys = cursor.fetchone()
        connection.rollback()

    report = {
        "month": args.month,
        "source_only_file": str(keys_path),
        "source_only_keys": source_only_keys,
        "matching_snapshot_delete_keys": matching_delete_keys,
        "unmatched_source_only_keys": unmatched_source_only_keys,
        "status": "MATCH" if unmatched_source_only_keys == 0 else "MISMATCH",
        "database_changes": False,
    }
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {report_path}")
    return 0 if report["status"] == "MATCH" else 2


if __name__ == "__main__":
    raise SystemExit(main())