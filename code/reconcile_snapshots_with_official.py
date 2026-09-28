"""Reconcile establishment snapshots with official source payloads.

This read-only audit compares October DELETE previous payloads and November
INSERT new payloads from PostgreSQL with the corresponding official filtered
payloads. It does not modify PostgreSQL or any source/output data.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

SNAPSHOT_FIELDS = [
    "identificador_matriz_filial", "situacao_cadastral", "data_situacao_cadastral",
    "motivo_situacao_cadastral", "pais", "data_inicio_atividade",
    "cnae_fiscal_principal", "cnae_fiscal_secundaria", "uf", "municipio",
    "situacao_especial", "data_situacao_especial",
]
NUMERIC_FIELDS = {
    "identificador_matriz_filial", "situacao_cadastral", "data_situacao_cadastral",
    "motivo_situacao_cadastral", "pais", "data_inicio_atividade",
    "cnae_fiscal_principal", "municipio", "data_situacao_especial",
}
OFFICIAL_INDEX = {
    "identificador_matriz_filial": 0,
    "situacao_cadastral": 2,
    "data_situacao_cadastral": 3,
    "motivo_situacao_cadastral": 4,
    "pais": 6,
    "data_inicio_atividade": 7,
    "cnae_fiscal_principal": 8,
    "cnae_fiscal_secundaria": 9,
    "uf": 16,
    "municipio": 17,
    "situacao_especial": 25,
    "data_situacao_especial": 26,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile official payloads with PostgreSQL snapshots.")
    parser.add_argument("--payload-dir", default="audit_source_verification/official_filtered_payloads")
    parser.add_argument("--report", default="audit_source_verification/snapshot_payload_reconciliation.json")
    return parser.parse_args()


def normalize(field: str, value: object) -> str | None:
    if value is None or value == "":
        return None
    text = str(value)
    if field in NUMERIC_FIELDS:
        try:
            return str(int(text))
        except ValueError:
            return text
    return text


def official_payloads(path: Path):
    with path.open(encoding="utf-8") as source:
        for line in source:
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 28:
                continue
            yield fields[0], fields[1:]


def connect():
    load_dotenv(".env")
    import os
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "cnpj"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )


def reconcile_case(conn, month: str, event_type: str, json_column: str, official_path: Path, target_keys: set[str]) -> dict[str, object]:
    cursor_name = f"audit_{month.replace('-', '')}_{event_type.lower()}"
    query = f"""
        SELECT cnpj_basico, cnpj_ordem, cnpj_dv, {json_column}
        FROM public.snapshots
        WHERE tabela = 'estabelecimento'
          AND mes_referencia = %s
          AND tipo_alteracao = %s
        ORDER BY cnpj_basico, cnpj_ordem, cnpj_dv
    """
    with conn.cursor(name=cursor_name) as cursor:
        cursor.itersize = 50_000
        cursor.execute(query, (month, event_type))
        database_row = cursor.fetchone()
        official_iterator = official_payloads(official_path)
        official_row = next(official_iterator, None)
        counts = Counter()
        field_mismatches: Counter[str] = Counter()
        examples: list[dict[str, object]] = []
        while database_row is not None:
            key = "|".join(str(part) for part in database_row[:3])
            if key not in target_keys:
                database_row = cursor.fetchone()
                continue
            while official_row is not None and official_row[0] < key:
                official_row = next(official_iterator, None)
            if official_row is None or official_row[0] != key:
                counts["missing_official_payload"] += 1
            else:
                snapshot_payload = database_row[3] or {}
                official_values = official_row[1]
                row_differences = []
                for field in SNAPSHOT_FIELDS:
                    snapshot_value = normalize(field, snapshot_payload.get(field))
                    official_value = normalize(field, official_values[OFFICIAL_INDEX[field]])
                    if snapshot_value != official_value:
                        field_mismatches[field] += 1
                        row_differences.append({"field": field, "snapshot": snapshot_value, "official": official_value})
                if row_differences:
                    counts["payload_mismatch"] += 1
                    if len(examples) < 10:
                        examples.append({"key": key, "differences": row_differences})
                else:
                    counts["payload_match"] += 1
                official_row = next(official_iterator, None)
            counts["snapshot_events"] += 1
            database_row = cursor.fetchone()
    return {"month": month, "event_type": event_type, "counts": dict(counts), "field_mismatches": dict(field_mismatches.most_common()), "examples": examples}


def main() -> int:
    args = parse_args()
    payload_dir = Path(args.payload_dir).resolve()
    report_path = Path(args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect()
    try:
        previous_keys = {line.rstrip("\n").split("\t", 1)[0] for line in (payload_dir / "previous.filtered.sorted").open(encoding="utf-8")}
        current_keys = {line.rstrip("\n").split("\t", 1)[0] for line in (payload_dir / "current.filtered.sorted").open(encoding="utf-8")}
        results = [
            reconcile_case(conn, "2024-10", "DELETE", "conteudo_anterior", payload_dir / "previous.filtered.sorted", previous_keys),
            reconcile_case(conn, "2024-11", "INSERT", "conteudo_novo", payload_dir / "current.filtered.sorted", current_keys),
        ]
    finally:
        conn.close()
    report = {"results": results, "warning": "This audit compares snapshot payloads with official source payloads and does not modify production data."}
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
