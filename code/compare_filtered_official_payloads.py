"""Compare official payloads after filtering to the affected establishment keys.

Only the 3,054,993 keys that disappear from September to October are retained
while reading the official September and November archives. This keeps temporary
disk usage bounded and does not modify production data or source files.
"""

from __future__ import annotations

import argparse
import csv
import json
import zipfile
from collections import Counter
from pathlib import Path

PAYLOAD_FIELDS = [
    "identificador_matriz_filial", "nome_fantasia", "situacao_cadastral",
    "data_situacao_cadastral", "motivo_situacao_cadastral", "nome_cidade_exterior",
    "pais", "data_inicio_atividade", "cnae_fiscal_principal",
    "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero",
    "complemento", "bairro", "cep", "uf", "municipio", "ddd_1", "telefone_1",
    "ddd_2", "telefone_2", "ddd_fax", "fax", "correio_eletronico",
    "situacao_especial", "data_situacao_especial",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare filtered official CNPJ payloads.")
    parser.add_argument("--keys", default="audit_source_verification/official_transition/transition_2024-09_to_2024-10_only_previous.keys")
    parser.add_argument("--previous-dir", default="audit_source_verification/official_downloads/2024-09")
    parser.add_argument("--current-dir", default="audit_source_verification/official_downloads/2024-11")
    parser.add_argument("--work-dir", default="audit_source_verification/official_filtered_payloads")
    return parser.parse_args()


def load_target_keys(path: Path) -> set[str]:
    with path.open(encoding="ascii") as source:
        return {line.rstrip("\n") for line in source}


def extract_filtered(source_dir: Path, targets: set[str], output: Path) -> tuple[int, int, int, list[str]]:
    source_rows = 0
    matched_rows = 0
    malformed_rows = 0
    malformed_keys: list[str] = []
    with output.open("w", encoding="utf-8") as target_output:
        for archive_path in sorted(source_dir.glob("Estabelecimentos*.zip")):
            with zipfile.ZipFile(archive_path) as archive:
                member = archive.infolist()[0]
                with archive.open(member) as binary_member:
                    text_member = (line.decode("latin-1") for line in binary_member)
                    for fields in csv.reader(text_member, delimiter=";", quotechar='"'):
                        source_rows += 1
                        key = "|".join(fields[:3])
                        if key in targets:
                            if len(fields) - 3 != len(PAYLOAD_FIELDS):
                                malformed_rows += 1
                                malformed_keys.append(key)
                                continue
                            target_output.write(f"{key}\t{'\t'.join(fields[3:])}\n")
                            matched_rows += 1
            print(f"[FILTER] {source_dir.name}/{archive_path.name}: source={source_rows:,} matched={matched_rows:,}", flush=True)
    return source_rows, matched_rows, malformed_rows, malformed_keys


def sort_unique_by_key(raw_path: Path, sorted_path: Path) -> None:
    import subprocess
    with sorted_path.open("w", encoding="ascii") as output:
        subprocess.run(["sort", "-t", "\t", "-k1,1", "-u", str(raw_path)], check=True, stdout=output)
    raw_path.unlink()


def payload_iterator(path: Path):
    with path.open(encoding="utf-8") as source:
        for line in source:
            fields = line.rstrip("\n").split("\t")
            yield fields[0], fields[1:]


def compare_payloads(keys: Path, previous: Path, current: Path) -> dict[str, object]:
    changed_fields: Counter[str] = Counter()
    status_transitions: Counter[str] = Counter()
    opening_transitions: Counter[str] = Counter()
    previous_iter = payload_iterator(previous)
    current_iter = payload_iterator(current)
    previous_item = next(previous_iter, None)
    current_item = next(current_iter, None)
    requested = matched = identical = previous_missing = current_missing = 0
    malformed_pairs: list[str] = []

    with keys.open(encoding="ascii") as target_keys:
        for key_line in target_keys:
            key = key_line.rstrip("\n")
            requested += 1
            while previous_item is not None and previous_item[0] < key:
                previous_item = next(previous_iter, None)
            while current_item is not None and current_item[0] < key:
                current_item = next(current_iter, None)
            previous_payload = previous_item[1] if previous_item and previous_item[0] == key else None
            current_payload = current_item[1] if current_item and current_item[0] == key else None
            if previous_payload is None:
                previous_missing += 1
            if current_payload is None:
                current_missing += 1
            if previous_payload is None or current_payload is None:
                continue
            matched += 1
            if previous_payload[2] not in {"01", "02", "03", "04", "08"} or current_payload[2] not in {"01", "02", "03", "04", "08"}:
                malformed_pairs.append(key)
                continue
            if previous_payload == current_payload:
                identical += 1
            for field_name, old, new in zip(PAYLOAD_FIELDS, previous_payload, current_payload):
                if old != new:
                    changed_fields[field_name] += 1
            status_transitions[f"{previous_payload[2]}->{current_payload[2]}"] += 1
            opening_transitions[f"{previous_payload[7]}->{current_payload[7]}"] += 1

    return {
        "keys_requested": requested,
        "matched_payload_pairs": matched,
        "previous_missing": previous_missing,
        "current_missing": current_missing,
        "identical_payload_pairs": identical,
        "changed_payload_pairs": matched - identical - len(malformed_pairs),
        "malformed_payload_pairs": len(malformed_pairs),
        "malformed_payload_keys": malformed_pairs,
        "changed_fields": dict(changed_fields.most_common()),
        "status_transitions": dict(status_transitions.most_common()),
        "opening_date_transitions": dict(opening_transitions.most_common()),
    }


def main() -> int:
    args = parse_args()
    keys = Path(args.keys).resolve()
    previous_dir = Path(args.previous_dir).resolve()
    current_dir = Path(args.current_dir).resolve()
    work_dir = Path(args.work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    targets = load_target_keys(keys)
    print(f"Loaded {len(targets):,} target keys", flush=True)
    previous_raw = work_dir / "previous.filtered.raw"
    current_raw = work_dir / "current.filtered.raw"
    previous_sorted = work_dir / "previous.filtered.sorted"
    current_sorted = work_dir / "current.filtered.sorted"
    previous_source_rows, previous_matched_rows, previous_malformed_rows, previous_malformed_keys = extract_filtered(previous_dir, targets, previous_raw)
    current_source_rows, current_matched_rows, current_malformed_rows, current_malformed_keys = extract_filtered(current_dir, targets, current_raw)
    sort_unique_by_key(previous_raw, previous_sorted)
    sort_unique_by_key(current_raw, current_sorted)
    result = {
        "previous_source_rows": previous_source_rows,
        "previous_filtered_rows": previous_matched_rows,
        "previous_malformed_rows": previous_malformed_rows,
        "previous_malformed_keys": previous_malformed_keys,
        "current_source_rows": current_source_rows,
        "current_filtered_rows": current_matched_rows,
        "current_malformed_rows": current_malformed_rows,
        "current_malformed_keys": current_malformed_keys,
        "comparison": compare_payloads(keys, previous_sorted, current_sorted),
        "warning": "This describes official source payload changes; it does not decide whether October records should be imputed.",
    }
    report_path = work_dir / "filtered_official_payload_report.json"
    report_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
