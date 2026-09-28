# Official Source Reconstruction Runbook

**Status:** Investigation in progress
**Last updated:** 2026-09-28
**Scope:** Official October-November 2024 CNPJ reconstruction

## Objective

Explain and, if necessary, correct the official source-to-snapshot transition for October and November 2024 without modifying production data or existing Parquet outputs.

## Current Findings

- The official WebDAV endpoint is accessible through the VPN.
- The official October source contains `59,893,805` unique establishment keys after deduplication.
- The local October Parquet contains the same `59,893,805` unique keys.
- Official November contains `3,439,840` keys absent from official October.
- October snapshots contain `3,054,993` establishment deletes.
- November snapshots contain `3,439,840` establishment inserts.
- Of the October-deleted and November-reinserted establishments, `2,692,100` were opened before 2024 and none were opened in November 2024.

The reconstruction uses official Receita Federal files only.

## Controls

1. Keep production tables, existing Parquets, and the original ETL unchanged.
2. Download official files only into isolated audit directories.
3. Preserve source URLs, retrieval timestamps, sizes, checksums, and row counts.
4. Use a separate schema or database for any rebuilt snapshots.
5. Use atomic output creation and persistent checkpoints.
6. Do not classify mass delete/reinsert events as genuine business formation without payload and lineage evidence.

## Investigation Sequence

1. Download and validate all official files used by snapshot generation for October and November.
2. Compare official October and November states by normalized key and full payload.
3. Compare expected state transitions with `public.snapshots` by table and change type.
4. Audit staging completeness, `processed_files`, download checkpoints, deduplication, and deletion detection.
5. Reproduce snapshot generation in an isolated schema or database.
6. Compare `data_inicio_atividade` for affected records to distinguish new formation from old-record reentry.
7. Only regenerate snapshots if the official source-to-snapshot comparison identifies a real ETL error.
8. Rebuild the panel in a separate output directory only after snapshot validation.

## Resumable Execution

Use `code/run_resumable_recovery.py` for isolated, checkpointed preparation. The orchestrator supports `manifest`, `validate`, and `keys` stages and updates its manifest after each archive.

```bash
python code/run_resumable_recovery.py --stage manifest --source-root audit_source_verification/official_downloads
python code/run_resumable_recovery.py --stage validate --source-root audit_source_verification/official_downloads
python code/run_resumable_recovery.py --stage keys --source-root audit_source_verification/official_downloads
```

The official source directories must be complete before running the full sequence. The current local official download is only the revised October `Estabelecimentos3.zip`; the remaining official files must be downloaded into the isolated directory first.

## Execution Attempt: 2026-09-28

The official-only process was checked before starting any reconstruction.

- The repository branch `cleanup` is four commits ahead of `origin/cleanup` and has no commits behind it.
- The only local official archive currently available is `audit_source_verification/official_downloads/2024-10/Estabelecimentos3.zip`.
- Complete official archive sets for September, October, and November are not currently present locally.
- The official WebDAV request from the current shell returned HTTP `401 Unauthorized`.
- No production tables, snapshots, Parquet outputs, or original ETL files were modified.
- The isolated resumable manifest was not created because the required archive set is incomplete.

This is an access/availability blocker, not evidence that the official source is offline. The official files must be obtained through the authenticated VPN/WebDAV connection and placed under `audit_source_verification/official_downloads/` before running the manifest, validation, and key stages.

## Official Download and Validation: 2026-09-28

The authenticated official WebDAV connection became available during the audit. A standalone English-language downloader was used:

`code/download_official_establishment_archives.py`

The downloader used one sequential request stream, a five-second pause between files, a 60-second connect/read timeout, retry backoff, HTTP range resumption, and an atomic manifest. It wrote only to `audit_source_verification/official_downloads/`.

All 30 official establishment archives were downloaded or resumed successfully:

| Reference month | Archives | Raw rows | CRC result |
| --- | ---: | ---: | --- |
| `2024-09` | 10 | 62,634,863 | PASS |
| `2024-10` | 10 | 64,528,601 | PASS |
| `2024-11` | 10 | 63,333,645 | PASS |

Each archive contains exactly one CSV member and passed ZIP CRC validation. The validation output is recorded in `audit_source_verification/official_archive_validation.json`, and download sizes/checksums are recorded in `audit_source_verification/official_download_manifest.json`.

No production tables, snapshots, Parquet outputs, or original ETL files were modified.

## Performance Controls

Before any large rerun:

- Run `ANALYZE` on relevant tables.
- Use session-local `work_mem` settings.
- Test the November insert with `EXPLAIN (ANALYZE, BUFFERS)` in isolation.
- Materialize partner aggregates once where possible.
- Process large inserts in batches with progress checkpoints.

## Acceptance Criteria

The affected months are provisionally usable only when:

- official source keys and payloads pass structural and duplicate checks;
- official source-to-snapshot differences are explained;
- staging and processed-file lineage is documented;
- October and November rebuild successfully outside production;
- no existing Parquet or production table is modified; and
- the dissertation documents the official source validation and remaining uncertainty.

Until then, the October-November panel outputs remain provisional.
