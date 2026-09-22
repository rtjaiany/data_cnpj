# Audit Report: October-November 2024 Reconstruction

**Audit date:** 2026-09-10
**Repository branch:** `cleanup`
**Scope:** `2024-10` and `2024-11` establishment snapshots and longitudinal panel reconstruction for São Paulo (`SP`)

## Executive Summary

The reconstruction process completed successfully through `2024-10`, but the `2024-11` run was stopped after approximately 3 hours and 53 minutes while processing the establishment `INSERT` step.

The delay was caused by an exceptional data-volume pattern in the monthly snapshots. October contains approximately 3.05 million establishment deletions, while November contains approximately 3.44 million establishment insertions. Every establishment deleted in October reappears as an insertion in November. This strongly indicates a temporary coverage problem in the October establishment data, rather than genuine business turnover or ordinary monthly growth.

The available evidence does **not** prove whether the problem originated in the official Receita Federal dump or during the local ETL process, because the original ZIP files for these months are not currently available locally. The derived Parquet files and database snapshots are sufficient to establish the anomaly and its impact on reconstruction.

## Processing Status

- Months requested: `2023-05` through `2024-11`.
- Months successfully reconstructed and checkpointed: `2023-05` through `2024-10`.
- Last valid panel partition: `2024-10`.
- Last valid partition row count: `17,256,192` rows.
- `2024-11` was not checkpointed and did not produce a valid panel partition.
- The running process was stopped after the current PostgreSQL transaction became unreasonably slow.
- The incomplete transaction was terminated at the PostgreSQL backend level and rolled back.
- Previous checkpoints remained intact after rollback.

## Tests and Evidence

### 1. Repository and runtime checks

- Git working tree was clean on branch `cleanup` and aligned with `origin/cleanup`.
- PostgreSQL was accepting connections.
- PostgreSQL version: 15.17.
- The reconstruction process was running from the project virtual environment.
- The standalone incremental test could not run under the system `python3` because `psycopg2` and `python-dotenv` were unavailable there. This did not affect the reconstruction process, which used the project virtual environment.

### 2. Checkpoint integrity

The database checkpoint state was inspected before and after stopping the process:

- `analytics.panel_checkpoint_tracker.last_state_month = '2024-10'`.
- The latest metadata row was for `2024-10`.
- The `2024-10` partition recorded `17,256,192` rows and `450,898,791` bytes.
- No `2024-11` metadata row was created.
- The checkpoint state contained no null values in the establishment key fields (`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`).

### 3. Parquet structural validation

All reconstructed panel partitions from `2023-05` through `2024-10` were readable with PyArrow.

- All partitions used the same 36-column schema.
- All partitions were non-empty.
- Row counts increased month over month until `2024-09`.
- The decrease in `2024-10` was consistent with the large number of October establishment deletions and was recorded in the checkpoint metadata.

### 4. Snapshot duplication test

For `2024-10` and `2024-11`, the following values were equal for every table and change type tested:

- Total event count.
- Distinct snapshot IDs.
- Distinct event keys.

For establishment snapshots:

| Reference month | Events | Distinct keys |
| --- | ---: | ---: |
| 2024-10 | 3,742,106 | 3,742,106 |
| 2024-11 | 3,821,886 | 3,821,886 |

**Result:** no evidence of duplicate snapshot events.

### 5. Monthly establishment event comparison

| Month | Deletes | Inserts | Updates |
| --- | ---: | ---: | ---: |
| 2024-09 | 0 | 780,538 | 1,979,750 |
| 2024-10 | 3,054,993 | 313,935 | 373,178 |
| 2024-11 | 0 | 3,439,840 | 382,046 |

November is an extreme outlier in establishment insertions. October is an extreme outlier in establishment deletions.

### 6. Reappearance test

The October deletion keys were joined against November insertion keys:

- October establishment deletions: `3,054,993`.
- November establishment insertions: `3,439,840`.
- October deletions reappearing in November: `3,054,993`.
- Reappeared records with identical previous/new payloads: `2,990,905`.
- Reappeared records with changed payloads: `64,088`.

Therefore, **100% of the October establishment deletions reappear as November insertions**.

### 7. Derived Parquet row-count comparison

The local ETL-derived Parquet files show the same discontinuity independently of the snapshot table:

| Dataset | 2024-09 rows | 2024-10 rows | 2024-11 rows |
| --- | ---: | ---: | ---: |
| `estabelecimento` | 62,634,863 | 59,893,805 | 63,333,645 |
| `estabelecimento_enderecos` | 62,634,863 | 59,893,805 | 63,333,645 |
| `empresas` | 59,616,971 | 59,954,871 | 60,294,480 |

The difference between the October and November establishment Parquets is:

`63,333,645 - 59,893,805 = 3,439,840`

This is exactly the number of November establishment insert events.

The same difference appears in `estabelecimento_enderecos`, while the `empresas` dataset follows a normal gradual increase. This indicates that the anomaly is isolated to establishment-level coverage rather than a general database or company-table failure.

The establishment Parquet files were structurally readable and were created by `parquet-cpp-arrow version 25.0.1`. Their row-group counts were consistent with their row counts:

- October: 1,198 row groups.
- November: 1,267 row groups.

### 8. Runtime behavior of the failed reconstruction

The process was observed while processing `2024-11`:

- Python process remained alive but was waiting on PostgreSQL.
- PostgreSQL remained active in the establishment `INSERT` statement.
- CPU usage was approximately 90-99% for much of the run.
- PostgreSQL eventually reported `IO / BufFileRead`, indicating temporary-file I/O.
- Approximately 1.9 GB of temporary files were observed.
- PostgreSQL `work_mem` was `4MB`.
- No lock wait was observed.
- The transaction remained uncommitted for approximately 3 hours and 53 minutes.

The relevant code path is the establishment insert in `code/reconstruct_and_stream_panel.py`. That statement joins November snapshots with the current panel state, company data, Simples data, municipality data, and an aggregation over `public.socios` for the inserted establishment roots.

The code does not expose row-level progress inside this statement. The only durable progress marker is written after the complete monthly delta and export finish.

## Interpretation

The immediate cause of the slow November reconstruction is the exceptional number of establishment insertions. The code does not re-compare every previous month, but it does process all `3,439,840` November establishment insert events and recompute related partner aggregates.

The query is made more expensive by:

1. A large monthly insert set.
2. Aggregation over the large `public.socios` table.
3. Multiple joins against the accumulated panel state and snapshot tables.
4. A low PostgreSQL `work_mem` setting of `4MB`, causing disk-based temporary operations.
5. Lack of recent table statistics reported for the main tables during the audit.
6. No intermediate progress checkpoint inside the long-running SQL statement.

However, query cost alone does not explain why November is so much larger than other months. The key evidence points to a temporary October establishment coverage gap, followed by restoration in November.

## Limitations

The original compressed RFB files for October and November were not present in the local `OUTPUT_FILES` or `EXTRACTED_FILES` directories at audit time. Consequently, this audit cannot determine conclusively whether:

- the October official dump itself omitted the establishments; or
- the local download, extraction, parsing, deduplication, or staging process omitted them.

The available local Parquets are derived ETL outputs, not the original source files.

## Recollection Attempt

On 2026-09-10, a recollection was attempted using the configured Receita Federal WebDAV endpoint for `2024-10`. The operation was intentionally stopped before downloading or modifying any production data.

Observed responses:

- Python `requests` `PROPFIND`: HTTP response body was an HTML `Request Rejected` page instead of a WebDAV XML directory listing.
- `curl` request: `401 Unauthorized` from the `SERPRO+` protected endpoint.
- No new ZIP files were downloaded.
- No production tables, snapshots, or existing Parquet partitions were modified.

Reference checksums for the currently preserved derived Parquets:

| File | SHA-256 |
| --- | --- |
| `ignored_fields/estabelecimento/reference_month=2024-10/part-000.parquet` | `772495c8e9d791ab0c6bcb57e6004418fe17e1776bffa811eed89bfb72242b48` |
| `ignored_fields/estabelecimento/reference_month=2024-11/part-000.parquet` | `15784384277f091fb75fd2e94585b8581d6e90398c5e088188fb021e356442d6` |

The recollection is therefore currently blocked by source access, not by local disk space or database state. A valid download requires a working official endpoint/token or locally supplied ZIP files.

## Alternative Source Verification

Because the official WebDAV endpoint was unavailable, the establishment archives were independently downloaded from the Casa dos Dados mirror:

`https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/`

A new English-language verifier was created at `code/verify_alternative_cnpj_source.py`. It is independent of the original collection ETL and uses the isolated directory `audit_source_verification/`. It does not modify production tables, existing Parquets, or the original collection code.

The verifier downloaded and validated all 20 establishment ZIP parts:

- `2024-10-16`: `Estabelecimentos0.zip` through `Estabelecimentos9.zip`.
- `2024-11-13`: `Estabelecimentos0.zip` through `Estabelecimentos9.zip`.
- All archives passed ZIP CRC validation.
- SHA-256 checksums were recorded in `audit_source_verification/alternative_source_report.json`.

| Source month | Source rows from ZIP members | Existing Parquet rows | Difference | Result |
| --- | ---: | ---: | ---: | --- |
| `2024-10` | 62,983,669 | 59,893,805 | +3,089,864 | MISMATCH |
| `2024-11` | 63,333,645 | 63,333,645 | 0 | MATCH |

This independently confirms that the local October establishment-derived file is incomplete relative to the alternative source, while the local November file has the same row count as the alternative source. The test is based on row counts and archive integrity; it does not yet identify the exact missing October keys. A key-level comparison is the next validation required before rebuilding.

### Key-level comparison

The new English-language key comparator at `code/compare_establishment_keys.py` performed an external-memory comparison using the normalized key `(cnpj_basico, cnpj_ordem, cnpj_dv)`. It did not modify the source ZIPs, reference Parquets, database tables, or the original ETL.

| Month | Source unique keys | Reference unique keys | Source-only keys | Reference-only keys | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| `2024-10` | 62,983,669 | 59,893,805 | 3,089,864 | 0 | MISMATCH |
| `2024-11` | 63,333,645 | 63,333,645 | 0 | 0 | MATCH |

This proves that the October local Parquet is a strict subset of the alternative October source by establishment key: all local October keys are present in the alternative source, and exactly `3,089,864` source keys are missing locally. The complete key lists are stored in `audit_source_verification/key_compare/`, with the October missing keys in `source_only.keys`.

The key comparison report is available at `audit_source_verification/key_compare/key_comparison_report.json`.

### Opening-date analysis

The new English-language analyzer at `code/analyze_establishment_opening_dates.py` extracted `data_inicio_atividade` from the alternative October and November source archives for all `3,089,864` source-only keys.

| Opening-date group | Source-only October keys |
| --- | ---: |
| Opened before `2024-01-01` | 2,692,193 |
| Opened during `2024-01` through `2024-09` | 382,758 |
| Opened during `2024-10` | 14,913 |
| Opened during `2024-11` | 0 |
| Missing opening date | 0 |
| **Total** | **3,089,864** |

Approximately 87.1% of the source-only establishments were opened before 2024, and only approximately 0.5% were opened in October 2024. None were opened in November 2024. The same keys and opening dates are present in the November source, confirming that these are not predominantly newly opened establishments; they are older establishments that were absent from the local October-derived file and then reappeared.

The detailed date distribution is stored in `audit_source_verification/opening_date_analysis/opening_date_report.json`.

## Recommended Next Steps

1. Obtain the official October 2024 establishment and establishment-address files again.
2. Record their file sizes and SHA-256 checksums.
3. Parse or load them into an isolated staging area without modifying production tables.
4. Compare row counts and distinct establishment keys against the current October Parquets.
5. Compare the official October keys against the `3,054,993` October deletion keys.
6. If the official file contains those keys, reprocess October and regenerate its snapshots.
7. Only after correcting October, regenerate the November state and panel partition.
8. Before rerunning, execute `ANALYZE` on the relevant tables and test the November insert with `EXPLAIN (ANALYZE, BUFFERS)` in a safe transaction or isolated database.
9. Consider refactoring the partner aggregation so it is materialized once for the November insert set rather than recomputed as part of the large statement.
10. Add intermediate progress reporting or chunked inserts so a future run exposes progress and can resume safely within a month.
11. Use the key-level comparison output to validate the corrected October staging data before replacing any data.

## Final Assessment

The October-November sequence should be treated as **data-quality incident under investigation**, not as a normal monthly change. The reconstruction results through `2024-09` are not implicated by this specific anomaly. The `2024-10` and `2024-11` panel outputs should not be considered final until the official October source data is revalidated and the affected months are rebuilt.
