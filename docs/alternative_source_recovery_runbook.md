# Alternative Source Recovery Runbook

**Status:** In progress
**Last updated:** 2026-09-21
**Scope:** October-November 2024 establishment coverage incident

## Objective

Recover and validate the affected October 2024 establishment data while the
official Receita Federal distribution endpoint remains unavailable. The
alternative source is used for a controlled, reproducible investigation only.
It must not silently replace the official source or modify production data.

## Evidence Preserved

The independent source audit is stored under `audit_source_verification/`.
The Casa dos Dados mirror provided 20 establishment archives:

- October source directory: `2024-10-16`, ten archive parts.
- November source directory: `2024-11-13`, ten archive parts.
- All archives passed ZIP CRC validation.
- SHA-256 checksums and row counts are recorded in
  `alternative_source_report.json`.

The key comparison recorded:

| Month | Source unique keys | Local unique keys | Source-only keys | Reference-only keys | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| 2024-10 | 62,983,669 | 59,893,805 | 3,089,864 | 0 | MISMATCH |
| 2024-11 | 63,333,645 | 63,333,645 | 0 | 0 | MATCH |

Therefore, the local October Parquet is a strict subset of the alternative
October source by establishment key. The November Parquet matches the
alternative source by key and row count.

## Controls

1. Keep the original Parquet files and production tables unchanged.
2. Store all alternative downloads, checksums, reports, and derived key lists
   under `audit_source_verification/`.
3. Use staging or a separate database for any rebuilt snapshots or panels.
4. Record source URLs, source directory names, retrieval timestamps, file sizes,
   and SHA-256 checksums.
5. Do not classify the mirror as an official source without independent
   confirmation from Receita Federal.
6. Do not repair the panel by manually deleting October events or copying
   November rows backward.

## Current Next Check

Run `code/compare_missing_keys_to_snapshots.py`. It loads the alternative-only
October keys into a PostgreSQL temporary table and compares them with October
`estabelecimento` `DELETE` events in `public.snapshots`. The transaction is
read-only and rolls back before the connection closes.

On 2026-09-21, the script passed syntax and workspace diagnostics. Execution
was blocked in the current shell because the system Python lacks `psycopg2` and
`python-dotenv`, the project virtual environment is located on a temporarily
unresponsive synchronized path, and the `psql` client is not available on the
shell `PATH`. No database connection was made and no data was changed.

When the project environment is available, execute:

```bash
source .venv/bin/activate
python code/compare_missing_keys_to_snapshots.py
```

If the virtual environment remains inaccessible, install or expose the
project's declared dependencies in a local Python environment and run the same
command there. Do not copy credentials into scripts or reports.

The expected result is that all `3,089,864` source-only keys match October
deletion events. A nonzero unmatched count must stop the recovery and trigger a
new investigation of the source, parser, or snapshot generation process.

## Recovery Sequence

1. Complete the source-only-to-snapshot deletion comparison.
2. Obtain or reconstruct the October establishment and address staging files
   from the alternative archives in an isolated workspace.
3. Compare normalized keys, row counts, duplicate counts, and required fields.
4. Re-run October change detection in staging and inspect the resulting delta
   counts before any panel reconstruction.
5. Rebuild October and November in a separate analytical schema or database.
6. Run `ANALYZE` and use a safe `EXPLAIN (ANALYZE, BUFFERS)` test before the
   large November insert.
7. Validate continuity against September and November, including establishment
   counts, key uniqueness, and the disappearance of the artificial mass
   deletion/reinsertion pattern.
8. Publish the result as an alternative-source reconstruction with an explicit
   methodological limitation.
9. Replace it with an official-source reconstruction only after the official
   files become available and pass the same checks.

## Resumable Preparation Orchestrator

The new English-language orchestrator is `code/run_resumable_recovery.py`. It is
independent from the original ETL and writes only to
`audit_source_verification/resumable_recovery/`.

It provides three checkpointed preparation stages:

1. `manifest`: discovers the ten establishment archives for each month and
   writes `recovery_manifest.json`.
2. `validate`: validates each ZIP independently, records CRC success, SHA-256,
   member metadata, and row count, then atomically updates the manifest after
   each archive.
3. `keys`: extracts and externally sorts establishment keys one archive at a
   time, recording completion per archive.

The first two stages have been executed successfully for all 20 archives. A
second validation run confirmed resume behavior: all archives were reported as
already complete and were not reprocessed.

Example commands:

```bash
python code/run_resumable_recovery.py --stage manifest
python code/run_resumable_recovery.py --stage validate
python code/run_resumable_recovery.py --stage keys
```

The `keys` stage should be run only after checking available disk space. It
creates sorted intermediate key files and can require tens of gigabytes. The
source ZIPs, original Parquets, production database, and original ETL remain
unchanged throughout these stages.

## Acceptance Criteria

The affected months can be treated as provisionally usable only when:

- the source-only key comparison has no unexplained unmatched keys;
- staging files pass structural, key, and duplicate checks;
- October and November rebuild successfully outside production;
- no persistent production table or existing Parquet was modified;
- all source and transformation metadata are preserved; and
- the dissertation documents the source outage and alternative-source use.

Until then, September 2024 remains the last fully validated panel month.
