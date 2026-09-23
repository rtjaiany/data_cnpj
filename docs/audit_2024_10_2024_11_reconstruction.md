# Audit Report: October-November 2024 Reconstruction

**Audit date:** 2026-09-23
**Scope:** Official October-November 2024 establishment transition and panel reconstruction

## Executive Summary

The October-November sequence remains a reconstruction data-quality incident under investigation. The official October unique establishment key set matches the local October Parquet exactly. The earlier alternative-source comparison was discarded and is not used for reconstruction.

The investigation now focuses on the official source-to-snapshot transition and the logic that generated the October deletes and November inserts.

## Processing Status

- The reconstruction completed through `2024-10`.
- The attempted `2024-11` reconstruction was stopped after an unreasonably long PostgreSQL insert.
- The last valid panel checkpoint remains `2024-10`.
- Existing production tables and Parquet outputs were not modified by the audit.
- The official October and November source listings were accessible through the VPN on 2026-09-23.

## Official Source Validation

The official WebDAV endpoint returned valid `207 Multi-Status` XML responses for `2024-10` and `2024-11`.

The official October files contain `64,528,601` raw rows. Due to overlap between archive parts, `4,634,796` rows are duplicate keys. After deduplication, the official October source contains exactly `59,893,805` unique establishment keys, matching the local October Parquet.

The revised official archive `2024-10/Estabelecimentos3.zip` was validated separately:

| Field | Value |
| --- | ---: |
| Bytes | 456,504,909 |
| Rows | 6,298,367 |
| CRC | PASS |
| SHA-256 | `6067c36753829f1c765a690f9abf7a7a9aa52fb0c30bad0182be74854259c070` |

## Official State Transition

| Comparison | Keys |
| --- | ---: |
| Shared by official October and November | 59,893,805 |
| Present only in official November | 3,439,840 |
| Present only in official October | 0 |
| October `DELETE` events in `public.snapshots` | 3,054,993 |
| November `INSERT` events in `public.snapshots` | 3,439,840 |

The official November state contains `3,439,840` keys absent from official October. This confirms that the large November insertion count exists in the official state transition, not only in the discarded alternative-source analysis.

## Opening-Date Evidence

The official November-only set was analyzed using `data_inicio_atividade`.

| Opening-date group | Keys |
| --- | ---: |
| Opened before `2024-01-01` | 2,693,151 |
| Opened during 2024 | 746,689 |
| Opened during November 2024 | 103,235 |
| Missing opening date | 0 |
| **Total** | **3,439,840** |

The subset that was deleted in October and reinserted in November contains `3,054,993` keys. Of these, `2,692,100` were opened before 2024 and none were opened in November 2024.

This is strong evidence that the mass delete/reinsert pattern is not ordinary formation of new establishments. It should be treated as a source-state or snapshot-transition anomaly until the complete payload and staging lineage audit explains it.

The official opening-date results are recorded in this report. Derived
intermediate files from the discarded alternative-source workflow were removed
from the computer.

## Interpretation

The official October Parquet is not missing unique establishment keys relative to the official October source. Therefore, the alternative-source hypothesis of an incomplete October file has been withdrawn.

The remaining questions are:

1. Why did `3,439,840` official November keys not exist in official October?
2. Why do `3,054,993` of those keys correspond to October `DELETE` events?
3. Were the October and November staging tables complete and isolated?
4. Did processed-file checkpoints, deduplication, or deletion detection alter the event stream?
5. Are the full row payloads consistent with genuine status changes or with source-state revisions?

## Next Steps

1. Compare official October and November payloads for all affected keys.
2. Compare official state differences with snapshot events by table and change type.
3. Audit staging completeness, `processed_files`, download checkpoints, deduplication, and deletion logic.
4. Reproduce October and November snapshot generation in an isolated schema or database.
5. Rebuild the panel only if the official source-to-snapshot comparison identifies an ETL or snapshot-generation error.
6. Before any rerun, use `ANALYZE` and a safe `EXPLAIN (ANALYZE, BUFFERS)` test.
7. Keep the November insert resumable with intermediate progress checkpoints.

## Final Assessment

The official October unique key set matches the local October Parquet. The earlier alternative-source evidence is excluded from the reconstruction and has been removed from the active audit workflow.

The `2024-10` and `2024-11` panel outputs should remain provisional until the official source-to-snapshot transition and the mass delete/reinsert pattern are explained.
