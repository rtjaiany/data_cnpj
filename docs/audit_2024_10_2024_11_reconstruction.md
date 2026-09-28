# Audit Report: October-November 2024 Reconstruction

**Audit date:** 2026-09-23
**Scope:** Official October-November 2024 establishment transition and panel reconstruction

## Executive Summary

- **Main Issue:** During the October–November 2024 CNPJ data transition, a major data-quality anomaly occurred: 3,054,993 establishment records abruptly disappeared in October (`DELETE` events) and 3,439,840 records were added in November (`INSERT` events), with 3,054,993 being the exact same pre-existing establishments (2,977,484 having identical payloads). This mass event caused database processing stalls during panel reconstruction.
- **Audit Purpose:** To determine whether this anomaly stemmed from local pipeline/ETL corruption (e.g., an incomplete local file or snapshot bug) or existed in the official Receita Federal monthly WebDAV archives; to reconstruct canonical panel checkpoints for October and November 2024 without mutating production tables; and to construct sensitivity panels for research robustness testing.

**Key Audit Steps:**

1. _Source Reconciliation:_ Extracted and validated unique keys from official WebDAV archives (Sep, Oct, Nov 2024) and compared them against local Parquets.
2. _Payload & Snapshot Alignment:_ Joined full payloads across months for the 3.05M target records and reconciled snapshot event payloads with raw official source data.
3. _Batched Canonical Reconstruction:_ Applied isolated 64-bucket hash-partitioned processing to safely rebuild the canonical November 2024 panel state.
4. _Sensitivity Panel Construction:_ Created tagged October 2024 sensitivity panels carrying forward pre-existing active September records that reappeared in November.

**Main Results:**

- _100% Official Source Alignment:_ Local Parquets match official WebDAV source key sets exactly across all months (`59,893,805` keys in October; `63,333,645` in November). The initial hypothesis of an incomplete local October file was disproven and withdrawn.
- _Source-Level Anomaly:_ The mass deletion and re-insertion originate entirely within the official Receita Federal published files, not local pipeline errors. Local snapshot payloads matched raw official source payloads for 3,054,989 out of 3,054,993 records (99.999%).
- _Non-Creation Disappearances:_ Over 88% (2.69M) of reappearing establishments had opening dates prior to 2024, confirming a source publication gap rather than genuine business creation.
- _Clean Isolated Reconstruction:_ November canonical panel reconstruction completed cleanly (63,333,645 keys) without touching production. São Paulo benchmark exports passed all quality checks (18,257,569 canonical keys; 17,759,642 October sensitivity keys with 503,450 tagged candidate gap rows).

**Conclusions:**

- The local ETL pipeline is operating correctly and faithfully capturing the official source. The October drop is an upstream Receita Federal publication artifact/gap.
- **Dual-Panel Policy:** Canonical panels preserve strict fidelity to official monthly source releases (retaining the October gap). Sensitivity panels with explicit `source_gap_candidate = true` flags are provided to assess research sensitivity.
- **Provisional Status:** The `2024-10` and `2024-11` panel reconstructions are completed and validated, but remain marked as provisional pending upstream clarification.

## Processing Status

- Existing production tables and Parquet outputs were not modified by the audit.
- The official October and November source listings were accessible through the UFRGS VPN on 2026-09-23. It was necessary to use the VPN because the access from different countries is blocked to the federal government website (I asked for a friend in France to try to download it, and the block persists for her as well), and I was thinking that the RFB website is broken or in maintenance.

## Official Source Validation

The official WebDAV endpoint returned valid `207 Multi-Status` XML responses for `2024-10` and `2024-11`.

The official October files contain `64,528,601` raw rows. Due to overlap between archive parts, `4,634,796` rows are duplicate keys. After deduplication, the official October source contains exactly `59,893,805` unique establishment keys, matching the local October Parquet.

The revised official archive `2024-10/Estabelecimentos3.zip` was validated separately:

| Field   |                                                              Value |
| ------- | -----------------------------------------------------------------: |
| Bytes   |                                                        456,504,909 |
| Rows    |                                                          6,298,367 |
| CRC     |                                                               PASS |
| SHA-256 | `6067c36753829f1c765a690f9abf7a7a9aa52fb0c30bad0182be74854259c070` |

## Official State Transition

| Comparison                                     |       Keys |
| ---------------------------------------------- | ---------: |
| Shared by official October and November        | 59,893,805 |
| Present only in official November              |  3,439,840 |
| Present only in official October               |          0 |
| October `DELETE` events in `public.snapshots`  |  3,054,993 |
| November `INSERT` events in `public.snapshots` |  3,439,840 |

The official November state contains `3,439,840` keys absent from official October. This confirms that the large November insertion count exists in the official state transition, not only in the discarded alternative-source analysis.

### Official key-level reconciliation

The official ZIP keys were extracted and compared with the local derived Parquets using `code/compare_official_transition.py`.

| Month     | Official unique keys | Local Parquet unique keys | Official-only | Local-only | Result |
| --------- | -------------------: | ------------------------: | ------------: | ---------: | ------ |
| `2024-09` |           62,634,863 |                62,634,863 |             0 |          0 | MATCH  |
| `2024-10` |           59,893,805 |                59,893,805 |             0 |          0 | MATCH  |
| `2024-11` |           63,333,645 |                63,333,645 |             0 |          0 | MATCH  |

The official month-to-month key transitions were:

| Transition             | Previous-only keys | Current-only keys |
| ---------------------- | -----------------: | ----------------: |
| `2024-09` to `2024-10` |          3,054,993 |           313,935 |
| `2024-10` to `2024-11` |                  0 |         3,439,840 |

These counts match the corresponding official snapshot event counts. This demonstrates that the local Parquets faithfully represent the official source key sets and that the mass transition is present in the official source data itself. The earlier conclusion that the local October file was incomplete is withdrawn.

The detailed comparison is recorded in `audit_source_verification/official_transition/official_transition_report.json`.

## Filtered Official Payload Comparison

The official September and November payloads were compared for the `3,054,993` establishments that disappear between September and October and reappear in November. The comparison used `code/compare_filtered_official_payloads.py`, filtering target keys before writing intermediate data to keep disk usage bounded.

| Measure                                                           |    Result |
| ----------------------------------------------------------------- | --------: |
| Target keys                                                       | 3,054,993 |
| Payload pairs found in both official months                       | 3,054,993 |
| Identical payload pairs                                           | 2,977,484 |
| Valid payload pairs with changes                                  |    77,508 |
| Structurally invalid payload pairs excluded from field statistics |         1 |

The single excluded key is `37257765|0001|11`, whose situation field is not a valid cadastral code in the filtered representation. It remains recorded as an anomaly.

Among valid payload pairs, the most frequent changed fields were `data_situacao_cadastral` (40,252), `situacao_cadastral` (40,226), `motivo_situacao_cadastral` (40,222), and `cnae_fiscal_secundaria` (20,484). The main valid situation transitions were:

- `02 -> 02`: 1,673,021 records;
- `08 -> 08`: 908,303 records;
- `04 -> 04`: 427,379 records;
- `02 -> 08`: 23,594 records;
- `04 -> 02`: 11,377 records;
- `04 -> 08`: 4,831 records.

The high number of identical payloads, together with the large group that was active in September and had an opening date before October, supports treating the October absence as a possible source-state gap. It does not prove that the records should be restored in the canonical official panel. The complete filtered report is `audit_source_verification/official_filtered_payloads/filtered_official_payload_report.json`.

## Snapshot-to-Official Reconciliation

The October establishment `DELETE` previous payloads and November establishment `INSERT` new payloads were reconciled against the official filtered payloads using `code/reconcile_snapshots_with_official.py`.

| Snapshot event                    | Events examined | Exact payload matches | Unmatched structural records | Field mismatches |
| --------------------------------- | --------------: | --------------------: | ---------------------------: | ---------------: |
| October `DELETE` previous payload |       3,054,993 |             3,054,989 |                            4 |                0 |
| November `INSERT` new payload     |       3,054,993 |             3,054,989 |                            4 |                0 |

Numeric codes were normalized during comparison, so values such as `02` and `2` were treated as equivalent. For all structurally valid records, the snapshots reproduce the official source payloads exactly in the fields audited. The four unmatched records are the same structural anomalies identified in the filtered payload analysis; they are not evidence of a broad ETL mismatch.

This confirms that the mass October `DELETE` and November `INSERT` events were generated consistently with the official source state transition. The current evidence points to an anomalous transition in the official monthly source states, not to the snapshot process inventing or corrupting those events. The reconciliation report is `audit_source_verification/snapshot_payload_reconciliation.json`.

## Batched Canonical Reconstruction

The canonical November state was reconstructed in an isolated table using `code/reconstruct_november_batched.py`. The optimized process copied the validated October state and applied November changes in 64 hash buckets with session-local `work_mem=256MB`, committing and checkpointing each bucket.

| Measure                                 |                                               Result |
| --------------------------------------- | ---------------------------------------------------: |
| Expected November establishment inserts |                                            3,439,840 |
| Completed buckets                       |                                                64/64 |
| Final isolated state rows               |                                           63,333,645 |
| Official November unique keys           |                                           63,333,645 |
| Target table                            | `analytics.panel_checkpoint_state_canonical_2024_11` |

The batched process completed without modifying the original `analytics.panel_checkpoint_state` table or existing Parquet outputs. Its final row count exactly matches the official November establishment key count.

The São Paulo export was generated by `code/export_canonical_november_sp.py` at:

`reconstructed_panel_canonical/reference_month=2024-11/part-000.parquet`

Final export validation:

- Rows: `18,257,569`.
- Unique establishment keys: `18,257,569`.
- Columns: `36`.
- Compression: Snappy.
- File size: `477,487,320` bytes.
- Quality status: `PASS`.

## Sensitivity Panel for October

An explicitly tagged sensitivity panel was generated by `code/build_sensitivity_panel.py` at:

`reconstructed_panel_sensitivity/reference_month=2024-10/part-000.parquet`

The sensitivity construction copied the canonical October São Paulo partition and appended only candidates tagged `pre_existing_active_in_september` that reappeared in November. Candidate rows use their observed November state as a proxy and are explicitly marked with:

- `source_gap_candidate = true`;
- `source_gap_basis = "active_sep_reappeared_nov"`.

Validation results:

- Canonical October São Paulo rows: `17,256,192`.
- Appended sensitivity candidates: `503,450`.
- Total sensitivity rows: `17,759,642`.
- Unique establishment keys: `17,759,642`.
- Columns: `38`.
- Quality status: `PASS`.

This is a sensitivity analysis, not a replacement for the official October panel. The appended values were observed in November and carried backward only to test the effect of the source-gap hypothesis.

The same tagging was applied to the canonical November São Paulo partition at:

`reconstructed_panel_sensitivity/reference_month=2024-11/part-000.parquet`

The São Paulo panel comparison was generated by `code/compare_sp_panels.py` and recorded in `audit_source_verification/sp_panel_comparison.json`:

| Partition            |       Rows | Unique keys | Candidate flags | Columns | Status |
| -------------------- | ---------: | ----------: | --------------: | ------: | ------ |
| Canonical October    | 17,256,192 |  17,256,192 |               0 |      36 | PASS   |
| Sensitivity October  | 17,759,642 |  17,759,642 |         503,450 |      38 | PASS   |
| Canonical November   | 18,257,569 |  18,257,569 |               0 |      36 | PASS   |
| Sensitivity November | 18,257,569 |  18,257,569 |         503,450 |      38 | PASS   |

No national panel was generated. Both sensitivity partitions are explicitly marked and must be used only for robustness analysis.

## São Paulo Canonical-versus-Sensitivity Summary

The statistical comparison was generated by `code/summarize_sp_panel_sensitivity.py` and saved to `audit_source_verification/sp_panel_sensitivity_summary.json`.

For October, the sensitivity panel adds `503,450` establishments. The added rows do not introduce new municipalities or principal CNAEs: both panels contain `645` municipalities and `1,327` principal CNAEs. The status-count changes among added rows are:

| Status code | Added rows |
| ----------- | ---------: |
| `02`        |    496,232 |
| `08`        |      7,173 |
| `03`        |         42 |
| `04`        |          1 |
| `01`        |          2 |

For November, the canonical and sensitivity panels have identical row counts and status distributions; only the source-gap flags differ. This makes the sensitivity comparison a clean test of the October gap hypothesis rather than a second November population.

## Opening-Date Evidence

The official November-only set was analyzed using `data_inicio_atividade`.

| Opening-date group          |          Keys |
| --------------------------- | ------------: |
| Opened before `2024-01-01`  |     2,693,151 |
| Opened during 2024          |       746,689 |
| Opened during November 2024 |       103,235 |
| Missing opening date        |             0 |
| **Total**                   | **3,439,840** |

The subset that was deleted in October and reinserted in November contains `3,054,993` keys. Of these, `2,692,100` were opened before 2024 and none were opened in November 2024.

This is strong evidence that the mass delete/reinsert pattern is not ordinary formation of new establishments. It should be treated as a source-state or snapshot-transition anomaly until the complete payload and staging lineage audit explains it.

The official opening-date results are recorded in this report.

## Official Disappearance Opening-Date Analysis

The official September records were joined to the `3,054,993` establishment keys that disappear between September and October. The analysis was performed by `code/analyze_official_disappearances.py` and wrote only to `audit_source_verification/official_disappearances/`.

All `3,054,993` disappearance keys were found in the official September source. Every one had a `data_inicio_atividade` before October 2024.

| Candidate tag                          | September status          |          Keys |
| -------------------------------------- | ------------------------- | ------------: |
| `pre_existing_active_in_september`     | `02`                      |     1,696,845 |
| `pre_existing_non_active_in_september` | `01`, `03`, `04`, or `08` |     1,358,148 |
| **Total**                              |                           | **3,054,993** |

This is important evidence, but it is not proof that the records should be restored to the official October state. It shows that the disappearing records were not newly formed establishments and that more than half were marked active in the official September snapshot. The official source still does not reveal whether their absence in October represents a genuine temporary cadastral state, a source publication artifact, or an upstream processing issue at the Receita Federal.

### Recommended treatment

The canonical reconstruction should remain faithful to the official monthly files and preserve the October absence as observed. Separately, create a sensitivity version in which records tagged `pre_existing_active_in_september` are carried forward into October only when they reappear in November. Those rows should receive an explicit flag such as `source_gap_candidate = true` and must never be silently merged into the canonical panel.

The non-active group should not be automatically carried forward. Its members require additional payload and cadastral-history evidence before any imputation is considered.

The detailed tagged file is `audit_source_verification/official_disappearances/disappeared_establishments_tagged.tsv`, and the summary is `audit_source_verification/official_disappearances/official_disappearance_opening_date_report.json`.

## Interpretation

The official October Parquet is not missing unique establishment keys relative to the official October source. Therefore, the alternative-source hypothesis of an incomplete October file has been withdrawn.

The remaining questions are:

1. Why did `3,439,840` official November keys not exist in official October?
2. Why do `3,054,993` of those keys correspond to October `DELETE` events?
3. Were the October and November staging tables complete and isolated?
4. Are the full row payloads consistent with genuine status changes or with source-state revisions?

## Final Assessment

The official October unique key set matches the local October Parquet.

The `2024-10` and `2024-11` panel outputs should remain provisional until the official source-to-snapshot transition and the mass delete/reinsert pattern are explained.
