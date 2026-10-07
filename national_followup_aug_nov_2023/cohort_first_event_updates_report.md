# First-Event `UPDATE` Cases in the Frozen Cohort

## Purpose

The frozen August-November 2023 SP entrant cohort contains 134,345 distinct establishment keys. The chronological national replay found that 134,343 keys first appeared through an `INSERT`, while two keys first appeared in the targeted event stream through an `UPDATE`.

These two records are documented separately so they can be excluded by an external analyst if the H1Y design requires an `INSERT`-only cohort.

## Identified establishments

| CNPJ | Origin month | First event | Previous state | New state |
| --- | --- | --- | --- | --- |
| `60.746.948/1032-72` | 2023-09 | `UPDATE` in 2023-09 | UF `RJ`, municipality `5921`, status `08`, activity start `1978-04-28` | UF `SP`, municipality `7107`, status `02`, activity start `2023-09-06` |
| `60.746.948/1174-94` | 2023-10 | `UPDATE` in 2023-10 | UF `PR`, municipality `7545`, status `08`, activity start `1979-06-06` | UF `SP`, municipality `7075`, status `02`, activity start `2023-10-06` |

The two records share `cnpj_basico = 60746948` but have different establishment branches (`cnpj_ordem`), so they are two distinct establishment keys.

## Interpretation

These are not accidental duplicate rows. They are pre-existing establishment keys whose observed state changed from an earlier non-SP/inactive state to an active SP state in the origin month. They satisfy the frozen cohort definition based on the origin-month panel, but their first targeted snapshot event is an `UPDATE` rather than an `INSERT`.

The canonical national follow-up retained both records because the frozen cohort was defined from the origin-month state, not from the first snapshot event type. The chronological replay initialized each record from `conteudo_anterior` and then applied the `UPDATE` using `conteudo_novo`; this avoids dropping a valid origin-month entrant or inventing a state.

If the H1Y analysis requires an `INSERT`-only population, the analyst can remove these two CNPJ keys using [cohort_first_event_updates.tsv](./cohort_first_event_updates.tsv). That produces:

- establishments: `134,343`;
- expected inclusive-horizon rows: `1,612,116`;
- removed establishment-month rows: `24`.

The default delivered files retain the documented frozen population of `134,345` establishments.

## Reproduction references

- Frozen cohort file: [frozen_sp_entrant_cohorts_aug_nov_2023.tsv](./frozen_sp_entrant_cohorts_aug_nov_2023.tsv)
- Canonical replay summary: [national_followup_canonical_aug_nov_2023_cohorts.forward.summary.json](./national_followup_canonical_aug_nov_2023_cohorts.forward.summary.json)
- Detailed CNPJ list and event payload summary: [cohort_first_event_updates.tsv](./cohort_first_event_updates.tsv)
