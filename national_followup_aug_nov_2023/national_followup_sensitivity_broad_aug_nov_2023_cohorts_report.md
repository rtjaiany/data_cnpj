# Broad Source-Gap Sensitivity Extract

This file reproduces the eligibility rule used by the existing October sensitivity code.

## Rule

A cohort establishment is a sensitivity candidate when it was:

1. nationally active in September 2024 (`pre_existing_active_in_september`);
2. absent from the canonical October 2024 state; and
3. present in São Paulo in November 2024.

This is intentionally the broad rule used by the existing code. It does not require the establishment to have been in São Paulo in September.

## Output

- Rows: **1,612,140**
- Tagged October rows: **2,622**
- Proxy month: **2024-11**
- Parquet SHA-256: `85ecfc5e245697a8c2956987db96b2cb1923cac91c059d033fde71155c7aba8b`
- Canonical October fields are preserved unchanged.
- Proxy fields are populated separately and never replace the canonical state.

## Sensitivity columns

- `source_gap_candidate`
- `source_gap_basis`
- `sensitivity_proxy_nationally_present`
- `sensitivity_proxy_uf`
- `sensitivity_proxy_municipio`
- `sensitivity_proxy_situacao_cadastral`
- `sensitivity_proxy_data_situacao_cadastral`
- `sensitivity_proxy_motivo_situacao_cadastral`
- `sensitivity_proxy_reference_month`
