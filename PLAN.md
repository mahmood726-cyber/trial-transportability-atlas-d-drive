# Trial Transportability Atlas Plan

## Goal

Build a deterministic local synthesis pipeline that links multinational trial
evidence from AACT to country-year burden and system context from the IHME,
WHO, and World Bank lakehouses, then ship one narrow phase-1 demo.

## Current local ingredients

- `D:\AACT-storage\AACT\2026-04-12`
- `D:\Projects\ihme-data-lakehouse`
- `D:\Projects\who-data-lakehouse`
- `D:\Projects\wb-data-lakehouse`

## Phase-1 topic freeze

Accepted topic: `sacubitril/valsartan in HFrEF`

Strict inclusion:

- active intervention text matching sacubitril/valsartan or Entresto
- HFrEF evidence from condition text or brief summary

Strict exclusion:

- placebo-only intervention rows
- HFpEF / preserved EF / diastolic HF
- pulmonary hypertension without HFrEF evidence
- resistant hypertension or metabolic-only studies

## Milestone status

- Milestone 0 foundations present:
  - `project_paths.py`
  - `contracts.py`
  - path and contract tests
- Milestone 2 foundations present:
  - `aact_bridge.py`
  - real-snapshot header validation
  - narrow `trial_country_year` and `trial_outcomes` extractors
- Current step:
  - exact topic filtering implemented in `topics.py`
  - filtered bridge materialization implemented in `materialize.py`
  - first IHME context adapter implemented in `source_adapters.py`
  - deterministic ISO3 resolution implemented in `country_iso3.py`
  - country-year context join materialization implemented in `context_join.py`
  - topic, bridge, effect-candidate, and context tests are green

## Latest verification

- `python -m pytest -q` -> `27 passed in 46.31s` on the D-drive repo on 2026-04-19
- real filtered bridge outputs materialized to:
  - `outputs/sacubitril_valsartan_hfref/trial_country_year.parquet`
  - `outputs/sacubitril_valsartan_hfref/trial_outcomes_long.parquet`
  - `outputs/sacubitril_valsartan_hfref/effect_candidates.parquet`
  - `outputs/sacubitril_valsartan_hfref/run_manifest.json`
  - `outputs/sacubitril_valsartan_hfref/context_joined.parquet`
  - `outputs/sacubitril_valsartan_hfref/context_join_manifest.json`

## Latest effect-candidate state

- `46` selected `NCT` ids
- `197` `trial_country_year` rows
- `2,715` `trial_outcomes_long` rows
- `953` `effect_candidates` rows
- `80` strict comparable candidates

Current family mix from the D-drive AACT snapshot:

- `binary_event_count`: dominant family, mostly fail-closed on comparability
- `continuous_mean`: main strict-comparable family
- `binary_participant_count`: smaller strict-comparable family

Note:

- The D-drive AACT snapshot produced materially fewer outcome and candidate
  rows than the C-drive fallback mirror. Treat the D-drive counts as the
  authoritative current local baseline for this repo.

## Latest context-join state

- `2,293` context-joined rows written from the D-drive `trial_country_year` output
- `2,287` joined rows have source-backed IHME context
- `0` rows failed ISO3 resolution on the trial side
- distinct sources in the joined surface:
  - `ihme_burden`
  - `ihme_population`
  - `ihme_sdi`
- distinct measures in the joined surface:
  - `DALYs (Disability-Adjusted Life Years)`
  - `Deaths`
  - `YLDs (Years Lived with Disability)`
  - `YLLs (Years of Life Lost)`
  - `population`
  - `sdi`

Known missing context rows are fail-closed:

- `Hong Kong` in `2009`
- `Hong Kong` in `2014`
- `Spain` in `2024`
- `Tanzania` in `2024`
- `Egypt` in `2025`
- `Saudi Arabia` in `2026`
