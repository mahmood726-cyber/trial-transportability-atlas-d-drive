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
  - curated IHME, WHO, and World Bank context adapters implemented in `source_adapters.py`
  - deterministic ISO3 resolution implemented in `country_iso3.py`
  - country-year context join materialization implemented in `context_join.py`
  - transportability scoring and evidence-gap materialization implemented in `transportability.py`
  - static dashboard generation implemented in `dashboard.py` and `generate_dashboard.py`
  - topic, bridge, effect-candidate, context, transportability, and dashboard tests are green

## Latest verification

- `python -m pytest -q tests\test_dashboard.py tests\test_ui.py` -> `3 passed in 1.38s`
- `python -m pytest -q` -> `40 passed in 32.53s` on the D-drive repo on 2026-04-20
- real filtered bridge outputs materialized to:
  - `outputs/sacubitril_valsartan_hfref/trial_country_year.parquet`
  - `outputs/sacubitril_valsartan_hfref/trial_outcomes_long.parquet`
  - `outputs/sacubitril_valsartan_hfref/effect_candidates.parquet`
  - `outputs/sacubitril_valsartan_hfref/run_manifest.json`
  - `outputs/sacubitril_valsartan_hfref/context_joined.parquet`
  - `outputs/sacubitril_valsartan_hfref/context_join_manifest.json`
  - `outputs/sacubitril_valsartan_hfref/transportability_country_year.parquet`
  - `outputs/sacubitril_valsartan_hfref/evidence_gap_summary.parquet`
  - `outputs/sacubitril_valsartan_hfref/evidence_gap_summary.md`
  - `outputs/sacubitril_valsartan_hfref/transportability_manifest.json`
  - `dashboard/transportability_dashboard.html`

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

- `4,858` context-joined rows written from the current D-drive `trial_country_year` output
- `4,856` joined rows have source-backed multi-source context
- `0` rows failed ISO3 resolution on the trial side
- distinct sources in the joined surface:
  - `ihme_burden`
  - `ihme_population`
  - `ihme_sdi`
  - `wb_gdp`
  - `wb_governance`
  - `wb_hnp`
  - `wb_population`
  - `wb_poverty`
  - `wb_uhc`
  - `who_ghed`
  - `who_gho`
- distinct measures in the joined surface:
  - `Alcohol, total per capita (15+) consumption (in litres of pure alcohol) (SDG Indicator 3.5.2), three-year average`
  - `Crude suicide rates (per 100 000 population)`
  - `Current health expenditure (% of GDP)`
  - `Current health expenditure per capita (USD)`
  - `DALYs (Disability-Adjusted Life Years)`
  - `Deaths`
  - `Domestic general government health expenditure (% of current health expenditure)`
  - `GDP per capita (current US$)`
  - `Gini index`
  - `Government Effectiveness: Estimate`
  - `Increase in poverty gap at $1.90 ($ 2011 PPP) poverty line due to out-of-pocket health care expenditure (% of poverty line)`
  - `Life expectancy at birth (years)`
  - `Out-of-pocket expenditure (% of current health expenditure)`
  - `Physicians (per 1,000 people)`
  - `Population, total`
  - `YLDs (Years Lived with Disability)`
  - `YLLs (Years of Life Lost)`
  - `population`
  - `sdi`

Current available-row counts by source:

- `ihme_burden`: `1,528`
- `who_gho`: `907`
- `who_ghed`: `744`
- `ihme_population`: `573`
- `wb_population`: `192`
- `wb_governance`: `190`
- `ihme_sdi`: `186`
- `wb_poverty`: `166`
- `wb_hnp`: `164`
- `wb_uhc`: `14`

Known missing context rows are fail-closed:

- `Egypt` in `2025`
- `Saudi Arabia` in `2026`

## Latest transportability state

- `176` scored `transportability_country_year` rows
- `65` country-level `evidence_gap_summary` rows
- `11` deterministic core signals scored from the multi-source context surface:
  - `daly_rate`
  - `death_rate`
  - `population`
  - `sdi`
  - `suicide_rate`
  - `alcohol_per_capita`
  - `health_expenditure_gdp`
  - `out_of_pocket_share`
  - `governance_effectiveness`
  - `gini_index`
  - `physicians_per_1000`

Highest latest-year fail-closed gaps in the current D-drive outputs:

- `Egypt` (`2025`) -> `latest_transportability_score=0.0`
- `Saudi Arabia` (`2026`) -> `latest_transportability_score=0.0`
- `Spain` (`2024`) -> `latest_transportability_score=0.0`
- `Tanzania` (`2024`) -> `latest_transportability_score=0.0`
- `Hong Kong` (`2014`) -> `latest_priority_gap_score=0.969697`

Interpretation rule:

- where source-backed context is absent for a trial country-year, the score stays at `0.0`; the pipeline does not impute forward or backfill context values

## Latest dashboard state

- `dashboard/transportability_dashboard.html` is now generated from:
  - `run_manifest.json`
  - `context_join_manifest.json`
  - `transportability_manifest.json`
  - `transportability_country_year.parquet`
  - `evidence_gap_summary.parquet`
- the dashboard shows:
  - AACT snapshot identifier
  - materialized-output update timestamp from the live manifests
  - context source badges and core-signal badges
  - top latest-year gaps and strongest current support tables
  - explicit fail-closed notice text
- browser smoke on `http://127.0.0.1:8000/dashboard/transportability_dashboard.html` is clean:
  - `0` console errors after adding an inline favicon
  - curated title now renders as `Sacubitril/Valsartan in HFrEF`
- it no longer uses the stale synthetic regional PEY surface
  or hardcoded `D:` paths inside the reporting logic

## Next bounded step

- add a lightweight publish-ready wrapper around `dashboard/transportability_dashboard.html`
- keep the phase-1 topic and source surface frozen while the reporting layer is reviewed for Pages-ready publishing
