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
  - topic, bridge, effect-candidate, context, and transportability tests are green

## Latest verification

- `python -m pytest -q` -> `35 passed in 77.75s` on the D-drive repo on 2026-04-20
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

- `4,666` context-joined rows written from the D-drive `trial_country_year` output
- `4,664` joined rows have source-backed multi-source context
- `0` rows failed ISO3 resolution on the trial side
- distinct sources in the joined surface:
  - `ihme_burden`
  - `ihme_population`
  - `ihme_sdi`
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

## Next bounded step

- wire the static dashboard/report layer to `transportability_country_year.parquet` and `evidence_gap_summary.*`
- keep the phase-1 topic and source surface frozen while the reporting layer is built
