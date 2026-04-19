# Phase 1 Topic Selection

Status: accepted

Accepted topic: sacubitril/valsartan in HFrEF

Selection rules:

- Prefer a multinational cardiology topic.
- Prefer intervention naming that is clean in AACT.
- Prefer outcomes and dates that can be typed deterministically.
- Prefer a topic that already has useful country spread in the local AACT
  snapshot.
- Reject topics that need aggressive synonym expansion or ambiguous country
  imputation in phase 1.

Candidate examples to evaluate:

- empagliflozin in heart failure
- sacubitril/valsartan in heart failure
- apixaban in atrial fibrillation

Accepted topic rationale:

- Existing local summary file:
  `C:\Projects\registry_first_rct_meta\outputs\population_runs\heart_failure_sacubitril_valsartan\population_evidence_summary.json`
  reports:
  - `registered_trials = 63`
  - `reported_trials = 16`
  - `participant_coverage = 0.546473482777474`
  - `evidence_completeness = 0.40022086837286397`
- That phase-1 completeness is stronger than the two other current candidates:
  - `atrial_fibrillation_apixaban` (`evidence_completeness = 0.16654015021710813`)
  - `heart_failure_empagliflozin` (`evidence_completeness = 0.23957450007517667`)
- The topic fits the repo rule to start with a multinational cardiology
  intervention with relatively clean naming.

Exact inclusion terms:

- `sacubitril`
- `valsartan`
- `sacubitril/valsartan`
- `LCZ696`
- heart failure terms consistent with reduced ejection fraction when the study
  text or condition labels support that interpretation

Exact exclusion terms:

- trials where the intervention match resolves to valsartan alone without
  sacubitril support
- heart-failure studies that are clearly HFpEF-only
- records with unresolved intervention ambiguity after deterministic matching

Expected AACT source tables:

- `studies`
- `id_information`
- `conditions`
- `interventions`
- `countries`
- `outcomes`
- `outcome_analyses`
- `outcome_measurements`
- `reported_events`
- `eligibilities`
- `brief_summaries`
- `design_outcomes`

Intended IHME / WHO / WB joins:

- IHME population
- IHME SDI
- IHME country-level burden available on disk
- WHO stable country-year system indicators
- WB HNP, UHC, poverty, governance, and population indicators

Known failure modes:

- outcome labels may be clinically related but not directly comparable
- effect extraction may remain unavailable for many posted-result rows
- HFrEF versus generic heart-failure labeling may require strict fail-closed
  exclusion
- some country joins may remain unsupported when source granularity is not
  country-year compatible
