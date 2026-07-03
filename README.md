# Trial Transportability Atlas

Deterministic local scaffold for linking AACT trial evidence to country-year
context from the IHME, WHO, and World Bank lakehouses.

Current phase focus:

- freeze the first topic: `sacubitril/valsartan in HFrEF`
- build source-backed AACT bridge outputs
- materialize source-backed context joins and transportability scores
- render the static phase-1 dashboard from those materialized outputs

Static dashboard command:

- `python generate_dashboard.py`
- Topic pages are written to `dashboard/transportability_<topic>.html`
- The publish wrapper is written to `dashboard/transportability_dashboard.html` and `dashboard/index.html`

UI smoke commands:

- `python -m http.server 8000 --bind 127.0.0.1`
- `npx --yes --package @playwright/cli playwright-cli open http://127.0.0.1:8000/dashboard/transportability_dashboard.html --headed`

## Reproducible benchmark (no external snapshots)

`benchmark_transportability.py` runs the core transportability scoring engine
end to end on a small, bundled **synthetic** dataset. It needs none of the
external AACT / IHME / WHO / World Bank snapshots — the trial footprint, effect
candidates, and long-form context surface are all generated deterministically
in memory — so it runs from a fresh clone with only the packaged dependencies
(`numpy`, `pandas`, `pyarrow`, `pycountry`).

Run it:

```
# print the country-year scores and evidence-gap summary to stdout
python benchmark_transportability.py

# also write parquet / CSV / markdown artifacts to a directory
python benchmark_transportability.py --out outputs/benchmark
```

It scores a four-country footprint (United Kingdom, United States, Germany,
Kenya). The context coverage is deliberately sparse for Kenya and none of its
trials carry a comparable effect family, so Kenya surfaces as the highest
`priority_gap_score` country — the worked example that shows how the score
composes country context coverage, eligibility support, and reporting
completeness:

```
transportability_score = mean(
    country_coverage_score,       # available_core_signal_count / expected
    eligibility_support_score,    # comparable_trial_count / trial_count
    reporting_completeness_score, # comparable_candidate_count / total_candidate_count
)
priority_gap_score = 1 - transportability_score
```

The printed numbers are a fixed property of the synthetic inputs and the
scoring formulas, so they are stable across runs. `tests/test_benchmark.py`
locks in the benchmark's determinism and score invariants.

### Path resolution for the real pipeline

The full pipeline resolves the external AACT snapshot and the IHME/WHO/WB
lakehouse repos from candidate drive roots (`D:`, `C:`, `F:`) or explicit
environment overrides: `TTA_AACT_PATH`, `TTA_IHME_PATH`, `TTA_WHO_PATH`,
`TTA_WB_PATH`. Set those if your snapshots live elsewhere.
