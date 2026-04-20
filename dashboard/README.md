# Dashboard Placeholder

Static dashboard assets for the phase-1 demo should live here.

Requirements for the first shipped dashboard:

- show source snapshot identifiers
- show run timestamp
- show missingness and fail-closed notices
- avoid unsupported causal language

Current generator:

- `python generate_dashboard.py --output-dir outputs/sacubitril_valsartan_hfref --dashboard-path dashboard/transportability_dashboard.html`

Current UI smoke:

- serve from repo root: `python -m http.server 8000 --bind 127.0.0.1`
- open in browser: `npx --yes --package @playwright/cli playwright-cli open http://127.0.0.1:8000/dashboard/transportability_dashboard.html --headed`

Current source-backed inputs:

- `outputs/sacubitril_valsartan_hfref/run_manifest.json`
- `outputs/sacubitril_valsartan_hfref/context_join_manifest.json`
- `outputs/sacubitril_valsartan_hfref/transportability_manifest.json`
- `outputs/sacubitril_valsartan_hfref/transportability_country_year.parquet`
- `outputs/sacubitril_valsartan_hfref/evidence_gap_summary.parquet`
