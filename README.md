# Trial Transportability Atlas

Deterministic local scaffold for linking AACT trial evidence to country-year
context from the IHME, WHO, and World Bank lakehouses.

Current phase focus:

- freeze the first topic: `sacubitril/valsartan in HFrEF`
- build source-backed AACT bridge outputs
- materialize source-backed context joins and transportability scores
- render the static phase-1 dashboard from those materialized outputs

Static dashboard command:

- `python generate_dashboard.py --output-dir outputs/sacubitril_valsartan_hfref --dashboard-path dashboard/transportability_dashboard.html`

UI smoke commands:

- `python -m http.server 8000 --bind 127.0.0.1`
- `npx --yes --package @playwright/cli playwright-cli open http://127.0.0.1:8000/dashboard/transportability_dashboard.html --headed`
