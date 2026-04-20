from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.dashboard import build_dashboard_html


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_ui_fixture(output_dir: Path) -> None:
    _write_json(
        output_dir / "run_manifest.json",
        {
            "topic_slug": "sacubitril_valsartan_hfref",
            "snapshot_dir": r"D:\AACT-storage\AACT\2026-04-12",
            "selected_nct_ids": ["NCT00000001"],
            "strict_comparable_candidates": 1,
        },
    )
    _write_json(
        output_dir / "context_join_manifest.json",
        {
            "context_rows": 5,
            "context_available_rows": 4,
            "distinct_context_sources": ["ihme_burden"],
        },
    )
    _write_json(
        output_dir / "transportability_manifest.json",
        {
            "country_year_rows": 2,
            "summary_rows": 1,
            "core_signal_keys": ["daly_rate"],
        },
    )
    pd.DataFrame(
        [
            {
                "iso3": "EGY",
                "country_name": "Egypt",
                "trial_year_count": 1,
                "trial_count": 1,
                "comparable_trial_count": 0,
                "comparable_candidate_count": 0,
                "latest_year": 2025,
                "latest_transportability_score": 0.0,
                "latest_priority_gap_score": 1.0,
                "mean_country_coverage_score": 0.0,
                "mean_eligibility_support_score": 0.0,
                "mean_reporting_completeness_score": 0.0,
                "mean_transportability_score": 0.0,
                "max_priority_gap_score": 1.0,
                "missing_core_signals_union": "daly_rate",
                "trial_nct_ids": "NCT00000001",
                "comparable_nct_ids": "",
            }
        ]
    ).to_parquet(output_dir / "evidence_gap_summary.parquet", index=False)
    pd.DataFrame(
        [
            {
                "iso3": "EGY",
                "country_name": "Egypt",
                "year": 2025,
                "trial_count": 1,
                "total_candidate_count": 0,
                "comparable_candidate_count": 0,
                "trial_nct_ids": "NCT00000001",
                "comparable_trial_count": 0,
                "comparable_nct_ids": "",
                "comparable_family_count": 0,
                "signal_daly_rate": None,
                "available_core_signal_count": 0,
                "expected_core_signal_count": 1,
                "country_coverage_score": 0.0,
                "context_distance": 1.0,
                "eligibility_support_score": 0.0,
                "reporting_completeness_score": 0.0,
                "transportability_score": 0.0,
                "priority_gap_score": 1.0,
                "effect_status": "no_comparable_evidence",
                "available_core_signals": "",
                "missing_core_signals": "daly_rate",
            },
            {
                "iso3": "EGY",
                "country_name": "Egypt",
                "year": 2024,
                "trial_count": 1,
                "total_candidate_count": 0,
                "comparable_candidate_count": 0,
                "trial_nct_ids": "NCT00000001",
                "comparable_trial_count": 0,
                "comparable_nct_ids": "",
                "comparable_family_count": 0,
                "signal_daly_rate": 100.0,
                "available_core_signal_count": 1,
                "expected_core_signal_count": 1,
                "country_coverage_score": 1.0,
                "context_distance": 0.0,
                "eligibility_support_score": 0.0,
                "reporting_completeness_score": 0.0,
                "transportability_score": 0.333333,
                "priority_gap_score": 0.666667,
                "effect_status": "no_comparable_evidence",
                "available_core_signals": "daly_rate",
                "missing_core_signals": "",
            },
        ]
    ).to_parquet(output_dir / "transportability_country_year.parquet", index=False)


def test_dashboard_ui_contract_uses_curated_title_and_inline_favicon(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "sacubitril_valsartan_hfref"
    output_dir.mkdir(parents=True)
    _build_ui_fixture(output_dir)

    html = build_dashboard_html(output_dir)

    assert '<link rel="icon" href="data:,">' in html
    assert "Sacubitril/Valsartan in HFrEF" in html
    assert "Sacubitril Valsartan Hfref" not in html
    assert "Fail-closed rule" in html
