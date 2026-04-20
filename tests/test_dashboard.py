from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.dashboard import build_dashboard_html, materialize_dashboard


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_dashboard_fixture(output_dir: Path) -> None:
    _write_json(
        output_dir / "run_manifest.json",
        {
            "topic_slug": "example_topic",
            "snapshot_dir": r"D:\AACT-storage\AACT\2026-04-12",
            "selected_nct_ids": ["NCT00000001", "NCT00000002"],
            "strict_comparable_candidates": 1,
        },
    )
    _write_json(
        output_dir / "context_join_manifest.json",
        {
            "context_rows": 12,
            "context_available_rows": 11,
            "distinct_context_sources": ["ihme_burden", "who_gho"],
        },
    )
    _write_json(
        output_dir / "transportability_manifest.json",
        {
            "country_year_rows": 3,
            "summary_rows": 2,
            "core_signal_keys": ["daly_rate", "population"],
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
                "missing_core_signals_union": "daly_rate;population",
                "trial_nct_ids": "NCT00000001",
                "comparable_nct_ids": "",
            },
            {
                "iso3": "GBR",
                "country_name": "United Kingdom",
                "trial_year_count": 1,
                "trial_count": 1,
                "comparable_trial_count": 1,
                "comparable_candidate_count": 1,
                "latest_year": 2024,
                "latest_transportability_score": 0.75,
                "latest_priority_gap_score": 0.25,
                "mean_country_coverage_score": 1.0,
                "mean_eligibility_support_score": 1.0,
                "mean_reporting_completeness_score": 0.25,
                "mean_transportability_score": 0.75,
                "max_priority_gap_score": 0.25,
                "missing_core_signals_union": "",
                "trial_nct_ids": "NCT00000002",
                "comparable_nct_ids": "NCT00000002",
            },
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
                "signal_population": None,
                "available_core_signal_count": 0,
                "expected_core_signal_count": 2,
                "country_coverage_score": 0.0,
                "context_distance": 1.0,
                "eligibility_support_score": 0.0,
                "reporting_completeness_score": 0.0,
                "transportability_score": 0.0,
                "priority_gap_score": 1.0,
                "effect_status": "no_comparable_evidence",
                "available_core_signals": "",
                "missing_core_signals": "daly_rate;population",
            },
            {
                "iso3": "GBR",
                "country_name": "United Kingdom",
                "year": 2024,
                "trial_count": 1,
                "total_candidate_count": 1,
                "comparable_candidate_count": 1,
                "trial_nct_ids": "NCT00000002",
                "comparable_trial_count": 1,
                "comparable_nct_ids": "NCT00000002",
                "comparable_family_count": 1,
                "signal_daly_rate": 100.0,
                "signal_population": 10.0,
                "available_core_signal_count": 2,
                "expected_core_signal_count": 2,
                "country_coverage_score": 1.0,
                "context_distance": 0.0,
                "eligibility_support_score": 1.0,
                "reporting_completeness_score": 0.25,
                "transportability_score": 0.75,
                "priority_gap_score": 0.25,
                "effect_status": "comparable_evidence_available",
                "available_core_signals": "daly_rate;population",
                "missing_core_signals": "",
            },
            {
                "iso3": "GBR",
                "country_name": "United Kingdom",
                "year": 2023,
                "trial_count": 1,
                "total_candidate_count": 1,
                "comparable_candidate_count": 1,
                "trial_nct_ids": "NCT00000002",
                "comparable_trial_count": 1,
                "comparable_nct_ids": "NCT00000002",
                "comparable_family_count": 1,
                "signal_daly_rate": 100.0,
                "signal_population": 10.0,
                "available_core_signal_count": 2,
                "expected_core_signal_count": 2,
                "country_coverage_score": 1.0,
                "context_distance": 0.0,
                "eligibility_support_score": 1.0,
                "reporting_completeness_score": 0.25,
                "transportability_score": 0.75,
                "priority_gap_score": 0.25,
                "effect_status": "comparable_evidence_available",
                "available_core_signals": "daly_rate;population",
                "missing_core_signals": "",
            },
        ]
    ).to_parquet(output_dir / "transportability_country_year.parquet", index=False)


def test_materialize_dashboard_writes_source_backed_html(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "example_topic"
    output_dir.mkdir(parents=True)
    _build_dashboard_fixture(output_dir)

    dashboard_path = tmp_path / "dashboard" / "transportability_dashboard.html"
    manifest = materialize_dashboard(output_dir, dashboard_path=dashboard_path)

    html = dashboard_path.read_text(encoding="utf-8")
    assert manifest["summary_rows"] == 2
    assert manifest["dashboard_path"] == str(dashboard_path)
    assert "D:\\AACT-storage\\AACT\\2026-04-12" in html
    assert "example_topic" in html
    assert "ihme_burden" in html
    assert "who_gho" in html
    assert "Egypt" in html
    assert "United Kingdom" in html
    assert "Fail-closed rule" in html


def test_build_dashboard_html_uses_materialized_contract(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "example_topic"
    output_dir.mkdir(parents=True)
    _build_dashboard_fixture(output_dir)

    html = build_dashboard_html(output_dir)

    assert "Latest-Year Evidence Gaps" in html
    assert "Highest latest transportability scores" in html
    assert "NCT00000002" not in html
    assert "Full-signal country-years: 2. Zero-signal country-years: 1." in html
