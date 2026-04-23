from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.dashboard import (
    build_dashboard_html,
    build_publish_wrapper_html,
    materialize_dashboard,
    materialize_publish_wrapper,
)


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


def _build_publish_fixture(
    output_dir: Path,
    *,
    topic_slug: str,
    snapshot_dir: str,
    source_names: list[str],
    signal_names: list[str],
    gap_country: str,
    support_country: str,
) -> None:
    _write_json(
        output_dir / "run_manifest.json",
        {
            "topic_slug": topic_slug,
            "snapshot_dir": snapshot_dir,
            "selected_nct_ids": ["NCT00000001", "NCT00000002", "NCT00000003"],
            "strict_comparable_candidates": 2,
        },
    )
    _write_json(
        output_dir / "context_join_manifest.json",
        {
            "context_rows": 8,
            "context_available_rows": 7,
            "distinct_context_sources": source_names,
        },
    )
    _write_json(
        output_dir / "transportability_manifest.json",
        {
            "country_year_rows": 4,
            "summary_rows": 2,
            "core_signal_keys": signal_names,
        },
    )
    pd.DataFrame(
        [
            {
                "iso3": "AAA",
                "country_name": gap_country,
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
                "missing_core_signals_union": ";".join(signal_names),
                "trial_nct_ids": "NCT00000001",
                "comparable_nct_ids": "",
            },
            {
                "iso3": "BBB",
                "country_name": support_country,
                "trial_year_count": 1,
                "trial_count": 1,
                "comparable_trial_count": 1,
                "comparable_candidate_count": 2,
                "latest_year": 2024,
                "latest_transportability_score": 0.81,
                "latest_priority_gap_score": 0.19,
                "mean_country_coverage_score": 1.0,
                "mean_eligibility_support_score": 1.0,
                "mean_reporting_completeness_score": 0.5,
                "mean_transportability_score": 0.81,
                "max_priority_gap_score": 0.19,
                "missing_core_signals_union": "",
                "trial_nct_ids": "NCT00000002",
                "comparable_nct_ids": "NCT00000002",
            },
        ]
    ).to_parquet(output_dir / "evidence_gap_summary.parquet", index=False)
    pd.DataFrame(
        [
            {
                "iso3": "AAA",
                "country_name": gap_country,
                "year": 2025,
                "trial_count": 1,
                "total_candidate_count": 0,
                "comparable_candidate_count": 0,
                "trial_nct_ids": "NCT00000001",
                "comparable_trial_count": 0,
                "comparable_nct_ids": "",
                "comparable_family_count": 0,
                "available_core_signal_count": 0,
                "expected_core_signal_count": len(signal_names),
                "country_coverage_score": 0.0,
                "context_distance": 1.0,
                "eligibility_support_score": 0.0,
                "reporting_completeness_score": 0.0,
                "transportability_score": 0.0,
                "priority_gap_score": 1.0,
                "effect_status": "no_comparable_evidence",
                "available_core_signals": "",
                "missing_core_signals": ";".join(signal_names),
            },
            {
                "iso3": "BBB",
                "country_name": support_country,
                "year": 2024,
                "trial_count": 1,
                "total_candidate_count": 2,
                "comparable_candidate_count": 2,
                "trial_nct_ids": "NCT00000002",
                "comparable_trial_count": 1,
                "comparable_nct_ids": "NCT00000002",
                "comparable_family_count": 1,
                "available_core_signal_count": len(signal_names),
                "expected_core_signal_count": len(signal_names),
                "country_coverage_score": 1.0,
                "context_distance": 0.0,
                "eligibility_support_score": 1.0,
                "reporting_completeness_score": 0.5,
                "transportability_score": 0.81,
                "priority_gap_score": 0.19,
                "effect_status": "comparable_evidence_available",
                "available_core_signals": ";".join(signal_names),
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


def test_materialize_publish_wrapper_writes_landing_page_from_topics(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    outputs_root = repo_root / "outputs"
    topic_one = outputs_root / "sacubitril_valsartan_hfref"
    topic_two = outputs_root / "sglt2_inhibitors"
    topic_one.mkdir(parents=True)
    topic_two.mkdir(parents=True)
    _build_publish_fixture(
        topic_one,
        topic_slug="sacubitril_valsartan_hfref",
        snapshot_dir=r"D:\AACT-storage\AACT\2026-04-12",
        source_names=["ihme_burden", "who_gho"],
        signal_names=["daly_rate", "population"],
        gap_country="Egypt",
        support_country="United Kingdom",
    )
    _build_publish_fixture(
        topic_two,
        topic_slug="sglt2_inhibitors",
        snapshot_dir=r"D:\AACT-storage\AACT\2026-04-12",
        source_names=["ihme_burden", "wb_population"],
        signal_names=["daly_rate", "population", "sdi"],
        gap_country="Saudi Arabia",
        support_country="Brazil",
    )

    wrapper_path = repo_root / "dashboard" / "transportability_dashboard.html"
    index_path = repo_root / "dashboard" / "index.html"
    manifest = materialize_publish_wrapper(
        repo_root,
        wrapper_path=wrapper_path,
        index_path=index_path,
        topic_slugs=["sacubitril_valsartan_hfref", "sglt2_inhibitors"],
    )

    html = wrapper_path.read_text(encoding="utf-8")
    assert manifest["topic_count"] == 2
    assert index_path.read_text(encoding="utf-8") == html
    assert "Publish-Ready Wrapper" in html
    assert "Sacubitril/Valsartan in HFrEF" in html
    assert "SGLT2 Inhibitors" in html
    assert "transportability_sacubitril_valsartan_hfref.html" in html
    assert "transportability_sglt2_inhibitors.html" in html
    assert "Egypt gap 1.000 (2025)" in html
    assert "Brazil score 0.810" in html
    assert "dashboard/index.html" in html


def test_build_publish_wrapper_html_fails_closed_without_topics(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    (repo_root / "outputs").mkdir(parents=True)

    try:
        build_publish_wrapper_html(repo_root)
    except ValueError as exc:
        assert "No publishable topic outputs" in str(exc)
    else:
        raise AssertionError("Expected build_publish_wrapper_html to fail closed without topics")
