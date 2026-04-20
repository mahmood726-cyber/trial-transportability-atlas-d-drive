from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from trial_transportability_atlas.transportability import (
    CORE_SIGNAL_SPECS,
    build_country_year_context_signals,
    build_country_year_transportability,
    build_evidence_gap_summary,
    materialize_transportability_outputs,
)


def build_trial_country_year_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"nct_id": "N1", "country_name": "United Kingdom", "iso3": "GBR", "year": 2020},
            {"nct_id": "N2", "country_name": "United Kingdom", "iso3": "GBR", "year": 2020},
            {"nct_id": "N3", "country_name": "United States", "iso3": "USA", "year": 2020},
        ]
    )


def build_effect_candidates_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "candidate_id": "C1",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "comparable_flag": True,
            },
            {
                "candidate_id": "C2",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "comparable_flag": True,
            },
            {
                "candidate_id": "C3",
                "nct_id": "N2",
                "candidate_family": "unsupported_number",
                "comparable_flag": False,
            },
            {
                "candidate_id": "C4",
                "nct_id": "N3",
                "candidate_family": "binary_event_count",
                "comparable_flag": False,
            },
        ]
    )


def build_context_join_fixture() -> pd.DataFrame:
    rows = []
    signal_values = {
        "daly_rate": 100.0,
        "death_rate": 10.0,
        "population": 1000.0,
        "sdi": 0.8,
    }
    for spec in CORE_SIGNAL_SPECS:
        if spec.key not in signal_values:
            continue
        rows.append(
            {
                "iso3_resolved": "GBR",
                "country_name": "United Kingdom",
                "year": 2020,
                "source": spec.source,
                "measure": spec.measure,
                "metric": spec.metric,
                "sex": spec.sex,
                "age_group": spec.age_group,
                "value": signal_values[spec.key],
                "context_available_flag": True,
            }
        )
    return pd.DataFrame(rows)


def test_build_country_year_context_signals_selects_curated_values() -> None:
    signals = build_country_year_context_signals(
        trial_country_year=build_trial_country_year_fixture(),
        context_joined=build_context_join_fixture(),
    )

    uk = signals.loc[signals["iso3"] == "GBR"].iloc[0]
    usa = signals.loc[signals["iso3"] == "USA"].iloc[0]
    assert uk["signal_daly_rate"] == 100.0
    assert uk["signal_sdi"] == 0.8
    assert pd.isna(usa["signal_daly_rate"])


def test_build_country_year_transportability_scores_country_years() -> None:
    country_year = build_country_year_transportability(
        trial_country_year=build_trial_country_year_fixture(),
        effect_candidates=build_effect_candidates_fixture(),
        context_joined=build_context_join_fixture(),
    )

    uk = country_year.loc[country_year["iso3"] == "GBR"].iloc[0]
    usa = country_year.loc[country_year["iso3"] == "USA"].iloc[0]

    assert uk["trial_count"] == 2
    assert uk["comparable_trial_count"] == 1
    assert uk["comparable_candidate_count"] == 2
    assert uk["total_candidate_count"] == 3
    assert uk["available_core_signal_count"] == 4
    assert uk["expected_core_signal_count"] == len(CORE_SIGNAL_SPECS)
    assert uk["country_coverage_score"] == pytest.approx(4 / len(CORE_SIGNAL_SPECS))
    assert uk["eligibility_support_score"] == pytest.approx(0.5)
    assert uk["reporting_completeness_score"] == pytest.approx(2 / 3)
    assert uk["transportability_score"] == pytest.approx(
        ((4 / len(CORE_SIGNAL_SPECS)) + 0.5 + (2 / 3)) / 3
    )
    assert usa["priority_gap_score"] == pytest.approx(1.0)


def test_build_evidence_gap_summary_aggregates_latest_rows() -> None:
    summary = build_evidence_gap_summary(
        build_country_year_transportability(
            trial_country_year=build_trial_country_year_fixture(),
            effect_candidates=build_effect_candidates_fixture(),
            context_joined=build_context_join_fixture(),
        )
    )

    assert list(summary["iso3"]) == ["USA", "GBR"]
    assert summary.loc[summary["iso3"] == "USA", "latest_priority_gap_score"].iloc[0] == pytest.approx(1.0)
    assert "daly_rate" in summary.loc[summary["iso3"] == "USA", "missing_core_signals_union"].iloc[0]


def test_materialize_transportability_outputs_writes_files(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)
    build_trial_country_year_fixture().to_parquet(output_dir / "trial_country_year.parquet", index=False)
    build_effect_candidates_fixture().to_parquet(output_dir / "effect_candidates.parquet", index=False)
    build_context_join_fixture().to_parquet(output_dir / "context_joined.parquet", index=False)

    manifest = materialize_transportability_outputs(output_dir)

    assert manifest["country_year_rows"] == 2
    assert manifest["summary_rows"] == 2
    assert (output_dir / "transportability_country_year.parquet").exists()
    assert (output_dir / "evidence_gap_summary.parquet").exists()
    assert (output_dir / "evidence_gap_summary.md").exists()
