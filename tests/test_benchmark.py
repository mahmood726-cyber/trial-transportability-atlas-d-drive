"""Reproducibility tests for the self-contained transportability benchmark."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import benchmark_transportability as bench  # noqa: E402


def test_benchmark_inputs_are_self_consistent() -> None:
    trials = bench.build_trial_country_year()
    candidates = bench.build_effect_candidates()
    context = bench.build_context_long()

    # Every effect candidate references a trial that exists in the footprint.
    assert set(candidates["nct_id"]).issubset(set(trials["nct_id"]))
    # Context frame carries the columns the adapters emit and the scorer reads.
    for column in ("iso3", "year", "source", "measure", "metric", "value"):
        assert column in context.columns
    # iso3 is intentionally left blank in the trial footprint (resolved by lookup).
    assert trials["iso3"].isna().all()


def test_benchmark_runs_and_scores_expected_shape() -> None:
    country_year, summary = bench.run_benchmark()

    # Four unique countries appear in the footprint.
    assert set(summary["iso3"]) == {"GBR", "USA", "DEU", "KEN"}
    # Two GBR country-years (2019, 2020) plus one each for USA/DEU/KEN.
    assert len(country_year) == 5

    # All scores are within [0, 1].
    for column in (
        "country_coverage_score",
        "eligibility_support_score",
        "reporting_completeness_score",
        "transportability_score",
        "priority_gap_score",
    ):
        assert country_year[column].between(0.0, 1.0).all()

    # priority_gap_score is the complement of transportability_score.
    complement = country_year["transportability_score"] + country_year["priority_gap_score"]
    assert complement.round(9).eq(1.0).all()


def test_benchmark_kenya_is_the_top_priority_gap() -> None:
    _, summary = bench.run_benchmark()

    # Kenya has the sparsest context and no comparable evidence, so it should
    # carry the highest latest-year priority gap. The summary is sorted with
    # highest gap first.
    top = summary.iloc[0]
    assert top["iso3"] == "KEN"
    assert top["comparable_trial_count"] == 0
    assert top["latest_priority_gap_score"] == pytest.approx(
        1.0 - top["latest_transportability_score"]
    )


def test_benchmark_is_deterministic() -> None:
    first_cy, first_summary = bench.run_benchmark()
    second_cy, second_summary = bench.run_benchmark()

    pd.testing.assert_frame_equal(first_cy, second_cy)
    pd.testing.assert_frame_equal(first_summary, second_summary)


def test_benchmark_writer_emits_all_artifacts(tmp_path: Path) -> None:
    country_year, summary = bench.run_benchmark()
    out_dir = tmp_path / "bench"
    out_dir.mkdir()

    country_year.to_parquet(out_dir / "benchmark_country_year.parquet", index=False)
    summary.to_parquet(out_dir / "benchmark_evidence_gap_summary.parquet", index=False)
    (out_dir / "benchmark_evidence_gap_summary.md").write_text(
        bench.render_evidence_gap_summary_markdown(summary), encoding="utf-8"
    )

    assert (out_dir / "benchmark_country_year.parquet").exists()
    assert (out_dir / "benchmark_evidence_gap_summary.parquet").exists()
    markdown = (out_dir / "benchmark_evidence_gap_summary.md").read_text(encoding="utf-8")
    assert "Evidence Gap Summary" in markdown
