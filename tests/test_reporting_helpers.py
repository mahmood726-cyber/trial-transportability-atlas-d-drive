from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas import policy_simulations, predictive_mapping, simulations


def test_generate_predictive_yield_handles_single_region_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_dir = tmp_path / "outputs" / "demo_topic"
    output_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "iso3_resolved": "USA",
                "measure": "DALYs (Disability-Adjusted Life Years)",
                "value": 1000.0,
            }
        ]
    ).to_parquet(output_dir / "context_joined.parquet", index=False)
    pd.DataFrame(
        {"Transportability Index": [0.8]},
        index=["North America"],
    ).to_csv(output_dir / "transportability_scores.csv")
    monkeypatch.setattr(predictive_mapping, "discover_topic_output_dir", lambda slug: output_dir)

    result = predictive_mapping.generate_predictive_yield("demo_topic")

    assert result is not None
    report_text = (output_dir / "predictive_yield_report.md").read_text(encoding="utf-8")
    assert "North America" in report_text
    assert "Primary Transport Target" in report_text


def test_run_transportability_simulation_handles_zero_baseline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_dir = tmp_path / "outputs" / "demo_topic"
    output_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"Transportability Index": 1.0, "Physicians": 4.0, "GDP pc": 50000.0},
            {"Transportability Index": 0.0, "Physicians": 1.0, "GDP pc": 5000.0},
        ],
        index=["North America", "Africa"],
    ).to_csv(output_dir / "transportability_scores.csv")
    monkeypatch.setattr(simulations, "discover_topic_output_dir", lambda slug: output_dir)
    monkeypatch.setattr(simulations, "calculate_transportability_score", lambda origin, target: 0.5)

    result = simulations.run_transportability_simulation("demo_topic")

    assert result is not None
    assert result["Lift (%)"].isna().all()
    assert (output_dir / "transportability_simulation.md").exists()


def test_run_policy_simulations_requires_local_burden_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_dir = tmp_path / "outputs" / "demo_topic"
    output_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"Transportability Index": 1.0, "Physicians": 4.0, "GDP pc": 50000.0, "Health Exp (%)": 10.0},
            {"Transportability Index": 0.2, "Physicians": 1.0, "GDP pc": 5000.0, "Health Exp (%)": 4.0},
        ],
        index=["North America", "Africa"],
    ).to_csv(output_dir / "transportability_scores.csv")
    pd.DataFrame(
        {"Wrong Column": [123.0]},
        index=["Africa"],
    ).to_csv(output_dir / "predictive_yield.csv")
    monkeypatch.setattr(policy_simulations, "discover_topic_output_dir", lambda slug: output_dir)

    result = policy_simulations.run_policy_simulations("demo_topic")

    assert result is None
    assert not (output_dir / "policy_simulation_report.md").exists()
