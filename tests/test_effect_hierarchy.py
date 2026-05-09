from __future__ import annotations

from math import isclose
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.effect_hierarchy import (
    build_canonical_effect_estimates,
    build_canonical_effect_trial_summary,
    materialize_canonical_effect_outputs,
)
from trial_transportability_atlas.topics import PHASE1_TOPIC


def build_signed_effect_estimates_fixture() -> pd.DataFrame:
    base = {
        "effect_direction": "treatment_minus_comparator",
        "treatment_arm_title": "LCZ696",
        "comparator_arm_title": "Enalapril",
        "treatment_intervention_names": "LCZ696",
        "comparator_intervention_names": "Enalapril",
    }
    return pd.DataFrame(
        [
            {
                **base,
                "surface_id": "s1",
                "candidate_id": "C1",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "outcome_name": "Walk distance",
                "time_frame": "Week 12",
                "surface_label": "Week 4",
                "analysis_population": "FAS",
                "estimand_type": "change_from_baseline",
                "effect_measure": "mean_difference",
                "effect_value": 1.0,
                "effect_precision": 0.2,
                "precision_metric": "standard_error",
                "precision_status": "reported_standard_error",
                "derivation_method": "continuous_surface_split",
            },
            {
                **base,
                "surface_id": "s2",
                "candidate_id": "C1",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "outcome_name": "Walk distance",
                "time_frame": "Week 12",
                "surface_label": "Week 8",
                "analysis_population": "FAS",
                "estimand_type": "change_from_baseline",
                "effect_measure": "mean_difference",
                "effect_value": 1.4,
                "effect_precision": 0.25,
                "precision_metric": "standard_error",
                "precision_status": "reported_standard_error",
                "derivation_method": "continuous_surface_split",
            },
            {
                **base,
                "surface_id": "s3",
                "candidate_id": "C1",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "outcome_name": "Walk distance",
                "time_frame": "Week 12",
                "surface_label": "Week 12",
                "analysis_population": "FAS",
                "estimand_type": "change_from_baseline",
                "effect_measure": "mean_difference",
                "effect_value": 0.8,
                "effect_precision": 0.3,
                "precision_metric": "standard_error",
                "precision_status": "reported_standard_error",
                "derivation_method": "continuous_surface_split",
            },
            {
                **base,
                "surface_id": "s4",
                "candidate_id": "C2",
                "nct_id": "N1",
                "candidate_family": "binary_event_count",
                "outcome_name": "Heart failure worsening",
                "time_frame": "12 months",
                "surface_label": "serious",
                "analysis_population": "Cardiac disorders",
                "estimand_type": "event_risk",
                "effect_measure": "log_risk_ratio",
                "effect_value": -0.5,
                "effect_precision": 0.4,
                "precision_metric": "standard_error",
                "precision_status": "wald_log_risk_ratio",
                "derivation_method": "reported_event_log_risk_ratio",
            },
            {
                **base,
                "surface_id": "s5",
                "candidate_id": "C3",
                "nct_id": "N2",
                "candidate_family": "continuous_mean",
                "outcome_name": "Activity",
                "time_frame": "Week 4",
                "surface_label": "Change from BL at Week 4",
                "analysis_population": "FAS",
                "estimand_type": "change_from_baseline",
                "effect_measure": "mean_difference",
                "effect_value": 2.0,
                "effect_precision": 0.5,
                "precision_metric": "standard_error",
                "precision_status": "reported_standard_error",
                "derivation_method": "continuous_surface_split",
            },
            {
                **base,
                "surface_id": "s6",
                "candidate_id": "C3",
                "nct_id": "N2",
                "candidate_family": "continuous_mean",
                "outcome_name": "Activity",
                "time_frame": "Week 4",
                "surface_label": "Week 4",
                "analysis_population": "FAS",
                "estimand_type": "post_baseline_level",
                "effect_measure": "mean_difference",
                "effect_value": 5.0,
                "effect_precision": 0.6,
                "precision_metric": "standard_error",
                "precision_status": "reported_standard_error",
                "derivation_method": "continuous_surface_split",
            },
        ]
    )


def test_build_canonical_effect_estimates_collapses_multi_surface_group_conservatively() -> None:
    canonical = build_canonical_effect_estimates(build_signed_effect_estimates_fixture())

    assert len(canonical) == 4

    c1 = canonical.loc[canonical["candidate_id"] == "C1"].iloc[0]
    assert c1["surface_count"] == 3
    assert isclose(float(c1["canonical_effect_value"]), 1.0, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(c1["surface_effect_mad_sd"]), 0.29652, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(c1["canonical_effect_precision"]), 0.29652, rel_tol=0.0, abs_tol=1e-6)
    assert c1["collapse_status"] == "dispersion_guardrail_applied"

    c2 = canonical.loc[canonical["candidate_id"] == "C2"].iloc[0]
    assert c2["surface_count"] == 1
    assert isclose(float(c2["canonical_effect_value"]), -0.5, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(c2["canonical_effect_precision"]), 0.4, rel_tol=0.0, abs_tol=1e-6)
    assert c2["collapse_method"] == "single_surface_identity"


def test_build_canonical_effect_trial_summary_tracks_contraction() -> None:
    canonical = build_canonical_effect_estimates(build_signed_effect_estimates_fixture())
    summary = build_canonical_effect_trial_summary(canonical)

    n1 = summary.loc[summary["nct_id"] == "N1"].iloc[0]
    n2 = summary.loc[summary["nct_id"] == "N2"].iloc[0]

    assert n1["canonical_effect_count"] == 2
    assert n1["raw_surface_count"] == 4
    assert isclose(float(n1["surface_contraction_ratio"]), 0.5, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(n1["mean_surface_count_per_canonical_effect"]), 2.0, rel_tol=0.0, abs_tol=1e-6)
    assert n2["canonical_effect_count"] == 2


def test_materialize_canonical_effect_outputs_writes_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)
    build_signed_effect_estimates_fixture().to_parquet(
        output_dir / "signed_effect_estimates.parquet",
        index=False,
    )

    manifest = materialize_canonical_effect_outputs(output_dir, topic=PHASE1_TOPIC)

    assert manifest["topic_slug"] == "sacubitril_valsartan_hfref"
    assert manifest["canonical_effect_count"] == 4
    assert manifest["raw_surface_count"] == 6
    assert manifest["precision_ready_canonical_effect_count"] == 4
    assert isclose(float(manifest["surface_contraction_ratio"]), 4.0 / 6.0, rel_tol=0.0, abs_tol=1e-6)
    assert (output_dir / "canonical_effect_estimates.parquet").exists()
    assert (output_dir / "canonical_effect_trial_summary.parquet").exists()
    assert (output_dir / "canonical_effect_report.md").exists()
    assert (output_dir / "canonical_effect_manifest.json").exists()
