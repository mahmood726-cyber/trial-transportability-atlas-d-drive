from __future__ import annotations

from math import isclose
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.effect_family_pooling import (
    build_family_pooled_effects,
    build_family_pooled_trial_summary,
    classify_effect_family,
    materialize_family_pooled_outputs,
)
from trial_transportability_atlas.topics import PHASE1_TOPIC


def build_canonical_effect_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_effect_id": "c1",
                "candidate_id": "A1",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "outcome_name": "Change From Baseline in Mean Daily Non-sedentary Daytime Activity",
                "time_frame": "Week 12",
                "analysis_population": "FAS",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "surface_count": 12,
                "precision_ready_surface_count": 12,
                "surface_label_count": 12,
                "surface_label_examples": "Week 1;Week 2",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 0.5,
                "surface_value_median": 1.0,
                "surface_value_max": 1.5,
                "surface_effect_iqr": 0.3,
                "surface_effect_mad_sd": 0.1,
                "canonical_effect_value": 1.0,
                "canonical_effect_precision": 0.30,
                "precision_metric": "standard_error",
                "collapse_method": "candidate_median_no_precision_gain",
                "collapse_status": "median_precision_retained",
                "treatment_arm_title": "LCZ696",
                "comparator_arm_title": "Enalapril",
                "treatment_intervention_names": "LCZ696",
                "comparator_intervention_names": "Enalapril",
            },
            {
                "canonical_effect_id": "c2",
                "candidate_id": "A2",
                "nct_id": "N1",
                "candidate_family": "continuous_mean",
                "outcome_name": "Total Weekly Time Spent in Non-sedentary Daytime Physical Activity",
                "time_frame": "Week 12",
                "analysis_population": "FAS",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "surface_count": 12,
                "precision_ready_surface_count": 12,
                "surface_label_count": 12,
                "surface_label_examples": "Week 1;Week 2",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 1.2,
                "surface_value_median": 1.8,
                "surface_value_max": 2.4,
                "surface_effect_iqr": 0.4,
                "surface_effect_mad_sd": 0.2,
                "canonical_effect_value": 1.8,
                "canonical_effect_precision": 0.50,
                "precision_metric": "standard_error",
                "collapse_method": "candidate_median_no_precision_gain",
                "collapse_status": "median_precision_retained",
                "treatment_arm_title": "LCZ696",
                "comparator_arm_title": "Enalapril",
                "treatment_intervention_names": "LCZ696",
                "comparator_intervention_names": "Enalapril",
            },
            {
                "canonical_effect_id": "c3",
                "candidate_id": "E1",
                "nct_id": "N1",
                "candidate_family": "binary_event_count",
                "outcome_name": "Cardiac failure",
                "time_frame": "12 months",
                "analysis_population": "Cardiac disorders",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "event_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "surface_count": 1,
                "precision_ready_surface_count": 1,
                "surface_label_count": 1,
                "surface_label_examples": "serious",
                "derivation_methods": "reported_event_log_risk_ratio",
                "surface_value_min": -0.2,
                "surface_value_median": -0.2,
                "surface_value_max": -0.2,
                "surface_effect_iqr": 0.0,
                "surface_effect_mad_sd": 0.0,
                "canonical_effect_value": -0.2,
                "canonical_effect_precision": 0.30,
                "precision_metric": "standard_error",
                "collapse_method": "single_surface_identity",
                "collapse_status": "single_surface_precision_retained",
                "treatment_arm_title": "LCZ696",
                "comparator_arm_title": "Enalapril",
                "treatment_intervention_names": "LCZ696",
                "comparator_intervention_names": "Enalapril",
            },
            {
                "canonical_effect_id": "c4",
                "candidate_id": "E2",
                "nct_id": "N1",
                "candidate_family": "binary_event_count",
                "outcome_name": "Cardiac arrest",
                "time_frame": "12 months",
                "analysis_population": "Cardiac disorders",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "event_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "surface_count": 1,
                "precision_ready_surface_count": 1,
                "surface_label_count": 1,
                "surface_label_examples": "serious",
                "derivation_methods": "reported_event_log_risk_ratio",
                "surface_value_min": -0.6,
                "surface_value_median": -0.6,
                "surface_value_max": -0.6,
                "surface_effect_iqr": 0.0,
                "surface_effect_mad_sd": 0.0,
                "canonical_effect_value": -0.6,
                "canonical_effect_precision": 0.35,
                "precision_metric": "standard_error",
                "collapse_method": "single_surface_identity",
                "collapse_status": "single_surface_precision_retained",
                "treatment_arm_title": "LCZ696",
                "comparator_arm_title": "Enalapril",
                "treatment_intervention_names": "LCZ696",
                "comparator_intervention_names": "Enalapril",
            },
            {
                "canonical_effect_id": "c5",
                "candidate_id": "E3",
                "nct_id": "N1",
                "candidate_family": "binary_event_count",
                "outcome_name": "Cardiac failure",
                "time_frame": "12 months",
                "analysis_population": "Cardiac disorders",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "event_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "surface_count": 1,
                "precision_ready_surface_count": 1,
                "surface_label_count": 1,
                "surface_label_examples": "other",
                "derivation_methods": "reported_event_log_risk_ratio",
                "surface_value_min": 0.1,
                "surface_value_median": 0.1,
                "surface_value_max": 0.1,
                "surface_effect_iqr": 0.0,
                "surface_effect_mad_sd": 0.0,
                "canonical_effect_value": 0.1,
                "canonical_effect_precision": 0.25,
                "precision_metric": "standard_error",
                "collapse_method": "single_surface_identity",
                "collapse_status": "single_surface_precision_retained",
                "treatment_arm_title": "LCZ696",
                "comparator_arm_title": "Enalapril",
                "treatment_intervention_names": "LCZ696",
                "comparator_intervention_names": "Enalapril",
            },
        ]
    )


def test_classify_effect_family_assigns_explicit_domains() -> None:
    frame = build_canonical_effect_fixture()
    activity_label, activity_domain = classify_effect_family(frame.iloc[0])
    event_label, event_domain = classify_effect_family(frame.iloc[2])

    assert activity_label == "activity_volume"
    assert activity_domain == "activity_volume"
    assert event_label == "adverse_event_risk_serious"
    assert event_domain == "adverse_event_risk"


def test_build_family_pooled_effects_collapses_within_family_without_precision_gain() -> None:
    pooled = build_family_pooled_effects(build_canonical_effect_fixture())

    assert len(pooled) == 3

    activity = pooled.loc[pooled["family_label"] == "activity_volume"].iloc[0]
    assert activity["canonical_effect_count"] == 2
    assert activity["raw_surface_count"] == 24
    assert isclose(float(activity["family_effect_value"]), 1.4, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(activity["family_effect_mad_sd"]), 0.59304, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(activity["family_effect_precision"]), 0.59304, rel_tol=0.0, abs_tol=1e-6)
    assert activity["pooling_status"] == "dispersion_guardrail_applied"

    serious = pooled.loc[pooled["family_label"] == "adverse_event_risk_serious"].iloc[0]
    assert serious["canonical_effect_count"] == 2
    assert serious["analysis_population"] == "Cardiac disorders"
    assert isclose(float(serious["family_effect_value"]), -0.4, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(serious["family_effect_precision"]), 0.325, rel_tol=0.0, abs_tol=1e-6)
    assert serious["pooling_status"] == "median_precision_retained"

    other = pooled.loc[pooled["family_label"] == "adverse_event_risk_other"].iloc[0]
    assert other["canonical_effect_count"] == 1
    assert isclose(float(other["family_effect_precision"]), 0.25, rel_tol=0.0, abs_tol=1e-6)


def test_build_family_pooled_trial_summary_tracks_family_contraction() -> None:
    pooled = build_family_pooled_effects(build_canonical_effect_fixture())
    summary = build_family_pooled_trial_summary(pooled)

    row = summary.iloc[0]
    assert row["nct_id"] == "N1"
    assert row["family_pool_count"] == 3
    assert row["canonical_effect_count"] == 5
    assert row["raw_surface_count"] == 27
    assert isclose(float(row["family_contraction_ratio"]), 0.6, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(row["mean_canonical_effects_per_family"]), 5.0 / 3.0, rel_tol=0.0, abs_tol=1e-6)


def test_materialize_family_pooled_outputs_writes_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)
    build_canonical_effect_fixture().to_parquet(
        output_dir / "canonical_effect_estimates.parquet",
        index=False,
    )

    manifest = materialize_family_pooled_outputs(output_dir, topic=PHASE1_TOPIC)

    assert manifest["topic_slug"] == "sacubitril_valsartan_hfref"
    assert manifest["family_pool_count"] == 3
    assert manifest["canonical_effect_count"] == 5
    assert manifest["raw_surface_count"] == 27
    assert manifest["precision_ready_family_pool_count"] == 3
    assert isclose(float(manifest["family_contraction_ratio"]), 3.0 / 5.0, rel_tol=0.0, abs_tol=1e-6)
    assert (output_dir / "family_pooled_effects.parquet").exists()
    assert (output_dir / "family_pooled_trial_summary.parquet").exists()
    assert (output_dir / "family_pooled_report.md").exists()
    assert (output_dir / "family_pooled_manifest.json").exists()
