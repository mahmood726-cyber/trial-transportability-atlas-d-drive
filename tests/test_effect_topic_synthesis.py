from __future__ import annotations

from math import isclose
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.effect_topic_synthesis import (
    build_topic_family_recurrence_summary,
    build_topic_family_synthesis,
    materialize_topic_family_synthesis_outputs,
)
from trial_transportability_atlas.topics import PHASE1_TOPIC


def build_family_pooled_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "family_pool_id": "f1",
                "nct_id": "N1",
                "family_label": "activity_volume",
                "family_domain": "activity_volume",
                "analysis_population": "FAS",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 2,
                "precision_ready_canonical_effect_count": 2,
                "raw_surface_count": 24,
                "outcome_name_count": 2,
                "candidate_id_count": 2,
                "outcome_name_examples": "Daily physical activity;Weekly activity",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 0.8,
                "surface_value_median": 1.2,
                "surface_value_max": 1.6,
                "family_effect_iqr": 0.2,
                "family_effect_mad_sd": 0.1,
                "family_effect_value": 1.2,
                "family_effect_precision": 0.40,
                "precision_metric": "standard_error",
                "pooling_method": "family_median_no_precision_gain",
                "pooling_status": "median_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f2",
                "nct_id": "N2",
                "family_label": "activity_volume",
                "family_domain": "activity_volume",
                "analysis_population": "FAS",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 12,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "Non sedentary activity",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 1.4,
                "surface_value_median": 1.8,
                "surface_value_max": 2.2,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 1.8,
                "family_effect_precision": 0.50,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f3",
                "nct_id": "N3",
                "family_label": "adverse_event_risk_serious",
                "family_domain": "adverse_event_risk",
                "analysis_population": "Cardiac disorders",
                "candidate_family": "binary_event_count",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "event_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "canonical_effect_count": 5,
                "precision_ready_canonical_effect_count": 5,
                "raw_surface_count": 5,
                "outcome_name_count": 5,
                "candidate_id_count": 5,
                "outcome_name_examples": "Cardiac arrest;Cardiac failure",
                "derivation_methods": "reported_event_log_risk_ratio",
                "surface_value_min": -0.1,
                "surface_value_median": -0.1,
                "surface_value_max": -0.1,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": -0.1,
                "family_effect_precision": 0.10,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f4",
                "nct_id": "N4",
                "family_label": "adverse_event_risk_serious",
                "family_domain": "adverse_event_risk",
                "analysis_population": "Cardiac disorders",
                "candidate_family": "binary_event_count",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "event_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "canonical_effect_count": 4,
                "precision_ready_canonical_effect_count": 4,
                "raw_surface_count": 4,
                "outcome_name_count": 4,
                "candidate_id_count": 4,
                "outcome_name_examples": "Cardiac disorder",
                "derivation_methods": "reported_event_log_risk_ratio",
                "surface_value_min": -0.9,
                "surface_value_median": -0.9,
                "surface_value_max": -0.9,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": -0.9,
                "family_effect_precision": 0.20,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f5",
                "nct_id": "N5",
                "family_label": "walk_responder_risk",
                "family_domain": "walk_responder_risk",
                "analysis_population": "FAS",
                "candidate_family": "binary_participant_count",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "responder_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 1,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "6MWT responder",
                "derivation_methods": "participant_partition_log_risk_ratio",
                "surface_value_min": 0.2,
                "surface_value_median": 0.2,
                "surface_value_max": 0.2,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 0.2,
                "family_effect_precision": 0.25,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f6",
                "nct_id": "N6",
                "family_label": "walk_responder_risk",
                "family_domain": "walk_responder_risk",
                "analysis_population": "FAS subset",
                "candidate_family": "binary_participant_count",
                "effect_measure": "log_risk_ratio",
                "estimand_type": "responder_risk",
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 1,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "6MWT responder subset",
                "derivation_methods": "participant_partition_log_risk_ratio",
                "surface_value_min": 0.1,
                "surface_value_median": 0.1,
                "surface_value_max": 0.1,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 0.1,
                "family_effect_precision": 0.35,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f7",
                "nct_id": "N7",
                "family_label": "sexual_function",
                "family_domain": "sexual_function",
                "analysis_population": "Safety set",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "post_baseline_level",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 3,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "IIEF overall score",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 2.5,
                "surface_value_median": 2.5,
                "surface_value_max": 2.5,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 2.5,
                "family_effect_precision": 0.30,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
        ]
    )


def test_build_topic_family_synthesis_pools_only_strictly_exchangeable_rows() -> None:
    synthesis = build_topic_family_synthesis(build_family_pooled_fixture())

    assert len(synthesis) == 5

    activity = synthesis.loc[synthesis["family_label"] == "activity_volume"].iloc[0]
    assert activity["trial_count"] == 2
    assert activity["family_pool_count"] == 2
    assert activity["canonical_effect_count"] == 3
    assert activity["raw_surface_count"] == 36
    assert activity["nct_ids"] == "N1;N2"
    assert isclose(float(activity["topic_effect_value"]), 1.5, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(activity["topic_effect_mad_sd"]), 0.44478, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(activity["topic_effect_precision"]), 0.45, rel_tol=0.0, abs_tol=1e-6)
    assert activity["synthesis_status"] == "median_precision_retained"

    serious = synthesis.loc[synthesis["family_label"] == "adverse_event_risk_serious"].iloc[0]
    assert serious["trial_count"] == 2
    assert isclose(float(serious["topic_effect_value"]), -0.5, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(serious["topic_effect_mad_sd"]), 0.59304, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(serious["topic_effect_precision"]), 0.59304, rel_tol=0.0, abs_tol=1e-6)
    assert serious["synthesis_status"] == "dispersion_guardrail_applied"

    walk = synthesis.loc[
        (synthesis["family_label"] == "walk_responder_risk")
        & (synthesis["analysis_population"] == "FAS")
    ].iloc[0]
    assert walk["trial_count"] == 1
    assert walk["synthesis_method"] == "single_trial_identity"
    assert isclose(float(walk["topic_effect_precision"]), 0.25, rel_tol=0.0, abs_tol=1e-6)


def test_build_topic_family_recurrence_summary_flags_population_fragmentation() -> None:
    family_pooled = build_family_pooled_fixture()
    synthesis = build_topic_family_synthesis(family_pooled)
    summary = build_topic_family_recurrence_summary(family_pooled, synthesis)

    assert len(summary) == 4

    activity = summary.loc[summary["family_label"] == "activity_volume"].iloc[0]
    assert activity["trial_count"] == 2
    assert activity["strict_manifold_count"] == 1
    assert activity["multi_trial_manifold_count"] == 1
    assert activity["recurrence_status"] == "strict_cross_trial_manifold_present"

    walk = summary.loc[summary["family_label"] == "walk_responder_risk"].iloc[0]
    assert walk["trial_count"] == 2
    assert walk["analysis_population_count"] == 2
    assert walk["strict_manifold_count"] == 2
    assert walk["multi_trial_manifold_count"] == 0
    assert walk["recurrence_status"] == "stratified_no_strict_overlap"

    sexual = summary.loc[summary["family_label"] == "sexual_function"].iloc[0]
    assert sexual["trial_count"] == 1
    assert sexual["recurrence_status"] == "single_trial_only"


def test_materialize_topic_family_synthesis_outputs_writes_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)
    build_family_pooled_fixture().to_parquet(
        output_dir / "family_pooled_effects.parquet",
        index=False,
    )

    manifest = materialize_topic_family_synthesis_outputs(output_dir, topic=PHASE1_TOPIC)

    assert manifest["topic_slug"] == "sacubitril_valsartan_hfref"
    assert manifest["topic_synthesis_count"] == 5
    assert manifest["source_family_pool_count"] == 7
    assert manifest["multi_trial_manifold_count"] == 2
    assert manifest["single_trial_identity_count"] == 3
    assert manifest["recurring_family_label_count"] == 3
    assert manifest["stratified_recurrence_count"] == 1
    assert manifest["precision_ready_topic_synthesis_count"] == 5
    assert (output_dir / "topic_family_synthesis.parquet").exists()
    assert (output_dir / "topic_family_recurrence.parquet").exists()
    assert (output_dir / "topic_family_synthesis_report.md").exists()
    assert (output_dir / "topic_family_synthesis_manifest.json").exists()
