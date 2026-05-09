from __future__ import annotations

from math import isclose
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.effect_normalization import (
    build_normalized_family_effects,
    build_normalized_topic_manifolds,
    build_normalized_topic_recurrence,
    materialize_normalized_topic_outputs,
)
from trial_transportability_atlas.topics import PHASE1_TOPIC


def build_family_pooled_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "family_pool_id": "f1",
                "nct_id": "N1",
                "family_label": "walk_performance",
                "family_domain": "walk_performance",
                "analysis_population": "The Full Analysis Set (FAS) and FAS population subset without AE/SAE were considered",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 4,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "6MWT distance",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 10.0,
                "surface_value_median": 10.0,
                "surface_value_max": 10.0,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 10.0,
                "family_effect_precision": 0.50,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f2",
                "nct_id": "N1",
                "family_label": "walk_performance",
                "family_domain": "walk_performance",
                "analysis_population": "The Full Analysis Set (FAS) and FAS population subset without AE/SAE were considered.",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 8,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "6MWT distance",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 10.4,
                "surface_value_median": 10.4,
                "surface_value_max": 10.4,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 10.4,
                "family_effect_precision": 0.60,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f3",
                "nct_id": "N1",
                "family_label": "activity_volume",
                "family_domain": "activity_volume",
                "analysis_population": "The Full Analysis Set was considered",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 2,
                "precision_ready_canonical_effect_count": 2,
                "raw_surface_count": 12,
                "outcome_name_count": 2,
                "candidate_id_count": 2,
                "outcome_name_examples": "Daily physical activity;Weekly activity",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 1.0,
                "surface_value_median": 1.2,
                "surface_value_max": 1.4,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 1.2,
                "family_effect_precision": 0.40,
                "precision_metric": "standard_error",
                "pooling_method": "family_median_no_precision_gain",
                "pooling_status": "median_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f4",
                "nct_id": "N1",
                "family_label": "activity_volume",
                "family_domain": "activity_volume",
                "analysis_population": "The FAS population with Multiple Imputation (MI), with Last Observation Carried Forward (LOCF) and without MI/LOCF were considered.",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 6,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "Daily physical activity sensitivity",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 0.8,
                "surface_value_median": 0.9,
                "surface_value_max": 1.0,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 0.9,
                "family_effect_precision": 0.55,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
            {
                "family_pool_id": "f5",
                "nct_id": "N2",
                "family_label": "activity_volume",
                "family_domain": "activity_volume",
                "analysis_population": "The Full Analysis Set was considered.",
                "candidate_family": "continuous_mean",
                "effect_measure": "mean_difference",
                "estimand_type": "change_from_baseline",
                "effect_direction": "treatment_minus_comparator",
                "canonical_effect_count": 1,
                "precision_ready_canonical_effect_count": 1,
                "raw_surface_count": 10,
                "outcome_name_count": 1,
                "candidate_id_count": 1,
                "outcome_name_examples": "Weekly activity",
                "derivation_methods": "continuous_surface_split",
                "surface_value_min": 1.5,
                "surface_value_median": 1.7,
                "surface_value_max": 1.9,
                "family_effect_iqr": 0.0,
                "family_effect_mad_sd": 0.0,
                "family_effect_value": 1.7,
                "family_effect_precision": 0.45,
                "precision_metric": "standard_error",
                "pooling_method": "single_canonical_identity",
                "pooling_status": "single_canonical_precision_retained",
                "sign_coherence": 1.0,
            },
        ]
    )


def test_build_normalized_family_effects_assigns_ontology_and_population_keys() -> None:
    normalized = build_normalized_family_effects(build_family_pooled_fixture())

    walk = normalized.loc[normalized["family_pool_id"] == "f1"].iloc[0]
    assert walk["normalized_family_label"] == "walking_capacity"
    assert walk["normalized_family_variant"] == "performance"
    assert walk["population_core"] == "full_analysis_set"
    assert walk["population_modifier_flags"] == "subset;without_ae_sae"
    assert walk["normalized_analysis_population"] == "full_analysis_set+subset+without_ae_sae"

    activity_sensitivity = normalized.loc[normalized["family_pool_id"] == "f4"].iloc[0]
    assert activity_sensitivity["normalized_family_label"] == "activity_function"
    assert activity_sensitivity["population_core"] == "full_analysis_set"
    assert activity_sensitivity["population_modifier_flags"] == "imputation_sensitivity"


def test_build_normalized_topic_manifolds_collapses_wording_and_preserves_strata() -> None:
    normalized = build_normalized_family_effects(build_family_pooled_fixture())
    manifolds = build_normalized_topic_manifolds(normalized)

    assert len(manifolds) == 3

    activity = manifolds.loc[
        (manifolds["normalized_family_label"] == "activity_function")
        & (manifolds["normalized_analysis_population"] == "full_analysis_set")
    ].iloc[0]
    assert activity["trial_count"] == 2
    assert activity["family_pool_count"] == 2
    assert activity["nct_ids"] == "N1;N2"
    assert isclose(float(activity["normalized_effect_value"]), 1.45, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(activity["normalized_effect_precision"]), 0.425, rel_tol=0.0, abs_tol=1e-6)
    assert activity["normalization_status"] == "median_precision_retained"

    walk = manifolds.loc[
        (manifolds["normalized_family_label"] == "walking_capacity")
        & (manifolds["normalized_analysis_population"] == "full_analysis_set+subset+without_ae_sae")
    ].iloc[0]
    assert walk["trial_count"] == 1
    assert walk["family_pool_count"] == 2
    assert walk["raw_surface_count"] == 12
    assert isclose(float(walk["normalized_effect_value"]), 10.2, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(walk["normalized_effect_mad_sd"]), 0.29652, rel_tol=0.0, abs_tol=1e-6)
    assert isclose(float(walk["normalized_effect_precision"]), 0.55, rel_tol=0.0, abs_tol=1e-6)

    sensitivity = manifolds.loc[
        manifolds["normalized_analysis_population"] == "full_analysis_set+imputation_sensitivity"
    ].iloc[0]
    assert sensitivity["family_pool_count"] == 1
    assert sensitivity["normalization_method"] == "single_family_pool_identity"


def test_build_normalized_topic_recurrence_separates_wording_from_stratification() -> None:
    normalized = build_normalized_family_effects(build_family_pooled_fixture())
    manifolds = build_normalized_topic_manifolds(normalized)
    recurrence = build_normalized_topic_recurrence(normalized, manifolds)

    assert len(recurrence) == 2

    activity = recurrence.loc[recurrence["normalized_family_label"] == "activity_function"].iloc[0]
    assert activity["trial_count"] == 2
    assert activity["family_pool_count"] == 3
    assert activity["normalized_population_count"] == 2
    assert activity["normalized_manifold_count"] == 2
    assert activity["multi_trial_manifold_count"] == 1
    assert activity["recurrence_status"] == "normalized_cross_trial_manifold_present"

    walk = recurrence.loc[recurrence["normalized_family_label"] == "walking_capacity"].iloc[0]
    assert walk["trial_count"] == 1
    assert walk["family_pool_count"] == 2
    assert walk["normalized_population_count"] == 1
    assert walk["normalized_manifold_count"] == 1
    assert walk["recurrence_status"] == "within_trial_wording_normalized"


def test_materialize_normalized_topic_outputs_writes_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)
    build_family_pooled_fixture().to_parquet(
        output_dir / "family_pooled_effects.parquet",
        index=False,
    )

    manifest = materialize_normalized_topic_outputs(output_dir, topic=PHASE1_TOPIC)

    assert manifest["topic_slug"] == "sacubitril_valsartan_hfref"
    assert manifest["source_family_pool_count"] == 5
    assert manifest["normalized_topic_manifold_count"] == 3
    assert manifest["multi_trial_normalized_manifold_count"] == 1
    assert manifest["precision_ready_normalized_manifold_count"] == 3
    assert manifest["wording_normalized_group_count"] == 1
    assert manifest["population_stratification_group_count"] == 0
    assert (output_dir / "normalized_family_effects.parquet").exists()
    assert (output_dir / "normalized_topic_manifolds.parquet").exists()
    assert (output_dir / "normalized_topic_recurrence.parquet").exists()
    assert (output_dir / "normalized_topic_report.md").exists()
    assert (output_dir / "normalized_topic_manifest.json").exists()
