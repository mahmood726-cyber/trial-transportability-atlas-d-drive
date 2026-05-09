from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas.effect_candidates import build_effect_candidates
from trial_transportability_atlas.frontier import (
    build_frontier_candidate_states,
    build_frontier_country_year,
    build_frontier_trial_summary,
    materialize_frontier_outputs,
)
from trial_transportability_atlas.topics import PHASE1_TOPIC


def build_outcomes_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nct_id": "N1",
                "source_table": "outcome_measurements",
                "record_type": "measurement",
                "outcome_id": "1",
                "outcome_type": "PRIMARY",
                "outcome_name": "Walk distance",
                "analysis_population": "all",
                "time_frame": "12 weeks",
                "unit": "meters",
                "result_group_id": "10",
                "ctgov_group_code": "OG1",
                "classification": None,
                "category": None,
                "param_type": "MEAN",
                "value_text": "100",
                "value_num": 100.0,
                "dispersion_type": "Standard Deviation",
                "dispersion_value": "15",
                "event_type": None,
                "subjects_affected": None,
                "subjects_at_risk": None,
                "event_count": None,
                "organ_system": None,
                "adverse_event_term": None,
                "provenance": "x",
            },
            {
                "nct_id": "N1",
                "source_table": "outcome_measurements",
                "record_type": "measurement",
                "outcome_id": "1",
                "outcome_type": "PRIMARY",
                "outcome_name": "Walk distance",
                "analysis_population": "all",
                "time_frame": "12 weeks",
                "unit": "meters",
                "result_group_id": "11",
                "ctgov_group_code": "OG2",
                "classification": None,
                "category": None,
                "param_type": "MEAN",
                "value_text": "85",
                "value_num": 85.0,
                "dispersion_type": "Standard Deviation",
                "dispersion_value": "18",
                "event_type": None,
                "subjects_affected": None,
                "subjects_at_risk": None,
                "event_count": None,
                "organ_system": None,
                "adverse_event_term": None,
                "provenance": "x",
            },
            {
                "nct_id": "N2",
                "source_table": "reported_events",
                "record_type": "reported_event",
                "outcome_id": None,
                "outcome_type": None,
                "outcome_name": "Heart failure worsening",
                "analysis_population": None,
                "time_frame": "12 months",
                "unit": "participants",
                "result_group_id": "20",
                "ctgov_group_code": "OG1",
                "classification": None,
                "category": None,
                "param_type": None,
                "value_text": None,
                "value_num": None,
                "dispersion_type": None,
                "dispersion_value": None,
                "event_type": "serious",
                "subjects_affected": 2,
                "subjects_at_risk": 100,
                "event_count": None,
                "organ_system": "Cardiac disorders",
                "adverse_event_term": "Heart failure worsening",
                "provenance": "y",
            },
            {
                "nct_id": "N2",
                "source_table": "reported_events",
                "record_type": "reported_event",
                "outcome_id": None,
                "outcome_type": None,
                "outcome_name": "Heart failure worsening",
                "analysis_population": None,
                "time_frame": "12 months",
                "unit": "participants",
                "result_group_id": "21",
                "ctgov_group_code": "OG2",
                "classification": None,
                "category": None,
                "param_type": None,
                "value_text": None,
                "value_num": None,
                "dispersion_type": None,
                "dispersion_value": None,
                "event_type": "serious",
                "subjects_affected": 6,
                "subjects_at_risk": 100,
                "event_count": None,
                "organ_system": "Cardiac disorders",
                "adverse_event_term": "Heart failure worsening",
                "provenance": "y",
            },
            {
                "nct_id": "N3",
                "source_table": "outcome_measurements",
                "record_type": "measurement",
                "outcome_id": "9",
                "outcome_type": "SECONDARY",
                "outcome_name": "Biomarker median",
                "analysis_population": "all",
                "time_frame": "12 weeks",
                "unit": "pg/mL",
                "result_group_id": "30",
                "ctgov_group_code": "OG1",
                "classification": None,
                "category": None,
                "param_type": "MEDIAN",
                "value_text": "10",
                "value_num": 10.0,
                "dispersion_type": None,
                "dispersion_value": None,
                "event_type": None,
                "subjects_affected": None,
                "subjects_at_risk": None,
                "event_count": None,
                "organ_system": None,
                "adverse_event_term": None,
                "provenance": "z",
            },
        ]
    )


def build_trial_country_year_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"nct_id": "N1", "country_name": "United Kingdom", "iso3": "GBR", "year": 2020},
            {"nct_id": "N2", "country_name": "United Kingdom", "iso3": "GBR", "year": 2020},
            {"nct_id": "N3", "country_name": "United States", "iso3": "USA", "year": 2020},
        ]
    )


def build_synthesis_output_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "iso3": "GBR",
                "country_name": "United Kingdom",
                "year": 2020,
                "intervention_name": "sacubitril/valsartan",
                "condition_name": "heart failure with reduced ejection fraction",
                "effect_status": "comparable_evidence_available",
                "effect_measure": pd.NA,
                "effect_value": pd.NA,
                "effect_precision": pd.NA,
                "country_coverage_score": 0.80,
                "context_distance": 0.20,
                "eligibility_support_score": 0.50,
                "reporting_completeness_score": 0.50,
                "transportability_score": 0.60,
                "priority_gap_score": 0.40,
                "source_manifest_id": "manifest-123",
            },
            {
                "iso3": "USA",
                "country_name": "United States",
                "year": 2020,
                "intervention_name": "sacubitril/valsartan",
                "condition_name": "heart failure with reduced ejection fraction",
                "effect_status": "no_comparable_evidence",
                "effect_measure": pd.NA,
                "effect_value": pd.NA,
                "effect_precision": pd.NA,
                "country_coverage_score": 0.20,
                "context_distance": 0.80,
                "eligibility_support_score": 0.00,
                "reporting_completeness_score": 0.00,
                "transportability_score": 0.066667,
                "priority_gap_score": 0.933333,
                "source_manifest_id": "manifest-123",
            },
        ]
    )


def test_frontier_candidate_states_promote_latent_reported_events() -> None:
    outcomes = build_outcomes_fixture()
    effect_candidates = build_effect_candidates(outcomes)

    states = build_frontier_candidate_states(outcomes, effect_candidates)
    latent = states.loc[states["nct_id"] == "N2"].iloc[0]
    direct = states.loc[states["nct_id"] == "N1"].iloc[0]

    assert bool(direct["recoverable_effect_flag"]) is True
    assert bool(latent["comparable_flag"]) is False
    assert bool(latent["latent_recovery_flag"]) is True
    assert bool(latent["recoverable_effect_flag"]) is True
    assert latent["recoverability_source"] == "subjects_affected_plus_denominator"


def test_frontier_country_year_joins_trial_scores() -> None:
    outcomes = build_outcomes_fixture()
    effect_candidates = build_effect_candidates(outcomes)
    candidate_states = build_frontier_candidate_states(outcomes, effect_candidates)
    trial_summary = build_frontier_trial_summary(candidate_states)

    frontier = build_frontier_country_year(
        trial_country_year=build_trial_country_year_fixture(),
        synthesis_output=build_synthesis_output_fixture(),
        trial_summary=trial_summary,
    )

    uk = frontier.loc[frontier["iso3"] == "GBR"].iloc[0]
    usa = frontier.loc[frontier["iso3"] == "USA"].iloc[0]

    assert uk["frontier_trial_count"] == 2
    assert uk["frontier_candidate_count"] == 2
    assert uk["frontier_recoverable_candidate_count"] == 2
    assert uk["frontier_latent_candidate_count"] == 1
    assert uk["frontier_latent_opportunity_ratio"] == 0.5
    assert uk["frontier_status"] == "needs_effect_recovery"
    assert usa["frontier_recoverable_candidate_count"] == 0
    assert usa["frontier_status"] == "frontier_gap"


def test_frontier_signed_effect_counts_flow_into_country_year_surface() -> None:
    outcomes = build_outcomes_fixture()
    effect_candidates = build_effect_candidates(outcomes)
    candidate_id = effect_candidates.loc[effect_candidates["nct_id"] == "N1", "candidate_id"].iloc[0]
    signed_effect_estimates = pd.DataFrame(
        [
            {
                "surface_id": "surface-1",
                "candidate_id": candidate_id,
                "nct_id": "N1",
                "effect_precision": 1.0,
            }
        ]
    )

    candidate_states = build_frontier_candidate_states(
        outcomes,
        effect_candidates,
        signed_effect_estimates=signed_effect_estimates,
    )
    trial_summary = build_frontier_trial_summary(candidate_states)
    frontier = build_frontier_country_year(
        trial_country_year=build_trial_country_year_fixture(),
        synthesis_output=build_synthesis_output_fixture(),
        trial_summary=trial_summary,
    )

    n1 = candidate_states.loc[candidate_states["nct_id"] == "N1"].iloc[0]
    uk = frontier.loc[frontier["iso3"] == "GBR"].iloc[0]

    assert bool(n1["signed_effect_recovered_flag"]) is True
    assert n1["signed_effect_surface_count"] == 1
    assert uk["frontier_signed_effect_candidate_count"] == 1
    assert uk["frontier_signed_effect_surface_count"] == 1
    assert uk["frontier_effect_realization_ratio"] == 0.5


def test_materialize_frontier_outputs_writes_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)

    outcomes = build_outcomes_fixture()
    effect_candidates = build_effect_candidates(outcomes)

    outcomes.to_parquet(output_dir / "trial_outcomes_long.parquet", index=False)
    effect_candidates.to_parquet(output_dir / "effect_candidates.parquet", index=False)
    build_trial_country_year_fixture().to_parquet(output_dir / "trial_country_year.parquet", index=False)
    build_synthesis_output_fixture().to_parquet(output_dir / "synthesis_output.parquet", index=False)

    manifest = materialize_frontier_outputs(output_dir, topic=PHASE1_TOPIC)

    assert manifest["topic_slug"] == "sacubitril_valsartan_hfref"
    assert manifest["candidate_rows"] == 3
    assert manifest["recoverable_candidate_count"] == 2
    assert manifest["latent_recoverable_candidate_count"] == 1
    assert (output_dir / "frontier_candidate_states.parquet").exists()
    assert (output_dir / "frontier_trial_summary.parquet").exists()
    assert (output_dir / "frontier_country_year.parquet").exists()
    assert (output_dir / "frontier_report.md").exists()
    assert (output_dir / "frontier_manifest.json").exists()
