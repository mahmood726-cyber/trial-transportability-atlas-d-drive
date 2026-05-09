from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas.arm_mapping import (
    build_arm_group_mapping,
    build_narrative_arm_counts,
    load_design_arm_catalog,
    materialize_arm_mapping_outputs,
)


def build_design_arms_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nct_id": "N1",
                "design_group_id": "DG1",
                "group_type": "EXPERIMENTAL",
                "arm_title": "LCZ696 (Sacubitril/Valsartan)",
                "intervention_names": "LCZ696 (Sacubitril/Valsartan)",
                "active_intervention_names": "LCZ696 (Sacubitril/Valsartan)",
                "arm_aliases": "lcz696 sacubitril valsartan;sacubitril valsartan;lcz696",
            },
            {
                "nct_id": "N1",
                "design_group_id": "DG2",
                "group_type": "ACTIVE_COMPARATOR",
                "arm_title": "Enalapril",
                "intervention_names": "Enalapril",
                "active_intervention_names": "Enalapril",
                "arm_aliases": "enalapril",
            },
            {
                "nct_id": "N2",
                "design_group_id": "DG3",
                "group_type": "EXPERIMENTAL",
                "arm_title": "LCZ696",
                "intervention_names": "LCZ696",
                "active_intervention_names": "LCZ696",
                "arm_aliases": "lcz696",
            },
            {
                "nct_id": "N3",
                "design_group_id": "DG4",
                "group_type": "EXPERIMENTAL",
                "arm_title": "sacubitril/valsartan (LCZ696)",
                "intervention_names": "sacubitril/valsartan (LCZ696)",
                "active_intervention_names": "sacubitril/valsartan (LCZ696)",
                "arm_aliases": "sacubitril valsartan;lcz696;sacubitril valsartan lcz696",
            },
            {
                "nct_id": "N3",
                "design_group_id": "DG5",
                "group_type": "ACTIVE_COMPARATOR",
                "arm_title": "Enalapril",
                "intervention_names": "Enalapril",
                "active_intervention_names": "Enalapril",
                "arm_aliases": "enalapril",
            },
            {
                "nct_id": "N4",
                "design_group_id": "DG6",
                "group_type": "EXPERIMENTAL",
                "arm_title": "sacubitril/valsartan",
                "intervention_names": "sacubitril/valsartan",
                "active_intervention_names": "sacubitril/valsartan",
                "arm_aliases": "sacubitril valsartan;lcz696",
            },
            {
                "nct_id": "N4",
                "design_group_id": "DG7",
                "group_type": "ACTIVE_COMPARATOR",
                "arm_title": "Enalapril",
                "intervention_names": "Enalapril",
                "active_intervention_names": "Enalapril",
                "arm_aliases": "enalapril",
            },
        ]
    )


def build_participant_flows_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nct_id": "N1",
                "participant_flow_text": "310 in the sacubitril/valsartan group and 311 in the enalapril group.",
                "participant_flow_text_norm": "310 in the sacubitril valsartan group and 311 in the enalapril group",
            },
            {
                "nct_id": "N2",
                "participant_flow_text": "",
                "participant_flow_text_norm": "",
            },
            {
                "nct_id": "N3",
                "participant_flow_text": (
                    "Of the 887 patients randomized, 444 to enalapril and 443 to "
                    "sacubitril/valsartan, 881 were treated."
                ),
                "participant_flow_text_norm": (
                    "of the 887 patients randomized 444 to enalapril and 443 to "
                    "sacubitril valsartan 881 were treated"
                ),
            },
            {
                "nct_id": "N4",
                "participant_flow_text": (
                    "Of the 465 randomized, 1 patient was randomized in error to the "
                    "sacubitril/valsartan group and was not treated. The Full Analysis "
                    "Set and Safety Set are based on 464 patients who received treatment."
                ),
                "participant_flow_text_norm": (
                    "of the 465 randomized 1 patient was randomized in error to the "
                    "sacubitril valsartan group and was not treated the full analysis "
                    "set and safety set are based on 464 patients who received treatment"
                ),
            },
        ]
    )


def build_group_codes_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "nct_id": "N1",
                "ctgov_group_code": "BG000",
                "result_group_id": "1",
                "group_family": "BG",
                "group_suffix": "000",
                "count_value": 310,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N1",
                "ctgov_group_code": "BG001",
                "result_group_id": "2",
                "group_family": "BG",
                "group_suffix": "001",
                "count_value": 311,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N1",
                "ctgov_group_code": "BG002",
                "result_group_id": "3",
                "group_family": "BG",
                "group_suffix": "002",
                "count_value": 621,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N1",
                "ctgov_group_code": "OG000",
                "result_group_id": "4",
                "group_family": "OG",
                "group_suffix": "000",
                "count_value": pd.NA,
                "count_sources": "trial_outcomes",
            },
            {
                "nct_id": "N1",
                "ctgov_group_code": "OG001",
                "result_group_id": "5",
                "group_family": "OG",
                "group_suffix": "001",
                "count_value": pd.NA,
                "count_sources": "trial_outcomes",
            },
            {
                "nct_id": "N1",
                "ctgov_group_code": "EG000",
                "result_group_id": "6",
                "group_family": "EG",
                "group_suffix": "000",
                "count_value": 309,
                "count_sources": "reported_event_totals",
            },
            {
                "nct_id": "N1",
                "ctgov_group_code": "EG001",
                "result_group_id": "7",
                "group_family": "EG",
                "group_suffix": "001",
                "count_value": 310,
                "count_sources": "reported_event_totals",
            },
            {
                "nct_id": "N2",
                "ctgov_group_code": "BG000",
                "result_group_id": "8",
                "group_family": "BG",
                "group_suffix": "000",
                "count_value": 20,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N2",
                "ctgov_group_code": "OG000",
                "result_group_id": "9",
                "group_family": "OG",
                "group_suffix": "000",
                "count_value": pd.NA,
                "count_sources": "trial_outcomes",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "BG000",
                "result_group_id": "10",
                "group_family": "BG",
                "group_suffix": "000",
                "count_value": 441,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "BG001",
                "result_group_id": "11",
                "group_family": "BG",
                "group_suffix": "001",
                "count_value": 440,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "BG002",
                "result_group_id": "12",
                "group_family": "BG",
                "group_suffix": "002",
                "count_value": 881,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "OG000",
                "result_group_id": "13",
                "group_family": "OG",
                "group_suffix": "000",
                "count_value": 441,
                "count_sources": "outcome_counts;trial_outcomes",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "OG001",
                "result_group_id": "14",
                "group_family": "OG",
                "group_suffix": "001",
                "count_value": 440,
                "count_sources": "outcome_counts;trial_outcomes",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "FG000",
                "result_group_id": "15",
                "group_family": "FG",
                "group_suffix": "000",
                "count_value": 444,
                "count_sources": "milestones",
            },
            {
                "nct_id": "N3",
                "ctgov_group_code": "FG001",
                "result_group_id": "16",
                "group_family": "FG",
                "group_suffix": "001",
                "count_value": 887,
                "count_sources": "milestones",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "FG000",
                "result_group_id": "17",
                "group_family": "FG",
                "group_suffix": "000",
                "count_value": 233,
                "count_sources": "milestones",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "FG001",
                "result_group_id": "18",
                "group_family": "FG",
                "group_suffix": "001",
                "count_value": 232,
                "count_sources": "milestones",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "BG000",
                "result_group_id": "19",
                "group_family": "BG",
                "group_suffix": "000",
                "count_value": 233,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "BG001",
                "result_group_id": "20",
                "group_family": "BG",
                "group_suffix": "001",
                "count_value": 231,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "BG002",
                "result_group_id": "21",
                "group_family": "BG",
                "group_suffix": "002",
                "count_value": 464,
                "count_sources": "baseline_counts",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "OG000",
                "result_group_id": "22",
                "group_family": "OG",
                "group_suffix": "000",
                "count_value": 233,
                "count_sources": "outcome_counts;trial_outcomes",
            },
            {
                "nct_id": "N4",
                "ctgov_group_code": "OG001",
                "result_group_id": "23",
                "group_family": "OG",
                "group_suffix": "001",
                "count_value": 231,
                "count_sources": "outcome_counts;trial_outcomes",
            },
        ]
    )


def build_trial_outcomes_fixture() -> pd.DataFrame:
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
                "result_group_id": "1",
                "ctgov_group_code": "OG000",
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
                "result_group_id": "2",
                "ctgov_group_code": "OG001",
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
                "source_table": "outcome_measurements",
                "record_type": "measurement",
                "outcome_id": "2",
                "outcome_type": "PRIMARY",
                "outcome_name": "Single arm",
                "analysis_population": "all",
                "time_frame": "12 weeks",
                "unit": "meters",
                "result_group_id": "9",
                "ctgov_group_code": "OG000",
                "classification": None,
                "category": None,
                "param_type": "MEAN",
                "value_text": "50",
                "value_num": 50.0,
                "dispersion_type": "Standard Deviation",
                "dispersion_value": "5",
                "event_type": None,
                "subjects_affected": None,
                "subjects_at_risk": None,
                "event_count": None,
                "organ_system": None,
                "adverse_event_term": None,
                "provenance": "y",
            },
        ]
    )


def test_build_narrative_arm_counts_extracts_exact_counts() -> None:
    counts = build_narrative_arm_counts(
        build_design_arms_fixture(),
        build_participant_flows_fixture(),
    )

    assert set(counts["nct_id"]) == {"N1", "N3"}
    by_design = dict(zip(counts["design_group_id"], counts["narrative_count"]))
    assert by_design["DG1"] == 310
    assert by_design["DG2"] == 311
    assert by_design["DG4"] == 443
    assert by_design["DG5"] == 444


def test_build_arm_group_mapping_anchors_and_projects_suffixes() -> None:
    mapping, summary = build_arm_group_mapping(
        build_design_arms_fixture(),
        build_participant_flows_fixture(),
        build_group_codes_fixture(),
    )

    bg000 = mapping.loc[mapping["ctgov_group_code"].eq("BG000")].iloc[0]
    og001 = mapping.loc[mapping["ctgov_group_code"].eq("OG001")].iloc[0]
    bg002 = mapping.loc[mapping["ctgov_group_code"].eq("BG002")].iloc[0]
    single = mapping.loc[
        mapping["nct_id"].eq("N2") & mapping["ctgov_group_code"].eq("OG000")
    ].iloc[0]
    n3_og000 = mapping.loc[
        mapping["nct_id"].eq("N3") & mapping["ctgov_group_code"].eq("OG000")
    ].iloc[0]
    n3_bg001 = mapping.loc[
        mapping["nct_id"].eq("N3") & mapping["ctgov_group_code"].eq("BG001")
    ].iloc[0]
    n3_fg001 = mapping.loc[
        mapping["nct_id"].eq("N3") & mapping["ctgov_group_code"].eq("FG001")
    ].iloc[0]
    n4_fg001 = mapping.loc[
        mapping["nct_id"].eq("N4") & mapping["ctgov_group_code"].eq("FG001")
    ].iloc[0]
    n4_og001 = mapping.loc[
        mapping["nct_id"].eq("N4") & mapping["ctgov_group_code"].eq("OG001")
    ].iloc[0]
    n4_bg002 = mapping.loc[
        mapping["nct_id"].eq("N4") & mapping["ctgov_group_code"].eq("BG002")
    ].iloc[0]

    assert bg000["mapped_arm_title"] == "LCZ696 (Sacubitril/Valsartan)"
    assert bg000["mapping_method"] == "narrative_count_match_bg"
    assert og001["mapped_arm_title"] == "Enalapril"
    assert og001["mapping_method"] == "suffix_projection_from_anchor"
    assert bg002["mapping_status"] == "unmapped"
    assert single["mapping_method"] == "direct_single_arm_suffix"
    assert summary.loc[summary["nct_id"].eq("N1"), "mapped_group_code_count"].iloc[0] == 6
    assert n3_og000["mapped_arm_title"] == "Enalapril"
    assert n3_og000["mapping_method"] == "near_exact_rank_match_og"
    assert n3_bg001["mapped_arm_title"] == "sacubitril/valsartan (LCZ696)"
    assert n3_bg001["mapping_method"] == "suffix_projection_from_anchor"
    assert n3_fg001["mapping_status"] == "unmapped"
    assert summary.loc[summary["nct_id"].eq("N3"), "mapped_group_code_count"].iloc[0] == 5
    assert n4_fg001["mapped_arm_title"] == "sacubitril/valsartan"
    assert n4_fg001["mapping_method"] == "arm_specific_exclusion_delta_match_fg"
    assert n4_og001["mapped_arm_title"] == "sacubitril/valsartan"
    assert n4_og001["mapping_method"] == "suffix_projection_from_anchor"
    assert n4_og001["anchor_count_value"] == 231
    assert n4_bg002["mapping_status"] == "unmapped"
    assert summary.loc[summary["nct_id"].eq("N4"), "mapped_group_code_count"].iloc[0] == 6


def test_load_design_arm_catalog_adds_parenthetical_stripped_aliases(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir(parents=True)

    (snapshot_dir / "design_groups.txt").write_text(
        "id|nct_id|group_type|title|description\n"
        "DG1|N1|EXPERIMENTAL|sacubitril/valsartan (LCZ696)|x\n",
        encoding="utf-8",
    )
    (snapshot_dir / "design_group_interventions.txt").write_text(
        "id|nct_id|design_group_id|intervention_id\n"
        "1|N1|DG1|I1\n",
        encoding="utf-8",
    )
    (snapshot_dir / "interventions.txt").write_text(
        "id|nct_id|intervention_type|name|description\n"
        "I1|N1|DRUG|sacubitril/valsartan (LCZ696)|x\n",
        encoding="utf-8",
    )
    (snapshot_dir / "participant_flows.txt").write_text(
        "id|nct_id|recruitment_details|pre_assignment_details\n"
        "1|N1||\n",
        encoding="utf-8",
    )
    (snapshot_dir / "baseline_counts.txt").write_text(
        "id|nct_id|result_group_id|ctgov_group_code|units|scope|count\n",
        encoding="utf-8",
    )
    (snapshot_dir / "outcome_counts.txt").write_text(
        "id|nct_id|outcome_id|result_group_id|ctgov_group_code|scope|units|count\n",
        encoding="utf-8",
    )
    (snapshot_dir / "milestones.txt").write_text(
        "id|nct_id|result_group_id|ctgov_group_code|title|period|description|count\n",
        encoding="utf-8",
    )
    (snapshot_dir / "reported_event_totals.txt").write_text(
        "id|nct_id|ctgov_group_code|event_type|classification|subjects_affected|subjects_at_risk\n",
        encoding="utf-8",
    )

    design = load_design_arm_catalog(snapshot_dir)

    aliases = design.iloc[0]["arm_aliases"].split(";")
    assert "sacubitril valsartan" in aliases


def test_materialize_arm_mapping_outputs_writes_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True)
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir(parents=True)

    build_trial_outcomes_fixture().to_parquet(output_dir / "trial_outcomes_long.parquet", index=False)

    (snapshot_dir / "design_groups.txt").write_text(
        "id|nct_id|group_type|title|description\n"
        "DG1|N1|EXPERIMENTAL|LCZ696 (Sacubitril/Valsartan)|x\n"
        "DG2|N1|ACTIVE_COMPARATOR|Enalapril|y\n"
        "DG3|N2|EXPERIMENTAL|LCZ696|z\n",
        encoding="utf-8",
    )
    (snapshot_dir / "design_group_interventions.txt").write_text(
        "id|nct_id|design_group_id|intervention_id\n"
        "1|N1|DG1|I1\n"
        "2|N1|DG2|I2\n"
        "3|N2|DG3|I3\n",
        encoding="utf-8",
    )
    (snapshot_dir / "interventions.txt").write_text(
        "id|nct_id|intervention_type|name|description\n"
        "I1|N1|DRUG|LCZ696 (Sacubitril/Valsartan)|x\n"
        "I2|N1|DRUG|Enalapril|y\n"
        "I3|N2|DRUG|LCZ696|z\n",
        encoding="utf-8",
    )
    (snapshot_dir / "participant_flows.txt").write_text(
        "id|nct_id|recruitment_details|pre_assignment_details|units_analyzed\n"
        "1|N1|310 in the sacubitril/valsartan group and 311 in the enalapril group.||\n"
        "2|N2|||Participants\n",
        encoding="utf-8",
    )
    (snapshot_dir / "baseline_counts.txt").write_text(
        "id|nct_id|result_group_id|ctgov_group_code|units|scope|count\n"
        "1|N1|1|BG000|Participants|overall|310\n"
        "2|N1|2|BG001|Participants|overall|311\n"
        "3|N1|3|BG002|Participants|overall|621\n"
        "4|N2|8|BG000|Participants|overall|20\n",
        encoding="utf-8",
    )
    (snapshot_dir / "outcome_counts.txt").write_text(
        "id|nct_id|outcome_id|result_group_id|ctgov_group_code|scope|units|count\n"
        "1|N1|1|4|OG000|Measure|Participants|310\n"
        "2|N1|1|5|OG001|Measure|Participants|311\n"
        "3|N2|2|9|OG000|Measure|Participants|20\n",
        encoding="utf-8",
    )
    (snapshot_dir / "milestones.txt").write_text(
        "id|nct_id|result_group_id|ctgov_group_code|title|period|description|count|milestone_description|count_units\n"
        "1|N1|1|FG000|STARTED|Overall Study||310||Participants\n"
        "2|N1|2|FG001|STARTED|Overall Study||311||Participants\n",
        encoding="utf-8",
    )
    (snapshot_dir / "reported_event_totals.txt").write_text(
        "id|nct_id|ctgov_group_code|event_type|classification|subjects_affected|subjects_at_risk|created_at|updated_at\n"
        "1|N1|EG000|deaths|Total, all-cause mortality|1|309|x|x\n"
        "2|N1|EG001|deaths|Total, all-cause mortality|4|310|x|x\n",
        encoding="utf-8",
    )

    manifest = materialize_arm_mapping_outputs(output_dir, snapshot_dir=snapshot_dir)

    assert manifest["trial_count"] == 2
    assert manifest["mapped_trial_count"] == 2
    assert manifest["mapped_group_code_count"] == 12
    assert manifest["total_group_code_count"] == 13
    assert (output_dir / "arm_design_catalog.parquet").exists()
    assert (output_dir / "arm_group_mapping.parquet").exists()
    assert (output_dir / "arm_mapping_summary.parquet").exists()
    assert (output_dir / "arm_mapping_report.md").exists()
    assert (output_dir / "arm_mapping_manifest.json").exists()
