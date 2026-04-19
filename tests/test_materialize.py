from __future__ import annotations

from pathlib import Path

import json
import pandas as pd

from trial_transportability_atlas.materialize import materialize_topic_bridge

from tests.test_aact_bridge import build_minimal_snapshot, write_table


def test_materialize_topic_bridge_writes_filtered_parquet_outputs(tmp_path: Path) -> None:
    snapshot_dir = build_minimal_snapshot(tmp_path / "snapshot")
    write_table(
        snapshot_dir,
        "interventions",
        [
            "id|nct_id|intervention_type|name|description",
            "1|NCTGOOD|Drug|Sacubitril/Valsartan|Active drug",
            "2|NCTBAD|Drug|Sacubitril/Valsartan|Active drug",
        ],
    )
    write_table(
        snapshot_dir,
        "conditions",
        [
            "id|nct_id|name|downcase_name",
            "1|NCTGOOD|Heart Failure, Reduced Ejection Fraction|heart failure, reduced ejection fraction",
            "2|NCTBAD|Pulmonary Hypertension|pulmonary hypertension",
        ],
    )
    write_table(
        snapshot_dir,
        "brief_summaries",
        [
            "id|nct_id|description",
            "1|NCTGOOD|Patients with reduced ejection fraction.",
            "2|NCTBAD|Pulmonary hypertension only.",
        ],
    )
    write_table(
        snapshot_dir,
        "countries",
        [
            "id|nct_id|name|removed",
            "20|NCTGOOD|United Kingdom|f",
            "21|NCTBAD|France|f",
        ],
    )
    write_table(
        snapshot_dir,
        "calculated_values",
        [
            "id|nct_id|number_of_facilities|number_of_nsae_subjects|number_of_sae_subjects|registered_in_calendar_year|nlm_download_date|actual_duration|were_results_reported|months_to_report_results|has_us_facility|has_single_facility|minimum_age_num|maximum_age_num|minimum_age_unit|maximum_age_unit|number_of_primary_outcomes_to_measure|number_of_secondary_outcomes_to_measure|number_of_other_outcomes_to_measure",
            "50|NCTGOOD||||2020||18|t||||||||||",
            "51|NCTBAD||||2019||12|f||||||||||",
        ],
    )
    write_table(
        snapshot_dir,
        "outcomes",
        [
            "id|nct_id|outcome_type|title|description|time_frame|population|anticipated_posting_date|anticipated_posting_month_year|units|units_analyzed|dispersion_type|param_type",
            "60|NCTGOOD|PRIMARY|All-cause mortality|Deaths from any cause|12 months|all participants|||Participants|||COUNT_OF_PARTICIPANTS",
            "61|NCTBAD|PRIMARY|Pressure|Pressure outcome|12 months|all participants|||Participants|||COUNT_OF_PARTICIPANTS",
        ],
    )
    write_table(
        snapshot_dir,
        "outcome_measurements",
        [
            "id|nct_id|outcome_id|result_group_id|ctgov_group_code|classification|category|title|description|units|param_type|param_value|param_value_num|dispersion_type|dispersion_value|dispersion_value_num|dispersion_lower_limit|dispersion_upper_limit|explanation_of_na|dispersion_upper_limit_raw|dispersion_lower_limit_raw",
            "70|NCTGOOD|60|500|OG001|Deaths||All-cause mortality||Participants|COUNT_OF_PARTICIPANTS|5|5||||||||",
            "71|NCTGOOD|60|501|OG002|Deaths||All-cause mortality||Participants|COUNT_OF_PARTICIPANTS|7|7||||||||",
            "71|NCTBAD|61|501|OG002|Pressure||Pressure||Participants|COUNT_OF_PARTICIPANTS|7|7||||||||",
        ],
    )

    output_dir = tmp_path / "outputs"
    summary = materialize_topic_bridge(snapshot_dir, output_dir)

    country_df = pd.read_parquet(output_dir / "trial_country_year.parquet")
    outcomes_df = pd.read_parquet(output_dir / "trial_outcomes_long.parquet")
    candidates_df = pd.read_parquet(output_dir / "effect_candidates.parquet")
    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))

    assert summary["selected_nct_ids"] == ["NCTGOOD"]
    assert country_df["nct_id"].unique().tolist() == ["NCTGOOD"]
    assert outcomes_df["nct_id"].unique().tolist() == ["NCTGOOD"]
    assert candidates_df["nct_id"].unique().tolist() == ["NCTGOOD"]
    assert candidates_df["comparable_flag"].tolist() == [True]
    assert candidates_df["candidate_family"].tolist() == ["binary_participant_count"]
    assert manifest["topic_slug"] == "sacubitril_valsartan_hfref"
    assert manifest["effect_candidates_rows"] == 1
    assert manifest["strict_comparable_candidates"] == 1
