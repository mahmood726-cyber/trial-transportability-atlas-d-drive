from __future__ import annotations

from pathlib import Path

from trial_transportability_atlas.aact_bridge import (
    extract_trial_country_year,
    extract_trial_outcomes,
)


def write_table(snapshot_dir: Path, table_name: str, lines: list[str]) -> None:
    (snapshot_dir / f"{table_name}.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def build_minimal_snapshot(snapshot_dir: Path) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    write_table(
        snapshot_dir,
        "id_information",
        [
            "id|nct_id|id_source|id_value|id_type|id_type_description|id_link",
            "1|NCT0001|ctgov|NCT0001|org_study_id||",
        ],
    )
    write_table(
        snapshot_dir,
        "conditions",
        [
            "id|nct_id|name|downcase_name",
            "1|NCT0001|Heart Failure|heart failure",
        ],
    )
    write_table(
        snapshot_dir,
        "interventions",
        [
            "id|nct_id|intervention_type|name|description",
            "10|NCT0001|Drug|Empagliflozin|Study drug",
        ],
    )
    write_table(
        snapshot_dir,
        "countries",
        [
            "id|nct_id|name|removed",
            "20|NCT0001|United Kingdom|f",
            "21|NCT0002|France|t",
        ],
    )
    write_table(
        snapshot_dir,
        "facilities",
        [
            "id|nct_id|status|name|city|state|zip|country|latitude|longitude",
            "30|NCT0002||Hospital A|Paris|||France||",
        ],
    )
    write_table(
        snapshot_dir,
        "brief_summaries",
        [
            "id|nct_id|description",
            "40|NCT0001|Summary text",
        ],
    )
    write_table(
        snapshot_dir,
        "calculated_values",
        [
            "id|nct_id|number_of_facilities|number_of_nsae_subjects|number_of_sae_subjects|registered_in_calendar_year|nlm_download_date|actual_duration|were_results_reported|months_to_report_results|has_us_facility|has_single_facility|minimum_age_num|maximum_age_num|minimum_age_unit|maximum_age_unit|number_of_primary_outcomes_to_measure|number_of_secondary_outcomes_to_measure|number_of_other_outcomes_to_measure",
            "50|NCT0001||||2020||18|t||||||||||",
            "51|NCT0002||||2019||12|f||||||||||",
        ],
    )
    write_table(
        snapshot_dir,
        "outcomes",
        [
            "id|nct_id|outcome_type|title|description|time_frame|population|anticipated_posting_date|anticipated_posting_month_year|units|units_analyzed|dispersion_type|param_type",
            "60|NCT0001|PRIMARY|All-cause mortality|Deaths from any cause|12 months|all participants|||Participants|||COUNT_OF_PARTICIPANTS",
        ],
    )
    write_table(
        snapshot_dir,
        "outcome_measurements",
        [
            "id|nct_id|outcome_id|result_group_id|ctgov_group_code|classification|category|title|description|units|param_type|param_value|param_value_num|dispersion_type|dispersion_value|dispersion_value_num|dispersion_lower_limit|dispersion_upper_limit|explanation_of_na|dispersion_upper_limit_raw|dispersion_lower_limit_raw",
            "70|NCT0001|60|500|OG001|Deaths||All-cause mortality||Participants|COUNT_OF_PARTICIPANTS|5|5||||||||",
        ],
    )
    write_table(
        snapshot_dir,
        "reported_events",
        [
            "id|nct_id|result_group_id|ctgov_group_code|time_frame|event_type|default_vocab|default_assessment|subjects_affected|subjects_at_risk|description|event_count|organ_system|adverse_event_term|frequency_threshold|vocab|assessment",
            "80|NCT0001|500|OG001|12 months|serious|||2|100|event description|4|Cardiac disorders|Heart failure worsening|||",
        ],
    )
    return snapshot_dir


def test_extract_trial_country_year_uses_active_countries_and_facility_fallback(
    tmp_path: Path,
) -> None:
    snapshot_dir = build_minimal_snapshot(tmp_path / "snapshot")

    records = extract_trial_country_year(snapshot_dir)

    assert records == [
        {
            "nct_id": "NCT0001",
            "country_name": "United Kingdom",
            "iso3": None,
            "year": 2020,
            "completion_date": None,
            "country_source_table": "countries",
            "year_source_table": "calculated_values",
            "provenance": "countries+calculated_values",
        },
        {
            "nct_id": "NCT0002",
            "country_name": "France",
            "iso3": None,
            "year": 2019,
            "completion_date": None,
            "country_source_table": "facilities",
            "year_source_table": "calculated_values",
            "provenance": "facilities+calculated_values",
        },
    ]


def test_extract_trial_outcomes_combines_measurements_and_reported_events(
    tmp_path: Path,
) -> None:
    snapshot_dir = build_minimal_snapshot(tmp_path / "snapshot")

    records = extract_trial_outcomes(snapshot_dir, nct_ids={"NCT0001"})

    assert len(records) == 2
    measurement = next(record for record in records if record["record_type"] == "measurement")
    reported_event = next(
        record for record in records if record["record_type"] == "reported_event"
    )

    assert measurement["outcome_name"] == "All-cause mortality"
    assert measurement["outcome_type"] == "PRIMARY"
    assert measurement["value_num"] == 5.0
    assert measurement["provenance"] == "outcomes+outcome_measurements"

    assert reported_event["outcome_name"] == "Heart failure worsening"
    assert reported_event["event_type"] == "serious"
    assert reported_event["subjects_at_risk"] == 100
    assert reported_event["event_count"] == 4
