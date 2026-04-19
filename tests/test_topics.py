from __future__ import annotations

from pathlib import Path

import pytest

from trial_transportability_atlas.aact_bridge import validate_aact_snapshot
from trial_transportability_atlas.project_paths import discover_aact_snapshot
from trial_transportability_atlas.topics import (
    TopicConfigError,
    load_frozen_topic,
    normalize_text,
    select_topic_nct_ids,
)

from tests.test_aact_bridge import build_minimal_snapshot, write_table


def test_load_frozen_topic_reads_repo_phase1_note() -> None:
    topic = load_frozen_topic()

    assert topic.status == "accepted"
    assert topic.name == "sacubitril/valsartan in HFrEF"
    assert topic.slug == "sacubitril_valsartan_in_hfref"
    assert "sacubitril" in topic.inclusion_terms
    assert "LCZ696" in topic.inclusion_terms
    assert "studies" in topic.expected_source_tables
    assert any("IHME population" in join for join in topic.intended_joins)


def test_load_frozen_topic_fails_closed_on_unaccepted_status(tmp_path: Path) -> None:
    topic_path = tmp_path / "phase1-topic.md"
    topic_path.write_text(
        "\n".join(
            [
                "# Phase 1 Topic Selection",
                "",
                "Status: undecided",
                "",
                "Accepted topic: toy topic",
                "",
                "Exact inclusion terms:",
                "- `toy`",
                "",
                "Exact exclusion terms:",
                "- `not-toy`",
                "",
                "Expected AACT source tables:",
                "- `studies`",
                "",
                "Intended IHME / WHO / WB joins:",
                "- IHME population",
                "",
                "Known failure modes:",
                "- no real source data",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(TopicConfigError) as excinfo:
        load_frozen_topic(topic_path)

    assert "accepted" in str(excinfo.value)


def test_load_frozen_topic_requires_expected_sections(tmp_path: Path) -> None:
    topic_path = tmp_path / "phase1-topic.md"
    topic_path.write_text(
        "\n".join(
            [
                "# Phase 1 Topic Selection",
                "",
                "Status: accepted",
                "",
                "Accepted topic: toy topic",
                "",
                "Exact inclusion terms:",
                "- `toy`",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(TopicConfigError) as excinfo:
        load_frozen_topic(topic_path)

    assert "Missing required section" in str(excinfo.value)


def test_normalize_text_flattens_common_variants() -> None:
    assert normalize_text("Sacubitril / Valsartan Oral Tablet [Entresto]") == (
        "sacubitril valsartan oral tablet entresto"
    )


def test_select_topic_nct_ids_uses_strict_hfref_rules(tmp_path: Path) -> None:
    snapshot_dir = build_minimal_snapshot(tmp_path / "snapshot")
    write_table(
        snapshot_dir,
        "interventions",
        [
            "id|nct_id|intervention_type|name|description",
            "1|NCTHFR|Drug|Sacubitril/Valsartan|Active drug",
            "2|NCTHFP|Drug|Sacubitril/Valsartan|Active drug",
            "3|NCTPHT|Drug|Entresto|Active drug",
        ],
    )
    write_table(
        snapshot_dir,
        "conditions",
        [
            "id|nct_id|name|downcase_name",
            "1|NCTHFR|Heart Failure, Reduced Ejection Fraction|heart failure, reduced ejection fraction",
            "2|NCTHFP|Heart Failure With Preserved Ejection Fraction|heart failure with preserved ejection fraction",
            "3|NCTPHT|Pulmonary Hypertension|pulmonary hypertension",
        ],
    )
    write_table(
        snapshot_dir,
        "brief_summaries",
        [
            "id|nct_id|description",
            "1|NCTHFR|Patients with reduced ejection fraction.",
            "2|NCTHFP|HFpEF population.",
            "3|NCTPHT|Pulmonary hypertension only.",
        ],
    )

    selected = select_topic_nct_ids(snapshot_dir)

    assert selected == {"NCTHFR"}


def test_real_snapshot_selector_includes_hfref_and_excludes_known_off_topic_trials() -> None:
    snapshot_dir = discover_aact_snapshot()
    validate_aact_snapshot(snapshot_dir)

    selected = select_topic_nct_ids(snapshot_dir)

    assert "NCT02970669" in selected
    assert "NCT03988634" not in selected
    assert "NCT04753112" not in selected
    assert "NCT04637152" not in selected
    assert "NCT03938389" not in selected
