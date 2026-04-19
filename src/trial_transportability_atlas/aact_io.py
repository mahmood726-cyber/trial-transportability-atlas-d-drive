"""AACT snapshot IO and schema validation helpers."""
from __future__ import annotations

from csv import DictReader
from pathlib import Path
from typing import Iterable


class AactSchemaError(ValueError):
    """Raised when the local AACT snapshot does not match expected headers."""


REQUIRED_AACT_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "studies": (
        "nct_id",
        "brief_title",
        "official_title",
        "overall_status",
        "completion_date",
        "completion_date_type",
    ),
    "id_information": ("id", "nct_id", "id_value", "id_type"),
    "conditions": ("id", "nct_id", "name", "downcase_name"),
    "interventions": ("id", "nct_id", "intervention_type", "name", "description"),
    "countries": ("id", "nct_id", "name", "removed"),
    "facilities": ("id", "nct_id", "country"),
    "outcomes": (
        "id",
        "nct_id",
        "outcome_type",
        "title",
        "description",
        "time_frame",
        "population",
        "units",
        "param_type",
    ),
    "outcome_analyses": (
        "id",
        "nct_id",
        "outcome_id",
        "param_type",
        "param_value",
        "dispersion_type",
        "dispersion_value",
        "p_value",
        "ci_percent",
        "ci_lower_limit",
        "ci_upper_limit",
        "method",
        "estimate_description",
        "groups_description",
    ),
    "outcome_measurements": (
        "id",
        "nct_id",
        "outcome_id",
        "result_group_id",
        "ctgov_group_code",
        "classification",
        "category",
        "title",
        "units",
        "param_type",
        "param_value",
        "param_value_num",
        "dispersion_type",
        "dispersion_value",
        "dispersion_value_num",
    ),
    "reported_events": (
        "id",
        "nct_id",
        "result_group_id",
        "ctgov_group_code",
        "time_frame",
        "event_type",
        "subjects_affected",
        "subjects_at_risk",
        "description",
        "event_count",
        "organ_system",
        "adverse_event_term",
    ),
    "eligibilities": (
        "id",
        "nct_id",
        "gender",
        "minimum_age",
        "maximum_age",
        "population",
        "criteria",
        "adult",
        "child",
        "older_adult",
    ),
    "brief_summaries": ("id", "nct_id", "description"),
    "design_outcomes": (
        "id",
        "nct_id",
        "outcome_type",
        "measure",
        "time_frame",
        "population",
        "description",
    ),
}


def aact_table_path(snapshot_dir: Path, table_name: str) -> Path:
    """Return the local text-export path for one AACT table."""

    return snapshot_dir / f"{table_name}.txt"


def read_aact_header(snapshot_dir: Path, table_name: str) -> tuple[str, ...]:
    """Read the header for one pipe-delimited AACT table."""

    table_path = aact_table_path(snapshot_dir, table_name)
    if not table_path.exists():
        raise FileNotFoundError(f"Missing AACT table: {table_path}")
    with table_path.open("r", encoding="utf-8", newline="") as handle:
        first_line = handle.readline().strip()
    if not first_line:
        raise AactSchemaError(f"AACT table has an empty header: {table_path}")
    return tuple(first_line.split("|"))


def validate_aact_snapshot(
    snapshot_dir: Path,
    *,
    required_tables: Iterable[str] | None = None,
) -> dict[str, tuple[str, ...]]:
    """Validate the required AACT tables against the expected header contract."""

    tables = tuple(required_tables or REQUIRED_AACT_TABLE_COLUMNS.keys())
    resolved: dict[str, tuple[str, ...]] = {}
    missing_messages: list[str] = []

    for table_name in tables:
        if table_name not in REQUIRED_AACT_TABLE_COLUMNS:
            raise AactSchemaError(f"Unknown AACT table contract: {table_name}")
        columns = read_aact_header(snapshot_dir, table_name)
        required_columns = REQUIRED_AACT_TABLE_COLUMNS[table_name]
        missing = [column for column in required_columns if column not in columns]
        if missing:
            missing_messages.append(
                f"{table_name} missing columns: {', '.join(missing)}"
            )
        resolved[table_name] = columns

    if missing_messages:
        raise AactSchemaError("; ".join(missing_messages))
    return resolved


def iter_aact_rows(
    snapshot_dir: Path,
    table_name: str,
    *,
    nct_ids: set[str] | None = None,
) -> Iterable[dict[str, str]]:
    """Yield AACT rows from one table with an optional `nct_id` filter."""

    table_path = aact_table_path(snapshot_dir, table_name)
    with table_path.open("r", encoding="utf-8", newline="") as handle:
        reader = DictReader(handle, delimiter="|")
        for row in reader:
            nct_id = (row.get("nct_id") or "").strip()
            if nct_ids is not None and nct_id not in nct_ids:
                continue
            yield {key: value if value is not None else "" for key, value in row.items()}
