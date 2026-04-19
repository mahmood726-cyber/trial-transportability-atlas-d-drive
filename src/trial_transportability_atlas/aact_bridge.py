"""Narrow AACT bridge helpers for phase-1 atlas extraction."""
from __future__ import annotations

from csv import DictReader
from dataclasses import asdict, dataclass
import io
from pathlib import Path
import re
import shutil
import subprocess
from typing import Iterable


class AactSchemaError(ValueError):
    """Raised when the local AACT snapshot does not match expected headers."""


REQUIRED_AACT_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "id_information": ("id", "nct_id", "id_value", "id_type"),
    "conditions": ("id", "nct_id", "name", "downcase_name"),
    "interventions": ("id", "nct_id", "intervention_type", "name"),
    "countries": ("id", "nct_id", "name", "removed"),
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
    "facilities": ("id", "nct_id", "country"),
    "brief_summaries": ("id", "nct_id", "description"),
    "calculated_values": (
        "id",
        "nct_id",
        "registered_in_calendar_year",
        "actual_duration",
        "were_results_reported",
    ),
}


@dataclass(frozen=True)
class TrialCountryYearRecord:
    nct_id: str
    country_name: str
    iso3: str | None
    year: int | None
    completion_date: str | None
    country_source_table: str
    year_source_table: str | None
    provenance: str


@dataclass(frozen=True)
class TrialOutcomeRecord:
    nct_id: str
    source_table: str
    record_type: str
    outcome_id: str | None
    outcome_type: str | None
    outcome_name: str
    analysis_population: str | None
    time_frame: str | None
    unit: str | None
    result_group_id: str | None
    ctgov_group_code: str | None
    classification: str | None
    category: str | None
    param_type: str | None
    value_text: str | None
    value_num: float | None
    dispersion_type: str | None
    dispersion_value: str | None
    event_type: str | None
    subjects_affected: int | None
    subjects_at_risk: int | None
    event_count: int | None
    organ_system: str | None
    adverse_event_term: str | None
    provenance: str


def aact_table_path(snapshot_dir: Path, table_name: str) -> Path:
    return snapshot_dir / f"{table_name}.txt"


def read_aact_header(snapshot_dir: Path, table_name: str) -> tuple[str, ...]:
    table_path = aact_table_path(snapshot_dir, table_name)
    if not table_path.exists():
        raise FileNotFoundError(f"Missing AACT table: {table_path}")
    with table_path.open("r", encoding="utf-8", newline="") as handle:
        first_line = handle.readline().strip()
    if not first_line:
        raise AactSchemaError(f"AACT table has an empty header: {table_path}")
    return tuple(first_line.split("|"))


def validate_aact_snapshot(snapshot_dir: Path) -> dict[str, tuple[str, ...]]:
    resolved: dict[str, tuple[str, ...]] = {}
    missing_messages: list[str] = []
    for table_name, required_columns in REQUIRED_AACT_TABLE_COLUMNS.items():
        columns = read_aact_header(snapshot_dir, table_name)
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
    table_path = aact_table_path(snapshot_dir, table_name)
    if nct_ids:
        rg_path = shutil.which("rg")
        if rg_path:
            pattern = "|".join(re.escape(nct_id) for nct_id in sorted(nct_ids))
            regex = rf"^[^|]*\|(?:{pattern})\|"
            completed = subprocess.run(
                [rg_path, "--no-line-number", "-N", "-e", regex, str(table_path)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            if completed.returncode not in (0, 1):
                raise RuntimeError(
                    f"rg failed for {table_name}: {completed.stderr.strip()}"
                )
            if completed.returncode == 0 and completed.stdout:
                header = "|".join(read_aact_header(snapshot_dir, table_name))
                buffer = io.StringIO(header + "\n" + completed.stdout)
                yield from DictReader(buffer, delimiter="|")
                return
            return

    with table_path.open("r", encoding="utf-8", newline="") as handle:
        reader = DictReader(handle, delimiter="|")
        for row in reader:
            nct_id = (row.get("nct_id") or "").strip()
            if nct_ids is not None and nct_id not in nct_ids:
                continue
            yield row


def normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def parse_int(value: str | None) -> int | None:
    normalized = normalize_optional(value)
    if normalized is None:
        return None
    return int(float(normalized))


def parse_float(value: str | None) -> float | None:
    normalized = normalize_optional(value)
    if normalized is None:
        return None
    return float(normalized)


def parse_bool_flag(value: str | None) -> bool:
    normalized = (normalize_optional(value) or "").lower()
    return normalized in {"t", "true", "1", "yes", "y"}


def extract_trial_country_year(
    snapshot_dir: Path,
    *,
    nct_ids: Iterable[str] | None = None,
) -> list[dict[str, object]]:
    """Build trial-country-year records from the local AACT extract.

    The local 2026-04-12 snapshot exposes `registered_in_calendar_year` in
    `calculated_values.txt`, but not an exact study completion-date field in
    the visible study-level tables. This extractor therefore leaves
    `completion_date` null and records the year provenance explicitly.
    """

    validate_aact_snapshot(snapshot_dir)
    nct_filter = set(nct_ids) if nct_ids is not None else None

    years_by_nct: dict[str, int | None] = {}
    for row in iter_aact_rows(snapshot_dir, "calculated_values", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        years_by_nct[nct_id] = parse_int(row.get("registered_in_calendar_year"))

    countries_by_nct: dict[str, set[str]] = {}
    country_source_by_nct: dict[str, str] = {}
    for row in iter_aact_rows(snapshot_dir, "countries", nct_ids=nct_filter):
        if parse_bool_flag(row.get("removed")):
            continue
        nct_id = row["nct_id"].strip()
        country_name = normalize_optional(row.get("name"))
        if country_name is None:
            continue
        countries_by_nct.setdefault(nct_id, set()).add(country_name)
        country_source_by_nct[nct_id] = "countries"

    for row in iter_aact_rows(snapshot_dir, "facilities", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        if nct_id in countries_by_nct and countries_by_nct[nct_id]:
            continue
        country_name = normalize_optional(row.get("country"))
        if country_name is None:
            continue
        countries_by_nct.setdefault(nct_id, set()).add(country_name)
        country_source_by_nct[nct_id] = "facilities"

    records: list[dict[str, object]] = []
    for nct_id in sorted(countries_by_nct):
        year = years_by_nct.get(nct_id)
        source_table = country_source_by_nct.get(nct_id, "unknown")
        for country_name in sorted(countries_by_nct[nct_id]):
            records.append(
                asdict(
                    TrialCountryYearRecord(
                        nct_id=nct_id,
                        country_name=country_name,
                        iso3=None,
                        year=year,
                        completion_date=None,
                        country_source_table=source_table,
                        year_source_table="calculated_values" if year is not None else None,
                        provenance=f"{source_table}+calculated_values",
                    )
                )
            )
    return records


def extract_trial_outcomes(
    snapshot_dir: Path,
    *,
    nct_ids: Iterable[str] | None = None,
) -> list[dict[str, object]]:
    validate_aact_snapshot(snapshot_dir)
    nct_filter = set(nct_ids) if nct_ids is not None else None

    outcome_lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in iter_aact_rows(snapshot_dir, "outcomes", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        outcome_id = row["id"].strip()
        outcome_lookup[(nct_id, outcome_id)] = row

    records: list[dict[str, object]] = []
    for row in iter_aact_rows(snapshot_dir, "outcome_measurements", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        outcome_id = row["outcome_id"].strip()
        meta = outcome_lookup.get((nct_id, outcome_id), {})
        records.append(
            asdict(
                TrialOutcomeRecord(
                    nct_id=nct_id,
                    source_table="outcome_measurements",
                    record_type="measurement",
                    outcome_id=outcome_id or None,
                    outcome_type=normalize_optional(meta.get("outcome_type")),
                    outcome_name=(
                        normalize_optional(row.get("title"))
                        or normalize_optional(meta.get("title"))
                        or "Unnamed outcome"
                    ),
                    analysis_population=normalize_optional(meta.get("population")),
                    time_frame=normalize_optional(meta.get("time_frame")),
                    unit=normalize_optional(row.get("units"))
                    or normalize_optional(meta.get("units")),
                    result_group_id=normalize_optional(row.get("result_group_id")),
                    ctgov_group_code=normalize_optional(row.get("ctgov_group_code")),
                    classification=normalize_optional(row.get("classification")),
                    category=normalize_optional(row.get("category")),
                    param_type=normalize_optional(row.get("param_type"))
                    or normalize_optional(meta.get("param_type")),
                    value_text=normalize_optional(row.get("param_value")),
                    value_num=parse_float(row.get("param_value_num")),
                    dispersion_type=normalize_optional(row.get("dispersion_type")),
                    dispersion_value=normalize_optional(row.get("dispersion_value")),
                    event_type=None,
                    subjects_affected=None,
                    subjects_at_risk=None,
                    event_count=None,
                    organ_system=None,
                    adverse_event_term=None,
                    provenance="outcomes+outcome_measurements",
                )
            )
        )

    for row in iter_aact_rows(snapshot_dir, "reported_events", nct_ids=nct_filter):
        adverse_event_term = normalize_optional(row.get("adverse_event_term"))
        organ_system = normalize_optional(row.get("organ_system"))
        records.append(
            asdict(
                TrialOutcomeRecord(
                    nct_id=row["nct_id"].strip(),
                    source_table="reported_events",
                    record_type="reported_event",
                    outcome_id=None,
                    outcome_type=None,
                    outcome_name=adverse_event_term or organ_system or "Reported event",
                    analysis_population=None,
                    time_frame=normalize_optional(row.get("time_frame")),
                    unit="participants",
                    result_group_id=normalize_optional(row.get("result_group_id")),
                    ctgov_group_code=normalize_optional(row.get("ctgov_group_code")),
                    classification=None,
                    category=None,
                    param_type=None,
                    value_text=normalize_optional(row.get("event_count")),
                    value_num=parse_float(row.get("event_count")),
                    dispersion_type=None,
                    dispersion_value=None,
                    event_type=normalize_optional(row.get("event_type")),
                    subjects_affected=parse_int(row.get("subjects_affected")),
                    subjects_at_risk=parse_int(row.get("subjects_at_risk")),
                    event_count=parse_int(row.get("event_count")),
                    organ_system=organ_system,
                    adverse_event_term=adverse_event_term,
                    provenance="reported_events",
                )
            )
        )

    return records
