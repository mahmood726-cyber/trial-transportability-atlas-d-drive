"""Shared schema contracts for atlas datasets."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


class SchemaValidationError(ValueError):
    """Raised when required columns are absent from a dataset."""


ATLAS_CONTEXT_COLUMNS = (
    "iso3",
    "year",
    "measure",
    "metric",
    "value",
    "source",
    "source_file",
    "source_version",
    "location_name",
    "provenance",
)

TRIAL_BRIDGE_COLUMNS = (
    "nct_id",
    "study_title",
    "completion_date",
    "completion_year",
    "condition_name",
    "intervention_name",
    "country_name",
    "iso3",
    "outcome_name",
    "outcome_type",
    "analysis_type",
    "unit",
    "result_value",
    "analysis_population",
    "source_table",
    "source_file",
    "source_version",
)

SYNTHESIS_OUTPUT_COLUMNS = (
    "iso3",
    "year",
    "intervention_name",
    "condition_name",
    "effect_status",
    "effect_measure",
    "effect_value",
    "effect_precision",
    "country_coverage_score",
    "context_distance",
    "eligibility_support_score",
    "reporting_completeness_score",
    "transportability_score",
    "priority_gap_score",
    "source_manifest_id",
)


@dataclass(frozen=True)
class DatasetContract:
    """Required-column contract for one dataset surface."""

    name: str
    required_columns: tuple[str, ...]

    def validate_columns(self, columns: Iterable[str]) -> None:
        seen = set(columns)
        missing = tuple(column for column in self.required_columns if column not in seen)
        if missing:
            missing_str = ", ".join(missing)
            raise SchemaValidationError(
                f"{self.name} missing required columns: {missing_str}"
            )


ATLAS_CONTEXT_CONTRACT = DatasetContract(
    name="atlas_context",
    required_columns=ATLAS_CONTEXT_COLUMNS,
)

TRIAL_BRIDGE_CONTRACT = DatasetContract(
    name="trial_bridge",
    required_columns=TRIAL_BRIDGE_COLUMNS,
)

SYNTHESIS_OUTPUT_CONTRACT = DatasetContract(
    name="synthesis_output",
    required_columns=SYNTHESIS_OUTPUT_COLUMNS,
)

ALL_DATASET_CONTRACTS = (
    ATLAS_CONTEXT_CONTRACT,
    TRIAL_BRIDGE_CONTRACT,
    SYNTHESIS_OUTPUT_CONTRACT,
)
