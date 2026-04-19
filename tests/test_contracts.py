from __future__ import annotations

import pytest

from trial_transportability_atlas.contracts import (
    ATLAS_CONTEXT_COLUMNS,
    ATLAS_CONTEXT_CONTRACT,
    DatasetContract,
    SYNTHESIS_OUTPUT_COLUMNS,
    SYNTHESIS_OUTPUT_CONTRACT,
    SchemaValidationError,
    TRIAL_BRIDGE_COLUMNS,
    TRIAL_BRIDGE_CONTRACT,
)


def test_atlas_context_contract_accepts_exact_required_columns() -> None:
    ATLAS_CONTEXT_CONTRACT.validate_columns(ATLAS_CONTEXT_COLUMNS)


def test_trial_bridge_contract_accepts_extra_columns() -> None:
    TRIAL_BRIDGE_CONTRACT.validate_columns(TRIAL_BRIDGE_COLUMNS + ("extra_col",))


def test_synthesis_output_contract_accepts_exact_required_columns() -> None:
    SYNTHESIS_OUTPUT_CONTRACT.validate_columns(SYNTHESIS_OUTPUT_COLUMNS)


def test_contract_validation_reports_missing_columns() -> None:
    contract = DatasetContract(name="toy_contract", required_columns=("a", "b", "c"))

    with pytest.raises(SchemaValidationError) as excinfo:
        contract.validate_columns(("a", "c"))

    assert "toy_contract" in str(excinfo.value)
    assert "b" in str(excinfo.value)


def test_contract_constants_cover_expected_shared_fields() -> None:
    assert "iso3" in ATLAS_CONTEXT_COLUMNS
    assert "source_version" in ATLAS_CONTEXT_COLUMNS
    assert "nct_id" in TRIAL_BRIDGE_COLUMNS
    assert "source_table" in TRIAL_BRIDGE_COLUMNS
    assert "transportability_score" in SYNTHESIS_OUTPUT_COLUMNS
    assert "source_manifest_id" in SYNTHESIS_OUTPUT_COLUMNS
