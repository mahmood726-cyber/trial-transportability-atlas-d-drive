from __future__ import annotations

from pathlib import Path

import pytest

from trial_transportability_atlas.project_paths import (
    MissingRequiredPathError,
    discover_external_paths,
    discover_output_root,
    discover_topic_output_dir,
)


def test_discover_external_paths_prefers_existing_candidate_root(tmp_path: Path) -> None:
    c_root = tmp_path / "c-root"
    d_root = tmp_path / "d-root"

    (d_root / "AACT-storage" / "AACT" / "2026-04-12").mkdir(parents=True)
    (d_root / "Projects" / "ihme-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "who-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "wb-data-lakehouse").mkdir(parents=True)

    paths = discover_external_paths(candidate_roots=(c_root, d_root), env={})

    assert paths.aact_snapshot == d_root / "AACT-storage" / "AACT" / "2026-04-12"
    assert paths.ihme_repo == d_root / "Projects" / "ihme-data-lakehouse"
    assert paths.who_repo == d_root / "Projects" / "who-data-lakehouse"
    assert paths.wb_repo == d_root / "Projects" / "wb-data-lakehouse"


def test_discover_external_paths_uses_c_drive_aact_fallback(tmp_path: Path) -> None:
    c_root = tmp_path / "c-root"
    (c_root / "Users" / "user" / "AACT" / "2026-04-12").mkdir(parents=True)
    (c_root / "Projects" / "ihme-data-lakehouse").mkdir(parents=True)
    (c_root / "Projects" / "who-data-lakehouse").mkdir(parents=True)
    (c_root / "Projects" / "wb-data-lakehouse").mkdir(parents=True)

    paths = discover_external_paths(candidate_roots=(c_root,), env={})

    assert paths.aact_snapshot == c_root / "Users" / "user" / "AACT" / "2026-04-12"
    assert paths.ihme_repo == c_root / "Projects" / "ihme-data-lakehouse"


def test_discover_external_paths_prefers_env_override(tmp_path: Path) -> None:
    override = tmp_path / "override-aact"
    override.mkdir(parents=True)

    d_root = tmp_path / "d-root"
    (d_root / "Projects" / "ihme-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "who-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "wb-data-lakehouse").mkdir(parents=True)
    (d_root / "AACT-storage" / "AACT" / "2026-04-12").mkdir(parents=True)

    paths = discover_external_paths(
        candidate_roots=(d_root,),
        env={"TTA_AACT_PATH": str(override)},
    )

    assert paths.aact_snapshot == override


def test_discover_external_paths_fails_closed_on_missing_dependency(
    tmp_path: Path,
) -> None:
    d_root = tmp_path / "d-root"
    (d_root / "AACT-storage" / "AACT" / "2026-04-12").mkdir(parents=True)
    (d_root / "Projects" / "ihme-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "who-data-lakehouse").mkdir(parents=True)

    with pytest.raises(MissingRequiredPathError) as excinfo:
        discover_external_paths(candidate_roots=(d_root,), env={})

    assert "wb_repo" in str(excinfo.value)


def test_discover_external_paths_rejects_broken_env_override(tmp_path: Path) -> None:
    d_root = tmp_path / "d-root"
    (d_root / "AACT-storage" / "AACT" / "2026-04-12").mkdir(parents=True)
    (d_root / "Projects" / "ihme-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "who-data-lakehouse").mkdir(parents=True)
    (d_root / "Projects" / "wb-data-lakehouse").mkdir(parents=True)

    with pytest.raises(MissingRequiredPathError) as excinfo:
        discover_external_paths(
            candidate_roots=(d_root,),
            env={"TTA_AACT_PATH": str(tmp_path / "missing-aact")},
        )

    assert "TTA_AACT_PATH" in str(excinfo.value)


def test_discover_output_root_defaults_to_repo_outputs() -> None:
    resolved = discover_output_root(env={})

    assert resolved.name == "outputs"
    assert resolved.parent.name == "trial-transportability-atlas"


def test_discover_output_root_prefers_env_override(tmp_path: Path) -> None:
    override = tmp_path / "custom-outputs"

    assert discover_output_root(env={"TTA_OUTPUT_ROOT": str(override)}) == override


def test_discover_topic_output_dir_uses_output_root_override(tmp_path: Path) -> None:
    override = tmp_path / "custom-outputs"

    resolved = discover_topic_output_dir(
        "sglt2_inhibitors",
        env={"TTA_OUTPUT_ROOT": str(override)},
    )

    assert resolved == override / "sglt2_inhibitors"
