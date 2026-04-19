"""Path resolution helpers for local source dependencies."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Iterable, Mapping


DEFAULT_CANDIDATE_ROOTS = (Path("D:/"), Path("C:/"))


class MissingRequiredPathError(FileNotFoundError):
    """Raised when one or more required local source paths are unavailable."""


@dataclass(frozen=True)
class ExternalProjectPaths:
    """Resolved local paths for the phase-1 source dependencies."""

    aact_snapshot: Path
    ihme_repo: Path
    who_repo: Path
    wb_repo: Path


@dataclass(frozen=True)
class PathSpec:
    """Resolution spec for one required external path."""

    label: str
    env_var: str
    candidate_relatives: tuple[Path, ...]


PATH_SPECS = (
    PathSpec(
        label="aact_snapshot",
        env_var="TTA_AACT_PATH",
        candidate_relatives=(
            Path("AACT-storage/AACT/2026-04-12"),
            Path("Users/user/AACT/2026-04-12"),
        ),
    ),
    PathSpec(
        label="ihme_repo",
        env_var="TTA_IHME_PATH",
        candidate_relatives=(Path("Projects/ihme-data-lakehouse"),),
    ),
    PathSpec(
        label="who_repo",
        env_var="TTA_WHO_PATH",
        candidate_relatives=(Path("Projects/who-data-lakehouse"),),
    ),
    PathSpec(
        label="wb_repo",
        env_var="TTA_WB_PATH",
        candidate_relatives=(Path("Projects/wb-data-lakehouse"),),
    ),
)


def _normalize_roots(candidate_roots: Iterable[Path] | None) -> tuple[Path, ...]:
    roots = candidate_roots or DEFAULT_CANDIDATE_ROOTS
    return tuple(Path(root) for root in roots)


def _existing_path(path: Path) -> Path | None:
    return path if path.exists() else None


def resolve_path_spec(
    spec: PathSpec,
    *,
    candidate_roots: Iterable[Path] | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Resolve one required path from env overrides or candidate roots."""

    env_map = env if env is not None else os.environ
    override = env_map.get(spec.env_var)
    if override:
        override_path = Path(override)
        existing = _existing_path(override_path)
        if existing is None:
            raise MissingRequiredPathError(
                f"{spec.env_var} points to a missing path: {override_path}"
            )
        return existing

    attempted: list[str] = []
    for root in _normalize_roots(candidate_roots):
        for relative in spec.candidate_relatives:
            candidate = root / relative
            attempted.append(str(candidate))
            existing = _existing_path(candidate)
            if existing is not None:
                return existing

    raise MissingRequiredPathError(
        f"Unable to resolve required path '{spec.label}'. Tried: {', '.join(attempted)}"
    )


def discover_external_paths(
    *,
    candidate_roots: Iterable[Path] | None = None,
    env: Mapping[str, str] | None = None,
) -> ExternalProjectPaths:
    """Resolve all required external source paths for the atlas repo."""

    resolved = {
        spec.label: resolve_path_spec(
            spec,
            candidate_roots=candidate_roots,
            env=env,
        )
        for spec in PATH_SPECS
    }
    return ExternalProjectPaths(**resolved)


def discover_aact_snapshot(
    *,
    candidate_roots: Iterable[Path] | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Resolve only the local AACT snapshot path."""

    return resolve_path_spec(
        PATH_SPECS[0],
        candidate_roots=candidate_roots,
        env=env,
    )
