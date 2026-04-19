"""Minimal phase-1 context source adapters."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.contracts import ATLAS_CONTEXT_CONTRACT
from trial_transportability_atlas.country_iso3 import country_name_to_iso3


class SourceAdapterError(ValueError):
    """Raised when a context source cannot be normalized safely."""


@dataclass(frozen=True)
class IhmeDatasetPaths:
    burden: Path
    population: Path
    sdi: Path


def resolve_ihme_dataset_paths(repo_root: Path) -> IhmeDatasetPaths:
    """Resolve the fixed phase-1 IHME dataset paths."""

    datasets_dir = repo_root / "datasets"
    burden = datasets_dir / "gbd2023_all_burden_204countries_1990_2023.parquet"
    population = datasets_dir / "gbd2023_population_204countries_1990_2023.parquet"
    sdi = datasets_dir / "gbd2021_sdi_1950_2021.parquet"
    missing = [str(path) for path in (burden, population, sdi) if not path.exists()]
    if missing:
        raise SourceAdapterError(f"Missing IHME datasets: {', '.join(missing)}")
    return IhmeDatasetPaths(burden=burden, population=population, sdi=sdi)


def _find_column(frame: pd.DataFrame, names: tuple[str, ...], *, required: bool = True) -> str | None:
    lower_map = {str(column).casefold(): str(column) for column in frame.columns}
    for name in names:
        if name.casefold() in lower_map:
            return lower_map[name.casefold()]
    if required:
        raise SourceAdapterError(
            f"Missing required column. Tried: {', '.join(names)}; available: {', '.join(map(str, frame.columns))}"
        )
    return None


def _optional_text(frame: pd.DataFrame, column_name: str | None) -> pd.Series:
    if column_name is None:
        return pd.Series([None] * len(frame), dtype="object")
    return frame[column_name].astype("string").where(frame[column_name].notna(), None).astype("object")


def _normalize_context_frame(
    frame: pd.DataFrame,
    *,
    source_name: str,
    source_file: Path,
    measure_default: str,
    metric_default: str,
    value_candidates: tuple[str, ...],
    allow_unresolved_locations: bool,
) -> pd.DataFrame:
    location_col = _find_column(
        frame,
        (
            "location",
            "location_name",
            "country",
            "country_name",
            "iso3c",
            "SpatialDim",
            "spatial_dim",
            "code",
        ),
    )
    year_col = _find_column(frame, ("year", "year_id", "TimeDim", "time_dim"))
    value_col = _find_column(frame, value_candidates)
    sex_col = _find_column(frame, ("sex", "sex_name"), required=False)
    age_col = _find_column(frame, ("age_group", "age_name"), required=False)
    measure_col = _find_column(frame, ("measure", "measure_name"), required=False)
    metric_col = _find_column(frame, ("metric", "metric_name"), required=False)
    cause_col = _find_column(frame, ("cause", "cause_name"), required=False)
    location_id_col = _find_column(frame, ("location_id",), required=False)
    upper_col = _find_column(frame, ("upper", "sdi_upper"), required=False)
    lower_col = _find_column(frame, ("lower", "sdi_lower"), required=False)

    location_names = frame[location_col].astype("string").astype("object")
    iso3 = location_names.map(country_name_to_iso3)

    normalized = pd.DataFrame(
        {
            "iso3": iso3,
            "year": pd.to_numeric(frame[year_col], errors="coerce").astype("Int64"),
            "sex": _optional_text(frame, sex_col),
            "age_group": _optional_text(frame, age_col),
            "measure": _optional_text(frame, measure_col).where(
                _optional_text(frame, measure_col).notna(),
                measure_default,
            ),
            "metric": _optional_text(frame, metric_col).where(
                _optional_text(frame, metric_col).notna(),
                metric_default,
            ),
            "value": pd.to_numeric(frame[value_col], errors="coerce"),
            "source": source_name,
            "source_file": str(source_file),
            "source_version": source_file.stem,
            "location_name": location_names,
            "provenance": str(source_file),
        }
    )

    if cause_col is not None:
        normalized["cause_name"] = _optional_text(frame, cause_col)
    if location_id_col is not None:
        normalized["location_id"] = pd.to_numeric(frame[location_id_col], errors="coerce").astype("Int64")
    if upper_col is not None:
        normalized["upper"] = pd.to_numeric(frame[upper_col], errors="coerce")
    if lower_col is not None:
        normalized["lower"] = pd.to_numeric(frame[lower_col], errors="coerce")

    normalized = normalized.dropna(subset=["year", "value"])
    normalized["year"] = normalized["year"].astype(int)

    unresolved = normalized["iso3"].isna()
    if unresolved.any():
        unresolved_names = sorted(set(normalized.loc[unresolved, "location_name"].dropna().astype(str)))
        if not allow_unresolved_locations:
            raise SourceAdapterError(
                f"{source_name} contains unresolved country names: {', '.join(unresolved_names[:10])}"
            )
        normalized = normalized.loc[~unresolved].copy()

    return normalized.reset_index(drop=True)


def load_ihme_context(repo_root: Path) -> pd.DataFrame:
    """Load a minimal phase-1 IHME country-year context surface."""

    paths = resolve_ihme_dataset_paths(repo_root)
    burden = _normalize_context_frame(
        pd.read_parquet(paths.burden),
        source_name="ihme_burden",
        source_file=paths.burden,
        measure_default="burden",
        metric_default="burden",
        value_candidates=("value", "mean"),
        allow_unresolved_locations=False,
    )
    population = _normalize_context_frame(
        pd.read_parquet(paths.population),
        source_name="ihme_population",
        source_file=paths.population,
        measure_default="population",
        metric_default="population",
        value_candidates=("value", "population"),
        allow_unresolved_locations=False,
    )
    sdi = _normalize_context_frame(
        pd.read_parquet(paths.sdi),
        source_name="ihme_sdi",
        source_file=paths.sdi,
        measure_default="sdi",
        metric_default="sdi",
        value_candidates=("value", "sdi"),
        allow_unresolved_locations=True,
    )

    return pd.concat([burden, population, sdi], ignore_index=True)


def load_wb_context(repo_root: Path) -> pd.DataFrame:
    """Load minimal WB country-year context surface."""

    wdi_dir = repo_root / "data" / "silver" / "wdi" / "harmonized"
    gdp_path = wdi_dir / "NY.GDP.PCAP.CD.parquet"
    water_path = wdi_dir / "SH.H2O.BASW.ZS.parquet"

    missing = [str(p) for p in (gdp_path, water_path) if not p.exists()]
    if missing:
        raise SourceAdapterError(f"Missing WB datasets: {', '.join(missing)}")

    gdp = _normalize_context_frame(
        pd.read_parquet(gdp_path),
        source_name="wb_wdi",
        source_file=gdp_path,
        measure_default="gdp_per_capita",
        metric_default="usd_current",
        value_candidates=("value",),
        allow_unresolved_locations=True,
    )
    water = _normalize_context_frame(
        pd.read_parquet(water_path),
        source_name="wb_wdi",
        source_file=water_path,
        measure_default="basic_water_access",
        metric_default="percent",
        value_candidates=("value",),
        allow_unresolved_locations=True,
    )

    return pd.concat([gdp, water], ignore_index=True)


def load_who_context(repo_root: Path) -> pd.DataFrame:
    """Load minimal WHO country-year context surface."""

    gho_dir = repo_root / "data" / "silver" / "gho" / "observations_wide"
    ghed_dir = repo_root / "data" / "silver" / "ghed"
    le_path = gho_dir / "WHOSIS_000001.parquet"
    che_path = ghed_dir / "ghed_data.parquet"

    missing = [str(p) for p in (le_path, che_path) if not p.exists()]
    if missing:
        raise SourceAdapterError(f"Missing WHO datasets: {', '.join(missing)}")

    le = _normalize_context_frame(
        pd.read_parquet(le_path),
        source_name="who_gho",
        source_file=le_path,
        measure_default="life_expectancy",
        metric_default="years",
        value_candidates=("numeric_value", "value", "Numeric"),
        allow_unresolved_locations=True,
    )
    
    # GHED is wide, needs special handling or simpler column find
    che_raw = pd.read_parquet(che_path)
    che = _normalize_context_frame(
        che_raw,
        source_name="who_ghed",
        source_file=che_path,
        measure_default="che_gdp",
        metric_default="percent",
        value_candidates=("che_gdp",),
        allow_unresolved_locations=True,
    )

    return pd.concat([le, che], ignore_index=True)


def load_unified_context(
    *,
    ihme_repo_root: Path,
    wb_repo_root: Path,
    who_repo_root: Path,
) -> pd.DataFrame:
    """Load and unify all available context sources."""

    ihme = load_ihme_context(ihme_repo_root)
    wb = load_wb_context(wb_repo_root)
    who = load_who_context(who_repo_root)

    context = pd.concat([ihme, wb, who], ignore_index=True)
    ATLAS_CONTEXT_CONTRACT.validate_columns(context.columns)

    sort_columns = [
        "source",
        "location_name",
        "year",
        "measure",
        "metric",
        "sex",
        "age_group",
    ]
    return context.sort_values(sort_columns, kind="stable", na_position="last").reset_index(drop=True)
