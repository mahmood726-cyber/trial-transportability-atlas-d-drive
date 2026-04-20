"""Curated phase-1 context source adapters."""
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


@dataclass(frozen=True)
class WbDatasetPaths:
    population: Path
    governance: Path
    poverty: Path
    hnp: Path
    uhc: Path
    gdp: Path


@dataclass(frozen=True)
class WhoDatasetPaths:
    indicators: Path
    life_expectancy: Path
    suicide: Path
    alcohol: Path
    ghed_data: Path


WHO_GHED_METRICS = (
    ("che_gdp", "Current health expenditure (% of GDP)"),
    ("che_pc_usd", "Current health expenditure per capita (USD)"),
    (
        "gghed_che",
        "Domestic general government health expenditure (% of current health expenditure)",
    ),
    ("oops_che", "Out-of-pocket expenditure (% of current health expenditure)"),
)

WHO_GHO_SEX_MAP = {
    "SEX_BTSX": "Both",
    "SEX_FMLE": "Female",
    "SEX_MLE": "Male",
}


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


def resolve_wb_dataset_paths(repo_root: Path) -> WbDatasetPaths:
    """Resolve the curated phase-1 WB indicator files."""

    silver_dir = repo_root / "data" / "silver"
    paths = WbDatasetPaths(
        population=silver_dir / "population" / "harmonized" / "SP.POP.TOTL.parquet",
        governance=silver_dir / "governance" / "harmonized" / "GE.EST.parquet",
        poverty=silver_dir / "poverty" / "harmonized" / "SI.POV.GINI.parquet",
        hnp=silver_dir / "hnp" / "harmonized" / "SH.MED.PHYS.ZS.parquet",
        uhc=silver_dir / "uhc" / "harmonized" / "SH.UHC.NOP1.ZG.parquet",
        gdp=silver_dir / "wdi" / "harmonized" / "NY.GDP.PCAP.CD.parquet",
    )
    missing = [str(path) for path in paths.__dict__.values() if not path.exists()]
    if missing:
        raise SourceAdapterError(f"Missing WB datasets: {', '.join(missing)}")
    return paths


def resolve_who_dataset_paths(repo_root: Path) -> WhoDatasetPaths:
    """Resolve the curated phase-1 WHO indicator files."""

    silver_dir = repo_root / "data" / "silver"
    paths = WhoDatasetPaths(
        indicators=silver_dir / "gho" / "indicators.parquet",
        life_expectancy=silver_dir / "gho" / "observations_wide" / "WHOSIS_000001.parquet",
        suicide=silver_dir / "gho" / "observations_wide" / "SDGSUICIDE.parquet",
        alcohol=silver_dir / "gho" / "observations_wide" / "SA_0000001688.parquet",
        ghed_data=silver_dir / "ghed" / "ghed_data.parquet",
    )
    missing = [str(path) for path in paths.__dict__.values() if not path.exists()]
    if missing:
        raise SourceAdapterError(f"Missing WHO datasets: {', '.join(missing)}")
    return paths


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


def _optional_text_series(frame: pd.DataFrame, column_name: str | None) -> pd.Series:
    if column_name is None:
        return pd.Series([None] * len(frame), dtype="object")
    series = frame[column_name].astype("string")
    return series.where(series.notna(), None).astype("object")


def _with_default(series: pd.Series, default: str) -> pd.Series:
    return series.where(series.notna(), default)


def _drop_invalid_rows(
    frame: pd.DataFrame,
    *,
    source_name: str,
    location_column: str = "location_name",
    allow_unresolved_locations: bool,
) -> pd.DataFrame:
    filtered = frame.dropna(subset=["year", "value"]).copy()
    filtered["year"] = filtered["year"].astype(int)

    unresolved = filtered["iso3"].isna()
    if unresolved.any():
        unresolved_names = sorted(
            set(filtered.loc[unresolved, location_column].dropna().astype(str))
        )
        if not allow_unresolved_locations:
            preview = ", ".join(unresolved_names[:10])
            raise SourceAdapterError(
                f"{source_name} contains unresolved country rows: {preview}"
            )
        filtered = filtered.loc[~unresolved].copy()
    return filtered.reset_index(drop=True)


def _finalize_context(context: pd.DataFrame) -> pd.DataFrame:
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


def _normalize_ihme_frame(
    frame: pd.DataFrame,
    *,
    source_name: str,
    source_file: Path,
    measure_default: str,
    metric_default: str,
    value_candidates: tuple[str, ...],
    allow_unresolved_locations: bool,
) -> pd.DataFrame:
    location_col = _find_column(frame, ("location",))
    year_col = _find_column(frame, ("year", "year_id"))
    value_col = _find_column(frame, value_candidates)
    sex_col = _find_column(frame, ("sex", "sex_name"), required=False)
    age_col = _find_column(frame, ("age_group", "age_name"), required=False)
    measure_col = _find_column(frame, ("measure", "measure_name"), required=False)
    metric_col = _find_column(frame, ("metric", "metric_name"), required=False)
    cause_col = _find_column(frame, ("cause", "cause_name"), required=False)
    location_id_col = _find_column(frame, ("location_id",), required=False)
    upper_col = _find_column(frame, ("upper", "sdi_upper"), required=False)
    lower_col = _find_column(frame, ("lower", "sdi_lower"), required=False)

    location_name = _optional_text_series(frame, location_col)
    normalized = pd.DataFrame(
        {
            "iso3": location_name.map(country_name_to_iso3),
            "year": pd.to_numeric(frame[year_col], errors="coerce").astype("Int64"),
            "sex": _optional_text_series(frame, sex_col),
            "age_group": _optional_text_series(frame, age_col),
            "measure": _with_default(_optional_text_series(frame, measure_col), measure_default),
            "metric": _with_default(_optional_text_series(frame, metric_col), metric_default),
            "value": pd.to_numeric(frame[value_col], errors="coerce"),
            "source": source_name,
            "source_file": str(source_file),
            "source_version": source_file.stem,
            "location_name": location_name,
            "provenance": str(source_file),
        }
    )
    if cause_col is not None:
        normalized["cause_name"] = _optional_text_series(frame, cause_col)
    if location_id_col is not None:
        normalized["location_id"] = pd.to_numeric(frame[location_id_col], errors="coerce").astype("Int64")
    if upper_col is not None:
        normalized["upper"] = pd.to_numeric(frame[upper_col], errors="coerce")
    if lower_col is not None:
        normalized["lower"] = pd.to_numeric(frame[lower_col], errors="coerce")
    return _drop_invalid_rows(
        normalized,
        source_name=source_name,
        allow_unresolved_locations=allow_unresolved_locations,
    )


def _normalize_wb_harmonized_frame(
    frame: pd.DataFrame,
    *,
    source_name: str,
    source_file: Path,
) -> pd.DataFrame:
    iso3_col = _find_column(frame, ("iso3c",))
    year_col = _find_column(frame, ("year",))
    indicator_code_col = _find_column(frame, ("indicator_code",))
    indicator_name_col = _find_column(frame, ("indicator_name",))
    value_col = _find_column(frame, ("value",))
    sex_col = _find_column(frame, ("sex",), required=False)
    age_col = _find_column(frame, ("age_group",), required=False)
    lower_col = _find_column(frame, ("lower",), required=False)
    upper_col = _find_column(frame, ("upper",), required=False)

    location_name = _optional_text_series(frame, iso3_col)
    normalized = pd.DataFrame(
        {
            "iso3": location_name.map(country_name_to_iso3),
            "year": pd.to_numeric(frame[year_col], errors="coerce").astype("Int64"),
            "sex": _optional_text_series(frame, sex_col),
            "age_group": _optional_text_series(frame, age_col),
            "measure": _optional_text_series(frame, indicator_name_col),
            "metric": _optional_text_series(frame, indicator_code_col),
            "value": pd.to_numeric(frame[value_col], errors="coerce"),
            "source": source_name,
            "source_file": str(source_file),
            "source_version": source_file.stem,
            "location_name": location_name,
            "provenance": str(source_file),
        }
    )
    if lower_col is not None:
        normalized["lower"] = pd.to_numeric(frame[lower_col], errors="coerce")
    if upper_col is not None:
        normalized["upper"] = pd.to_numeric(frame[upper_col], errors="coerce")
    return _drop_invalid_rows(
        normalized,
        source_name=source_name,
        allow_unresolved_locations=True,
    )


def _extract_prefixed_dimension(frame: pd.DataFrame, prefix: str) -> pd.Series:
    values = pd.Series([None] * len(frame), dtype="object")
    for column_name in ("dim1", "dim2", "dim3"):
        if column_name not in frame.columns:
            continue
        series = frame[column_name].astype("string")
        matched = series.where(series.str.startswith(prefix, na=False), None).astype("object")
        values = values.where(values.notna(), matched)
    return values


def _normalize_who_gho_sex(frame: pd.DataFrame) -> pd.Series:
    sex_codes = _extract_prefixed_dimension(frame, "SEX_")
    return sex_codes.map(lambda value: WHO_GHO_SEX_MAP.get(value, value))


def _normalize_who_gho_age(frame: pd.DataFrame) -> pd.Series:
    age_codes = _extract_prefixed_dimension(frame, "AGEGROUP_")
    return age_codes.map(
        lambda value: value.removeprefix("AGEGROUP_") if isinstance(value, str) else None
    )


def _normalize_who_gho_frame(
    frame: pd.DataFrame,
    *,
    source_file: Path,
    indicator_name: str,
) -> pd.DataFrame:
    if frame.empty:
        raise SourceAdapterError(f"{source_file.name} is empty")

    spatial_type_col = _find_column(frame, ("spatial_dim_type",))
    spatial_dim_col = _find_column(frame, ("spatial_dim",))
    year_col = _find_column(frame, ("time_dim",))
    indicator_code_col = _find_column(frame, ("indicator_code",))
    value_col = _find_column(frame, ("numeric_value",))
    low_col = _find_column(frame, ("low",), required=False)
    high_col = _find_column(frame, ("high",), required=False)

    country_rows = frame.loc[frame[spatial_type_col].astype("string") == "COUNTRY"].copy()
    if country_rows.empty:
        raise SourceAdapterError(f"{source_file.name} has no COUNTRY rows")

    location_name = _optional_text_series(country_rows, spatial_dim_col)
    normalized = pd.DataFrame(
        {
            "iso3": location_name.map(country_name_to_iso3),
            "year": pd.to_numeric(country_rows[year_col], errors="coerce").astype("Int64"),
            "sex": _normalize_who_gho_sex(country_rows),
            "age_group": _normalize_who_gho_age(country_rows),
            "measure": indicator_name,
            "metric": _optional_text_series(country_rows, indicator_code_col),
            "value": pd.to_numeric(country_rows[value_col], errors="coerce"),
            "source": "who_gho",
            "source_file": str(source_file),
            "source_version": source_file.stem,
            "location_name": location_name,
            "provenance": str(source_file),
        }
    )
    if low_col is not None:
        normalized["lower"] = pd.to_numeric(country_rows[low_col], errors="coerce")
    if high_col is not None:
        normalized["upper"] = pd.to_numeric(country_rows[high_col], errors="coerce")
    return _drop_invalid_rows(
        normalized,
        source_name="who_gho",
        allow_unresolved_locations=True,
    )


def _normalize_who_ghed_frame(frame: pd.DataFrame, *, source_file: Path) -> pd.DataFrame:
    location_col = _find_column(frame, ("location",))
    code_col = _find_column(frame, ("code",))
    year_col = _find_column(frame, ("year",))
    metric_columns = [column for column, _ in WHO_GHED_METRICS if column in frame.columns]
    missing_metrics = [column for column, _ in WHO_GHED_METRICS if column not in frame.columns]
    if missing_metrics:
        raise SourceAdapterError(
            f"ghed_data.parquet missing required metric columns: {', '.join(missing_metrics)}"
        )

    base = frame[[location_col, code_col, year_col, *metric_columns]].copy()
    base.rename(
        columns={
            location_col: "location_name",
            code_col: "location_code",
            year_col: "year",
        },
        inplace=True,
    )
    base["iso3"] = _optional_text_series(base, "location_code").map(country_name_to_iso3)
    fallback_iso3 = _optional_text_series(base, "location_name").map(country_name_to_iso3)
    base["iso3"] = base["iso3"].where(base["iso3"].notna(), fallback_iso3)

    melted = base.melt(
        id_vars=["location_name", "location_code", "iso3", "year"],
        value_vars=metric_columns,
        var_name="metric",
        value_name="value",
    )
    metric_labels = dict(WHO_GHED_METRICS)
    normalized = pd.DataFrame(
        {
            "iso3": melted["iso3"],
            "year": pd.to_numeric(melted["year"], errors="coerce").astype("Int64"),
            "sex": None,
            "age_group": None,
            "measure": melted["metric"].map(metric_labels),
            "metric": "who_ghed_" + melted["metric"].astype(str),
            "value": pd.to_numeric(melted["value"], errors="coerce"),
            "source": "who_ghed",
            "source_file": str(source_file),
            "source_version": source_file.stem,
            "location_name": _optional_text_series(melted, "location_name"),
            "provenance": str(source_file),
        }
    )
    return _drop_invalid_rows(
        normalized,
        source_name="who_ghed",
        allow_unresolved_locations=True,
    )


def load_ihme_context(repo_root: Path) -> pd.DataFrame:
    """Load the curated IHME country-year context surface."""

    paths = resolve_ihme_dataset_paths(repo_root)
    burden = _normalize_ihme_frame(
        pd.read_parquet(paths.burden),
        source_name="ihme_burden",
        source_file=paths.burden,
        measure_default="burden",
        metric_default="burden",
        value_candidates=("value", "mean"),
        allow_unresolved_locations=False,
    )
    population = _normalize_ihme_frame(
        pd.read_parquet(paths.population),
        source_name="ihme_population",
        source_file=paths.population,
        measure_default="population",
        metric_default="population",
        value_candidates=("value", "population"),
        allow_unresolved_locations=False,
    )
    sdi = _normalize_ihme_frame(
        pd.read_parquet(paths.sdi),
        source_name="ihme_sdi",
        source_file=paths.sdi,
        measure_default="sdi",
        metric_default="sdi",
        value_candidates=("value", "sdi"),
        allow_unresolved_locations=True,
    )
    return _finalize_context(pd.concat([burden, population, sdi], ignore_index=True))


def load_wb_context(repo_root: Path) -> pd.DataFrame:
    """Load curated WB country-year context rows."""

    paths = resolve_wb_dataset_paths(repo_root)
    frames = [
        _normalize_wb_harmonized_frame(
            pd.read_parquet(paths.population),
            source_name="wb_population",
            source_file=paths.population,
        ),
        _normalize_wb_harmonized_frame(
            pd.read_parquet(paths.governance),
            source_name="wb_governance",
            source_file=paths.governance,
        ),
        _normalize_wb_harmonized_frame(
            pd.read_parquet(paths.poverty),
            source_name="wb_poverty",
            source_file=paths.poverty,
        ),
        _normalize_wb_harmonized_frame(
            pd.read_parquet(paths.hnp),
            source_name="wb_hnp",
            source_file=paths.hnp,
        ),
        _normalize_wb_harmonized_frame(
            pd.read_parquet(paths.uhc),
            source_name="wb_uhc",
            source_file=paths.uhc,
        ),
        _normalize_wb_harmonized_frame(
            pd.read_parquet(paths.gdp),
            source_name="wb_gdp",
            source_file=paths.gdp,
        ),
    ]
    return _finalize_context(pd.concat(frames, ignore_index=True))


def load_who_context(repo_root: Path) -> pd.DataFrame:
    """Load curated WHO country-year context rows."""

    paths = resolve_who_dataset_paths(repo_root)
    indicators = pd.read_parquet(paths.indicators)
    indicator_lookup = {
        row["indicator_code"]: row["indicator_name"]
        for _, row in indicators.iterrows()
    }

    gho_frames = [
        _normalize_who_gho_frame(
            pd.read_parquet(paths.life_expectancy),
            source_file=paths.life_expectancy,
            indicator_name=indicator_lookup["WHOSIS_000001"],
        ),
        _normalize_who_gho_frame(
            pd.read_parquet(paths.suicide),
            source_file=paths.suicide,
            indicator_name=indicator_lookup["SDGSUICIDE"],
        ),
        _normalize_who_gho_frame(
            pd.read_parquet(paths.alcohol),
            source_file=paths.alcohol,
            indicator_name=indicator_lookup["SA_0000001688"],
        ),
    ]
    ghed = _normalize_who_ghed_frame(pd.read_parquet(paths.ghed_data), source_file=paths.ghed_data)
    return _finalize_context(pd.concat([*gho_frames, ghed], ignore_index=True))


def load_unified_context(
    *,
    ihme_repo_root: Path,
    wb_repo_root: Path,
    who_repo_root: Path,
) -> pd.DataFrame:
    """Load and unify all curated phase-1 context sources."""

    context = pd.concat(
        [
            load_ihme_context(ihme_repo_root),
            load_wb_context(wb_repo_root),
            load_who_context(who_repo_root),
        ],
        ignore_index=True,
    )
    return _finalize_context(context)
