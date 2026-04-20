from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas.contracts import ATLAS_CONTEXT_CONTRACT
from trial_transportability_atlas.source_adapters import (
    load_ihme_context,
    load_unified_context,
    load_wb_context,
    load_who_context,
)


def build_ihme_fixture_repo(repo_root: Path) -> Path:
    datasets = repo_root / "datasets"
    datasets.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "location": "United Kingdom",
                "year": 2020,
                "measure": "Deaths",
                "metric": "Number",
                "cause": "All causes",
                "value": 10.0,
            },
        ]
    ).to_parquet(datasets / "gbd2023_all_burden_204countries_1990_2023.parquet", index=False)

    pd.DataFrame(
        [
            {
                "location": "United Kingdom",
                "year": 2020,
                "sex": "Both",
                "population": 1000,
            },
        ]
    ).to_parquet(datasets / "gbd2023_population_204countries_1990_2023.parquet", index=False)

    pd.DataFrame(
        [
            {"location": "United Kingdom", "year": 2020, "sdi": 0.8},
            {"location": "Global", "year": 2020, "sdi": 0.7},
        ]
    ).to_parquet(datasets / "gbd2021_sdi_1950_2021.parquet", index=False)
    return repo_root


def build_wb_fixture_repo(repo_root: Path) -> Path:
    silver_dir = repo_root / "data" / "silver"
    specs = [
        ("population", "SP.POP.TOTL.parquet", "wb_SP.POP.TOTL", "Population, total", 1000.0),
        ("governance", "GE.EST.parquet", "wb_GE.EST", "Government Effectiveness: Estimate", 0.5),
        ("poverty", "SI.POV.GINI.parquet", "wb_SI.POV.GINI", "Gini index", 29.4),
        ("hnp", "SH.MED.PHYS.ZS.parquet", "wb_SH.MED.PHYS.ZS", "Physicians (per 1,000 people)", 2.8),
        (
            "uhc",
            "SH.UHC.NOP1.ZG.parquet",
            "wb_SH.UHC.NOP1.ZG",
            "Increase in poverty gap due to out-of-pocket health care expenditure",
            0.2,
        ),
    ]
    for domain, filename, indicator_code, indicator_name, value in specs:
        path = silver_dir / domain / "harmonized" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "iso3c": "GBR",
                    "year": 2020,
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_name,
                    "value": value,
                    "lower": None,
                    "upper": None,
                    "sex": None,
                    "age_group": None,
                },
                {
                    "iso3c": "AFE",
                    "year": 2020,
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_name,
                    "value": value * 10,
                    "lower": None,
                    "upper": None,
                    "sex": None,
                    "age_group": None,
                },
            ]
        ).to_parquet(path, index=False)
    return repo_root


def build_who_fixture_repo(repo_root: Path) -> Path:
    silver_dir = repo_root / "data" / "silver"
    gho_dir = silver_dir / "gho"
    observations_dir = gho_dir / "observations_wide"
    observations_dir.mkdir(parents=True, exist_ok=True)
    (silver_dir / "ghed").mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"indicator_code": "WHOSIS_000001", "indicator_name": "Life expectancy at birth (years)", "language": "EN"},
            {"indicator_code": "SDGSUICIDE", "indicator_name": "Crude suicide rates (per 100 000 population)", "language": "EN"},
            {
                "indicator_code": "SA_0000001688",
                "indicator_name": "Alcohol, total per capita (15+) consumption (litres of pure alcohol)",
                "language": "EN",
            },
        ]
    ).to_parquet(gho_dir / "indicators.parquet", index=False)

    pd.DataFrame(
        [
            {
                "indicator_code": "WHOSIS_000001",
                "spatial_dim_type": "COUNTRY",
                "spatial_dim": "GBR",
                "time_dim": 2020,
                "dim1": "SEX_BTSX",
                "dim2": None,
                "dim3": None,
                "numeric_value": 81.0,
                "low": 80.0,
                "high": 82.0,
            },
            {
                "indicator_code": "WHOSIS_000001",
                "spatial_dim_type": "REGION",
                "spatial_dim": "EUR",
                "time_dim": 2020,
                "dim1": "SEX_BTSX",
                "dim2": None,
                "dim3": None,
                "numeric_value": 80.0,
                "low": None,
                "high": None,
            },
        ]
    ).to_parquet(observations_dir / "WHOSIS_000001.parquet", index=False)

    pd.DataFrame(
        [
            {
                "indicator_code": "SDGSUICIDE",
                "spatial_dim_type": "COUNTRY",
                "spatial_dim": "GBR",
                "time_dim": 2020,
                "dim1": "SEX_FMLE",
                "dim2": "AGEGROUP_YEARSALL",
                "dim3": None,
                "numeric_value": 4.2,
                "low": 3.0,
                "high": 5.0,
            }
        ]
    ).to_parquet(observations_dir / "SDGSUICIDE.parquet", index=False)

    pd.DataFrame(
        [
            {
                "indicator_code": "SA_0000001688",
                "spatial_dim_type": "COUNTRY",
                "spatial_dim": "GBR",
                "time_dim": 2020,
                "dim1": "SEX_BTSX",
                "dim2": None,
                "dim3": None,
                "numeric_value": 9.5,
                "low": 8.8,
                "high": 10.2,
            }
        ]
    ).to_parquet(observations_dir / "SA_0000001688.parquet", index=False)

    pd.DataFrame(
        [
            {
                "location": "United Kingdom",
                "code": "GBR",
                "year": 2020,
                "che_gdp": 10.0,
                "che_pc_usd": 4000.0,
                "gghed_che": 80.0,
                "oops_che": 15.0,
            },
            {
                "location": "Europe",
                "code": "EUR",
                "year": 2020,
                "che_gdp": 9.0,
                "che_pc_usd": 3500.0,
                "gghed_che": 70.0,
                "oops_che": 20.0,
            },
        ]
    ).to_parquet(silver_dir / "ghed" / "ghed_data.parquet", index=False)
    return repo_root


def test_load_ihme_context_normalizes_minimal_fixture_repo(tmp_path: Path) -> None:
    context = load_ihme_context(build_ihme_fixture_repo(tmp_path / "ihme"))

    assert len(context) == 3
    assert not context["iso3"].isna().any()
    assert "Global" not in context["location_name"].tolist()
    assert sorted(context["source"].unique().tolist()) == [
        "ihme_burden",
        "ihme_population",
        "ihme_sdi",
    ]
    ATLAS_CONTEXT_CONTRACT.validate_columns(context.columns)
    assert {"sex", "age_group", "location_name"}.issubset(context.columns)


def test_load_wb_context_normalizes_curated_harmonized_files(tmp_path: Path) -> None:
    context = load_wb_context(build_wb_fixture_repo(tmp_path / "wb"))

    assert len(context) == 5
    assert set(context["source"].unique().tolist()) == {
        "wb_population",
        "wb_governance",
        "wb_poverty",
        "wb_hnp",
        "wb_uhc",
    }
    assert set(context["iso3"].unique().tolist()) == {"GBR"}
    assert "AFE" not in context["location_name"].tolist()


def test_load_who_context_normalizes_gho_and_ghed_sources(tmp_path: Path) -> None:
    context = load_who_context(build_who_fixture_repo(tmp_path / "who"))

    assert len(context) == 7
    assert set(context["source"].unique().tolist()) == {"who_gho", "who_ghed"}
    assert set(context["iso3"].unique().tolist()) == {"GBR"}
    assert "Europe" not in context["location_name"].tolist()
    assert "Female" in context["sex"].tolist()
    assert "YEARSALL" in context["age_group"].tolist()


def test_load_unified_context_concatenates_all_sources(tmp_path: Path) -> None:
    context = load_unified_context(
        ihme_repo_root=build_ihme_fixture_repo(tmp_path / "ihme"),
        wb_repo_root=build_wb_fixture_repo(tmp_path / "wb"),
        who_repo_root=build_who_fixture_repo(tmp_path / "who"),
    )

    assert len(context) == 15
    assert {
        "ihme_burden",
        "ihme_population",
        "ihme_sdi",
        "wb_population",
        "wb_governance",
        "wb_poverty",
        "wb_hnp",
        "wb_uhc",
        "who_gho",
        "who_ghed",
    }.issubset(set(context["source"].unique().tolist()))
