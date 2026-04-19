from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas.contracts import ATLAS_CONTEXT_CONTRACT
from trial_transportability_atlas.source_adapters import load_ihme_context


def test_load_ihme_context_normalizes_minimal_fixture_repo(tmp_path: Path) -> None:
    datasets = tmp_path / "datasets"
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

    context = load_ihme_context(tmp_path)

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
