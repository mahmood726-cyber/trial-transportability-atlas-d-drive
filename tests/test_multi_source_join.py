from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas.context_join import materialize_context_join
from tests.test_source_adapters import (
    build_ihme_fixture_repo,
    build_wb_fixture_repo,
    build_who_fixture_repo,
)


def test_multi_source_materialize_join(tmp_path: Path) -> None:
    trial_output_dir = tmp_path / "outputs" / "topic_a"
    trial_output_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"nct_id": "N1", "country_name": "United Kingdom", "iso3": "GBR", "year": 2020},
            {"nct_id": "N1", "country_name": "United States", "iso3": "USA", "year": 2020},
        ]
    ).to_parquet(trial_output_dir / "trial_country_year.parquet", index=False)

    summary = materialize_context_join(
        trial_output_dir=trial_output_dir,
        ihme_repo_root=build_ihme_fixture_repo(tmp_path / "ihme"),
        wb_repo_root=build_wb_fixture_repo(tmp_path / "wb"),
        who_repo_root=build_who_fixture_repo(tmp_path / "who"),
    )

    joined = pd.read_parquet(trial_output_dir / "context_joined.parquet")

    assert summary["context_rows"] == 16
    assert summary["context_available_rows"] == 15
    assert summary["unresolved_trial_rows"] == 0

    sources = set(joined["source"].dropna().unique().tolist())
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
    }.issubset(sources)

    measures = set(joined["measure"].dropna().unique().tolist())
    assert "Population, total" in measures
    assert "Life expectancy at birth (years)" in measures
    assert "Current health expenditure (% of GDP)" in measures

    gbr_life_expectancy = joined[
        (joined["iso3_resolved"] == "GBR")
        & (joined["measure"] == "Life expectancy at birth (years)")
    ]
    assert not gbr_life_expectancy.empty
    assert gbr_life_expectancy.iloc[0]["value"] == 81.0
    assert joined["context_available_flag"].eq(False).sum() == 1
