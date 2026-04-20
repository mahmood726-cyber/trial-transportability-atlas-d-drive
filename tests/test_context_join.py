from __future__ import annotations

from pathlib import Path

import pandas as pd

from trial_transportability_atlas.context_join import (
    build_context_joined,
    enrich_trial_country_year_iso3,
    materialize_context_join,
)
from tests.test_source_adapters import (
    build_ihme_fixture_repo,
    build_wb_fixture_repo,
    build_who_fixture_repo,
)


def test_enrich_trial_country_year_iso3_resolves_country_names() -> None:
    frame = pd.DataFrame(
        [
            {"nct_id": "N1", "country_name": "United Kingdom", "iso3": None, "year": 2020},
            {"nct_id": "N2", "country_name": "Atlantis", "iso3": None, "year": 2020},
        ]
    )

    enriched = enrich_trial_country_year_iso3(frame)

    assert enriched.loc[0, "iso3_resolved"] == "GBR"
    assert enriched.loc[0, "iso3_resolution_status"] == "resolved"
    assert enriched.loc[1, "iso3_resolved"] is None
    assert enriched.loc[1, "iso3_resolution_status"] == "unresolved"


def test_build_context_joined_expands_long_context_rows() -> None:
    trial = pd.DataFrame(
        [
            {"nct_id": "N1", "country_name": "United Kingdom", "iso3": None, "year": 2020},
        ]
    )
    context = pd.DataFrame(
        [
            {
                "iso3": "GBR",
                "year": 2020,
                "sex": None,
                "age_group": None,
                "measure": "population",
                "metric": "population",
                "value": 1000,
                "source": "ihme_population",
                "source_file": "x",
                "source_version": "x",
                "location_name": "United Kingdom",
                "provenance": "x",
            },
            {
                "iso3": "GBR",
                "year": 2020,
                "sex": None,
                "age_group": None,
                "measure": "sdi",
                "metric": "sdi",
                "value": 0.8,
                "source": "ihme_sdi",
                "source_file": "y",
                "source_version": "y",
                "location_name": "United Kingdom",
                "provenance": "y",
            },
        ]
    )

    joined = build_context_joined(trial, context)

    assert len(joined) == 2
    assert joined["context_available_flag"].tolist() == [True, True]
    assert sorted(joined["measure"].tolist()) == ["population", "sdi"]


def test_materialize_context_join_writes_context_joined(tmp_path: Path) -> None:
    trial_output_dir = tmp_path / "outputs"
    trial_output_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "nct_id": "N1",
                "country_name": "United Kingdom",
                "iso3": None,
                "year": 2020,
                "completion_date": None,
                "country_source_table": "countries",
                "year_source_table": "calculated_values",
                "provenance": "countries+calculated_values",
            },
        ]
    ).to_parquet(trial_output_dir / "trial_country_year.parquet", index=False)

    ihme_root = build_ihme_fixture_repo(tmp_path / "ihme")
    wb_root = build_wb_fixture_repo(tmp_path / "wb")
    who_root = build_who_fixture_repo(tmp_path / "who")

    summary = materialize_context_join(
        trial_output_dir=trial_output_dir,
        ihme_repo_root=ihme_root,
        wb_repo_root=wb_root,
        who_repo_root=who_root,
    )
    joined = pd.read_parquet(trial_output_dir / "context_joined.parquet")

    assert summary["context_rows"] == 15
    assert summary["context_available_rows"] == 15
    assert summary["unresolved_trial_rows"] == 0
    assert "Deaths" in summary["distinct_context_measures"]
    assert "Population, total" in summary["distinct_context_measures"]
    assert "Life expectancy at birth (years)" in summary["distinct_context_measures"]
    assert "Current health expenditure (% of GDP)" in summary["distinct_context_measures"]
    assert "wb_uhc" in summary["distinct_context_sources"]
    assert "who_ghed" in summary["distinct_context_sources"]
    assert Path(summary["outputs"]["context_joined"]).exists()
    assert Path(summary["outputs"]["context_join_manifest"]).exists()
    assert len(joined) == 15
