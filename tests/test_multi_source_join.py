from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

from trial_transportability_atlas.context_join import materialize_context_join
from trial_transportability_atlas.project_paths import discover_external_paths

def test_multi_source_materialize_join(tmp_path: Path) -> None:
    # 1. Setup Mock Trial Data
    trial_output_dir = tmp_path / "outputs" / "topic_a"
    trial_output_dir.mkdir(parents=True)
    pd.DataFrame([
        {"nct_id": "N1", "country_name": "United Kingdom", "iso3": "GBR", "year": 2022},
        {"nct_id": "N1", "country_name": "United States", "iso3": "USA", "year": 2022},
    ]).to_parquet(trial_output_dir / "trial_country_year.parquet")

    # 2. Setup Mock IHME (D-drive style)
    ihme_root = tmp_path / "ihme"
    (ihme_root / "datasets").mkdir(parents=True)
    pd.DataFrame([{"location": "United Kingdom", "year": 2022, "value": 10.0}]).to_parquet(
        ihme_root / "datasets" / "gbd2023_all_burden_204countries_1990_2023.parquet"
    )
    pd.DataFrame([{"location": "United Kingdom", "year": 2022, "population": 1000}]).to_parquet(
        ihme_root / "datasets" / "gbd2023_population_204countries_1990_2023.parquet"
    )
    pd.DataFrame([{"location": "United Kingdom", "year": 2022, "sdi": 0.8}]).to_parquet(
        ihme_root / "datasets" / "gbd2021_sdi_1950_2021.parquet"
    )

    # 3. Setup Mock World Bank
    wb_root = tmp_path / "wb"
    wb_silver = wb_root / "data" / "silver" / "wdi" / "harmonized"
    wb_silver.mkdir(parents=True)
    pd.DataFrame([{"iso3c": "GBR", "year": 2022, "value": 45000}]).to_parquet(
        wb_silver / "NY.GDP.PCAP.CD.parquet"
    )
    pd.DataFrame([{"iso3c": "GBR", "year": 2022, "value": 99.0}]).to_parquet(
        wb_silver / "SH.H2O.BASW.ZS.parquet"
    )

    # 4. Setup Mock WHO
    who_root = tmp_path / "who"
    who_gho = who_root / "data" / "silver" / "gho" / "observations_wide"
    who_ghed = who_root / "data" / "silver" / "ghed"
    who_gho.mkdir(parents=True)
    who_ghed.mkdir(parents=True)
    
    pd.DataFrame([{"spatial_dim": "GBR", "time_dim": 2022, "numeric_value": 81.2}]).to_parquet(
        who_gho / "WHOSIS_000001.parquet"
    )
    pd.DataFrame([{"code": "GBR", "year": 2022, "che_gdp": 10.1}]).to_parquet(
        who_ghed / "ghed_data.parquet"
    )

    # 5. Execute Multi-Source Join
    summary = materialize_context_join(
        trial_output_dir=trial_output_dir,
        ihme_repo_root=ihme_root,
        wb_repo_root=wb_root,
        who_repo_root=who_root,
    )

    # 6. Verify Results
    joined = pd.read_parquet(trial_output_dir / "context_joined.parquet")
    
    # Check sources
    sources = set(joined["source"].unique())
    assert "ihme_burden" in sources
    assert "wb_wdi" in sources
    assert "who_gho" in sources
    assert "who_ghed" in sources

    # Check specific metrics
    measures = set(joined["measure"].unique())
    assert "gdp_per_capita" in measures
    assert "life_expectancy" in measures
    assert "che_gdp" in measures

    # Verify GBR row has WHO LE
    gbr_le = joined[(joined["iso3_resolved"] == "GBR") & (joined["measure"] == "life_expectancy")]
    assert not gbr_le.empty
    assert gbr_le.iloc[0]["value"] == 81.2
