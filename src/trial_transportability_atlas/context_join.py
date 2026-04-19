"""Country-year context join helpers."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.country_iso3 import country_name_to_iso3
from trial_transportability_atlas.source_adapters import load_unified_context


def enrich_trial_country_year_iso3(trial_country_year: pd.DataFrame) -> pd.DataFrame:
    """Fill missing ISO3 codes from trial country names with deterministic lookup."""

    frame = trial_country_year.copy()
    inferred = frame["country_name"].map(country_name_to_iso3)
    frame["iso3_resolved"] = frame["iso3"].fillna(inferred)
    frame["iso3_resolution_status"] = frame["iso3_resolved"].notna().map(
        {True: "resolved", False: "unresolved"}
    )
    return frame


def build_context_joined(
    trial_country_year: pd.DataFrame,
    context_long: pd.DataFrame,
) -> pd.DataFrame:
    """Join trial country-year rows to long-form context rows."""

    left = enrich_trial_country_year_iso3(trial_country_year)
    right = context_long.rename(columns={"iso3": "context_iso3"})
    joined = left.merge(
        right,
        how="left",
        left_on=["iso3_resolved", "year"],
        right_on=["context_iso3", "year"],
    )
    joined["context_available_flag"] = joined["value"].notna()
    sort_columns = [
        "nct_id",
        "country_name",
        "year",
        "source",
        "measure",
        "metric",
        "sex",
        "age_group",
    ]
    return joined.sort_values(sort_columns, kind="stable", na_position="last").reset_index(drop=True)


def materialize_context_join(
    *,
    trial_output_dir: Path,
    ihme_repo_root: Path,
    wb_repo_root: Path,
    who_repo_root: Path,
) -> dict[str, object]:
    """Materialize the multi-source unified context join."""

    trial_country_year = pd.read_parquet(trial_output_dir / "trial_country_year.parquet")
    context_long = load_unified_context(
        ihme_repo_root=ihme_repo_root,
        wb_repo_root=wb_repo_root,
        who_repo_root=who_repo_root,
    )
    context_joined = build_context_joined(trial_country_year, context_long)

    context_path = trial_output_dir / "context_joined.parquet"
    summary_path = trial_output_dir / "context_join_manifest.json"
    context_joined.to_parquet(context_path, index=False)

    summary = {
        "trial_country_year_rows": int(len(trial_country_year)),
        "context_rows": int(len(context_joined)),
        "context_available_rows": int(context_joined["context_available_flag"].sum()),
        "unresolved_trial_rows": int((context_joined["iso3_resolution_status"] == "unresolved").sum()),
        "distinct_context_measures": sorted(context_joined["measure"].dropna().unique().tolist()),
        "distinct_context_sources": sorted(context_joined["source"].dropna().unique().tolist()),
        "outputs": {
            "context_joined": str(context_path),
            "context_join_manifest": str(summary_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
