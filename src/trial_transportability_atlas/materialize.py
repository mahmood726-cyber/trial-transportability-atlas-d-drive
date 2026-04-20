"""Materialization helpers for filtered topic bridge outputs."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.aact_bridge import (
    extract_trial_country_year,
    extract_trial_outcomes,
)
from trial_transportability_atlas.effect_candidates import build_effect_candidates
from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, select_topic_nct_ids


def materialize_topic_bridge(
    snapshot_dir: Path,
    output_dir: Path,
    topic: TopicSpec = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write filtered bridge outputs for a specific topic."""

    output_dir.mkdir(parents=True, exist_ok=True)
    nct_ids = sorted(select_topic_nct_ids(snapshot_dir, topic))
    trial_country_year = extract_trial_country_year(snapshot_dir, nct_ids=nct_ids)
    trial_outcomes = extract_trial_outcomes(snapshot_dir, nct_ids=nct_ids)
    outcomes_df = pd.DataFrame(trial_outcomes)
    effect_candidates_df = build_effect_candidates(outcomes_df)

    country_path = output_dir / "trial_country_year.parquet"
    outcomes_path = output_dir / "trial_outcomes_long.parquet"
    effect_candidates_path = output_dir / "effect_candidates.parquet"
    pd.DataFrame(trial_country_year).to_parquet(country_path, index=False)
    outcomes_df.to_parquet(outcomes_path, index=False)
    effect_candidates_df.to_parquet(effect_candidates_path, index=False)

    summary = {
        "topic_slug": topic.slug,
        "snapshot_dir": str(snapshot_dir),
        "selected_nct_ids": nct_ids,
        "trial_country_year_rows": len(trial_country_year),
        "trial_outcomes_rows": len(trial_outcomes),
        "effect_candidates_rows": int(len(effect_candidates_df)),
        "strict_comparable_candidates": int(effect_candidates_df["comparable_flag"].sum()),
        "outputs": {
            "trial_country_year": str(country_path),
            "trial_outcomes_long": str(outcomes_path),
            "effect_candidates": str(effect_candidates_path),
        },
    }
    (output_dir / "run_manifest.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    return summary
