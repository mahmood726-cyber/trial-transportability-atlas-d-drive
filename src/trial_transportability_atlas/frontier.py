# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Frontier evidence-state proof-of-concept built on the atlas outputs."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.context_join import enrich_trial_country_year_iso3
from trial_transportability_atlas.effect_candidates import _candidate_key
from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec


FRONTIER_CANDIDATE_COLUMNS = (
    "candidate_id",
    "nct_id",
    "candidate_family",
    "effect_model_hint",
    "row_count",
    "group_count",
    "pairwise_contrast_count",
    "comparable_flag",
    "comparability_reason",
    "event_numerator_row_count",
    "denominator_row_count",
    "value_num_row_count",
    "dispersion_value_row_count",
    "latent_recovery_flag",
    "recoverable_effect_flag",
    "precision_ready_flag",
    "signed_effect_recovered_flag",
    "signed_effect_surface_count",
    "signed_effect_precision_ready_count",
    "recoverability_source",
    "data_completeness_score",
    "purification_score",
)

FRONTIER_TRIAL_COLUMNS = (
    "nct_id",
    "frontier_candidate_count",
    "recoverable_candidate_count",
    "latent_recoverable_candidate_count",
    "recoverable_family_count",
    "signed_effect_candidate_count",
    "signed_effect_surface_count",
    "signed_effect_precision_candidate_count",
    "signed_effect_precision_surface_count",
    "recoverability_ratio",
    "latent_opportunity_ratio",
    "effect_realization_ratio",
    "purification_score_mean",
    "frontier_trial_score",
)

FRONTIER_COUNTRY_YEAR_COLUMNS = (
    "iso3",
    "country_name",
    "year",
    "frontier_trial_count",
    "frontier_candidate_count",
    "frontier_recoverable_candidate_count",
    "frontier_latent_candidate_count",
    "frontier_signed_effect_candidate_count",
    "frontier_signed_effect_surface_count",
    "frontier_signed_effect_precision_surface_count",
    "frontier_recoverability_ratio",
    "frontier_latent_opportunity_ratio",
    "frontier_effect_realization_ratio",
    "frontier_mean_trial_score",
    "frontier_purification_score",
    "frontier_status",
)

RECOVERABLE_FAMILIES = {
    "continuous_mean",
    "binary_participant_count",
    "binary_event_count",
}


def _event_numerator(series: pd.DataFrame) -> pd.Series:
    return (
        series["subjects_affected"]
        .combine_first(series["event_count"])
        .combine_first(series["value_num"])
    )


def _candidate_observed_surface(outcomes: pd.DataFrame) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame(
            columns=[
                "candidate_id",
                "observed_event_numerator_row_count",
                "observed_denominator_row_count",
                "observed_value_num_row_count",
                "observed_dispersion_value_row_count",
            ]
        )

    frame = outcomes.copy()
    frame["candidate_id"] = _candidate_key(frame)
    frame["event_numerator"] = _event_numerator(frame)
    frame["has_dispersion_value"] = frame["dispersion_value"].notna()

    observed = (
        frame.groupby("candidate_id", sort=True)
        .agg(
            observed_event_numerator_row_count=("event_numerator", lambda s: int(s.notna().sum())),
            observed_denominator_row_count=("subjects_at_risk", lambda s: int(s.notna().sum())),
            observed_value_num_row_count=("value_num", lambda s: int(s.notna().sum())),
            observed_dispersion_value_row_count=("has_dispersion_value", lambda s: int(s.sum())),
        )
        .reset_index()
    )
    return observed


def _family_completeness(row: pd.Series) -> float:
    row_count = float(row.get("row_count", 0) or 0)
    if row_count <= 0:
        return 0.0

    family = row.get("candidate_family")
    if family == "continuous_mean":
        return (
            (float(row.get("value_num_row_count", 0)) / row_count)
            + (float(row.get("dispersion_value_row_count", 0)) / row_count)
        ) / 2.0
    if family == "binary_event_count":
        return (
            (float(row.get("event_numerator_row_count", 0)) / row_count)
            + (float(row.get("denominator_row_count", 0)) / row_count)
        ) / 2.0
    if family == "binary_participant_count":
        return float(row.get("value_num_row_count", 0)) / row_count
    return 0.0


def _recoverability_source(row: pd.Series) -> str:
    if bool(row.get("comparable_flag")):
        return "existing_comparable"
    if bool(row.get("signed_effect_recovered_flag")):
        return "signed_surface_derivation"
    if bool(row.get("latent_recovery_flag")):
        if row.get("candidate_family") == "binary_event_count":
            return "subjects_affected_plus_denominator"
        return "latent_surface"
    return "not_recoverable"


def _signed_effect_summary(signed_effect_estimates: pd.DataFrame | None) -> pd.DataFrame:
    if signed_effect_estimates is None or signed_effect_estimates.empty:
        return pd.DataFrame(
            columns=[
                "candidate_id",
                "signed_effect_surface_count",
                "signed_effect_precision_ready_count",
            ]
        )

    summary = (
        signed_effect_estimates.groupby("candidate_id", sort=True)
        .agg(
            signed_effect_surface_count=("surface_id", "nunique"),
            signed_effect_precision_ready_count=("effect_precision", lambda s: int(s.notna().sum())),
        )
        .reset_index()
    )
    return summary


def _frontier_status(score: float, latent_ratio: float) -> str:
    if score >= 0.75 and latent_ratio < 0.10:
        return "purified"
    if latent_ratio >= 0.25:
        return "needs_effect_recovery"
    if score >= 0.50:
        return "emergent"
    return "frontier_gap"


def build_frontier_candidate_states(
    outcomes: pd.DataFrame,
    effect_candidates: pd.DataFrame,
    signed_effect_estimates: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Attach latent recoverability to strict effect candidates."""

    if effect_candidates.empty:
        return pd.DataFrame(columns=FRONTIER_CANDIDATE_COLUMNS)

    candidates = effect_candidates.copy()
    for column, default in (
        ("effect_model_hint", pd.NA),
        ("row_count", 0),
        ("group_count", 0),
        ("pairwise_contrast_count", 0),
        ("comparable_flag", False),
        ("comparability_reason", ""),
    ):
        if column not in candidates.columns:
            candidates[column] = default

    observed = _candidate_observed_surface(outcomes)
    states = candidates.merge(observed, how="left", on="candidate_id")
    states = states.merge(_signed_effect_summary(signed_effect_estimates), how="left", on="candidate_id")
    observed_columns = (
        "observed_event_numerator_row_count",
        "observed_denominator_row_count",
        "observed_value_num_row_count",
        "observed_dispersion_value_row_count",
    )
    for column in observed_columns:
        states[column] = pd.to_numeric(states[column], errors="coerce").fillna(0).astype(int)
    for column in (
        "signed_effect_surface_count",
        "signed_effect_precision_ready_count",
    ):
        states[column] = pd.to_numeric(states[column], errors="coerce").fillna(0).astype(int)

    states["event_numerator_row_count"] = states["observed_event_numerator_row_count"]
    states["denominator_row_count"] = states["observed_denominator_row_count"]
    states["value_num_row_count"] = states["observed_value_num_row_count"]
    states["dispersion_value_row_count"] = states["observed_dispersion_value_row_count"]

    states["comparable_flag"] = states["comparable_flag"].fillna(False).astype(bool)
    states["latent_recovery_flag"] = (
        states["candidate_family"].eq("binary_event_count")
        & states["group_count"].ge(2)
        & states["observed_event_numerator_row_count"].eq(states["row_count"])
        & states["observed_denominator_row_count"].eq(states["row_count"])
        & ~states["comparable_flag"]
    )
    states["signed_effect_recovered_flag"] = states["signed_effect_surface_count"].gt(0)
    states["recoverable_effect_flag"] = (
        states["comparable_flag"]
        | states["latent_recovery_flag"]
        | states["signed_effect_recovered_flag"]
    )
    states["data_completeness_score"] = states.apply(_family_completeness, axis=1)
    states["precision_ready_flag"] = (
        states["candidate_family"].eq("continuous_mean")
        & states["observed_value_num_row_count"].eq(states["row_count"])
        & states["observed_dispersion_value_row_count"].eq(states["row_count"])
    ) | (
        states["candidate_family"].eq("binary_event_count")
        & states["observed_event_numerator_row_count"].eq(states["row_count"])
        & states["observed_denominator_row_count"].eq(states["row_count"])
    ) | (
        states["candidate_family"].eq("binary_participant_count")
        & states["observed_value_num_row_count"].eq(states["row_count"])
    ) | states["signed_effect_precision_ready_count"].gt(0)
    states["precision_ready_flag"] = states["precision_ready_flag"].astype(bool)
    states["purification_score"] = (
        states["group_count"].ge(2).astype(float)
        + states["data_completeness_score"]
        + states["recoverable_effect_flag"].astype(float)
        + states["signed_effect_recovered_flag"].astype(float)
    ) / 4.0
    states["recoverability_source"] = states.apply(_recoverability_source, axis=1)

    return states[list(FRONTIER_CANDIDATE_COLUMNS)].sort_values(
        ["nct_id", "candidate_id"],
        kind="stable",
    ).reset_index(drop=True)


def build_frontier_trial_summary(candidate_states: pd.DataFrame) -> pd.DataFrame:
    """Aggregate frontier candidate states into a trial-level evidence surface."""

    if candidate_states.empty:
        return pd.DataFrame(columns=FRONTIER_TRIAL_COLUMNS)

    rows: list[dict[str, object]] = []
    for nct_id, group in candidate_states.groupby("nct_id", sort=True):
        recoverable = group.loc[group["recoverable_effect_flag"]]
        recoverable_family_count = recoverable["candidate_family"].isin(RECOVERABLE_FAMILIES)
        recoverable_family_count = int(
            recoverable.loc[recoverable_family_count, "candidate_family"].nunique()
        )
        candidate_count = int(len(group))
        recoverable_count = int(group["recoverable_effect_flag"].sum())
        latent_count = int(group["latent_recovery_flag"].sum())
        signed_effect_candidate_count = int(group["signed_effect_recovered_flag"].sum())
        signed_effect_surface_count = int(group["signed_effect_surface_count"].sum())
        signed_effect_precision_candidate_count = int(
            group["signed_effect_precision_ready_count"].gt(0).sum()
        )
        signed_effect_precision_surface_count = int(
            group["signed_effect_precision_ready_count"].sum()
        )
        recoverability_ratio = recoverable_count / candidate_count if candidate_count else 0.0
        latent_ratio = latent_count / candidate_count if candidate_count else 0.0
        effect_realization_ratio = (
            signed_effect_candidate_count / candidate_count if candidate_count else 0.0
        )
        purification_mean = float(group["purification_score"].mean()) if candidate_count else 0.0
        family_diversity_score = min(recoverable_family_count / 3.0, 1.0)
        frontier_trial_score = (
            recoverability_ratio + purification_mean + family_diversity_score
        ) / 3.0
        rows.append(
            {
                "nct_id": nct_id,
                "frontier_candidate_count": candidate_count,
                "recoverable_candidate_count": recoverable_count,
                "latent_recoverable_candidate_count": latent_count,
                "recoverable_family_count": recoverable_family_count,
                "signed_effect_candidate_count": signed_effect_candidate_count,
                "signed_effect_surface_count": signed_effect_surface_count,
                "signed_effect_precision_candidate_count": signed_effect_precision_candidate_count,
                "signed_effect_precision_surface_count": signed_effect_precision_surface_count,
                "recoverability_ratio": recoverability_ratio,
                "latent_opportunity_ratio": latent_ratio,
                "effect_realization_ratio": effect_realization_ratio,
                "purification_score_mean": purification_mean,
                "frontier_trial_score": frontier_trial_score,
            }
        )

    return pd.DataFrame.from_records(rows, columns=FRONTIER_TRIAL_COLUMNS).sort_values(
        ["frontier_trial_score", "nct_id"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def build_frontier_country_year(
    trial_country_year: pd.DataFrame,
    synthesis_output: pd.DataFrame,
    trial_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Join trial-level frontier states onto the atlas country-year surface."""

    if synthesis_output.empty:
        base_columns = list(FRONTIER_COUNTRY_YEAR_COLUMNS) + [
            "transportability_score",
            "country_coverage_score",
            "eligibility_support_score",
            "reporting_completeness_score",
        ]
        return pd.DataFrame(columns=base_columns)

    trial_rows = enrich_trial_country_year_iso3(trial_country_year)[
        ["nct_id", "country_name", "iso3_resolved", "year"]
    ].copy()
    merged_trials = trial_rows.merge(trial_summary, how="left", on="nct_id")
    fill_zero_columns = [
        "frontier_candidate_count",
        "recoverable_candidate_count",
        "latent_recoverable_candidate_count",
        "recoverable_family_count",
        "signed_effect_candidate_count",
        "signed_effect_surface_count",
        "signed_effect_precision_candidate_count",
        "signed_effect_precision_surface_count",
        "recoverability_ratio",
        "latent_opportunity_ratio",
        "effect_realization_ratio",
        "purification_score_mean",
        "frontier_trial_score",
    ]
    for column in fill_zero_columns:
        merged_trials[column] = merged_trials[column].fillna(0.0)

    grouped = (
        merged_trials.groupby(["iso3_resolved", "country_name", "year"], sort=True)
        .agg(
            frontier_trial_count=("nct_id", "nunique"),
            frontier_candidate_count=("frontier_candidate_count", "sum"),
            frontier_recoverable_candidate_count=("recoverable_candidate_count", "sum"),
            frontier_latent_candidate_count=("latent_recoverable_candidate_count", "sum"),
            frontier_signed_effect_candidate_count=("signed_effect_candidate_count", "sum"),
            frontier_signed_effect_surface_count=("signed_effect_surface_count", "sum"),
            frontier_signed_effect_precision_surface_count=("signed_effect_precision_surface_count", "sum"),
            frontier_mean_trial_score=("frontier_trial_score", "mean"),
        )
        .reset_index()
        .rename(columns={"iso3_resolved": "iso3"})
    )

    grouped["frontier_recoverability_ratio"] = (
        grouped["frontier_recoverable_candidate_count"] / grouped["frontier_candidate_count"]
    ).fillna(0.0)
    grouped["frontier_latent_opportunity_ratio"] = (
        grouped["frontier_latent_candidate_count"] / grouped["frontier_candidate_count"]
    ).fillna(0.0)
    grouped["frontier_effect_realization_ratio"] = (
        grouped["frontier_signed_effect_candidate_count"] / grouped["frontier_candidate_count"]
    )
    grouped["frontier_effect_realization_ratio"] = grouped["frontier_effect_realization_ratio"].fillna(0.0)

    synthesis_columns = [
        "iso3",
        "country_name",
        "year",
        "transportability_score",
        "country_coverage_score",
        "eligibility_support_score",
        "reporting_completeness_score",
    ]
    frontier = synthesis_output[synthesis_columns].merge(
        grouped,
        how="left",
        on=["iso3", "country_name", "year"],
    )
    fill_columns = [
        "frontier_trial_count",
        "frontier_candidate_count",
        "frontier_recoverable_candidate_count",
        "frontier_latent_candidate_count",
        "frontier_signed_effect_candidate_count",
        "frontier_signed_effect_surface_count",
        "frontier_signed_effect_precision_surface_count",
        "frontier_recoverability_ratio",
        "frontier_latent_opportunity_ratio",
        "frontier_effect_realization_ratio",
        "frontier_mean_trial_score",
    ]
    for column in fill_columns:
        frontier[column] = frontier[column].fillna(0.0)

    frontier["frontier_purification_score"] = frontier[
        [
            "country_coverage_score",
            "eligibility_support_score",
            "reporting_completeness_score",
            "frontier_mean_trial_score",
        ]
    ].mean(axis=1)
    frontier["frontier_status"] = frontier.apply(
        lambda row: _frontier_status(
            float(row["frontier_purification_score"]),
            float(row["frontier_latent_opportunity_ratio"]),
        ),
        axis=1,
    )

    ordered_columns = list(FRONTIER_COUNTRY_YEAR_COLUMNS) + [
        "transportability_score",
        "country_coverage_score",
        "eligibility_support_score",
        "reporting_completeness_score",
    ]
    return frontier[ordered_columns].sort_values(
        ["frontier_purification_score", "country_name", "year"],
        ascending=[False, True, True],
        kind="stable",
    ).reset_index(drop=True)


def render_frontier_report_markdown(
    candidate_states: pd.DataFrame,
    trial_summary: pd.DataFrame,
    frontier_country_year: pd.DataFrame,
    *,
    topic: TopicSpec,
) -> str:
    """Render a compact Markdown summary for the proof-of-concept."""

    total_candidates = int(len(candidate_states))
    recoverable_candidates = int(candidate_states["recoverable_effect_flag"].sum()) if not candidate_states.empty else 0
    latent_candidates = int(candidate_states["latent_recovery_flag"].sum()) if not candidate_states.empty else 0
    signed_effect_candidates = int(candidate_states["signed_effect_recovered_flag"].sum()) if not candidate_states.empty else 0
    signed_effect_surfaces = int(candidate_states["signed_effect_surface_count"].sum()) if not candidate_states.empty else 0
    top_country_rows = frontier_country_year.head(10)
    top_latent_rows = frontier_country_year.sort_values(
        ["frontier_latent_opportunity_ratio", "country_name", "year"],
        ascending=[False, True, True],
        kind="stable",
    ).head(10)

    lines = [
        f"# Frontier Evidence Manifold: {topic.slug}",
        "",
        "This proof-of-concept treats meta-analysis as an evidence-state problem rather than a pooling-only problem.",
        f"- total_candidates: {total_candidates}",
        f"- recoverable_candidates: {recoverable_candidates}",
        f"- latent_recoverable_candidates: {latent_candidates}",
        f"- signed_effect_candidates: {signed_effect_candidates}",
        f"- signed_effect_surfaces: {signed_effect_surfaces}",
        f"- frontier_trials: {int(len(trial_summary))}",
        f"- frontier_country_year_rows: {int(len(frontier_country_year))}",
        "",
        "## Highest frontier purification scores",
        "",
        "| Country | Year | Frontier score | Latent opportunity | Effect realization | Status |",
        "|---|---:|---:|---:|---:|---|",
    ]

    for row in top_country_rows.itertuples(index=False):
        lines.append(
            f"| {row.country_name} | {int(row.year)} | {float(row.frontier_purification_score):.3f} | "
            f"{float(row.frontier_latent_opportunity_ratio):.3f} | "
            f"{float(row.frontier_effect_realization_ratio):.3f} | {row.frontier_status} |"
        )

    lines.extend(
        [
            "",
            "## Highest latent opportunity ratios",
            "",
            "| Country | Year | Latent opportunity | Effect realization | Frontier score | Status |",
            "|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in top_latent_rows.itertuples(index=False):
        lines.append(
            f"| {row.country_name} | {int(row.year)} | {float(row.frontier_latent_opportunity_ratio):.3f} | "
            f"{float(row.frontier_effect_realization_ratio):.3f} | "
            f"{float(row.frontier_purification_score):.3f} | {row.frontier_status} |"
        )

    return "\n".join(lines) + "\n"


def materialize_frontier_outputs(
    output_dir: Path | str,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write frontier proof-of-concept outputs from existing atlas artifacts."""

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    output_path = Path(output_dir)

    trial_outcomes = pd.read_parquet(output_path / "trial_outcomes_long.parquet")
    effect_candidates = pd.read_parquet(output_path / "effect_candidates.parquet")
    trial_country_year = pd.read_parquet(output_path / "trial_country_year.parquet")
    synthesis_output = pd.read_parquet(output_path / "synthesis_output.parquet")
    signed_effects_path = output_path / "signed_effect_estimates.parquet"
    signed_effect_estimates = (
        pd.read_parquet(signed_effects_path)
        if signed_effects_path.exists()
        else None
    )

    candidate_states = build_frontier_candidate_states(
        trial_outcomes,
        effect_candidates,
        signed_effect_estimates=signed_effect_estimates,
    )
    trial_summary = build_frontier_trial_summary(candidate_states)
    frontier_country_year = build_frontier_country_year(
        trial_country_year=trial_country_year,
        synthesis_output=synthesis_output,
        trial_summary=trial_summary,
    )
    report = render_frontier_report_markdown(
        candidate_states,
        trial_summary,
        frontier_country_year,
        topic=topic_spec,
    )

    candidate_path = output_path / "frontier_candidate_states.parquet"
    trial_path = output_path / "frontier_trial_summary.parquet"
    country_year_path = output_path / "frontier_country_year.parquet"
    report_path = output_path / "frontier_report.md"
    manifest_path = output_path / "frontier_manifest.json"

    candidate_states.to_parquet(candidate_path, index=False)
    trial_summary.to_parquet(trial_path, index=False)
    frontier_country_year.to_parquet(country_year_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    source_manifest_id = ""
    if not synthesis_output.empty and "source_manifest_id" in synthesis_output.columns:
        source_manifest_id = next(iter(synthesis_output["source_manifest_id"].dropna().astype(str)), "")
    manifest_seed = {
        "topic_slug": topic_spec.slug,
        "source_manifest_id": source_manifest_id,
        "candidate_rows": int(len(candidate_states)),
        "trial_rows": int(len(trial_summary)),
        "country_year_rows": int(len(frontier_country_year)),
        "latent_recoverable_candidate_count": int(candidate_states["latent_recovery_flag"].sum()) if not candidate_states.empty else 0,
        "signed_effect_candidate_count": int(candidate_states["signed_effect_recovered_flag"].sum()) if not candidate_states.empty else 0,
        "signed_effect_surface_count": int(candidate_states["signed_effect_surface_count"].sum()) if not candidate_states.empty else 0,
    }
    manifest = {
        **manifest_seed,
        "frontier_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "recoverable_candidate_count": int(candidate_states["recoverable_effect_flag"].sum()) if not candidate_states.empty else 0,
        "max_frontier_purification_score": float(frontier_country_year["frontier_purification_score"].max()) if not frontier_country_year.empty else 0.0,
        "outputs": {
            "frontier_candidate_states": str(candidate_path),
            "frontier_trial_summary": str(trial_path),
            "frontier_country_year": str(country_year_path),
            "frontier_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
