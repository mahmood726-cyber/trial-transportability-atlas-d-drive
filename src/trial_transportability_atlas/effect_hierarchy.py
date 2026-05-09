# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Conservative hierarchy collapse for signed-effect surfaces."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec


CANONICAL_EFFECT_COLUMNS = (
    "canonical_effect_id",
    "candidate_id",
    "nct_id",
    "candidate_family",
    "outcome_name",
    "time_frame",
    "analysis_population",
    "effect_measure",
    "estimand_type",
    "effect_direction",
    "surface_count",
    "precision_ready_surface_count",
    "surface_label_count",
    "surface_label_examples",
    "derivation_methods",
    "surface_value_min",
    "surface_value_median",
    "surface_value_max",
    "surface_effect_iqr",
    "surface_effect_mad_sd",
    "canonical_effect_value",
    "canonical_effect_precision",
    "precision_metric",
    "collapse_method",
    "collapse_status",
    "treatment_arm_title",
    "comparator_arm_title",
    "treatment_intervention_names",
    "comparator_intervention_names",
)

CANONICAL_TRIAL_COLUMNS = (
    "nct_id",
    "canonical_effect_count",
    "precision_ready_canonical_effect_count",
    "raw_surface_count",
    "surface_contraction_ratio",
    "mean_surface_count_per_canonical_effect",
    "effect_measures",
    "estimand_types",
    "candidate_families",
)


def _join_unique(values: pd.Series) -> str:
    unique = sorted(
        {
            str(value).strip()
            for value in values
            if value is not None and not pd.isna(value) and str(value).strip()
        }
    )
    return ";".join(unique)


def _join_limited(values: pd.Series, limit: int = 6) -> str:
    unique = sorted(
        {
            str(value).strip()
            for value in values
            if value is not None and not pd.isna(value) and str(value).strip()
        }
    )
    return ";".join(unique[:limit])


def _surface_iqr(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(numeric) <= 1:
        return 0.0
    return float(numeric.quantile(0.75) - numeric.quantile(0.25))


def _surface_mad_sd(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(numeric) <= 1:
        return 0.0
    median_value = float(numeric.median())
    mad = float((numeric - median_value).abs().median())
    return mad * 1.4826


def build_canonical_effect_estimates(
    signed_effect_estimates: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse repeated signed surfaces into conservative canonical effects."""

    if signed_effect_estimates.empty:
        return pd.DataFrame(columns=CANONICAL_EFFECT_COLUMNS)

    estimates = signed_effect_estimates.copy()
    group_columns = [
        "candidate_id",
        "nct_id",
        "candidate_family",
        "outcome_name",
        "time_frame",
        "analysis_population",
        "effect_measure",
        "estimand_type",
        "effect_direction",
    ]

    rows: list[dict[str, object]] = []
    for group_key, group in estimates.groupby(group_columns, sort=True, dropna=False):
        (
            candidate_id,
            nct_id,
            candidate_family,
            outcome_name,
            time_frame,
            analysis_population,
            effect_measure,
            estimand_type,
            effect_direction,
        ) = group_key

        values = pd.to_numeric(group["effect_value"], errors="coerce").dropna().astype(float)
        if values.empty:
            continue
        ready = pd.to_numeric(group["effect_precision"], errors="coerce").dropna().astype(float)
        surface_count = int(group["surface_id"].nunique())
        precision_ready_surface_count = int(ready.notna().sum())
        canonical_effect_value = float(values.median())
        surface_value_min = float(values.min())
        surface_value_max = float(values.max())
        surface_effect_iqr = _surface_iqr(values)
        surface_effect_mad_sd = _surface_mad_sd(values)

        if surface_count == 1:
            canonical_effect_precision = next(iter(ready), pd.NA) if precision_ready_surface_count == 1 else pd.NA
            precision_metric = next(iter(group["precision_metric"].dropna()), pd.NA)
            collapse_method = "single_surface_identity"
            collapse_status = (
                "single_surface_precision_retained"
                if precision_ready_surface_count == 1
                else "single_surface_no_precision"
            )
        elif precision_ready_surface_count == 0:
            canonical_effect_precision = pd.NA
            precision_metric = pd.NA
            collapse_method = "candidate_median_no_precision"
            collapse_status = "no_precision_ready_surface"
        else:
            base_precision = float(ready.median())
            canonical_effect_precision = max(base_precision, surface_effect_mad_sd)
            precision_metric = _join_unique(group["precision_metric"])
            if surface_effect_mad_sd > base_precision + 1e-12:
                collapse_method = "candidate_median_with_dispersion_guardrail"
                collapse_status = "dispersion_guardrail_applied"
            else:
                collapse_method = "candidate_median_no_precision_gain"
                collapse_status = "median_precision_retained"

        seed = {
            "candidate_id": candidate_id,
            "nct_id": nct_id,
            "analysis_population": analysis_population,
            "effect_measure": effect_measure,
            "estimand_type": estimand_type,
        }
        rows.append(
            {
                "canonical_effect_id": hashlib.sha256(
                    json.dumps(seed, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "candidate_id": candidate_id,
                "nct_id": nct_id,
                "candidate_family": candidate_family,
                "outcome_name": outcome_name,
                "time_frame": time_frame,
                "analysis_population": analysis_population,
                "effect_measure": effect_measure,
                "estimand_type": estimand_type,
                "effect_direction": effect_direction,
                "surface_count": surface_count,
                "precision_ready_surface_count": precision_ready_surface_count,
                "surface_label_count": int(group["surface_label"].nunique()),
                "surface_label_examples": _join_limited(group["surface_label"]),
                "derivation_methods": _join_unique(group["derivation_method"]),
                "surface_value_min": surface_value_min,
                "surface_value_median": canonical_effect_value,
                "surface_value_max": surface_value_max,
                "surface_effect_iqr": surface_effect_iqr,
                "surface_effect_mad_sd": surface_effect_mad_sd,
                "canonical_effect_value": canonical_effect_value,
                "canonical_effect_precision": canonical_effect_precision,
                "precision_metric": precision_metric,
                "collapse_method": collapse_method,
                "collapse_status": collapse_status,
                "treatment_arm_title": _join_unique(group["treatment_arm_title"]),
                "comparator_arm_title": _join_unique(group["comparator_arm_title"]),
                "treatment_intervention_names": _join_unique(group["treatment_intervention_names"]),
                "comparator_intervention_names": _join_unique(group["comparator_intervention_names"]),
            }
        )

    if not rows:
        return pd.DataFrame(columns=CANONICAL_EFFECT_COLUMNS)

    canonical = pd.DataFrame.from_records(rows, columns=CANONICAL_EFFECT_COLUMNS)
    return canonical.sort_values(
        ["nct_id", "candidate_family", "outcome_name", "estimand_type"],
        kind="stable",
    ).reset_index(drop=True)


def build_canonical_effect_trial_summary(
    canonical_effect_estimates: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate canonical effects to one summary row per trial."""

    if canonical_effect_estimates.empty:
        return pd.DataFrame(columns=CANONICAL_TRIAL_COLUMNS)

    rows: list[dict[str, object]] = []
    for nct_id, group in canonical_effect_estimates.groupby("nct_id", sort=True):
        canonical_effect_count = int(len(group))
        raw_surface_count = int(group["surface_count"].sum())
        precision_ready = group["canonical_effect_precision"].notna()
        rows.append(
            {
                "nct_id": nct_id,
                "canonical_effect_count": canonical_effect_count,
                "precision_ready_canonical_effect_count": int(precision_ready.sum()),
                "raw_surface_count": raw_surface_count,
                "surface_contraction_ratio": (
                    canonical_effect_count / raw_surface_count if raw_surface_count else 0.0
                ),
                "mean_surface_count_per_canonical_effect": (
                    raw_surface_count / canonical_effect_count if canonical_effect_count else 0.0
                ),
                "effect_measures": _join_unique(group["effect_measure"]),
                "estimand_types": _join_unique(group["estimand_type"]),
                "candidate_families": _join_unique(group["candidate_family"]),
            }
        )

    return pd.DataFrame.from_records(rows, columns=CANONICAL_TRIAL_COLUMNS).sort_values(
        ["raw_surface_count", "nct_id"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def render_canonical_effect_report(
    canonical_effect_estimates: pd.DataFrame,
    trial_summary: pd.DataFrame,
    *,
    topic: TopicSpec,
) -> str:
    """Render a compact Markdown report for hierarchy-aware collapse."""

    raw_surface_count = int(canonical_effect_estimates["surface_count"].sum()) if not canonical_effect_estimates.empty else 0
    precision_ready_canonical = int(canonical_effect_estimates["canonical_effect_precision"].notna().sum()) if not canonical_effect_estimates.empty else 0
    top_multi = canonical_effect_estimates.sort_values(
        ["surface_count", "nct_id", "outcome_name"],
        ascending=[False, True, True],
        kind="stable",
    ).head(12)

    lines = [
        f"# Canonical Effect Hierarchy: {topic.slug}",
        "",
        "This proof-of-concept collapses repeated signed surfaces into conservative canonical effects.",
        f"- canonical_effect_count: {int(len(canonical_effect_estimates))}",
        f"- raw_surface_count: {raw_surface_count}",
        f"- precision_ready_canonical_effect_count: {precision_ready_canonical}",
        f"- mapped_trial_count: {int(len(trial_summary))}",
        "",
        "## Trial contraction",
        "",
        "| NCT ID | Canonical effects | Raw surfaces | Contraction ratio | Mean surfaces per effect |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in trial_summary.head(10).itertuples(index=False):
        lines.append(
            f"| {row.nct_id} | {int(row.canonical_effect_count)} | {int(row.raw_surface_count)} | "
            f"{float(row.surface_contraction_ratio):.3f} | {float(row.mean_surface_count_per_canonical_effect):.2f} |"
        )

    lines.extend(
        [
            "",
            "## Highest multi-surface canonical effects",
            "",
            "| NCT ID | Outcome | Estimand | Surfaces | Canonical effect | Canonical precision | Status |",
            "|---|---|---|---:|---:|---:|---|",
        ]
    )
    for row in top_multi.itertuples(index=False):
        precision_text = (
            f"{float(row.canonical_effect_precision):.4f}"
            if pd.notna(row.canonical_effect_precision)
            else "NA"
        )
        lines.append(
            f"| {row.nct_id} | {row.outcome_name} | {row.estimand_type} | "
            f"{int(row.surface_count)} | {float(row.canonical_effect_value):.4f} | "
            f"{precision_text} | {row.collapse_status} |"
        )

    return "\n".join(lines) + "\n"


def materialize_canonical_effect_outputs(
    output_dir: Path | str,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write hierarchy-aware canonical effect outputs from signed-effect artifacts."""

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    output_path = Path(output_dir)
    signed_effect_estimates = pd.read_parquet(output_path / "signed_effect_estimates.parquet")

    canonical = build_canonical_effect_estimates(signed_effect_estimates)
    trial_summary = build_canonical_effect_trial_summary(canonical)
    report = render_canonical_effect_report(canonical, trial_summary, topic=topic_spec)

    canonical_path = output_path / "canonical_effect_estimates.parquet"
    trial_summary_path = output_path / "canonical_effect_trial_summary.parquet"
    report_path = output_path / "canonical_effect_report.md"
    manifest_path = output_path / "canonical_effect_manifest.json"

    canonical.to_parquet(canonical_path, index=False)
    trial_summary.to_parquet(trial_summary_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    raw_surface_count = int(canonical["surface_count"].sum()) if not canonical.empty else 0
    manifest_seed = {
        "topic_slug": topic_spec.slug,
        "canonical_effect_count": int(len(canonical)),
        "raw_surface_count": raw_surface_count,
        "precision_ready_canonical_effect_count": int(canonical["canonical_effect_precision"].notna().sum()) if not canonical.empty else 0,
    }
    manifest = {
        **manifest_seed,
        "canonical_effect_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "mapped_trial_count": int(len(trial_summary)),
        "surface_contraction_ratio": (
            float(len(canonical)) / raw_surface_count if raw_surface_count else 0.0
        ),
        "outputs": {
            "canonical_effect_estimates": str(canonical_path),
            "canonical_effect_trial_summary": str(trial_summary_path),
            "canonical_effect_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
