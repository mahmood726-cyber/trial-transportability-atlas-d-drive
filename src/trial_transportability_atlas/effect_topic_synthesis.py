# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Strict topic-level synthesis across trial-local family pools."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec


TOPIC_SYNTHESIS_COLUMNS = (
    "topic_family_id",
    "family_label",
    "family_domain",
    "analysis_population",
    "candidate_family",
    "effect_measure",
    "estimand_type",
    "effect_direction",
    "trial_count",
    "family_pool_count",
    "precision_ready_family_pool_count",
    "canonical_effect_count",
    "raw_surface_count",
    "nct_ids",
    "outcome_name_examples",
    "derivation_methods",
    "surface_value_min",
    "surface_value_median",
    "surface_value_max",
    "topic_effect_iqr",
    "topic_effect_mad_sd",
    "topic_effect_value",
    "topic_effect_precision",
    "precision_metric",
    "synthesis_method",
    "synthesis_status",
    "sign_coherence",
)

TOPIC_RECURRENCE_COLUMNS = (
    "family_label",
    "family_domain",
    "candidate_family",
    "effect_measure",
    "estimand_type",
    "effect_direction",
    "trial_count",
    "family_pool_count",
    "analysis_population_count",
    "strict_manifold_count",
    "multi_trial_manifold_count",
    "analysis_population_examples",
    "nct_ids",
    "recurrence_status",
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


def _split_join_unique(values: pd.Series, limit: int | None = None) -> str:
    unique: set[str] = set()
    for value in values:
        if value is None or pd.isna(value):
            continue
        parts = [part.strip() for part in str(value).split(";")]
        unique.update(part for part in parts if part)
    ordered = sorted(unique)
    if limit is not None:
        ordered = ordered[:limit]
    return ";".join(ordered)


def _iqr(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(numeric) <= 1:
        return 0.0
    return float(numeric.quantile(0.75) - numeric.quantile(0.25))


def _mad_sd(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if len(numeric) <= 1:
        return 0.0
    median_value = float(numeric.median())
    mad = float((numeric - median_value).abs().median())
    return mad * 1.4826


def _sign_coherence(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if numeric.empty:
        return 0.0
    positive = float((numeric > 0).mean())
    negative = float((numeric < 0).mean())
    zero = float((numeric == 0).mean())
    return max(positive, negative, zero)


def build_topic_family_synthesis(
    family_pooled_effects: pd.DataFrame,
) -> pd.DataFrame:
    """Pool trial-local family effects into strict topic-level manifolds."""

    if family_pooled_effects.empty:
        return pd.DataFrame(columns=TOPIC_SYNTHESIS_COLUMNS)

    group_columns = [
        "family_label",
        "family_domain",
        "analysis_population",
        "candidate_family",
        "effect_measure",
        "estimand_type",
        "effect_direction",
    ]
    rows: list[dict[str, object]] = []

    for group_key, group in family_pooled_effects.groupby(group_columns, sort=True, dropna=False):
        (
            family_label,
            family_domain,
            analysis_population,
            candidate_family,
            effect_measure,
            estimand_type,
            effect_direction,
        ) = group_key

        values = pd.to_numeric(group["family_effect_value"], errors="coerce").dropna().astype(float)
        if values.empty:
            continue
        precisions = pd.to_numeric(group["family_effect_precision"], errors="coerce").dropna().astype(float)
        trial_count = int(group["nct_id"].nunique())
        family_pool_count = int(len(group))
        precision_ready_count = int(group["family_effect_precision"].notna().sum())
        canonical_effect_count = int(group["canonical_effect_count"].sum())
        raw_surface_count = int(group["raw_surface_count"].sum())
        topic_effect_value = float(values.median())
        topic_effect_iqr = _iqr(values)
        topic_effect_mad_sd = _mad_sd(values)

        if trial_count == 1 and family_pool_count == 1:
            topic_effect_precision = next(iter(precisions), pd.NA)
            precision_metric = next(iter(group["precision_metric"].dropna()), pd.NA)
            synthesis_method = "single_trial_identity"
            synthesis_status = (
                "single_trial_precision_retained"
                if not precisions.empty
                else "single_trial_no_precision"
            )
        elif precisions.empty:
            topic_effect_precision = pd.NA
            precision_metric = pd.NA
            synthesis_method = "topic_family_median_no_precision"
            synthesis_status = "no_precision_ready_trial_family"
        else:
            median_precision = float(precisions.median())
            topic_effect_precision = max(median_precision, topic_effect_mad_sd)
            precision_metric = _split_join_unique(group["precision_metric"])
            if topic_effect_mad_sd > median_precision + 1e-12:
                synthesis_method = "topic_family_median_with_dispersion_guardrail"
                synthesis_status = "dispersion_guardrail_applied"
            else:
                synthesis_method = "topic_family_median_no_precision_gain"
                synthesis_status = "median_precision_retained"

        seed = {
            "family_label": family_label,
            "analysis_population": analysis_population,
            "effect_measure": effect_measure,
            "estimand_type": estimand_type,
            "effect_direction": effect_direction,
        }
        rows.append(
            {
                "topic_family_id": hashlib.sha256(
                    json.dumps(seed, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "family_label": family_label,
                "family_domain": family_domain,
                "analysis_population": analysis_population,
                "candidate_family": candidate_family,
                "effect_measure": effect_measure,
                "estimand_type": estimand_type,
                "effect_direction": effect_direction,
                "trial_count": trial_count,
                "family_pool_count": family_pool_count,
                "precision_ready_family_pool_count": precision_ready_count,
                "canonical_effect_count": canonical_effect_count,
                "raw_surface_count": raw_surface_count,
                "nct_ids": _join_unique(group["nct_id"]),
                "outcome_name_examples": _split_join_unique(group["outcome_name_examples"], limit=8),
                "derivation_methods": _split_join_unique(group["derivation_methods"]),
                "surface_value_min": float(values.min()),
                "surface_value_median": topic_effect_value,
                "surface_value_max": float(values.max()),
                "topic_effect_iqr": topic_effect_iqr,
                "topic_effect_mad_sd": topic_effect_mad_sd,
                "topic_effect_value": topic_effect_value,
                "topic_effect_precision": topic_effect_precision,
                "precision_metric": precision_metric,
                "synthesis_method": synthesis_method,
                "synthesis_status": synthesis_status,
                "sign_coherence": _sign_coherence(values),
            }
        )

    if not rows:
        return pd.DataFrame(columns=TOPIC_SYNTHESIS_COLUMNS)

    synthesis = pd.DataFrame.from_records(rows, columns=TOPIC_SYNTHESIS_COLUMNS)
    return synthesis.sort_values(
        ["trial_count", "family_label", "analysis_population", "estimand_type"],
        ascending=[False, True, True, True],
        kind="stable",
    ).reset_index(drop=True)


def build_topic_family_recurrence_summary(
    family_pooled_effects: pd.DataFrame,
    topic_family_synthesis: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize broader family recurrence even when strict pooling fails closed."""

    if family_pooled_effects.empty:
        return pd.DataFrame(columns=TOPIC_RECURRENCE_COLUMNS)

    loose_group_columns = [
        "family_label",
        "family_domain",
        "candidate_family",
        "effect_measure",
        "estimand_type",
        "effect_direction",
    ]
    rows: list[dict[str, object]] = []

    for group_key, group in family_pooled_effects.groupby(loose_group_columns, sort=True, dropna=False):
        (
            family_label,
            family_domain,
            candidate_family,
            effect_measure,
            estimand_type,
            effect_direction,
        ) = group_key

        synthesis_match = topic_family_synthesis.loc[
            (topic_family_synthesis["family_label"] == family_label)
            & (topic_family_synthesis["family_domain"] == family_domain)
            & (topic_family_synthesis["candidate_family"] == candidate_family)
            & (topic_family_synthesis["effect_measure"] == effect_measure)
            & (topic_family_synthesis["estimand_type"] == estimand_type)
            & (topic_family_synthesis["effect_direction"] == effect_direction)
        ]

        trial_count = int(group["nct_id"].nunique())
        family_pool_count = int(len(group))
        analysis_population_count = int(group["analysis_population"].nunique(dropna=False))
        strict_manifold_count = int(len(synthesis_match))
        multi_trial_manifold_count = int((synthesis_match["trial_count"] >= 2).sum())

        if multi_trial_manifold_count >= 1:
            recurrence_status = "strict_cross_trial_manifold_present"
        elif trial_count >= 2:
            recurrence_status = "stratified_no_strict_overlap"
        else:
            recurrence_status = "single_trial_only"

        rows.append(
            {
                "family_label": family_label,
                "family_domain": family_domain,
                "candidate_family": candidate_family,
                "effect_measure": effect_measure,
                "estimand_type": estimand_type,
                "effect_direction": effect_direction,
                "trial_count": trial_count,
                "family_pool_count": family_pool_count,
                "analysis_population_count": analysis_population_count,
                "strict_manifold_count": strict_manifold_count,
                "multi_trial_manifold_count": multi_trial_manifold_count,
                "analysis_population_examples": _split_join_unique(group["analysis_population"], limit=8),
                "nct_ids": _join_unique(group["nct_id"]),
                "recurrence_status": recurrence_status,
            }
        )

    summary = pd.DataFrame.from_records(rows, columns=TOPIC_RECURRENCE_COLUMNS)
    return summary.sort_values(
        ["trial_count", "family_pool_count", "family_label"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def render_topic_family_synthesis_report(
    topic_family_synthesis: pd.DataFrame,
    recurrence_summary: pd.DataFrame,
    *,
    topic: TopicSpec,
) -> str:
    """Render a compact Markdown report for strict topic-level synthesis."""

    multi_trial = topic_family_synthesis.loc[topic_family_synthesis["trial_count"] >= 2]
    lines = [
        f"# Topic Family Synthesis: {topic.slug}",
        "",
        "This proof-of-concept pools family effects only when family, population, estimand, and effect measure align exactly.",
        f"- topic_synthesis_count: {int(len(topic_family_synthesis))}",
        f"- multi_trial_manifold_count: {int(len(multi_trial))}",
        f"- single_trial_identity_count: {int((topic_family_synthesis['trial_count'] == 1).sum()) if not topic_family_synthesis.empty else 0}",
        f"- recurring_family_label_count: {int((recurrence_summary['trial_count'] >= 2).sum()) if not recurrence_summary.empty else 0}",
        "",
    ]

    if multi_trial.empty:
        lines.append("No strict cross-trial manifolds were recoverable from the current family pools.")
        lines.append("")

    lines.extend(
        [
            "## Strict topic manifolds",
            "",
            "| Family | Population | Trials | Family pools | Topic effect | Precision | Status |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in topic_family_synthesis.head(12).itertuples(index=False):
        precision_text = (
            f"{float(row.topic_effect_precision):.4f}"
            if pd.notna(row.topic_effect_precision)
            else "NA"
        )
        lines.append(
            f"| {row.family_label} | {row.analysis_population} | {int(row.trial_count)} | "
            f"{int(row.family_pool_count)} | {float(row.topic_effect_value):.4f} | "
            f"{precision_text} | {row.synthesis_status} |"
        )

    lines.extend(
        [
            "",
            "## Broader recurrence",
            "",
            "| Family | Trials | Populations | Strict manifolds | Multi-trial manifolds | Status |",
            "|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in recurrence_summary.head(12).itertuples(index=False):
        lines.append(
            f"| {row.family_label} | {int(row.trial_count)} | {int(row.analysis_population_count)} | "
            f"{int(row.strict_manifold_count)} | {int(row.multi_trial_manifold_count)} | "
            f"{row.recurrence_status} |"
        )

    return "\n".join(lines) + "\n"


def materialize_topic_family_synthesis_outputs(
    output_dir: Path | str,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write strict topic-level synthesis outputs from family-pooled artifacts."""

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    output_path = Path(output_dir)
    family_pooled_effects = pd.read_parquet(output_path / "family_pooled_effects.parquet")

    synthesis = build_topic_family_synthesis(family_pooled_effects)
    recurrence_summary = build_topic_family_recurrence_summary(family_pooled_effects, synthesis)
    report = render_topic_family_synthesis_report(
        synthesis,
        recurrence_summary,
        topic=topic_spec,
    )

    synthesis_path = output_path / "topic_family_synthesis.parquet"
    recurrence_path = output_path / "topic_family_recurrence.parquet"
    report_path = output_path / "topic_family_synthesis_report.md"
    manifest_path = output_path / "topic_family_synthesis_manifest.json"

    synthesis.to_parquet(synthesis_path, index=False)
    recurrence_summary.to_parquet(recurrence_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    manifest_seed = {
        "topic_slug": topic_spec.slug,
        "topic_synthesis_count": int(len(synthesis)),
        "source_family_pool_count": int(len(family_pooled_effects)),
        "multi_trial_manifold_count": int((synthesis["trial_count"] >= 2).sum()) if not synthesis.empty else 0,
        "precision_ready_topic_synthesis_count": int(synthesis["topic_effect_precision"].notna().sum()) if not synthesis.empty else 0,
    }
    manifest = {
        **manifest_seed,
        "topic_family_synthesis_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "mapped_trial_count": int(family_pooled_effects["nct_id"].nunique()) if not family_pooled_effects.empty else 0,
        "single_trial_identity_count": int((synthesis["trial_count"] == 1).sum()) if not synthesis.empty else 0,
        "recurring_family_label_count": int((recurrence_summary["trial_count"] >= 2).sum()) if not recurrence_summary.empty else 0,
        "stratified_recurrence_count": int((recurrence_summary["recurrence_status"] == "stratified_no_strict_overlap").sum()) if not recurrence_summary.empty else 0,
        "outputs": {
            "topic_family_synthesis": str(synthesis_path),
            "topic_family_recurrence": str(recurrence_path),
            "topic_family_synthesis_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
