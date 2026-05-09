# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Explicit family-level pooling for canonical effect surfaces."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

import pandas as pd

from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec


FAMILY_POOLED_COLUMNS = (
    "family_pool_id",
    "nct_id",
    "family_label",
    "family_domain",
    "analysis_population",
    "candidate_family",
    "effect_measure",
    "estimand_type",
    "effect_direction",
    "canonical_effect_count",
    "precision_ready_canonical_effect_count",
    "raw_surface_count",
    "outcome_name_count",
    "candidate_id_count",
    "outcome_name_examples",
    "derivation_methods",
    "surface_value_min",
    "surface_value_median",
    "surface_value_max",
    "family_effect_iqr",
    "family_effect_mad_sd",
    "family_effect_value",
    "family_effect_precision",
    "precision_metric",
    "pooling_method",
    "pooling_status",
    "sign_coherence",
)

FAMILY_TRIAL_COLUMNS = (
    "nct_id",
    "family_pool_count",
    "precision_ready_family_pool_count",
    "canonical_effect_count",
    "raw_surface_count",
    "family_contraction_ratio",
    "mean_canonical_effects_per_family",
    "mean_surfaces_per_family",
    "family_labels",
    "effect_measures",
    "estimand_types",
)

_MULTISPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+")


def _normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = _PUNCT_RE.sub(" ", str(value).casefold())
    return _MULTISPACE_RE.sub(" ", text).strip()


def _slug(value: object, default: str = "unspecified") -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return default
    return normalized.replace(" ", "_")


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


def classify_effect_family(row: pd.Series) -> tuple[str, str]:
    """Return (family_label, family_domain) for one canonical effect row."""

    candidate_family = str(row.get("candidate_family") or "")
    outcome_name = _normalize_text(row.get("outcome_name"))

    if candidate_family == "binary_event_count":
        seriousness = _slug(row.get("surface_label_examples"), default="overall")
        return f"adverse_event_risk_{seriousness}", "adverse_event_risk"

    if candidate_family == "binary_participant_count":
        if "pga" in outcome_name or "patient global assessment" in outcome_name:
            return "symptom_responder_risk", "symptom_responder_risk"
        if "6mwt" in outcome_name or "six minute walk" in outcome_name:
            return "walk_responder_risk", "walk_responder_risk"
        if "physical activity" in outcome_name or "non sedentary" in outcome_name:
            return "activity_responder_risk", "activity_responder_risk"
        return "participant_responder_risk", "participant_responder_risk"

    if candidate_family == "continuous_mean":
        if "6mwt" in outcome_name or "six minute walk" in outcome_name:
            return "walk_performance", "walk_performance"
        if "erectile function" in outcome_name or "iief" in outcome_name:
            return "sexual_function", "sexual_function"
        if "sexual activity" in outcome_name:
            return "sexual_activity", "sexual_activity"
        if (
            "physical activity" in outcome_name
            or "non sedentary" in outcome_name
            or "weekly time spent" in outcome_name
        ):
            return "activity_volume", "activity_volume"
        return "continuous_other", "continuous_other"

    return f"{_slug(candidate_family)}_other", f"{_slug(candidate_family)}_other"


def build_family_pooled_effects(
    canonical_effect_estimates: pd.DataFrame,
) -> pd.DataFrame:
    """Pool canonical effects within explicit trial-local families."""

    if canonical_effect_estimates.empty:
        return pd.DataFrame(columns=FAMILY_POOLED_COLUMNS)

    frame = canonical_effect_estimates.copy()
    family_pairs = frame.apply(classify_effect_family, axis=1, result_type="expand")
    family_pairs.columns = ["family_label", "family_domain"]
    frame = pd.concat([frame, family_pairs], axis=1)

    group_columns = [
        "nct_id",
        "family_label",
        "family_domain",
        "analysis_population",
        "candidate_family",
        "effect_measure",
        "estimand_type",
        "effect_direction",
    ]

    rows: list[dict[str, object]] = []
    for group_key, group in frame.groupby(group_columns, sort=True, dropna=False):
        (
            nct_id,
            family_label,
            family_domain,
            analysis_population,
            candidate_family,
            effect_measure,
            estimand_type,
            effect_direction,
        ) = group_key

        values = pd.to_numeric(group["canonical_effect_value"], errors="coerce").dropna().astype(float)
        if values.empty:
            continue
        precisions = pd.to_numeric(group["canonical_effect_precision"], errors="coerce").dropna().astype(float)
        canonical_effect_count = int(len(group))
        raw_surface_count = int(group["raw_surface_count"].sum()) if "raw_surface_count" in group.columns else int(group["surface_count"].sum())
        precision_ready_count = int(group["canonical_effect_precision"].notna().sum())
        family_effect_value = float(values.median())
        family_effect_iqr = _iqr(values)
        family_effect_mad_sd = _mad_sd(values)

        if canonical_effect_count == 1:
            family_effect_precision = next(iter(precisions), pd.NA)
            precision_metric = next(iter(group["precision_metric"].dropna()), pd.NA)
            pooling_method = "single_canonical_identity"
            pooling_status = (
                "single_canonical_precision_retained"
                if not precisions.empty
                else "single_canonical_no_precision"
            )
        elif precisions.empty:
            family_effect_precision = pd.NA
            precision_metric = pd.NA
            pooling_method = "family_median_no_precision"
            pooling_status = "no_precision_ready_canonical_effect"
        else:
            median_precision = float(precisions.median())
            family_effect_precision = max(median_precision, family_effect_mad_sd)
            precision_metric = _join_unique(group["precision_metric"])
            if family_effect_mad_sd > median_precision + 1e-12:
                pooling_method = "family_median_with_dispersion_guardrail"
                pooling_status = "dispersion_guardrail_applied"
            else:
                pooling_method = "family_median_no_precision_gain"
                pooling_status = "median_precision_retained"

        seed = {
            "nct_id": nct_id,
            "family_label": family_label,
            "analysis_population": analysis_population,
            "effect_measure": effect_measure,
            "estimand_type": estimand_type,
        }
        rows.append(
            {
                "family_pool_id": hashlib.sha256(
                    json.dumps(seed, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "nct_id": nct_id,
                "family_label": family_label,
                "family_domain": family_domain,
                "analysis_population": analysis_population,
                "candidate_family": candidate_family,
                "effect_measure": effect_measure,
                "estimand_type": estimand_type,
                "effect_direction": effect_direction,
                "canonical_effect_count": canonical_effect_count,
                "precision_ready_canonical_effect_count": precision_ready_count,
                "raw_surface_count": raw_surface_count,
                "outcome_name_count": int(group["outcome_name"].nunique()),
                "candidate_id_count": int(group["candidate_id"].nunique()),
                "outcome_name_examples": _join_limited(group["outcome_name"]),
                "derivation_methods": _join_unique(group["derivation_methods"]),
                "surface_value_min": float(values.min()),
                "surface_value_median": family_effect_value,
                "surface_value_max": float(values.max()),
                "family_effect_iqr": family_effect_iqr,
                "family_effect_mad_sd": family_effect_mad_sd,
                "family_effect_value": family_effect_value,
                "family_effect_precision": family_effect_precision,
                "precision_metric": precision_metric,
                "pooling_method": pooling_method,
                "pooling_status": pooling_status,
                "sign_coherence": _sign_coherence(values),
            }
        )

    if not rows:
        return pd.DataFrame(columns=FAMILY_POOLED_COLUMNS)

    pooled = pd.DataFrame.from_records(rows, columns=FAMILY_POOLED_COLUMNS)
    return pooled.sort_values(
        ["nct_id", "family_label", "analysis_population", "estimand_type"],
        kind="stable",
    ).reset_index(drop=True)


def build_family_pooled_trial_summary(
    family_pooled_effects: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate family pools to one summary row per trial."""

    if family_pooled_effects.empty:
        return pd.DataFrame(columns=FAMILY_TRIAL_COLUMNS)

    rows: list[dict[str, object]] = []
    for nct_id, group in family_pooled_effects.groupby("nct_id", sort=True):
        family_pool_count = int(len(group))
        raw_surface_count = int(group["raw_surface_count"].sum())
        canonical_effect_count = int(group["canonical_effect_count"].sum())
        rows.append(
            {
                "nct_id": nct_id,
                "family_pool_count": family_pool_count,
                "precision_ready_family_pool_count": int(group["family_effect_precision"].notna().sum()),
                "canonical_effect_count": canonical_effect_count,
                "raw_surface_count": raw_surface_count,
                "family_contraction_ratio": (
                    family_pool_count / canonical_effect_count if canonical_effect_count else 0.0
                ),
                "mean_canonical_effects_per_family": (
                    canonical_effect_count / family_pool_count if family_pool_count else 0.0
                ),
                "mean_surfaces_per_family": (
                    raw_surface_count / family_pool_count if family_pool_count else 0.0
                ),
                "family_labels": _join_unique(group["family_label"]),
                "effect_measures": _join_unique(group["effect_measure"]),
                "estimand_types": _join_unique(group["estimand_type"]),
            }
        )

    return pd.DataFrame.from_records(rows, columns=FAMILY_TRIAL_COLUMNS).sort_values(
        ["raw_surface_count", "nct_id"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def render_family_pooled_report(
    family_pooled_effects: pd.DataFrame,
    trial_summary: pd.DataFrame,
    *,
    topic: TopicSpec,
) -> str:
    """Render a compact Markdown report for family-level pooling."""

    raw_surface_count = int(family_pooled_effects["raw_surface_count"].sum()) if not family_pooled_effects.empty else 0
    canonical_effect_count = int(family_pooled_effects["canonical_effect_count"].sum()) if not family_pooled_effects.empty else 0
    top_families = family_pooled_effects.sort_values(
        ["canonical_effect_count", "nct_id", "family_label"],
        ascending=[False, True, True],
        kind="stable",
    ).head(12)

    lines = [
        f"# Family Pooled Effects: {topic.slug}",
        "",
        "This proof-of-concept pools canonical effects within explicit trial-local families.",
        f"- family_pool_count: {int(len(family_pooled_effects))}",
        f"- canonical_effect_count: {canonical_effect_count}",
        f"- raw_surface_count: {raw_surface_count}",
        f"- precision_ready_family_pool_count: {int(family_pooled_effects['family_effect_precision'].notna().sum()) if not family_pooled_effects.empty else 0}",
        "",
        "## Trial contraction",
        "",
        "| NCT ID | Family pools | Canonical effects | Raw surfaces | Family contraction |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in trial_summary.head(10).itertuples(index=False):
        lines.append(
            f"| {row.nct_id} | {int(row.family_pool_count)} | {int(row.canonical_effect_count)} | "
            f"{int(row.raw_surface_count)} | {float(row.family_contraction_ratio):.3f} |"
        )

    lines.extend(
        [
            "",
            "## Highest-yield family pools",
            "",
            "| NCT ID | Family | Population | Canonical effects | Family effect | Precision | Status |",
            "|---|---|---|---:|---:|---:|---|",
        ]
    )
    for row in top_families.itertuples(index=False):
        precision_text = (
            f"{float(row.family_effect_precision):.4f}"
            if pd.notna(row.family_effect_precision)
            else "NA"
        )
        lines.append(
            f"| {row.nct_id} | {row.family_label} | {row.analysis_population} | "
            f"{int(row.canonical_effect_count)} | {float(row.family_effect_value):.4f} | "
            f"{precision_text} | {row.pooling_status} |"
        )

    return "\n".join(lines) + "\n"


def materialize_family_pooled_outputs(
    output_dir: Path | str,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write explicit family-pooled outputs from canonical-effect artifacts."""

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    output_path = Path(output_dir)
    canonical_effect_estimates = pd.read_parquet(output_path / "canonical_effect_estimates.parquet")

    family_pooled = build_family_pooled_effects(canonical_effect_estimates)
    trial_summary = build_family_pooled_trial_summary(family_pooled)
    report = render_family_pooled_report(family_pooled, trial_summary, topic=topic_spec)

    pooled_path = output_path / "family_pooled_effects.parquet"
    trial_summary_path = output_path / "family_pooled_trial_summary.parquet"
    report_path = output_path / "family_pooled_report.md"
    manifest_path = output_path / "family_pooled_manifest.json"

    family_pooled.to_parquet(pooled_path, index=False)
    trial_summary.to_parquet(trial_summary_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    canonical_effect_count = int(family_pooled["canonical_effect_count"].sum()) if not family_pooled.empty else 0
    raw_surface_count = int(family_pooled["raw_surface_count"].sum()) if not family_pooled.empty else 0
    manifest_seed = {
        "topic_slug": topic_spec.slug,
        "family_pool_count": int(len(family_pooled)),
        "canonical_effect_count": canonical_effect_count,
        "raw_surface_count": raw_surface_count,
        "precision_ready_family_pool_count": int(family_pooled["family_effect_precision"].notna().sum()) if not family_pooled.empty else 0,
    }
    manifest = {
        **manifest_seed,
        "family_pooled_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "mapped_trial_count": int(len(trial_summary)),
        "family_contraction_ratio": (
            float(len(family_pooled)) / canonical_effect_count if canonical_effect_count else 0.0
        ),
        "outputs": {
            "family_pooled_effects": str(pooled_path),
            "family_pooled_trial_summary": str(trial_summary_path),
            "family_pooled_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
