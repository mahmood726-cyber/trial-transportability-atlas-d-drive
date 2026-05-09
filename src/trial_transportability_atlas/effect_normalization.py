# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Ontology and population normalization for family-level effect pools."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

import pandas as pd

from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec


NORMALIZED_MANIFOLD_COLUMNS = (
    "normalized_manifold_id",
    "normalized_family_label",
    "normalized_family_domain",
    "normalized_family_variant",
    "normalized_analysis_population",
    "population_core",
    "population_modifier_flags",
    "effect_measure",
    "estimand_type",
    "effect_direction",
    "trial_count",
    "family_pool_count",
    "precision_ready_family_pool_count",
    "canonical_effect_count",
    "raw_surface_count",
    "nct_ids",
    "source_family_labels",
    "source_analysis_populations",
    "derivation_methods",
    "surface_value_min",
    "surface_value_median",
    "surface_value_max",
    "normalized_effect_iqr",
    "normalized_effect_mad_sd",
    "normalized_effect_value",
    "normalized_effect_precision",
    "precision_metric",
    "normalization_method",
    "normalization_status",
    "sign_coherence",
)

NORMALIZED_RECURRENCE_COLUMNS = (
    "normalized_family_label",
    "normalized_family_domain",
    "normalized_family_variant",
    "population_core",
    "effect_measure",
    "estimand_type",
    "effect_direction",
    "trial_count",
    "family_pool_count",
    "normalized_population_count",
    "normalized_manifold_count",
    "multi_trial_manifold_count",
    "normalized_population_examples",
    "source_analysis_populations",
    "nct_ids",
    "recurrence_status",
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


def normalize_family_ontology(row: pd.Series) -> tuple[str, str, str]:
    """Return normalized family ontology labels for one family pool row."""

    family_label = str(row.get("family_label") or "")
    family_domain = str(row.get("family_domain") or "")

    if family_label == "activity_volume":
        return "activity_function", "activity_function", "volume"
    if family_label == "activity_responder_risk":
        return "activity_function", "activity_function", "responder_risk"
    if family_label == "walk_performance":
        return "walking_capacity", "walking_capacity", "performance"
    if family_label == "walk_responder_risk":
        return "walking_capacity", "walking_capacity", "responder_risk"
    if family_label == "sexual_activity":
        return "sexual_function", "sexual_function", "activity"
    if family_label == "sexual_function":
        return "sexual_function", "sexual_function", "function"
    if family_label == "symptom_responder_risk":
        return "symptom_status", "symptom_status", "responder_risk"
    if family_domain == "adverse_event_risk" or family_label.startswith("adverse_event_risk"):
        if family_label.endswith("serious"):
            variant = "serious"
        elif family_label.endswith("other"):
            variant = "other"
        else:
            variant = "overall"
        return "adverse_event_risk", "adverse_event_risk", variant

    normalized_label = _slug(family_label)
    normalized_domain = _slug(family_domain, default=normalized_label)
    return normalized_label, normalized_domain, normalized_label


def normalize_analysis_population(row: pd.Series) -> tuple[str, str, str]:
    """Return normalized analysis-population descriptors for one family pool row."""

    raw_population = row.get("analysis_population")
    population_text = str(raw_population).strip() if raw_population is not None and not pd.isna(raw_population) else ""
    normalized_text = _normalize_text(population_text)
    family_domain = str(row.get("family_domain") or "")

    if family_domain == "adverse_event_risk":
        population_core = "meddra_system"
        population_modifier_flags = _slug(population_text)
        normalized_population = f"{population_core}+{population_modifier_flags}"
        return population_core, population_modifier_flags, normalized_population

    tokens = set(normalized_text.split())
    if "full analysis set" in normalized_text or "fas" in tokens:
        population_core = "full_analysis_set"
    elif "safety" in normalized_text:
        population_core = "safety_set"
    elif "received at least one dose" in normalized_text:
        population_core = "treated_set"
    elif "valid assessment" in normalized_text:
        population_core = "outcome_evaluable_set"
    elif not normalized_text:
        population_core = "unspecified"
    else:
        population_core = "other_population"

    modifiers: list[str] = []
    if "subset" in normalized_text:
        modifiers.append("subset")
    if "without ae sae" in normalized_text:
        modifiers.append("without_ae_sae")
    if "multiple imputation" in normalized_text or "locf" in normalized_text:
        modifiers.append("imputation_sensitivity")
    if "received at least one dose" in normalized_text and population_core != "treated_set":
        modifiers.append("received_one_dose")
    if "valid assessment" in normalized_text and population_core != "outcome_evaluable_set":
        modifiers.append("outcome_evaluable")
    if "baseline 6mwt equal to or less than 300" in normalized_text:
        modifiers.append("baseline_6mwt_le_300")
    if "baseline 6mwt between 100 or above and less than 450" in normalized_text:
        modifiers.append("baseline_6mwt_100_to_lt_450")

    modifier_flags = ";".join(sorted(set(modifiers)))
    normalized_population = population_core
    if modifier_flags:
        normalized_population = f"{population_core}+{modifier_flags.replace(';', '+')}"
    return population_core, modifier_flags, normalized_population


def build_normalized_family_effects(
    family_pooled_effects: pd.DataFrame,
) -> pd.DataFrame:
    """Augment family pools with normalized ontology and population labels."""

    if family_pooled_effects.empty:
        return family_pooled_effects.copy()

    frame = family_pooled_effects.copy()
    family_parts = frame.apply(normalize_family_ontology, axis=1, result_type="expand")
    family_parts.columns = [
        "normalized_family_label",
        "normalized_family_domain",
        "normalized_family_variant",
    ]
    population_parts = frame.apply(normalize_analysis_population, axis=1, result_type="expand")
    population_parts.columns = [
        "population_core",
        "population_modifier_flags",
        "normalized_analysis_population",
    ]
    return pd.concat([frame, family_parts, population_parts], axis=1)


def build_normalized_topic_manifolds(
    normalized_family_effects: pd.DataFrame,
) -> pd.DataFrame:
    """Pool family effects within normalized ontology and population manifolds."""

    if normalized_family_effects.empty:
        return pd.DataFrame(columns=NORMALIZED_MANIFOLD_COLUMNS)

    group_columns = [
        "normalized_family_label",
        "normalized_family_domain",
        "normalized_family_variant",
        "normalized_analysis_population",
        "population_core",
        "population_modifier_flags",
        "effect_measure",
        "estimand_type",
        "effect_direction",
    ]
    rows: list[dict[str, object]] = []

    for group_key, group in normalized_family_effects.groupby(group_columns, sort=True, dropna=False):
        (
            normalized_family_label,
            normalized_family_domain,
            normalized_family_variant,
            normalized_analysis_population,
            population_core,
            population_modifier_flags,
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
        normalized_effect_value = float(values.median())
        normalized_effect_iqr = _iqr(values)
        normalized_effect_mad_sd = _mad_sd(values)

        if trial_count == 1 and family_pool_count == 1:
            normalized_effect_precision = next(iter(precisions), pd.NA)
            precision_metric = next(iter(group["precision_metric"].dropna()), pd.NA)
            normalization_method = "single_family_pool_identity"
            normalization_status = (
                "single_family_pool_precision_retained"
                if not precisions.empty
                else "single_family_pool_no_precision"
            )
        elif precisions.empty:
            normalized_effect_precision = pd.NA
            precision_metric = pd.NA
            normalization_method = "normalized_median_no_precision"
            normalization_status = "no_precision_ready_family_pool"
        else:
            median_precision = float(precisions.median())
            normalized_effect_precision = max(median_precision, normalized_effect_mad_sd)
            precision_metric = _split_join_unique(group["precision_metric"])
            if normalized_effect_mad_sd > median_precision + 1e-12:
                normalization_method = "normalized_median_with_dispersion_guardrail"
                normalization_status = "dispersion_guardrail_applied"
            else:
                normalization_method = "normalized_median_no_precision_gain"
                normalization_status = "median_precision_retained"

        seed = {
            "normalized_family_label": normalized_family_label,
            "normalized_family_variant": normalized_family_variant,
            "normalized_analysis_population": normalized_analysis_population,
            "effect_measure": effect_measure,
            "estimand_type": estimand_type,
            "effect_direction": effect_direction,
        }
        rows.append(
            {
                "normalized_manifold_id": hashlib.sha256(
                    json.dumps(seed, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "normalized_family_label": normalized_family_label,
                "normalized_family_domain": normalized_family_domain,
                "normalized_family_variant": normalized_family_variant,
                "normalized_analysis_population": normalized_analysis_population,
                "population_core": population_core,
                "population_modifier_flags": population_modifier_flags,
                "effect_measure": effect_measure,
                "estimand_type": estimand_type,
                "effect_direction": effect_direction,
                "trial_count": trial_count,
                "family_pool_count": family_pool_count,
                "precision_ready_family_pool_count": precision_ready_count,
                "canonical_effect_count": canonical_effect_count,
                "raw_surface_count": raw_surface_count,
                "nct_ids": _join_unique(group["nct_id"]),
                "source_family_labels": _join_unique(group["family_label"]),
                "source_analysis_populations": _join_unique(group["analysis_population"]),
                "derivation_methods": _split_join_unique(group["derivation_methods"]),
                "surface_value_min": float(values.min()),
                "surface_value_median": normalized_effect_value,
                "surface_value_max": float(values.max()),
                "normalized_effect_iqr": normalized_effect_iqr,
                "normalized_effect_mad_sd": normalized_effect_mad_sd,
                "normalized_effect_value": normalized_effect_value,
                "normalized_effect_precision": normalized_effect_precision,
                "precision_metric": precision_metric,
                "normalization_method": normalization_method,
                "normalization_status": normalization_status,
                "sign_coherence": _sign_coherence(values),
            }
        )

    if not rows:
        return pd.DataFrame(columns=NORMALIZED_MANIFOLD_COLUMNS)

    manifolds = pd.DataFrame.from_records(rows, columns=NORMALIZED_MANIFOLD_COLUMNS)
    return manifolds.sort_values(
        ["trial_count", "family_pool_count", "normalized_family_label", "normalized_analysis_population"],
        ascending=[False, False, True, True],
        kind="stable",
    ).reset_index(drop=True)


def build_normalized_topic_recurrence(
    normalized_family_effects: pd.DataFrame,
    normalized_topic_manifolds: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize normalized recurrence and remaining population stratification."""

    if normalized_family_effects.empty:
        return pd.DataFrame(columns=NORMALIZED_RECURRENCE_COLUMNS)

    loose_group_columns = [
        "normalized_family_label",
        "normalized_family_domain",
        "normalized_family_variant",
        "population_core",
        "effect_measure",
        "estimand_type",
        "effect_direction",
    ]
    rows: list[dict[str, object]] = []

    for group_key, group in normalized_family_effects.groupby(loose_group_columns, sort=True, dropna=False):
        (
            normalized_family_label,
            normalized_family_domain,
            normalized_family_variant,
            population_core,
            effect_measure,
            estimand_type,
            effect_direction,
        ) = group_key

        manifold_match = normalized_topic_manifolds.loc[
            (normalized_topic_manifolds["normalized_family_label"] == normalized_family_label)
            & (normalized_topic_manifolds["normalized_family_domain"] == normalized_family_domain)
            & (normalized_topic_manifolds["normalized_family_variant"] == normalized_family_variant)
            & (normalized_topic_manifolds["population_core"] == population_core)
            & (normalized_topic_manifolds["effect_measure"] == effect_measure)
            & (normalized_topic_manifolds["estimand_type"] == estimand_type)
            & (normalized_topic_manifolds["effect_direction"] == effect_direction)
        ]

        trial_count = int(group["nct_id"].nunique())
        family_pool_count = int(len(group))
        normalized_population_count = int(group["normalized_analysis_population"].nunique(dropna=False))
        normalized_manifold_count = int(len(manifold_match))
        multi_trial_manifold_count = int((manifold_match["trial_count"] >= 2).sum())

        if multi_trial_manifold_count >= 1:
            recurrence_status = "normalized_cross_trial_manifold_present"
        elif normalized_manifold_count < family_pool_count:
            recurrence_status = (
                "cross_trial_wording_normalized"
                if trial_count >= 2
                else "within_trial_wording_normalized"
            )
        elif family_pool_count > 1:
            recurrence_status = (
                "cross_trial_population_stratification"
                if trial_count >= 2
                else "within_trial_population_stratification"
            )
        else:
            recurrence_status = "single_pool_only"

        rows.append(
            {
                "normalized_family_label": normalized_family_label,
                "normalized_family_domain": normalized_family_domain,
                "normalized_family_variant": normalized_family_variant,
                "population_core": population_core,
                "effect_measure": effect_measure,
                "estimand_type": estimand_type,
                "effect_direction": effect_direction,
                "trial_count": trial_count,
                "family_pool_count": family_pool_count,
                "normalized_population_count": normalized_population_count,
                "normalized_manifold_count": normalized_manifold_count,
                "multi_trial_manifold_count": multi_trial_manifold_count,
                "normalized_population_examples": _join_unique(group["normalized_analysis_population"]),
                "source_analysis_populations": _join_unique(group["analysis_population"]),
                "nct_ids": _join_unique(group["nct_id"]),
                "recurrence_status": recurrence_status,
            }
        )

    summary = pd.DataFrame.from_records(rows, columns=NORMALIZED_RECURRENCE_COLUMNS)
    return summary.sort_values(
        ["trial_count", "family_pool_count", "normalized_family_label"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def render_normalized_topic_report(
    normalized_topic_manifolds: pd.DataFrame,
    normalized_topic_recurrence: pd.DataFrame,
    *,
    topic: TopicSpec,
) -> str:
    """Render a compact report for ontology and population normalization."""

    lines = [
        f"# Normalized Topic Manifolds: {topic.slug}",
        "",
        "This proof-of-concept normalizes family ontology and analysis-population wording before synthesis.",
        f"- normalized_topic_manifold_count: {int(len(normalized_topic_manifolds))}",
        f"- multi_trial_normalized_manifold_count: {int((normalized_topic_manifolds['trial_count'] >= 2).sum()) if not normalized_topic_manifolds.empty else 0}",
        f"- wording_normalized_group_count: {int(normalized_topic_recurrence['recurrence_status'].isin(['within_trial_wording_normalized', 'cross_trial_wording_normalized']).sum()) if not normalized_topic_recurrence.empty else 0}",
        f"- population_stratification_group_count: {int(normalized_topic_recurrence['recurrence_status'].isin(['within_trial_population_stratification', 'cross_trial_population_stratification']).sum()) if not normalized_topic_recurrence.empty else 0}",
        "",
        "## Highest-yield normalized manifolds",
        "",
        "| Family | Variant | Population | Trials | Family pools | Effect | Precision | Status |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for row in normalized_topic_manifolds.head(12).itertuples(index=False):
        precision_text = (
            f"{float(row.normalized_effect_precision):.4f}"
            if pd.notna(row.normalized_effect_precision)
            else "NA"
        )
        lines.append(
            f"| {row.normalized_family_label} | {row.normalized_family_variant} | "
            f"{row.normalized_analysis_population} | {int(row.trial_count)} | "
            f"{int(row.family_pool_count)} | {float(row.normalized_effect_value):.4f} | "
            f"{precision_text} | {row.normalization_status} |"
        )

    lines.extend(
        [
            "",
            "## Remaining recurrence structure",
            "",
            "| Family | Variant | Population core | Trials | Pools | Manifolds | Status |",
            "|---|---|---|---:|---:|---:|---|",
        ]
    )
    for row in normalized_topic_recurrence.head(12).itertuples(index=False):
        lines.append(
            f"| {row.normalized_family_label} | {row.normalized_family_variant} | {row.population_core} | "
            f"{int(row.trial_count)} | {int(row.family_pool_count)} | "
            f"{int(row.normalized_manifold_count)} | {row.recurrence_status} |"
        )

    return "\n".join(lines) + "\n"


def materialize_normalized_topic_outputs(
    output_dir: Path | str,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write ontology-normalized manifold outputs from family-pooled artifacts."""

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    output_path = Path(output_dir)
    family_pooled_effects = pd.read_parquet(output_path / "family_pooled_effects.parquet")

    normalized_family_effects = build_normalized_family_effects(family_pooled_effects)
    normalized_topic_manifolds = build_normalized_topic_manifolds(normalized_family_effects)
    normalized_topic_recurrence = build_normalized_topic_recurrence(
        normalized_family_effects,
        normalized_topic_manifolds,
    )
    report = render_normalized_topic_report(
        normalized_topic_manifolds,
        normalized_topic_recurrence,
        topic=topic_spec,
    )

    normalized_family_path = output_path / "normalized_family_effects.parquet"
    manifolds_path = output_path / "normalized_topic_manifolds.parquet"
    recurrence_path = output_path / "normalized_topic_recurrence.parquet"
    report_path = output_path / "normalized_topic_report.md"
    manifest_path = output_path / "normalized_topic_manifest.json"

    normalized_family_effects.to_parquet(normalized_family_path, index=False)
    normalized_topic_manifolds.to_parquet(manifolds_path, index=False)
    normalized_topic_recurrence.to_parquet(recurrence_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    manifest_seed = {
        "topic_slug": topic_spec.slug,
        "source_family_pool_count": int(len(family_pooled_effects)),
        "normalized_topic_manifold_count": int(len(normalized_topic_manifolds)),
        "multi_trial_normalized_manifold_count": int((normalized_topic_manifolds["trial_count"] >= 2).sum()) if not normalized_topic_manifolds.empty else 0,
        "precision_ready_normalized_manifold_count": int(normalized_topic_manifolds["normalized_effect_precision"].notna().sum()) if not normalized_topic_manifolds.empty else 0,
    }
    manifest = {
        **manifest_seed,
        "normalized_topic_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "mapped_trial_count": int(family_pooled_effects["nct_id"].nunique()) if not family_pooled_effects.empty else 0,
        "wording_normalized_group_count": int(
            normalized_topic_recurrence["recurrence_status"].isin(
                ["within_trial_wording_normalized", "cross_trial_wording_normalized"]
            ).sum()
        ) if not normalized_topic_recurrence.empty else 0,
        "population_stratification_group_count": int(
            normalized_topic_recurrence["recurrence_status"].isin(
                ["within_trial_population_stratification", "cross_trial_population_stratification"]
            ).sum()
        ) if not normalized_topic_recurrence.empty else 0,
        "outputs": {
            "normalized_family_effects": str(normalized_family_path),
            "normalized_topic_manifolds": str(manifolds_path),
            "normalized_topic_recurrence": str(recurrence_path),
            "normalized_topic_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
