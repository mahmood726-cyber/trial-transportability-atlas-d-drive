# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Fail-closed signed treatment-control effect derivation from mapped arm surfaces."""
from __future__ import annotations

import hashlib
import json
from math import log, sqrt
from pathlib import Path
import re

import pandas as pd

from trial_transportability_atlas.effect_candidates import _candidate_key
from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec


SIGNED_EFFECT_COLUMNS = (
    "surface_id",
    "candidate_id",
    "nct_id",
    "candidate_family",
    "outcome_name",
    "time_frame",
    "surface_label",
    "analysis_population",
    "estimand_type",
    "effect_measure",
    "effect_value",
    "effect_precision",
    "precision_metric",
    "precision_status",
    "effect_direction",
    "treatment_group_code",
    "comparator_group_code",
    "treatment_arm_title",
    "comparator_arm_title",
    "treatment_intervention_names",
    "comparator_intervention_names",
    "treatment_value",
    "comparator_value",
    "treatment_dispersion_type",
    "comparator_dispersion_type",
    "treatment_dispersion_value",
    "comparator_dispersion_value",
    "treatment_n",
    "comparator_n",
    "mapping_confidence",
    "derivation_method",
)

SIGNED_EFFECT_TRIAL_COLUMNS = (
    "nct_id",
    "signed_effect_surface_count",
    "signed_effect_candidate_count",
    "precision_ready_surface_count",
    "precision_ready_candidate_count",
    "effect_measures",
    "estimand_types",
)

_MULTISPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]+")
_AMBIGUOUS_COUNT_RE = re.compile(
    r"\b(?:subset|without|multiple imputation|mi|locf|carried forward)\b",
    re.IGNORECASE,
)
_POSITIVE_CATEGORY_PATTERNS = (
    re.compile(r"^yes$"),
    re.compile(r"^improvement$"),
    re.compile(r"^has .* improved$"),
)
_NONPOSITIVE_CATEGORY_PATTERNS = (
    re.compile(r"^no$"),
    re.compile(r"^gets worse$"),
    re.compile(r"^is unchanged$"),
    re.compile(r"^is .* worse$"),
    re.compile(r"^missing$"),
)


def _normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = _PUNCT_RE.sub(" ", str(value).casefold())
    return _MULTISPACE_RE.sub(" ", text).strip()


def _safe_label(value: object, default: str = "overall") -> str:
    if value is None or pd.isna(value):
        return default
    text = str(value).strip()
    return text or default


def _first_row(frame: pd.DataFrame) -> pd.Series | None:
    if frame.empty:
        return None
    return next(frame.iterrows())[1]


def _first_value(values: pd.Series, default: object = pd.NA) -> object:
    return next(iter(values), default)


def _join_text(row: pd.Series) -> str:
    parts = [
        row.get("mapped_arm_title"),
        row.get("mapped_intervention_names"),
    ]
    return " ".join(str(part).strip() for part in parts if part is not None and not pd.isna(part))


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    normalized = _normalize_text(text)
    return any(_normalize_text(pattern) in normalized for pattern in patterns)


def _confidence_rank(value: object) -> int:
    return {
        "high": 3,
        "medium": 2,
        "low": 1,
        "none": 0,
    }.get(str(value or "").strip().casefold(), -1)


def _best_mapping_rows(arm_mapping: pd.DataFrame, topic: TopicSpec) -> pd.DataFrame:
    if arm_mapping.empty:
        return pd.DataFrame(
            columns=[
                "nct_id",
                "ctgov_group_code",
                "mapped_arm_title",
                "mapped_intervention_names",
                "mapping_confidence",
                "anchor_count_value",
                "role_label",
            ]
        )

    mapping = arm_mapping.copy()
    mapping = mapping.loc[mapping["mapping_status"].eq("mapped")].copy()
    if mapping.empty:
        return pd.DataFrame(
            columns=[
                "nct_id",
                "ctgov_group_code",
                "mapped_arm_title",
                "mapped_intervention_names",
                "mapping_confidence",
                "anchor_count_value",
                "role_label",
            ]
        )

    mapping["mapping_confidence_rank"] = mapping["mapping_confidence"].map(_confidence_rank)
    mapping["arm_text"] = mapping.apply(_join_text, axis=1)
    mapping["role_label"] = mapping["arm_text"].map(
        lambda text: (
            "treatment"
            if _contains_any(text, topic.intervention_terms)
            and not _contains_any(text, topic.intervention_exclude_terms)
            else "comparator"
        )
    )

    mapping = mapping.sort_values(
        [
            "nct_id",
            "ctgov_group_code",
            "mapping_confidence_rank",
            "mapped_arm_title",
            "mapped_intervention_names",
        ],
        ascending=[True, True, False, True, True],
        kind="stable",
    )
    mapping = mapping.drop_duplicates(["nct_id", "ctgov_group_code"], keep="first")
    columns = [
        "nct_id",
        "ctgov_group_code",
        "mapped_arm_title",
        "mapped_intervention_names",
        "mapping_confidence",
        "anchor_count_value",
        "role_label",
    ]
    return mapping[columns].reset_index(drop=True)


def _candidate_family_lookup(effect_candidates: pd.DataFrame) -> pd.DataFrame:
    if effect_candidates.empty:
        return pd.DataFrame(columns=["candidate_id", "candidate_family"])
    return effect_candidates[["candidate_id", "candidate_family"]].drop_duplicates().reset_index(drop=True)


def _baseline_surface(label: str) -> bool:
    normalized = _normalize_text(label)
    return normalized.startswith("baseline")


def _estimand_type(outcome_name: str, surface_label: str) -> str:
    normalized_label = _normalize_text(surface_label)
    normalized_name = _normalize_text(outcome_name)
    if "change from bl" in normalized_label or "change from baseline" in normalized_label:
        return "change_from_baseline"
    if normalized_name.startswith("change from baseline") or normalized_name.startswith("change from bl"):
        return "change_from_baseline"
    return "post_baseline_level"


def _role_anchor_rows(group: pd.DataFrame) -> tuple[pd.Series, pd.Series] | None:
    rows: dict[str, pd.Series] = {}
    for role in ("treatment", "comparator"):
        role_group = group.loc[group["role_label"].eq(role)].copy()
        if role_group.empty or role_group["ctgov_group_code"].nunique() != 1:
            return None
        rows[role] = (
            _first_row(
                role_group.sort_values(
                ["ctgov_group_code", "mapping_confidence"],
                ascending=[True, False],
                kind="stable",
            )
            .drop_duplicates(["ctgov_group_code"], keep="first")
            )
        )
        if rows[role] is None:
            return None
    return rows["treatment"], rows["comparator"]


def _pair_mapping_confidence(treatment_row: pd.Series, comparator_row: pd.Series) -> object:
    return min(
        treatment_row.get("mapping_confidence"),
        comparator_row.get("mapping_confidence"),
        key=_confidence_rank,
    )


def _surface_id(
    *,
    candidate_id: str,
    surface_label: str,
    analysis_population: str,
    treatment_group_code: str,
    comparator_group_code: str,
) -> str:
    surface_seed = {
        "candidate_id": candidate_id,
        "surface_label": surface_label,
        "analysis_population": analysis_population,
        "treatment_group_code": treatment_group_code,
        "comparator_group_code": comparator_group_code,
    }
    return hashlib.sha256(
        json.dumps(surface_seed, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _binary_partition_role(category_key: str) -> str | None:
    for pattern in _POSITIVE_CATEGORY_PATTERNS:
        if pattern.fullmatch(category_key):
            return "positive"
    for pattern in _NONPOSITIVE_CATEGORY_PATTERNS:
        if pattern.fullmatch(category_key):
            return "nonpositive"
    return None


def _log_risk_ratio(
    event_treatment: float,
    total_treatment: float,
    event_comparator: float,
    total_comparator: float,
) -> tuple[float, float, str]:
    if (
        total_treatment <= 0
        or total_comparator <= 0
        or event_treatment < 0
        or event_comparator < 0
        or event_treatment > total_treatment
        or event_comparator > total_comparator
    ):
        raise ValueError("invalid_event_or_denominator")

    continuity = (
        event_treatment == 0
        or event_comparator == 0
        or event_treatment == total_treatment
        or event_comparator == total_comparator
    )
    if continuity:
        event_treatment += 0.5
        event_comparator += 0.5
        total_treatment += 1.0
        total_comparator += 1.0
        precision_status = "wald_log_risk_ratio_with_continuity_correction"
    else:
        precision_status = "wald_log_risk_ratio"

    effect_value = log((event_treatment / total_treatment) / (event_comparator / total_comparator))
    effect_precision = sqrt(
        (1.0 / event_treatment)
        - (1.0 / total_treatment)
        + (1.0 / event_comparator)
        - (1.0 / total_comparator)
    )
    return effect_value, effect_precision, precision_status


def _event_numerator(frame: pd.DataFrame) -> pd.Series:
    subjects_affected = pd.to_numeric(frame["subjects_affected"], errors="coerce")
    event_count = pd.to_numeric(frame["event_count"], errors="coerce")
    value_num = pd.to_numeric(frame["value_num"], errors="coerce")
    numerator = subjects_affected.where(subjects_affected.notna(), event_count)
    return numerator.where(numerator.notna(), value_num)


def _precision_from_rows(
    treatment_row: pd.Series,
    comparator_row: pd.Series,
    *,
    analysis_population: str,
    surface_label: str,
) -> tuple[object, str | None, str]:
    treatment_type = _safe_label(treatment_row.get("dispersion_type"), default="")
    comparator_type = _safe_label(comparator_row.get("dispersion_type"), default="")
    treatment_value = treatment_row.get("dispersion_value")
    comparator_value = comparator_row.get("dispersion_value")

    if pd.isna(treatment_value) or pd.isna(comparator_value):
        return pd.NA, None, "missing_dispersion"

    if treatment_type == "Standard Error" and comparator_type == "Standard Error":
        precision = sqrt(float(treatment_value) ** 2 + float(comparator_value) ** 2)
        return precision, "standard_error", "reported_standard_error"

    if treatment_type != "Standard Deviation" or comparator_type != "Standard Deviation":
        return pd.NA, None, "incompatible_dispersion_types"

    if _AMBIGUOUS_COUNT_RE.search(f"{analysis_population} {surface_label}"):
        return pd.NA, None, "ambiguous_population_for_anchor_counts"

    treatment_n = treatment_row.get("anchor_count_value")
    comparator_n = comparator_row.get("anchor_count_value")
    if pd.isna(treatment_n) or pd.isna(comparator_n):
        return pd.NA, None, "missing_anchor_counts"
    if float(treatment_n) <= 1 or float(comparator_n) <= 1:
        return pd.NA, None, "invalid_anchor_counts"

    precision = sqrt(
        (float(treatment_value) ** 2 / float(treatment_n))
        + (float(comparator_value) ** 2 / float(comparator_n))
    )
    return precision, "standard_error", "derived_from_standard_deviation_and_anchor_counts"


def _build_continuous_estimate_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    continuous = frame.loc[
        frame["record_type"].eq("measurement")
        & frame["candidate_family"].eq("continuous_mean")
    ].copy()
    if continuous.empty:
        return []

    continuous["surface_label"] = continuous["classification"].map(lambda value: _safe_label(value, "overall"))
    continuous["analysis_label"] = continuous["analysis_population"].map(lambda value: _safe_label(value, "unspecified"))
    continuous["surface_key"] = continuous["surface_label"].map(_normalize_text)
    continuous["analysis_key"] = continuous["analysis_label"].map(_normalize_text)
    continuous["category_key"] = continuous["category"].map(lambda value: _normalize_text(_safe_label(value, "overall")))

    records: list[dict[str, object]] = []
    group_keys = [
        "candidate_id",
        "nct_id",
        "surface_key",
        "analysis_key",
        "category_key",
    ]
    for _, group in continuous.groupby(group_keys, sort=True, dropna=False):
        if not group["category_key"].eq("overall").all():
            continue

        surface_label = _safe_label(_first_value(group["surface_label"]), "overall")
        if _baseline_surface(surface_label):
            continue

        pair = _role_anchor_rows(group)
        if pair is None:
            continue
        treatment_row, comparator_row = pair
        if pd.isna(treatment_row.get("value_num")) or pd.isna(comparator_row.get("value_num")):
            continue

        analysis_population = _safe_label(treatment_row.get("analysis_label"), "unspecified")
        effect_precision, precision_metric, precision_status = _precision_from_rows(
            treatment_row,
            comparator_row,
            analysis_population=analysis_population,
            surface_label=surface_label,
        )
        records.append(
            {
                "surface_id": _surface_id(
                    candidate_id=str(treatment_row["candidate_id"]),
                    surface_label=surface_label,
                    analysis_population=analysis_population,
                    treatment_group_code=str(treatment_row["ctgov_group_code"]),
                    comparator_group_code=str(comparator_row["ctgov_group_code"]),
                ),
                "candidate_id": treatment_row["candidate_id"],
                "nct_id": treatment_row["nct_id"],
                "candidate_family": treatment_row["candidate_family"],
                "outcome_name": treatment_row["outcome_name"],
                "time_frame": treatment_row["time_frame"],
                "surface_label": surface_label,
                "analysis_population": analysis_population,
                "estimand_type": _estimand_type(
                    str(treatment_row["outcome_name"]),
                    surface_label,
                ),
                "effect_measure": "mean_difference",
                "effect_value": float(treatment_row["value_num"]) - float(comparator_row["value_num"]),
                "effect_precision": effect_precision,
                "precision_metric": precision_metric,
                "precision_status": precision_status,
                "effect_direction": "treatment_minus_comparator",
                "treatment_group_code": treatment_row["ctgov_group_code"],
                "comparator_group_code": comparator_row["ctgov_group_code"],
                "treatment_arm_title": treatment_row.get("mapped_arm_title"),
                "comparator_arm_title": comparator_row.get("mapped_arm_title"),
                "treatment_intervention_names": treatment_row.get("mapped_intervention_names"),
                "comparator_intervention_names": comparator_row.get("mapped_intervention_names"),
                "treatment_value": float(treatment_row["value_num"]),
                "comparator_value": float(comparator_row["value_num"]),
                "treatment_dispersion_type": treatment_row.get("dispersion_type"),
                "comparator_dispersion_type": comparator_row.get("dispersion_type"),
                "treatment_dispersion_value": treatment_row.get("dispersion_value"),
                "comparator_dispersion_value": comparator_row.get("dispersion_value"),
                "treatment_n": treatment_row.get("anchor_count_value"),
                "comparator_n": comparator_row.get("anchor_count_value"),
                "mapping_confidence": _pair_mapping_confidence(treatment_row, comparator_row),
                "derivation_method": "continuous_surface_split",
            }
        )

    return records


def _build_binary_participant_estimate_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    participant = frame.loc[
        frame["record_type"].eq("measurement")
        & frame["candidate_family"].eq("binary_participant_count")
    ].copy()
    if participant.empty:
        return []

    participant["surface_label"] = participant["classification"].map(lambda value: _safe_label(value, "overall"))
    participant["analysis_label"] = participant["analysis_population"].map(lambda value: _safe_label(value, "unspecified"))
    participant["surface_key"] = participant["surface_label"].map(_normalize_text)
    participant["analysis_key"] = participant["analysis_label"].map(_normalize_text)
    participant["category_label"] = participant["category"].map(lambda value: _safe_label(value, "overall"))
    participant["category_key"] = participant["category_label"].map(_normalize_text)

    records: list[dict[str, object]] = []
    group_keys = [
        "candidate_id",
        "nct_id",
        "surface_key",
        "analysis_key",
    ]
    for _, group in participant.groupby(group_keys, sort=True, dropna=False):
        if group["value_num"].isna().any():
            continue

        category_roles = group["category_key"].map(_binary_partition_role)
        if category_roles.isna().any():
            continue
        if "positive" not in set(category_roles):
            continue

        pair = _role_anchor_rows(group)
        if pair is None:
            continue
        treatment_row, comparator_row = pair

        treatment_code = str(treatment_row["ctgov_group_code"])
        comparator_code = str(comparator_row["ctgov_group_code"])
        treatment_group = group.loc[group["ctgov_group_code"].eq(treatment_code)].copy()
        comparator_group = group.loc[group["ctgov_group_code"].eq(comparator_code)].copy()

        treatment_total = float(treatment_group["value_num"].sum())
        comparator_total = float(comparator_group["value_num"].sum())
        treatment_positive = float(treatment_group.loc[treatment_group["category_key"].map(_binary_partition_role).eq("positive"), "value_num"].sum())
        comparator_positive = float(comparator_group.loc[comparator_group["category_key"].map(_binary_partition_role).eq("positive"), "value_num"].sum())

        try:
            effect_value, effect_precision, precision_status = _log_risk_ratio(
                treatment_positive,
                treatment_total,
                comparator_positive,
                comparator_total,
            )
        except ValueError:
            continue

        surface_label = _safe_label(_first_value(group["surface_label"]), "overall")
        analysis_population = _safe_label(_first_value(group["analysis_label"]), "unspecified")
        records.append(
            {
                "surface_id": _surface_id(
                    candidate_id=str(treatment_row["candidate_id"]),
                    surface_label=surface_label,
                    analysis_population=analysis_population,
                    treatment_group_code=treatment_code,
                    comparator_group_code=comparator_code,
                ),
                "candidate_id": treatment_row["candidate_id"],
                "nct_id": treatment_row["nct_id"],
                "candidate_family": treatment_row["candidate_family"],
                "outcome_name": treatment_row["outcome_name"],
                "time_frame": treatment_row["time_frame"],
                "surface_label": surface_label,
                "analysis_population": analysis_population,
                "estimand_type": "responder_risk",
                "effect_measure": "log_risk_ratio",
                "effect_value": effect_value,
                "effect_precision": effect_precision,
                "precision_metric": "standard_error",
                "precision_status": precision_status,
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "treatment_group_code": treatment_code,
                "comparator_group_code": comparator_code,
                "treatment_arm_title": treatment_row.get("mapped_arm_title"),
                "comparator_arm_title": comparator_row.get("mapped_arm_title"),
                "treatment_intervention_names": treatment_row.get("mapped_intervention_names"),
                "comparator_intervention_names": comparator_row.get("mapped_intervention_names"),
                "treatment_value": treatment_positive,
                "comparator_value": comparator_positive,
                "treatment_dispersion_type": pd.NA,
                "comparator_dispersion_type": pd.NA,
                "treatment_dispersion_value": pd.NA,
                "comparator_dispersion_value": pd.NA,
                "treatment_n": treatment_total,
                "comparator_n": comparator_total,
                "mapping_confidence": _pair_mapping_confidence(treatment_row, comparator_row),
                "derivation_method": "participant_partition_log_risk_ratio",
            }
        )

    return records


def _build_binary_event_estimate_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    events = frame.loc[
        frame["record_type"].eq("reported_event")
        & frame["candidate_family"].eq("binary_event_count")
    ].copy()
    if events.empty:
        return []

    events["event_numerator"] = _event_numerator(events)
    records: list[dict[str, object]] = []
    for _, group in events.groupby(["candidate_id", "nct_id"], sort=True, dropna=False):
        if group["event_numerator"].isna().any() or group["subjects_at_risk"].isna().any():
            continue

        pair = _role_anchor_rows(group)
        if pair is None:
            continue
        treatment_row, comparator_row = pair
        try:
            effect_value, effect_precision, precision_status = _log_risk_ratio(
                float(treatment_row["event_numerator"]),
                float(treatment_row["subjects_at_risk"]),
                float(comparator_row["event_numerator"]),
                float(comparator_row["subjects_at_risk"]),
            )
        except ValueError:
            continue

        surface_label = _safe_label(treatment_row.get("event_type"), "overall")
        analysis_population = _safe_label(treatment_row.get("organ_system"), "reported_event")
        records.append(
            {
                "surface_id": _surface_id(
                    candidate_id=str(treatment_row["candidate_id"]),
                    surface_label=surface_label,
                    analysis_population=analysis_population,
                    treatment_group_code=str(treatment_row["ctgov_group_code"]),
                    comparator_group_code=str(comparator_row["ctgov_group_code"]),
                ),
                "candidate_id": treatment_row["candidate_id"],
                "nct_id": treatment_row["nct_id"],
                "candidate_family": treatment_row["candidate_family"],
                "outcome_name": treatment_row["outcome_name"],
                "time_frame": treatment_row["time_frame"],
                "surface_label": surface_label,
                "analysis_population": analysis_population,
                "estimand_type": "event_risk",
                "effect_measure": "log_risk_ratio",
                "effect_value": effect_value,
                "effect_precision": effect_precision,
                "precision_metric": "standard_error",
                "precision_status": precision_status,
                "effect_direction": "treatment_vs_comparator_log_ratio",
                "treatment_group_code": treatment_row["ctgov_group_code"],
                "comparator_group_code": comparator_row["ctgov_group_code"],
                "treatment_arm_title": treatment_row.get("mapped_arm_title"),
                "comparator_arm_title": comparator_row.get("mapped_arm_title"),
                "treatment_intervention_names": treatment_row.get("mapped_intervention_names"),
                "comparator_intervention_names": comparator_row.get("mapped_intervention_names"),
                "treatment_value": float(treatment_row["event_numerator"]),
                "comparator_value": float(comparator_row["event_numerator"]),
                "treatment_dispersion_type": pd.NA,
                "comparator_dispersion_type": pd.NA,
                "treatment_dispersion_value": pd.NA,
                "comparator_dispersion_value": pd.NA,
                "treatment_n": float(treatment_row["subjects_at_risk"]),
                "comparator_n": float(comparator_row["subjects_at_risk"]),
                "mapping_confidence": _pair_mapping_confidence(treatment_row, comparator_row),
                "derivation_method": "reported_event_log_risk_ratio",
            }
        )

    return records


def build_signed_effect_estimates(
    outcomes: pd.DataFrame,
    effect_candidates: pd.DataFrame,
    arm_mapping: pd.DataFrame,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> pd.DataFrame:
    """Derive signed treatment-control effects from mapped continuous and binary surfaces."""

    if outcomes.empty or effect_candidates.empty or arm_mapping.empty:
        return pd.DataFrame(columns=SIGNED_EFFECT_COLUMNS)

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    mapping = _best_mapping_rows(arm_mapping, topic_spec)
    if mapping.empty:
        return pd.DataFrame(columns=SIGNED_EFFECT_COLUMNS)

    frame = outcomes.copy()
    frame["candidate_id"] = _candidate_key(frame)
    frame = frame.merge(_candidate_family_lookup(effect_candidates), how="left", on="candidate_id")
    frame = frame.merge(mapping, how="inner", on=["nct_id", "ctgov_group_code"])
    if frame.empty:
        return pd.DataFrame(columns=SIGNED_EFFECT_COLUMNS)

    records = [
        *_build_continuous_estimate_records(frame),
        *_build_binary_participant_estimate_records(frame),
        *_build_binary_event_estimate_records(frame),
    ]

    if not records:
        return pd.DataFrame(columns=SIGNED_EFFECT_COLUMNS)

    estimates = pd.DataFrame.from_records(records, columns=SIGNED_EFFECT_COLUMNS)
    return estimates.sort_values(
        ["nct_id", "candidate_id", "surface_label"],
        kind="stable",
    ).reset_index(drop=True)


def build_signed_effect_trial_summary(estimates: pd.DataFrame) -> pd.DataFrame:
    """Aggregate signed effects to one summary row per trial."""

    if estimates.empty:
        return pd.DataFrame(columns=SIGNED_EFFECT_TRIAL_COLUMNS)

    rows: list[dict[str, object]] = []
    for nct_id, group in estimates.groupby("nct_id", sort=True):
        precision_ready = group["effect_precision"].notna()
        rows.append(
            {
                "nct_id": nct_id,
                "signed_effect_surface_count": int(len(group)),
                "signed_effect_candidate_count": int(group["candidate_id"].nunique()),
                "precision_ready_surface_count": int(precision_ready.sum()),
                "precision_ready_candidate_count": int(group.loc[precision_ready, "candidate_id"].nunique()),
                "effect_measures": ";".join(sorted(group["effect_measure"].dropna().astype(str).unique())),
                "estimand_types": ";".join(sorted(group["estimand_type"].dropna().astype(str).unique())),
            }
        )

    return pd.DataFrame.from_records(rows, columns=SIGNED_EFFECT_TRIAL_COLUMNS).sort_values(
        ["signed_effect_surface_count", "nct_id"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def render_signed_effect_report(
    estimates: pd.DataFrame,
    trial_summary: pd.DataFrame,
    *,
    topic: TopicSpec,
) -> str:
    """Render a compact Markdown report for signed-effect recovery."""

    precision_ready_surface_count = int(estimates["effect_precision"].notna().sum()) if not estimates.empty else 0
    lines = [
        f"# Signed Effect Recovery: {topic.slug}",
        "",
        "This proof-of-concept derives signed treatment-minus-comparator estimates from mapped outcome surfaces.",
        f"- signed_effect_surface_count: {int(len(estimates))}",
        f"- signed_effect_candidate_count: {int(estimates['candidate_id'].nunique()) if not estimates.empty else 0}",
        f"- precision_ready_surface_count: {precision_ready_surface_count}",
        f"- mapped_trial_count: {int(len(trial_summary))}",
        "",
        "## Highest-yield trials",
        "",
        "| NCT ID | Signed surfaces | Candidates | Precision-ready surfaces | Estimands |",
        "|---|---:|---:|---:|---|",
    ]
    for row in trial_summary.head(10).itertuples(index=False):
        lines.append(
            f"| {row.nct_id} | {int(row.signed_effect_surface_count)} | "
            f"{int(row.signed_effect_candidate_count)} | {int(row.precision_ready_surface_count)} | "
            f"{row.estimand_types} |"
        )

    lines.extend(
        [
            "",
            "## Example signed surfaces",
            "",
            "| NCT ID | Outcome | Surface | Effect | Precision | Status |",
            "|---|---|---|---:|---:|---|",
        ]
    )
    preview = estimates.head(12)
    for row in preview.itertuples(index=False):
        precision_text = (
            f"{float(row.effect_precision):.4f}"
            if pd.notna(row.effect_precision)
            else "NA"
        )
        lines.append(
            f"| {row.nct_id} | {row.outcome_name} | {row.surface_label} | "
            f"{float(row.effect_value):.4f} | {precision_text} | {row.precision_status} |"
        )

    return "\n".join(lines) + "\n"


def materialize_signed_effect_outputs(
    output_dir: Path | str,
    *,
    topic: TopicSpec | str = PHASE1_TOPIC,
) -> dict[str, object]:
    """Write signed-effect proof-of-concept outputs from existing atlas artifacts."""

    topic_spec = topic if isinstance(topic, TopicSpec) else resolve_topic_spec(topic)
    output_path = Path(output_dir)

    outcomes = pd.read_parquet(output_path / "trial_outcomes_long.parquet")
    effect_candidates = pd.read_parquet(output_path / "effect_candidates.parquet")
    arm_mapping = pd.read_parquet(output_path / "arm_group_mapping.parquet")

    estimates = build_signed_effect_estimates(
        outcomes,
        effect_candidates,
        arm_mapping,
        topic=topic_spec,
    )
    trial_summary = build_signed_effect_trial_summary(estimates)
    report = render_signed_effect_report(estimates, trial_summary, topic=topic_spec)

    estimates_path = output_path / "signed_effect_estimates.parquet"
    trial_summary_path = output_path / "signed_effect_trial_summary.parquet"
    report_path = output_path / "signed_effect_report.md"
    manifest_path = output_path / "signed_effect_manifest.json"

    estimates.to_parquet(estimates_path, index=False)
    trial_summary.to_parquet(trial_summary_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    manifest_seed = {
        "topic_slug": topic_spec.slug,
        "signed_effect_surface_count": int(len(estimates)),
        "signed_effect_candidate_count": int(estimates["candidate_id"].nunique()) if not estimates.empty else 0,
        "precision_ready_surface_count": int(estimates["effect_precision"].notna().sum()) if not estimates.empty else 0,
    }
    manifest = {
        **manifest_seed,
        "signed_effect_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "mapped_trial_count": int(len(trial_summary)),
        "outputs": {
            "signed_effect_estimates": str(estimates_path),
            "signed_effect_trial_summary": str(trial_summary_path),
            "signed_effect_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
