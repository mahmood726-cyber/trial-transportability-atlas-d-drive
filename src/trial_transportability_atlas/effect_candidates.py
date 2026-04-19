"""Build strict effect-candidate summaries from filtered trial outcomes."""
from __future__ import annotations

from math import comb

import pandas as pd


PARTICIPANT_UNITS = {"participant", "participants"}
CONTINUOUS_PARAM_TYPES = {"MEAN", "LEAST_SQUARES_MEAN", "GEOMETRIC_MEAN"}


def _normalize_unit(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().casefold()


def _measurement_family(param_type: str | None, unit: str) -> str:
    param = (param_type or "").strip().upper()
    if param == "COUNT_OF_PARTICIPANTS" and unit in PARTICIPANT_UNITS:
        return "binary_participant_count"
    if param in CONTINUOUS_PARAM_TYPES and unit not in PARTICIPANT_UNITS:
        return "continuous_mean"
    if param == "MEDIAN":
        return "unsupported_median"
    if param == "NUMBER":
        return "unsupported_number"
    return "unsupported_other"


def _candidate_key(frame: pd.DataFrame) -> pd.Series:
    measurement_mask = frame["record_type"].eq("measurement")
    measurement_key = (
        frame["nct_id"].astype(str)
        + "|measurement|"
        + frame["outcome_id"].fillna("NA").astype(str)
        + "|"
        + frame["outcome_name"].fillna("NA").astype(str)
        + "|"
        + frame["time_frame"].fillna("NA").astype(str)
        + "|"
        + frame["unit"].fillna("NA").astype(str)
        + "|"
        + frame["param_type"].fillna("NA").astype(str)
    )
    event_key = (
        frame["nct_id"].astype(str)
        + "|reported_event|"
        + frame["outcome_name"].fillna("NA").astype(str)
        + "|"
        + frame["time_frame"].fillna("NA").astype(str)
        + "|"
        + frame["event_type"].fillna("NA").astype(str)
        + "|"
        + frame["organ_system"].fillna("NA").astype(str)
    )
    return measurement_key.where(measurement_mask, event_key)


def build_effect_candidates(outcomes: pd.DataFrame) -> pd.DataFrame:
    """Aggregate filtered trial outcomes into strict effect-candidate rows."""

    if outcomes.empty:
        return pd.DataFrame(
            columns=[
                "candidate_id",
                "nct_id",
                "record_type",
                "source_table",
                "outcome_id",
                "outcome_name",
                "outcome_type",
                "time_frame",
                "unit",
                "param_type",
                "event_type",
                "organ_system",
                "candidate_family",
                "effect_model_hint",
                "row_count",
                "group_count",
                "pairwise_contrast_count",
                "numeric_row_count",
                "denominator_row_count",
                "dispersion_row_count",
                "has_multi_group",
                "has_complete_numeric",
                "has_complete_denominator",
                "has_complete_dispersion",
                "comparable_flag",
                "comparability_reason",
            ]
        )

    frame = outcomes.copy()
    frame["candidate_id"] = _candidate_key(frame)
    frame["unit_norm"] = frame["unit"].map(_normalize_unit)
    frame["group_id"] = frame["ctgov_group_code"].fillna(frame["result_group_id"]).astype(str)
    frame["has_group"] = frame["group_id"].ne("").fillna(False)
    frame["has_numeric_value"] = frame["value_num"].notna()
    frame["has_denominator"] = frame["subjects_at_risk"].notna()
    frame["has_dispersion"] = frame["dispersion_type"].notna()

    records: list[dict[str, object]] = []
    for candidate_id, group in frame.groupby("candidate_id", dropna=False, sort=True):
        first = group.iloc[0]
        record_type = str(first["record_type"])
        if record_type == "measurement":
            family = _measurement_family(first.get("param_type"), first["unit_norm"])
            effect_model_hint = {
                "binary_participant_count": "participant_count_contrast",
                "continuous_mean": "continuous_mean_contrast",
            }.get(family)
        else:
            family = "binary_event_count"
            effect_model_hint = "event_rate_contrast"

        row_count = int(len(group))
        group_count = int(group.loc[group["has_group"], "group_id"].nunique())
        numeric_row_count = int(group["has_numeric_value"].sum())
        denominator_row_count = int(group["has_denominator"].sum())
        dispersion_row_count = int(group["has_dispersion"].sum())

        has_multi_group = group_count >= 2
        has_complete_numeric = numeric_row_count == row_count
        has_complete_denominator = denominator_row_count == row_count
        has_complete_dispersion = dispersion_row_count == row_count

        if not has_multi_group:
            comparable_flag = False
            reason = "single_group_only"
        elif family == "binary_event_count":
            comparable_flag = has_complete_numeric and has_complete_denominator
            reason = (
                "ok"
                if comparable_flag
                else "missing_event_count_or_denominator"
            )
        elif family == "binary_participant_count":
            comparable_flag = has_complete_numeric
            reason = "ok" if comparable_flag else "missing_participant_counts"
        elif family == "continuous_mean":
            comparable_flag = has_complete_numeric and has_complete_dispersion
            reason = (
                "ok"
                if comparable_flag
                else "missing_mean_or_dispersion"
            )
        else:
            comparable_flag = False
            reason = family

        records.append(
            {
                "candidate_id": candidate_id,
                "nct_id": first["nct_id"],
                "record_type": record_type,
                "source_table": first["source_table"],
                "outcome_id": first["outcome_id"],
                "outcome_name": first["outcome_name"],
                "outcome_type": first["outcome_type"],
                "time_frame": first["time_frame"],
                "unit": first["unit"],
                "param_type": first["param_type"],
                "event_type": first["event_type"],
                "organ_system": first["organ_system"],
                "candidate_family": family,
                "effect_model_hint": effect_model_hint,
                "row_count": row_count,
                "group_count": group_count,
                "pairwise_contrast_count": comb(group_count, 2) if group_count >= 2 else 0,
                "numeric_row_count": numeric_row_count,
                "denominator_row_count": denominator_row_count,
                "dispersion_row_count": dispersion_row_count,
                "has_multi_group": has_multi_group,
                "has_complete_numeric": has_complete_numeric,
                "has_complete_denominator": has_complete_denominator,
                "has_complete_dispersion": has_complete_dispersion,
                "comparable_flag": comparable_flag,
                "comparability_reason": reason,
            }
        )

    return pd.DataFrame.from_records(records).sort_values(
        ["nct_id", "record_type", "outcome_name", "time_frame"],
        kind="stable",
    ).reset_index(drop=True)
