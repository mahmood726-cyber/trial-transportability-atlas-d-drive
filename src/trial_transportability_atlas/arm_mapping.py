# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Fail-closed arm/group mapping for atlas topics."""
from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

import pandas as pd

from trial_transportability_atlas.aact_bridge import (
    AactSchemaError,
    iter_aact_rows,
    normalize_optional,
    read_aact_header,
)


ARM_MAPPING_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "design_groups": ("id", "nct_id", "group_type", "title", "description"),
    "design_group_interventions": ("id", "nct_id", "design_group_id", "intervention_id"),
    "interventions": ("id", "nct_id", "intervention_type", "name", "description"),
    "participant_flows": ("id", "nct_id", "recruitment_details", "pre_assignment_details"),
    "baseline_counts": ("id", "nct_id", "result_group_id", "ctgov_group_code", "units", "scope", "count"),
    "outcome_counts": ("id", "nct_id", "outcome_id", "result_group_id", "ctgov_group_code", "scope", "units", "count"),
    "milestones": (
        "id",
        "nct_id",
        "result_group_id",
        "ctgov_group_code",
        "title",
        "period",
        "description",
        "count",
    ),
    "reported_event_totals": (
        "id",
        "nct_id",
        "ctgov_group_code",
        "event_type",
        "classification",
        "subjects_affected",
        "subjects_at_risk",
    ),
}

ARM_MAPPING_COLUMNS = (
    "nct_id",
    "ctgov_group_code",
    "group_family",
    "group_suffix",
    "mapped_design_group_id",
    "mapped_arm_title",
    "mapped_group_type",
    "mapped_intervention_names",
    "mapping_status",
    "mapping_method",
    "mapping_confidence",
    "anchor_group_family",
    "anchor_ctgov_group_code",
    "anchor_count_value",
)

ARM_MAPPING_SUMMARY_COLUMNS = (
    "nct_id",
    "design_arm_count",
    "narrative_count_arm_count",
    "mapped_group_code_count",
    "total_group_code_count",
    "mapping_coverage",
    "mapping_methods",
    "mapped_arm_titles",
    "unmapped_group_codes",
)

GROUP_CODE_RE = re.compile(r"^(?P<family>[A-Z]+)(?P<suffix>\d+)$")
PLACEBO_RE = re.compile(r"\bplacebo(?:\s+(?:of|to))?\b", re.IGNORECASE)
ROLE_PREFIX_RE = re.compile(
    r"^(?:experimental|active comparator|comparator|control|intervention|study arm|group)\s*[:\-]?\s*",
    re.IGNORECASE,
)
MULTISPACE_RE = re.compile(r"\s+")
NEAR_EXACT_COUNT_DELTA = 5
ARM_SPECIFIC_EXCLUSION_WINDOW = 24


def validate_arm_mapping_tables(snapshot_dir: Path) -> dict[str, tuple[str, ...]]:
    """Validate the minimal AACT surface needed for arm mapping."""

    resolved: dict[str, tuple[str, ...]] = {}
    missing_messages: list[str] = []
    for table_name, required_columns in ARM_MAPPING_REQUIRED_COLUMNS.items():
        columns = read_aact_header(snapshot_dir, table_name)
        missing = [column for column in required_columns if column not in columns]
        if missing:
            missing_messages.append(
                f"{table_name} missing columns: {', '.join(missing)}"
            )
        resolved[table_name] = columns
    if missing_messages:
        raise AactSchemaError("; ".join(missing_messages))
    return resolved


def _normalize_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).casefold()
    text = re.sub(r"[^\w\s]+", " ", text)
    return MULTISPACE_RE.sub(" ", text).strip()


def _count_int(value: object) -> int | None:
    normalized = normalize_optional(None if value is None else str(value))
    if normalized is None:
        return None
    return int(float(normalized))


def _first_row(frame: pd.DataFrame) -> pd.Series | None:
    if frame.empty:
        return None
    return next(frame.iterrows())[1]


def _first_value(values: pd.Series, default: object = pd.NA) -> object:
    return next(iter(values), default)


def _group_code_parts(ctgov_group_code: str) -> tuple[str, str] | tuple[None, None]:
    match = GROUP_CODE_RE.match(ctgov_group_code or "")
    if not match:
        return None, None
    return match.group("family"), match.group("suffix")


def _extract_parenthetical_aliases(value: str) -> list[str]:
    aliases = []
    for match in re.finditer(r"\(([^()]+)\)", value):
        normalized = _normalize_text(match.group(1))
        if normalized:
            aliases.append(normalized)
    return aliases


def _strip_parenthetical_content(value: str) -> str:
    stripped = re.sub(r"\([^()]*\)", " ", value)
    return MULTISPACE_RE.sub(" ", stripped).strip()


def _clean_alias(value: str) -> str:
    cleaned = ROLE_PREFIX_RE.sub("", value)
    cleaned = PLACEBO_RE.sub(" ", cleaned)
    return MULTISPACE_RE.sub(" ", cleaned).strip()


def _alias_variants(title: str | None, intervention_names: list[str]) -> list[str]:
    aliases: set[str] = set()
    sources = [title or "", *intervention_names]
    for source in sources:
        normalized = _normalize_text(source)
        if not normalized:
            continue
        aliases.add(normalized)
        cleaned = _clean_alias(normalized)
        if cleaned and cleaned != normalized:
            aliases.add(cleaned)
        stripped = _normalize_text(_strip_parenthetical_content(source))
        if stripped and stripped != normalized:
            aliases.add(stripped)
            cleaned_stripped = _clean_alias(stripped)
            if cleaned_stripped:
                aliases.add(cleaned_stripped)
        for parenthetical in _extract_parenthetical_aliases(source):
            aliases.add(parenthetical)
            cleaned_parenthetical = _clean_alias(parenthetical)
            if cleaned_parenthetical:
                aliases.add(cleaned_parenthetical)
    return sorted({alias for alias in aliases if alias}, key=len, reverse=True)


def _nearest_alias_count(text: str, alias: str) -> set[int]:
    if not text or not alias:
        return set()

    number_matches = list(re.finditer(r"(?<![A-Za-z])\d{1,5}(?![A-Za-z])", text))
    for alias_match in re.finditer(re.escape(alias), text):
        alias_start, alias_end = alias_match.span()
        candidates: list[tuple[int, int, int]] = []
        for number_match in number_matches:
            number_start, number_end = number_match.span()
            if number_end <= alias_start:
                distance = alias_start - number_end
                before_flag = 0
            elif number_start >= alias_end:
                distance = number_start - alias_end
                before_flag = 1
            else:
                continue
            if distance > 40:
                continue
            context = text[min(number_start, alias_start) : max(number_end, alias_end)]
            if "." in context or ";" in context:
                continue
            count_value = int(number_match.group(0))
            if count_value < 2:
                continue
            candidates.append((count_value, distance, before_flag))
        if candidates:
            best_count, _, _ = sorted(candidates, key=lambda item: (item[2], item[1]))[0]
            return {best_count}

    return set()


def load_design_arm_catalog(
    snapshot_dir: Path,
    *,
    nct_ids: list[str] | set[str] | None = None,
) -> pd.DataFrame:
    """Load one design-arm row per AACT design group."""

    validate_arm_mapping_tables(snapshot_dir)
    nct_filter = set(nct_ids) if nct_ids is not None else None

    interventions_by_key: dict[tuple[str, str], dict[str, str | None]] = {}
    for row in iter_aact_rows(snapshot_dir, "interventions", nct_ids=nct_filter):
        interventions_by_key[(row["nct_id"].strip(), row["id"].strip())] = {
            "name": normalize_optional(row.get("name")),
            "intervention_type": normalize_optional(row.get("intervention_type")),
        }

    linked_interventions: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in iter_aact_rows(snapshot_dir, "design_group_interventions", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        design_group_id = row["design_group_id"].strip()
        intervention_id = row["intervention_id"].strip()
        intervention = interventions_by_key.get((nct_id, intervention_id))
        if intervention is None or not intervention.get("name"):
            continue
        linked_interventions[(nct_id, design_group_id)].append(str(intervention["name"]))

    records: list[dict[str, object]] = []
    for row in iter_aact_rows(snapshot_dir, "design_groups", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        design_group_id = row["id"].strip()
        title = normalize_optional(row.get("title"))
        interventions = sorted(
            {
                name
                for name in linked_interventions.get((nct_id, design_group_id), [])
                if name
            }
        )
        non_placebo_interventions = [
            name for name in interventions if not PLACEBO_RE.search(name)
        ]
        records.append(
            {
                "nct_id": nct_id,
                "design_group_id": design_group_id,
                "group_type": normalize_optional(row.get("group_type")),
                "arm_title": title,
                "intervention_names": ";".join(interventions),
                "active_intervention_names": ";".join(non_placebo_interventions),
                "arm_aliases": ";".join(_alias_variants(title, non_placebo_interventions or interventions)),
            }
        )

    return pd.DataFrame.from_records(records).sort_values(
        ["nct_id", "design_group_id"],
        kind="stable",
    ).reset_index(drop=True)


def load_participant_flow_text(
    snapshot_dir: Path,
    *,
    nct_ids: list[str] | set[str] | None = None,
) -> pd.DataFrame:
    """Load one normalized participant-flow narrative per NCT."""

    validate_arm_mapping_tables(snapshot_dir)
    nct_filter = set(nct_ids) if nct_ids is not None else None

    records: list[dict[str, str]] = []
    for row in iter_aact_rows(snapshot_dir, "participant_flows", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        text_parts = [
            normalize_optional(row.get("recruitment_details")),
            normalize_optional(row.get("pre_assignment_details")),
        ]
        joined = " ".join(part for part in text_parts if part)
        records.append(
            {
                "nct_id": nct_id,
                "participant_flow_text": joined,
                "participant_flow_text_norm": _normalize_text(joined),
            }
        )

    return pd.DataFrame.from_records(records).sort_values(
        ["nct_id"],
        kind="stable",
    ).reset_index(drop=True)


def load_trial_narrative_text(
    snapshot_dir: Path,
    *,
    nct_ids: list[str] | set[str] | None = None,
) -> pd.DataFrame:
    """Load a combined narrative corpus from participant flow and study text tables."""

    validate_arm_mapping_tables(snapshot_dir)
    nct_filter = set(nct_ids) if nct_ids is not None else None
    text_by_nct: dict[str, list[str]] = defaultdict(list)

    for row in iter_aact_rows(snapshot_dir, "participant_flows", nct_ids=nct_filter):
        nct_id = row["nct_id"].strip()
        parts = [
            normalize_optional(row.get("recruitment_details")),
            normalize_optional(row.get("pre_assignment_details")),
        ]
        joined = " ".join(part for part in parts if part)
        if joined:
            text_by_nct[nct_id].append(joined)

    for table_name in ("brief_summaries", "detailed_descriptions"):
        table_path = snapshot_dir / f"{table_name}.txt"
        if not table_path.exists():
            continue
        for row in iter_aact_rows(snapshot_dir, table_name, nct_ids=nct_filter):
            nct_id = row["nct_id"].strip()
            description = normalize_optional(row.get("description"))
            if description:
                text_by_nct[nct_id].append(description)

    records = [
        {
            "nct_id": nct_id,
            "narrative_text": " ".join(parts).strip(),
            "narrative_text_norm": _normalize_text(" ".join(parts)),
        }
        for nct_id, parts in sorted(text_by_nct.items())
        if any(part.strip() for part in parts)
    ]
    return pd.DataFrame.from_records(records).sort_values(
        ["nct_id"],
        kind="stable",
    ).reset_index(drop=True)


def build_narrative_arm_counts(
    design_arms: pd.DataFrame,
    participant_flows: pd.DataFrame,
) -> pd.DataFrame:
    """Extract arm-specific counts from participant-flow prose when exact."""

    if design_arms.empty or participant_flows.empty:
        return pd.DataFrame(
            columns=["nct_id", "design_group_id", "narrative_count", "matched_alias"]
        )

    flow_by_nct = participant_flows.set_index("nct_id")["participant_flow_text_norm"].to_dict()
    records: list[dict[str, object]] = []
    for row in design_arms.itertuples(index=False):
        text = flow_by_nct.get(row.nct_id, "")
        if not text:
            continue

        counts_by_alias: dict[str, set[int]] = {}
        aliases = [alias for alias in str(row.arm_aliases).split(";") if alias]
        for alias in aliases:
            counts = _nearest_alias_count(text, alias)
            if counts:
                counts_by_alias[alias] = counts

        resolved_pairs = [
            (alias, next(iter(counts)))
            for alias, counts in counts_by_alias.items()
            if len(counts) == 1
        ]
        unique_counts = {count for _, count in resolved_pairs}
        if len(unique_counts) != 1:
            continue
        matched_alias, count = sorted(resolved_pairs, key=lambda item: len(item[0]), reverse=True)[0]
        records.append(
            {
                "nct_id": row.nct_id,
                "design_group_id": row.design_group_id,
                "narrative_count": count,
                "matched_alias": matched_alias,
            }
        )

    return pd.DataFrame.from_records(records).sort_values(
        ["nct_id", "design_group_id"],
        kind="stable",
    ).reset_index(drop=True)


def _arm_specific_not_treated_counts(text: str, alias: str) -> set[int]:
    if not text or not alias:
        return set()

    pattern = re.compile(
        rf"(?P<count>\d{{1,5}})\s+patients?\b"
        rf"\s+(?:was|were)\s+randomized(?:\s+in\s+error)?"
        rf"\s+to\s+the\s+{re.escape(alias)}(?:\s+group)?\b"
        rf"(?:\s+\w+){{0,{ARM_SPECIFIC_EXCLUSION_WINDOW}}}?"
        rf"\s+(?:was|were)\s+not\s+treated\b"
    )
    return {int(match.group("count")) for match in pattern.finditer(text)}


def build_arm_specific_exclusion_counts(
    design_arms: pd.DataFrame,
    trial_narratives: pd.DataFrame,
) -> pd.DataFrame:
    """Extract explicit arm-specific untreated exclusions from narrative text."""

    if design_arms.empty or trial_narratives.empty:
        return pd.DataFrame(
            columns=["nct_id", "design_group_id", "excluded_count", "matched_alias"]
        )

    narrative_by_nct = trial_narratives.set_index("nct_id")["narrative_text_norm"].to_dict()
    records: list[dict[str, object]] = []
    for row in design_arms.itertuples(index=False):
        text = narrative_by_nct.get(row.nct_id, "")
        if not text:
            continue

        counts_by_alias: dict[str, set[int]] = {}
        aliases = [alias for alias in str(row.arm_aliases).split(";") if alias]
        for alias in aliases:
            counts = _arm_specific_not_treated_counts(text, alias)
            if counts:
                counts_by_alias[alias] = counts

        resolved_pairs = [
            (alias, next(iter(counts)))
            for alias, counts in counts_by_alias.items()
            if len(counts) == 1
        ]
        unique_counts = {count for _, count in resolved_pairs}
        if len(unique_counts) != 1:
            continue
        matched_alias, count = sorted(
            resolved_pairs,
            key=lambda item: len(item[0]),
            reverse=True,
        )[0]
        records.append(
            {
                "nct_id": row.nct_id,
                "design_group_id": row.design_group_id,
                "excluded_count": count,
                "matched_alias": matched_alias,
            }
        )

    if not records:
        return pd.DataFrame(
            columns=["nct_id", "design_group_id", "excluded_count", "matched_alias"]
        )

    return pd.DataFrame.from_records(records).sort_values(
        ["nct_id", "design_group_id"],
        kind="stable",
    ).reset_index(drop=True)


def build_group_code_catalog(
    snapshot_dir: Path,
    output_dir: Path,
    *,
    nct_ids: list[str] | set[str] | None = None,
) -> pd.DataFrame:
    """Aggregate counted and count-free group-code surfaces by trial."""

    validate_arm_mapping_tables(snapshot_dir)
    nct_filter = set(nct_ids) if nct_ids is not None else None
    records: list[dict[str, object]] = []

    for row in iter_aact_rows(snapshot_dir, "baseline_counts", nct_ids=nct_filter):
        group_code = normalize_optional(row.get("ctgov_group_code"))
        family, suffix = _group_code_parts(group_code or "")
        count_value = _count_int(row.get("count"))
        if group_code is None or family is None:
            continue
        records.append(
            {
                "nct_id": row["nct_id"].strip(),
                "ctgov_group_code": group_code,
                "result_group_id": normalize_optional(row.get("result_group_id")),
                "group_family": family,
                "group_suffix": suffix,
                "count_value": count_value,
                "count_source": "baseline_counts",
            }
        )

    for row in iter_aact_rows(snapshot_dir, "outcome_counts", nct_ids=nct_filter):
        group_code = normalize_optional(row.get("ctgov_group_code"))
        family, suffix = _group_code_parts(group_code or "")
        count_value = _count_int(row.get("count"))
        if group_code is None or family is None:
            continue
        records.append(
            {
                "nct_id": row["nct_id"].strip(),
                "ctgov_group_code": group_code,
                "result_group_id": normalize_optional(row.get("result_group_id")),
                "group_family": family,
                "group_suffix": suffix,
                "count_value": count_value,
                "count_source": "outcome_counts",
            }
        )

    for row in iter_aact_rows(snapshot_dir, "milestones", nct_ids=nct_filter):
        group_code = normalize_optional(row.get("ctgov_group_code"))
        family, suffix = _group_code_parts(group_code or "")
        count_value = _count_int(row.get("count"))
        if group_code is None or family is None:
            continue
        records.append(
            {
                "nct_id": row["nct_id"].strip(),
                "ctgov_group_code": group_code,
                "result_group_id": normalize_optional(row.get("result_group_id")),
                "group_family": family,
                "group_suffix": suffix,
                "count_value": count_value,
                "count_source": "milestones",
            }
        )

    for row in iter_aact_rows(snapshot_dir, "reported_event_totals", nct_ids=nct_filter):
        group_code = normalize_optional(row.get("ctgov_group_code"))
        family, suffix = _group_code_parts(group_code or "")
        count_value = _count_int(row.get("subjects_at_risk"))
        if group_code is None or family is None:
            continue
        records.append(
            {
                "nct_id": row["nct_id"].strip(),
                "ctgov_group_code": group_code,
                "result_group_id": None,
                "group_family": family,
                "group_suffix": suffix,
                "count_value": count_value,
                "count_source": "reported_event_totals",
            }
        )

    outcomes = pd.read_parquet(output_dir / "trial_outcomes_long.parquet")
    if nct_filter is not None:
        outcomes = outcomes.loc[outcomes["nct_id"].isin(nct_filter)].copy()
    codes = (
        outcomes.loc[outcomes["ctgov_group_code"].notna(), ["nct_id", "ctgov_group_code", "result_group_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    for row in codes.itertuples(index=False):
        family, suffix = _group_code_parts(str(row.ctgov_group_code))
        if family is None:
            continue
        records.append(
            {
                "nct_id": row.nct_id,
                "ctgov_group_code": row.ctgov_group_code,
                "result_group_id": row.result_group_id,
                "group_family": family,
                "group_suffix": suffix,
                "count_value": pd.NA,
                "count_source": "trial_outcomes",
            }
        )

    catalog = pd.DataFrame.from_records(records)
    if catalog.empty:
        return pd.DataFrame(
            columns=[
                "nct_id",
                "ctgov_group_code",
                "result_group_id",
                "group_family",
                "group_suffix",
                "count_value",
                "count_sources",
            ]
        )

    aggregated = (
        catalog.groupby(
            ["nct_id", "ctgov_group_code", "result_group_id", "group_family", "group_suffix"],
            dropna=False,
            sort=True,
        )
        .agg(
            count_value=("count_value", lambda s: max([value for value in s if pd.notna(value)], default=pd.NA)),
            count_sources=("count_source", lambda s: ";".join(sorted(set(str(value) for value in s if value)))),
        )
        .reset_index()
    )
    return aggregated.sort_values(
        ["nct_id", "group_family", "ctgov_group_code"],
        kind="stable",
    ).reset_index(drop=True)


def _distinct_family_code_counts(
    trial_codes: pd.DataFrame,
    family: str,
) -> pd.DataFrame:
    family_codes = trial_codes.loc[
        trial_codes["group_family"].eq(family) & trial_codes["count_value"].notna()
    ].copy()
    if family_codes.empty:
        return pd.DataFrame(
            columns=["ctgov_group_code", "group_suffix", "count_value", "count_sources"]
        )
    family_codes["count_value_num"] = pd.to_numeric(
        family_codes["count_value"],
        errors="coerce",
    )
    family_codes = family_codes.loc[family_codes["count_value_num"].notna()].copy()
    if family_codes.empty:
        return pd.DataFrame(
            columns=["ctgov_group_code", "group_suffix", "count_value", "count_sources"]
        )
    return (
        family_codes.groupby(["ctgov_group_code", "group_suffix"], sort=True, dropna=False)
        .agg(
            count_value=("count_value_num", "max"),
            count_sources=(
                "count_sources",
                lambda s: ";".join(
                    sorted(
                        {
                            source
                            for value in s
                            for source in str(value).split(";")
                            if source
                        }
                    )
                ),
            ),
        )
        .reset_index()
    )


def _near_exact_rank_matches(
    trial_narrative: pd.DataFrame,
    trial_codes: pd.DataFrame,
    family: str,
) -> list[tuple[pd.Series, pd.Series]]:
    if len(trial_narrative) != 2 or trial_narrative["narrative_count"].nunique() != 2:
        return []

    candidate_codes = _distinct_family_code_counts(trial_codes, family)
    if candidate_codes.empty:
        return []
    candidate_codes = candidate_codes.loc[candidate_codes["count_value"].gt(0)].copy()
    if len(candidate_codes) < 2:
        return []

    narrative_rows = [
        row
        for _, row in trial_narrative.sort_values(
            ["narrative_count", "design_group_id"],
            ascending=[False, True],
            kind="stable",
        ).iterrows()
    ]
    narrative_counts = [int(row["narrative_count"]) for row in narrative_rows]

    matches: list[list[tuple[pd.Series, pd.Series]]] = []
    code_records = [
        row
        for _, row in candidate_codes.sort_values(
            ["count_value", "ctgov_group_code"],
            ascending=[False, True],
            kind="stable",
        ).iterrows()
    ]
    for first_index in range(len(code_records)):
        for second_index in range(first_index + 1, len(code_records)):
            code_pair = [code_records[first_index], code_records[second_index]]
            code_pair = sorted(
                code_pair,
                key=lambda row: (-float(row["count_value"]), str(row["ctgov_group_code"])),
            )
            deltas = [
                narrative_counts[index] - int(code_pair[index]["count_value"])
                for index in range(2)
            ]
            if any(delta < 0 or delta > NEAR_EXACT_COUNT_DELTA for delta in deltas):
                continue
            if len(set(deltas)) != 1:
                continue
            if not all(
                any(
                    source in str(code_row["count_sources"]).split(";")
                    for source in ("outcome_counts", "baseline_counts")
                )
                for code_row in code_pair
            ):
                continue
            matches.append(
                [
                    (narrative_rows[index], code_pair[index])
                    for index in range(2)
                ]
            )

    if len(matches) != 1:
        return []
    return matches[0]


def _is_total_like_projection_code(
    code_row: pd.Series | object,
    anchor_suffix_map: dict[str, tuple[str, str, int]],
    narrative_counts: list[int],
) -> bool:
    count_values = pd.to_numeric(pd.Series([getattr(code_row, "count_value", pd.NA)]), errors="coerce")
    count_value = _first_value(count_values, pd.NA)
    if pd.isna(count_value):
        return False

    anchor_counts = [int(anchor[2]) for anchor in anchor_suffix_map.values() if anchor[2] is not None]
    candidate_totals = set()
    if len(anchor_counts) >= 2:
        candidate_totals.add(sum(anchor_counts))
    if len(narrative_counts) >= 2:
        candidate_totals.add(sum(narrative_counts))
    if not candidate_totals:
        return False

    reference_counts = [*anchor_counts, *narrative_counts]
    if not reference_counts:
        return False

    numeric_value = float(count_value)
    if numeric_value <= max(reference_counts):
        return False
    return any(abs(numeric_value - total) <= 1e-9 for total in candidate_totals)


def _exclusion_delta_suffix_match(
    trial_design: pd.DataFrame,
    trial_codes: pd.DataFrame,
    trial_exclusions: pd.DataFrame,
) -> tuple[dict[str, tuple[str, str, int]], str] | tuple[None, None]:
    if len(trial_design) != 2 or len(trial_exclusions) != 1:
        return None, None

    exclusion_row = _first_row(trial_exclusions)
    if exclusion_row is None:
        return None, None
    exclusion_count = int(exclusion_row["excluded_count"])
    named_design_group_id = str(exclusion_row["design_group_id"])
    other_design = trial_design.loc[~trial_design["design_group_id"].eq(named_design_group_id)].copy()
    if len(other_design) != 1:
        return None, None

    started = _distinct_family_code_counts(trial_codes, "FG")
    started = started.loc[started["count_value"].gt(0)].copy()
    if len(started) != 2 or started["group_suffix"].nunique() != 2:
        return None, None
    started_counts = {
        str(row["group_suffix"]): int(row["count_value"])
        for _, row in started.iterrows()
    }

    named_suffix_candidates: set[str] = set()
    for family in ("BG", "OG", "EG"):
        family_codes = _distinct_family_code_counts(trial_codes, family)
        family_codes = family_codes.loc[
            family_codes["count_value"].gt(0)
            & family_codes["group_suffix"].isin(started_counts)
        ].copy()
        if len(family_codes) != 2 or set(family_codes["group_suffix"]) != set(started_counts):
            continue

        deltas = {
            str(row["group_suffix"]): started_counts[str(row["group_suffix"])] - int(row["count_value"])
            for _, row in family_codes.iterrows()
        }
        if any(delta < 0 for delta in deltas.values()):
            continue
        if sorted(deltas.values()) != [0, exclusion_count]:
            continue
        suffixes = [suffix for suffix, delta in deltas.items() if delta == exclusion_count]
        if len(suffixes) != 1:
            continue
        named_suffix_candidates.add(suffixes[0])

    if len(named_suffix_candidates) != 1:
        return None, None

    named_suffix = next(iter(named_suffix_candidates))
    other_suffixes = [suffix for suffix in started_counts if suffix != named_suffix]
    if len(other_suffixes) != 1:
        return None, None
    other_suffix = other_suffixes[0]

    named_design = _first_row(trial_design.loc[trial_design["design_group_id"].eq(named_design_group_id)])
    other_design_row = _first_row(other_design)
    if named_design is None or other_design_row is None:
        return None, None
    suffix_map = {
        named_suffix: (
            str(named_design["design_group_id"]),
            str(named_design["arm_title"]),
            started_counts[named_suffix] - exclusion_count,
        ),
        other_suffix: (
            str(other_design_row["design_group_id"]),
            str(other_design_row["arm_title"]),
            started_counts[other_suffix],
        ),
    }
    return suffix_map, named_suffix


def build_arm_group_mapping(
    design_arms: pd.DataFrame,
    participant_flows: pd.DataFrame,
    group_codes: pd.DataFrame,
    trial_narratives: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map ctgov group codes onto design arms with explicit confidence tiers."""

    if design_arms.empty or group_codes.empty:
        return (
            pd.DataFrame(columns=ARM_MAPPING_COLUMNS),
            pd.DataFrame(columns=ARM_MAPPING_SUMMARY_COLUMNS),
        )

    narrative_counts = build_narrative_arm_counts(design_arms, participant_flows)
    effective_narratives = trial_narratives
    if effective_narratives is None:
        effective_narratives = participant_flows.rename(
            columns={
                "participant_flow_text": "narrative_text",
                "participant_flow_text_norm": "narrative_text_norm",
            }
        )
    exclusion_counts = build_arm_specific_exclusion_counts(design_arms, effective_narratives)
    design_lookup = {
        (row.nct_id, row.design_group_id): row
        for row in design_arms.itertuples(index=False)
    }
    participant_count_lookup = {
        (row.nct_id, row.design_group_id): int(row.narrative_count)
        for row in narrative_counts.itertuples(index=False)
    }

    mapping_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    all_ncts = sorted(set(design_arms["nct_id"]) | set(group_codes["nct_id"]))
    for nct_id in all_ncts:
        trial_design = design_arms.loc[design_arms["nct_id"].eq(nct_id)].copy()
        trial_codes = group_codes.loc[group_codes["nct_id"].eq(nct_id)].copy()
        if trial_design.empty or trial_codes.empty:
            continue

        trial_narrative = narrative_counts.loc[narrative_counts["nct_id"].eq(nct_id)].copy()
        trial_exclusions = exclusion_counts.loc[exclusion_counts["nct_id"].eq(nct_id)].copy()
        design_arm_count = int(len(trial_design))
        mapped_by_code: dict[str, dict[str, object]] = {}
        anchor_suffix_map: dict[str, tuple[str, str, int]] = {}
        narrative_count_values = [
            int(value)
            for value in trial_narrative["narrative_count"].dropna().astype(int).tolist()
        ]

        if design_arm_count == 1:
            suffixes = sorted(set(trial_codes["group_suffix"]))
            if len(suffixes) == 1:
                arm = _first_row(trial_design)
                if arm is None:
                    continue
                anchor_group_code = sorted(trial_codes["ctgov_group_code"])[0]
                anchor_count_value = _first_value(trial_codes["count_value"].dropna(), pd.NA)
                for code_row in trial_codes.itertuples(index=False):
                    mapped_by_code[code_row.ctgov_group_code] = {
                        "mapped_design_group_id": arm["design_group_id"],
                        "mapped_arm_title": arm["arm_title"],
                        "mapped_group_type": arm["group_type"],
                        "mapped_intervention_names": arm["active_intervention_names"] or arm["intervention_names"],
                        "mapping_status": "mapped",
                        "mapping_method": "direct_single_arm_suffix",
                        "mapping_confidence": "high",
                        "anchor_group_family": code_row.group_family,
                        "anchor_ctgov_group_code": anchor_group_code,
                        "anchor_count_value": anchor_count_value,
                    }
        elif len(trial_narrative) == design_arm_count and trial_narrative["narrative_count"].nunique() == design_arm_count:
            for family in ("BG", "FG", "EG"):
                family_codes = trial_codes.loc[
                    trial_codes["group_family"].eq(family) & trial_codes["count_value"].notna()
                ].copy()
                if family_codes.empty:
                    continue

                local_matches: list[tuple[pd.Series, pd.Series]] = []
                used_codes: set[str] = set()
                success = True
                for arm_row in trial_narrative.itertuples(index=False):
                    matches = family_codes.loc[family_codes["count_value"].eq(arm_row.narrative_count)]
                    matches = matches.loc[~matches["ctgov_group_code"].isin(used_codes)]
                    if len(matches) != 1:
                        success = False
                        break
                    code_row = _first_row(matches)
                    if code_row is None:
                        success = False
                        break
                    used_codes.add(str(code_row["ctgov_group_code"]))
                    local_matches.append((arm_row, code_row))

                if not success:
                    continue

                for arm_row, code_row in local_matches:
                    design_row = design_lookup[(nct_id, arm_row.design_group_id)]
                    mapped_by_code[str(code_row["ctgov_group_code"])] = {
                        "mapped_design_group_id": design_row.design_group_id,
                        "mapped_arm_title": design_row.arm_title,
                        "mapped_group_type": design_row.group_type,
                        "mapped_intervention_names": design_row.active_intervention_names or design_row.intervention_names,
                        "mapping_status": "mapped",
                        "mapping_method": f"narrative_count_match_{family.lower()}",
                        "mapping_confidence": "high",
                        "anchor_group_family": family,
                        "anchor_ctgov_group_code": code_row["ctgov_group_code"],
                        "anchor_count_value": arm_row.narrative_count,
                    }
                    anchor_suffix_map[str(code_row["group_suffix"])] = (
                        design_row.design_group_id,
                        design_row.arm_title,
                        arm_row.narrative_count,
                    )

            if not anchor_suffix_map and design_arm_count == 2:
                for family in ("OG", "BG"):
                    local_matches = _near_exact_rank_matches(trial_narrative, trial_codes, family)
                    if not local_matches:
                        continue
                    for arm_row, code_row in local_matches:
                        design_row = design_lookup[(nct_id, arm_row["design_group_id"])]
                        mapped_by_code[str(code_row["ctgov_group_code"])] = {
                            "mapped_design_group_id": design_row.design_group_id,
                            "mapped_arm_title": design_row.arm_title,
                            "mapped_group_type": design_row.group_type,
                            "mapped_intervention_names": design_row.active_intervention_names or design_row.intervention_names,
                            "mapping_status": "mapped",
                            "mapping_method": f"near_exact_rank_match_{family.lower()}",
                            "mapping_confidence": "medium",
                            "anchor_group_family": family,
                            "anchor_ctgov_group_code": code_row["ctgov_group_code"],
                            "anchor_count_value": int(code_row["count_value"]),
                        }
                        anchor_suffix_map[str(code_row["group_suffix"])] = (
                            design_row.design_group_id,
                            design_row.arm_title,
                            int(code_row["count_value"]),
                        )
                    break

        if not anchor_suffix_map and design_arm_count == 2:
            exclusion_suffix_map, named_suffix = _exclusion_delta_suffix_match(
                trial_design,
                trial_codes,
                trial_exclusions,
            )
            if exclusion_suffix_map:
                for code_row in trial_codes.itertuples(index=False):
                    suffix = str(code_row.group_suffix)
                    anchor = exclusion_suffix_map.get(suffix)
                    if anchor is None:
                        continue
                    if _is_total_like_projection_code(code_row, exclusion_suffix_map, narrative_count_values):
                        continue
                    design_group_id, _, anchor_count = anchor
                    design_row = design_lookup[(nct_id, design_group_id)]
                    if code_row.group_family == "FG":
                        method = "arm_specific_exclusion_delta_match_fg"
                    else:
                        method = "suffix_projection_from_anchor"
                    count_value = (
                        int(code_row.count_value)
                        if pd.notna(code_row.count_value)
                        else anchor_count
                    )
                    mapped_by_code[code_row.ctgov_group_code] = {
                        "mapped_design_group_id": design_row.design_group_id,
                        "mapped_arm_title": design_row.arm_title,
                        "mapped_group_type": design_row.group_type,
                        "mapped_intervention_names": design_row.active_intervention_names or design_row.intervention_names,
                        "mapping_status": "mapped",
                        "mapping_method": method,
                        "mapping_confidence": "medium",
                        "anchor_group_family": (
                            "FG"
                            if code_row.group_family == "FG"
                            else f"FG_to_{code_row.group_family}"
                        ),
                        "anchor_ctgov_group_code": (
                            code_row.ctgov_group_code
                            if code_row.group_family == "FG"
                            else f"FG{suffix}"
                        ),
                        "anchor_count_value": count_value,
                    }
                anchor_suffix_map = exclusion_suffix_map.copy()

        if anchor_suffix_map:
            for code_row in trial_codes.itertuples(index=False):
                if code_row.ctgov_group_code in mapped_by_code:
                    continue
                anchor = anchor_suffix_map.get(str(code_row.group_suffix))
                if anchor is None:
                    continue
                if _is_total_like_projection_code(code_row, anchor_suffix_map, narrative_count_values):
                    continue
                design_group_id, _, anchor_count = anchor
                design_row = design_lookup[(nct_id, design_group_id)]
                count_value = (
                    int(code_row.count_value)
                    if pd.notna(code_row.count_value)
                    else anchor_count
                )
                mapped_by_code[code_row.ctgov_group_code] = {
                    "mapped_design_group_id": design_row.design_group_id,
                    "mapped_arm_title": design_row.arm_title,
                    "mapped_group_type": design_row.group_type,
                    "mapped_intervention_names": design_row.active_intervention_names or design_row.intervention_names,
                    "mapping_status": "mapped",
                    "mapping_method": "suffix_projection_from_anchor",
                    "mapping_confidence": "medium",
                    "anchor_group_family": None,
                    "anchor_ctgov_group_code": None,
                    "anchor_count_value": count_value,
                }

        for code_row in trial_codes.itertuples(index=False):
            mapped = mapped_by_code.get(code_row.ctgov_group_code)
            if mapped is None:
                mapping_rows.append(
                    {
                        "nct_id": nct_id,
                        "ctgov_group_code": code_row.ctgov_group_code,
                        "group_family": code_row.group_family,
                        "group_suffix": code_row.group_suffix,
                        "mapped_design_group_id": pd.NA,
                        "mapped_arm_title": pd.NA,
                        "mapped_group_type": pd.NA,
                        "mapped_intervention_names": pd.NA,
                        "mapping_status": "unmapped",
                        "mapping_method": "fail_closed_no_anchor",
                        "mapping_confidence": "none",
                        "anchor_group_family": pd.NA,
                        "anchor_ctgov_group_code": pd.NA,
                        "anchor_count_value": pd.NA,
                    }
                )
            else:
                mapping_rows.append(
                    {
                        "nct_id": nct_id,
                        "ctgov_group_code": code_row.ctgov_group_code,
                        "group_family": code_row.group_family,
                        "group_suffix": code_row.group_suffix,
                        **mapped,
                    }
                )

        trial_mapping = [row for row in mapping_rows if row["nct_id"] == nct_id]
        mapped_codes = [row["ctgov_group_code"] for row in trial_mapping if row["mapping_status"] == "mapped"]
        total_codes = [row["ctgov_group_code"] for row in trial_mapping]
        mapped_titles = sorted(
            {
                str(row["mapped_arm_title"])
                for row in trial_mapping
                if row["mapping_status"] == "mapped" and pd.notna(row["mapped_arm_title"])
            }
        )
        methods = sorted(
            {
                str(row["mapping_method"])
                for row in trial_mapping
                if row["mapping_status"] == "mapped"
            }
        )
        unmapped_codes = sorted(
            {
                str(row["ctgov_group_code"])
                for row in trial_mapping
                if row["mapping_status"] != "mapped"
            }
        )
        summary_rows.append(
            {
                "nct_id": nct_id,
                "design_arm_count": design_arm_count,
                "narrative_count_arm_count": int(len(trial_narrative)),
                "mapped_group_code_count": len(mapped_codes),
                "total_group_code_count": len(total_codes),
                "mapping_coverage": len(mapped_codes) / len(total_codes) if total_codes else 0.0,
                "mapping_methods": ";".join(methods),
                "mapped_arm_titles": ";".join(mapped_titles),
                "unmapped_group_codes": ";".join(unmapped_codes),
            }
        )

    mapping = pd.DataFrame.from_records(mapping_rows, columns=ARM_MAPPING_COLUMNS)
    summary = pd.DataFrame.from_records(summary_rows, columns=ARM_MAPPING_SUMMARY_COLUMNS)
    return (
        mapping.sort_values(["nct_id", "group_family", "ctgov_group_code"], kind="stable").reset_index(drop=True),
        summary.sort_values(["mapping_coverage", "nct_id"], ascending=[False, True], kind="stable").reset_index(drop=True),
    )


def render_arm_mapping_report(
    design_arms: pd.DataFrame,
    summary: pd.DataFrame,
    mapping: pd.DataFrame,
) -> str:
    """Render a compact Markdown summary for the arm-mapping proof-of-concept."""

    total_trials = int(design_arms["nct_id"].nunique()) if not design_arms.empty else 0
    mapped_trials = int(summary["mapped_group_code_count"].gt(0).sum()) if not summary.empty else 0
    full_trials = int(summary["mapping_coverage"].eq(1.0).sum()) if not summary.empty else 0
    total_codes = int(len(mapping))
    mapped_codes = int(mapping["mapping_status"].eq("mapped").sum()) if not mapping.empty else 0

    lines = [
        "# Arm Mapping Proof of Concept",
        "",
        "- total_trials_with_design_arms: " + str(total_trials),
        "- trials_with_any_group_mapping: " + str(mapped_trials),
        "- fully_mapped_trials: " + str(full_trials),
        "- total_group_codes: " + str(total_codes),
        "- mapped_group_codes: " + str(mapped_codes),
        "",
        "## Highest coverage trials",
        "",
        "| Trial | Coverage | Mapped codes | Total codes | Methods |",
        "|---|---:|---:|---:|---|",
    ]

    for row in summary.head(15).itertuples(index=False):
        lines.append(
            f"| {row.nct_id} | {float(row.mapping_coverage):.3f} | {int(row.mapped_group_code_count)} | "
            f"{int(row.total_group_code_count)} | {row.mapping_methods or 'none'} |"
        )

    return "\n".join(lines) + "\n"


def materialize_arm_mapping_outputs(
    output_dir: Path | str,
    *,
    snapshot_dir: Path | str,
) -> dict[str, object]:
    """Write arm-mapping outputs for one topic output directory."""

    output_path = Path(output_dir)
    snapshot_path = Path(snapshot_dir)
    validate_arm_mapping_tables(snapshot_path)

    outcomes = pd.read_parquet(output_path / "trial_outcomes_long.parquet")
    nct_ids = sorted(set(outcomes["nct_id"]))

    design_arms = load_design_arm_catalog(snapshot_path, nct_ids=nct_ids)
    participant_flows = load_participant_flow_text(snapshot_path, nct_ids=nct_ids)
    trial_narratives = load_trial_narrative_text(snapshot_path, nct_ids=nct_ids)
    group_codes = build_group_code_catalog(snapshot_path, output_path, nct_ids=nct_ids)
    mapping, summary = build_arm_group_mapping(
        design_arms,
        participant_flows,
        group_codes,
        trial_narratives,
    )
    report = render_arm_mapping_report(design_arms, summary, mapping)

    design_path = output_path / "arm_design_catalog.parquet"
    mapping_path = output_path / "arm_group_mapping.parquet"
    summary_path = output_path / "arm_mapping_summary.parquet"
    report_path = output_path / "arm_mapping_report.md"
    manifest_path = output_path / "arm_mapping_manifest.json"

    design_arms.to_parquet(design_path, index=False)
    mapping.to_parquet(mapping_path, index=False)
    summary.to_parquet(summary_path, index=False)
    report_path.write_text(report, encoding="utf-8")

    manifest_seed = {
        "trial_count": int(design_arms["nct_id"].nunique()) if not design_arms.empty else 0,
        "mapped_trial_count": int(summary["mapped_group_code_count"].gt(0).sum()) if not summary.empty else 0,
        "mapped_group_code_count": int(mapping["mapping_status"].eq("mapped").sum()) if not mapping.empty else 0,
        "total_group_code_count": int(len(mapping)),
    }
    manifest = {
        **manifest_seed,
        "arm_mapping_manifest_id": hashlib.sha256(
            json.dumps(manifest_seed, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "outputs": {
            "arm_design_catalog": str(design_path),
            "arm_group_mapping": str(mapping_path),
            "arm_mapping_summary": str(summary_path),
            "arm_mapping_report": str(report_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
