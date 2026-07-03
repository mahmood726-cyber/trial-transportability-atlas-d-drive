# sentinel:skip-file — hardcoded paths / templated placeholders are fixture/registry/audit-narrative data for this repo's research workflow, not portable application configuration. Same pattern as push_all_repos.py and E156 workbook files.
"""Transportability scoring and evidence-gap summaries."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.contracts import SYNTHESIS_OUTPUT_CONTRACT
from trial_transportability_atlas.context_join import enrich_trial_country_year_iso3
from trial_transportability_atlas.topics import PHASE1_TOPIC, TopicSpec, resolve_topic_spec
from trial_transportability_atlas.source_adapters import (
    SourceAdapterError,
    load_unified_context,
)
from trial_transportability_atlas.project_paths import (
    MissingRequiredPathError,
    discover_external_paths,
)


@dataclass(frozen=True)
class TransportSignalSpec:
    """One curated signal used in phase-1 transportability scoring."""

    key: str
    source: str
    measure: str
    metric: str | None = None
    sex: str | None = None
    age_group: str | None = None


CORE_SIGNAL_SPECS = (
    TransportSignalSpec(
        key="daly_rate",
        source="ihme_burden",
        measure="DALYs (Disability-Adjusted Life Years)",
        metric="Rate",
        sex="Both",
        age_group="All ages",
    ),
    TransportSignalSpec(
        key="death_rate",
        source="ihme_burden",
        measure="Deaths",
        metric="Rate",
        sex="Both",
        age_group="All ages",
    ),
    TransportSignalSpec(
        key="population",
        source="ihme_population",
        measure="population",
        metric="Number",
        sex="Both",
        age_group="All ages",
    ),
    TransportSignalSpec(
        key="sdi",
        source="ihme_sdi",
        measure="sdi",
        metric="sdi",
        sex="Both",
        age_group="All Ages",
    ),
    TransportSignalSpec(
        key="suicide_rate",
        source="who_gho",
        measure="Crude suicide rates (per 100 000 population)",
        metric="SDGSUICIDE",
        sex="Both",
        age_group="YEARSALL",
    ),
    TransportSignalSpec(
        key="alcohol_per_capita",
        source="who_gho",
        measure="Alcohol, total per capita (15+) consumption (in litres of pure alcohol) (SDG Indicator 3.5.2), three-year average",
        metric="SA_0000001688",
        sex="Both",
        age_group=None,
    ),
    TransportSignalSpec(
        key="health_expenditure_gdp",
        source="who_ghed",
        measure="Current health expenditure (% of GDP)",
        metric="who_ghed_che_gdp",
    ),
    TransportSignalSpec(
        key="out_of_pocket_share",
        source="who_ghed",
        measure="Out-of-pocket expenditure (% of current health expenditure)",
        metric="who_ghed_oops_che",
    ),
    TransportSignalSpec(
        key="governance_effectiveness",
        source="wb_governance",
        measure="Government Effectiveness: Estimate",
        metric="wb_GE.EST",
    ),
    TransportSignalSpec(
        key="gini_index",
        source="wb_poverty",
        measure="Gini index",
        metric="wb_SI.POV.GINI",
    ),
    TransportSignalSpec(
        key="physicians_per_1000",
        source="wb_hnp",
        measure="Physicians (per 1,000 people)",
        metric="wb_SH.MED.PHYS.ZS",
    ),
)


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...], *, label: str) -> None:
    """Raise a clear error if ``frame`` is missing any required column."""

    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{label} must be a pandas DataFrame, got {type(frame).__name__}.")
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        available = ", ".join(map(str, frame.columns)) or "<none>"
        raise ValueError(
            f"{label} is missing required column(s): {', '.join(missing)}. "
            f"Available columns: {available}."
        )


TRIAL_COUNTRY_YEAR_REQUIRED_COLUMNS = ("nct_id", "country_name", "iso3", "year")
EFFECT_CANDIDATES_REQUIRED_COLUMNS = (
    "candidate_id",
    "nct_id",
    "candidate_family",
    "comparable_flag",
)
CONTEXT_REQUIRED_COLUMNS = ("year", "source", "measure", "metric", "sex", "age_group", "value")


def _join_unique(values: pd.Series) -> str:
    seen = sorted(
        {
            str(value).strip()
            for value in values
            if value is not None and not pd.isna(value) and str(value).strip()
        }
    )
    return ";".join(seen)


def _split_joined(value: object) -> set[str]:
    if value is None or pd.isna(value):
        return set()
    return {part for part in str(value).split(";") if part}


def _normalize_context_frame(context: pd.DataFrame) -> pd.DataFrame:
    """Return a signal-selection frame keyed by a canonical ``iso3`` column.

    Accepts both the raw unified ``context_long`` schema (keyed by ``iso3``)
    and the per-country-year ``context_joined`` schema (keyed by
    ``iso3_resolved`` and carrying a ``context_available_flag``). When the
    availability flag is present, only rows flagged as available are retained
    — this preserves the historical ``context_joined`` selection semantics
    without changing any selected value for the flag-free ``context_long``
    path used in production.
    """

    frame = context
    if "context_available_flag" in frame.columns:
        frame = frame.loc[frame["context_available_flag"].astype("boolean").fillna(False)]
    if "iso3" not in frame.columns:
        if "iso3_resolved" not in frame.columns:
            raise KeyError(
                "context frame must expose an 'iso3' or 'iso3_resolved' column; "
                f"available: {', '.join(map(str, context.columns))}"
            )
        frame = frame.rename(columns={"iso3_resolved": "iso3"})
    return frame.copy()


def _resolve_context_argument(
    context_long: pd.DataFrame | None,
    context_joined: pd.DataFrame | None,
) -> pd.DataFrame:
    """Accept the ``context_long`` or the legacy ``context_joined`` keyword."""

    if context_long is not None and context_joined is not None:
        raise TypeError("Pass either 'context_long' or 'context_joined', not both.")
    resolved = context_long if context_long is not None else context_joined
    if resolved is None:
        raise TypeError("A context frame is required ('context_long' or 'context_joined').")
    return resolved


def _preference_rank(series: pd.Series, preferred: str | None) -> pd.Series:
    if preferred is None:
        return series.notna().astype(int)
    rank = pd.Series(2, index=series.index, dtype="int64")
    rank = rank.where(series.notna(), 1)
    return rank.where(series != preferred, 0)


def _build_nct_evidence_summary(effect_candidates: pd.DataFrame) -> pd.DataFrame:
    if effect_candidates.empty:
        return pd.DataFrame(
            columns=[
                "nct_id",
                "total_candidate_count",
                "comparable_candidate_count",
                "comparable_trial_flag",
                "comparable_family_count",
                "comparable_families",
            ]
        )

    frame = effect_candidates.copy()
    comparable = frame.loc[frame["comparable_flag"]].copy()

    summary = frame.groupby("nct_id", sort=True).agg(
        total_candidate_count=("candidate_id", "size"),
        comparable_candidate_count=("comparable_flag", "sum"),
    )
    family_labels = comparable.groupby("nct_id", sort=True)["candidate_family"].agg(_join_unique)
    family_counts = comparable.groupby("nct_id", sort=True)["candidate_family"].nunique()

    summary["comparable_trial_flag"] = summary["comparable_candidate_count"].gt(0)
    summary["comparable_family_count"] = summary.index.map(family_counts).fillna(0).astype(int)
    summary["comparable_families"] = summary.index.map(family_labels).fillna("")
    return summary.reset_index()


def build_country_year_context_signals(
    trial_country_year: pd.DataFrame,
    context_long: pd.DataFrame | None = None,
    *,
    context_joined: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Select one curated signal value per country-year with LVCF support.

    The context frame may be supplied either as the raw unified
    ``context_long`` surface or, for backward compatibility, as the
    per-country-year ``context_joined`` surface. See
    :func:`_normalize_context_frame` for the accepted schemas.
    """

    _require_columns(
        trial_country_year,
        TRIAL_COUNTRY_YEAR_REQUIRED_COLUMNS,
        label="trial_country_year",
    )
    resolved_context = _resolve_context_argument(context_long, context_joined)
    _require_columns(resolved_context, CONTEXT_REQUIRED_COLUMNS, label="context frame")
    context = _normalize_context_frame(resolved_context)

    base = enrich_trial_country_year_iso3(trial_country_year)
    country_years = (
        base[["country_name", "iso3_resolved", "year"]]
        .drop_duplicates()
        .rename(columns={"iso3_resolved": "iso3"})
        .sort_values(["country_name", "year"], kind="stable")
        .reset_index(drop=True)
    )

    result = country_years.copy()
    for spec in CORE_SIGNAL_SPECS:
        candidates = context.loc[
            context["source"].eq(spec.source)
            & context["measure"].eq(spec.measure)
        ].copy()
        if candidates.empty:
            result[f"signal_{spec.key}"] = pd.NA
            continue

        candidates["metric_rank"] = _preference_rank(candidates["metric"], spec.metric)
        candidates["sex_rank"] = _preference_rank(candidates["sex"], spec.sex)
        candidates["age_rank"] = _preference_rank(candidates["age_group"], spec.age_group)
        
        # Select best signal per country-year from the full context pool
        # First, match exact years
        selected = (
            candidates.sort_values(
                ["iso3", "year", "metric_rank", "sex_rank", "age_rank"],
                kind="stable",
            )
            .drop_duplicates(["iso3", "year"], keep="first")
            .rename(
                columns={
                    "value": f"signal_{spec.key}",
                }
            )[["iso3", "year", f"signal_{spec.key}"]]
        )
        
        # Merge onto result (exact year matches)
        result = result.merge(selected, how="left", on=["iso3", "year"])
        
        # For missing years, we need to fill from other years in the pool
        # We'll create a per-country 'latest available' lookup
        latest_available = (
            candidates.sort_values(
                ["iso3", "year", "metric_rank", "sex_rank", "age_rank"],
                kind="stable",
            )
            .drop_duplicates(["iso3"], keep="last")
            .rename(columns={"value": f"fill_{spec.key}"})
            [["iso3", f"fill_{spec.key}"]]
        )
        
        result = result.merge(latest_available, how="left", on="iso3")
        result[f"signal_{spec.key}"] = result[f"signal_{spec.key}"].fillna(result[f"fill_{spec.key}"])
        result.drop(columns=[f"fill_{spec.key}"], inplace=True)

    return result


def build_country_year_transportability(
    trial_country_year: pd.DataFrame,
    effect_candidates: pd.DataFrame,
    context_long: pd.DataFrame | None = None,
    *,
    context_joined: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Score trial-footprint country-years for transportability readiness.

    Accepts the context surface as either ``context_long`` or the legacy
    ``context_joined`` keyword.
    """

    _require_columns(
        effect_candidates,
        EFFECT_CANDIDATES_REQUIRED_COLUMNS,
        label="effect_candidates",
    )
    context = _resolve_context_argument(context_long, context_joined)
    evidence_by_nct = _build_nct_evidence_summary(effect_candidates)
    country_year_signals = build_country_year_context_signals(
        trial_country_year=trial_country_year,
        context_long=context,
    )

    trial_rows = enrich_trial_country_year_iso3(trial_country_year)
    trial_rows = trial_rows.merge(evidence_by_nct, how="left", on="nct_id")
    for column in (
        "total_candidate_count",
        "comparable_candidate_count",
        "comparable_family_count",
    ):
        trial_rows[column] = trial_rows[column].fillna(0).astype(int)
    trial_rows["comparable_trial_flag"] = (
        trial_rows["comparable_trial_flag"]
        .astype("boolean")
        .fillna(False)
        .astype(bool)
    )
    trial_rows["comparable_families"] = trial_rows["comparable_families"].fillna("")

    grouped = trial_rows.groupby(["iso3_resolved", "country_name", "year"], sort=True)
    country_year = grouped.agg(
        trial_count=("nct_id", "nunique"),
        total_candidate_count=("total_candidate_count", "sum"),
        comparable_candidate_count=("comparable_candidate_count", "sum"),
    ).reset_index()
    country_year["trial_nct_ids"] = grouped["nct_id"].agg(_join_unique).values
    country_year["comparable_trial_count"] = grouped["comparable_trial_flag"].agg(lambda s: int(s.sum())).values
    comparable_nct_ids = (
        trial_rows.loc[trial_rows["comparable_trial_flag"]]
        .groupby(["iso3_resolved", "country_name", "year"], sort=True)["nct_id"]
        .agg(_join_unique)
    )
    country_year["comparable_nct_ids"] = [
        comparable_nct_ids.get((row.iso3_resolved, row.country_name, row.year), "")
        for row in country_year.itertuples(index=False)
    ]
    country_year["comparable_family_count"] = grouped["comparable_families"].agg(
        lambda values: len(set().union(*(_split_joined(value) for value in values)))
    ).values

    merged = country_year.rename(columns={"iso3_resolved": "iso3"}).merge(
        country_year_signals,
        how="left",
        on=["iso3", "country_name", "year"],
    )

    signal_columns = [f"signal_{spec.key}" for spec in CORE_SIGNAL_SPECS]
    merged["available_core_signal_count"] = merged[signal_columns].notna().sum(axis=1).astype(int)
    merged["expected_core_signal_count"] = len(CORE_SIGNAL_SPECS)
    merged["country_coverage_score"] = (
        merged["available_core_signal_count"] / merged["expected_core_signal_count"]
    )
    merged["context_distance"] = 1.0 - merged["country_coverage_score"]
    merged["eligibility_support_score"] = (
        merged["comparable_trial_count"] / merged["trial_count"]
    ).fillna(0.0)
    merged["reporting_completeness_score"] = (
        merged["comparable_candidate_count"] / merged["total_candidate_count"]
    ).fillna(0.0)
    merged["transportability_score"] = merged[
        [
            "country_coverage_score",
            "eligibility_support_score",
            "reporting_completeness_score",
        ]
    ].mean(axis=1)
    merged["priority_gap_score"] = 1.0 - merged["transportability_score"]
    merged["effect_status"] = merged["comparable_candidate_count"].gt(0).map(
        {True: "comparable_evidence_available", False: "no_comparable_evidence"}
    )

    merged["available_core_signals"] = merged.apply(
        lambda row: ";".join(
            spec.key for spec in CORE_SIGNAL_SPECS if pd.notna(row[f"signal_{spec.key}"])
        ),
        axis=1,
    )
    merged["missing_core_signals"] = merged.apply(
        lambda row: ";".join(
            spec.key for spec in CORE_SIGNAL_SPECS if pd.isna(row[f"signal_{spec.key}"])
        ),
        axis=1,
    )

    sort_columns = ["priority_gap_score", "country_name", "year"]
    return merged.sort_values(sort_columns, ascending=[False, True, True], kind="stable").reset_index(drop=True)


def build_synthesis_output(
    country_year_transportability: pd.DataFrame,
    *,
    topic: TopicSpec,
    source_manifest_id: str,
) -> pd.DataFrame:
    """Add the canonical synthesis-output contract columns to transportability rows."""

    synthesis = country_year_transportability.copy()
    synthesis["intervention_name"] = topic.intervention_label
    synthesis["condition_name"] = topic.condition_label
    synthesis["effect_measure"] = pd.Series([pd.NA] * len(synthesis), dtype="object")
    synthesis["effect_value"] = pd.Series([pd.NA] * len(synthesis), dtype="Float64")
    synthesis["effect_precision"] = pd.Series([pd.NA] * len(synthesis), dtype="Float64")
    synthesis["source_manifest_id"] = source_manifest_id

    contract_columns = list(SYNTHESIS_OUTPUT_CONTRACT.required_columns)
    extra_columns = [column for column in synthesis.columns if column not in contract_columns]
    synthesis = synthesis[contract_columns + extra_columns]
    SYNTHESIS_OUTPUT_CONTRACT.validate_columns(synthesis.columns)
    return synthesis


def build_evidence_gap_summary(country_year_transportability: pd.DataFrame) -> pd.DataFrame:
    """Aggregate country-year transportability rows into one country summary row."""

    if country_year_transportability.empty:
        return pd.DataFrame(
            columns=[
                "iso3",
                "country_name",
                "trial_year_count",
                "trial_count",
                "comparable_trial_count",
                "comparable_candidate_count",
                "latest_year",
                "latest_transportability_score",
                "latest_priority_gap_score",
                "mean_country_coverage_score",
                "mean_eligibility_support_score",
                "mean_reporting_completeness_score",
                "mean_transportability_score",
                "max_priority_gap_score",
                "missing_core_signals_union",
                "trial_nct_ids",
                "comparable_nct_ids",
            ]
        )

    rows: list[dict[str, object]] = []
    for (iso3, country_name), group in country_year_transportability.groupby(
        ["iso3", "country_name"],
        sort=True,
    ):
        ordered = group.sort_values(["year"], kind="stable")
        latest = next(ordered.tail(1).itertuples(index=False), None)
        if latest is None:
            continue
        rows.append(
            {
                "iso3": iso3,
                "country_name": country_name,
                "trial_year_count": int(group["year"].nunique()),
                "trial_count": len(set().union(*(_split_joined(value) for value in group["trial_nct_ids"]))),
                "comparable_trial_count": len(
                    set().union(*(_split_joined(value) for value in group["comparable_nct_ids"]))
                ),
                "comparable_candidate_count": int(group["comparable_candidate_count"].sum()),
                "latest_year": int(latest.year),
                "latest_transportability_score": float(latest.transportability_score),
                "latest_priority_gap_score": float(latest.priority_gap_score),
                "mean_country_coverage_score": float(group["country_coverage_score"].mean()),
                "mean_eligibility_support_score": float(group["eligibility_support_score"].mean()),
                "mean_reporting_completeness_score": float(group["reporting_completeness_score"].mean()),
                "mean_transportability_score": float(group["transportability_score"].mean()),
                "max_priority_gap_score": float(group["priority_gap_score"].max()),
                "missing_core_signals_union": _join_unique(
                    pd.Series(sorted(set().union(*(_split_joined(value) for value in group["missing_core_signals"]))))
                ),
                "trial_nct_ids": _join_unique(group["trial_nct_ids"]),
                "comparable_nct_ids": _join_unique(group["comparable_nct_ids"]),
            }
        )

    summary = pd.DataFrame.from_records(rows)
    return summary.sort_values(
        ["latest_priority_gap_score", "country_name"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def render_evidence_gap_summary_markdown(summary: pd.DataFrame) -> str:
    """Render a compact Markdown summary for the highest-gap rows."""

    top = summary.head(15).copy()
    lines = [
        "# Evidence Gap Summary",
        "",
        "Generated from the phase-1 `sacubitril/valsartan in HFrEF` local atlas outputs.",
        "",
        "Core scoring signals:",
        "",
    ]
    lines.extend(f"- `{spec.key}`" for spec in CORE_SIGNAL_SPECS)
    lines.extend(
        [
            "",
            "Scoring formula:",
            "",
            "- `country_coverage_score = available_core_signal_count / expected_core_signal_count`",
            "- `eligibility_support_score = comparable_trial_count / trial_count`",
            "- `reporting_completeness_score = comparable_candidate_count / total_candidate_count`",
            "- `transportability_score = mean(country_coverage_score, eligibility_support_score, reporting_completeness_score)`",
            "- `priority_gap_score = 1 - transportability_score`",
            "",
            "Highest latest-year gaps:",
            "",
            "| Country | ISO3 | Latest Year | Trial Count | Comparable Trials | Coverage | Transportability | Missing Core Signals |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for _, row in top.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["country_name"]),
                    str(row["iso3"]),
                    str(int(row["latest_year"])),
                    str(int(row["trial_count"])),
                    str(int(row["comparable_trial_count"])),
                    f"{float(row['mean_country_coverage_score']):.3f}",
                    f"{float(row['latest_transportability_score']):.3f}",
                    str(row["missing_core_signals_union"] or "-"),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def _hash_source_paths(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        if not path.exists():
            continue
        digest.update(str(path.name).encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _load_unified_context_or_none() -> pd.DataFrame | None:
    """Load the full unified context surface, or ``None`` if it is unavailable.

    Returns ``None`` when the external source repos cannot be resolved
    (:class:`MissingRequiredPathError`) or when a resolved repo is missing the
    required dataset files (:class:`SourceAdapterError`), so callers can fall
    back to the local joined context surface.
    """

    try:
        paths = discover_external_paths()
        return load_unified_context(
            ihme_repo_root=paths.ihme_repo,
            wb_repo_root=paths.wb_repo,
            who_repo_root=paths.who_repo,
        )
    except (MissingRequiredPathError, SourceAdapterError):
        return None


def _load_materialize_context(
    *,
    context_joined_path: Path,
    use_unified_context: bool | None,
) -> pd.DataFrame:
    """Resolve the context surface used by :func:`materialize_transportability_outputs`."""

    if use_unified_context is True:
        context = _load_unified_context_or_none()
        if context is None:
            raise MissingRequiredPathError(
                "use_unified_context=True requires the IHME/WHO/WB source repos, "
                "but they could not be resolved."
            )
        return context

    if use_unified_context is None:
        context = _load_unified_context_or_none()
        if context is not None:
            return context

    if not context_joined_path.exists():
        raise FileNotFoundError(
            f"Local context surface not found: {context_joined_path}. "
            "Provide the joined context parquet or set use_unified_context=True "
            "with the source repos available."
        )
    return pd.read_parquet(context_joined_path)


def _resolve_transport_topic(trial_output_dir: Path, explicit_topic: TopicSpec | None) -> TopicSpec:
    if explicit_topic is not None:
        return explicit_topic

    run_manifest_path = trial_output_dir / "run_manifest.json"
    if run_manifest_path.exists():
        run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
        slug = run_manifest.get("topic_slug")
        if slug:
            return resolve_topic_spec(str(slug))
    return PHASE1_TOPIC


def materialize_transportability_outputs(
    trial_output_dir: Path,
    *,
    topic: TopicSpec | None = None,
    use_unified_context: bool | None = None,
) -> dict[str, object]:
    """Write transportability-scored country-year outputs from existing parquet surfaces.

    Context resolution:

    - ``use_unified_context=True`` forces loading the full unified
      ``context_long`` surface (enables cross-year LVCF fill) and raises if
      the external IHME/WHO/WB repos cannot be resolved.
    - ``use_unified_context=False`` uses only the local
      ``context_joined.parquet`` written alongside the trial surfaces
      (fully offline; exact-year selection with per-country carry-forward
      limited to the joined rows).
    - ``use_unified_context=None`` (default) prefers the unified surface when
      the external repos resolve, and otherwise falls back to the local
      ``context_joined.parquet``.
    """

    trial_output_dir = Path(trial_output_dir)
    trial_country_year_path = trial_output_dir / "trial_country_year.parquet"
    effect_candidates_path = trial_output_dir / "effect_candidates.parquet"
    context_joined_path = trial_output_dir / "context_joined.parquet"
    run_manifest_path = trial_output_dir / "run_manifest.json"
    context_manifest_path = trial_output_dir / "context_join_manifest.json"

    trial_country_year = pd.read_parquet(trial_country_year_path)
    effect_candidates = pd.read_parquet(effect_candidates_path)

    context_long = _load_materialize_context(
        context_joined_path=context_joined_path,
        use_unified_context=use_unified_context,
    )

    resolved_topic = _resolve_transport_topic(trial_output_dir, topic)
    source_manifest_id = _hash_source_paths(
        [
            run_manifest_path,
            context_manifest_path,
            trial_country_year_path,
            effect_candidates_path,
            context_joined_path,
        ]
    )

    country_year = build_country_year_transportability(
        trial_country_year=trial_country_year,
        effect_candidates=effect_candidates,
        context_long=context_long,
    )
    synthesis_output = build_synthesis_output(
        country_year,
        topic=resolved_topic,
        source_manifest_id=source_manifest_id,
    )
    summary = build_evidence_gap_summary(synthesis_output)
    markdown = render_evidence_gap_summary_markdown(summary)

    country_year_path = trial_output_dir / "transportability_country_year.parquet"
    synthesis_output_path = trial_output_dir / "synthesis_output.parquet"
    summary_path = trial_output_dir / "evidence_gap_summary.parquet"
    markdown_path = trial_output_dir / "evidence_gap_summary.md"
    manifest_path = trial_output_dir / "transportability_manifest.json"

    synthesis_output.to_parquet(country_year_path, index=False)
    synthesis_output.to_parquet(synthesis_output_path, index=False)
    summary.to_parquet(summary_path, index=False)
    markdown_path.write_text(markdown, encoding="utf-8")

    manifest = {
        "topic_slug": resolved_topic.slug,
        "source_manifest_id": source_manifest_id,
        "country_year_rows": int(len(synthesis_output)),
        "summary_rows": int(len(summary)),
        "core_signal_keys": [spec.key for spec in CORE_SIGNAL_SPECS],
        "max_transportability_score": float(synthesis_output["transportability_score"].max()) if not synthesis_output.empty else 0.0,
        "min_transportability_score": float(synthesis_output["transportability_score"].min()) if not synthesis_output.empty else 0.0,
        "outputs": {
            "transportability_country_year": str(country_year_path),
            "synthesis_output": str(synthesis_output_path),
            "evidence_gap_summary": str(summary_path),
            "evidence_gap_markdown": str(markdown_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
