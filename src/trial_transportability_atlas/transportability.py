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
    context_joined: pd.DataFrame,
) -> pd.DataFrame:
    """Select one curated signal value per country-year."""

    base = enrich_trial_country_year_iso3(trial_country_year)
    country_years = (
        base[["country_name", "iso3_resolved", "year"]]
        .drop_duplicates()
        .rename(columns={"iso3_resolved": "iso3"})
        .sort_values(["country_name", "year"], kind="stable")
        .reset_index(drop=True)
    )

    context_unique = (
        context_joined.loc[context_joined["context_available_flag"]]
        [
            [
                "iso3_resolved",
                "year",
                "source",
                "measure",
                "metric",
                "sex",
                "age_group",
                "value",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    result = country_years.copy()
    for spec in CORE_SIGNAL_SPECS:
        candidates = context_unique.loc[
            context_unique["source"].eq(spec.source)
            & context_unique["measure"].eq(spec.measure)
        ].copy()
        if candidates.empty:
            result[f"signal_{spec.key}"] = pd.NA
            continue

        candidates["metric_rank"] = _preference_rank(candidates["metric"], spec.metric)
        candidates["sex_rank"] = _preference_rank(candidates["sex"], spec.sex)
        candidates["age_rank"] = _preference_rank(candidates["age_group"], spec.age_group)
        selected = (
            candidates.sort_values(
                ["iso3_resolved", "year", "metric_rank", "sex_rank", "age_rank"],
                kind="stable",
            )
            .drop_duplicates(["iso3_resolved", "year"], keep="first")
            .rename(
                columns={
                    "iso3_resolved": "iso3",
                    "value": f"signal_{spec.key}",
                }
            )[["iso3", "year", f"signal_{spec.key}"]]
        )
        result = result.merge(selected, how="left", on=["iso3", "year"])

    return result


def build_country_year_transportability(
    trial_country_year: pd.DataFrame,
    effect_candidates: pd.DataFrame,
    context_joined: pd.DataFrame,
) -> pd.DataFrame:
    """Score trial-footprint country-years for transportability readiness."""

    evidence_by_nct = _build_nct_evidence_summary(effect_candidates)
    country_year_signals = build_country_year_context_signals(
        trial_country_year=trial_country_year,
        context_joined=context_joined,
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
        latest = ordered.iloc[-1]
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
                "latest_year": int(latest["year"]),
                "latest_transportability_score": float(latest["transportability_score"]),
                "latest_priority_gap_score": float(latest["priority_gap_score"]),
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
) -> dict[str, object]:
    """Write transportability-scored country-year outputs from existing parquet surfaces."""

    trial_output_dir = Path(trial_output_dir)
    trial_country_year_path = trial_output_dir / "trial_country_year.parquet"
    effect_candidates_path = trial_output_dir / "effect_candidates.parquet"
    context_joined_path = trial_output_dir / "context_joined.parquet"
    run_manifest_path = trial_output_dir / "run_manifest.json"
    context_manifest_path = trial_output_dir / "context_join_manifest.json"

    trial_country_year = pd.read_parquet(trial_country_year_path)
    effect_candidates = pd.read_parquet(effect_candidates_path)
    context_joined = pd.read_parquet(context_joined_path)
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
        context_joined=context_joined,
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
