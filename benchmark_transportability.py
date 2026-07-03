"""Self-contained, reproducible benchmark for the transportability engine.

This script exercises the core transportability scoring method end to end on a
small, bundled synthetic dataset. It requires NO external AACT / IHME / WHO /
World Bank snapshots — everything it needs is generated deterministically in
memory — so it runs from a fresh clone with only the packaged dependencies.

What it demonstrates:

1. A synthetic multi-country trial footprint (``trial_country_year``).
2. A synthetic effect-candidate table (``effect_candidates``).
3. A synthetic long-form context surface in the same schema the real
   IHME/WHO/WB adapters emit (``context_long``).
4. Scoring those into country-year transportability rows and an evidence-gap
   summary using the same public functions the production pipeline calls.

Run:

    python benchmark_transportability.py            # print tables to stdout
    python benchmark_transportability.py --out DIR  # also write parquet/markdown/CSV

The numbers this benchmark prints are a property of the synthetic inputs below
and the scoring formulas in ``transportability.py``; they are stable across
runs because the inputs are fixed.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from trial_transportability_atlas.transportability import (
    CORE_SIGNAL_SPECS,
    build_country_year_transportability,
    build_evidence_gap_summary,
    render_evidence_gap_summary_markdown,
)


# A small, curated trial footprint across four countries and two years.
# Country names are chosen so the packaged pycountry lookup resolves them
# deterministically to ISO3 codes.
_TRIALS = [
    # nct_id, country_name, year
    ("NCT90000001", "United Kingdom", 2019),
    ("NCT90000001", "United States", 2019),
    ("NCT90000002", "United Kingdom", 2020),
    ("NCT90000003", "Germany", 2020),
    ("NCT90000004", "Kenya", 2021),
    ("NCT90000005", "Kenya", 2021),
]

# Effect candidates per trial. ``comparable_flag`` marks whether the candidate
# is a supported, comparable effect family (drives the eligibility / reporting
# completeness scores).
_CANDIDATES = [
    # candidate_id, nct_id, candidate_family, comparable_flag
    ("C1", "NCT90000001", "binary_event_count", True),
    ("C2", "NCT90000001", "binary_event_count", True),
    ("C3", "NCT90000002", "continuous_mean", True),
    ("C4", "NCT90000002", "unsupported_number", False),
    ("C5", "NCT90000003", "continuous_mean", True),
    ("C6", "NCT90000004", "unsupported_number", False),
    ("C7", "NCT90000005", "hazard_ratio", False),
]

# Per-country context richness: how many of the four "rich" IHME signals we
# populate for each ISO3. Higher coverage -> higher country_coverage_score.
# GBR: all four; USA: three; DEU: two; KEN: one (a deliberate low-context gap).
_CONTEXT_SIGNAL_VALUES = {
    "daly_rate": 1200.0,
    "death_rate": 95.0,
    "population": 5.0e7,
    "sdi": 0.85,
}
_COUNTRY_CONTEXT = {
    # iso3: (country_name, base_year, list of signal keys populated)
    "GBR": ("United Kingdom", 2019, ["daly_rate", "death_rate", "population", "sdi"]),
    "USA": ("United States", 2019, ["daly_rate", "death_rate", "population"]),
    "DEU": ("Germany", 2020, ["daly_rate", "sdi"]),
    "KEN": ("Kenya", 2021, ["daly_rate"]),
}


def build_trial_country_year() -> pd.DataFrame:
    """Synthetic ``trial_country_year`` surface (iso3 left blank on purpose)."""

    return pd.DataFrame(
        [
            {"nct_id": nct, "country_name": country, "iso3": None, "year": year}
            for nct, country, year in _TRIALS
        ]
    )


def build_effect_candidates() -> pd.DataFrame:
    """Synthetic ``effect_candidates`` surface."""

    return pd.DataFrame(
        [
            {
                "candidate_id": cid,
                "nct_id": nct,
                "candidate_family": family,
                "comparable_flag": comparable,
            }
            for cid, nct, family, comparable in _CANDIDATES
        ]
    )


def build_context_long() -> pd.DataFrame:
    """Synthetic long-form context surface matching the adapter output schema."""

    spec_by_key = {spec.key: spec for spec in CORE_SIGNAL_SPECS}
    rows: list[dict[str, object]] = []
    for iso3, (country_name, base_year, populated_keys) in _COUNTRY_CONTEXT.items():
        for key in populated_keys:
            spec = spec_by_key[key]
            rows.append(
                {
                    "iso3": iso3,
                    "country_name": country_name,
                    "year": base_year,
                    "source": spec.source,
                    "measure": spec.measure,
                    "metric": spec.metric,
                    "sex": spec.sex,
                    "age_group": spec.age_group,
                    "value": _CONTEXT_SIGNAL_VALUES[key],
                }
            )
    return pd.DataFrame(rows)


def run_benchmark() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the scoring engine on the bundled synthetic inputs."""

    country_year = build_country_year_transportability(
        trial_country_year=build_trial_country_year(),
        effect_candidates=build_effect_candidates(),
        context_long=build_context_long(),
    )
    summary = build_evidence_gap_summary(country_year)
    return country_year, summary


def _format_country_year(country_year: pd.DataFrame) -> str:
    columns = [
        "country_name",
        "iso3",
        "year",
        "trial_count",
        "comparable_trial_count",
        "available_core_signal_count",
        "country_coverage_score",
        "eligibility_support_score",
        "reporting_completeness_score",
        "transportability_score",
        "priority_gap_score",
    ]
    view = country_year[columns].copy()
    for col in view.columns:
        if view[col].dtype.kind == "f":
            view[col] = view[col].round(3)
    return view.to_string(index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional directory to write parquet/markdown/CSV benchmark outputs.",
    )
    args = parser.parse_args()

    country_year, summary = run_benchmark()

    print("=" * 78)
    print("Trial Transportability Atlas - synthetic benchmark")
    print("=" * 78)
    print(f"Core scoring signals: {len(CORE_SIGNAL_SPECS)}")
    print(f"Country-year rows scored: {len(country_year)}")
    print(f"Country summary rows: {len(summary)}")
    print()
    print("Country-year transportability scores")
    print("-" * 78)
    print(_format_country_year(country_year))
    print()
    print("Evidence-gap summary (highest latest-year gap first)")
    print("-" * 78)
    summary_cols = [
        "country_name",
        "iso3",
        "latest_year",
        "trial_count",
        "comparable_trial_count",
        "latest_transportability_score",
        "latest_priority_gap_score",
        "missing_core_signals_union",
    ]
    summary_view = summary[summary_cols].copy()
    for col in ("latest_transportability_score", "latest_priority_gap_score"):
        summary_view[col] = summary_view[col].round(3)
    print(summary_view.to_string(index=False))

    if args.out is not None:
        out_dir = Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        country_year.to_parquet(out_dir / "benchmark_country_year.parquet", index=False)
        summary.to_parquet(out_dir / "benchmark_evidence_gap_summary.parquet", index=False)
        country_year.to_csv(out_dir / "benchmark_country_year.csv", index=False)
        summary.to_csv(out_dir / "benchmark_evidence_gap_summary.csv", index=False)
        (out_dir / "benchmark_evidence_gap_summary.md").write_text(
            render_evidence_gap_summary_markdown(summary), encoding="utf-8"
        )
        print()
        print(f"Wrote benchmark outputs to {out_dir}")


if __name__ == "__main__":
    main()
