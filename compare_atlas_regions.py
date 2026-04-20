from __future__ import annotations

from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_TOPIC_OUTPUT_DIR = REPO_ROOT / "outputs" / "sacubitril_valsartan_hfref"

REGION_MEMBERS = {
    "North America": {"USA", "CAN"},
    "South America": {"BRA", "ARG", "CHL", "COL", "PER"},
    "Asia": {"CHN", "IND", "JPN", "KOR", "THA", "VNM"},
    "Africa": {"ZAF", "EGY", "NGA", "KEN", "ETH"},
}

REPORT_MEASURE_ALIASES = (
    ("Life Exp (Years)", ("Life expectancy at birth (years)", "life_expectancy")),
    ("Health Exp (% GDP)", ("Current health expenditure (% of GDP)", "che_gdp")),
    ("Health Exp pc (USD)", ("Current health expenditure per capita (USD)", "che_pc_usd")),
    ("Physicians (per 1,000)", ("Physicians (per 1,000 people)", "physician_density")),
    ("Gov Effectiveness", ("Government Effectiveness: Estimate",)),
    ("Gini", ("Gini index",)),
    (
        "OOP Poverty Gap",
        ("Increase in poverty gap due to out-of-pocket health care expenditure",),
    ),
    ("Avg Pop (M)", ("Population, total", "population")),
)


def _iso_to_region() -> dict[str, str]:
    return {
        iso3: region
        for region, members in REGION_MEMBERS.items()
        for iso3 in members
    }


def _select_report_columns(regional_avg: pd.DataFrame) -> pd.DataFrame:
    selected: dict[str, pd.Series] = {}
    for label, aliases in REPORT_MEASURE_ALIASES:
        match = next((alias for alias in aliases if alias in regional_avg.columns), None)
        if match is not None:
            selected[label] = regional_avg[match]

    if not selected:
        return pd.DataFrame(index=regional_avg.index)

    report = pd.DataFrame(selected)
    if "Avg Pop (M)" in report.columns:
        report["Avg Pop (M)"] = report["Avg Pop (M)"] / 1e6
    return report


def _render_report_markdown(final_report: pd.DataFrame, coverage: pd.DataFrame) -> str:
    lines = [
        "# Deep Regional Comparison: Trial Transportability Context",
        "",
        "This report summarizes the available context surface for trial countries grouped into broad atlas regions.",
        "",
    ]

    if final_report.empty:
        lines.extend(
            [
                "No configured report measures were available in the context surface.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                final_report.round(2).to_markdown(),
                "",
            ]
        )

    lines.extend(
        [
            "## Coverage",
            "",
            coverage.to_markdown(index=False),
            "",
        ]
    )
    return "\n".join(lines)


def generate_regional_comparison(
    context_path: Path | None = None,
    report_path: Path | None = None,
) -> pd.DataFrame:
    resolved_output_dir = DEFAULT_TOPIC_OUTPUT_DIR
    resolved_context_path = context_path or (resolved_output_dir / "context_joined.parquet")
    resolved_report_path = report_path or (resolved_output_dir / "regional_comparison_report.md")
    if not resolved_context_path.exists():
        raise FileNotFoundError(f"Missing context join parquet: {resolved_context_path}")

    df = pd.read_parquet(resolved_context_path)
    df["atlas_region"] = df["iso3_resolved"].map(_iso_to_region())
    focus = df.loc[df["atlas_region"].notna()].copy()
    if focus.empty:
        raise ValueError("No atlas-region rows available in the context surface")

    pivot = (
        focus.groupby(["atlas_region", "country_name", "year", "measure"], dropna=False)["value"]
        .mean()
        .reset_index()
    )
    regional_avg = pivot.groupby(["atlas_region", "measure"])["value"].mean().unstack()
    final_report = _select_report_columns(regional_avg)

    coverage = (
        focus.groupby("atlas_region", dropna=False)
        .agg(
            countries=("iso3_resolved", "nunique"),
            years=("year", "nunique"),
            measures=("measure", "nunique"),
            rows=("value", "count"),
        )
        .reset_index()
        .sort_values("atlas_region", kind="stable")
    )

    markdown = _render_report_markdown(final_report, coverage)
    resolved_report_path.write_text(markdown, encoding="utf-8")

    print(markdown)
    return final_report


if __name__ == "__main__":
    generate_regional_comparison()
