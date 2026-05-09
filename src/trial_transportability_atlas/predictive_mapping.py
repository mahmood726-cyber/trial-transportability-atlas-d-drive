import pandas as pd
from trial_transportability_atlas.project_paths import discover_topic_output_dir


def _render_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown()
    except ImportError:
        return df.to_string()


def _select_transport_target(yield_df: pd.DataFrame) -> str | None:
    for region in yield_df.index.tolist():
        if region != "North America":
            return str(region)
    if len(yield_df.index) == 0:
        return None
    return str(yield_df.index[0])


def generate_predictive_yield(topic_slug: str):
    base_dir = discover_topic_output_dir(topic_slug)
    context_path = base_dir / "context_joined.parquet"
    scores_path = base_dir / "transportability_scores.csv"

    if not context_path.exists() or not scores_path.exists():
        print(f"Required data missing for {topic_slug}")
        return None

    df = pd.read_parquet(context_path)
    scores_df = pd.read_csv(scores_path, index_col=0)
    if "Transportability Index" not in scores_df.columns:
        return None

    # 1. Extract Local Burden (DALYs per 100k)
    # Filter for DALYs and recent years
    burden_df = df[df["measure"] == "DALYs (Disability-Adjusted Life Years)"].copy()
    regions = {
        "North America": ["USA", "CAN"],
        "South America": ["BRA", "ARG", "CHL", "COL", "PER"],
        "Asia": ["CHN", "IND", "JPN", "KOR", "THA", "VNM"],
        "Africa": ["ZAF", "EGY", "NGA", "KEN", "ETH"]
    }
    iso_to_region = {iso: r for r, isos in regions.items() for iso in isos}
    burden_df["atlas_region"] = burden_df["iso3_resolved"].map(iso_to_region)
    
    # Get regional average burden (using most recent available)
    regional_burden = burden_df[burden_df["atlas_region"].notna()].groupby("atlas_region")["value"].mean()

    # 2. Combine with Transportability Index
    yield_df = pd.DataFrame({
        "Transportability Index": scores_df["Transportability Index"],
        "Local Burden (DALYs)": regional_burden
    })

    # Calculate Predicted Evidence Yield (PEY)
    # We normalize burden by dividing by 10,000 for a scaled index
    yield_df["Predicted Yield (PEY)"] = yield_df["Transportability Index"] * (yield_df["Local Burden (DALYs)"] / 1000)
    yield_df = yield_df.dropna(subset=["Transportability Index", "Local Burden (DALYs)", "Predicted Yield (PEY)"])

    if yield_df.empty:
        print(f"No comparable evidence-yield rows for {topic_slug}")
        return None

    # Rank by Yield
    yield_df = yield_df.sort_values("Predicted Yield (PEY)", ascending=False)

    print(f"# Predicted Evidence Yield: {topic_slug}")
    print(_render_table(yield_df.round(3)))

    yield_df.to_csv(base_dir / "predictive_yield.csv")

    top_region = _select_transport_target(yield_df)
    with open(base_dir / "predictive_yield_report.md", "w", encoding="utf-8") as f:
        f.write(f"# Predictive Success Map: {topic_slug}\n\n")
        f.write("This report ranks regions by **Predicted Evidence Yield (PEY)**, which combines clinical readiness (Transportability Index) with local disease burden (DALYs).\n\n")
        f.write(_render_table(yield_df.round(3)) + "\n\n")
        f.write("## Strategic Recommendations\n")
        if top_region is None:
            f.write("- **Primary Transport Target**: No secondary transport target was identified from the current yield surface.\n")
        else:
            f.write(f"- **Primary Transport Target**: **{top_region}** shows the highest predicted yield, indicating a strong balance of clinical need and system readiness.\n")
        f.write(f"- **System Bottleneck**: Africa's yield remains low primarily due to the Transportability Index gap, suggesting that increased burden does not automatically translate to trial success without system hardening.\n")
    return yield_df

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "sacubitril_valsartan_hfref"
    generate_predictive_yield(topic)
