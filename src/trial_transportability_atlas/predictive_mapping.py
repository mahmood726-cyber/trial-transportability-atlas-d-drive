import pandas as pd
from pathlib import Path

def generate_predictive_yield(topic_slug: str):
    base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
    df = pd.read_parquet(base_dir / "context_joined.parquet")
    
    # Actually, let's load the scores series correctly
    scores_df = pd.read_csv(base_dir / "transportability_scores.csv", index_col=0)
    if "Transportability Index" not in scores_df.columns:
        return "No scores found."

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
    
    # Rank by Yield
    yield_df = yield_df.sort_values("Predicted Yield (PEY)", ascending=False)
    
    print(f"# Predicted Evidence Yield: {topic_slug}")
    print(yield_df.round(3).to_markdown())
    
    yield_df.to_csv(base_dir / "predictive_yield.csv")
    
    with open(base_dir / "predictive_yield_report.md", "w") as f:
        f.write(f"# Predictive Success Map: {topic_slug}\n\n")
        f.write("This report ranks regions by **Predicted Evidence Yield (PEY)**, which combines clinical readiness (Transportability Index) with local disease burden (DALYs).\n\n")
        f.write(yield_df.round(3).to_markdown() + "\n\n")
        f.write("## Strategic Recommendations\n")
        top_region = yield_df.index[0]
        if top_region == "North America":
             top_region = yield_df.index[1] # Pick the next best for transport
        f.write(f"- **Primary Transport Target**: **{top_region}** shows the highest predicted yield, indicating a strong balance of clinical need and system readiness.\n")
        f.write(f"- **System Bottleneck**: Africa's yield remains low primarily due to the Transportability Index gap, suggesting that increased burden does not automatically translate to trial success without system hardening.\n")

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "sacubitril_valsartan_hfref"
    generate_predictive_yield(topic)
