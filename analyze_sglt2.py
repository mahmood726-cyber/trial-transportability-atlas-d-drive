from pathlib import Path
import pandas as pd
from trial_transportability_atlas.scoring import generate_transportability_heatmap

def analyze_sglt2():
    topic_slug = "sglt2_inhibitors"
    output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
    df = pd.read_parquet(output_dir / "context_joined.parquet")
    
    # ... [keep existing region mapping logic] ...
    regions = {
        "North America": ["USA", "CAN"],
        "South America": ["BRA", "ARG", "CHL", "COL", "PER"],
        "Asia": ["CHN", "IND", "JPN", "KOR", "THA", "VNM"],
        "Africa": ["ZAF", "EGY", "NGA", "KEN", "ETH"]
    }
    iso_to_region = {iso: r for r, isos in regions.items() for iso in isos}
    df["atlas_region"] = df["iso3_resolved"].map(iso_to_region)
    
    focus = df[df["atlas_region"].notna() & (df["year"] >= 2010)].copy()
    
    # Pre-pivot to get raw GDP (which was missing in my previous run due to filter)
    # Use World Bank indicator specifically for GDP
    gdp_df = df[df["measure"].str.contains("GDP per capita", na=False)].copy()
    gdp_df["atlas_region"] = gdp_df["iso3_resolved"].map(iso_to_region)
    gdp_avg = gdp_df[gdp_df["atlas_region"].notna() & (gdp_df["year"] >= 2015)].groupby("atlas_region")["value"].mean()

    pivot = focus.groupby(["atlas_region", "country_name", "year", "measure"], dropna=False)["value"].mean().reset_index()
    regional_avg = pivot.groupby(["atlas_region", "measure"])["value"].mean().unstack()
    
    measure_map = {
        "Life expectancy": "Life Exp",
        "GDP per capita": "GDP pc",
        "drinking water": "Water (%)",
        "health expenditure (% of GDP)": "Health Exp (%)",
        "Physicians": "Physicians",
        "UHC": "UHC Index",
        "Population, total": "Avg Pop (M)"
    }
    
    found_measures = regional_avg.columns.tolist()
    report_mapping = {}
    for long_name in found_measures:
        for pattern, short_name in measure_map.items():
            if pattern.lower() in str(long_name).lower():
                if short_name not in report_mapping.values():
                    report_mapping[long_name] = short_name
                break
    
    report = regional_avg[list(report_mapping.keys())].rename(columns=report_mapping)
    
    # Merge back the GDP pc if missing from the main pivot
    if "GDP pc" not in report.columns:
        report["GDP pc"] = gdp_avg

    if "Avg Pop (M)" in report.columns:
        report["Avg Pop (M)"] = report["Avg Pop (M)"] / 1e6
    
    # 3. Calculate Transportability Scores
    scores = generate_transportability_heatmap(report)
    if scores is not None:
        report["Transportability Index"] = scores

    print(f"\n--- SGLT2 Transportability Index (vs. North America) ---")
    final_cols = ["Transportability Index", "GDP pc", "Physicians", "Health Exp (%)", "Avg Pop (M)"]
    existing_cols = [c for c in final_cols if c in report.columns]
    report = report[existing_cols]
    print(report.round(3).to_markdown())
    
    # Save results
    report.to_csv(output_dir / "transportability_scores.csv")

if __name__ == "__main__":
    analyze_sglt2()
