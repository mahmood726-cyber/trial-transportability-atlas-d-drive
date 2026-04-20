import pandas as pd
from pathlib import Path
from trial_transportability_atlas.scoring import generate_transportability_heatmap

def run_sensitivity_check():
    topics = ["sacubitril_valsartan_hfref", "sglt2_inhibitors"]
    base_dir = Path("D:/Projects/trial-transportability-atlas/outputs")
    
    all_scores = {}
    
    regions = {
        "North America": ["USA", "CAN"],
        "South America": ["BRA", "ARG", "CHL", "COL", "PER"],
        "Asia": ["CHN", "IND", "JPN", "KOR", "THA", "VNM"],
        "Africa": ["ZAF", "EGY", "NGA", "KEN", "ETH"]
    }
    iso_to_region = {iso: r for r, isos in regions.items() for iso in isos}

    for topic in topics:
        context_path = base_dir / topic / "context_joined.parquet"
        if not context_path.exists():
            print(f"Skipping {topic}, no joined context found.")
            continue
            
        df = pd.read_parquet(context_path)
        df["atlas_region"] = df["iso3_resolved"].map(iso_to_region)
        focus = df[df["atlas_region"].notna() & (df["year"] >= 2010)].copy()
        
        # Calculate regional averages for metrics
        pivot = focus.groupby(["atlas_region", "measure"], dropna=False)["value"].mean().unstack()
        
        # Simplified mapping for scoring
        measure_map = {
            "Life expectancy": "Life Exp",
            "GDP per capita": "GDP pc",
            "Physicians": "Physicians",
            "health expenditure (% of GDP)": "Health Exp (%)"
        }
        
        report_mapping = {}
        for long_name in pivot.columns:
            for pattern, short_name in measure_map.items():
                if pattern.lower() in str(long_name).lower():
                    if short_name not in report_mapping.values():
                        report_mapping[long_name] = short_name
                    break
        
        report = pivot[list(report_mapping.keys())].rename(columns=report_mapping)
        
        # Calculate scores vs North America
        scores = generate_transportability_heatmap(report)
        if scores is not None:
            all_scores[topic] = scores
            
    if all_scores:
        sensitivity_df = pd.DataFrame(all_scores)
        print("# Cross-Topic Transportability Sensitivity Check")
        print(sensitivity_df.round(3).to_markdown())
        
        # Calculate Variance (Stability)
        sensitivity_df["Score Variance"] = sensitivity_df.std(axis=1)
        print("\n## Stability Insights")
        print("- A low Score Variance indicates that a region's transportability is stable across different cardiovascular classes.")
        
        report_path = Path("D:/Projects/trial-transportability-atlas/outputs/sensitivity_check.md")
        with report_path.open("w") as f:
            f.write("# Cross-Topic Transportability Sensitivity Check\n\n")
            f.write(sensitivity_df.round(3).to_markdown() + "\n")

if __name__ == "__main__":
    run_sensitivity_check()
