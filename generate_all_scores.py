import pandas as pd
from pathlib import Path
from trial_transportability_atlas.scoring import generate_transportability_heatmap

def generate_scores(topic_slug: str):
    output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
    context_path = output_dir / "context_joined.parquet"
    if not context_path.exists():
        return
        
    df = pd.read_parquet(context_path)
    
    regions = {
        "North America": ["USA", "CAN"],
        "South America": ["BRA", "ARG", "CHL", "COL", "PER"],
        "Asia": ["CHN", "IND", "JPN", "KOR", "THA", "VNM"],
        "Africa": ["ZAF", "EGY", "NGA", "KEN", "ETH"]
    }
    iso_to_region = {iso: r for r, isos in regions.items() for iso in isos}
    df["atlas_region"] = df["iso3_resolved"].map(iso_to_region)
    
    focus = df[df["atlas_region"].notna() & (df["year"] >= 2010)].copy()
    pivot = focus.groupby(["atlas_region", "measure"], dropna=False)["value"].mean().unstack()
    
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
    
    scores = generate_transportability_heatmap(report)
    if scores is not None:
        report["Transportability Index"] = scores
        report.to_csv(output_dir / "transportability_scores.csv")
        print(f"Saved scores for {topic_slug}")

if __name__ == "__main__":
    generate_scores("sacubitril_valsartan_hfref")
    generate_scores("sglt2_inhibitors")
