from pathlib import Path
import json
import sys
import pandas as pd
from trial_transportability_atlas.project_paths import discover_external_paths, discover_aact_snapshot
from trial_transportability_atlas.materialize import materialize_topic_bridge
from trial_transportability_atlas.context_join import materialize_context_join
from trial_transportability_atlas.topics import resolve_topic_spec
from trial_transportability_atlas.scoring import generate_transportability_heatmap

def run_topic_pipeline(slug: str):
    topic = resolve_topic_spec(slug)
    paths = discover_external_paths()
    aact_snapshot = discover_aact_snapshot()
    
    output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic.slug}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"--- Step 1: Materializing Bridge for {topic.slug} ---")
    bridge_summary = materialize_topic_bridge(
        snapshot_dir=aact_snapshot,
        output_dir=output_dir,
        topic=topic
    )
    print(f"Selected {bridge_summary['trial_country_year_rows']} country-year rows from AACT.")
    
    print(f"\n--- Step 2: Joining Unified Context ---")
    join_summary = materialize_context_join(
        trial_output_dir=output_dir,
        ihme_repo_root=paths.ihme_repo,
        wb_repo_root=paths.wb_repo,
        who_repo_root=paths.who_repo,
    )
    print(f"Joined context rows: {join_summary['context_rows']}")
    
    # Analysis Synthesis
    df = pd.read_parquet(output_dir / "context_joined.parquet")
    
    regions = {
        "North America": ["USA", "CAN"],
        "South America": ["BRA", "ARG", "CHL", "COL", "PER"],
        "Asia": ["CHN", "IND", "JPN", "KOR", "THA", "VNM"],
        "Africa": ["ZAF", "EGY", "NGA", "KEN", "ETH"]
    }
    iso_to_region = {iso: r for r, isos in regions.items() for iso in isos}
    df["atlas_region"] = df["iso3_resolved"].map(iso_to_region)
    
    focus = df[df["atlas_region"].notna() & (df["year"] >= 2010)].copy()
    
    # Pre-pivot to get raw GDP
    gdp_df = df[df["measure"].str.contains("GDP per capita", na=False)].copy()
    gdp_df["atlas_region"] = gdp_df["iso3_resolved"].map(iso_to_region)
    gdp_avg = gdp_df[gdp_df["atlas_region"].notna() & (gdp_df["year"] >= 2010)].groupby("atlas_region")["value"].mean()

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
    
    if "GDP pc" not in report.columns:
        report["GDP pc"] = gdp_avg

    if "Avg Pop (M)" in report.columns:
        report["Avg Pop (M)"] = report["Avg Pop (M)"] / 1e6
    
    # 3. Calculate Transportability Scores
    scores = generate_transportability_heatmap(report)
    if scores is not None:
        report["Transportability Index"] = scores

    print(f"\n--- Regional Transportability Profile: {topic.slug} ---")
    final_cols = ["Transportability Index", "GDP pc", "Physicians", "Health Exp (%)", "Avg Pop (M)"]
    existing_cols = [c for c in final_cols if c in report.columns]
    report = report[existing_cols]
    print(report.round(3).to_markdown())
    
    # Save results
    report.to_csv(output_dir / "transportability_scores.csv")
    print(f"Results saved to {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python orchestrate_topic.py <topic_slug>")
    else:
        run_topic_pipeline(sys.argv[1])
