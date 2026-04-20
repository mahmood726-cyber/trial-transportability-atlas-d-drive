from pathlib import Path
import json
import pandas as pd
from trial_transportability_atlas.project_paths import discover_external_paths, discover_aact_snapshot
from trial_transportability_atlas.materialize import materialize_topic_bridge
from trial_transportability_atlas.context_join import materialize_context_join
from trial_transportability_atlas.topics import SGLT2_TOPIC

def run_sglt2_pipeline():
    paths = discover_external_paths()
    aact_snapshot = discover_aact_snapshot()
    
    topic = SGLT2_TOPIC
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
    
    # Filter to valid regions and broaden years for better coverage
    focus = df[df["atlas_region"].notna() & (df["year"] >= 2010)].copy()
    
    pivot = focus.groupby(["atlas_region", "country_name", "year", "measure"], dropna=False)["value"].mean().reset_index()
    regional_avg = pivot.groupby(["atlas_region", "measure"])["value"].mean().unstack()
    
    # Clean up column names for report (using flexible substring matching)
    measure_map = {
        "Life expectancy": "Life Exp",
        "GDP per capita": "GDP pc",
        "drinking water": "Water (%)",
        "health expenditure (% of GDP)": "Health Exp (%)",
        "Physicians": "Physicians",
        "UHC": "UHC Index",
        "population": "Avg Pop (M)",
        "Population, total": "Avg Pop (M)"
    }
    
    # Map the long measure names to report headers
    found_measures = regional_avg.columns.tolist()
    report_mapping = {}
    for long_name in found_measures:
        for pattern, short_name in measure_map.items():
            if pattern.lower() in str(long_name).lower():
                # Avoid collision by only taking the first hit for each short_name category
                if short_name not in report_mapping.values():
                    report_mapping[long_name] = short_name
                break
    
    report = regional_avg[list(report_mapping.keys())].rename(columns=report_mapping)
    
    # Consolidation and Scaling
    if "Avg Pop (M)" in report.columns:
        report["Avg Pop (M)"] = report["Avg Pop (M)"] / 1e6 # Convert to millions
    
    # Fill missing Life Exp with IHME data if available (IHME uses simple 'burden' or 'sdi' indicators)
    # Actually, we'll just report what we have for now.
    
    print(f"\n--- Regional Transportability Profile: {topic.slug} ---")
    # Sort columns for readability
    final_cols = ["Avg Pop (M)", "Life Exp", "GDP pc", "Physicians", "Health Exp (%)", "UHC Index"]
    existing_cols = [c for c in final_cols if c in report.columns]
    report = report[existing_cols]
    
    print(report.round(2).to_markdown())
    
    report_path = output_dir / "regional_comparison_report.md"
    with report_path.open("w") as f:
        f.write(f"# Regional Transportability Profile: {topic.slug}\n\n")
        f.write(report.round(2).to_markdown() + "\n")

if __name__ == "__main__":
    run_sglt2_pipeline()
