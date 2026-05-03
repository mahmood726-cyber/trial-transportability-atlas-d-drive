from pathlib import Path
import pandas as pd
import json
from trial_transportability_atlas.aact_bridge import extract_trial_country_year
from trial_transportability_atlas.project_paths import discover_aact_snapshot, discover_output_root
from trial_transportability_atlas.aact_io import iter_aact_rows
from trial_transportability_atlas.country_iso3 import country_name_to_iso3

def extract_all_africa_data():
    """
    Extracts all trials with African locations and maps their conditions.
    """
    snapshot_dir = discover_aact_snapshot()
    output_root = discover_output_root()
    equator_dir = output_root / "evidence_equator"
    equator_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Extracting all trials from {snapshot_dir}...")
    
    # 1. Get All Country-Year Records
    all_trials = extract_trial_country_year(snapshot_dir)
    df_trials = pd.DataFrame(all_trials)
    
    # Map ISO3
    df_trials["iso3"] = df_trials["country_name"].map(country_name_to_iso3)
    
    # 2. Extract Conditions for these trials
    print("Extracting conditions...")
    conditions = []
    for row in iter_aact_rows(snapshot_dir, "conditions"):
        conditions.append({
            "nct_id": row["nct_id"],
            "condition": row["name"].lower()
        })
    df_conditions = pd.DataFrame(conditions)
    
    # 3. Join Trials and Conditions
    df_full = df_trials.merge(df_conditions, on="nct_id", how="left")
    
    # Save base dataset
    df_full.to_parquet(equator_dir / "all_trials_conditions.parquet", index=False)
    print(f"Extracted {len(df_full)} trial-condition-location rows.")

if __name__ == "__main__":
    extract_all_africa_data()
