import os
import pandas as pd
from pathlib import Path
import json
import sys

# Make the in-repo package importable without a hardcoded drive
# (scripts/<this>.py -> repo root is one level up, package lives in src/).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trial_transportability_atlas.project_paths import discover_external_paths, discover_output_root
from trial_transportability_atlas.source_adapters import load_unified_context

def advanced_sovereignty_analysis():
    paths = discover_external_paths()
    output_root = discover_output_root()
    equator_dir = output_root / "evidence_equator"
    
    # 1. Load Data
    print("Loading datasets...")
    df_trials = pd.read_parquet(equator_dir / "all_trials_conditions.parquet")
    df_truth = pd.read_parquet(equator_dir / "truthcert_audit_full.parquet")
    
    # Define Africa
    with open("C:/AfricaRCT/data/collected_data.json", 'r') as f:
        collected = json.load(f)
    african_countries = list(collected['country_totals'].keys())
    df_trials['is_africa'] = df_trials['country_name'].isin(african_countries)
    
    # 2. Fiscal Sovereignty Analysis (WHO GHED)
    print("Performing Fiscal Sovereignty Audit...")
    _who_root = Path(os.environ.get("WHO_DATA_LAKEHOUSE",
                                    Path(__file__).resolve().parents[1] / "data" / "who-data-lakehouse"))
    ghed = pd.read_parquet(_who_root / "data" / "silver" / "ghed" / "ghed_data.parquet")
    
    # Get latest Health Exp per country
    latest_ghed = ghed.sort_values('year').groupby('location').last()[['che_pc_usd', 'code']]
    latest_ghed.index.name = 'country_name'
    
    # Join with trials
    country_stats = df_trials.groupby('country_name').agg(
        trial_count=('nct_id', 'nunique'),
        is_africa=('is_africa', 'first')
    ).join(latest_ghed, how='inner')
    
    # Investment Intensity = (Trial Count / Health Exp per Capita)
    # Higher value = Heavy external research relative to local fiscal capacity (External Extraction)
    country_stats['fiscal_extraction_index'] = country_stats['trial_count'] / country_stats['che_pc_usd']
    
    # 3. Termination Sensitivity (Termination Cascade)
    print("Analyzing Termination Sensitivity...")
    # Map trial status
    df_status = df_truth.merge(df_trials[['nct_id', 'is_africa']].drop_duplicates(), on='nct_id', how='left')
    df_status = df_status[['nct_id', 'overall_status', 'is_africa']].drop_duplicates()
    df_status['is_failed'] = df_status['overall_status'].isin(['TERMINATED', 'WITHDRAWN'])
    
    termination_stats = df_status.groupby('is_africa')['is_failed'].mean() * 100
    print(f"Termination Rate - Africa: {termination_stats.get(True, 0):.1f}%, Global North: {termination_stats.get(False, 0):.1f}%")
    
    # 4. Site Fragmentation (Network Entropy)
    print("Measuring Site Fragmentation...")
    df_sites = df_truth.merge(df_trials[['nct_id', 'is_africa']].drop_duplicates(), on='nct_id', how='left')
    site_stats = df_sites.groupby(['nct_id', 'is_africa'])['facility_count'].first().groupby('is_africa').mean()
    print(f"Avg Sites per Trial - Africa: {site_stats.get(True, 0):.1f}, Global: {site_stats.get(False, 0):.1f}")
    
    # 5. Synthesis by Condition
    print("Synthesizing Advanced Metrics...")
    mapping = {
        "Sickle Cell": ["sickle cell"],
        "Heart Failure": ["heart failure"],
        "Malaria": ["malaria"],
        "Tuberculosis": ["tuberculosis", "tb "],
        "HIV/AIDS": ["hiv", "aids"],
        "Cancer": ["cancer", "carcinoma", "neoplasm", "melanoma", "lymphoma"],
        "Diabetes": ["diabetes"],
    }
    
    def map_condition(c):
        if not isinstance(c, str): return "Other"
        for label, keywords in mapping.items():
            if any(k in c for k in keywords):
                return label
        return "Other"

    df_full = df_trials.merge(df_status[['nct_id', 'is_failed']], on='nct_id', how='left')
    df_full = df_full.merge(df_truth[['nct_id', 'facility_count']], on='nct_id', how='left')
    df_full['broad_condition'] = df_full['condition'].map(map_condition)
    
    condition_stats = df_full[df_full['is_africa']].groupby('broad_condition').agg(
        africa_trials=('nct_id', 'nunique'),
        termination_rate=('is_failed', 'mean'),
        avg_sites=('facility_count', 'mean')
    )
    
    # Map back fiscal intensity (weighted average for countries in that condition)
    # This is a bit complex, let's just use the regional avg for now
    africa_fiscal_intensity = country_stats[country_stats['is_africa']]['fiscal_extraction_index'].mean()
    condition_stats['fiscal_intensity'] = africa_fiscal_intensity
    
    # 6. Save Final Advanced Dataset
    output_path = equator_dir / "advanced_sovereignty_audit.csv"
    condition_stats.to_csv(output_path)
    print(f"Advanced Audit saved to {output_path}")
    print(condition_stats.head(10).to_markdown())

if __name__ == "__main__":
    advanced_sovereignty_analysis()
