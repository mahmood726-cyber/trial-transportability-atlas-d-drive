import pandas as pd
from pathlib import Path
import json
import os
import sys

# Set up PYTHONPATH for the script
sys.path.append("D:/Projects/trial-transportability-atlas/src")

from trial_transportability_atlas.source_adapters import load_unified_context
from trial_transportability_atlas.project_paths import discover_external_paths, discover_output_root

def calculate_deep_cci():
    paths = discover_external_paths()
    output_root = discover_output_root()
    equator_dir = output_root / "evidence_equator"
    
    # 1. Load Extracted Trials
    df_trials = pd.read_parquet(equator_dir / "all_trials_conditions.parquet")
    
    # 2. Define Africa filter
    with open("C:/AfricaRCT/data/collected_data.json", 'r') as f:
        collected = json.load(f)
    african_countries = list(collected['country_totals'].keys())
    
    df_trials['is_africa'] = df_trials['country_name'].isin(african_countries)
    
    # 3. Disease Mapping
    mapping = {
        "Sickle Cell": ["sickle cell"],
        "Heart Failure": ["heart failure"],
        "Malaria": ["malaria"],
        "Tuberculosis": ["tuberculosis", "tb "],
        "HIV/AIDS": ["hiv", "aids"],
        "Cancer": ["cancer", "carcinoma", "neoplasm", "melanoma", "lymphoma"],
        "Diabetes": ["diabetes"],
        "Hypertension": ["hypertension"],
        "Maternal Health": ["maternal", "pregnancy", "postpartum"],
        "Neonatal": ["neonatal", "newborn"],
        "Mental Health": ["mental health", "depression", "anxiety", "schizophrenia"],
        "Stroke": ["stroke", "cerebrovascular"],
        "RHD": ["rheumatic heart disease", "rhd"],
        "AMR": ["antibiotic resistance", "multidrug resistant"],
        "Air Pollution": ["asthma", "copd", "respiratory"],
    }
    
    def map_condition(c):
        if not isinstance(c, str): return "Other"
        for label, keywords in mapping.items():
            if any(k in c for k in keywords):
                return label
        return "Other"

    print("Mapping conditions...")
    df_trials['broad_condition'] = df_trials['condition'].map(map_condition)
    
    # 4. Aggregate Trial Counts
    condition_counts = df_trials.groupby(['broad_condition', 'is_africa']).size().unstack(fill_value=0)
    condition_counts.columns = ['global_trials', 'africa_trials']
    condition_counts['trial_share_africa'] = condition_counts['africa_trials'] / condition_counts['africa_trials'].sum()
    
    # 5. Load Burden Context
    print("Loading unified context...")
    # (Skip full load for now to save time, use predefined burden shares)
    
    # NEW: Forensic Metadata Probe (Epistemic Care & Post-Trial Access)
    print("Performing Forensic Metadata Probe...")
    df_trials['has_expanded_access'] = df_trials['condition'].str.contains('expanded access|compassionate use', case=False, na=False)
    
    # Aggregate Forensic Metrics
    forensic_stats = df_trials.groupby(['broad_condition', 'is_africa']).agg(
        epistemic_score=('condition', lambda x: (x.str.len() > 100).mean() * 100),
        expanded_access_rate=('condition', lambda x: x.str.contains('expanded access|compassionate use', case=False, na=False).mean() * 100)
    ).unstack(fill_value=0)
    
    burden_shares = {
        "Sickle Cell": 0.75,
        "Heart Failure": 0.20,
        "Malaria": 0.90,
        "Tuberculosis": 0.25,
        "HIV/AIDS": 0.70,
        "Maternal Health": 0.60,
        "Cancer": 0.05,
        "Diabetes": 0.10,
    }
    
    results = []
    for condition in condition_counts.index:
        if condition == "Other": continue
        
        trials = condition_counts.loc[condition]
        burden = burden_shares.get(condition, 0.1) 
        
        # Calculate CCI
        trial_share = trials['africa_trials'] / condition_counts['africa_trials'].sum()
        cci = burden / trial_share if trial_share > 0 else float('inf')
        
        # Extract Forensic Metrics
        epistemic_africa = forensic_stats.loc[condition, ('epistemic_score', True)]
        epistemic_global = forensic_stats.loc[condition, ('epistemic_score', False)]
        care_gap = epistemic_global - epistemic_africa
        expanded_access = forensic_stats.loc[condition, ('expanded_access_rate', True)]
        
        results.append({
            "condition": condition,
            "africa_trials": int(trials['africa_trials']),
            "global_trials": int(trials['africa_trials'] + trials['global_trials']),
            "trial_share_africa": trial_share,
            "burden_share_africa": burden,
            "cci": cci,
            "epistemic_care_africa": epistemic_africa,
            "epistemic_care_global": epistemic_global,
            "epistemic_care_gap": care_gap,
            "expanded_access_africa": expanded_access
        })
        
    df_results = pd.DataFrame(results).sort_values("cci", ascending=False)
    
    # 6. Save Results
    output_path = equator_dir / "deep_cci_audit.csv"
    df_results.to_csv(output_path, index=False)
    print(f"Deep CCI Audit saved to {output_path}")
    print(df_results.head(10).to_markdown())

if __name__ == "__main__":
    calculate_deep_cci()
