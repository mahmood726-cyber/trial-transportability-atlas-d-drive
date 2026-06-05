import pandas as pd
from pathlib import Path
import json
import sys

# Make the in-repo package importable without a hardcoded drive.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trial_transportability_atlas.project_paths import discover_output_root

def classify_source(s):
    s = str(s).upper()
    if any(x in s for x in ["INC", "LTD", "PHARMA", "BIOTECH", "NOVARTIS", "PFIZER", "ASTRAZENECA", "ROCHE", "SANOFI", "LILLY", "GLAXOSMITHKLINE", "NOVO NORDISK", "BRISTOL-MYERS", "BAYER", "BOEHRINGER", "MERCK", "AMGEN", "JANSSEN", "TAKEDA", "ABBVIE"]):
        return "INDUSTRIAL"
    if any(x in s for x in ["UNIVERSITY", "COLLEGE", "HOSPITAL", "CLINIC", "INSTITUTE", "FOUNDATION", "CENTER", "NIH", "NCI", "NIAID", "GOVERNMENT"]):
        return "PHILANTHROPIC/ACADEMIC"
    return "OTHER"

def generate_sovereignty_ledger():
    output_root = discover_output_root()
    equator_dir = output_root / "evidence_equator"
    
    # 1. Load Data
    print("Loading Audit Data...")
    df_audit = pd.read_parquet(equator_dir / "truthcert_audit_full.parquet")
    df_syn = pd.read_csv(equator_dir / "evidence_equator_synthesis.csv")
    
    # 2. Funding Archetype Classification
    print("Classifying funding archetypes...")
    # Redefine is_africa
    with open("C:/AfricaRCT/data/collected_data.json", 'r') as f:
        collected = json.load(f)
    african_countries = list(collected['country_totals'].keys())
    
    df_trials = pd.read_parquet(equator_dir / "all_trials_conditions.parquet")
    df_trials['is_africa'] = df_trials['country_name'].isin(african_countries)
    
    df_audit = df_audit.merge(df_trials[['nct_id', 'is_africa']].drop_duplicates(), on='nct_id', how='left')
    
    unique_trials = df_audit.drop_duplicates("nct_id").copy()
    unique_trials['archetype'] = unique_trials['source'].map(classify_source)
    
    # 3. Calculate Self-Reliance Score (SRS)
    # SRS = (Philanthropic Trials / Total Trials) per condition
    # High SRS = More public-good research, potentially lower commercial extraction risk.
    
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

    unique_trials['broad_condition'] = unique_trials['condition'].map(map_condition)
    
    srs_stats = unique_trials[unique_trials['is_africa']].groupby('broad_condition')['archetype'].value_counts(normalize=True).unstack(fill_value=0)
    srs_stats['self_reliance_score'] = srs_stats.get('PHILANTHROPIC/ACADEMIC', 0) * 100
    
    # 4. Generate Regulatory Briefs (JSON)
    print("Generating Regulatory Briefs...")
    df_merged = df_syn.merge(srs_stats[['self_reliance_score']], left_on='condition', right_index=True, how='left')
    
    briefs = {}
    for _, row in df_merged.iterrows():
        cond = row['condition']
        brief = {
            "condition": cond,
            "status": "CRITICAL" if row['cci'] > 10 else "MONITOR",
            "evidence_distance": f"{int(row['evidence_distance_km']):,} km",
            "self_reliance": f"{row.get('self_reliance_score', 0):.1f}%",
            "policy_mandate": row['policy_recommendation'],
            "negotiation_leverage": "HIGH" if row['cci'] > 20 else "MEDIUM",
            "recommendation": f"Due to a CCI of {row['cci']:.1f}x and a {row.get('self_reliance_score', 0):.1f}% public-funding share, we recommend mandatory technology transfer for all commercial trials in {cond}."
        }
        briefs[cond] = brief
        
    with open(equator_dir / "regulatory_briefs.json", 'w') as f:
        json.dump(briefs, f, indent=4)
        
    df_merged.to_csv(equator_dir / "evidence_equator_ledger.csv", index=False)
    print(f"Sovereignty Ledger saved at {equator_dir / 'evidence_equator_ledger.csv'}")
    print(df_merged[['condition', 'cci', 'self_reliance_score']].head(10).to_markdown())

if __name__ == "__main__":
    generate_sovereignty_ledger()
