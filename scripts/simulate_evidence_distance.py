import pandas as pd
from pathlib import Path
import json
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from trial_transportability_atlas.project_paths import discover_output_root

def simulate_evidence_distance():
    output_root = discover_output_root()
    equator_dir = output_root / "evidence_equator"
    
    # 1. Load Deep CCI Audit (with Forensic Metrics)
    df_cci = pd.read_csv(equator_dir / "deep_cci_audit.csv")
    
    # 2. Simulate Phenotypic Drift (Evidence Distance)
    drift_factors = {
        "Sickle Cell": 0.9, 
        "Heart Failure": 0.7,
        "Diabetes": 0.4,
        "Cancer": 0.3,
        "HIV/AIDS": 0.2,
        "Malaria": 0.1,
        "RHD": 0.8,
        "Hypertension": 0.5
    }
    
    df_cci['phenotypic_drift'] = df_cci['condition'].map(drift_factors).fillna(0.3)
    
    # NEW: Epistemic Multiplier
    # If the care gap is high, the "distance" increases because we don't trust the data context.
    df_cci['epistemic_multiplier'] = 1 + (df_cci['epistemic_care_gap'].clip(lower=0) / 10)
    
    # Evidence Distance = CCI * (1 + Drift) * Epistemic Multiplier
    df_cci['evidence_distance_km'] = (df_cci['cci'] * (1 + df_cci['phenotypic_drift']) * df_cci['epistemic_multiplier'] * 100).round(0)
    
    # 3. Simulate Policy Recommendations
    def get_recommendation(row):
        if row['evidence_distance_km'] > 5000:
            return "MANDATORY: Local Bridging Trial Required (High Drift/Gap)"
        if row['evidence_distance_km'] > 2000:
            return "HIGH PRIORITY: Observational Validation Needed"
        if row['evidence_distance_km'] > 500:
            return "MEDIUM: Monitoring Recommended"
        return "LOW: Direct Implementation Safe"

    df_cci['policy_recommendation'] = df_cci.apply(get_recommendation, axis=1)
    
    # 4. Save Final Synthesis
    output_path = equator_dir / "evidence_equator_synthesis.csv"
    df_cci.to_csv(output_path, index=False)
    
    print(f"Policy Simulation completed at {output_path}")
    print(df_cci[['condition', 'evidence_distance_km', 'policy_recommendation']].head(10).to_markdown())

if __name__ == "__main__":
    simulate_evidence_distance()
