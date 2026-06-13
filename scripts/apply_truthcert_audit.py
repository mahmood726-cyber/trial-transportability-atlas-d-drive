import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from trial_transportability_atlas.aact_io import iter_aact_rows
from trial_transportability_atlas.project_paths import discover_aact_snapshot, discover_output_root

def apply_truthcert_audit():
    snapshot_dir = discover_aact_snapshot()
    output_root = discover_output_root()
    equator_dir = output_root / "evidence_equator"
    
    # 1. Extract Enrollment and Study Info
    print("Extracting enrollment and sponsor info...")
    study_info = []
    # Using 'studies' table for enrollment in ClinicalTrials.gov AACT
    for row in iter_aact_rows(snapshot_dir, "studies"):
        study_info.append({
            "nct_id": row["nct_id"],
            "enrollment": pd.to_numeric(row.get("enrollment"), errors='coerce'),
            "source": row.get("source"), # Sponsor/Lead Org
            "study_type": row.get("study_type"),
            "overall_status": row.get("overall_status")
        })
    df_studies = pd.DataFrame(study_info)
    
    # 2. Extract Location counts per trial
    print("Counting locations...")
    loc_counts = []
    for row in iter_aact_rows(snapshot_dir, "facilities"):
        loc_counts.append(row["nct_id"])
    df_locs = pd.Series(loc_counts).value_counts().reset_index()
    df_locs.columns = ["nct_id", "facility_count"]
    
    # 3. Join with Africa Trials
    df_equator = pd.read_parquet(equator_dir / "all_trials_conditions.parquet")
    df_audit = df_equator.merge(df_studies, on="nct_id", how="left")
    df_audit = df_audit.merge(df_locs, on="nct_id", how="left")
    
    # 4. Simulate TruthCert Forensic Audit (Benford's Law Proxy)
    # Sentinel identifies "Artificiality" in reporting.
    # We'll flag trials with "suspiciously round" enrollment or "extreme clustering"
    
    def calculate_audit_grade(row):
        # Suspicious Enrollment (e.g. exactly 100, 500, 1000)
        enroll = row['enrollment']
        if pd.isna(enroll) or enroll == 0: return "PENDING"
        
        score = 100
        if enroll % 100 == 0: score -= 20
        if enroll % 500 == 0: score -= 10
        
        # Site Clustering
        fac = row.get('facility_count', 1)
        if fac > 50: score -= 15 # Hub Hegemony
        
        # Industry vs Academic
        if "INDUSTRY" in str(row.get('source', '')).upper():
            score += 5 # Often more rigorous data entry
            
        if score > 85: return "AAA (Gold)"
        if score > 70: return "AA (Silver)"
        if score > 50: return "A (Bronze)"
        return "B (Flagged)"

    print("Applying TruthCert Audit grades...")
    # Group by nct_id to avoid redundant audits in country-year-condition long form
    unique_trials = df_audit.drop_duplicates("nct_id").copy()
    unique_trials['audit_grade'] = unique_trials.apply(calculate_audit_grade, axis=1)
    
    # Map back to full dataset
    audit_map = unique_trials.set_index("nct_id")['audit_grade']
    df_audit['audit_grade'] = df_audit['nct_id'].map(audit_map)
    
    # Save Audit Dataset
    df_audit.to_parquet(equator_dir / "truthcert_audit_full.parquet", index=False)
    print(f"TruthCert Audit completed for {len(unique_trials)} trials.")
    print(unique_trials['audit_grade'].value_counts().to_markdown())

if __name__ == "__main__":
    apply_truthcert_audit()
