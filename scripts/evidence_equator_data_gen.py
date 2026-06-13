from pathlib import Path
import pandas as pd
import json
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from trial_transportability_atlas.project_paths import (
    discover_africa_rct_root,
    discover_output_root,
)

def generate_evidence_equator_data():
    """
    Synthesizes AfricaRCT audits and local trial data
    to generate the Mismatch Radar (CCI) dataset.
    """
    # 1. Load Burden Data (from AfricaRCT artifacts)
    # In a real run, this would pull from ihme-data-lakehouse silver tables
    # Here we use the pre-calculated CCI data from the e156 JSONs to seed the radar

    africa_rct_root = discover_africa_rct_root()
    e156_files = list(africa_rct_root.glob("e156-*-paper.json"))
    
    mismatch_records = []
    
    for f in e156_files:
        try:
            with open(f, 'r', encoding='utf-8') as jf:
                data = json.load(jf)
                
            title = data.get("title", "")
            body = data.get("body", "")
            
            # Extract CCI and Burden from validation or body
            cci = None
            burden_share = None
            trial_share = None
            
            # Heuristic extraction for prototype
            if "Condition Colonialism Index" in body or "CCI" in body:
                # Try to find the number
                import re
                cci_match = re.search(r"(?:CCI|Condition Colonialism Index) (?:of |is )?([0-9.]+)", body)
                if cci_match:
                    cci_str = cci_match.group(1).rstrip(".")
                    cci = float(cci_str)
            
            # Specific handling for the 2 known high-quality audits
            if "scd-africa" in f.name:
                burden_share = 0.75
                trial_share = 0.016
                cci = 47.7
                condition = "Sickle Cell Disease"
            elif "heart-failure-africa" in f.name:
                burden_share = 0.20
                trial_share = 41 / 1855 * 0.20 # placeholder logic from paper
                cci = 11.0
                condition = "Heart Failure"
            else:
                # Generic fallback for other e156 papers
                condition = title.split(":")[0].replace("in Africa", "").strip()
                if cci is None: cci = 1.0 # Default/Unknown
                burden_share = 0.1 # Placeholder
                trial_share = 0.1 / cci
            
            mismatch_records.append({
                "condition": condition,
                "cci": cci,
                "burden_share": burden_share,
                "trial_share": trial_share,
                "source_paper": f.name
            })
        except Exception as e:
            print(f"Error processing {f.name}: {e}")

    df = pd.DataFrame(mismatch_records)
    
    # 2. Map to Transportability Index (from D: drive logic)
    # We'll simulate a 'Sovereignty Score' which is (1 / Evidence Distance)
    df["evidence_distance"] = df["cci"] * 0.5 # Proxy: high mismatch = high distance
    df["sovereignty_score"] = 100 / (1 + df["evidence_distance"])
    
    output_path = discover_output_root() / "evidence_equator_mismatch.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Mismatch Radar data generated at {output_path}")

if __name__ == "__main__":
    generate_evidence_equator_data()
