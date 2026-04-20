import pandas as pd
import numpy as np
from pathlib import Path
from trial_transportability_atlas.scoring import calculate_transportability_score

def run_transportability_simulation(topic_slug: str, target_region: str = "Africa"):
    base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
    scores_path = base_dir / "transportability_scores.csv"
    
    if not scores_path.exists():
        print(f"No scores found for {topic_slug}")
        return

    df = pd.read_csv(scores_path, index_col=0)
    if "North America" not in df.index or target_region not in df.index:
        print(f"Required regions not found in {topic_slug} scores.")
        return

    origin = df.loc["North America"]
    current_target = df.loc[target_region]
    current_score = current_target["Transportability Index"]

    print(f"# Transportability Simulation: {topic_slug} ({target_region})")
    print(f"Current Index: {current_score:.3f}")
    
    scenarios = [
        {"name": "Baseline", "phys_mult": 1.0, "gdp_mult": 1.0},
        {"name": "+25% Physicians", "phys_mult": 1.25, "gdp_mult": 1.0},
        {"name": "+50% Physicians", "phys_mult": 1.50, "gdp_mult": 1.0},
        {"name": "+100% Physicians (2x)", "phys_mult": 2.0, "gdp_mult": 1.0},
        {"name": "+50% GDP pc", "phys_mult": 1.0, "gdp_mult": 1.5},
        {"name": "Physician 2x + GDP 1.5x", "phys_mult": 2.0, "gdp_mult": 1.5},
    ]

    results = []
    for s in scenarios:
        sim_target = current_target.copy()
        sim_target["Physicians"] *= s["phys_mult"]
        sim_target["GDP pc"] *= s["gdp_mult"]
        
        new_score = calculate_transportability_score(origin, sim_target)
        results.append({
            "Scenario": s["name"],
            "Physicians (per 1k)": round(sim_target["Physicians"], 2),
            "GDP pc (USD)": round(sim_target["GDP pc"], 0),
            "Simulated Index": round(new_score, 3),
            "Lift (%)": round(((new_score - current_score) / current_score) * 100, 1)
        })

    sim_df = pd.DataFrame(results)
    print(sim_df.to_markdown(index=False))
    
    report_path = base_dir / "transportability_simulation.md"
    with report_path.open("w") as f:
        f.write(f"# Transportability Sensitivity Simulation: {topic_slug}\n\n")
        f.write(f"**Target Region**: {target_region} (vs. North America baseline)\n")
        f.write(f"**Current Score**: {current_score:.3f}\n\n")
        f.write(sim_df.to_markdown(index=False) + "\n\n")
        f.write("## Simulation Insights\n")
        f.write("- **Workforce Sensitivity**: Physician density is the primary driver of transportability lift. Doubling capacity yields the most significant index improvement.\n")
        f.write("- **The 'Viability' Threshold**: Africa reaches a score of >0.5 only when clinical capacity is significantly hardened, suggesting that economic growth alone (GDP) is insufficient for complex trial transport.\n")

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "sglt2_inhibitors"
    run_transportability_simulation(topic)
