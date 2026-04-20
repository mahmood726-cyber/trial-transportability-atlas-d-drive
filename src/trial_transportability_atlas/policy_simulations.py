import pandas as pd
import numpy as np
from pathlib import Path
from trial_transportability_atlas.scoring import calculate_transportability_score

def run_policy_simulations(topic_slug: str, target_region: str = "Africa"):
    base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
    scores_path = base_dir / "transportability_scores.csv"
    yield_path = base_dir / "predictive_yield.csv"
    
    if not (scores_path.exists() and yield_path.exists()):
        print(f"Required data missing for {topic_slug}")
        return

    df_scores = pd.read_csv(scores_path, index_col=0)
    df_yield = pd.read_csv(yield_path, index_col=0)
    
    if "North America" not in df_scores.index or target_region not in df_scores.index:
        return

    origin = df_scores.loc["North America"]
    current_stats = df_scores.loc[target_region]
    current_score = current_stats["Transportability Index"]
    local_burden = df_yield.loc[target_region, "Local Burden (DALYs)"]

    # Define Policy Bundles
    policy_bundles = [
        {
            "name": "Status Quo",
            "phys_lift": 1.0,
            "gdp_lift": 1.0,
            "health_exp_lift": 1.0,
            "description": "No intervention"
        },
        {
            "name": "WHO Workforce Initiative",
            "phys_lift": 2.0, # Double physicians
            "gdp_lift": 1.0,
            "health_exp_lift": 1.2, # 20% increase in efficiency/spending
            "description": "Focused clinical capacity building"
        },
        {
            "name": "WB Economic Uplift",
            "phys_lift": 1.1,
            "gdp_lift": 1.5, # 50% GDP growth
            "health_exp_lift": 1.1,
            "description": "General economic development"
        },
        {
            "name": "Integrated Health Reform",
            "phys_lift": 1.8,
            "gdp_lift": 1.2,
            "health_exp_lift": 1.5, # Significant spending increase
            "description": "Combined workforce and resource boost"
        }
    ]

    results = []
    for p in policy_bundles:
        sim = current_stats.copy()
        sim["Physicians"] *= p["phys_lift"]
        sim["GDP pc"] *= p["gdp_lift"]
        sim["Health Exp (%)"] *= p["health_exp_lift"]
        
        new_score = calculate_transportability_score(origin, sim)
        new_yield = new_score * (local_burden / 1000)
        
        results.append({
            "Policy Bundle": p["name"],
            "Description": p["description"],
            "Simulated Index": round(new_score, 3),
            "Simulated Yield (PEY)": round(new_yield, 0),
            "Index Lift (%)": round(((new_score - current_score) / current_score) * 100, 1),
            "Viability": "VIABLE" if new_score >= 0.55 else "BOTTLENECK"
        })

    results_df = pd.DataFrame(results)
    
    print(f"# Regional Policy Simulation: {target_region} ({topic_slug})")
    print(results_df[["Policy Bundle", "Simulated Index", "Simulated Yield (PEY)", "Index Lift (%)", "Viability"]].to_markdown(index=False))
    
    report_path = base_dir / "policy_simulation_report.md"
    with report_path.open("w") as f:
        f.write(f"# Policy Impact Report: {target_region} ({topic_slug})\n\n")
        f.write(f"This simulation measures the impact of strategic policy bundles on the transportability of {topic_slug.replace('_', ' ')} evidence.\n\n")
        f.write(results_df.to_markdown(index=False) + "\n\n")
        f.write("## Strategic Recommendation\n")
        top_policy = results_df.sort_values("Simulated Index", ascending=False).iloc[0]
        f.write(f"The **{top_policy['Policy Bundle']}** is the most effective path to regional viability, providing a {top_policy['Index Lift (%)']}% lift in transportability readiness.\n")

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "glp1_agonists"
    run_policy_simulations(topic)
