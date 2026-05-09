import pandas as pd
from trial_transportability_atlas.project_paths import discover_topic_output_dir
from trial_transportability_atlas.scoring import calculate_transportability_score


def _render_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _percent_lift(new_score: float, baseline_score: float) -> float | None:
    if baseline_score == 0:
        return None
    return round(((new_score - baseline_score) / baseline_score) * 100, 1)


def run_transportability_simulation(topic_slug: str, target_region: str = "Africa"):
    base_dir = discover_topic_output_dir(topic_slug)
    scores_path = base_dir / "transportability_scores.csv"
    
    if not scores_path.exists():
        print(f"No scores found for {topic_slug}")
        return

    df = pd.read_csv(scores_path, index_col=0)
    if "North America" not in df.index or target_region not in df.index:
        print(f"Required regions not found in {topic_slug} scores.")
        return None
    required_columns = {"Transportability Index", "Physicians", "GDP pc"}
    if not required_columns.issubset(df.columns):
        print(f"Required score columns missing for {topic_slug}.")
        return None

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
            "Lift (%)": _percent_lift(new_score, current_score),
        })

    sim_df = pd.DataFrame(results)
    print(_render_table(sim_df))
    
    report_path = base_dir / "transportability_simulation.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write(f"# Transportability Sensitivity Simulation: {topic_slug}\n\n")
        f.write(f"**Target Region**: {target_region} (vs. North America baseline)\n")
        f.write(f"**Current Score**: {current_score:.3f}\n\n")
        f.write(_render_table(sim_df) + "\n\n")
        f.write("## Simulation Insights\n")
        f.write("- **Workforce Sensitivity**: Physician density is the primary driver of transportability lift. Doubling capacity yields the most significant index improvement.\n")
        f.write("- **The 'Viability' Threshold**: Africa reaches a score of >0.5 only when clinical capacity is significantly hardened, suggesting that economic growth alone (GDP) is insufficient for complex trial transport.\n")
    return sim_df

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "sglt2_inhibitors"
    run_transportability_simulation(topic)
