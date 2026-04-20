import pandas as pd
import numpy as np

def calculate_transportability_score(origin_stats: pd.Series, target_stats: pd.Series) -> float:
    """
    Calculates a transportability index based on health system and economic distance.
    1.0 = Identical context (Perfect transportability)
    0.0 = Maximal distance
    """
    # Key dimensions for cardiovascular transportability
    # 1. Health Spending (% GDP) - Proxy for resource availability
    # 2. Physicians (per 1k) - Proxy for clinical delivery capacity
    # 3. GDP per capita - Proxy for patient access/affordability
    
    metrics = {
        "GDP pc": 0.3,
        "Physicians": 0.4,
        "Health Exp (%)": 0.3
    }
    
    distances = []
    for metric, weight in metrics.items():
        if metric in origin_stats and metric in target_stats:
            v_orig = origin_stats[metric]
            v_targ = target_stats[metric]
            
            if pd.isna(v_orig) or pd.isna(v_targ) or v_orig == 0:
                continue
                
            # Log-normalized distance for economic metrics
            if metric == "GDP pc":
                dist = abs(np.log10(v_orig) - np.log10(v_targ)) / 2.0 # Max log distance approx 2.0
            else:
                dist = abs(v_orig - v_targ) / v_orig
            
            distances.append(min(dist, 1.0) * weight)
            
    if not distances:
        return 0.0
        
    avg_dist = sum(distances) / sum(metrics.values())
    return max(1.0 - avg_dist, 0.0)

def generate_transportability_heatmap(report_df: pd.DataFrame):
    """Computes scores for all regions relative to North America."""
    if "North America" not in report_df.index:
        return None
        
    origin = report_df.loc["North America"]
    scores = {}
    
    for region in report_df.index:
        target = report_df.loc[region]
        scores[region] = calculate_transportability_score(origin, target)
        
    return pd.Series(scores, name="Transportability Index")
