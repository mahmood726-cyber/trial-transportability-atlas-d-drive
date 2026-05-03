import pandas as pd
from pathlib import Path
import json

def generate_radar_ui():
    ledger_path = Path("D:/Projects/trial-transportability-atlas/outputs/evidence_equator/evidence_equator_ledger.csv")
    advanced_path = Path("D:/Projects/trial-transportability-atlas/outputs/evidence_equator/advanced_sovereignty_audit.csv")
    
    if not ledger_path.exists() or not advanced_path.exists():
        print("Data not found. Run synthesis and ledger scripts first.")
        return
    
    df_ledger = pd.read_csv(ledger_path)
    df_adv = pd.read_csv(advanced_path).rename(columns={'broad_condition': 'condition'})
    
    df = df_ledger.merge(df_adv, on='condition', how='left')
    df = df.sort_values("evidence_distance_km", ascending=False)
    
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>The Evidence Equator | Sovereignty Ledger v2.5</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1600px; margin: 0 auto; padding: 20px; background: #f4f7f6; }}
        h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-bottom: 10px; }}
        .radar-container {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-top: 20px; }}
        .card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 0.82em; }}
        th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f9fa; color: #7f8c8d; text-transform: uppercase; font-size: 0.7em; }}
        .cci-high {{ color: #e74c3c; font-weight: bold; }}
        .badge {{ padding: 3px 6px; border-radius: 4px; font-size: 0.7em; font-weight: 600; text-transform: uppercase; }}
        .badge-red {{ background: #fdeaea; color: #e74c3c; }}
        .badge-orange {{ background: #fff5e6; color: #d35400; }}
        .badge-blue {{ background: #eaf2ff; color: #3498db; }}
        .badge-green {{ background: #eafaf1; color: #2ecc71; }}
        .radar-header {{ display: flex; justify-content: space-between; align-items: center; }}
        .desc {{ color: #7f8c8d; font-size: 0.85em; margin-bottom: 10px; }}
        .policy-mandatory {{ border-left: 4px solid #e74c3c; padding-left: 8px; background: #fffcfc; }}
        .policy-high {{ border-left: 4px solid #f39c12; padding-left: 8px; }}
        .policy-med {{ border-left: 4px solid #3498db; padding-left: 8px; }}
        .negotiation-tip {{ font-size: 0.8em; color: #e67e22; font-weight: bold; margin-top: 5px; display: block; }}
    </style>
</head>
<body>
    <header class="radar-header">
        <h1>THE EVIDENCE EQUATOR: Sovereignty Ledger v2.5</h1>
        <div>
            <span class="badge badge-red">Regulatory Briefing Active</span>
            <span class="badge badge-blue">Self-Reliance Mapping</span>
        </div>
    </header>
    
    <p class="desc">Autonomous health policy intelligence. Correlating <strong>Condition Colonialism</strong> with <strong>Funding Archetypes</strong> and <strong>Fiscal Extraction</strong>.</p>

    <div class="radar-container">
        <div class="card">
            <h2>The Sovereignty Ledger</h2>
            <p class="desc">Self-Reliance Score (SRS) = % Philanthropic/Academic funding. Low SRS + High CCI = Commercial Extraction Risk.</p>
            <table>
                <thead>
                    <tr>
                        <th>Condition</th>
                        <th>CCI</th>
                        <th>SRS %</th>
                        <th>Risk Status</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for _, row in df.sort_values("cci", ascending=False).iterrows():
        srs = row.get('self_reliance_score', 0)
        risk = "EXTRACTIVE" if (row['cci'] > 10 and srs < 50) else "SOVEREIGN" if srs > 80 else "VULNERABLE"
        risk_badge = "badge-red" if risk == "EXTRACTIVE" else "badge-green" if risk == "SOVEREIGN" else "badge-orange"
        
        html_content += f"""
                    <tr>
                        <td><strong>{row['condition']}</strong></td>
                        <td class="cci-high">{row['cci']:.1f}x</td>
                        <td>{srs:.1f}%</td>
                        <td><span class="badge {risk_badge}">{risk}</span></td>
                    </tr>
        """
        
    html_content += """
                </tbody>
            </table>
        </div>
        
        <div class="card">
            <h2>Extraction Intensity</h2>
            <p class="desc">Fiscal Intensity vs Network Attrition. Measuring the fragility of the research ecosystem.</p>
            <table>
                <thead>
                    <tr>
                        <th>Condition</th>
                        <th>Fiscal Int.</th>
                        <th>Term. Rate</th>
                        <th>Entropy</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for _, row in df.sort_values("fiscal_intensity", ascending=False).iterrows():
        html_content += f"""
                    <tr>
                        <td>{row['condition']}</td>
                        <td>{row.get('fiscal_intensity', 0):.1f}</td>
                        <td>{row.get('termination_rate', 0)*100:.1f}%</td>
                        <td>{row.get('avg_sites', 0):.1f}</td>
                    </tr>
        """
        
    html_content += """
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>Regulatory Mandates</h2>
            <p class="desc">Autonomous policy briefs for Ministry negotiations. High distance + Low SRS = Strict Mandates.</p>
            <table>
                <thead>
                    <tr>
                        <th>Condition</th>
                        <th>Distance</th>
                        <th>Negotiation Brief</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for _, row in df.sort_values("evidence_distance_km", ascending=False).iterrows():
        rec_class = "policy-mandatory" if "MANDATORY" in row['policy_recommendation'] else "policy-high" if "HIGH" in row['policy_recommendation'] else "policy-med"
        srs = row.get('self_reliance_score', 0)
        leverage = "MAXIMAL" if row['cci'] > 20 and srs < 30 else "HIGH" if row['cci'] > 10 else "MODERATE"
        
        html_content += f"""
                    <tr class="{rec_class}">
                        <td>{row['condition']}</td>
                        <td>{int(row['evidence_distance_km']):,} km</td>
                        <td>
                            <strong>{row['policy_recommendation']}</strong>
                            <span class="negotiation-tip">LEVERAGE: {leverage}</span>
                            <small>Mandate Tech-Transfer due to extraction risk.</small>
                        </td>
                    </tr>
        """
        
    html_content += f"""
                </tbody>
            </table>
        </div>
    </div>

    <footer style="margin-top: 30px; border-top: 1px solid #eee; padding-top: 15px; font-size: 0.8em; color: #7f8c8d;">
        Generated on {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')} | Ledger v2.5 | <strong>Regulatory Briefing Engine Active</strong>
    </footer>
</body>
</html>
    """
    
    ui_path = Path("D:/Projects/trial-transportability-atlas/dashboard/evidence_equator_radar.html")
    ui_path.parent.mkdir(parents=True, exist_ok=True)
    ui_path.write_text(html_content, encoding="utf-8")
    print(f"Sovereignty Ledger UI generated at {ui_path}")

if __name__ == "__main__":
    generate_radar_ui()
