import pandas as pd
from pathlib import Path
import json

def generate_html_dashboard():
    base_dir = Path("D:/Projects/trial-transportability-atlas")
    topics = ["sacubitril_valsartan_hfref", "sglt2_inhibitors"]
    
    # Synthesize data
    data = {}
    for topic in topics:
        yield_path = base_dir / "outputs" / topic / "predictive_yield.csv"
        scores_path = base_dir / "outputs" / topic / "transportability_scores.csv"
        
        if yield_path.exists() and scores_path.exists():
            y_df = pd.read_csv(yield_path, index_col=0)
            s_df = pd.read_csv(scores_path, index_col=0)
            
            # Combine
            combined = y_df.join(s_df[["Physicians", "Health Exp (%)"]], how="left")
            data[topic] = combined.round(2).to_dict(orient="index")
            
    # HTML Template
    html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Transportability Success Map</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; max-width: 1000px; margin: 0 auto; padding: 20px; background-color: #f4f7f6; }}
        h1, h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .topic-container {{ background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background-color: #3498db; color: white; }}
        tr:hover {{ background-color: #f1f1f1; }}
        .index-bar {{ height: 10px; background: #e0e0e0; border-radius: 5px; overflow: hidden; }}
        .index-fill {{ height: 100%; background: #27ae60; }}
        .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .badge-high {{ background: #d4edda; color: #155724; }}
        .badge-mid {{ background: #fff3cd; color: #856404; }}
        .badge-low {{ background: #f8d7da; color: #721c24; }}
    </style>
</head>
<body>
    <h1>Global Trial Transportability Atlas (2026)</h1>
    <p>Synthesizing IHME, WHO, and World Bank lakehouses to predict clinical evidence success.</p>
    
    <div id="content"></div>

    <script>
        const data = {json.dumps(data)};
        const content = document.getElementById('content');

        for (const [topic, regions] of Object.entries(data)) {{
            let html = `<div class="topic-container">
                <h2>Topic: ${{topic.replace(/_/g, ' ').toUpperCase()}}</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Region</th>
                            <th>Transportability Index</th>
                            <th>Local Burden (DALYs)</th>
                            <th>PEY Score</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>`;
            
            for (const [region, stats] of Object.entries(regions)) {{
                const pey = stats['Predicted Yield (PEY)'];
                let status = pey > 50000 ? '<span class="badge badge-high">Priority</span>' : 
                             pey > 10000 ? '<span class="badge badge-mid">Viable</span>' : 
                             '<span class="badge badge-low">Bottleneck</span>';
                
                html += `<tr>
                    <td><strong>${{region}}</strong></td>
                    <td>
                        <div style="display: flex; align-items: center; gap: 10px;">
                            ${{stats['Transportability Index']}}
                            <div class="index-bar" style="flex-grow: 1;">
                                <div class="index-fill" style="width: ${{stats['Transportability Index'] * 100}}%;"></div>
                            </div>
                        </div>
                    </td>
                    <td>${{stats['Local Burden (DALYs)'].toLocaleString()}}</td>
                    <td>${{pey.toLocaleString()}}</td>
                    <td>${{status}}</td>
                </tr>`;
            }}
            
            html += `</tbody></table></div>`;
            content.innerHTML += html;
        }}
    </script>
</body>
</html>
    """
    
    output_path = base_dir / "dashboard" / "transportability_dashboard.html"
    output_path.write_text(html_template, encoding="utf-8")
    print(f"Dashboard generated at: {output_path}")

if __name__ == "__main__":
    generate_html_dashboard()
