import json
import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from trial_transportability_atlas.dashboard import materialize_dashboard


def generate_html_dashboards():
    parser = argparse.ArgumentParser(description="Generate static dashboards for all topics.")
    parser.add_argument(
        "--topics",
        nargs="+",
        default=["sacubitril_valsartan_hfref", "sglt2_inhibitors", "glp1_agonists"],
        help="List of topic slugs to process.",
    )
    args = parser.parse_args()
    
    results = []
    for slug in args.topics:
        output_dir = REPO_ROOT / "outputs" / slug
        if not output_dir.exists():
            print(f"Skipping {slug}, output dir not found.")
            continue
            
        dashboard_path = REPO_ROOT / "dashboard" / f"transportability_{slug}.html"
        print(f"Generating dashboard for {slug}...")
        res = materialize_dashboard(
            output_dir=output_dir,
            dashboard_path=dashboard_path,
        )
        results.append(res)
    return results

if __name__ == "__main__":
    print(json.dumps(generate_html_dashboards(), indent=2))
