from pathlib import Path
import sys
from trial_transportability_atlas.transportability import materialize_transportability_outputs
from trial_transportability_atlas.topics import resolve_topic_spec

def materialize_all_transport():
    topics = ["sacubitril_valsartan_hfref", "sglt2_inhibitors", "glp1_agonists"]
    for slug in topics:
        output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{slug}")
        if not output_dir.exists():
            print(f"Skipping {slug}, output dir not found.")
            continue
            
        print(f"Materializing transportability for {slug}...")
        try:
            topic = resolve_topic_spec(slug)
            materialize_transportability_outputs(output_dir, topic=topic)
            print(f"  Done.")
        except Exception as e:
            print(f"  Failed: {e}")

if __name__ == "__main__":
    materialize_all_transport()
