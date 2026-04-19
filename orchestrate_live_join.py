from pathlib import Path
import json
import pandas as pd
from trial_transportability_atlas.project_paths import discover_external_paths
from trial_transportability_atlas.context_join import materialize_context_join

def run_live_join():
    paths = discover_external_paths()
    print(f"Discovered paths:")
    print(f"  IHME: {paths.ihme_repo}")
    print(f"  WHO:  {paths.who_repo}")
    print(f"  WB:   {paths.wb_repo}")
    
    trial_output_dir = Path("D:/Projects/trial-transportability-atlas/outputs/sacubitril_valsartan_hfref")
    
    summary = materialize_context_join(
        trial_output_dir=trial_output_dir,
        ihme_repo_root=paths.ihme_repo,
        wb_repo_root=paths.wb_repo,
        who_repo_root=paths.who_repo,
    )
    
    print("\nMaterialization Summary:")
    print(json.dumps(summary, indent=2))
    
    # Quick sanity check on results
    df = pd.read_parquet(trial_output_dir / "context_joined.parquet")
    print(f"\nJoined Data Shape: {df.shape}")
    print(f"Available Sources: {df['source'].unique().tolist()}")
    print(f"Available Measures: {df['measure'].unique().tolist()}")

if __name__ == "__main__":
    run_live_join()
