from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from trial_transportability_atlas.context_join import materialize_context_join
from trial_transportability_atlas.project_paths import discover_external_paths


DEFAULT_TOPIC_OUTPUT_DIR = REPO_ROOT / "outputs" / "sacubitril_valsartan_hfref"


def run_live_join(trial_output_dir: Path | None = None) -> dict[str, object]:
    paths = discover_external_paths()
    resolved_output_dir = trial_output_dir or DEFAULT_TOPIC_OUTPUT_DIR
    trial_country_year_path = resolved_output_dir / "trial_country_year.parquet"
    if not trial_country_year_path.exists():
        raise FileNotFoundError(f"Missing trial country-year parquet: {trial_country_year_path}")

    print("Discovered paths:")
    print(f"  IHME: {paths.ihme_repo}")
    print(f"  WHO:  {paths.who_repo}")
    print(f"  WB:   {paths.wb_repo}")
    print(f"  Trial output dir: {resolved_output_dir}")

    summary = materialize_context_join(
        trial_output_dir=resolved_output_dir,
        ihme_repo_root=paths.ihme_repo,
        wb_repo_root=paths.wb_repo,
        who_repo_root=paths.who_repo,
    )

    print("\nMaterialization Summary:")
    print(json.dumps(summary, indent=2))

    context_joined = pd.read_parquet(resolved_output_dir / "context_joined.parquet")
    print(f"\nJoined Data Shape: {context_joined.shape}")
    print(f"Available Sources: {sorted(context_joined['source'].dropna().unique().tolist())}")
    print(f"Available Measures: {sorted(context_joined['measure'].dropna().unique().tolist())}")
    return summary


if __name__ == "__main__":
    run_live_join()
