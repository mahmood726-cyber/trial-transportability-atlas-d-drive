from __future__ import annotations

from trial_transportability_atlas.aact_bridge import validate_aact_snapshot
from trial_transportability_atlas.project_paths import discover_aact_snapshot


def test_real_aact_snapshot_headers_match_bridge_contract() -> None:
    snapshot_dir = discover_aact_snapshot()
    resolved = validate_aact_snapshot(snapshot_dir)

    assert "countries" in resolved
    assert "calculated_values" in resolved
    assert "outcome_measurements" in resolved
    assert "reported_events" in resolved
