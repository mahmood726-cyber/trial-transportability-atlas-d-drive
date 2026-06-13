from __future__ import annotations

import pandas as pd
import pytest

from trial_transportability_atlas.scoring import calculate_transportability_score


def test_identical_context_scores_one() -> None:
    stats = pd.Series({"GDP pc": 50000.0, "Physicians": 3.0, "Health Exp (%)": 10.0})
    assert calculate_transportability_score(stats, stats) == 1.0


def test_no_shared_metrics_returns_zero() -> None:
    origin = pd.Series({"GDP pc": 50000.0})
    target = pd.Series({"Unrelated": 1.0})
    assert calculate_transportability_score(origin, target) == 0.0


def test_missing_metric_does_not_inflate_score() -> None:
    # Only one weighted metric is shared and it differs substantially.
    # The score must normalize by the present weight, not the full weight set,
    # so a partial-context comparison is not artificially pushed toward 1.0.
    origin = pd.Series({"Physicians": 4.0})
    target = pd.Series({"Physicians": 1.0})

    # Physicians distance = |4 - 1| / 4 = 0.75, weight = 0.4.
    # Correct (normalize by present weight 0.4): 1 - (0.75 * 0.4 / 0.4) = 0.25.
    # Buggy (normalize by full weight 1.0): 1 - (0.75 * 0.4 / 1.0) = 0.70.
    score = calculate_transportability_score(origin, target)
    assert score == pytest.approx(0.25)
