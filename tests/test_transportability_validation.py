"""Input-validation tests for the public transportability entry points."""
from __future__ import annotations

import pandas as pd
import pytest

from trial_transportability_atlas.transportability import (
    build_country_year_context_signals,
    build_country_year_transportability,
)


def _valid_context() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["iso3", "year", "source", "measure", "metric", "sex", "age_group", "value"]
    )


def _valid_effect_candidates() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["candidate_id", "nct_id", "candidate_family", "comparable_flag"]
    )


def _valid_trials() -> pd.DataFrame:
    return pd.DataFrame([{"nct_id": "N1", "country_name": "United Kingdom", "iso3": "GBR", "year": 2020}])


def test_context_signals_reports_missing_trial_column() -> None:
    trials = _valid_trials().drop(columns=["country_name"])
    with pytest.raises(ValueError, match="trial_country_year is missing required column"):
        build_country_year_context_signals(trial_country_year=trials, context_long=_valid_context())


def test_context_signals_reports_missing_context_column() -> None:
    context = _valid_context().drop(columns=["value"])
    with pytest.raises(ValueError, match="context frame is missing required column"):
        build_country_year_context_signals(trial_country_year=_valid_trials(), context_long=context)


def test_context_signals_rejects_non_dataframe_context() -> None:
    with pytest.raises(TypeError, match="must be a pandas DataFrame"):
        build_country_year_context_signals(trial_country_year=_valid_trials(), context_long=[1, 2, 3])


def test_transportability_reports_missing_effect_candidate_column() -> None:
    with pytest.raises(ValueError, match="effect_candidates is missing required column"):
        build_country_year_transportability(
            trial_country_year=_valid_trials(),
            effect_candidates=pd.DataFrame([{"x": 1}]),
            context_long=_valid_context(),
        )


def test_passing_both_context_kwargs_is_rejected() -> None:
    with pytest.raises(TypeError, match="not both"):
        build_country_year_context_signals(
            trial_country_year=_valid_trials(),
            context_long=_valid_context(),
            context_joined=_valid_context(),
        )


def test_missing_context_argument_is_rejected() -> None:
    with pytest.raises(TypeError, match="context frame is required"):
        build_country_year_context_signals(trial_country_year=_valid_trials())


def test_context_joined_keyword_still_accepted() -> None:
    # The legacy per-country-year schema (iso3_resolved + context_available_flag)
    # must remain callable via the context_joined keyword.
    context_joined = pd.DataFrame(
        [
            {
                "iso3_resolved": "GBR",
                "country_name": "United Kingdom",
                "year": 2020,
                "source": "ihme_burden",
                "measure": "DALYs (Disability-Adjusted Life Years)",
                "metric": "Rate",
                "sex": "Both",
                "age_group": "All ages",
                "value": 100.0,
                "context_available_flag": True,
            }
        ]
    )
    signals = build_country_year_context_signals(
        trial_country_year=_valid_trials(), context_joined=context_joined
    )
    assert signals.loc[signals["iso3"] == "GBR", "signal_daly_rate"].iloc[0] == 100.0
