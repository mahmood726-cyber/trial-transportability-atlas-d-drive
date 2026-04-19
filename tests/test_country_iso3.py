from __future__ import annotations

from trial_transportability_atlas.country_iso3 import country_name_to_iso3


def test_country_name_to_iso3_handles_common_aliases() -> None:
    assert country_name_to_iso3("United Kingdom") == "GBR"
    assert country_name_to_iso3("Turkey (T\u00fcrkiye)") == "TUR"
    assert country_name_to_iso3("T\ufffdrkiye") == "TUR"
    assert country_name_to_iso3("Russia") == "RUS"


def test_country_name_to_iso3_fails_closed_for_unknown_name() -> None:
    assert country_name_to_iso3("Atlantis") is None
