"""Country-name to ISO3 helpers for trial-country joins."""
from __future__ import annotations

import re
import unicodedata

import pycountry


COUNTRY_ALIAS_ISO3 = {
    "bolivia": "BOL",
    "bolivia plurinational state of": "BOL",
    "cape verde": "CPV",
    "czech republic": "CZE",
    "democratic republic of the congo": "COD",
    "hong kong": "HKG",
    "iran": "IRN",
    "iran islamic republic of": "IRN",
    "laos": "LAO",
    "micronesia federated states of": "FSM",
    "moldova": "MDA",
    "north korea": "PRK",
    "palestine": "PSE",
    "republic of korea": "KOR",
    "russia": "RUS",
    "south korea": "KOR",
    "syria": "SYR",
    "taiwan": "TWN",
    "trkiye": "TUR",
    "turkey": "TUR",
    "turkey trkiye": "TUR",
    "turkey turkiye": "TUR",
    "turkiye": "TUR",
    "united states": "USA",
    "united states virgin islands": "VIR",
    "usa": "USA",
    "venezuela": "VEN",
    "venezuela bolivarian republic of": "VEN",
    "viet nam": "VNM",
}


def normalize_country_name(value: str | None) -> str:
    """Return an ASCII-stable normalized country name."""

    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.casefold()
    normalized = normalized.replace("(", " ").replace(")", " ")
    normalized = normalized.replace(";", " ").replace(",", " ")
    normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def country_name_to_iso3(country_name: str | None) -> str | None:
    """Map one country name to an ISO3 code without fuzzy guessing."""

    normalized = normalize_country_name(country_name)
    if not normalized:
        return None

    aliased = COUNTRY_ALIAS_ISO3.get(normalized)
    if aliased is not None:
        return aliased

    try:
        country = pycountry.countries.lookup(country_name)
    except LookupError:
        try:
            country = pycountry.countries.lookup(normalized)
        except LookupError:
            return None
    return getattr(country, "alpha_3", None)
