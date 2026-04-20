"""Trial Transportability Atlas scaffold."""

from .aact_bridge import extract_trial_country_year, extract_trial_outcomes, validate_aact_snapshot
from .context_join import build_context_joined, materialize_context_join
from .contracts import (
    ALL_DATASET_CONTRACTS,
    ATLAS_CONTEXT_CONTRACT,
    SYNTHESIS_OUTPUT_CONTRACT,
    TRIAL_BRIDGE_CONTRACT,
)
from .country_iso3 import country_name_to_iso3
from .project_paths import discover_aact_snapshot, discover_external_paths
from .source_adapters import (
    load_ihme_context,
    load_unified_context,
    load_wb_context,
    load_who_context,
)
from .topics import (
    FrozenTopic,
    PHASE1_TOPIC,
    load_frozen_topic,
    normalize_text,
    select_topic_nct_ids,
)

__all__ = [
    "ALL_DATASET_CONTRACTS",
    "ATLAS_CONTEXT_CONTRACT",
    "FrozenTopic",
    "PHASE1_TOPIC",
    "SYNTHESIS_OUTPUT_CONTRACT",
    "TRIAL_BRIDGE_CONTRACT",
    "build_context_joined",
    "country_name_to_iso3",
    "discover_aact_snapshot",
    "discover_external_paths",
    "extract_trial_country_year",
    "extract_trial_outcomes",
    "load_frozen_topic",
    "load_ihme_context",
    "load_unified_context",
    "load_wb_context",
    "load_who_context",
    "materialize_context_join",
    "normalize_text",
    "select_topic_nct_ids",
    "validate_aact_snapshot",
]
