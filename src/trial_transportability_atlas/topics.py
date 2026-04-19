"""Topic-loading helpers for the frozen phase-1 atlas demo."""
from __future__ import annotations

from csv import DictReader
from dataclasses import dataclass
from pathlib import Path
import re

from trial_transportability_atlas.aact_bridge import validate_aact_snapshot


TOPICS_DIR = Path(__file__).resolve().parents[2] / "configs" / "topics"
DEFAULT_PHASE1_TOPIC_PATH = TOPICS_DIR / "phase1-topic.md"


class TopicConfigError(ValueError):
    """Raised when the phase-1 topic configuration is incomplete or invalid."""


@dataclass(frozen=True)
class FrozenTopic:
    """Deterministic topic definition parsed from the phase-1 note."""

    name: str
    status: str
    inclusion_terms: tuple[str, ...]
    exclusion_terms: tuple[str, ...]
    expected_source_tables: tuple[str, ...]
    intended_joins: tuple[str, ...]
    known_failure_modes: tuple[str, ...]
    source_path: Path

    @property
    def slug(self) -> str:
        normalized = self.name.lower()
        translation = str.maketrans(
            {
                "/": "_",
                " ": "_",
                "-": "_",
                "(": "",
                ")": "",
                ",": "",
            }
        )
        collapsed = normalized.translate(translation)
        while "__" in collapsed:
            collapsed = collapsed.replace("__", "_")
        return collapsed.strip("_")


@dataclass(frozen=True)
class TopicSpec:
    """Deterministic AACT selection rules for the phase-1 topic."""

    slug: str
    intervention_terms: tuple[str, ...]
    intervention_exclude_terms: tuple[str, ...]
    condition_terms: tuple[str, ...]
    condition_exclude_terms: tuple[str, ...]
    summary_include_terms: tuple[str, ...]
    summary_exclude_terms: tuple[str, ...]


PHASE1_TOPIC = TopicSpec(
    slug="sacubitril_valsartan_hfref",
    intervention_terms=(
        "sacubitril valsartan",
        "entresto",
        "lcz696",
        "sacubitril valsartan oral tablet entresto",
    ),
    intervention_exclude_terms=(
        "matching placebo",
        "placebo sacubitril valsartan",
    ),
    condition_terms=(
        "heart failure reduced ejection fraction",
        "heart failure with reduced ejection fraction",
        "heart failure systolic",
        "systolic heart failure",
        "hfref",
        "hfr ef",
        "hf ref",
    ),
    condition_exclude_terms=(
        "preserved ejection fraction",
        "hfpef",
        "diastolic",
        "pulmonary hypertension",
        "resistant hypertension",
        "prediabetes",
        "impaired glucose tolerance",
        "blood pressure",
    ),
    summary_include_terms=(
        "heart failure with reduced ejection fraction",
        "reduced ejection fraction",
        "lvad",
        "left ventricular assist devices",
    ),
    summary_exclude_terms=(
        "preserved ejection fraction",
        "hfpef",
        "pulmonary hypertension",
        "resistant hypertension",
        "impaired glucose tolerance",
        "prediabetes",
    ),
)


def normalize_text(value: str | None) -> str:
    """Normalize free text for strict exact-term matching."""

    if not value:
        return ""
    lowered = value.casefold()
    lowered = lowered.replace("™", " ").replace("®", " ")
    lowered = re.sub(r"[\s/_\-]+", " ", lowered)
    lowered = re.sub(r"[^\w\s()]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _parse_markdown_sections(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current_header: str | None = None
    current_lines: list[str] = []

    for raw_line in lines:
        line = raw_line.rstrip()
        if line.endswith(":") and not line.startswith("#"):
            if current_header is not None:
                sections[current_header] = current_lines
            current_header = line[:-1]
            current_lines = []
            continue
        if current_header is not None:
            current_lines.append(line)

    if current_header is not None:
        sections[current_header] = current_lines
    return sections


def _extract_scalar(lines: list[str], prefix: str) -> str:
    for line in lines:
        if line.startswith(prefix):
            value = line.split(":", 1)[1].strip()
            if value:
                return value
    raise TopicConfigError(f"Missing required scalar field: {prefix}")


def _extract_bullets(sections: dict[str, list[str]], header: str) -> tuple[str, ...]:
    lines = sections.get(header)
    if not lines:
        raise TopicConfigError(f"Missing required section: {header}")

    values: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        bullet = stripped[2:].strip()
        if bullet.startswith("`") and bullet.endswith("`"):
            bullet = bullet[1:-1]
        values.append(bullet)

    if not values:
        raise TopicConfigError(f"Section has no bullet values: {header}")
    return tuple(values)


def _iter_rows(snapshot_dir: Path, table_name: str):
    table_path = snapshot_dir / f"{table_name}.txt"
    with table_path.open("r", encoding="utf-8", newline="") as handle:
        yield from DictReader(handle, delimiter="|")


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern in text for pattern in patterns)


def _trial_matches_topic(
    topic: TopicSpec,
    *,
    conditions: list[str],
    summaries: list[str],
) -> bool:
    if any(_contains_any(text, topic.condition_exclude_terms) for text in conditions):
        return False
    if any(_contains_any(text, topic.summary_exclude_terms) for text in summaries):
        return False

    condition_hit = any(_contains_any(text, topic.condition_terms) for text in conditions)
    summary_hit = any(_contains_any(text, topic.summary_include_terms) for text in summaries)
    return condition_hit or summary_hit


def load_frozen_topic(path: Path = DEFAULT_PHASE1_TOPIC_PATH) -> FrozenTopic:
    """Load the accepted phase-1 topic from the repo note."""

    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    status = _extract_scalar(lines, "Status")
    name = _extract_scalar(lines, "Accepted topic")
    sections = _parse_markdown_sections(lines)

    if status.lower() != "accepted":
        raise TopicConfigError(f"Phase-1 topic status must be accepted, got: {status}")

    return FrozenTopic(
        name=name,
        status=status,
        inclusion_terms=_extract_bullets(sections, "Exact inclusion terms"),
        exclusion_terms=_extract_bullets(sections, "Exact exclusion terms"),
        expected_source_tables=_extract_bullets(sections, "Expected AACT source tables"),
        intended_joins=_extract_bullets(sections, "Intended IHME / WHO / WB joins"),
        known_failure_modes=_extract_bullets(sections, "Known failure modes"),
        source_path=path,
    )


def select_topic_nct_ids(snapshot_dir: Path, topic: TopicSpec = PHASE1_TOPIC) -> set[str]:
    """Select NCT ids for the frozen topic from the local AACT snapshot."""

    validate_aact_snapshot(snapshot_dir)

    candidate_nct_ids: set[str] = set()
    for row in _iter_rows(snapshot_dir, "interventions"):
        nct_id = (row.get("nct_id") or "").strip()
        text = normalize_text(row.get("name"))
        if not text:
            continue
        if _contains_any(text, topic.intervention_terms) and not _contains_any(
            text, topic.intervention_exclude_terms
        ):
            candidate_nct_ids.add(nct_id)

    conditions_by_nct: dict[str, list[str]] = {}
    for row in _iter_rows(snapshot_dir, "conditions"):
        nct_id = (row.get("nct_id") or "").strip()
        if nct_id not in candidate_nct_ids:
            continue
        text = normalize_text(row.get("name"))
        if text:
            conditions_by_nct.setdefault(nct_id, []).append(text)

    summaries_by_nct: dict[str, list[str]] = {}
    for row in _iter_rows(snapshot_dir, "brief_summaries"):
        nct_id = (row.get("nct_id") or "").strip()
        if nct_id not in candidate_nct_ids:
            continue
        text = normalize_text(row.get("description"))
        if text:
            summaries_by_nct.setdefault(nct_id, []).append(text)

    selected: set[str] = set()
    for nct_id in candidate_nct_ids:
        if _trial_matches_topic(
            topic,
            conditions=conditions_by_nct.get(nct_id, []),
            summaries=summaries_by_nct.get(nct_id, []),
        ):
            selected.add(nct_id)
    return selected
