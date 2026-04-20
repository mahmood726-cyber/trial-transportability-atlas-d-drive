from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path

import pandas as pd


DISPLAY_TOPIC_TITLES = {
    "sacubitril_valsartan_hfref": "Sacubitril/Valsartan in HFrEF",
}


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _format_int(value: int | float) -> str:
    return f"{int(value):,}"


def _format_score(value: int | float) -> str:
    return f"{float(value):.3f}"


def _format_timestamp(path: Path) -> str:
    timestamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return timestamp.strftime("%Y-%m-%d %H:%M UTC")


def _count_missing_signals(value: str) -> int:
    if not value:
        return 0
    return len([item for item in value.split(";") if item])


def _humanize_topic(topic_slug: str) -> str:
    return topic_slug.replace("_", " ")


def _display_topic_title(topic_slug: str) -> str:
    return DISPLAY_TOPIC_TITLES.get(topic_slug, _humanize_topic(topic_slug).title())


def _latest_timestamp(paths: list[Path]) -> str:
    latest_path = max(paths, key=lambda path: path.stat().st_mtime)
    return _format_timestamp(latest_path)


def _render_metric_cards(cards: list[tuple[str, str, str]]) -> str:
    blocks = []
    for label, value, detail in cards:
        blocks.append(
            "\n".join(
                [
                    '<section class="metric-card">',
                    f'  <div class="metric-label">{escape(label)}</div>',
                    f'  <div class="metric-value">{escape(value)}</div>',
                    f'  <div class="metric-detail">{escape(detail)}</div>',
                    "</section>",
                ]
            )
        )
    return "\n".join(blocks)


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>")
    return "\n".join(
        [
            '<div class="table-wrap">',
            "  <table>",
            f"    <thead><tr>{header_html}</tr></thead>",
            f"    <tbody>{''.join(body_rows)}</tbody>",
            "  </table>",
            "</div>",
        ]
    )


def _render_badges(values: list[str], css_class: str) -> str:
    return "".join(f'<span class="{css_class}">{escape(value)}</span>' for value in values)


def build_dashboard_html(output_dir: Path) -> str:
    output_dir = Path(output_dir)
    run_manifest_path = output_dir / "run_manifest.json"
    context_manifest_path = output_dir / "context_join_manifest.json"
    transport_manifest_path = output_dir / "transportability_manifest.json"
    summary_path = output_dir / "evidence_gap_summary.parquet"
    country_year_path = output_dir / "transportability_country_year.parquet"

    run_manifest = _read_json(run_manifest_path)
    context_manifest = _read_json(context_manifest_path)
    transport_manifest = _read_json(transport_manifest_path)
    summary = pd.read_parquet(summary_path).sort_values(
        ["latest_priority_gap_score", "country_name"],
        ascending=[False, True],
    )
    country_year = pd.read_parquet(country_year_path).sort_values(
        ["priority_gap_score", "country_name", "year"],
        ascending=[False, True, True],
    )

    topic_slug = str(run_manifest.get("topic_slug", output_dir.name))
    latest_materialization = _latest_timestamp(
        [
            run_manifest_path,
            context_manifest_path,
            transport_manifest_path,
        ]
    )
    zero_signal_rows = int(country_year["available_core_signal_count"].eq(0).sum())
    no_comparable_rows = int(country_year["comparable_trial_count"].eq(0).sum())
    full_signal_rows = int(
        country_year["available_core_signal_count"].eq(country_year["expected_core_signal_count"]).sum()
    )
    strongest = summary.sort_values(
        ["latest_transportability_score", "comparable_candidate_count", "country_name"],
        ascending=[False, False, True],
    ).head(10)
    highest_gaps = summary.head(10)

    latest_zero_context = summary.loc[summary["latest_transportability_score"].eq(0.0)].head(4)
    latest_zero_context_labels = [
        f"{row.country_name} {int(row.latest_year)}" for row in latest_zero_context.itertuples()
    ]

    metric_cards = _render_metric_cards(
        [
            (
                "Selected NCT IDs",
                _format_int(len(run_manifest.get("selected_nct_ids", []))),
                f"{_format_int(run_manifest.get('strict_comparable_candidates', 0))} strict comparable candidates",
            ),
            (
                "Context Rows",
                _format_int(context_manifest.get("context_rows", 0)),
                f"{_format_int(context_manifest.get('context_available_rows', 0))} with source-backed context",
            ),
            (
                "Scored Country-Years",
                _format_int(transport_manifest.get("country_year_rows", 0)),
                f"{_format_int(transport_manifest.get('summary_rows', 0))} country summaries",
            ),
            (
                "Fail-Closed Rows",
                _format_int(zero_signal_rows),
                f"{_format_int(no_comparable_rows)} rows without comparable evidence",
            ),
        ]
    )

    source_badges = _render_badges(context_manifest.get("distinct_context_sources", []), "chip")
    signal_badges = _render_badges(transport_manifest.get("core_signal_keys", []), "chip chip-muted")

    gap_rows = []
    for row in highest_gaps.itertuples():
        gap_rows.append(
            [
                f"<strong>{escape(row.country_name)}</strong>",
                escape(row.iso3),
                escape(str(int(row.latest_year))),
                escape(_format_int(row.trial_count)),
                escape(_format_int(row.comparable_trial_count)),
                escape(_format_score(row.latest_transportability_score)),
                escape(_format_score(row.latest_priority_gap_score)),
                escape(str(_count_missing_signals(row.missing_core_signals_union))),
            ]
        )

    strongest_rows = []
    for row in strongest.itertuples():
        strongest_rows.append(
            [
                f"<strong>{escape(row.country_name)}</strong>",
                escape(row.iso3),
                escape(str(int(row.latest_year))),
                escape(_format_score(row.latest_transportability_score)),
                escape(_format_score(row.mean_country_coverage_score)),
                escape(_format_score(row.mean_eligibility_support_score)),
                escape(_format_int(row.comparable_candidate_count)),
            ]
        )

    zero_context_text = ", ".join(latest_zero_context_labels) if latest_zero_context_labels else "none"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="data:,">
  <title>Trial Transportability Atlas Dashboard</title>
  <style>
    :root {{
      --ink: #1e2430;
      --muted: #5d6878;
      --paper: #f6f1e8;
      --panel: #fffdf8;
      --border: #d8cdbd;
      --accent: #b6452c;
      --accent-deep: #7f2716;
      --accent-soft: #f4d8cf;
      --olive: #54633f;
      --shadow: rgba(45, 34, 20, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(182, 69, 44, 0.16), transparent 28rem),
        radial-gradient(circle at top right, rgba(84, 99, 63, 0.16), transparent 30rem),
        linear-gradient(180deg, #fbf8f1 0%, var(--paper) 100%);
      font-family: Georgia, "Palatino Linotype", "Book Antiqua", serif;
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero {{
      padding: 28px;
      border: 1px solid var(--border);
      border-radius: 28px;
      background: linear-gradient(145deg, rgba(255, 253, 248, 0.96), rgba(249, 242, 231, 0.9));
      box-shadow: 0 16px 40px var(--shadow);
    }}
    .eyebrow {{
      margin: 0 0 10px;
      color: var(--accent-deep);
      font-size: 0.86rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2.3rem, 4vw, 4.2rem);
      line-height: 0.96;
      letter-spacing: -0.03em;
    }}
    .lede {{
      max-width: 70ch;
      margin: 18px 0 0;
      color: var(--muted);
      font-size: 1.02rem;
    }}
    .meta-grid,
    .metrics-grid,
    .panel-grid {{
      display: grid;
      gap: 16px;
    }}
    .meta-grid {{
      margin-top: 24px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .metrics-grid {{
      margin-top: 26px;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    }}
    .panel-grid {{
      margin-top: 22px;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }}
    .meta-card,
    .metric-card,
    .panel {{
      border: 1px solid var(--border);
      border-radius: 22px;
      background: var(--panel);
      box-shadow: 0 14px 30px rgba(45, 34, 20, 0.05);
    }}
    .meta-card,
    .panel {{
      padding: 20px;
    }}
    .metric-card {{
      padding: 18px 18px 16px;
      background: linear-gradient(180deg, rgba(255, 251, 244, 0.95), rgba(247, 240, 228, 0.95));
    }}
    .meta-label,
    .metric-label,
    .panel-label {{
      color: var(--muted);
      font-size: 0.84rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .meta-value {{
      margin-top: 8px;
      font-size: 1rem;
      word-break: break-word;
    }}
    .metric-value {{
      margin-top: 10px;
      font-size: 2rem;
      line-height: 1;
      color: var(--accent-deep);
    }}
    .metric-detail {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .section-title {{
      margin: 34px 0 12px;
      font-size: 1.45rem;
      letter-spacing: -0.02em;
    }}
    .panel-title {{
      margin: 0 0 8px;
      font-size: 1.18rem;
      letter-spacing: -0.02em;
    }}
    .notice {{
      border-left: 4px solid var(--accent);
      padding: 14px 16px;
      margin-top: 22px;
      border-radius: 16px;
      background: rgba(244, 216, 207, 0.6);
      color: #4f2b23;
    }}
    .chip-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 14px;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      padding: 7px 11px;
      border-radius: 999px;
      background: rgba(182, 69, 44, 0.1);
      color: var(--accent-deep);
      font-size: 0.9rem;
      border: 1px solid rgba(182, 69, 44, 0.18);
    }}
    .chip-muted {{
      background: rgba(84, 99, 63, 0.09);
      color: var(--olive);
      border-color: rgba(84, 99, 63, 0.18);
    }}
    .table-wrap {{
      overflow-x: auto;
      margin-top: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}
    th,
    td {{
      padding: 11px 10px;
      border-bottom: 1px solid #e8dece;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      font-size: 0.82rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .footer-note {{
      margin-top: 24px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    @media (max-width: 720px) {{
      main {{ padding: 20px 14px 40px; }}
      .hero,
      .meta-card,
      .panel,
      .metric-card {{ border-radius: 18px; }}
      h1 {{ font-size: 2.2rem; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">Phase-1 Topic Dashboard</p>
      <h1>{escape(_display_topic_title(topic_slug))}</h1>
      <p class="lede">
        Static reporting layer built from the materialized AACT bridge, multi-source context join,
        and transportability scoring outputs. Missing source-backed context stays missing; the
        dashboard does not backfill or impute country-year values.
      </p>
      <div class="meta-grid">
        <section class="meta-card">
          <div class="meta-label">AACT Snapshot</div>
          <div class="meta-value">{escape(str(run_manifest.get("snapshot_dir", "")))}</div>
        </section>
        <section class="meta-card">
          <div class="meta-label">Topic Slug</div>
          <div class="meta-value">{escape(topic_slug)}</div>
        </section>
        <section class="meta-card">
          <div class="meta-label">Outputs Last Updated</div>
          <div class="meta-value">{escape(latest_materialization)}</div>
        </section>
        <section class="meta-card">
          <div class="meta-label">Context Sources</div>
          <div class="meta-value">{escape(str(len(context_manifest.get("distinct_context_sources", []))))} sources</div>
        </section>
      </div>
      <div class="metrics-grid">
        {metric_cards}
      </div>
      <div class="notice">
        <strong>Fail-closed rule:</strong> latest-year score zeroes in the current outputs include
        {escape(zero_context_text)}. These are source-coverage limits or non-comparable evidence
        surfaces, not inferred values.
      </div>
    </section>

    <h2 class="section-title">Source Surface</h2>
    <div class="panel-grid">
      <section class="panel">
        <div class="panel-label">Context Sources</div>
        <div class="chip-row">{source_badges}</div>
      </section>
      <section class="panel">
        <div class="panel-label">Core Signals</div>
        <div class="chip-row">{signal_badges}</div>
      </section>
    </div>

    <h2 class="section-title">Latest-Year Evidence Gaps</h2>
    <section class="panel">
      <h3 class="panel-title">Highest latest-year priority gaps</h3>
      <p class="lede">
        Countries are sorted by latest-year priority gap. Higher values mean lower transportability
        readiness under the current source-backed context and comparable-evidence surface.
      </p>
      {_render_table(
          ["Country", "ISO3", "Latest year", "Trials", "Comparable trials", "Latest score", "Gap", "Missing signals"],
          gap_rows,
      )}
    </section>

    <h2 class="section-title">Strongest Current Support</h2>
    <section class="panel">
      <h3 class="panel-title">Highest latest transportability scores</h3>
      <p class="lede">
        This table ranks countries by latest transportability score, then by comparable candidate
        support. It is descriptive only and should not be read as a causal deployment ranking.
      </p>
      {_render_table(
          ["Country", "ISO3", "Latest year", "Latest score", "Mean coverage", "Mean eligibility", "Comparable candidates"],
          strongest_rows,
      )}
    </section>

    <p class="footer-note">
      Reporting inputs: <code>run_manifest.json</code>, <code>context_join_manifest.json</code>,
      <code>transportability_manifest.json</code>, <code>transportability_country_year.parquet</code>,
      and <code>evidence_gap_summary.parquet</code>.
      Full-signal country-years: {_format_int(full_signal_rows)}. Zero-signal country-years: {_format_int(zero_signal_rows)}.
    </p>
  </main>
</body>
</html>
"""


def materialize_dashboard(output_dir: Path, dashboard_path: Path | None = None) -> dict:
    output_dir = Path(output_dir)
    if dashboard_path is None:
        repo_root = output_dir.parent.parent if output_dir.parent.name == "outputs" else output_dir.parent
        dashboard_path = repo_root / "dashboard" / "transportability_dashboard.html"
    dashboard_path = Path(dashboard_path)
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)

    html = build_dashboard_html(output_dir)
    dashboard_path.write_text(html, encoding="utf-8")

    summary = pd.read_parquet(output_dir / "evidence_gap_summary.parquet")
    return {
        "dashboard_path": str(dashboard_path),
        "topic_slug": output_dir.name,
        "summary_rows": int(len(summary)),
    }
