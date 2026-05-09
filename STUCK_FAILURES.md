# STUCK_FAILURES.md

*Written by Sentinel — BLOCK-tier violations.*

## [BLOCK] P0-hardcoded-local-path
- **Location:** `src/trial_transportability_atlas/simulations.py:7`
- **Detail:** pattern matched: base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.572061+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `src/trial_transportability_atlas/policy_simulations.py:7`
- **Detail:** pattern matched: base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.633392+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `configs/topics/phase1-topic.md:26`
- **Detail:** pattern matched: `C:\Projects\registry_first_rct_meta\outputs\population_runs\heart_failure_sacubitril_valsartan\population_evidence_summ
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.646658+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `PLAN.md:12`
- **Detail:** pattern matched: - `D:\Projects\ihme-data-lakehouse`
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.651264+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `PLAN.md:13`
- **Detail:** pattern matched: - `D:\Projects\who-data-lakehouse`
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.651288+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `PLAN.md:14`
- **Detail:** pattern matched: - `D:\Projects\wb-data-lakehouse`
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.651302+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `analyze_sglt2.py:7`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.785302+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `orchestrate_topic.py:16`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic.slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.825925+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `generate_all_scores.py:6`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.845383+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `cross_topic_sensitivity.py:7`
- **Detail:** pattern matched: base_dir = Path("D:/Projects/trial-transportability-atlas/outputs")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.858786+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `cross_topic_sensitivity.py:65`
- **Detail:** pattern matched: report_path = Path("D:/Projects/trial-transportability-atlas/outputs/sensitivity_check.md")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.859347+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `src/trial_transportability_atlas/predictive_mapping.py:5`
- **Detail:** pattern matched: base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.930521+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `orchestrate_sglt2.py:14`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic.slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.932476+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `materialize_all_transport.py:9`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T15:50:24.958807+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `generate_all_scores.py:6`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.371411+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `materialize_all_transport.py:9`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.381554+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `orchestrate_sglt2.py:14`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic.slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.384157+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `PLAN.md:12`
- **Detail:** pattern matched: - `D:\Projects\ihme-data-lakehouse`
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.396869+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `PLAN.md:13`
- **Detail:** pattern matched: - `D:\Projects\who-data-lakehouse`
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.396898+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `PLAN.md:14`
- **Detail:** pattern matched: - `D:\Projects\wb-data-lakehouse`
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.396932+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `src/trial_transportability_atlas/policy_simulations.py:7`
- **Detail:** pattern matched: base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.416615+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `src/trial_transportability_atlas/simulations.py:7`
- **Detail:** pattern matched: base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.419332+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `src/trial_transportability_atlas/predictive_mapping.py:5`
- **Detail:** pattern matched: base_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.427036+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `configs/topics/phase1-topic.md:26`
- **Detail:** pattern matched: `C:\Projects\registry_first_rct_meta\outputs\population_runs\heart_failure_sacubitril_valsartan\population_evidence_summ
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.428731+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `cross_topic_sensitivity.py:7`
- **Detail:** pattern matched: base_dir = Path("D:/Projects/trial-transportability-atlas/outputs")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.444399+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `cross_topic_sensitivity.py:65`
- **Detail:** pattern matched: report_path = Path("D:/Projects/trial-transportability-atlas/outputs/sensitivity_check.md")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.444659+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `orchestrate_topic.py:16`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic.slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.448213+00:00

## [BLOCK] P0-hardcoded-local-path
- **Location:** `analyze_sglt2.py:7`
- **Detail:** pattern matched: output_dir = Path(f"D:/Projects/trial-transportability-atlas/outputs/{topic_slug}")
- **Fix hint:** Replace absolute paths with relative paths, config-driven roots, or environment variables. Use candidate-root discovery for data snapshots.

- **Source:** lessons.md#code-quality
- **When:** 2026-04-23T16:27:47.462541+00:00
