# Frontier Evidence Manifold

This is the variant that looks most capable of surpassing classical meta-analysis over the next decade.

It starts from a different first principle: the atomic object is not a pooled effect, but a certified evidence state. A study only becomes decision-grade when four surfaces are aligned: contrast recoverability, internal completeness, external transportability, and policy-facing deployability.

`Fatiha` is the main inspiration for the posture. It treats evidence as something that must be purified before it is trusted, rather than merely aggregated. The atlas provides the missing substrate: real AACT trial rows plus IHME, WHO, and World Bank context from D-drive, all under deterministic contracts.

## Why This Beats Pooling-Only Systems

Classical meta-analysis assumes the hard part starts after effect sizes already exist. In practice, the dominant failure modes happen earlier:

- effect surfaces are missing or only partially recoverable
- study records do not map cleanly across groups, arms, and time
- an effect may be estimable in one trial but not portable to a target country-year
- dashboards often report pooled numbers without exposing how much latent signal was discarded

The frontier approach moves those failure modes into the primary data model.

## Core Object

Each row in the evidence manifold should eventually carry:

- recoverable contrast state
- precision state
- contradiction state
- transportability state
- integrity state
- deployment state

Pooling becomes a downstream query over rows that have already survived purification.

## POC in This Repo

The proof-of-concept does not try to solve the entire problem. It does one high-value thing with real local data:

1. Read the current `trial_outcomes_long.parquet`, `effect_candidates.parquet`, `trial_country_year.parquet`, and `synthesis_output.parquet`.
2. Detect latent recoverable event contrasts that the strict candidate builder still leaves unused because AACT stores counts in `subjects_affected`.
3. Aggregate those candidate states into a trial-level frontier score.
4. Join that frontier score onto the country-year transportability surface.
5. Emit a purification-oriented report showing where the evidence manifold is already strong and where effect recovery is the main bottleneck.

## What Makes It Transformative

If this direction is pushed fully, the dominant system is not a single estimator. It is an evidence operating system with these properties:

- every study becomes a typed state object, not an unexamined row in a forest plot
- effect estimation, contradiction handling, and transportability are one pipeline
- country, year, patient archetype, and outcome family become query axes
- fail-closed missingness is preserved instead of silently averaged away
- policy and bedside decisions are made against purified local evidence states, not generic pooled means

## Immediate Next Step

The next technical unlock is arm mapping. Once AACT group metadata can be tied to treatment/control identities, the frontier layer can move from latent recoverability to signed effect estimation and then to genuine purified synthesis.
