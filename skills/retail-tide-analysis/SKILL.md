---
name: retail-tide-analysis
description: Analyze a running RetailTide instance through its read-only API for retail-investor heat, Topic comparisons, post evidence, Wikimedia attention, source coverage, events, and return studies. Use for evidence-backed RetailTide data questions; do not use to collect data, change configuration, restart services, access credentials, or provide personalized trading advice.
---

# Retail Tide Analysis

Use RetailTide as a traceable research dataset, not as a trading oracle. Query the HTTP API first and keep every conclusion tied to an explicit Shanghai date range, coverage state, and evidence layer.

## Start with a bounded evidence bundle

Run the bundled script from this skill directory:

```bash
python scripts/retail_tide_query.py bundle \
  --from-date 2026-08-01 --to-date 2026-08-30 \
  --topic semiconductor --post-limit 100
```

Omit both dates to use the latest available 30 Shanghai calendar days. Omit `--topic` for the whole market. Use `--filter`, `--source`, and a smaller `--post-limit` to narrow evidence before increasing context size.

The bundle contains source health, aggregate history, independent Wikimedia attention, bounded posts with model provenance, and detected events. Treat its `warnings` as required limitations in the answer.

For a smaller request, use one of:

```bash
python scripts/retail_tide_query.py overview --topic ai
python scripts/retail_tide_query.py posts --filter panic --post-limit 50
python scripts/retail_tide_query.py sources
python scripts/retail_tide_query.py research --topic gold --event fomo_spike --metric fomo_ratio --horizon 5d
```

Use `--base-url` or `RETAIL_TIDE_URL` when the service is not at `http://127.0.0.1:8000`.

## Choose the analysis mode

- Market or Topic comparison: start with `overview`; compare Heat, historical percentile, confidence, sample counts, and trend windows.
- Narrative or intent question: inspect `posts.facets`, then read a bounded cross-source post sample and its evidence-bearing analyses.
- External-attention validation: compare Wikimedia and RetailTide histories without combining their values.
- Data-quality question: use `sources` plus overview/post coverage; distinguish collected, normalized, analyzed, and derived layers.
- Event or return question: use `research`, report sample maturity and benchmark alignment, and avoid causal language.

Before interpreting fields or confidence, read [references/data-contract.md](references/data-contract.md). For direct endpoint access or unsupported drilldowns, read [references/api-reference.md](references/api-reference.md).

## Analysis rules

1. State the inclusive `Asia/Shanghai` date range and Topic/source filters.
2. Check coverage before ranking, comparing, or generalizing. A configured or healthy source is not proof of exhaustive coverage.
3. Preserve source roles: posts drive LLM sentiment; Zhihu is reference evidence; Wikimedia is independent attention; market bars are representative-price validation.
4. Preserve model provenance. Different models may coexist, and valid `unknown` labels must remain unknown.
5. Separate observation from inference. Report counts, ratios, percentiles, and post evidence before explaining a possible narrative.
6. Describe correlations as co-movement, divergence, or association. Do not claim causality or expected return from Heat alone.
7. Prefer the read-only API. Do not read SQLite, trigger collection/analysis, mutate configuration, or restart services unless the user separately authorizes that work.

## Answer shape

Lead with the result, then include only the evidence needed to audit it:

- scope: date range, Topic, sources, and filters;
- coverage: observed/analyzed days, confidence, missing or degraded sources, and truncation;
- findings: exact comparisons and direction of change;
- evidence: aggregate fields plus a few representative posts or event rows;
- limitations: sampling, model provenance, missing market days, and non-causal interpretation.

If evidence is insufficient, say what is present, what layer is missing, and which read-only query would resolve it. Do not fill gaps with assumptions.
