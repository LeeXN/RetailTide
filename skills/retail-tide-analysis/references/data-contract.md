# RetailTide analysis contract

Read this reference before interpreting heat, model labels, source coverage, or market validation.

## Time and evidence layers

- All user-facing daily analysis uses `Asia/Shanghai` calendar days.
- Treat a date range as inclusive at both ends. Do not silently substitute UTC dates.
- Keep these layers distinct: source observation -> normalized content or trend observation -> LLM analysis -> aggregate metric -> event -> return study.
- A later layer can be absent even when an earlier layer is present. State which layer supports each claim.
- Historical collection is a bounded sample, not proof of platform-wide completeness.

## Source roles

| Source | Evidence role | Important boundary |
| --- | --- | --- |
| `guba` | Forum posts and LLM analyses | Public search plus local Shanghai-day filtering; sampled, not exhaustive. |
| `taoguba` | Forum posts and LLM analyses | Public search plus local Shanghai-day filtering; sampled, not exhaustive. |
| `xiaohongshu` | Stable daily discovery sample and LLM analyses | Search is ranked; only detail `note.time` proves the publication day. Never describe it as full-platform coverage. |
| `zhihu` | High-interaction answer snapshots used as market-session references | `reference_date` is the referenced trading day. Do not present an edit timestamp as reliable first publication time. |
| `wikimedia-pageviews` | Independent external-attention validation | Never count pageviews as posts and never add them directly to RetailTide Heat. |
| Market bars | Representative-asset price validation | Missing weekend/holiday bars are expected. Association does not prove causality. |

## Analysis provenance

- Different valid models may coexist. Preserve `analysis.model`, `prompt_version`, and confidence as provenance.
- `unknown` is a valid result, not an error and not a reason to invent a directional label.
- Exclude `promotion=true` and `spam=true` from retail intent/emotion conclusions, while they may remain in total attention or engagement counts.
- Prefer post-level `review.intent_evidence` and `review.rationale` when explaining a label. If the API omits review evidence, quote or paraphrase only what is present in the post body.

## Heat index

For one Topic and one Shanghai day:

- `A`: all content records.
- `R`: analyzed `retail` content with `promotion=false` and `spam=false`.
- `B`, `S`: retail items with buy or sell intent.
- `F`: retail items whose FOMO score is at least `0.5`.
- `P`: retail items with `emotion.panic=true`.

```text
retail_share     = R / A
retail_volume    = R / (R + 20)
intent_activity  = min(1, (B + S) / max(R, 1))
emotion_activity = min(1, (F + P) / max(R, 1))
direction_agree  = min(1, abs(B - S) / max(R, 1))

Heat = 100 * clamp(
  0.35 * retail_share
  + 0.25 * retail_volume
  + 0.20 * intent_activity
  + 0.15 * emotion_activity
  + 0.05 * direction_agree,
  0, 1
)
```

Heat measures activity and expressed retail sentiment, not expected return. High Heat can occur in bullish or bearish discussions.

## Confidence, percentile, and trend

- High confidence: at least 30 analyzed items and analysis coverage at least 80%.
- Medium confidence: at least 10 analyzed items and coverage at least 50%.
- Otherwise, a computed index is low confidence.
- Historical percentile compares the daily index with up to 30 prior valid days using mid-rank. It is unavailable until at least 5 earlier valid days exist.
- Trend combines today vs yesterday (50%), current vs previous 7-day mean (30%), and current vs previous 30-day mean (20%). Missing windows are removed from the weight denominator.
- Trend score thresholds: `>=12` fast warming, `>=4` warming, `<=-12` fast cooling, `<=-4` cooling, otherwise stable.

## Coverage checks before conclusions

Inspect these fields before ranking or comparing:

- End-day collection: `overview.coverage.collection_status` and `overview.coverage.sources`.
- Analysis: `analysis_complete`, `analysis_pending_count`, each Topic's `analysis_coverage`, and `daily_index_confidence`.
- History: `history_coverage.window_days`, `observed_days`, `index_days`, `percentile_days`, and `warming_up_days`.
- Posts: `posts.total`, `posts.returned`, `posts.truncated`, `facets`, and `source_facets`.
- Source health: `source_status[].health_status`, persisted `evidence`, and `quality`.

Do not treat a healthy process, a configured endpoint, or a nonzero count as proof that every relevant post was collected.

## Useful analysis patterns

### Cross-track comparison

Compare Heat, historical percentile, retail count, analysis coverage, and direction/emotion ratios. Rank only after excluding or separately marking low-confidence Topics.

### Internal vs external attention

Compare Topic Heat history with Wikimedia percentile or change. Describe agreement as co-movement and disagreement as divergence. Do not merge the two scales or infer that one caused the other.

### Narrative analysis

Use aggregate facets to choose a bounded post sample, then inspect evidence across sources. Separate author self-action (`buy`, `sell`, `hold`, `wait`) from advice, quoted claims, or general market description.

### Event and return research

Report event count, mature vs pending return rows, horizon, benchmark alignment, sample size, and hit rate. Use “associated with” rather than causal language unless the study design supports causality.
