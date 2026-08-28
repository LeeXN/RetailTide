# RetailTide read-only API reference

Use this reference when the bundled query script is insufficient or the request needs a specific endpoint.

Default local base URL: `http://127.0.0.1:8000`. Override it with `RETAIL_TIDE_URL` or the script's `--base-url` option.

## Endpoint routing

| Endpoint | Use |
| --- | --- |
| `GET /health` | Confirm the service is reachable. |
| `GET /topics` | Resolve Topic slug/name to numeric ID. |
| `GET /topics/overview` | Market and Topic history, Heat, Wikimedia series, representative assets, and coverage. |
| `GET /contents` | Whole-market post evidence and latest analysis. |
| `GET /topics/{topic_id}/contents` | Topic-scoped post evidence and latest Topic analysis. |
| `GET /trends/attention` | All Wikimedia attention observations and derived signals. |
| `GET /topics/{topic_id}/attention` | Topic-scoped Wikimedia series. |
| `GET /sources/status` | Source health, persisted evidence, configuration readiness, and quality. |
| `GET /events` | Detected FOMO, panic, buy-intent, and cross-platform events. |
| `GET /events/{event_id}` | Event metrics, returns, and traceable raw drilldown. |
| `GET /research/event-study` | Event-conditioned return study. |
| `GET /research/quantile-study` | Metric-quantile return study. |

## Date ranges

`/topics/overview` accepts inclusive Shanghai dates:

```text
from_date=2026-08-01&to_date=2026-08-30
```

The default is the latest available data day and its preceding 29 calendar days. The maximum custom range is 366 days.

Post endpoints use timestamps with `period=custom`:

```text
from_at=2026-08-01T00:00:00+08:00
to_at=2026-08-30T23:59:59.999999+08:00
```

## Post filters and pagination

- `filter`: `all`, `retail`, `buy`, `sell`, `hold`, `wait`, `fomo`, `panic`, or `promotion`.
- `source`: `all` or a source name such as `guba`, `taoguba`, `xiaohongshu`, or `zhihu`.
- `limit`: 1–100 per request.
- `offset`: zero-based pagination offset.
- Read `total`, `facets`, and `source_facets` before deciding how much evidence to fetch.

The API returns the latest preferred analysis for each post. Preserve its `model`, confidence, and optional review evidence.

## Research parameters

Event study:

```text
GET /research/event-study?topic=gold&event=fomo_spike
```

Common event values are `fomo_spike`, `panic_spike`, `buy_intent_spike`, and `cross_platform_spike`.

Quantile study:

```text
GET /research/quantile-study?topic=gold&metric=fomo_ratio&horizon=5d
```

Horizons are `1d`, `3d`, `5d`, `10d`, and `20d`. Useful metrics include `fomo_ratio`, `panic_ratio`, `buy_intent_ratio`, retail ratios/counts, and other metric names returned by `/metrics`.

## Direct examples

```bash
curl -fsS 'http://127.0.0.1:8000/topics/overview?from_date=2026-08-01&to_date=2026-08-30'
curl -fsS 'http://127.0.0.1:8000/contents?period=30d&filter=fomo&limit=50'
curl -fsS 'http://127.0.0.1:8000/sources/status'
```

These routes are read-only. Collection, analysis, service restarts, configuration changes, and database access are outside this Skill's implied authority.
