from __future__ import annotations

import logging
import sys
from collections import defaultdict
from time import monotonic

COUNTER_NAMES = (
    "collector_requests_total",
    "collector_items_total",
    "collector_errors_total",
    "analysis_total",
    "analysis_errors_total",
    "analysis_failover_total",
)
GAUGE_NAMES = (
    "analysis_latency_seconds",
    "job_duration_seconds",
    "source_latest_item_timestamp",
    "pending_analysis_count",
    "pending_event_return_count",
)

_values: dict[str, float] = defaultdict(float)


def configure_logging(level: str = "INFO") -> None:
    """Configure concise process logs without mixing them into JSON stdout."""

    normalized = str(level or "INFO").strip().upper()
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    numeric_level = levels.get(normalized)
    if numeric_level is None:
        raise ValueError(f"invalid log level: {level!r}")
    logging.basicConfig(
        level=numeric_level,
        format=("%(asctime)s %(levelname)s %(name)s %(message)s"),
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stderr,
    )
    # Dependency request logs can expose noisy URLs and make source/task events
    # hard to follow. RetailTide emits its own request/result summaries.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def increment(name: str, value: float = 1.0) -> None:
    _values[name] += value


def set_gauge(name: str, value: float) -> None:
    _values[name] = value


def timer(name: str):
    started = monotonic()

    def finish() -> float:
        elapsed = monotonic() - started
        set_gauge(name, elapsed)
        return elapsed

    return finish


def prometheus_text() -> str:
    lines = []
    for name in (*COUNTER_NAMES, *GAUGE_NAMES):
        value = _values.get(name, 0.0)
        kind = "counter" if name.endswith("_total") else "gauge"
        lines.append(f"# TYPE {name} {kind}")
        lines.append(f"{name} {value}")
    return "\n".join(lines) + "\n"
