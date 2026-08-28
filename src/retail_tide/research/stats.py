from __future__ import annotations

import math
from collections.abc import Iterable
from statistics import mean, median, pstdev


def clean(values: Iterable[float | None]) -> list[float]:
    return [float(value) for value in values if value is not None and math.isfinite(float(value))]


def confidence_interval_95(values: Iterable[float | None]) -> tuple[float | None, float | None]:
    values = clean(values)
    if not values:
        return None, None
    average = mean(values)
    if len(values) == 1:
        return average, average
    standard_error = pstdev(values) / math.sqrt(len(values))
    return average - 1.96 * standard_error, average + 1.96 * standard_error


def summarize(values: Iterable[float | None]) -> dict[str, float | int | None]:
    values = clean(values)
    if not values:
        return {
            "N": 0,
            "mean": None,
            "median": None,
            "std": None,
            "hit_rate": None,
            "ci95": [None, None],
        }
    ci = confidence_interval_95(values)
    return {
        "N": len(values),
        "mean": mean(values),
        "median": median(values),
        "std": pstdev(values) if len(values) > 1 else 0.0,
        "hit_rate": sum(value > 0 for value in values) / len(values),
        "ci95": [ci[0], ci[1]],
    }


def rank(values: list[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        average_rank = (index + 1 + end) / 2
        for position in range(index, end):
            ranks[ordered[position][0]] = average_rank
        index = end
    return ranks


def spearman(x: Iterable[float | None], y: Iterable[float | None]) -> float | None:
    pairs = [(float(a), float(b)) for a, b in zip(x, y) if a is not None and b is not None]
    if len(pairs) < 2:
        return None
    rx = rank([pair[0] for pair in pairs])
    ry = rank([pair[1] for pair in pairs])
    mx, my = mean(rx), mean(ry)
    numerator = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    denominator = math.sqrt(sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry))
    return numerator / denominator if denominator else 0.0
