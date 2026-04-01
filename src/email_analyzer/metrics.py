from __future__ import annotations

from collections import defaultdict
from time import perf_counter_ns
from typing import Any, Callable, TypeVar

T = TypeVar("T")


def measure_step(step_name: str, timings: dict[str, float], func: Callable[..., T], *args: Any, **kwargs: Any) -> tuple[T, float]:
    start_ns = perf_counter_ns()
    result = func(*args, **kwargs)
    duration_ms = round((perf_counter_ns() - start_ns) / 1_000_000, 3)
    timings[step_name] = duration_ms
    return result, duration_ms


def total_duration_ms(start_ns: int) -> float:
    return round((perf_counter_ns() - start_ns) / 1_000_000, 3)


def estimate_token_count(text: str) -> int:
    compact = text.strip()
    if not compact:
        return 0
    return max(1, (len(compact) + 3) // 4)


def aggregate_step_metrics(file_summaries: list[dict[str, Any]]) -> dict[str, dict[str, float | int | None]]:
    durations: dict[str, list[float]] = defaultdict(list)
    bytes_by_step: dict[str, int] = defaultdict(int)

    for summary in file_summaries:
        source_bytes = int(summary.get("stats", {}).get("source_bytes", 0))
        for step, duration in summary.get("timings_ms", {}).items():
            durations[step].append(float(duration))
            bytes_by_step[step] += source_bytes

    aggregate: dict[str, dict[str, float | int | None]] = {}
    for step, values in sorted(durations.items()):
        ordered = sorted(values)
        total_ms = round(sum(ordered), 3)
        aggregate[step] = {
            "count": len(ordered),
            "total_ms": total_ms,
            "avg_ms": round(total_ms / len(ordered), 3),
            "p50_ms": _percentile(ordered, 0.50),
            "p95_ms": _percentile(ordered, 0.95),
            "emails_per_second": round(len(ordered) / (total_ms / 1000), 3) if total_ms else None,
            "megabytes_per_second": round((bytes_by_step[step] / 1024 / 1024) / (total_ms / 1000), 3)
            if total_ms
            else None,
        }

    return aggregate


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 3)

    index = (len(values) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return round(values[lower], 3)

    weight = index - lower
    interpolated = values[lower] * (1 - weight) + values[upper] * weight
    return round(interpolated, 3)
