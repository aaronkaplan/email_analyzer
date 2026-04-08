from __future__ import annotations

from email_analyzer.metrics import (
    aggregate_step_metrics,
    estimate_token_count,
    measure_step,
    total_duration_ms,
)


# --- measure_step ---


def test_measure_step_records_timing() -> None:
    timings: dict[str, float] = {}
    result, duration = measure_step("test_step", timings, lambda: 42)
    assert result == 42
    assert "test_step" in timings
    assert timings["test_step"] >= 0
    assert duration >= 0


def test_measure_step_passes_args() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    timings: dict[str, float] = {}
    result, _ = measure_step("add_step", timings, add, 3, 7)
    assert result == 10


def test_measure_step_passes_kwargs() -> None:
    def greet(name: str = "world") -> str:
        return f"hello {name}"

    timings: dict[str, float] = {}
    result, _ = measure_step("greet_step", timings, greet, name="alice")
    assert result == "hello alice"


def test_measure_step_propagates_exception() -> None:
    def fail() -> None:
        raise ValueError("test error")

    timings: dict[str, float] = {}
    try:
        measure_step("fail_step", timings, fail)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert str(e) == "test error"


# --- total_duration_ms ---


def test_total_duration_ms_returns_positive() -> None:
    from time import perf_counter_ns

    start = perf_counter_ns()
    duration = total_duration_ms(start)
    assert duration >= 0.0


# --- estimate_token_count ---


def test_estimate_token_count_normal_text() -> None:
    # "Hello world!" = 12 chars -> (12 + 3) // 4 = 3
    count = estimate_token_count("Hello world!")
    assert count == 3


def test_estimate_token_count_empty() -> None:
    assert estimate_token_count("") == 0


def test_estimate_token_count_whitespace_only() -> None:
    assert estimate_token_count("   ") == 0


def test_estimate_token_count_minimum_is_one() -> None:
    # Single char -> (1+3)//4 = 1
    assert estimate_token_count("a") == 1


def test_estimate_token_count_longer_text() -> None:
    text = "A" * 400
    count = estimate_token_count(text)
    # (400+3)//4 = 100
    assert count == 100


# --- aggregate_step_metrics ---


def test_aggregate_step_metrics_basic() -> None:
    summaries = [
        {
            "timings_ms": {"parse": 10.0, "decode": 20.0},
            "stats": {"source_bytes": 1000},
        },
        {
            "timings_ms": {"parse": 30.0, "decode": 40.0},
            "stats": {"source_bytes": 2000},
        },
    ]
    result = aggregate_step_metrics(summaries)
    assert "parse" in result
    assert "decode" in result
    assert result["parse"]["count"] == 2
    assert result["parse"]["total_ms"] == 40.0
    assert result["parse"]["avg_ms"] == 20.0
    assert result["decode"]["count"] == 2
    assert result["decode"]["total_ms"] == 60.0


def test_aggregate_step_metrics_empty_input() -> None:
    assert aggregate_step_metrics([]) == {}


def test_aggregate_step_metrics_single_entry() -> None:
    summaries = [
        {
            "timings_ms": {"parse": 10.0},
            "stats": {"source_bytes": 500},
        },
    ]
    result = aggregate_step_metrics(summaries)
    assert result["parse"]["count"] == 1
    assert result["parse"]["p50_ms"] == 10.0
    assert result["parse"]["p95_ms"] == 10.0


def test_aggregate_step_metrics_percentiles() -> None:
    summaries = [
        {"timings_ms": {"step": float(i)}, "stats": {"source_bytes": 100}}
        for i in range(1, 101)
    ]
    result = aggregate_step_metrics(summaries)
    assert result["step"]["count"] == 100
    assert result["step"]["p50_ms"] is not None
    assert result["step"]["p95_ms"] is not None
    # p50 should be around 50, p95 around 95
    assert 49.0 <= result["step"]["p50_ms"] <= 51.0
    assert 94.0 <= result["step"]["p95_ms"] <= 96.0


def test_aggregate_step_metrics_emails_per_second() -> None:
    summaries = [
        {
            "timings_ms": {"parse": 500.0},
            "stats": {"source_bytes": 1000},
        },
        {
            "timings_ms": {"parse": 500.0},
            "stats": {"source_bytes": 1000},
        },
    ]
    result = aggregate_step_metrics(summaries)
    # total_ms = 1000, count = 2 -> 2/1.0 = 2.0 emails/sec
    assert result["parse"]["emails_per_second"] == 2.0
