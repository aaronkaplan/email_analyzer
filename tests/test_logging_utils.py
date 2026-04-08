from __future__ import annotations

import json
import logging
from pathlib import Path

from email_analyzer.logging_utils import (
    ConsoleFormatter,
    JsonFormatter,
    configure_worker_logging,
    get_logger,
    log_event,
    start_logging,
    stop_logging,
)


# --- JsonFormatter ---


def test_json_formatter_produces_valid_json() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=None,
        exc_info=None,
    )
    record.event = {"step": "parse", "email_id": "test.eml"}  # type: ignore[attr-defined]
    output = formatter.format(record)
    parsed = json.loads(output)
    assert parsed["message"] == "Test message"
    assert parsed["level"] == "info"
    assert parsed["step"] == "parse"
    assert parsed["email_id"] == "test.eml"
    assert "timestamp" in parsed


def test_json_formatter_handles_missing_event() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.WARNING,
        pathname="",
        lineno=0,
        msg="Warning msg",
        args=None,
        exc_info=None,
    )
    output = formatter.format(record)
    parsed = json.loads(output)
    assert parsed["message"] == "Warning msg"
    assert parsed["level"] == "warning"


# --- ConsoleFormatter ---


def test_console_formatter_includes_source_and_step() -> None:
    formatter = ConsoleFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Parsed email",
        args=None,
        exc_info=None,
    )
    record.event = {  # type: ignore[attr-defined]
        "source_filename": "msg.eml",
        "step": "parse",
        "status": "success",
        "duration_ms": 12.345,
    }
    output = formatter.format(record)
    assert "msg.eml" in output
    assert "parse" in output
    assert "success" in output
    assert "12.345ms" in output
    assert "Parsed email" in output


def test_console_formatter_defaults_when_no_event() -> None:
    formatter = ConsoleFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Some message",
        args=None,
        exc_info=None,
    )
    output = formatter.format(record)
    assert "- log info" in output
    assert "Some message" in output


# --- log_event ---


def test_log_event_attaches_event_to_record() -> None:
    captured: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    logger = logging.getLogger("test_log_event")
    logger.handlers.clear()
    logger.addHandler(Capture())
    logger.setLevel(logging.DEBUG)

    log_event(logger, "Test event", step="parse", email_id="test.eml")
    assert len(captured) == 1
    event = getattr(captured[0], "event", {})
    assert event["step"] == "parse"
    assert event["email_id"] == "test.eml"


def test_log_event_custom_level() -> None:
    captured: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    logger = logging.getLogger("test_log_event_level")
    logger.handlers.clear()
    logger.addHandler(Capture())
    logger.setLevel(logging.DEBUG)

    log_event(logger, "Warning event", level=logging.WARNING, step="warn_step")
    assert len(captured) == 1
    assert captured[0].levelno == logging.WARNING


# --- get_logger ---


def test_get_logger_returns_email_analyzer_logger() -> None:
    logger = get_logger()
    assert logger.name == "email_analyzer"


# --- start_logging / stop_logging ---


def test_start_stop_logging_lifecycle(tmp_path: Path) -> None:
    logs_dir = tmp_path / "logs"
    runtime = start_logging(logs_dir)
    try:
        assert logs_dir.exists()
        assert (logs_dir / "pipeline.jsonl").exists()
        logger = get_logger()
        log_event(logger, "Test lifecycle", step="test", status="ok")
    finally:
        stop_logging(runtime)

    # Verify the log was written
    lines = (
        (logs_dir / "pipeline.jsonl").read_text(encoding="utf-8").strip().splitlines()
    )
    assert len(lines) >= 1
    parsed = json.loads(lines[0])
    assert parsed["message"] == "Test lifecycle"


# --- configure_worker_logging ---


def test_configure_worker_logging_sets_queue_handler() -> None:
    import multiprocessing

    manager = multiprocessing.Manager()
    try:
        queue = manager.Queue(-1)
        configure_worker_logging(queue)
        logger = logging.getLogger("email_analyzer")
        assert any(
            isinstance(h, logging.handlers.QueueHandler) for h in logger.handlers
        )
    finally:
        manager.shutdown()
