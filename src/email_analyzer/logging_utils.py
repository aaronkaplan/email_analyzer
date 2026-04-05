from __future__ import annotations

import json
import logging
import multiprocessing
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class LoggingRuntime:
    manager: Any
    queue: Any
    listener: QueueListener
    handlers: tuple[logging.Handler, ...]


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = dict(getattr(record, "event", {}))
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(event)
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


class ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", {})
        source = event.get("source_filename") or event.get("email_id") or "-"
        step = event.get("step") or "log"
        status = event.get("status") or record.levelname.lower()
        duration = event.get("duration_ms")
        duration_fragment = ""
        if isinstance(duration, int | float):
            duration_fragment = f" {duration:.3f}ms"
        return f"{source} {step} {status}{duration_fragment}: {record.getMessage()}"


def start_logging(logs_dir: Path) -> LoggingRuntime:
    logs_dir.mkdir(parents=True, exist_ok=True)

    manager = multiprocessing.Manager()
    queue = manager.Queue(-1)

    pipeline_handler = logging.FileHandler(
        logs_dir / "pipeline.jsonl", mode="w", encoding="utf-8"
    )
    pipeline_handler.setFormatter(JsonFormatter())

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ConsoleFormatter())

    listener = QueueListener(queue, pipeline_handler, console_handler)
    listener.start()
    _configure_main_logging(queue)

    return LoggingRuntime(
        manager=manager,
        queue=queue,
        listener=listener,
        handlers=(pipeline_handler, console_handler),
    )


def stop_logging(runtime: LoggingRuntime) -> None:
    runtime.listener.stop()
    for handler in runtime.handlers:
        handler.close()
    runtime.manager.shutdown()


def _configure_main_logging(queue: Any) -> None:
    _configure_queue_logger(queue)


def configure_worker_logging(queue: Any) -> None:
    _configure_queue_logger(queue)


def _configure_queue_logger(queue: Any) -> None:
    logger = logging.getLogger("email_analyzer")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(QueueHandler(queue))
    logger.propagate = False


def get_logger() -> logging.Logger:
    return logging.getLogger("email_analyzer")


def log_event(
    logger: logging.Logger, message: str, level: int = logging.INFO, **event: Any
) -> None:
    logger.log(level, message, extra={"event": event})
