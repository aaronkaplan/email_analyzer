from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from .config import OPENAI_BATCH_ENDPOINT


@dataclass(slots=True)
class BatchValidationResult:
    total_requests: int
    model: str


@dataclass(slots=True)
class BatchDisplaySnapshot:
    batch_id: str
    status: str
    total_requests: int
    completed_requests: int
    failed_requests: int
    processed_requests: int
    remaining_requests: int
    percent_complete: float
    elapsed_seconds: float
    state_elapsed_seconds: float
    speed_emails_per_sec: float | None
    eta_seconds: float | None


class StatusReporter(Protocol):
    def start(self, snapshot: BatchDisplaySnapshot) -> None: ...

    def update(self, snapshot: BatchDisplaySnapshot) -> None: ...

    def stop(self, snapshot: BatchDisplaySnapshot) -> None: ...


class RichBatchStatusReporter:
    def __init__(self, console: Console | None = None) -> None:
        self.console: Console = console or Console()
        self._live = self.console.is_terminal
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None

    def start(self, snapshot: BatchDisplaySnapshot) -> None:
        if self._print_static_snapshot(snapshot):
            return

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.fields[status]}[/bold]"),
            TextColumn("{task.fields[batch_suffix]}"),
            BarColumn(bar_width=20),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TextColumn(
                "ok={task.fields[ok]} failed={task.fields[failed]} remaining={task.fields[remaining]}"
            ),
            TimeElapsedColumn(),
            TextColumn("state={task.fields[state_elapsed]}"),
            TextColumn("{task.fields[speed]}"),
            TextColumn("{task.fields[eta]}"),
            console=self.console,
            transient=True,
        )
        progress.start()
        self._progress = progress
        self._task_id = progress.add_task("batch", **_progress_fields(snapshot))

    def update(self, snapshot: BatchDisplaySnapshot) -> None:
        if self._print_static_snapshot(snapshot):
            return

        if self._progress is None or self._task_id is None:
            return

        self._progress.update(self._task_id, **_progress_fields(snapshot))

    def stop(self, snapshot: BatchDisplaySnapshot) -> None:
        if self._progress is not None:
            self._progress.stop()
        self.console.print(format_status_line(snapshot))

    def _print_static_snapshot(self, snapshot: BatchDisplaySnapshot) -> bool:
        if self._live:
            return False
        self.console.print(format_status_line(snapshot))
        return True


def resolve_prompt_override(
    prompt: str | None,
    prompt_from_file: Path | None,
) -> tuple[str | None, str | None, str | None]:
    if prompt is not None and prompt_from_file is not None:
        raise ValueError("Use either --prompt or --prompt-from-file, not both")

    if prompt is not None:
        return prompt, "inline", None

    if prompt_from_file is not None:
        prompt_text = prompt_from_file.read_text(encoding="utf-8").rstrip()
        return prompt_text, "file", str(prompt_from_file)

    return None, None, None


def load_and_validate_batch(
    batch_jsonl: Path,
    *,
    endpoint: str = OPENAI_BATCH_ENDPOINT,
) -> tuple[list[dict[str, Any]], BatchValidationResult]:
    if not batch_jsonl.exists():
        raise FileNotFoundError(f"Batch file does not exist: {batch_jsonl}")

    requests: list[dict[str, Any]] = []
    custom_ids: set[str] = set()
    model: str | None = None

    for line_number, raw_line in enumerate(read_jsonl_lines(batch_jsonl), start=1):
        if not raw_line.strip():
            raise ValueError(f"Batch file contains an empty line at {line_number}")

        try:
            request = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON on line {line_number}: {exc.msg}") from exc

        if not isinstance(request, dict):
            raise ValueError(f"Batch line {line_number} must be a JSON object")

        custom_id = request.get("custom_id")
        if not isinstance(custom_id, str) or not custom_id:
            raise ValueError(f"Batch line {line_number} is missing a valid custom_id")
        if custom_id in custom_ids:
            raise ValueError(f"Duplicate custom_id in batch file: {custom_id}")
        custom_ids.add(custom_id)

        if request.get("method") != "POST":
            raise ValueError(f"Batch line {line_number} must use method POST")
        if request.get("url") != endpoint:
            raise ValueError(f"Batch line {line_number} must target {endpoint}")

        body = request.get("body")
        if not isinstance(body, dict):
            raise ValueError(f"Batch line {line_number} is missing a valid body object")

        body_model = body.get("model")
        if not isinstance(body_model, str) or not body_model:
            raise ValueError(f"Batch line {line_number} is missing a valid body.model")
        if model is None:
            model = body_model
        elif body_model != model:
            raise ValueError("Batch file must contain requests for only one model")

        requests.append(request)

    if not requests:
        raise ValueError("Batch file is empty")

    return requests, BatchValidationResult(
        total_requests=len(requests), model=model or ""
    )


def build_display_snapshot(
    batch: dict[str, Any],
    local_total_requests: int,
    started_monotonic: float,
    state_started_monotonic: float,
    now_monotonic: float,
) -> BatchDisplaySnapshot:
    counts = normalize_request_counts(batch.get("request_counts"))
    processed = counts["completed"] + counts["failed"]
    total_requests = max(local_total_requests, counts["total"], processed)
    remaining = max(0, total_requests - processed)
    percent_complete = (
        0.0 if total_requests == 0 else (processed / total_requests) * 100.0
    )
    elapsed = max(0.0, now_monotonic - started_monotonic)
    speed = processed / elapsed if elapsed > 0 and processed > 0 else None
    eta = remaining / speed if speed and speed > 0 and remaining > 0 else None
    return BatchDisplaySnapshot(
        batch_id=str(batch.get("id", "unknown")),
        status=str(batch.get("status", "unknown")),
        total_requests=total_requests,
        completed_requests=counts["completed"],
        failed_requests=counts["failed"],
        processed_requests=processed,
        remaining_requests=remaining,
        percent_complete=percent_complete,
        elapsed_seconds=elapsed,
        state_elapsed_seconds=max(0.0, now_monotonic - state_started_monotonic),
        speed_emails_per_sec=speed,
        eta_seconds=eta,
    )


def append_status_history(
    history_path: Path, batch: dict[str, Any], snapshot: BatchDisplaySnapshot
) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_record = {
        "observed_at": utcnow_iso(),
        "batch_id": snapshot.batch_id,
        "status": snapshot.status,
        "request_counts": normalize_request_counts(batch.get("request_counts")),
        "output_file_id": batch.get("output_file_id"),
        "error_file_id": batch.get("error_file_id"),
        "elapsed_seconds": round(snapshot.elapsed_seconds, 3),
        "state_elapsed_seconds": round(snapshot.state_elapsed_seconds, 3),
    }
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(history_record, ensure_ascii=False, sort_keys=True) + "\n"
        )


def build_summary(
    *,
    source_batch_jsonl: Path | None,
    output_dir: Path,
    submission_record: dict[str, Any],
    batch: dict[str, Any],
    poll_count: int,
    output_line_count: int,
    error_line_count: int,
    waiting_mode: str,
    completion_window: str | None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_counts = normalize_request_counts(batch.get("request_counts"))
    stage_durations = compute_stage_durations(batch)
    summary = {
        "source_batch_jsonl": str(source_batch_jsonl)
        if source_batch_jsonl is not None
        else None,
        "output_dir": str(output_dir),
        "submitted_batch_jsonl": submission_record.get("submitted_batch_jsonl"),
        "prompt_override_applied": submission_record.get(
            "prompt_override_applied", False
        ),
        "prompt_source": submission_record.get("prompt_source"),
        "prompt_source_path": submission_record.get("prompt_source_path"),
        "prompt_sha256": submission_record.get("prompt_sha256"),
        "batch_id": batch.get("id"),
        "status": batch.get("status"),
        "input_file_id": batch.get("input_file_id")
        or submission_record.get("input_file_id"),
        "completion_window": batch.get("completion_window") or completion_window,
        "output_file_id": batch.get("output_file_id"),
        "error_file_id": batch.get("error_file_id"),
        "request_counts": request_counts,
        "successful_processed_mails": request_counts["completed"],
        "failed_mails": request_counts["failed"],
        "output_line_count": output_line_count,
        "error_line_count": error_line_count,
        "poll_count": poll_count,
        "waiting_mode": waiting_mode,
        "elapsed_seconds": stage_durations.pop("total_elapsed", None),
        "stage_durations_seconds": stage_durations,
        "batch_errors": batch.get("errors"),
        "finished_at": utcnow_iso(),
    }
    if extra_fields:
        summary.update(extra_fields)
    return summary


def compute_stage_durations(batch: dict[str, Any]) -> dict[str, float]:
    timestamps = {
        key: as_timestamp(batch.get(key))
        for key in (
            "created_at",
            "in_progress_at",
            "finalizing_at",
            "completed_at",
            "failed_at",
            "expired_at",
            "cancelling_at",
            "cancelled_at",
        )
    }
    durations: dict[str, float] = {}

    validating_end = first_timestamp(
        timestamps,
        "in_progress_at",
        "failed_at",
        "cancelled_at",
        "expired_at",
        "completed_at",
    )
    if timestamps["created_at"] is not None and validating_end is not None:
        durations["validating"] = round(validating_end - timestamps["created_at"], 3)

    in_progress_end = first_timestamp(
        timestamps,
        "finalizing_at",
        "expired_at",
        "cancelling_at",
        "completed_at",
        "failed_at",
        "cancelled_at",
    )
    if timestamps["in_progress_at"] is not None and in_progress_end is not None:
        durations["in_progress"] = round(
            in_progress_end - timestamps["in_progress_at"], 3
        )

    finalizing_end = first_timestamp(
        timestamps,
        "completed_at",
        "failed_at",
        "expired_at",
        "cancelled_at",
    )
    if timestamps["finalizing_at"] is not None and finalizing_end is not None:
        durations["finalizing"] = round(finalizing_end - timestamps["finalizing_at"], 3)

    if (
        timestamps["cancelling_at"] is not None
        and timestamps["cancelled_at"] is not None
    ):
        durations["cancelling"] = round(
            timestamps["cancelled_at"] - timestamps["cancelling_at"], 3
        )

    total_end = first_timestamp(
        timestamps,
        "completed_at",
        "failed_at",
        "expired_at",
        "cancelled_at",
    )
    if timestamps["created_at"] is not None and total_end is not None:
        durations["total_elapsed"] = round(total_end - timestamps["created_at"], 3)

    return {key: value for key, value in durations.items() if value >= 0}


def read_jsonl_lines(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [line.rstrip("\r") for line in lines]


def count_jsonl_lines(path: Path) -> int:
    return sum(1 for line in read_jsonl_lines(path) if line.strip())


def normalize_request_counts(counts: Any) -> dict[str, int]:
    if hasattr(counts, "model_dump"):
        counts = counts.model_dump()
    if not isinstance(counts, dict):
        return {"total": 0, "completed": 0, "failed": 0}
    return {
        "total": int(counts.get("total") or 0),
        "completed": int(counts.get("completed") or 0),
        "failed": int(counts.get("failed") or 0),
    }


def as_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def first_timestamp(timestamps: dict[str, float | None], *keys: str) -> float | None:
    for key in keys:
        value = timestamps.get(key)
        if value is not None:
            return value
    return None


def print_summary(
    console: Console, summary: dict[str, Any], batch: dict[str, Any]
) -> None:
    table = Table(title="Batch Summary")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Status", str(summary.get("status")))
    table.add_row("Batch ID", str(summary.get("batch_id")))
    table.add_row("Input File ID", str(summary.get("input_file_id")))
    table.add_row("Output File ID", str(summary.get("output_file_id")))
    table.add_row("Error File ID", str(summary.get("error_file_id")))
    table.add_row("Successful Mails", str(summary.get("successful_processed_mails")))
    table.add_row("Failed Mails", str(summary.get("failed_mails")))
    table.add_row("Waiting Mode", str(summary.get("waiting_mode")))
    table.add_row("Elapsed Seconds", str(summary.get("elapsed_seconds")))
    table.add_row("Poll Count", str(summary.get("poll_count")))
    console.print(table)

    stage_durations = summary.get("stage_durations_seconds", {})
    if isinstance(stage_durations, dict) and stage_durations:
        duration_table = Table(title="State Durations")
        duration_table.add_column("State")
        duration_table.add_column("Seconds")
        for state, seconds in stage_durations.items():
            duration_table.add_row(state, str(seconds))
        console.print(duration_table)

    batch_errors = batch.get("errors")
    if isinstance(batch_errors, dict) and batch_errors.get("data"):
        console.print("[yellow]Batch reported validation errors:[/yellow]")
        for error in batch_errors["data"]:
            console.print(json.dumps(error, ensure_ascii=False, sort_keys=True))


def exit_code_for_summary(summary: dict[str, Any]) -> int:
    status = summary.get("status")
    failed = int(summary.get("failed_mails") or 0)
    if status == "completed" and failed == 0:
        return 0
    return 1


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, content: dict[str, Any]) -> None:
    write_text_atomic(
        path, json.dumps(content, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)


def format_duration(seconds: float) -> str:
    whole_seconds = max(0, int(seconds))
    hours, remainder = divmod(whole_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_speed(speed: float | None) -> str:
    if speed is None or speed <= 0:
        return "?it/s"
    if speed >= 1.0:
        return f"{speed:.2f}it/s"
    return f"{1.0 / speed:.2f}s/it"


def format_eta(eta_seconds: float | None) -> str:
    if eta_seconds is None:
        return "ETA --:--:--"
    return f"ETA {format_duration(eta_seconds)}"


def format_status_line(snapshot: BatchDisplaySnapshot) -> str:
    return (
        f"[{format_duration(snapshot.elapsed_seconds)}] {snapshot.status:<11} {snapshot.batch_id[-8:]} "
        f"processed {snapshot.processed_requests}/{snapshot.total_requests} "
        f"{snapshot.percent_complete:5.1f}% ok {snapshot.completed_requests} "
        f"failed {snapshot.failed_requests} remaining {snapshot.remaining_requests} "
        f"state_elapsed {format_duration(snapshot.state_elapsed_seconds)} "
        f"{format_speed(snapshot.speed_emails_per_sec)} "
        f"{format_eta(snapshot.eta_seconds)}"
    )


def _progress_fields(snapshot: BatchDisplaySnapshot) -> dict[str, int | str]:
    return {
        "total": max(1, snapshot.total_requests),
        "completed": snapshot.processed_requests,
        "status": snapshot.status,
        "batch_suffix": snapshot.batch_id[-8:],
        "ok": snapshot.completed_requests,
        "failed": snapshot.failed_requests,
        "remaining": snapshot.remaining_requests,
        "state_elapsed": format_duration(snapshot.state_elapsed_seconds),
        "speed": format_speed(snapshot.speed_emails_per_sec),
        "eta": format_eta(snapshot.eta_seconds),
    }
