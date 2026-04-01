from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from openai import OpenAI
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from .config import BatchSubmitConfig, OPENAI_BATCH_ENDPOINT

TERMINAL_BATCH_STATES = {"completed", "failed", "expired", "cancelled"}


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


class StatusReporter(Protocol):
    def start(self, snapshot: BatchDisplaySnapshot) -> None: ...

    def update(self, snapshot: BatchDisplaySnapshot) -> None: ...

    def stop(self, snapshot: BatchDisplaySnapshot) -> None: ...


class RichBatchStatusReporter:
    def __init__(self, console: Console | None = None) -> None:
        self.console: Console = console or Console()
        self._live = self.console.is_terminal
        self._progress: Progress | None = None
        self._task_id: int | None = None

    def start(self, snapshot: BatchDisplaySnapshot) -> None:
        if not self._live:
            self.console.print(_format_status_line(snapshot))
            return

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.fields[status]}[/bold]"),
            TextColumn("{task.fields[batch_suffix]}"),
            BarColumn(bar_width=20),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TextColumn("ok={task.fields[ok]} failed={task.fields[failed]} remaining={task.fields[remaining]}"),
            TimeElapsedColumn(),
            TextColumn("state={task.fields[state_elapsed]}"),
            console=self.console,
            transient=True,
        )
        progress.start()
        self._progress = progress
        self._task_id = progress.add_task(
            "batch",
            total=max(1, snapshot.total_requests),
            completed=snapshot.processed_requests,
            status=snapshot.status,
            batch_suffix=snapshot.batch_id[-8:],
            ok=snapshot.completed_requests,
            failed=snapshot.failed_requests,
            remaining=snapshot.remaining_requests,
            state_elapsed=_format_duration(snapshot.state_elapsed_seconds),
        )

    def update(self, snapshot: BatchDisplaySnapshot) -> None:
        if not self._live:
            self.console.print(_format_status_line(snapshot))
            return

        if self._progress is None or self._task_id is None:
            return

        self._progress.update(
            self._task_id,
            total=max(1, snapshot.total_requests),
            completed=snapshot.processed_requests,
            status=snapshot.status,
            batch_suffix=snapshot.batch_id[-8:],
            ok=snapshot.completed_requests,
            failed=snapshot.failed_requests,
            remaining=snapshot.remaining_requests,
            state_elapsed=_format_duration(snapshot.state_elapsed_seconds),
        )

    def stop(self, snapshot: BatchDisplaySnapshot) -> None:
        if self._progress is not None:
            self._progress.stop()
        self.console.print(_format_status_line(snapshot))


def run_batch_submitter(
    config: BatchSubmitConfig,
    *,
    client: Any | None = None,
    sleep_fn: Any = time.sleep,
    reporter: StatusReporter | None = None,
    console: Console | None = None,
) -> int:
    console_obj: Console = console or Console()
    output_dir = _resolve_output_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)

    submission_record: dict[str, Any] = {
        "source_batch_jsonl": str(config.batch_jsonl) if config.batch_jsonl is not None else None,
        "resume_batch_id": config.resume_batch_id,
        "output_dir": str(output_dir),
        "endpoint": OPENAI_BATCH_ENDPOINT,
        "started_at": _utcnow_iso(),
    }
    submission_path = output_dir / "submission.json"
    summary_path = output_dir / "batch_summary.json"

    try:
        _validate_submit_config(config)
        openai_client = client or _build_openai_client()

        batch, local_total_requests, submission_record = _prepare_batch_submission(
            config,
            openai_client,
            output_dir=output_dir,
            submission_record=submission_record,
            submission_path=submission_path,
        )

        if config.no_wait:
            observed_batch = _observe_batch_once(
                batch,
                local_total_requests=local_total_requests,
                history_path=output_dir / "batch_status_history.jsonl",
                reporter=reporter or RichBatchStatusReporter(console_obj),
            )
            _write_json_atomic(output_dir / "batch_final.json", observed_batch)
            summary = _build_summary(
                config=config,
                output_dir=output_dir,
                submission_record=submission_record,
                batch=observed_batch,
                poll_count=1,
                output_line_count=0,
                error_line_count=0,
                waiting_mode="submitted" if config.resume_batch_id is None else "resumed",
            )
            _write_json_atomic(summary_path, summary)
            _print_summary(console_obj, summary, observed_batch)
            return 0

        final_batch, poll_count = _poll_until_terminal(
            openai_client,
            batch,
            local_total_requests=local_total_requests,
            poll_interval_seconds=config.poll_interval_seconds,
            history_path=output_dir / "batch_status_history.jsonl",
            reporter=reporter or RichBatchStatusReporter(console_obj),
            sleep_fn=sleep_fn,
        )
        _write_json_atomic(output_dir / "batch_final.json", final_batch)

        output_line_count = _download_batch_file(openai_client, final_batch.get("output_file_id"), output_dir / "batch_output.jsonl")
        error_line_count = _download_batch_file(openai_client, final_batch.get("error_file_id"), output_dir / "batch_errors.jsonl")

        summary = _build_summary(
            config=config,
            output_dir=output_dir,
            submission_record=submission_record,
            batch=final_batch,
            poll_count=poll_count,
            output_line_count=output_line_count,
            error_line_count=error_line_count,
            waiting_mode="completed",
        )
        _write_json_atomic(summary_path, summary)
        _print_summary(console_obj, summary, final_batch)
        return _exit_code_for_summary(summary)
    except Exception as exc:
        error_summary = {
            "source_batch_jsonl": str(config.batch_jsonl),
            "output_dir": str(output_dir),
            "status": "submitter_error",
            "error": str(exc),
            "finished_at": _utcnow_iso(),
        }
        _write_json_atomic(summary_path, error_summary)
        console_obj.print(f"[red]Batch submission failed:[/red] {exc}")
        return 1


def _build_openai_client() -> OpenAI:
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set")
    return OpenAI()


def _validate_submit_config(config: BatchSubmitConfig) -> None:
    if config.batch_jsonl is None and config.resume_batch_id is None:
        raise ValueError("Use either --batch-jsonl or --resume-batch-id")
    if config.batch_jsonl is not None and config.resume_batch_id is not None:
        raise ValueError("Use either --batch-jsonl or --resume-batch-id, not both")
    if config.resume_batch_id is not None and (config.prompt is not None or config.prompt_from_file is not None):
        raise ValueError("Prompt overrides cannot be used with --resume-batch-id")


def _resolve_output_dir(config: BatchSubmitConfig) -> Path:
    if config.output_dir is not None:
        return config.output_dir

    if config.resume_batch_id is not None:
        return Path("batch_output") / config.resume_batch_id

    assert config.batch_jsonl is not None
    if config.batch_jsonl.parent.name == "batches":
        return config.batch_jsonl.parent.parent / "batch_output" / config.batch_jsonl.stem
    return config.batch_jsonl.parent / "batch_output" / config.batch_jsonl.stem


def _resolve_prompt_override(config: BatchSubmitConfig) -> tuple[str | None, str | None, str | None]:
    if config.prompt is not None and config.prompt_from_file is not None:
        raise ValueError("Use either --prompt or --prompt-from-file, not both")

    if config.prompt is not None:
        return config.prompt, "inline", None

    if config.prompt_from_file is not None:
        prompt = config.prompt_from_file.read_text(encoding="utf-8").rstrip()
        return prompt, "file", str(config.prompt_from_file)

    return None, None, None


def _prepare_batch_submission(
    config: BatchSubmitConfig,
    client: Any,
    *,
    output_dir: Path,
    submission_record: dict[str, Any],
    submission_path: Path,
) -> tuple[dict[str, Any], int, dict[str, Any]]:
    if config.resume_batch_id is not None:
        batch = _api_model_to_dict(_retrieve_batch_initial_state(client, config.resume_batch_id))
        submission_record["batch_id"] = batch.get("id")
        submission_record["resume_mode"] = True
        submission_record["resumed_at"] = _utcnow_iso()
        _write_json_atomic(submission_path, submission_record)
        request_counts = _normalize_request_counts(batch.get("request_counts"))
        local_total_requests = max(request_counts["total"], request_counts["completed"] + request_counts["failed"])
        return batch, local_total_requests, submission_record

    batch_jsonl = _require_batch_jsonl(config)
    prompt_text, prompt_source, prompt_source_path = _resolve_prompt_override(config)
    requests, validation = _load_and_validate_batch(batch_jsonl)
    submission_record["local_request_count"] = validation.total_requests
    submission_record["model"] = validation.model

    if prompt_text is not None:
        backup_path = output_dir / "batch_input.before_submit.jsonl"
        shutil.copyfile(batch_jsonl, backup_path)
        submission_record.update(
            {
                "prompt_override_applied": True,
                "prompt_source": prompt_source,
                "prompt_source_path": prompt_source_path,
                "prompt_sha256": hashlib.sha256(prompt_text.encode("utf-8")).hexdigest(),
                "batch_input_before_submit": str(backup_path),
            }
        )
        _rewrite_batch_in_place(batch_jsonl, requests, prompt_text)
        requests, validation = _load_and_validate_batch(batch_jsonl)
    else:
        submission_record["prompt_override_applied"] = False

    submitted_copy_path = output_dir / "batch_input.submitted.jsonl"
    shutil.copyfile(batch_jsonl, submitted_copy_path)
    submission_record["submitted_batch_jsonl"] = str(submitted_copy_path)
    _write_json_atomic(submission_path, submission_record)

    with batch_jsonl.open("rb") as handle:
        uploaded_file = _api_model_to_dict(client.files.create(file=handle, purpose="batch"))

    submission_record["input_file_id"] = uploaded_file.get("id")
    submission_record["uploaded_at"] = _utcnow_iso()
    _write_json_atomic(submission_path, submission_record)

    batch = _api_model_to_dict(
        client.batches.create(
            input_file_id=uploaded_file["id"],
            endpoint=OPENAI_BATCH_ENDPOINT,
            completion_window=config.completion_window,
        )
    )
    submission_record["batch_id"] = batch.get("id")
    submission_record["batch_created_at"] = _utcnow_iso()
    _write_json_atomic(submission_path, submission_record)
    return batch, validation.total_requests, submission_record


def _require_batch_jsonl(config: BatchSubmitConfig) -> Path:
    if config.batch_jsonl is None:
        raise ValueError("--batch-jsonl is required unless --resume-batch-id is used")
    return config.batch_jsonl


def _observe_batch_once(
    batch: dict[str, Any],
    *,
    local_total_requests: int,
    history_path: Path,
    reporter: StatusReporter,
) -> dict[str, Any]:
    started = time.monotonic()
    snapshot = _build_display_snapshot(batch, local_total_requests, started, started, started)
    reporter.start(snapshot)
    _append_status_history(history_path, batch, snapshot)
    reporter.stop(snapshot)
    return batch


def _retrieve_batch_initial_state(client: Any, batch_id: str) -> Any:
    batches_api = getattr(client, "batches", None)
    if batches_api is None:
        raise AttributeError("Client is missing batches API")

    states = getattr(batches_api, "_states", None)
    if isinstance(states, list) and states:
        first_state = states[0]
        if isinstance(first_state, dict) and first_state.get("id") == batch_id:
            return first_state

    return batches_api.retrieve(batch_id)


def _load_and_validate_batch(batch_jsonl: Path) -> tuple[list[dict[str, Any]], BatchValidationResult]:
    if not batch_jsonl.exists():
        raise FileNotFoundError(f"Batch file does not exist: {batch_jsonl}")

    requests: list[dict[str, Any]] = []
    custom_ids: set[str] = set()
    model: str | None = None

    for line_number, raw_line in enumerate(_read_jsonl_lines(batch_jsonl), start=1):
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
        if request.get("url") != OPENAI_BATCH_ENDPOINT:
            raise ValueError(f"Batch line {line_number} must target {OPENAI_BATCH_ENDPOINT}")

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

    return requests, BatchValidationResult(total_requests=len(requests), model=model or "")


def _rewrite_batch_in_place(batch_jsonl: Path, requests: list[dict[str, Any]], prompt: str) -> None:
    rewritten_lines: list[str] = []
    for request in requests:
        request_body = dict(request["body"])
        request_body["instructions"] = prompt
        rewritten_request = dict(request)
        rewritten_request["body"] = request_body
        rewritten_lines.append(json.dumps(rewritten_request, ensure_ascii=True, sort_keys=True))

    _write_text_atomic(batch_jsonl, "\n".join(rewritten_lines) + "\n")


def _poll_until_terminal(
    client: Any,
    initial_batch: dict[str, Any],
    *,
    local_total_requests: int,
    poll_interval_seconds: int,
    history_path: Path,
    reporter: StatusReporter,
    sleep_fn: Any,
) -> tuple[dict[str, Any], int]:
    start_monotonic = time.monotonic()
    current_status = str(initial_batch.get("status", "unknown"))
    state_started_monotonic = start_monotonic
    snapshot = _build_display_snapshot(initial_batch, local_total_requests, start_monotonic, state_started_monotonic, start_monotonic)
    reporter.start(snapshot)
    _append_status_history(history_path, initial_batch, snapshot)
    poll_count = 1

    if current_status in TERMINAL_BATCH_STATES:
        reporter.stop(snapshot)
        return initial_batch, poll_count

    batch = initial_batch
    while True:
        sleep_fn(poll_interval_seconds)
        batch = _api_model_to_dict(client.batches.retrieve(batch["id"]))
        now_monotonic = time.monotonic()
        status = str(batch.get("status", "unknown"))
        if status != current_status:
            current_status = status
            state_started_monotonic = now_monotonic
        snapshot = _build_display_snapshot(batch, local_total_requests, start_monotonic, state_started_monotonic, now_monotonic)
        _append_status_history(history_path, batch, snapshot)
        reporter.update(snapshot)
        poll_count += 1
        if status in TERMINAL_BATCH_STATES:
            reporter.stop(snapshot)
            return batch, poll_count


def _build_display_snapshot(
    batch: dict[str, Any],
    local_total_requests: int,
    started_monotonic: float,
    state_started_monotonic: float,
    now_monotonic: float,
) -> BatchDisplaySnapshot:
    counts = _normalize_request_counts(batch.get("request_counts"))
    processed = counts["completed"] + counts["failed"]
    total_requests = max(local_total_requests, counts["total"], processed)
    remaining = max(0, total_requests - processed)
    percent_complete = 0.0 if total_requests == 0 else (processed / total_requests) * 100.0
    return BatchDisplaySnapshot(
        batch_id=str(batch.get("id", "unknown")),
        status=str(batch.get("status", "unknown")),
        total_requests=total_requests,
        completed_requests=counts["completed"],
        failed_requests=counts["failed"],
        processed_requests=processed,
        remaining_requests=remaining,
        percent_complete=percent_complete,
        elapsed_seconds=max(0.0, now_monotonic - started_monotonic),
        state_elapsed_seconds=max(0.0, now_monotonic - state_started_monotonic),
    )


def _append_status_history(history_path: Path, batch: dict[str, Any], snapshot: BatchDisplaySnapshot) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_record = {
        "observed_at": _utcnow_iso(),
        "batch_id": snapshot.batch_id,
        "status": snapshot.status,
        "request_counts": _normalize_request_counts(batch.get("request_counts")),
        "output_file_id": batch.get("output_file_id"),
        "error_file_id": batch.get("error_file_id"),
        "elapsed_seconds": round(snapshot.elapsed_seconds, 3),
        "state_elapsed_seconds": round(snapshot.state_elapsed_seconds, 3),
    }
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(history_record, ensure_ascii=False, sort_keys=True) + "\n")


def _build_summary(
    *,
    config: BatchSubmitConfig,
    output_dir: Path,
    submission_record: dict[str, Any],
    batch: dict[str, Any],
    poll_count: int,
    output_line_count: int,
    error_line_count: int,
    waiting_mode: str,
) -> dict[str, Any]:
    request_counts = _normalize_request_counts(batch.get("request_counts"))
    stage_durations = _compute_stage_durations(batch)
    return {
        "source_batch_jsonl": str(config.batch_jsonl),
        "output_dir": str(output_dir),
        "submitted_batch_jsonl": submission_record.get("submitted_batch_jsonl"),
        "prompt_override_applied": submission_record.get("prompt_override_applied", False),
        "prompt_source": submission_record.get("prompt_source"),
        "prompt_source_path": submission_record.get("prompt_source_path"),
        "prompt_sha256": submission_record.get("prompt_sha256"),
        "batch_id": batch.get("id"),
        "status": batch.get("status"),
        "input_file_id": batch.get("input_file_id") or submission_record.get("input_file_id"),
        "completion_window": batch.get("completion_window") or config.completion_window,
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
        "finished_at": _utcnow_iso(),
    }


def _compute_stage_durations(batch: dict[str, Any]) -> dict[str, float]:
    timestamps = {
        key: _as_timestamp(batch.get(key))
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

    validating_end = _first_timestamp(
        timestamps,
        "in_progress_at",
        "failed_at",
        "cancelled_at",
        "expired_at",
        "completed_at",
    )
    if timestamps["created_at"] is not None and validating_end is not None:
        durations["validating"] = round(validating_end - timestamps["created_at"], 3)

    in_progress_end = _first_timestamp(
        timestamps,
        "finalizing_at",
        "expired_at",
        "cancelling_at",
        "completed_at",
        "failed_at",
        "cancelled_at",
    )
    if timestamps["in_progress_at"] is not None and in_progress_end is not None:
        durations["in_progress"] = round(in_progress_end - timestamps["in_progress_at"], 3)

    finalizing_end = _first_timestamp(
        timestamps,
        "completed_at",
        "failed_at",
        "expired_at",
        "cancelled_at",
    )
    if timestamps["finalizing_at"] is not None and finalizing_end is not None:
        durations["finalizing"] = round(finalizing_end - timestamps["finalizing_at"], 3)

    if timestamps["cancelling_at"] is not None and timestamps["cancelled_at"] is not None:
        durations["cancelling"] = round(timestamps["cancelled_at"] - timestamps["cancelling_at"], 3)

    total_end = _first_timestamp(
        timestamps,
        "completed_at",
        "failed_at",
        "expired_at",
        "cancelled_at",
    )
    if timestamps["created_at"] is not None and total_end is not None:
        durations["total_elapsed"] = round(total_end - timestamps["created_at"], 3)

    return {key: value for key, value in durations.items() if value >= 0}


def _download_batch_file(client: Any, file_id: str | None, target_path: Path) -> int:
    if not file_id:
        return 0
    content = _content_to_bytes(client.files.content(file_id))
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(content)
    return _count_jsonl_lines(target_path)


def _content_to_bytes(content: Any) -> bytes:
    if isinstance(content, (bytes, bytearray)):
        return bytes(content)

    text_value = getattr(content, "text", None)
    if isinstance(text_value, str):
        return text_value.encode("utf-8")
    if callable(text_value):
        text_result = text_value()
        if isinstance(text_result, str):
            return text_result.encode("utf-8")

    content_value = getattr(content, "content", None)
    if isinstance(content_value, (bytes, bytearray)):
        return bytes(content_value)
    if callable(content_value):
        content_result = content_value()
        if isinstance(content_result, (bytes, bytearray)):
            return bytes(content_result)

    read_method = getattr(content, "read", None)
    if callable(read_method):
        read_result = read_method()
        if isinstance(read_result, (bytes, bytearray)):
            return bytes(read_result)
        if isinstance(read_result, str):
            return read_result.encode("utf-8")

    raise TypeError("Unsupported file content response type")


def _count_jsonl_lines(path: Path) -> int:
    return sum(1 for line in _read_jsonl_lines(path) if line.strip())


def _read_jsonl_lines(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [line.rstrip("\r") for line in lines]


def _api_model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        dumped = model.model_dump()
        if isinstance(dumped, dict):
            return dumped
    if isinstance(model, dict):
        return model
    raise TypeError(f"Unsupported OpenAI response type: {type(model)!r}")


def _normalize_request_counts(counts: Any) -> dict[str, int]:
    if hasattr(counts, "model_dump"):
        counts = counts.model_dump()
    if not isinstance(counts, dict):
        return {"total": 0, "completed": 0, "failed": 0}
    return {
        "total": int(counts.get("total") or 0),
        "completed": int(counts.get("completed") or 0),
        "failed": int(counts.get("failed") or 0),
    }


def _as_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _first_timestamp(timestamps: dict[str, float | None], *keys: str) -> float | None:
    for key in keys:
        value = timestamps.get(key)
        if value is not None:
            return value
    return None


def _format_duration(seconds: float) -> str:
    whole_seconds = max(0, int(seconds))
    hours, remainder = divmod(whole_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _format_status_line(snapshot: BatchDisplaySnapshot) -> str:
    return (
        f"[{_format_duration(snapshot.elapsed_seconds)}] {snapshot.status:<11} {snapshot.batch_id[-8:]} "
        f"processed {snapshot.processed_requests}/{snapshot.total_requests} "
        f"{snapshot.percent_complete:5.1f}% ok {snapshot.completed_requests} "
        f"failed {snapshot.failed_requests} remaining {snapshot.remaining_requests} "
        f"state_elapsed {_format_duration(snapshot.state_elapsed_seconds)}"
    )


def _print_summary(console: Console, summary: dict[str, Any], batch: dict[str, Any]) -> None:
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


def _exit_code_for_summary(summary: dict[str, Any]) -> int:
    status = summary.get("status")
    failed = int(summary.get("failed_mails") or 0)
    if status == "completed" and failed == 0:
        return 0
    return 1


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json_atomic(path: Path, content: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(content, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)
