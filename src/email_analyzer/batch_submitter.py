from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

from openai import OpenAI
from rich.console import Console

from .batch_submitter_common import (
    BatchDisplaySnapshot,
    RichBatchStatusReporter,
    StatusReporter,
    append_status_history,
    build_display_snapshot,
    build_summary,
    compute_stage_durations,
    exit_code_for_summary,
    format_duration,
    load_and_validate_batch,
    normalize_request_counts,
    print_summary,
    resolve_prompt_override,
    utcnow_iso,
    write_json_atomic,
    write_text_atomic,
)
from .config import BatchSubmitConfig, OPENAI_BATCH_ENDPOINT

TERMINAL_BATCH_STATES = {"completed", "failed", "expired", "cancelled"}

_compute_stage_durations = compute_stage_durations
_load_and_validate_batch = load_and_validate_batch


def _format_status_line(snapshot: BatchDisplaySnapshot) -> str:
    return (
        f"[{format_duration(snapshot.elapsed_seconds)}] {snapshot.status:<11} {snapshot.batch_id[-8:]} "
        f"processed {snapshot.processed_requests}/{snapshot.total_requests} "
        f"{snapshot.percent_complete:5.1f}% ok {snapshot.completed_requests} "
        f"failed {snapshot.failed_requests} remaining {snapshot.remaining_requests} "
        f"state_elapsed {format_duration(snapshot.state_elapsed_seconds)}"
    )


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
        "source_batch_jsonl": str(config.batch_jsonl)
        if config.batch_jsonl is not None
        else None,
        "resume_batch_id": config.resume_batch_id,
        "output_dir": str(output_dir),
        "endpoint": OPENAI_BATCH_ENDPOINT,
        "started_at": utcnow_iso(),
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
            write_json_atomic(output_dir / "batch_final.json", observed_batch)
            summary = build_summary(
                source_batch_jsonl=config.batch_jsonl,
                output_dir=output_dir,
                submission_record=submission_record,
                batch=observed_batch,
                poll_count=1,
                output_line_count=0,
                error_line_count=0,
                waiting_mode="submitted"
                if config.resume_batch_id is None
                else "resumed",
                completion_window=config.completion_window,
            )
            write_json_atomic(summary_path, summary)
            print_summary(console_obj, summary, observed_batch)
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
        write_json_atomic(output_dir / "batch_final.json", final_batch)

        output_line_count = _download_batch_file(
            openai_client,
            final_batch.get("output_file_id"),
            output_dir / "batch_output.jsonl",
        )
        error_line_count = _download_batch_file(
            openai_client,
            final_batch.get("error_file_id"),
            output_dir / "batch_errors.jsonl",
        )

        summary = build_summary(
            source_batch_jsonl=config.batch_jsonl,
            output_dir=output_dir,
            submission_record=submission_record,
            batch=final_batch,
            poll_count=poll_count,
            output_line_count=output_line_count,
            error_line_count=error_line_count,
            waiting_mode="completed",
            completion_window=config.completion_window,
        )
        write_json_atomic(summary_path, summary)
        print_summary(console_obj, summary, final_batch)
        return exit_code_for_summary(summary)
    except Exception as exc:
        error_summary = {
            "source_batch_jsonl": str(config.batch_jsonl),
            "output_dir": str(output_dir),
            "status": "submitter_error",
            "error": str(exc),
            "finished_at": utcnow_iso(),
        }
        write_json_atomic(summary_path, error_summary)
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
    if config.resume_batch_id is not None and (
        config.prompt is not None or config.prompt_from_file is not None
    ):
        raise ValueError("Prompt overrides cannot be used with --resume-batch-id")


def _resolve_output_dir(config: BatchSubmitConfig) -> Path:
    if config.output_dir is not None:
        return config.output_dir

    if config.resume_batch_id is not None:
        return Path("batch_output") / config.resume_batch_id

    assert config.batch_jsonl is not None
    if config.batch_jsonl.parent.name == "batches":
        return (
            config.batch_jsonl.parent.parent / "batch_output" / config.batch_jsonl.stem
        )
    return config.batch_jsonl.parent / "batch_output" / config.batch_jsonl.stem


def _prepare_batch_submission(
    config: BatchSubmitConfig,
    client: Any,
    *,
    output_dir: Path,
    submission_record: dict[str, Any],
    submission_path: Path,
) -> tuple[dict[str, Any], int, dict[str, Any]]:
    if config.resume_batch_id is not None:
        batch = _api_model_to_dict(
            _retrieve_batch_initial_state(client, config.resume_batch_id)
        )
        submission_record["batch_id"] = batch.get("id")
        submission_record["resume_mode"] = True
        submission_record["resumed_at"] = utcnow_iso()
        write_json_atomic(submission_path, submission_record)
        request_counts = normalize_request_counts(batch.get("request_counts"))
        local_total_requests = max(
            request_counts["total"],
            request_counts["completed"] + request_counts["failed"],
        )
        return batch, local_total_requests, submission_record

    batch_jsonl = _require_batch_jsonl(config)
    prompt_text, prompt_source, prompt_source_path = resolve_prompt_override(
        config.prompt, config.prompt_from_file
    )
    requests, validation = load_and_validate_batch(batch_jsonl)
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
                "prompt_sha256": hashlib.sha256(
                    prompt_text.encode("utf-8")
                ).hexdigest(),
                "batch_input_before_submit": str(backup_path),
            }
        )
        _rewrite_batch_in_place(batch_jsonl, requests, prompt_text)
        requests, validation = load_and_validate_batch(batch_jsonl)
    else:
        submission_record["prompt_override_applied"] = False

    submitted_copy_path = output_dir / "batch_input.submitted.jsonl"
    shutil.copyfile(batch_jsonl, submitted_copy_path)
    submission_record["submitted_batch_jsonl"] = str(submitted_copy_path)
    write_json_atomic(submission_path, submission_record)

    with batch_jsonl.open("rb") as handle:
        uploaded_file = _api_model_to_dict(
            client.files.create(file=handle, purpose="batch")
        )

    submission_record["input_file_id"] = uploaded_file.get("id")
    submission_record["uploaded_at"] = utcnow_iso()
    write_json_atomic(submission_path, submission_record)

    batch = _api_model_to_dict(
        client.batches.create(
            input_file_id=uploaded_file["id"],
            endpoint=OPENAI_BATCH_ENDPOINT,
            completion_window=config.completion_window,
        )
    )
    submission_record["batch_id"] = batch.get("id")
    submission_record["batch_created_at"] = utcnow_iso()
    write_json_atomic(submission_path, submission_record)
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
    snapshot = build_display_snapshot(
        batch, local_total_requests, started, started, started
    )
    reporter.start(snapshot)
    append_status_history(history_path, batch, snapshot)
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


def _rewrite_batch_in_place(
    batch_jsonl: Path, requests: list[dict[str, Any]], prompt: str
) -> None:
    rewritten_lines: list[str] = []
    for request in requests:
        request_body = dict(request["body"])
        request_body["instructions"] = prompt
        rewritten_request = dict(request)
        rewritten_request["body"] = request_body
        rewritten_lines.append(
            json.dumps(rewritten_request, ensure_ascii=True, sort_keys=True)
        )

    write_text_atomic(batch_jsonl, "\n".join(rewritten_lines) + "\n")


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
    snapshot = build_display_snapshot(
        initial_batch,
        local_total_requests,
        start_monotonic,
        state_started_monotonic,
        start_monotonic,
    )
    reporter.start(snapshot)
    append_status_history(history_path, initial_batch, snapshot)
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
        snapshot = build_display_snapshot(
            batch,
            local_total_requests,
            start_monotonic,
            state_started_monotonic,
            now_monotonic,
        )
        append_status_history(history_path, batch, snapshot)
        reporter.update(snapshot)
        poll_count += 1
        if status in TERMINAL_BATCH_STATES:
            reporter.stop(snapshot)
            return batch, poll_count


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
