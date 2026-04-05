from __future__ import annotations

import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from rich.console import Console

from .batch_submitter_common import (
    RichBatchStatusReporter,
    StatusReporter,
    append_status_history,
    build_display_snapshot,
    build_summary,
    exit_code_for_summary,
    load_and_validate_batch,
    print_summary,
    resolve_prompt_override,
    utcnow_iso,
    write_json_atomic,
    write_text_atomic,
)
from .config import OLLAMA_CHAT_ENDPOINT, OPENAI_BATCH_ENDPOINT, OllamaBatchSubmitConfig


@dataclass(slots=True)
class OllamaPreparedRequest:
    index: int
    custom_id: str
    submitted_request: dict[str, Any]
    ollama_payload: dict[str, Any]
    requires_json_object: bool


@dataclass(slots=True)
class OllamaExecutionResult:
    index: int
    custom_id: str
    success_record: dict[str, Any] | None
    error_record: dict[str, Any] | None
    failed: bool
    error_type: str | None = None
    error_message: str | None = None


def run_ollama_batch_submitter(
    config: OllamaBatchSubmitConfig,
    *,
    request_executor: Callable[[str, dict[str, Any], int], dict[str, Any]]
    | None = None,
    reporter: StatusReporter | None = None,
    console: Console | None = None,
) -> int:
    console_obj: Console = console or Console()
    output_dir = _resolve_output_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)

    submission_path = output_dir / "submission.json"
    summary_path = output_dir / "batch_summary.json"
    submitted_copy_path = output_dir / "batch_input.submitted.jsonl"
    before_submit_path = output_dir / "batch_input.before_submit.jsonl"
    history_path = output_dir / "batch_status_history.jsonl"
    batch_final_path = output_dir / "batch_final.json"
    batch_output_path = output_dir / "batch_output.jsonl"
    batch_errors_path = output_dir / "batch_errors.jsonl"

    submission_record: dict[str, Any] = {
        "source_batch_jsonl": str(config.batch_jsonl),
        "output_dir": str(output_dir),
        "endpoint": OPENAI_BATCH_ENDPOINT,
        "started_at": utcnow_iso(),
        "provider_meta": {
            "provider": "ollama",
            "provider_endpoint": OLLAMA_CHAT_ENDPOINT,
        },
    }

    try:
        _validate_submit_config(config)
        base_urls = _resolve_base_urls(config)
        base_url = base_urls[0]
        source_requests, validation = load_and_validate_batch(config.batch_jsonl)
        provider_model = _resolve_model(config)
        prompt_text, prompt_source, prompt_source_path = resolve_prompt_override(
            config.prompt, config.prompt_from_file
        )

        if prompt_text is not None:
            before_submit_path.write_bytes(config.batch_jsonl.read_bytes())
            submission_record.update(
                {
                    "prompt_override_applied": True,
                    "prompt_source": prompt_source,
                    "prompt_source_path": prompt_source_path,
                    "prompt_sha256": hashlib.sha256(
                        prompt_text.encode("utf-8")
                    ).hexdigest(),
                    "batch_input_before_submit": str(before_submit_path),
                }
            )
        else:
            submission_record["prompt_override_applied"] = False

        prepared_requests = _prepare_requests(
            source_requests,
            provider_model=provider_model,
            prompt_override=prompt_text,
        )

        submitted_lines = _render_jsonl_lines(
            [item.submitted_request for item in prepared_requests]
        )
        write_text_atomic(submitted_copy_path, "\n".join(submitted_lines) + "\n")

        batch_ids = _make_local_ids(submitted_lines, base_urls, provider_model)
        provider_meta: dict[str, Any] = {
            "provider": "ollama",
            "provider_endpoint": OLLAMA_CHAT_ENDPOINT,
            "base_url": base_url,
            "source_model": validation.model,
        }
        if len(base_urls) > 1:
            provider_meta.update(
                {
                    "base_urls": list(base_urls),
                    "num_shards": len(base_urls),
                    "per_shard_num_parallel_jobs": config.num_parallel_jobs,
                    "total_possible_parallel_requests": len(base_urls)
                    * config.num_parallel_jobs,
                }
            )
        submission_record.update(
            {
                "submitted_batch_jsonl": str(submitted_copy_path),
                "local_request_count": validation.total_requests,
                "model": provider_model,
                "batch_id": batch_ids["batch_id"],
                "input_file_id": batch_ids["input_file_id"],
                "output_file_id": batch_ids["output_file_id"],
                "error_file_id": batch_ids["error_file_id"],
                "uploaded_at": utcnow_iso(),
                "batch_created_at": utcnow_iso(),
            }
        )
        submission_record["provider_meta"] = provider_meta
        write_json_atomic(submission_path, submission_record)

        batch = _build_initial_batch(
            batch_ids,
            base_urls=base_urls,
            model=provider_model,
            num_parallel_jobs=config.num_parallel_jobs,
        )
        reporter_obj = reporter or RichBatchStatusReporter(console_obj)
        executor_fn = request_executor or _execute_ollama_chat_request
        total_requests = len(prepared_requests)
        progress_event_count = 0

        start_monotonic = time.monotonic()
        state_started_monotonic = start_monotonic
        _, progress_event_count = _report_status(
            batch=batch,
            total_requests=total_requests,
            started_monotonic=start_monotonic,
            state_started_monotonic=state_started_monotonic,
            now_monotonic=start_monotonic,
            reporter=reporter_obj,
            history_path=history_path,
            mode="start",
            progress_event_count=progress_event_count,
        )

        batch["status"] = "in_progress"
        batch["in_progress_at"] = time.time()
        batch["request_counts"] = {
            "total": total_requests,
            "completed": 0,
            "failed": 0,
        }
        state_started_monotonic = time.monotonic()
        _, progress_event_count = _report_status(
            batch=batch,
            total_requests=total_requests,
            started_monotonic=start_monotonic,
            state_started_monotonic=state_started_monotonic,
            now_monotonic=state_started_monotonic,
            reporter=reporter_obj,
            history_path=history_path,
            mode="update",
            progress_event_count=progress_event_count,
        )

        results: list[OllamaExecutionResult | None] = [None] * total_requests
        batch_errors: list[dict[str, Any]] = []

        with _ShardedExecutorPool(
            base_urls=base_urls,
            num_parallel_jobs=config.num_parallel_jobs,
        ) as pools:
            future_map = {
                pools.submit(
                    _run_single_request,
                    prepared,
                    base_url=base_urls[prepared.index % len(base_urls)],
                    request_timeout_seconds=config.request_timeout_seconds,
                    request_executor=executor_fn,
                ): prepared.index
                for prepared in prepared_requests
            }

            for future in as_completed(future_map):
                result = future.result()
                results[result.index] = result
                request_counts = batch["request_counts"]
                if result.failed:
                    request_counts["failed"] = (
                        int(request_counts.get("failed") or 0) + 1
                    )
                    batch_errors.append(
                        {
                            "code": result.error_type or "ollama_request_error",
                            "custom_id": result.custom_id,
                            "message": result.error_message
                            or "Unknown Ollama execution error",
                        }
                    )
                else:
                    request_counts["completed"] = (
                        int(request_counts.get("completed") or 0) + 1
                    )

                now_monotonic = time.monotonic()
                _, progress_event_count = _report_status(
                    batch=batch,
                    total_requests=total_requests,
                    started_monotonic=start_monotonic,
                    state_started_monotonic=state_started_monotonic,
                    now_monotonic=now_monotonic,
                    reporter=reporter_obj,
                    history_path=history_path,
                    mode="update",
                    progress_event_count=progress_event_count,
                )

        batch["status"] = "finalizing"
        batch["finalizing_at"] = time.time()
        state_started_monotonic = time.monotonic()
        _, progress_event_count = _report_status(
            batch=batch,
            total_requests=total_requests,
            started_monotonic=start_monotonic,
            state_started_monotonic=state_started_monotonic,
            now_monotonic=state_started_monotonic,
            reporter=reporter_obj,
            history_path=history_path,
            mode="update",
            progress_event_count=progress_event_count,
        )

        success_lines = _render_jsonl_lines(
            [
                result.success_record
                for result in results
                if result is not None and result.success_record is not None
            ]
        )
        error_lines = _render_jsonl_lines(
            [
                result.error_record
                for result in results
                if result is not None and result.error_record is not None
            ]
        )
        write_text_atomic(
            batch_output_path,
            ("\n".join(success_lines) + "\n") if success_lines else "",
        )
        write_text_atomic(
            batch_errors_path,
            ("\n".join(error_lines) + "\n") if error_lines else "",
        )

        batch["status"] = "completed"
        batch["completed_at"] = time.time()
        batch["output_file_id"] = batch_ids["output_file_id"]
        batch["error_file_id"] = batch_ids["error_file_id"]
        batch["errors"] = {"data": batch_errors} if batch_errors else None
        write_json_atomic(batch_final_path, batch)

        completed_monotonic = time.monotonic()
        _, progress_event_count = _report_status(
            batch=batch,
            total_requests=total_requests,
            started_monotonic=start_monotonic,
            state_started_monotonic=state_started_monotonic,
            now_monotonic=completed_monotonic,
            reporter=reporter_obj,
            history_path=history_path,
            mode="stop",
            progress_event_count=progress_event_count,
        )

        summary = build_summary(
            source_batch_jsonl=config.batch_jsonl,
            output_dir=output_dir,
            submission_record=submission_record,
            batch=batch,
            poll_count=progress_event_count,
            output_line_count=len(success_lines),
            error_line_count=len(error_lines),
            waiting_mode="completed",
            completion_window="local",
            extra_fields={
                "provider_meta": {
                    "provider": "ollama",
                    "provider_endpoint": OLLAMA_CHAT_ENDPOINT,
                    "base_url": base_url,
                    "base_urls": list(base_urls) if len(base_urls) > 1 else None,
                    "num_shards": len(base_urls),
                    "per_shard_num_parallel_jobs": config.num_parallel_jobs,
                    "total_possible_parallel_requests": len(base_urls)
                    * config.num_parallel_jobs,
                    "provider_model": submission_record.get("model"),
                    "source_model": validation.model,
                }
            },
        )
        write_json_atomic(summary_path, summary)
        print_summary(console_obj, summary, batch)
        return exit_code_for_summary(summary)
    except Exception as exc:
        error_summary = {
            "provider": "ollama",
            "source_batch_jsonl": str(config.batch_jsonl),
            "output_dir": str(output_dir),
            "status": "submitter_error",
            "error": str(exc),
            "finished_at": utcnow_iso(),
        }
        write_json_atomic(summary_path, error_summary)
        console_obj.print(f"[red]Ollama batch submission failed:[/red] {exc}")
        return 1


def _validate_submit_config(config: OllamaBatchSubmitConfig) -> None:
    if config.num_parallel_jobs < 1:
        raise ValueError("--num-parallel-jobs must be at least 1")
    if config.request_timeout_seconds < 1:
        raise ValueError("--request-timeout-seconds must be at least 1")
    if (
        config.num_shards is not None
        and len(config.base_urls) > 0
        and config.num_shards != len(config.base_urls)
    ):
        raise ValueError("--num-shards must equal the number of --base-url values")


def _resolve_output_dir(config: OllamaBatchSubmitConfig) -> Path:
    if config.output_dir is not None:
        return config.output_dir
    if config.batch_jsonl.parent.name == "batches":
        return (
            config.batch_jsonl.parent.parent
            / "ollama_batch_output"
            / config.batch_jsonl.stem
        )
    return config.batch_jsonl.parent / "ollama_batch_output" / config.batch_jsonl.stem


def _resolve_base_urls(config: OllamaBatchSubmitConfig) -> tuple[str, ...]:
    if config.base_urls:
        values = tuple(base_url.rstrip("/") for base_url in config.base_urls)
    else:
        env_value = os.environ.get("OLLAMA_BASE_URL")
        if not env_value:
            raise RuntimeError(
                "OLLAMA_BASE_URL is not set and --base-url was not provided"
            )
        values = (env_value.rstrip("/"),)

    if config.num_shards is not None and config.num_shards != len(values):
        raise ValueError("--num-shards must equal the number of --base-url values")
    return values


def _resolve_model(config: OllamaBatchSubmitConfig) -> str:
    value = config.model or os.environ.get("OLLAMA_MODEL")
    if not value:
        raise RuntimeError("OLLAMA_MODEL is not set and --model was not provided")
    return value


def _prepare_requests(
    source_requests: list[dict[str, Any]],
    *,
    provider_model: str,
    prompt_override: str | None,
) -> list[OllamaPreparedRequest]:
    prepared_requests: list[OllamaPreparedRequest] = []
    for index, source_request in enumerate(source_requests):
        submitted_request = _apply_overrides(
            source_request,
            provider_model=provider_model,
            prompt_override=prompt_override,
        )
        ollama_payload, requires_json_object = _translate_request(submitted_request)
        prepared_requests.append(
            OllamaPreparedRequest(
                index=index,
                custom_id=str(submitted_request["custom_id"]),
                submitted_request=submitted_request,
                ollama_payload=ollama_payload,
                requires_json_object=requires_json_object,
            )
        )
    return prepared_requests


def _apply_overrides(
    source_request: dict[str, Any],
    *,
    provider_model: str,
    prompt_override: str | None,
) -> dict[str, Any]:
    request_copy = dict(source_request)
    body = dict(source_request.get("body") or {})
    body["model"] = provider_model
    if prompt_override is not None:
        body["instructions"] = prompt_override
    request_copy["body"] = body
    return request_copy


def _translate_request(request: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    body = request.get("body")
    if not isinstance(body, dict):
        raise ValueError(
            f"Batch line for {request.get('custom_id')} is missing a valid body"
        )

    body_input = body.get("input")
    if not isinstance(body_input, list) or not body_input:
        raise ValueError(
            f"Batch line for {request.get('custom_id')} is missing body.input items"
        )

    messages: list[dict[str, str]] = []
    instructions = body.get("instructions")
    if isinstance(instructions, str) and instructions.strip():
        messages.append({"role": "system", "content": instructions})

    for item in body_input:
        if not isinstance(item, dict):
            raise ValueError(
                f"Batch line for {request.get('custom_id')} contains a non-object input item"
            )
        role = item.get("role")
        if not isinstance(role, str) or not role:
            raise ValueError(
                f"Batch line for {request.get('custom_id')} contains an input item with no role"
            )
        messages.append(
            {"role": role, "content": _extract_message_content(item, request)}
        )

    ollama_payload: dict[str, Any] = {
        "model": body.get("model"),
        "stream": False,
        "messages": messages,
    }

    response_text = body.get("text")
    if response_text is None:
        return ollama_payload, False
    if not isinstance(response_text, dict):
        raise ValueError(
            f"Batch line for {request.get('custom_id')} has an invalid body.text"
        )

    format_wrapper = response_text.get("format")
    if not isinstance(format_wrapper, dict):
        raise ValueError(
            f"Batch line for {request.get('custom_id')} has an invalid body.text.format"
        )
    if format_wrapper.get("type") != "json_schema":
        raise ValueError(
            f"Batch line for {request.get('custom_id')} must use body.text.format.type=json_schema"
        )
    schema = format_wrapper.get("schema")
    if not isinstance(schema, dict):
        raise ValueError(
            f"Batch line for {request.get('custom_id')} is missing body.text.format.schema"
        )
    ollama_payload["format"] = schema
    return ollama_payload, True


def _extract_message_content(item: dict[str, Any], request: dict[str, Any]) -> str:
    content = item.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list) or not content:
        raise ValueError(
            f"Batch line for {request.get('custom_id')} contains an input item with no content"
        )

    fragments: list[str] = []
    for content_item in content:
        if not isinstance(content_item, dict):
            raise ValueError(
                f"Batch line for {request.get('custom_id')} contains a non-object content item"
            )
        if content_item.get("type") != "input_text":
            raise ValueError(
                f"Batch line for {request.get('custom_id')} contains unsupported content type {content_item.get('type')!r}"
            )
        text = content_item.get("text")
        if not isinstance(text, str):
            raise ValueError(
                f"Batch line for {request.get('custom_id')} contains a non-string input_text"
            )
        fragments.append(text)

    return "".join(fragments)


def _run_single_request(
    prepared: OllamaPreparedRequest,
    *,
    base_url: str,
    request_timeout_seconds: int,
    request_executor: Callable[[str, dict[str, Any], int], dict[str, Any]],
) -> OllamaExecutionResult:
    try:
        raw_response = _request_with_retries(
            base_url,
            prepared.ollama_payload,
            request_timeout_seconds,
            request_executor,
        )
        output_text = _extract_output_text(raw_response)
        if prepared.requires_json_object:
            parsed = json.loads(output_text)
            if not isinstance(parsed, dict):
                raise ValueError("Structured output must be a JSON object")
        success_record = _build_success_record(
            prepared,
            raw_response=raw_response,
            output_text=output_text,
            base_url=base_url,
        )
        return OllamaExecutionResult(
            index=prepared.index,
            custom_id=prepared.custom_id,
            success_record=success_record,
            error_record=None,
            failed=False,
        )
    except Exception as exc:
        error_record = _build_error_record(prepared, exc, base_url=base_url)
        return OllamaExecutionResult(
            index=prepared.index,
            custom_id=prepared.custom_id,
            success_record=None,
            error_record=error_record,
            failed=True,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )


def _request_with_retries(
    base_url: str,
    payload: dict[str, Any],
    request_timeout_seconds: int,
    request_executor: Callable[[str, dict[str, Any], int], dict[str, Any]],
) -> dict[str, Any]:
    max_attempts = 3
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return request_executor(base_url, payload, request_timeout_seconds)
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts or not _is_retryable_error(exc):
                raise
            time.sleep(float(attempt))
    assert last_error is not None
    raise last_error


def _is_retryable_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    retryable_markers = (
        "connection reset",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "connection refused",
        "remote end closed connection",
        "bad gateway",
        "service unavailable",
    )
    return any(marker in lowered for marker in retryable_markers)


def _execute_ollama_chat_request(
    base_url: str,
    payload: dict[str, Any],
    request_timeout_seconds: int,
) -> dict[str, Any]:
    target_url = urllib_parse.urljoin(base_url + "/", OLLAMA_CHAT_ENDPOINT.lstrip("/"))
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib_request.Request(
        target_url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )

    try:
        with urllib_request.urlopen(req, timeout=request_timeout_seconds) as response:
            raw_body = response.read().decode("utf-8")
            status_code = response.getcode()
    except urllib_error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Ollama returned HTTP {exc.code}: {response_body.strip() or exc.reason}"
        ) from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Ollama request failed: {exc.reason}") from exc

    if status_code != 200:
        raise RuntimeError(f"Ollama returned HTTP {status_code}")

    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Ollama returned invalid JSON: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Ollama returned a non-object response")
    return parsed


class _ShardedExecutorPool:
    def __init__(self, *, base_urls: tuple[str, ...], num_parallel_jobs: int) -> None:
        self._pools = {
            base_url: ThreadPoolExecutor(max_workers=num_parallel_jobs)
            for base_url in base_urls
        }

    def __enter__(self) -> _ShardedExecutorPool:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        for pool in self._pools.values():
            pool.shutdown(wait=True)

    def submit(self, fn, *args, base_url: str, **kwargs):  # noqa: ANN001, ANN002, ANN003
        return self._pools[base_url].submit(fn, *args, base_url=base_url, **kwargs)


def _report_status(
    *,
    batch: dict[str, Any],
    total_requests: int,
    started_monotonic: float,
    state_started_monotonic: float,
    now_monotonic: float,
    reporter: StatusReporter,
    history_path: Path,
    mode: str,
    progress_event_count: int,
) -> tuple[Any, int]:
    snapshot = build_display_snapshot(
        batch,
        total_requests,
        started_monotonic,
        state_started_monotonic,
        now_monotonic,
    )
    if mode == "start":
        reporter.start(snapshot)
    elif mode == "stop":
        reporter.stop(snapshot)
    else:
        reporter.update(snapshot)
    append_status_history(history_path, batch, snapshot)
    return snapshot, progress_event_count + 1


def _extract_output_text(raw_response: dict[str, Any]) -> str:
    message = raw_response.get("message")
    if not isinstance(message, dict):
        raise ValueError("Ollama response is missing message")
    content = message.get("content")
    if not isinstance(content, str):
        raise ValueError("Ollama response is missing message.content")
    if not content.strip():
        raise ValueError("Ollama response did not contain any assistant content")
    return content


def _build_success_record(
    prepared: OllamaPreparedRequest,
    *,
    raw_response: dict[str, Any],
    output_text: str,
    base_url: str,
) -> dict[str, Any]:
    response_id = _record_id("resp", prepared.custom_id, prepared.index)
    message_id = _record_id("msg", prepared.custom_id, prepared.index)
    request_id = _record_id("req", prepared.custom_id, prepared.index)
    response_body = {
        "id": response_id,
        "object": "response",
        "created_at": _parse_ollama_timestamp(raw_response.get("created_at")),
        "status": "completed",
        "error": None,
        "incomplete_details": None,
        "instructions": prepared.submitted_request.get("body", {}).get("instructions"),
        "model": raw_response.get("model") or prepared.ollama_payload.get("model"),
        "output": [
            {
                "id": message_id,
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": output_text,
                        "annotations": [],
                    }
                ],
            }
        ],
        "text": prepared.submitted_request.get("body", {}).get("text"),
        "usage": _build_usage(raw_response),
        "provider_meta": {
            "provider": "ollama",
            "base_url": base_url,
            "endpoint": OLLAMA_CHAT_ENDPOINT,
            "done": raw_response.get("done"),
            "done_reason": raw_response.get("done_reason"),
            "load_duration": raw_response.get("load_duration"),
            "prompt_eval_count": raw_response.get("prompt_eval_count"),
            "prompt_eval_duration": raw_response.get("prompt_eval_duration"),
            "eval_count": raw_response.get("eval_count"),
            "eval_duration": raw_response.get("eval_duration"),
            "total_duration": raw_response.get("total_duration"),
        },
    }
    return {
        "custom_id": prepared.custom_id,
        "error": None,
        "response": {
            "body": response_body,
            "request_id": request_id,
            "status_code": 200,
        },
    }


def _build_error_record(
    prepared: OllamaPreparedRequest,
    exc: Exception,
    *,
    base_url: str,
) -> dict[str, Any]:
    request_id = _record_id("req", prepared.custom_id, prepared.index)
    message = str(exc)
    error_payload = {
        "type": exc.__class__.__name__,
        "message": message,
        "provider": "ollama",
        "base_url": base_url,
        "endpoint": OLLAMA_CHAT_ENDPOINT,
        "model": prepared.ollama_payload.get("model"),
    }
    return {
        "custom_id": prepared.custom_id,
        "error": error_payload,
        "response": {
            "body": {"error": error_payload},
            "request_id": request_id,
            "status_code": 500,
        },
    }


def _build_usage(raw_response: dict[str, Any]) -> dict[str, Any]:
    input_tokens = int(raw_response.get("prompt_eval_count") or 0)
    output_tokens = int(raw_response.get("eval_count") or 0)
    return {
        "input_tokens": input_tokens,
        "input_tokens_details": {"cached_tokens": 0},
        "output_tokens": output_tokens,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": input_tokens + output_tokens,
    }


def _build_initial_batch(
    batch_ids: dict[str, str],
    *,
    base_urls: tuple[str, ...],
    model: str,
    num_parallel_jobs: int,
) -> dict[str, Any]:
    provider_meta: dict[str, Any] = {
        "provider": "ollama",
        "provider_endpoint": OLLAMA_CHAT_ENDPOINT,
        "base_url": base_urls[0],
        "model": model,
    }
    if len(base_urls) > 1:
        provider_meta.update(
            {
                "base_urls": list(base_urls),
                "num_shards": len(base_urls),
                "per_shard_num_parallel_jobs": num_parallel_jobs,
                "total_possible_parallel_requests": len(base_urls) * num_parallel_jobs,
            }
        )
    return {
        "id": batch_ids["batch_id"],
        "status": "validating",
        "input_file_id": batch_ids["input_file_id"],
        "output_file_id": None,
        "error_file_id": None,
        "completion_window": "local",
        "created_at": time.time(),
        "request_counts": {"total": 0, "completed": 0, "failed": 0},
        "errors": None,
        "provider_meta": provider_meta,
    }


def _make_local_ids(
    submitted_lines: list[str], base_urls: tuple[str, ...], provider_model: str
) -> dict[str, str]:
    seed = "\n".join(submitted_lines) + f"\n{'\n'.join(base_urls)}\n{provider_model}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return {
        "batch_id": f"ollama_batch_{digest[:16]}",
        "input_file_id": f"ollama_file_input_{digest[16:32]}",
        "output_file_id": f"ollama_file_output_{digest[32:48]}",
        "error_file_id": f"ollama_file_error_{digest[48:64]}",
    }


def _parse_ollama_timestamp(value: Any) -> int:
    if not isinstance(value, str) or not value:
        return int(time.time())
    normalized = value.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(normalized).timestamp())
    except ValueError:
        return int(time.time())


def _record_id(prefix: str, custom_id: str, index: int) -> str:
    digest = hashlib.sha256(f"{prefix}:{custom_id}:{index}".encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:24]}"


def _render_jsonl_lines(records: list[dict[str, Any]]) -> list[str]:
    return [json.dumps(record, ensure_ascii=True, sort_keys=True) for record in records]
