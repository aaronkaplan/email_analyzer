from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from rich.console import Console

from email_analyzer.batch_submitter import run_batch_submitter
from email_analyzer.batch_submitter_common import (
    compute_stage_durations,
    format_status_line,
    load_and_validate_batch,
)
from email_analyzer.config import BatchSubmitConfig


class RecordingReporter:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, int, int, int]] = []

    def start(self, snapshot) -> None:
        self.events.append(
            (
                "start",
                snapshot.status,
                snapshot.total_requests,
                snapshot.completed_requests,
                snapshot.failed_requests,
            )
        )

    def update(self, snapshot) -> None:
        self.events.append(
            (
                "update",
                snapshot.status,
                snapshot.total_requests,
                snapshot.completed_requests,
                snapshot.failed_requests,
            )
        )

    def stop(self, snapshot) -> None:
        self.events.append(
            (
                "stop",
                snapshot.status,
                snapshot.total_requests,
                snapshot.completed_requests,
                snapshot.failed_requests,
            )
        )


class FakeFileResponse:
    def __init__(self, text: str) -> None:
        self.text = text


class FakeFilesAPI:
    def __init__(self, output_text: str, error_text: str) -> None:
        self.created_payloads: list[str] = []
        self._output_text = output_text
        self._error_text = error_text

    def create(self, *, file, purpose: str):  # noqa: ANN001
        assert purpose == "batch"
        payload = file.read().decode("utf-8")
        self.created_payloads.append(payload)
        return {"id": "file-input-123"}

    def content(self, file_id: str) -> FakeFileResponse:
        if file_id == "file-output-123":
            return FakeFileResponse(self._output_text)
        if file_id == "file-error-123":
            return FakeFileResponse(self._error_text)
        raise AssertionError(f"unexpected file id {file_id}")


class FakeBatchesAPI:
    def __init__(self, states: list[dict[str, object]]) -> None:
        self._states = states
        self.created: list[dict[str, object]] = []
        self.retrieve_calls = 0

    def create(self, *, input_file_id: str, endpoint: str, completion_window: str):
        self.created.append(
            {
                "input_file_id": input_file_id,
                "endpoint": endpoint,
                "completion_window": completion_window,
            }
        )
        return self._states[0]

    def retrieve(self, batch_id: str) -> dict[str, object]:
        self.retrieve_calls += 1
        index = min(self.retrieve_calls, len(self._states) - 1)
        state = dict(self._states[index])
        assert state["id"] == batch_id
        return state


class FakeOpenAIClient:
    def __init__(
        self, states: list[dict[str, object]], output_text: str, error_text: str
    ) -> None:
        self.files = FakeFilesAPI(output_text=output_text, error_text=error_text)
        self.batches = FakeBatchesAPI(states)


def test_load_and_validate_batch_rejects_duplicate_custom_id(tmp_path: Path) -> None:
    batch_jsonl = tmp_path / "batch.jsonl"
    line = {
        "custom_id": "dup.eml",
        "method": "POST",
        "url": "/v1/responses",
        "body": {"model": "gpt-4o-mini", "instructions": "x", "input": []},
    }
    batch_jsonl.write_text(
        json.dumps(line, sort_keys=True)
        + "\n"
        + json.dumps(line, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )

    try:
        load_and_validate_batch(batch_jsonl)
    except ValueError as exc:
        assert "Duplicate custom_id" in str(exc)
    else:
        raise AssertionError("expected duplicate custom_id validation error")


def test_load_and_validate_batch_handles_unicode_line_separator_in_string(
    tmp_path: Path,
) -> None:
    batch_jsonl = tmp_path / "batch.jsonl"
    line = {
        "custom_id": "unicode-separator.eml",
        "method": "POST",
        "url": "/v1/responses",
        "body": {
            "model": "gpt-4o-mini",
            "instructions": "prompt",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "before\u0085after"},
                    ],
                }
            ],
        },
    }
    batch_jsonl.write_text(
        json.dumps(line, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8"
    )

    requests, validation = load_and_validate_batch(batch_jsonl)

    assert validation.total_requests == 1
    assert validation.model == "gpt-4o-mini"
    assert requests[0]["custom_id"] == "unicode-separator.eml"


def test_run_batch_submitter_rewrites_prompt_and_downloads_outputs(
    tmp_path: Path,
) -> None:
    batch_jsonl = tmp_path / "batch-00001.jsonl"
    batch_jsonl.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "custom_id": "a.eml",
                        "method": "POST",
                        "url": "/v1/responses",
                        "body": {
                            "model": "gpt-4o-mini",
                            "instructions": "old prompt",
                            "input": [
                                {
                                    "role": "user",
                                    "content": [{"type": "input_text", "text": "{}"}],
                                }
                            ],
                        },
                    },
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "custom_id": "b.eml",
                        "method": "POST",
                        "url": "/v1/responses",
                        "body": {
                            "model": "gpt-4o-mini",
                            "instructions": "old prompt",
                            "input": [
                                {
                                    "role": "user",
                                    "content": [{"type": "input_text", "text": "{}"}],
                                }
                            ],
                        },
                    },
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    states = [
        {
            "id": "batch_1234567890",
            "status": "validating",
            "input_file_id": "file-input-123",
            "output_file_id": None,
            "error_file_id": None,
            "completion_window": "24h",
            "created_at": 100.0,
            "request_counts": {"total": 0, "completed": 0, "failed": 0},
        },
        {
            "id": "batch_1234567890",
            "status": "in_progress",
            "input_file_id": "file-input-123",
            "output_file_id": None,
            "error_file_id": None,
            "completion_window": "24h",
            "created_at": 100.0,
            "in_progress_at": 105.0,
            "request_counts": {"total": 2, "completed": 1, "failed": 0},
        },
        {
            "id": "batch_1234567890",
            "status": "completed",
            "input_file_id": "file-input-123",
            "output_file_id": "file-output-123",
            "error_file_id": "file-error-123",
            "completion_window": "24h",
            "created_at": 100.0,
            "in_progress_at": 105.0,
            "finalizing_at": 110.0,
            "completed_at": 112.0,
            "request_counts": {"total": 2, "completed": 2, "failed": 0},
        },
    ]
    client = FakeOpenAIClient(
        states=states,
        output_text='{"custom_id":"a.eml"}\n{"custom_id":"b.eml"}\n',
        error_text="",
    )
    reporter = RecordingReporter()
    console_stream = StringIO()
    console = Console(file=console_stream, force_terminal=False, color_system=None)

    exit_code = run_batch_submitter(
        BatchSubmitConfig(
            batch_jsonl=batch_jsonl,
            prompt="new prompt",
            poll_interval_seconds=1,
        ),
        client=client,
        sleep_fn=lambda _: None,
        reporter=reporter,
        console=console,
    )

    assert exit_code == 0
    rewritten_lines = [
        json.loads(line)
        for line in batch_jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert all(line["body"]["instructions"] == "new prompt" for line in rewritten_lines)

    output_dir = tmp_path / "batch_output" / "batch-00001"
    assert (output_dir / "batch_input.before_submit.jsonl").exists()
    assert (output_dir / "batch_input.submitted.jsonl").exists()
    assert (output_dir / "batch_output.jsonl").exists()
    assert (output_dir / "batch_errors.jsonl").exists()

    summary = json.loads(
        (output_dir / "batch_summary.json").read_text(encoding="utf-8")
    )
    assert summary["status"] == "completed"
    assert summary["successful_processed_mails"] == 2
    assert summary["failed_mails"] == 0
    assert summary["output_line_count"] == 2
    assert summary["error_line_count"] == 0
    assert summary["stage_durations_seconds"]["validating"] == 5.0
    assert summary["stage_durations_seconds"]["in_progress"] == 5.0
    assert summary["stage_durations_seconds"]["finalizing"] == 2.0

    history_lines = (
        (output_dir / "batch_status_history.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert len(history_lines) == 3
    assert reporter.events[0][:2] == ("start", "validating")
    assert reporter.events[-1][:2] == ("stop", "completed")
    assert "Batch Summary" in console_stream.getvalue()


def test_run_batch_submitter_no_wait_submits_and_exits_early(tmp_path: Path) -> None:
    batch_jsonl = tmp_path / "batch-00001.jsonl"
    batch_jsonl.write_text(
        json.dumps(
            {
                "custom_id": "a.eml",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": "gpt-4o-mini",
                    "instructions": "prompt",
                    "input": [
                        {
                            "role": "user",
                            "content": [{"type": "input_text", "text": "{}"}],
                        }
                    ],
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    client = FakeOpenAIClient(
        states=[
            {
                "id": "batch_nowait_123",
                "status": "validating",
                "input_file_id": "file-input-123",
                "output_file_id": None,
                "error_file_id": None,
                "completion_window": "24h",
                "created_at": 100.0,
                "request_counts": {"total": 0, "completed": 0, "failed": 0},
            }
        ],
        output_text="",
        error_text="",
    )
    reporter = RecordingReporter()

    exit_code = run_batch_submitter(
        BatchSubmitConfig(batch_jsonl=batch_jsonl, no_wait=True),
        client=client,
        sleep_fn=lambda _: None,
        reporter=reporter,
        console=Console(file=StringIO(), force_terminal=False, color_system=None),
    )

    assert exit_code == 0
    assert client.batches.retrieve_calls == 0
    assert reporter.events == [
        ("start", "validating", 1, 0, 0),
        ("stop", "validating", 1, 0, 0),
    ]

    output_dir = tmp_path / "batch_output" / "batch-00001"
    summary = json.loads(
        (output_dir / "batch_summary.json").read_text(encoding="utf-8")
    )
    assert summary["waiting_mode"] == "submitted"
    assert summary["status"] == "validating"
    assert summary["poll_count"] == 1
    assert not (output_dir / "batch_output.jsonl").exists()


def test_run_batch_submitter_resume_batch_id_polls_existing_batch(
    tmp_path: Path,
) -> None:
    client = FakeOpenAIClient(
        states=[
            {
                "id": "batch_resume_123",
                "status": "in_progress",
                "input_file_id": "file-existing-123",
                "output_file_id": None,
                "error_file_id": None,
                "completion_window": "24h",
                "created_at": 200.0,
                "in_progress_at": 205.0,
                "request_counts": {"total": 3, "completed": 1, "failed": 0},
            },
            {
                "id": "batch_resume_123",
                "status": "completed",
                "input_file_id": "file-existing-123",
                "output_file_id": "file-output-123",
                "error_file_id": None,
                "completion_window": "24h",
                "created_at": 200.0,
                "in_progress_at": 205.0,
                "finalizing_at": 209.0,
                "completed_at": 211.0,
                "request_counts": {"total": 3, "completed": 3, "failed": 0},
            },
        ],
        output_text='{"custom_id":"a.eml"}\n{"custom_id":"b.eml"}\n{"custom_id":"c.eml"}\n',
        error_text="",
    )
    reporter = RecordingReporter()
    output_dir = tmp_path / "resume-output"

    exit_code = run_batch_submitter(
        BatchSubmitConfig(
            resume_batch_id="batch_resume_123",
            output_dir=output_dir,
            poll_interval_seconds=1,
        ),
        client=client,
        sleep_fn=lambda _: None,
        reporter=reporter,
        console=Console(file=StringIO(), force_terminal=False, color_system=None),
    )

    assert exit_code == 0
    assert client.batches.created == []
    assert client.files.created_payloads == []
    assert reporter.events[0][:2] == ("start", "in_progress")
    assert reporter.events[-1][:2] == ("stop", "completed")

    summary = json.loads(
        (output_dir / "batch_summary.json").read_text(encoding="utf-8")
    )
    assert summary["batch_id"] == "batch_resume_123"
    assert summary["waiting_mode"] == "completed"
    assert summary["output_line_count"] == 3
    assert summary["successful_processed_mails"] == 3
    assert (
        json.loads((output_dir / "submission.json").read_text(encoding="utf-8"))[
            "resume_mode"
        ]
        is True
    )


def test_run_batch_submitter_rejects_prompt_override_with_resume(
    tmp_path: Path,
) -> None:
    console_stream = StringIO()
    exit_code = run_batch_submitter(
        BatchSubmitConfig(
            resume_batch_id="batch_resume_123",
            output_dir=tmp_path / "resume-output",
            prompt="not allowed",
        ),
        client=FakeOpenAIClient(states=[], output_text="", error_text=""),
        console=Console(file=console_stream, force_terminal=False, color_system=None),
    )

    assert exit_code == 1
    assert "Prompt overrides cannot be used" in console_stream.getvalue()


def test_compute_stage_durations_handles_expired_batch() -> None:
    durations = compute_stage_durations(
        {
            "created_at": 10,
            "in_progress_at": 14,
            "expired_at": 20,
        }
    )

    assert durations["validating"] == 4.0
    assert durations["in_progress"] == 6.0
    assert durations["total_elapsed"] == 10.0


def test_format_status_line_includes_progress_fields() -> None:
    from email_analyzer.batch_submitter_common import BatchDisplaySnapshot

    line = format_status_line(
        BatchDisplaySnapshot(
            batch_id="batch_abcdefgh",
            status="in_progress",
            total_requests=20,
            completed_requests=12,
            failed_requests=3,
            processed_requests=15,
            remaining_requests=5,
            percent_complete=75.0,
            elapsed_seconds=90.0,
            state_elapsed_seconds=12.0,
            speed_emails_per_sec=15.0 / 90.0,
            eta_seconds=5.0 / (15.0 / 90.0),
        )
    )

    assert "in_progress" in line
    assert "abcdefgh" in line
    assert "15/20" in line
    assert "75.0%" in line
    assert "failed 3" in line
    assert "6.00s/it" in line
    assert "ETA 00:00:30" in line
