from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

from rich.console import Console

from email_analyzer.config import OllamaBatchSubmitConfig
from email_analyzer.ollama_batch_submitter import (
    _resolve_output_dir,
    _translate_request,
    run_ollama_batch_submitter,
)


class RecordingReporter:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, int, int, int]] = []

    def start(self, snapshot) -> None:  # noqa: ANN001
        self.events.append(
            (
                "start",
                snapshot.status,
                snapshot.total_requests,
                snapshot.completed_requests,
                snapshot.failed_requests,
            )
        )

    def update(self, snapshot) -> None:  # noqa: ANN001
        self.events.append(
            (
                "update",
                snapshot.status,
                snapshot.total_requests,
                snapshot.completed_requests,
                snapshot.failed_requests,
            )
        )

    def stop(self, snapshot) -> None:  # noqa: ANN001
        self.events.append(
            (
                "stop",
                snapshot.status,
                snapshot.total_requests,
                snapshot.completed_requests,
                snapshot.failed_requests,
            )
        )


def test_translate_request_maps_openai_shape_to_ollama_chat() -> None:
    request = {
        "custom_id": "sample.eml",
        "method": "POST",
        "url": "/v1/responses",
        "body": {
            "model": "gpt-oss:120b",
            "instructions": "Classify this email.",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": '{"email_id":"sample.eml"}'}
                    ],
                }
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "mySchema",
                    "schema": {
                        "type": "object",
                        "properties": {"summary": {"type": "string"}},
                    },
                    "strict": True,
                }
            },
        },
    }

    payload, requires_json_object = _translate_request(request)

    assert payload["model"] == "gpt-oss:120b"
    assert payload["stream"] is False
    assert payload["messages"][0] == {
        "role": "system",
        "content": "Classify this email.",
    }
    assert payload["messages"][1] == {
        "role": "user",
        "content": '{"email_id":"sample.eml"}',
    }
    assert payload["format"] == {
        "type": "object",
        "properties": {"summary": {"type": "string"}},
    }
    assert requires_json_object is True


def test_resolve_output_dir_uses_ollama_batch_output_root(tmp_path: Path) -> None:
    batch_jsonl = tmp_path / "output" / "batches" / "batch-00001.jsonl"
    batch_jsonl.parent.mkdir(parents=True)
    batch_jsonl.write_text("", encoding="utf-8")

    output_dir = _resolve_output_dir(OllamaBatchSubmitConfig(batch_jsonl=batch_jsonl))

    assert output_dir == tmp_path / "output" / "ollama_batch_output" / "batch-00001"


def test_run_ollama_batch_submitter_writes_openai_compatible_output_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    batch_jsonl = tmp_path / "output" / "batches" / "batch-00001.jsonl"
    batch_jsonl.parent.mkdir(parents=True)
    batch_jsonl.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "custom_id": "a.eml",
                        "method": "POST",
                        "url": "/v1/responses",
                        "body": {
                            "model": "gpt-5.4-nano",
                            "instructions": "Return JSON.",
                            "input": [
                                {
                                    "role": "user",
                                    "content": [
                                        {
                                            "type": "input_text",
                                            "text": '{"email_id":"a.eml"}',
                                        }
                                    ],
                                }
                            ],
                            "text": {
                                "format": {
                                    "type": "json_schema",
                                    "name": "mySchema",
                                    "schema": {
                                        "type": "object",
                                        "properties": {"summary": {"type": "string"}},
                                        "required": ["summary"],
                                    },
                                    "strict": True,
                                }
                            },
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
                            "model": "gpt-5.4-nano",
                            "instructions": "Return JSON.",
                            "input": [
                                {
                                    "role": "user",
                                    "content": [
                                        {
                                            "type": "input_text",
                                            "text": '{"email_id":"b.eml"}',
                                        }
                                    ],
                                }
                            ],
                            "text": {
                                "format": {
                                    "type": "json_schema",
                                    "name": "mySchema",
                                    "schema": {
                                        "type": "object",
                                        "properties": {"summary": {"type": "string"}},
                                        "required": ["summary"],
                                    },
                                    "strict": True,
                                }
                            },
                        },
                    },
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("OLLAMA_BASE_URL", "http://nanu:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "gpt-oss:120b")

    responses = {
        "a.eml": {
            "model": "gpt-oss:120b",
            "created_at": "2026-04-02T12:00:00Z",
            "message": {"role": "assistant", "content": '{"summary":"A"}'},
            "done": True,
            "done_reason": "stop",
            "prompt_eval_count": 10,
            "eval_count": 5,
            "load_duration": 1,
            "prompt_eval_duration": 2,
            "eval_duration": 3,
            "total_duration": 4,
        },
        "b.eml": {
            "model": "gpt-oss:120b",
            "created_at": "2026-04-02T12:00:01Z",
            "message": {"role": "assistant", "content": '{"summary":"B"}'},
            "done": True,
            "done_reason": "stop",
            "prompt_eval_count": 11,
            "eval_count": 6,
            "load_duration": 1,
            "prompt_eval_duration": 2,
            "eval_duration": 3,
            "total_duration": 4,
        },
    }

    def fake_executor(
        base_url: str, payload: dict[str, object], timeout: int
    ) -> dict[str, object]:
        assert base_url == "http://nanu:11434"
        assert timeout == 600
        assert payload["model"] == "gpt-oss:120b"
        messages = payload["messages"]
        assert isinstance(messages, list)
        user_message = messages[-1]
        assert isinstance(user_message, dict)
        content = user_message["content"]
        parsed = json.loads(content)
        return dict(responses[parsed["email_id"]])

    reporter = RecordingReporter()
    console_stream = StringIO()
    console = Console(file=console_stream, force_terminal=False, color_system=None)

    exit_code = run_ollama_batch_submitter(
        OllamaBatchSubmitConfig(batch_jsonl=batch_jsonl, num_parallel_jobs=2),
        request_executor=fake_executor,
        reporter=reporter,
        console=console,
    )

    assert exit_code == 0
    output_dir = tmp_path / "output" / "ollama_batch_output" / "batch-00001"
    assert (output_dir / "submission.json").exists()
    assert (output_dir / "batch_input.submitted.jsonl").exists()
    assert (output_dir / "batch_status_history.jsonl").exists()
    assert (output_dir / "batch_final.json").exists()
    assert (output_dir / "batch_output.jsonl").exists()
    assert (output_dir / "batch_errors.jsonl").exists()
    assert (output_dir / "batch_summary.json").exists()

    summary = json.loads(
        (output_dir / "batch_summary.json").read_text(encoding="utf-8")
    )
    assert summary["status"] == "completed"
    assert summary["successful_processed_mails"] == 2
    assert summary["failed_mails"] == 0
    assert summary["output_line_count"] == 2
    assert summary["error_line_count"] == 0
    assert summary["provider_meta"]["provider"] == "ollama"
    assert summary["provider_meta"]["provider_model"] == "gpt-oss:120b"
    assert summary["provider_meta"]["source_model"] == "gpt-5.4-nano"

    history_lines = (
        (output_dir / "batch_status_history.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert len(history_lines) >= 4
    assert reporter.events[0][:2] == ("start", "validating")
    assert reporter.events[-1][:2] == ("stop", "completed")
    assert "Batch Summary" in console_stream.getvalue()

    output_lines = [
        json.loads(line)
        for line in (output_dir / "batch_output.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert [line["custom_id"] for line in output_lines] == ["a.eml", "b.eml"]
    assert output_lines[0]["response"]["status_code"] == 200
    assert (
        output_lines[0]["response"]["body"]["output"][0]["content"][0]["type"]
        == "output_text"
    )
    assert json.loads(
        output_lines[0]["response"]["body"]["output"][0]["content"][0]["text"]
    ) == {"summary": "A"}


def test_run_ollama_batch_submitter_records_invalid_structured_output_as_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    batch_jsonl = tmp_path / "batch.jsonl"
    batch_jsonl.write_text(
        json.dumps(
            {
                "custom_id": "bad.eml",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": "gpt-5.4-nano",
                    "instructions": "Return JSON.",
                    "input": [
                        {
                            "role": "user",
                            "content": [{"type": "input_text", "text": "{}"}],
                        }
                    ],
                    "text": {
                        "format": {
                            "type": "json_schema",
                            "name": "mySchema",
                            "schema": {"type": "object"},
                            "strict": True,
                        }
                    },
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("OLLAMA_BASE_URL", "http://nanu:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "gpt-oss:120b")

    def fake_executor(
        base_url: str, payload: dict[str, object], timeout: int
    ) -> dict[str, object]:
        return {
            "model": "gpt-oss:120b",
            "created_at": "2026-04-02T12:00:00Z",
            "message": {"role": "assistant", "content": "not json"},
            "done": True,
            "done_reason": "stop",
        }

    exit_code = run_ollama_batch_submitter(
        OllamaBatchSubmitConfig(batch_jsonl=batch_jsonl),
        request_executor=fake_executor,
        console=Console(file=StringIO(), force_terminal=False, color_system=None),
    )

    assert exit_code == 1
    output_dir = tmp_path / "ollama_batch_output" / "batch"
    error_lines = [
        json.loads(line)
        for line in (output_dir / "batch_errors.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(error_lines) == 1
    assert error_lines[0]["custom_id"] == "bad.eml"
    assert error_lines[0]["response"]["status_code"] == 500


def test_run_ollama_batch_submitter_retries_transient_connection_reset(
    tmp_path: Path,
    monkeypatch,
) -> None:
    batch_jsonl = tmp_path / "batch.jsonl"
    batch_jsonl.write_text(
        json.dumps(
            {
                "custom_id": "retry.eml",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": "gpt-5.4-nano",
                    "instructions": "Return JSON.",
                    "input": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "input_text",
                                    "text": '{"email_id":"retry.eml"}',
                                }
                            ],
                        }
                    ],
                    "text": {
                        "format": {
                            "type": "json_schema",
                            "name": "mySchema",
                            "schema": {"type": "object"},
                            "strict": True,
                        }
                    },
                },
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("OLLAMA_BASE_URL", "http://nanu:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "gpt-oss:120b")

    calls = {"count": 0}

    def fake_executor(
        base_url: str, payload: dict[str, object], timeout: int
    ) -> dict[str, object]:
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionResetError("[Errno 54] Connection reset by peer")
        return {
            "model": "gpt-oss:120b",
            "created_at": "2026-04-02T12:00:00Z",
            "message": {"role": "assistant", "content": '{"summary":"ok"}'},
            "done": True,
            "done_reason": "stop",
            "prompt_eval_count": 10,
            "eval_count": 5,
        }

    exit_code = run_ollama_batch_submitter(
        OllamaBatchSubmitConfig(batch_jsonl=batch_jsonl),
        request_executor=fake_executor,
        console=Console(file=StringIO(), force_terminal=False, color_system=None),
    )

    assert exit_code == 0
    assert calls["count"] == 3
