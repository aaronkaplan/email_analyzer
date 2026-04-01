from __future__ import annotations

import json
from pathlib import Path

from email_analyzer.config import RenderBatchConfig
from email_analyzer.render_batch import run_render_batch


def test_render_batch_writes_openai_jsonl(tmp_path: Path) -> None:
    processed_dir = tmp_path / "output"
    batch_dir = processed_dir / "batches"
    processed_dir.mkdir()

    processed_payload = {
        "email_id": "sample.eml",
        "source_filename": "sample.eml",
        "headers": {"subject": "Example"},
        "parser_defects": [],
        "canonical_body": {
            "snippet_id": "canonical_body",
            "kind": "canonical_body",
            "source_part_path": "1.1",
            "content_type": "text/plain",
            "filename": None,
            "text": "Hello world",
            "language": {"code": "en", "name": "english", "confidence": 0.99},
            "characters": 11,
            "token_estimate": 3,
            "metadata": {},
        },
        "kept_snippets": [],
        "attachments": [],
        "stats": {
            "estimated_total_tokens": 3,
            "kept_snippet_count": 1,
            "dropped_part_count": 0,
        },
    }
    (processed_dir / "sample.eml.json").write_text(
        json.dumps(processed_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    config = RenderBatchConfig(
        processed_dir=processed_dir,
        batch_dir=batch_dir,
        model="gpt-4o-mini",
    )

    exit_code = run_render_batch(config)
    assert exit_code == 0

    shard_path = batch_dir / "batch-00001.jsonl"
    assert shard_path.exists()

    shard_lines = shard_path.read_text(encoding="utf-8").splitlines()
    assert len(shard_lines) == 1

    line = json.loads(shard_lines[0])
    assert line["custom_id"] == "sample.eml"
    assert line["method"] == "POST"
    assert line["url"] == "/v1/responses"
    assert line["body"]["model"] == "gpt-4o-mini"
    assert isinstance(line["body"]["instructions"], str)

    input_items = line["body"]["input"]
    assert len(input_items) == 1
    assert input_items[0]["role"] == "user"
    assert input_items[0]["content"][0]["type"] == "input_text"

    payload = json.loads(input_items[0]["content"][0]["text"])
    assert payload["source_filename"] == "sample.eml"
    assert payload["headers"]["subject"] == "Example"

    batch_alias = processed_dir / "batch.jsonl"
    assert batch_alias.exists()


def test_render_batch_adds_structured_output_schema_when_requested(tmp_path: Path) -> None:
    processed_dir = tmp_path / "output"
    batch_dir = processed_dir / "batches"
    schema_file = tmp_path / "email_schema.py"
    processed_dir.mkdir()

    schema_file.write_text(
        "from pydantic import BaseModel\n\n"
        "class mySchema(BaseModel):\n"
        "    category: str\n"
        "    summary: str\n",
        encoding="utf-8",
    )

    processed_payload = {
        "email_id": "sample.eml",
        "source_filename": "sample.eml",
        "headers": {"subject": "Example"},
        "parser_defects": [],
        "canonical_body": {
            "snippet_id": "canonical_body",
            "kind": "canonical_body",
            "source_part_path": "1.1",
            "content_type": "text/plain",
            "filename": None,
            "text": "Hello world",
            "language": {"code": "en", "name": "english", "confidence": 0.99},
            "characters": 11,
            "token_estimate": 3,
            "metadata": {},
        },
        "kept_snippets": [],
        "attachments": [],
        "stats": {
            "estimated_total_tokens": 3,
            "kept_snippet_count": 1,
            "dropped_part_count": 0,
        },
    }
    (processed_dir / "sample.eml.json").write_text(
        json.dumps(processed_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    config = RenderBatchConfig(
        processed_dir=processed_dir,
        batch_dir=batch_dir,
        model="gpt-4o-mini",
        schema_file=schema_file,
    )

    exit_code = run_render_batch(config)
    assert exit_code == 0

    line = json.loads((batch_dir / "batch-00001.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert line["body"]["text"]["format"]["type"] == "json_schema"
    assert line["body"]["text"]["format"]["name"] == "mySchema"
    assert line["body"]["text"]["format"]["strict"] is True
    assert line["body"]["text"]["format"]["schema"]["type"] == "object"
    assert line["body"]["text"]["format"]["schema"]["additionalProperties"] is False
    assert set(line["body"]["text"]["format"]["schema"]["properties"]) == {"category", "summary"}
    assert line["body"]["text"]["format"]["schema"]["required"] == ["category", "summary"]


def test_render_batch_marks_defaulted_schema_fields_as_required(tmp_path: Path) -> None:
    processed_dir = tmp_path / "output"
    batch_dir = processed_dir / "batches"
    processed_dir.mkdir()

    processed_payload = {
        "email_id": "sample.eml",
        "source_filename": "sample.eml",
        "headers": {"subject": "Example"},
        "parser_defects": [],
        "canonical_body": {
            "snippet_id": "canonical_body",
            "kind": "canonical_body",
            "source_part_path": "1.1",
            "content_type": "text/plain",
            "filename": None,
            "text": "Hello world",
            "language": {"code": "en", "name": "english", "confidence": 0.99},
            "characters": 11,
            "token_estimate": 3,
            "metadata": {},
        },
        "kept_snippets": [],
        "attachments": [],
        "stats": {
            "estimated_total_tokens": 3,
            "kept_snippet_count": 1,
            "dropped_part_count": 0,
        },
    }
    (processed_dir / "sample.eml.json").write_text(
        json.dumps(processed_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    config = RenderBatchConfig(
        processed_dir=processed_dir,
        batch_dir=batch_dir,
        model="gpt-4o-mini",
        schema_file=Path("/Users/aaron/Desktop/git/work/ec/email_analyzer/docs/structured_output_schema_example.py"),
    )

    exit_code = run_render_batch(config)
    assert exit_code == 0

    line = json.loads((batch_dir / "batch-00001.jsonl").read_text(encoding="utf-8").splitlines()[0])
    schema = line["body"]["text"]["format"]["schema"]
    assert "action_items" in schema["properties"]
    assert "action_items" in schema["required"]
