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

    payload = json.loads(line["body"]["input"])
    assert payload["source_filename"] == "sample.eml"
    assert payload["headers"]["subject"] == "Example"

    batch_alias = processed_dir / "batch.jsonl"
    assert batch_alias.exists()
