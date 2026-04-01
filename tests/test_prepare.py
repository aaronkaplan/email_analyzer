from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from email_analyzer.config import PrepareConfig
from email_analyzer.prepare import run_prepare


def test_prepare_outputs_reduced_json_and_logs(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    logs_dir = tmp_path / "logs"
    input_dir.mkdir()

    fixture_path = Path(__file__).parent / "fixtures" / "alternative_duplicate.eml"
    target_path = input_dir / fixture_path.name
    target_path.write_bytes(fixture_path.read_bytes())

    config = PrepareConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        logs_dir=logs_dir,
        workers=1,
    )

    exit_code = run_prepare(config, executor_factory=ThreadPoolExecutor)
    assert exit_code == 0

    processed_path = output_dir / f"{fixture_path.name}.json"
    assert processed_path.exists()

    processed = json.loads(processed_path.read_text(encoding="utf-8"))
    assert processed["email_id"] == fixture_path.name
    assert processed["source_filename"] == fixture_path.name
    assert processed["canonical_body"]["text"] == "Hello Bob,\n\nHere is the update you asked for.\n\nRegards,\nAlice"
    assert any(item["reason"] == "duplicate_body_representation" for item in processed["dropped_parts"])
    assert any(item["filename"] == "body.html" for item in processed["dropped_parts"])

    kept_attachment_names = [item["filename"] for item in processed["attachments"] if item["kept"]]
    assert kept_attachment_names == ["notes.txt"]

    kept_snippet_names = [item["filename"] for item in processed["kept_snippets"] if item["kind"] == "attachment"]
    assert kept_snippet_names == ["notes.txt"]

    assert "write_output" in processed["timings_ms"]
    assert processed["total_duration_ms"] >= processed["timings_ms"]["write_output"]

    pipeline_log = logs_dir / "pipeline.jsonl"
    assert pipeline_log.exists()
    log_entries = [json.loads(line) for line in pipeline_log.read_text(encoding="utf-8").splitlines()]
    assert any(entry.get("step") == "filter_duplicate_body_representations" for entry in log_entries)
    assert any(entry.get("reason") == "duplicate_body_representation" for entry in log_entries)

    step_summary = json.loads((logs_dir / "step_summary.json").read_text(encoding="utf-8"))
    assert "parse_source" in step_summary
