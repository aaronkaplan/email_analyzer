from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import DEFAULT_OPENAI_INSTRUCTIONS, OPENAI_BATCH_ENDPOINT, RenderBatchConfig


def run_render_batch(config: RenderBatchConfig) -> int:
    config.batch_dir.mkdir(parents=True, exist_ok=True)
    config.processed_dir.mkdir(parents=True, exist_ok=True)

    processed_files = sorted(
        path
        for path in config.processed_dir.glob("*.json")
        if not path.name.endswith(".error.json")
    )

    instructions = _read_instructions(config.instructions_file)
    shards: list[list[str]] = []
    current_shard: list[str] = []
    current_requests = 0
    current_bytes = 0

    for processed_path in processed_files:
        processed = json.loads(processed_path.read_text(encoding="utf-8"))
        line = _render_line(processed, config.model, instructions)
        encoded_line = json.dumps(line, ensure_ascii=False, sort_keys=True)
        encoded_bytes = len((encoded_line + "\n").encode("utf-8"))

        if current_shard and (
            current_requests >= config.max_requests_per_file
            or current_bytes + encoded_bytes > config.max_bytes_per_file
        ):
            shards.append(current_shard)
            current_shard = []
            current_requests = 0
            current_bytes = 0

        current_shard.append(encoded_line)
        current_requests += 1
        current_bytes += encoded_bytes

    if current_shard:
        shards.append(current_shard)

    _write_shards(config, shards)
    return 0


def _read_instructions(instructions_file: Path | None) -> str:
    if instructions_file is None:
        return DEFAULT_OPENAI_INSTRUCTIONS.strip()
    return instructions_file.read_text(encoding="utf-8").strip()


def _render_line(processed: dict[str, Any], model: str, instructions: str) -> dict[str, Any]:
    email_package = {
        "email_id": processed["email_id"],
        "source_filename": processed["source_filename"],
        "headers": processed.get("headers", {}),
        "parser_defects": processed.get("parser_defects", []),
        "canonical_body": processed.get("canonical_body"),
        "kept_snippets": processed.get("kept_snippets", []),
        "attachments": processed.get("attachments", []),
        "stats": {
            "estimated_total_tokens": processed.get("stats", {}).get("estimated_total_tokens", 0),
            "kept_snippet_count": processed.get("stats", {}).get("kept_snippet_count", 0),
            "dropped_part_count": processed.get("stats", {}).get("dropped_part_count", 0),
        },
    }

    return {
        "custom_id": processed["source_filename"],
        "method": "POST",
        "url": OPENAI_BATCH_ENDPOINT,
        "body": {
            "model": model,
            "instructions": instructions,
            "input": json.dumps(email_package, ensure_ascii=False, sort_keys=True),
        },
    }


def _write_shards(config: RenderBatchConfig, shards: list[list[str]]) -> None:
    existing = sorted(config.batch_dir.glob("batch-*.jsonl"))
    for path in existing:
        path.unlink()

    batch_alias = config.processed_dir / "batch.jsonl"
    if batch_alias.exists():
        batch_alias.unlink()

    if not shards:
        return

    for index, shard in enumerate(shards, start=1):
        target = config.batch_dir / f"batch-{index:05d}.jsonl"
        _write_text_atomic(target, "\n".join(shard) + ("\n" if shard else ""))

    if len(shards) == 1:
        _write_text_atomic(batch_alias, "\n".join(shards[0]) + ("\n" if shards[0] else ""))


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)
