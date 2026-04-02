from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from .config import (
    DEFAULT_OPENAI_INSTRUCTIONS,
    OPENAI_BATCH_ENDPOINT,
    RenderBatchConfig,
)


def run_render_batch(config: RenderBatchConfig) -> int:
    config.batch_dir.mkdir(parents=True, exist_ok=True)
    config.processed_dir.mkdir(parents=True, exist_ok=True)

    processed_files = sorted(
        path
        for path in config.processed_dir.glob("*.json")
        if not path.name.endswith(".error.json")
    )

    instructions = _read_instructions(config.instructions_file)
    response_format = _load_schema_format(config.schema_file)
    shards: list[list[str]] = []
    current_shard: list[str] = []
    current_requests = 0
    current_bytes = 0

    for processed_path in processed_files:
        processed = json.loads(processed_path.read_text(encoding="utf-8"))
        line = _render_line(
            processed, config.model, instructions, response_format=response_format
        )
        # Keep JSONL records ASCII-escaped so Unicode line-separator code points
        # inside email text cannot be misread as record boundaries.
        encoded_line = json.dumps(line, ensure_ascii=True, sort_keys=True)
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


def _load_schema_format(schema_file: Path | None) -> dict[str, Any] | None:
    if schema_file is None:
        return None
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file does not exist: {schema_file}")

    module_name = f"email_analyzer_schema_{schema_file.stem.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, schema_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load schema module from {schema_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    schema_class = getattr(module, "mySchema", None)
    if schema_class is None:
        raise ValueError(
            f"Schema file must define class mySchema(BaseModel): {schema_file}"
        )

    model_json_schema = getattr(schema_class, "model_json_schema", None)
    if not callable(model_json_schema):
        raise ValueError(f"mySchema must provide model_json_schema(): {schema_file}")

    schema_name = getattr(schema_class, "__name__", "mySchema")
    schema = schema_class.model_json_schema()
    _ensure_strict_json_schema(schema)

    return {
        "format": {
            "type": "json_schema",
            "name": schema_name,
            "schema": schema,
            "strict": True,
        }
    }


def _ensure_strict_json_schema(node: Any) -> None:
    if isinstance(node, dict):
        if node.get("type") == "object" or "properties" in node:
            node.setdefault("additionalProperties", False)
            properties = node.get("properties")
            if isinstance(properties, dict):
                node["required"] = list(properties)
        for value in node.values():
            _ensure_strict_json_schema(value)
        return

    if isinstance(node, list):
        for item in node:
            _ensure_strict_json_schema(item)


def _render_line(
    processed: dict[str, Any],
    model: str,
    instructions: str,
    *,
    response_format: dict[str, Any] | None,
) -> dict[str, Any]:
    email_package = {
        "email_id": processed["email_id"],
        "source_filename": processed["source_filename"],
        "headers": processed.get("headers", {}),
        "parser_defects": processed.get("parser_defects", []),
        "canonical_body": processed.get("canonical_body"),
        "kept_snippets": processed.get("kept_snippets", []),
        "attachments": processed.get("attachments", []),
        "stats": {
            "estimated_total_tokens": processed.get("stats", {}).get(
                "estimated_total_tokens", 0
            ),
            "kept_snippet_count": processed.get("stats", {}).get(
                "kept_snippet_count", 0
            ),
            "dropped_part_count": processed.get("stats", {}).get(
                "dropped_part_count", 0
            ),
        },
    }

    body = {
        "model": model,
        "instructions": instructions,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            email_package, ensure_ascii=False, sort_keys=True
                        ),
                    }
                ],
            }
        ],
    }

    if response_format is not None:
        body["text"] = response_format

    return {
        "custom_id": processed["source_filename"],
        "method": "POST",
        "url": OPENAI_BATCH_ENDPOINT,
        "body": body,
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
        _write_text_atomic(
            batch_alias, "\n".join(shards[0]) + ("\n" if shards[0] else "")
        )


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)
