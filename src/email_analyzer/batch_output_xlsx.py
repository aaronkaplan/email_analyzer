from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from openpyxl import Workbook

from .config import BatchOutputXlsxConfig


def run_batch_output_to_xlsx(config: BatchOutputXlsxConfig) -> int:
    schema_columns = _load_schema_columns(config.schema_file)
    rows = _load_rows(config.input_jsonl)
    if not rows:
        raise ValueError(f"No batch output rows found in {config.input_jsonl}")

    columns = _resolve_columns(schema_columns, rows)
    _write_workbook(config.output_xlsx, columns, rows)
    return 0


def _load_schema_columns(schema_file: Path | None) -> list[str] | None:
    if schema_file is None:
        return None
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file does not exist: {schema_file}")

    module_name = f"email_analyzer_schema_export_{schema_file.stem.replace('-', '_')}"
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

    model_fields = getattr(schema_class, "model_fields", None)
    if not isinstance(model_fields, dict):
        raise ValueError(f"mySchema must provide model_fields: {schema_file}")

    columns: list[str] = []
    for field_name, field_info in model_fields.items():
        alias = getattr(field_info, "alias", None)
        columns.append(alias or field_name)
    return columns


def _load_rows(input_jsonl: Path) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    with input_jsonl.open("r", encoding="utf-8", newline="") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            record_text = raw_line.rstrip("\r\n")
            if not record_text.strip():
                continue

            try:
                record = json.loads(record_text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_number} of {input_jsonl}: {exc.msg}"
                ) from exc

            custom_id = record.get("custom_id")
            if not isinstance(custom_id, str) or not custom_id:
                raise ValueError(
                    f"Line {line_number} of {input_jsonl} is missing a string custom_id"
                )

            response = record.get("response")
            if not isinstance(response, dict):
                raise ValueError(
                    f"Line {line_number} for {custom_id} is missing a response object"
                )

            status_code = response.get("status_code")
            if status_code != 200:
                raise ValueError(
                    f"Line {line_number} for {custom_id} has status_code {status_code}; "
                    "convert a successful batch_output.jsonl file"
                )

            response_body = response.get("body")
            if not isinstance(response_body, dict):
                raise ValueError(
                    f"Line {line_number} for {custom_id} is missing a response body"
                )

            output_text = _extract_output_text(
                response_body, line_number=line_number, custom_id=custom_id
            )

            try:
                parsed_output = json.loads(output_text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Structured output for {custom_id} on line {line_number} is not valid JSON: {exc.msg}"
                ) from exc

            if not isinstance(parsed_output, dict):
                raise ValueError(
                    f"Structured output for {custom_id} on line {line_number} must be a JSON object"
                )

            rows.append((custom_id, parsed_output))

    return rows


def _extract_output_text(
    response_body: dict[str, Any], *, line_number: int, custom_id: str
) -> str:
    output_items = response_body.get("output")
    if not isinstance(output_items, list):
        raise ValueError(
            f"Line {line_number} for {custom_id} is missing response.body.output"
        )

    fragments: list[str] = []
    for output_item in output_items:
        if not isinstance(output_item, dict):
            continue
        content_items = output_item.get("content")
        if not isinstance(content_items, list):
            continue
        for content_item in content_items:
            if not isinstance(content_item, dict):
                continue
            if content_item.get("type") != "output_text":
                continue
            text = content_item.get("text")
            if isinstance(text, str):
                fragments.append(text)

    if not fragments:
        raise ValueError(
            f"Line {line_number} for {custom_id} does not contain any output_text content"
        )

    return "".join(fragments)


def _resolve_columns(
    schema_columns: list[str] | None,
    rows: list[tuple[str, dict[str, Any]]],
) -> list[str]:
    seen_columns: dict[str, None] = {}
    if schema_columns is not None:
        for column in schema_columns:
            seen_columns.setdefault(column, None)

    for _, payload in rows:
        for key in payload:
            seen_columns.setdefault(key, None)

    return ["filename", *seen_columns]


def _write_workbook(
    output_xlsx: Path,
    columns: list[str],
    rows: list[tuple[str, dict[str, Any]]],
) -> None:
    output_xlsx.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Batch Output"
    worksheet.append(columns)

    for custom_id, payload in rows:
        worksheet.append(
            [
                custom_id,
                *(_coerce_cell_value(payload.get(column)) for column in columns[1:]),
            ]
        )

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    workbook.save(output_xlsx)


def _coerce_cell_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)
