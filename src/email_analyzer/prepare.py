from __future__ import annotations

import json
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from .config import ERROR_SCHEMA_VERSION, PrepareConfig, SCHEMA_VERSION
from .dedupe import (
    choose_canonical_body,
    filter_duplicate_body_representations,
    is_attachment_like,
    is_body_like,
)
from .html import normalize_for_dedupe
from .language import detect_language
from .logging_utils import (
    configure_worker_logging,
    get_logger,
    log_event,
    start_logging,
    stop_logging,
)
from .batch_submitter_common import write_json_atomic
from .metrics import (
    aggregate_step_metrics,
    estimate_token_count,
    measure_step,
    total_duration_ms,
)
from .mime import (
    build_part_inventory,
    collect_parser_defects,
    decode_parts,
    is_textual_part,
    parse_email_bytes,
    select_headers,
)
from .models import (
    AttachmentSummary,
    DroppedPart,
    PartAnalysis,
    ProcessedEmail,
    Snippet,
)
from .quote_strip import strip_reply_text


def run_prepare(
    config: PrepareConfig,
    executor_factory: type[ProcessPoolExecutor]
    | type[ThreadPoolExecutor] = ProcessPoolExecutor,
) -> int:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.logs_dir.mkdir(parents=True, exist_ok=True)

    logging_runtime = start_logging(config.logs_dir)
    logger = get_logger()

    input_files = sorted(path for path in config.input_dir.iterdir() if path.is_file())
    log_event(
        logger,
        "Discovered input files",
        step="discover_input",
        action="scan_directory",
        status="success",
        duration_ms=0.0,
        input_dir=str(config.input_dir),
        file_count=len(input_files),
    )

    summaries: list[dict[str, Any]] = []
    file_summary_path = config.logs_dir / "file_summary.jsonl"
    file_summary_path.write_text("", encoding="utf-8")

    try:
        if not input_files:
            (config.logs_dir / "step_summary.json").write_text("{}\n", encoding="utf-8")
            return 0

        executor_kwargs: dict[str, Any] = {"max_workers": max(1, config.workers)}
        if executor_factory is ProcessPoolExecutor:
            executor_kwargs.update(
                initializer=configure_worker_logging,
                initargs=(logging_runtime.queue,),
            )

        with executor_factory(**executor_kwargs) as executor:
            future_map = {
                executor.submit(process_email_file, input_path, config): input_path
                for input_path in input_files
            }

            for future in as_completed(future_map):
                summary = future.result()
                summaries.append(summary)
                with file_summary_path.open(
                    "a", encoding="utf-8"
                ) as file_summary_handle:
                    file_summary_handle.write(
                        json.dumps(summary, ensure_ascii=False, sort_keys=True) + "\n"
                    )

        step_summary = aggregate_step_metrics(summaries)
        (config.logs_dir / "step_summary.json").write_text(
            json.dumps(step_summary, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
    finally:
        stop_logging(logging_runtime)

    return 0


def process_email_file(input_path: Path, config: PrepareConfig) -> dict[str, Any]:
    logger = get_logger()
    email_id = input_path.name
    timings: dict[str, float] = {}
    total_start_ns = perf_counter_ns()
    source_bytes = input_path.stat().st_size
    output_path = config.output_dir / f"{email_id}.json"
    error_path = config.output_dir / f"{email_id}.error.json"

    try:
        parsed, duration_ms = measure_step(
            "parse_source", timings, _parse_source, input_path
        )
        _raw_bytes, message, headers, parser_defects = parsed
        log_event(
            logger,
            "Parsed raw email",
            step="parse_source",
            action="parse_email",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            source_bytes=source_bytes,
            root_content_type=message.get_content_type(),
            parser_defect_count=len(parser_defects),
        )

        parts, duration_ms = measure_step(
            "inventory_parts", timings, build_part_inventory, message
        )
        textish_count = sum(1 for part in parts if is_textual_part(part))
        log_event(
            logger,
            "Built MIME part inventory",
            step="inventory_parts",
            action="inventory_parts",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            part_count=len(parts),
            text_part_count=textish_count,
        )

        parts, duration_ms = measure_step(
            "decode_and_canonicalize", timings, decode_parts, parts
        )
        charset_fallback_count = sum(
            1 for part in parts if part.charset_source == "charset-normalizer"
        )
        decoded_text_part_count = sum(1 for part in parts if part.visible_text)
        log_event(
            logger,
            "Decoded text-bearing parts",
            step="decode_and_canonicalize",
            action="decode_parts",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            decoded_text_part_count=decoded_text_part_count,
            charset_fallback_count=charset_fallback_count,
        )

        canonical_body, duration_ms = measure_step(
            "choose_canonical_body", timings, choose_canonical_body, parts
        )
        log_event(
            logger,
            "Selected canonical body",
            step="choose_canonical_body",
            action="choose_body",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            canonical_part_path=canonical_body.path if canonical_body else None,
            canonical_content_type=canonical_body.content_type
            if canonical_body
            else None,
        )

        dropped_by_path, duration_ms = measure_step(
            "filter_duplicate_body_representations",
            timings,
            filter_duplicate_body_representations,
            parts,
            canonical_body,
        )
        log_event(
            logger,
            "Filtered duplicate body representations",
            step="filter_duplicate_body_representations",
            action="dedupe_body_renderings",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            dropped_count=len(dropped_by_path),
        )
        for dropped_part in dropped_by_path.values():
            log_event(
                logger,
                "Dropped duplicate body representation",
                step="filter_duplicate_body_representations",
                action="drop_part",
                status="success",
                duration_ms=0.0,
                email_id=email_id,
                source_filename=email_id,
                part_path=dropped_part.source_part_path,
                attachment_filename=dropped_part.filename,
                content_type=dropped_part.content_type,
                reason=dropped_part.reason,
                similarity=dropped_part.similarity,
            )

        quote_result, duration_ms = measure_step(
            "strip_quotes_and_signatures",
            timings,
            _strip_canonical_body,
            canonical_body,
        )
        canonical_body, quote_metadata = quote_result
        log_event(
            logger,
            "Stripped reply history from canonical body",
            step="strip_quotes_and_signatures",
            action="strip_quotes",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            canonical_part_path=canonical_body.path if canonical_body else None,
            quote_changed=quote_metadata.get("changed"),
            characters_removed=quote_metadata.get("characters_removed"),
        )

        triage_result, duration_ms = measure_step(
            "triage_attachments",
            timings,
            _triage_parts,
            parts,
            dropped_by_path,
        )
        body_parts, attachment_parts, attachment_summaries, dropped_by_path = (
            triage_result
        )
        dropped_attachment_count = sum(
            1 for summary in attachment_summaries if not summary.kept
        )
        log_event(
            logger,
            "Triaged attachment parts",
            step="triage_attachments",
            action="triage_attachments",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            kept_attachment_count=sum(
                1 for summary in attachment_summaries if summary.kept
            ),
            dropped_attachment_count=dropped_attachment_count,
        )
        for dropped_part in dropped_by_path.values():
            if dropped_part.reason == "duplicate_body_representation":
                continue
            log_event(
                logger,
                "Dropped part during triage",
                step="triage_attachments",
                action="drop_part",
                status="success",
                duration_ms=0.0,
                email_id=email_id,
                source_filename=email_id,
                part_path=dropped_part.source_part_path,
                attachment_filename=dropped_part.filename,
                content_type=dropped_part.content_type,
                reason=dropped_part.reason,
            )

        snippets, duration_ms = measure_step(
            "detect_language",
            timings,
            _build_snippets,
            body_parts,
            attachment_parts,
            canonical_body,
        )
        log_event(
            logger,
            "Built snippet payloads and detected language",
            step="detect_language",
            action="detect_language",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            snippet_count=len(snippets),
            language_detected_count=sum(1 for snippet in snippets if snippet.language),
        )

        processed_email, duration_ms = measure_step(
            "rank_and_pack",
            timings,
            _build_processed_email,
            email_id,
            headers,
            parser_defects,
            parts,
            snippets,
            dropped_by_path,
            attachment_summaries,
            source_bytes,
        )
        log_event(
            logger,
            "Packed processed email artifact",
            step="rank_and_pack",
            action="pack_email",
            status="success",
            duration_ms=duration_ms,
            email_id=email_id,
            source_filename=email_id,
            kept_snippet_count=len(processed_email.kept_snippets),
            dropped_part_count=len(processed_email.dropped_parts),
        )

        total_ms = total_duration_ms(total_start_ns)
        processed_email.timings_ms = dict(timings)
        processed_email.total_duration_ms = total_ms

        write_start_ns = perf_counter_ns()
        _write_processed_email(output_path, processed_email)
        write_duration_ms = round((perf_counter_ns() - write_start_ns) / 1_000_000, 3)
        timings["write_output"] = write_duration_ms
        log_event(
            logger,
            "Wrote processed email artifact",
            step="write_output",
            action="write_output",
            status="success",
            duration_ms=write_duration_ms,
            email_id=email_id,
            source_filename=email_id,
            output_path=str(output_path),
        )

        summary = {
            "email_id": email_id,
            "source_filename": email_id,
            "status": "success",
            "output_path": str(output_path),
            "total_duration_ms": total_ms,
            "timings_ms": dict(timings),
            "stats": dict(processed_email.stats),
        }
        return summary
    except Exception as exc:
        total_ms = total_duration_ms(total_start_ns)
        error_payload = {
            "schema_version": ERROR_SCHEMA_VERSION,
            "email_id": email_id,
            "source_filename": email_id,
            "error_type": exc.__class__.__name__,
            "error_message": str(exc),
            "timings_ms": timings,
            "total_duration_ms": total_ms,
            "traceback": traceback.format_exc(),
        }
        write_json_atomic(error_path, error_payload)
        log_event(
            logger,
            "Failed to process email",
            level=logging.ERROR,
            step="process_email",
            action="process_email",
            status="error",
            duration_ms=total_ms,
            email_id=email_id,
            source_filename=email_id,
            error_type=exc.__class__.__name__,
            output_path=str(error_path),
        )
        return {
            "email_id": email_id,
            "source_filename": email_id,
            "status": "error",
            "output_path": str(error_path),
            "total_duration_ms": total_ms,
            "timings_ms": dict(timings),
            "stats": {"source_bytes": source_bytes},
            "error_type": exc.__class__.__name__,
            "error_message": str(exc),
        }


def _parse_source(input_path: Path) -> tuple[bytes, Any, dict[str, Any], list[str]]:
    raw_bytes = input_path.read_bytes()
    message = parse_email_bytes(raw_bytes)
    headers = select_headers(message)
    defects = collect_parser_defects(message)
    return raw_bytes, message, headers, defects


def _strip_canonical_body(canonical_body: PartAnalysis | None):
    if canonical_body is None or not canonical_body.visible_text:
        return canonical_body, {
            "tool": "mail-parser-reply",
            "changed": False,
            "characters_removed": 0,
        }

    stripped_text, metadata = strip_reply_text(canonical_body.visible_text)
    canonical_body.visible_text = stripped_text
    canonical_body.normalized_text = normalize_for_dedupe(stripped_text)
    return canonical_body, metadata


def _triage_parts(
    parts: list[PartAnalysis],
    dropped_by_path: dict[str, DroppedPart],
) -> tuple[
    list[PartAnalysis],
    list[PartAnalysis],
    list[AttachmentSummary],
    dict[str, DroppedPart],
]:
    body_parts = [
        part
        for part in parts
        if is_body_like(part) and part.path not in dropped_by_path
    ]
    attachment_parts = []
    attachment_summaries: list[AttachmentSummary] = []

    for part in parts:
        if not is_attachment_like(part):
            continue

        existing_drop = dropped_by_path.get(part.path)
        if existing_drop is not None:
            attachment_summaries.append(
                AttachmentSummary(
                    source_part_path=part.path,
                    filename=part.filename,
                    content_type=part.content_type,
                    classification=part.classification,
                    kept=False,
                    reason=existing_drop.reason,
                    text_extracted=bool(part.visible_text),
                    char_count=len(part.visible_text or ""),
                )
            )
            continue

        keep, reason = _should_keep_attachment(part)
        if keep:
            attachment_parts.append(part)
        else:
            dropped_by_path[part.path] = DroppedPart(
                source_part_path=part.path,
                content_type=part.content_type,
                filename=part.filename,
                classification=part.classification,
                reason=reason,
            )

        attachment_summaries.append(
            AttachmentSummary(
                source_part_path=part.path,
                filename=part.filename,
                content_type=part.content_type,
                classification=part.classification,
                kept=keep,
                reason=None if keep else reason,
                text_extracted=bool(part.visible_text) if keep else False,
                char_count=len(part.visible_text or "") if keep else 0,
            )
        )

    return body_parts, attachment_parts, attachment_summaries, dropped_by_path


def _should_keep_attachment(part: PartAnalysis) -> tuple[bool, str]:
    if part.content_type == "message/rfc822":
        return True, "keep_attached_message"
    if part.visible_text and part.visible_text.strip():
        return True, "keep_text_attachment"
    if part.content_type.startswith("text/"):
        return False, "empty_attachment_text"
    return False, "unsupported_attachment_type"


def _build_snippets(
    body_parts: list[PartAnalysis],
    attachment_parts: list[PartAnalysis],
    canonical_body: PartAnalysis | None,
) -> list[Snippet]:
    snippets: list[Snippet] = []

    if canonical_body is not None and canonical_body.visible_text:
        snippets.append(
            _part_to_snippet(
                canonical_body, kind="canonical_body", snippet_id="canonical_body"
            )
        )

    additional_body_parts = sorted(
        [
            part
            for part in body_parts
            if canonical_body is None or part.path != canonical_body.path
        ],
        key=lambda part: len(part.visible_text or ""),
        reverse=True,
    )
    for index, part in enumerate(additional_body_parts, start=1):
        if not part.visible_text:
            continue
        snippets.append(
            _part_to_snippet(
                part, kind="additional_body", snippet_id=f"additional_body_{index}"
            )
        )

    sorted_attachment_parts = sorted(
        [part for part in attachment_parts if part.visible_text],
        key=lambda part: len(part.visible_text or ""),
        reverse=True,
    )
    for index, part in enumerate(sorted_attachment_parts, start=1):
        snippets.append(
            _part_to_snippet(part, kind="attachment", snippet_id=f"attachment_{index}")
        )

    return snippets


def _part_to_snippet(part: PartAnalysis, kind: str, snippet_id: str) -> Snippet:
    text = part.visible_text or ""
    language = detect_language(text)
    return Snippet(
        snippet_id=snippet_id,
        kind=kind,
        source_part_path=part.path,
        content_type=part.content_type,
        filename=part.filename,
        text=text,
        language=language,
        characters=len(text),
        token_estimate=estimate_token_count(text),
        metadata={
            "classification": part.classification,
            "charset_used": part.charset_used,
            "charset_source": part.charset_source,
        },
    )


def _build_processed_email(
    email_id: str,
    headers: dict[str, Any],
    parser_defects: list[str],
    parts: list[PartAnalysis],
    snippets: list[Snippet],
    dropped_by_path: dict[str, DroppedPart],
    attachment_summaries: list[AttachmentSummary],
    source_bytes: int,
) -> ProcessedEmail:
    canonical_snippet = next(
        (snippet for snippet in snippets if snippet.kind == "canonical_body"), None
    )
    dropped_parts = sorted(
        dropped_by_path.values(), key=lambda item: item.source_part_path
    )
    estimated_total_tokens = sum(snippet.token_estimate for snippet in snippets)

    return ProcessedEmail(
        schema_version=SCHEMA_VERSION,
        email_id=email_id,
        source_filename=email_id,
        headers=headers,
        parser_defects=parser_defects,
        canonical_body=canonical_snippet,
        kept_snippets=snippets,
        dropped_parts=dropped_parts,
        attachments=attachment_summaries,
        part_inventory=[part.inventory_record() for part in parts],
        timings_ms={},
        total_duration_ms=0.0,
        stats={
            "source_bytes": source_bytes,
            "kept_snippet_count": len(snippets),
            "dropped_part_count": len(dropped_parts),
            "attachment_count": len(attachment_summaries),
            "estimated_total_tokens": estimated_total_tokens,
        },
    )


def _write_processed_email(output_path: Path, processed_email: ProcessedEmail) -> None:
    write_json_atomic(output_path, processed_email.to_dict())
