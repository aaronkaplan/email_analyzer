from __future__ import annotations

from email.message import Message

from email_analyzer.models import (
    AttachmentSummary,
    DroppedPart,
    PartAnalysis,
    ProcessedEmail,
    Snippet,
)


# --- PartAnalysis ---


def test_part_analysis_decoded_text_size_with_text() -> None:
    part = PartAnalysis(
        message_part=Message(),
        path="1.1",
        parent_content_types=[],
        content_type="text/plain",
        content_disposition=None,
        filename=None,
        content_id=None,
        charset="utf-8",
        classification="body_candidate",
        is_multipart=False,
        visible_text="Hello world",
    )
    assert part.decoded_text_size == 11


def test_part_analysis_decoded_text_size_without_text() -> None:
    part = PartAnalysis(
        message_part=Message(),
        path="1.1",
        parent_content_types=[],
        content_type="text/plain",
        content_disposition=None,
        filename=None,
        content_id=None,
        charset="utf-8",
        classification="body_candidate",
        is_multipart=False,
    )
    assert part.decoded_text_size == 0


def test_part_analysis_inventory_record() -> None:
    part = PartAnalysis(
        message_part=Message(),
        path="1.2",
        parent_content_types=["multipart/mixed"],
        content_type="text/html",
        content_disposition="inline",
        filename="body.html",
        content_id="<cid123>",
        charset="utf-8",
        classification="alternative_body",
        is_multipart=False,
        decoded_byte_size=500,
        visible_text="Hello",
        charset_used="utf-8",
        charset_source="declared",
        extraction_method="selectolax",
    )
    record = part.inventory_record()
    assert record["path"] == "1.2"
    assert record["content_type"] == "text/html"
    assert record["filename"] == "body.html"
    assert record["decoded_byte_size"] == 500
    assert record["decoded_text_size"] == 5
    assert record["extraction_method"] == "selectolax"
    assert record["charset_used"] == "utf-8"
    assert record["charset_source"] == "declared"
    assert record["classification"] == "alternative_body"
    assert record["parent_content_types"] == ["multipart/mixed"]
    assert isinstance(record["parent_content_types"], list)


def test_part_analysis_inventory_record_copies_parent_list() -> None:
    parents = ["multipart/mixed"]
    part = PartAnalysis(
        message_part=Message(),
        path="1",
        parent_content_types=parents,
        content_type="text/plain",
        content_disposition=None,
        filename=None,
        content_id=None,
        charset=None,
        classification="body_candidate",
        is_multipart=False,
    )
    record = part.inventory_record()
    # Mutating the record list should not affect the original
    record["parent_content_types"].append("extra")
    assert len(part.parent_content_types) == 1


# --- Snippet ---


def test_snippet_creation() -> None:
    snippet = Snippet(
        snippet_id="canonical_body",
        kind="canonical_body",
        source_part_path="1.1",
        content_type="text/plain",
        filename=None,
        text="Hello world",
        language={"code": "en", "name": "english", "confidence": 0.99},
        characters=11,
        token_estimate=3,
    )
    assert snippet.snippet_id == "canonical_body"
    assert snippet.characters == 11
    assert snippet.metadata == {}


def test_snippet_with_metadata() -> None:
    snippet = Snippet(
        snippet_id="attachment_0",
        kind="attachment",
        source_part_path="1.3",
        content_type="text/plain",
        filename="notes.txt",
        text="Notes content",
        language=None,
        characters=13,
        token_estimate=4,
        metadata={"original_size": 100},
    )
    assert snippet.metadata["original_size"] == 100


# --- DroppedPart ---


def test_dropped_part_creation() -> None:
    dp = DroppedPart(
        source_part_path="1.2",
        content_type="text/html",
        filename="body.html",
        classification="alternative_body",
        reason="duplicate_body_representation",
        similarity=1.0,
        details={"canonical_part_path": "1.1"},
    )
    assert dp.reason == "duplicate_body_representation"
    assert dp.similarity == 1.0
    assert dp.details["canonical_part_path"] == "1.1"


def test_dropped_part_defaults() -> None:
    dp = DroppedPart(
        source_part_path="1.2",
        content_type="text/html",
        filename=None,
        classification="body_candidate",
        reason="test",
    )
    assert dp.similarity is None
    assert dp.details == {}


# --- AttachmentSummary ---


def test_attachment_summary_creation() -> None:
    att = AttachmentSummary(
        source_part_path="1.3",
        filename="notes.txt",
        content_type="text/plain",
        classification="text_attachment",
        kept=True,
        reason=None,
        text_extracted=True,
        char_count=100,
    )
    assert att.kept is True
    assert att.char_count == 100


# --- ProcessedEmail ---


def test_processed_email_to_dict() -> None:
    canonical = Snippet(
        snippet_id="canonical_body",
        kind="canonical_body",
        source_part_path="1.1",
        content_type="text/plain",
        filename=None,
        text="Hello",
        language=None,
        characters=5,
        token_estimate=2,
    )
    pe = ProcessedEmail(
        schema_version="email_analyzer.processed.v1",
        email_id="test.eml",
        source_filename="test.eml",
        headers={"subject": "Test"},
        parser_defects=[],
        canonical_body=canonical,
        kept_snippets=[],
        dropped_parts=[],
        attachments=[],
        part_inventory=[],
        timings_ms={"parse": 10.0},
        total_duration_ms=10.0,
        stats={"source_bytes": 100},
    )
    d = pe.to_dict()
    assert d["email_id"] == "test.eml"
    assert d["schema_version"] == "email_analyzer.processed.v1"
    assert d["canonical_body"]["text"] == "Hello"
    assert isinstance(d["timings_ms"], dict)
    assert d["total_duration_ms"] == 10.0


def test_processed_email_to_dict_with_none_canonical() -> None:
    pe = ProcessedEmail(
        schema_version="email_analyzer.processed.v1",
        email_id="test.eml",
        source_filename="test.eml",
        headers={},
        parser_defects=[],
        canonical_body=None,
        kept_snippets=[],
        dropped_parts=[],
        attachments=[],
        part_inventory=[],
        timings_ms={},
        total_duration_ms=0.0,
        stats={},
    )
    d = pe.to_dict()
    assert d["canonical_body"] is None
