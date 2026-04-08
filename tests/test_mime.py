from __future__ import annotations

from pathlib import Path

from email_analyzer.mime import (
    build_part_inventory,
    collect_parser_defects,
    decode_parts,
    is_textual_part,
    normalized_text_hash,
    parse_email_bytes,
    select_headers,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


# --- parse_email_bytes ---


def test_parse_email_bytes_returns_email_message() -> None:
    raw = b"From: alice@example.com\r\nSubject: Test\r\n\r\nBody text"
    msg = parse_email_bytes(raw)
    assert msg["Subject"] == "Test"
    assert msg["From"] == "alice@example.com"


def test_parse_email_bytes_handles_multipart() -> None:
    raw = _load_fixture("alternative_duplicate.eml")
    msg = parse_email_bytes(raw)
    assert msg.is_multipart()
    assert msg.get_content_type() == "multipart/mixed"


# --- select_headers ---


def test_select_headers_extracts_known_headers() -> None:
    raw = (
        b"From: alice@example.com\r\n"
        b"To: bob@example.com\r\n"
        b"Subject: Hello\r\n"
        b"Date: Wed, 1 Apr 2026 10:00:00 +0000\r\n"
        b"Message-ID: <test@example.com>\r\n"
        b"\r\n"
        b"Body"
    )
    msg = parse_email_bytes(raw)
    headers = select_headers(msg)
    assert headers["subject"] == "Hello"
    assert headers["from"] == "alice@example.com"
    assert headers["to"] == "bob@example.com"
    assert "message-id" in headers


def test_select_headers_skips_missing_headers() -> None:
    raw = b"From: alice@example.com\r\nSubject: Hi\r\n\r\nBody"
    msg = parse_email_bytes(raw)
    headers = select_headers(msg)
    assert "cc" not in headers
    assert "bcc" not in headers
    assert "in-reply-to" not in headers


def test_select_headers_returns_list_for_multiple_values() -> None:
    raw = (
        b"From: alice@example.com\r\n"
        b"To: bob@example.com\r\n"
        b"To: carol@example.com\r\n"
        b"Subject: Multi-to\r\n"
        b"\r\n"
        b"Body"
    )
    msg = parse_email_bytes(raw)
    headers = select_headers(msg)
    assert isinstance(headers["to"], list)
    assert len(headers["to"]) == 2


# --- collect_parser_defects ---


def test_collect_parser_defects_returns_empty_for_valid_email() -> None:
    raw = b"From: alice@example.com\r\nSubject: Test\r\n\r\nBody"
    msg = parse_email_bytes(raw)
    defects = collect_parser_defects(msg)
    assert defects == []


# --- build_part_inventory ---


def test_build_part_inventory_multipart_fixture() -> None:
    raw = _load_fixture("alternative_duplicate.eml")
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    # multipart/mixed -> multipart/alternative (2 children) + body.html + notes.txt
    assert len(parts) >= 5
    content_types = [p.content_type for p in parts]
    assert "multipart/mixed" in content_types
    assert "multipart/alternative" in content_types
    assert "text/plain" in content_types
    assert "text/html" in content_types


def test_build_part_inventory_paths_are_hierarchical() -> None:
    raw = _load_fixture("alternative_duplicate.eml")
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    paths = [p.path for p in parts]
    assert paths[0] == "1"
    assert all("." in p for p in paths[1:])


def test_build_part_inventory_classifies_correctly() -> None:
    raw = _load_fixture("alternative_duplicate.eml")
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    # The notes.txt should be a text_attachment
    text_attachments = [p for p in parts if p.filename == "notes.txt"]
    assert len(text_attachments) == 1
    assert text_attachments[0].classification == "text_attachment"


# --- decode_parts ---


def test_decode_parts_populates_visible_text_for_plain() -> None:
    raw = b"From: a@b.com\r\nSubject: T\r\nContent-Type: text/plain\r\n\r\nHello world"
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    decoded = decode_parts(parts)
    text_parts = [p for p in decoded if p.content_type == "text/plain"]
    assert len(text_parts) == 1
    assert text_parts[0].visible_text is not None
    assert "Hello world" in text_parts[0].visible_text


def test_decode_parts_populates_visible_text_for_html() -> None:
    raw = (
        b"From: a@b.com\r\n"
        b"Subject: T\r\n"
        b"Content-Type: text/html\r\n"
        b"\r\n"
        b"<html><body><p>Hello world</p></body></html>"
    )
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    decoded = decode_parts(parts)
    html_parts = [p for p in decoded if p.content_type == "text/html"]
    assert len(html_parts) == 1
    assert "Hello world" in (html_parts[0].visible_text or "")


def test_decode_parts_sets_decoded_byte_size() -> None:
    raw = b"From: a@b.com\r\nSubject: T\r\nContent-Type: text/plain\r\n\r\nHello"
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    decoded = decode_parts(parts)
    text_parts = [p for p in decoded if p.content_type == "text/plain"]
    assert text_parts[0].decoded_byte_size > 0


def test_decode_parts_sets_normalized_text() -> None:
    raw = b"From: a@b.com\r\nSubject: T\r\nContent-Type: text/plain\r\n\r\nHello World"
    msg = parse_email_bytes(raw)
    parts = build_part_inventory(msg)
    decoded = decode_parts(parts)
    text_parts = [p for p in decoded if p.content_type == "text/plain"]
    assert text_parts[0].normalized_text == "hello world"


# --- is_textual_part ---


def test_is_textual_part_true_for_text_plain() -> None:
    from email.message import Message
    from email_analyzer.models import PartAnalysis

    part = PartAnalysis(
        message_part=Message(),
        path="1",
        parent_content_types=[],
        content_type="text/plain",
        content_disposition=None,
        filename=None,
        content_id=None,
        charset="utf-8",
        classification="body_candidate",
        is_multipart=False,
    )
    assert is_textual_part(part) is True


def test_is_textual_part_true_for_text_html() -> None:
    from email.message import Message
    from email_analyzer.models import PartAnalysis

    part = PartAnalysis(
        message_part=Message(),
        path="1",
        parent_content_types=[],
        content_type="text/html",
        content_disposition=None,
        filename=None,
        content_id=None,
        charset="utf-8",
        classification="body_candidate",
        is_multipart=False,
    )
    assert is_textual_part(part) is True


def test_is_textual_part_true_for_message_rfc822() -> None:
    from email.message import Message
    from email_analyzer.models import PartAnalysis

    part = PartAnalysis(
        message_part=Message(),
        path="1",
        parent_content_types=[],
        content_type="message/rfc822",
        content_disposition=None,
        filename=None,
        content_id=None,
        charset=None,
        classification="attached_message",
        is_multipart=False,
    )
    assert is_textual_part(part) is True


def test_is_textual_part_true_for_txt_extension() -> None:
    from email.message import Message
    from email_analyzer.models import PartAnalysis

    part = PartAnalysis(
        message_part=Message(),
        path="1",
        parent_content_types=[],
        content_type="application/octet-stream",
        content_disposition="attachment",
        filename="data.txt",
        content_id=None,
        charset=None,
        classification="attachment",
        is_multipart=False,
    )
    assert is_textual_part(part) is True


def test_is_textual_part_false_for_image() -> None:
    from email.message import Message
    from email_analyzer.models import PartAnalysis

    part = PartAnalysis(
        message_part=Message(),
        path="1",
        parent_content_types=[],
        content_type="image/png",
        content_disposition="attachment",
        filename="photo.png",
        content_id=None,
        charset=None,
        classification="attachment",
        is_multipart=False,
    )
    assert is_textual_part(part) is False


# --- normalized_text_hash ---


def test_normalized_text_hash_deterministic() -> None:
    h1 = normalized_text_hash("hello world")
    h2 = normalized_text_hash("hello world")
    assert h1 is not None
    assert h1 == h2


def test_normalized_text_hash_differs_for_different_text() -> None:
    h1 = normalized_text_hash("hello")
    h2 = normalized_text_hash("world")
    assert h1 != h2


def test_normalized_text_hash_returns_none_for_empty() -> None:
    assert normalized_text_hash("") is None
    assert normalized_text_hash(None) is None
