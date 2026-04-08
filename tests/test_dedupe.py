from __future__ import annotations

from email.message import Message

from email_analyzer.dedupe import (
    choose_canonical_body,
    filter_duplicate_body_representations,
    is_attachment_like,
    is_body_like,
)
from email_analyzer.models import PartAnalysis


def _make_part(
    *,
    path: str = "1.1",
    content_type: str = "text/plain",
    classification: str = "body_candidate",
    visible_text: str | None = None,
    normalized_text: str | None = None,
    filename: str | None = None,
    parent_content_types: list[str] | None = None,
) -> PartAnalysis:
    return PartAnalysis(
        message_part=Message(),
        path=path,
        parent_content_types=parent_content_types or [],
        content_type=content_type,
        content_disposition=None,
        filename=filename,
        content_id=None,
        charset="utf-8",
        classification=classification,
        is_multipart=False,
        visible_text=visible_text,
        normalized_text=normalized_text,
    )


# --- choose_canonical_body ---


def test_choose_canonical_body_prefers_plain_text() -> None:
    # Plain text must be at least max(80, 40% of HTML length) chars to be preferred
    body_text = "Hello world, " * 10  # 130 chars
    plain = _make_part(
        path="1.1",
        content_type="text/plain",
        visible_text=body_text,
        normalized_text=body_text.lower(),
    )
    html = _make_part(
        path="1.2",
        content_type="text/html",
        classification="alternative_body",
        visible_text=body_text,
        normalized_text=body_text.lower(),
        parent_content_types=["multipart/alternative"],
    )
    result = choose_canonical_body([plain, html])
    assert result is not None
    assert result.path == "1.1"


def test_choose_canonical_body_returns_html_when_plain_is_too_short() -> None:
    plain = _make_part(
        path="1.1",
        content_type="text/plain",
        visible_text="Hi",
        normalized_text="hi",
    )
    html = _make_part(
        path="1.2",
        content_type="text/html",
        classification="alternative_body",
        visible_text="Hello world, this is a longer HTML body with real content.",
        normalized_text="hello world, this is a longer html body with real content.",
        parent_content_types=["multipart/alternative"],
    )
    result = choose_canonical_body([plain, html])
    assert result is not None
    assert result.path == "1.2"


def test_choose_canonical_body_returns_none_when_no_candidates() -> None:
    attachment = _make_part(
        classification="attachment",
        visible_text="some text",
        normalized_text="some text",
    )
    assert choose_canonical_body([attachment]) is None


def test_choose_canonical_body_returns_none_for_empty_list() -> None:
    assert choose_canonical_body([]) is None


def test_choose_canonical_body_skips_parts_without_visible_text() -> None:
    empty = _make_part(path="1.1", visible_text=None, normalized_text=None)
    with_text = _make_part(
        path="1.2",
        content_type="text/html",
        classification="alternative_body",
        visible_text="Content here",
        normalized_text="content here",
    )
    result = choose_canonical_body([empty, with_text])
    assert result is not None
    assert result.path == "1.2"


# --- filter_duplicate_body_representations ---


def test_filter_duplicates_drops_exact_hash_match() -> None:
    canonical = _make_part(
        path="1.1",
        content_type="text/plain",
        visible_text="Hello world",
        normalized_text="hello world",
    )
    duplicate = _make_part(
        path="1.2",
        content_type="text/html",
        classification="alternative_body",
        visible_text="Hello world",
        normalized_text="hello world",
        parent_content_types=["multipart/alternative"],
    )
    dropped = filter_duplicate_body_representations([canonical, duplicate], canonical)
    assert "1.2" in dropped
    assert dropped["1.2"].reason == "duplicate_body_representation"


def test_filter_duplicates_keeps_message_rfc822() -> None:
    canonical = _make_part(
        path="1.1",
        visible_text="Hello world",
        normalized_text="hello world",
    )
    rfc822 = _make_part(
        path="1.2",
        content_type="message/rfc822",
        classification="attached_message",
        visible_text="Hello world",
        normalized_text="hello world",
    )
    dropped = filter_duplicate_body_representations([canonical, rfc822], canonical)
    assert "1.2" not in dropped


def test_filter_duplicates_returns_empty_when_no_canonical() -> None:
    part = _make_part(visible_text="Hello", normalized_text="hello")
    assert filter_duplicate_body_representations([part], None) == {}


def test_filter_duplicates_returns_empty_when_canonical_has_no_text() -> None:
    canonical = _make_part(visible_text="", normalized_text="")
    other = _make_part(
        path="1.2",
        classification="alternative_body",
        visible_text="Hello",
        normalized_text="hello",
    )
    assert filter_duplicate_body_representations([canonical, other], canonical) == {}


def test_filter_duplicates_records_similarity() -> None:
    canonical = _make_part(
        path="1.1",
        visible_text="Hello world",
        normalized_text="hello world",
    )
    dup = _make_part(
        path="1.2",
        content_type="text/html",
        classification="alternative_body",
        visible_text="Hello world",
        normalized_text="hello world",
        parent_content_types=["multipart/alternative"],
    )
    dropped = filter_duplicate_body_representations([canonical, dup], canonical)
    assert dropped["1.2"].similarity == 1.0


def test_filter_duplicates_drops_high_similarity_in_alternative() -> None:
    canonical = _make_part(
        path="1.1",
        visible_text="Hello world, how are you doing today?",
        normalized_text="hello world, how are you doing today?",
    )
    near_dup = _make_part(
        path="1.2",
        content_type="text/html",
        classification="alternative_body",
        visible_text="Hello world, how are you doing today?",
        normalized_text="hello world, how are you doing today!",
        parent_content_types=["multipart/alternative"],
    )
    dropped = filter_duplicate_body_representations([canonical, near_dup], canonical)
    assert "1.2" in dropped


def test_filter_duplicates_keeps_dissimilar_parts() -> None:
    canonical = _make_part(
        path="1.1",
        visible_text="Hello world",
        normalized_text="hello world",
    )
    different = _make_part(
        path="1.2",
        content_type="text/plain",
        classification="body_candidate",
        visible_text="Completely different content with no overlap whatsoever",
        normalized_text="completely different content with no overlap whatsoever",
    )
    dropped = filter_duplicate_body_representations([canonical, different], canonical)
    assert "1.2" not in dropped


# --- is_body_like / is_attachment_like ---


def test_is_body_like_true_for_body_candidate_with_text() -> None:
    part = _make_part(classification="body_candidate", visible_text="text")
    assert is_body_like(part) is True


def test_is_body_like_true_for_alternative_body_with_text() -> None:
    part = _make_part(classification="alternative_body", visible_text="text")
    assert is_body_like(part) is True


def test_is_body_like_false_for_attachment() -> None:
    part = _make_part(classification="attachment", visible_text="text")
    assert is_body_like(part) is False


def test_is_body_like_false_without_visible_text() -> None:
    part = _make_part(classification="body_candidate", visible_text=None)
    assert is_body_like(part) is False


def test_is_attachment_like_true_for_attachment() -> None:
    part = _make_part(classification="attachment")
    assert is_attachment_like(part) is True


def test_is_attachment_like_true_for_text_attachment() -> None:
    part = _make_part(classification="text_attachment")
    assert is_attachment_like(part) is True


def test_is_attachment_like_true_for_attached_message() -> None:
    part = _make_part(classification="attached_message")
    assert is_attachment_like(part) is True


def test_is_attachment_like_false_for_body_candidate() -> None:
    part = _make_part(classification="body_candidate")
    assert is_attachment_like(part) is False
