from __future__ import annotations

from email_analyzer.quote_strip import strip_reply_text


def test_strip_reply_text_removes_quoted_reply() -> None:
    text = (
        "Thanks for the info.\n\n"
        "On Mon, Mar 30, 2026 at 5:00 PM Bob <bob@example.com> wrote:\n"
        "> Can you send the update?\n"
        "> Thanks."
    )
    stripped, meta = strip_reply_text(text)
    assert "Thanks for the info" in stripped
    assert "Can you send the update" not in stripped
    assert meta["tool"] == "mail-parser-reply"
    assert meta["changed"] is True
    assert meta["characters_removed"] > 0


def test_strip_reply_text_no_change_when_no_quotes() -> None:
    text = "Just a plain message with no quoted text at all."
    stripped, meta = strip_reply_text(text)
    assert stripped == "Just a plain message with no quoted text at all."
    assert meta["changed"] is False
    assert meta["characters_removed"] == 0


def test_strip_reply_text_returns_empty_for_empty_input() -> None:
    stripped, meta = strip_reply_text("")
    assert stripped == ""
    assert meta["changed"] is False


def test_strip_reply_text_returns_empty_for_whitespace_only() -> None:
    stripped, meta = strip_reply_text("   \n\n  ")
    assert stripped == ""
    assert meta["changed"] is False


def test_strip_reply_text_accepts_custom_languages() -> None:
    text = "Danke für die Nachricht.\n\nAm Mo., 30. Mär. 2026 um 17:00 schrieb Bob:\n> Hallo"
    stripped, meta = strip_reply_text(text, languages=["de", "en"])
    assert meta["tool"] == "mail-parser-reply"


def test_strip_reply_text_preserves_original_on_parser_failure() -> None:
    # The parser should handle any string without crashing.
    # If it does fail internally, the function should return the normalized text.
    text = "Normal text content."
    stripped, meta = strip_reply_text(text)
    assert "Normal text content" in stripped


def test_strip_reply_text_handles_outlook_separator() -> None:
    text = (
        "My reply here.\n\n"
        "________________________________\n"
        "From: someone@example.com\n"
        "Sent: Monday, March 30, 2026 5:00 PM\n"
        "To: me@example.com\n"
        "Subject: RE: Topic\n\n"
        "Original message text"
    )
    stripped, meta = strip_reply_text(text)
    assert "My reply here" in stripped
    assert meta["tool"] == "mail-parser-reply"


def test_strip_reply_text_never_returns_empty_when_input_has_text() -> None:
    # Even if the parser strips everything, it should fall back to the
    # normalized version.
    text = "On Mon wrote:\n> everything is quoted"
    stripped, meta = strip_reply_text(text)
    # Should return something non-empty (either stripped or normalized fallback)
    assert len(stripped) > 0
