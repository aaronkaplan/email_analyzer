from __future__ import annotations

from email_analyzer.language import detect_language


def test_detect_language_returns_expected_shape() -> None:
    result = detect_language("This is a simple English sentence used for language detection.")
    assert result is not None
    assert result["code"] == "en"
    assert result["name"] == "english"
    assert isinstance(result["confidence"], float)


def test_detect_language_skips_very_short_text() -> None:
    assert detect_language("short text") is None
