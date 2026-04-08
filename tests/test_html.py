from __future__ import annotations

from email_analyzer.html import (
    html_to_text,
    normalize_for_dedupe,
    normalize_visible_text,
)


# --- html_to_text ---


def test_html_to_text_extracts_visible_text() -> None:
    html = "<html><body><p>Hello</p><p>World</p></body></html>"
    result = html_to_text(html)
    assert "Hello" in result
    assert "World" in result


def test_html_to_text_strips_script_and_style() -> None:
    html = (
        "<html><head><style>body{color:red}</style></head>"
        "<body><script>alert(1)</script><p>Content</p></body></html>"
    )
    result = html_to_text(html)
    assert "Content" in result
    assert "alert" not in result
    assert "color:red" not in result


def test_html_to_text_strips_noscript_svg_meta_link() -> None:
    html = (
        "<html><head><meta charset='utf-8'><link rel='stylesheet'></head>"
        "<body><noscript>Enable JS</noscript><svg></svg><p>Visible</p></body></html>"
    )
    result = html_to_text(html)
    assert "Visible" in result
    assert "Enable JS" not in result


def test_html_to_text_returns_empty_for_empty_input() -> None:
    assert html_to_text("") == ""


def test_html_to_text_handles_malformed_html() -> None:
    html = "<p>Unclosed paragraph<b>Bold text"
    result = html_to_text(html)
    assert "Unclosed paragraph" in result
    assert "Bold text" in result


def test_html_to_text_unescapes_entities() -> None:
    html = "<p>AT&amp;T &lt;hello&gt;</p>"
    result = html_to_text(html)
    assert "AT&T" in result
    assert "<hello>" in result


def test_html_to_text_handles_blockquote() -> None:
    html = "<body><p>Main</p><blockquote>Quoted text</blockquote></body>"
    result = html_to_text(html)
    assert "Main" in result
    assert "Quoted text" in result


# --- normalize_visible_text ---


def test_normalize_visible_text_collapses_whitespace() -> None:
    text = "Hello    world\t\tthere"
    result = normalize_visible_text(text)
    assert result == "Hello world there"


def test_normalize_visible_text_collapses_blank_lines() -> None:
    text = "First\n\n\n\n\nSecond"
    result = normalize_visible_text(text)
    assert result == "First\n\nSecond"


def test_normalize_visible_text_removes_zero_width_space() -> None:
    text = "Hello\u200bWorld"
    result = normalize_visible_text(text)
    assert result == "HelloWorld"


def test_normalize_visible_text_replaces_nbsp_with_space() -> None:
    text = "Hello\xa0World"
    result = normalize_visible_text(text)
    assert result == "Hello World"


def test_normalize_visible_text_normalizes_line_endings() -> None:
    text = "Line1\r\nLine2\rLine3\nLine4"
    result = normalize_visible_text(text)
    assert "\r" not in result
    assert result == "Line1\nLine2\nLine3\nLine4"


def test_normalize_visible_text_strips_leading_trailing() -> None:
    text = "  \n  Hello  \n  "
    result = normalize_visible_text(text)
    assert result == "Hello"


def test_normalize_visible_text_empty_input() -> None:
    assert normalize_visible_text("") == ""


def test_normalize_visible_text_nfkc_normalization() -> None:
    # NFKC normalizes fullwidth characters
    text = "\uff28\uff45\uff4c\uff4c\uff4f"  # fullwidth "Hello"
    result = normalize_visible_text(text)
    assert result == "Hello"


# --- normalize_for_dedupe ---


def test_normalize_for_dedupe_casefolds() -> None:
    text = "Hello WORLD"
    result = normalize_for_dedupe(text)
    assert result == "hello world"


def test_normalize_for_dedupe_collapses_all_whitespace() -> None:
    text = "Hello\n\n  World\tthere"
    result = normalize_for_dedupe(text)
    assert result == "hello world there"


def test_normalize_for_dedupe_empty_input() -> None:
    assert normalize_for_dedupe("") == ""


def test_normalize_for_dedupe_strips_leading_trailing() -> None:
    text = "  Hello World  "
    result = normalize_for_dedupe(text)
    assert result == "hello world"
