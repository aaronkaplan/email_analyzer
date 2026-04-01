from __future__ import annotations

import html as html_lib
import re
import unicodedata

from selectolax.parser import HTMLParser

_SPACE_RE = re.compile(r"[ \t]+")
_BLANK_LINES_RE = re.compile(r"\n{3,}")
_TAG_RE = re.compile(r"<[^>]+>")


def html_to_text(html: str) -> str:
    """Convert HTML to visible text suitable for downstream NLP work."""
    if not html:
        return ""

    try:
        tree = HTMLParser(html)
        for selector in ("script", "style", "noscript", "svg", "title", "meta", "link", "head"):
            for node in tree.css(selector):
                node.decompose()

        root = tree.body or getattr(tree, "html", None) or tree
        text = root.text(separator="\n") if hasattr(root, "text") else html
    except Exception:
        text = _TAG_RE.sub(" ", html)

    return normalize_visible_text(text)


def normalize_visible_text(text: str) -> str:
    if not text:
        return ""

    normalized = unicodedata.normalize("NFKC", html_lib.unescape(text))
    normalized = normalized.replace("\u200b", "")
    normalized = normalized.replace("\xa0", " ")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")

    lines: list[str] = []
    for raw_line in normalized.split("\n"):
        collapsed = _SPACE_RE.sub(" ", raw_line).strip()
        lines.append(collapsed)

    normalized = "\n".join(lines)
    normalized = _BLANK_LINES_RE.sub("\n\n", normalized)
    return normalized.strip()


def normalize_for_dedupe(text: str) -> str:
    normalized = normalize_visible_text(text)
    normalized = normalized.casefold()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()
