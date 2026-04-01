from __future__ import annotations

from collections.abc import Sequence

from mailparser_reply import EmailReplyParser

from .config import DEFAULT_REPLY_PARSER_LANGUAGES
from .html import normalize_visible_text

_PARSER_CACHE: dict[tuple[str, ...], EmailReplyParser] = {}


def strip_reply_text(text: str, languages: Sequence[str] | None = None) -> tuple[str, dict[str, object]]:
    normalized = normalize_visible_text(text)
    if not normalized:
        return "", {"tool": "mail-parser-reply", "changed": False, "characters_removed": 0}

    parser_languages = tuple(languages or DEFAULT_REPLY_PARSER_LANGUAGES)
    parser = _get_parser(parser_languages)
    try:
        stripped = parser.parse_reply(text=normalized) or normalized
    except Exception as exc:
        return normalized, {
            "tool": "mail-parser-reply",
            "changed": False,
            "characters_removed": 0,
            "error": str(exc),
        }

    stripped = normalize_visible_text(stripped)
    if not stripped:
        stripped = normalized

    return stripped, {
        "tool": "mail-parser-reply",
        "changed": stripped != normalized,
        "characters_removed": max(0, len(normalized) - len(stripped)),
    }


def _get_parser(languages: tuple[str, ...]) -> EmailReplyParser:
    parser = _PARSER_CACHE.get(languages)
    if parser is None:
        parser = EmailReplyParser(languages=list(languages))
        _PARSER_CACHE[languages] = parser
    return parser
