from __future__ import annotations

from pathlib import Path

from rapidfuzz import fuzz

from .mime import normalized_text_hash
from .models import DroppedPart, PartAnalysis

_BODY_CLASSIFICATIONS = {"body_candidate", "alternative_body"}
_ATTACHMENT_CLASSIFICATIONS = {"attachment", "text_attachment", "attached_message"}
_HTMLISH_EXTENSIONS = {".htm", ".html", ".mht", ".mhtml", ".xhtml"}
_TEXT_DUPLICATE_EXTENSIONS = _HTMLISH_EXTENSIONS | {".txt"}


def choose_canonical_body(parts: list[PartAnalysis]) -> PartAnalysis | None:
    candidates = [part for part in parts if _is_candidate_body(part)]
    if not candidates:
        return None

    plain_candidates = sorted(
        [part for part in candidates if part.content_type == "text/plain"],
        key=_body_rank,
        reverse=True,
    )
    html_candidates = sorted(
        [part for part in candidates if part.content_type == "text/html"],
        key=_body_rank,
        reverse=True,
    )

    if plain_candidates and html_candidates:
        best_plain = plain_candidates[0]
        best_html = html_candidates[0]
        plain_length = len(best_plain.normalized_text or "")
        html_length = len(best_html.normalized_text or "")
        if plain_length >= max(80, int(html_length * 0.4)):
            return best_plain
        return best_html

    return max(candidates, key=_body_rank)


def filter_duplicate_body_representations(
    parts: list[PartAnalysis], canonical_body: PartAnalysis | None
) -> dict[str, DroppedPart]:
    if canonical_body is None or not canonical_body.normalized_text:
        return {}

    canonical_text = canonical_body.normalized_text
    canonical_hash = normalized_text_hash(canonical_text)
    dropped: dict[str, DroppedPart] = {}

    for part in parts:
        if part.path == canonical_body.path:
            continue
        if not part.normalized_text:
            continue
        if part.content_type == "message/rfc822":
            continue
        if not _is_duplicate_candidate(part):
            continue

        current_hash = normalized_text_hash(part.normalized_text)
        similarity = fuzz.ratio(canonical_text, part.normalized_text)
        length_ratio = _length_ratio(canonical_text, part.normalized_text)
        mime_context_match = any(content_type in {"multipart/alternative", "multipart/related"} for content_type in part.parent_content_types)
        htmlish_filename = _has_htmlish_filename(part.filename)

        if current_hash == canonical_hash:
            reason = "duplicate_body_representation"
        elif mime_context_match and similarity >= 97 and length_ratio >= 0.9:
            reason = "duplicate_body_representation"
        elif htmlish_filename and similarity >= 99 and length_ratio >= 0.9:
            reason = "duplicate_body_representation"
        else:
            continue

        dropped[part.path] = DroppedPart(
            source_part_path=part.path,
            content_type=part.content_type,
            filename=part.filename,
            classification=part.classification,
            reason=reason,
            similarity=round(similarity / 100, 4),
            details={"canonical_part_path": canonical_body.path},
        )

    return dropped


def is_body_like(part: PartAnalysis) -> bool:
    return part.classification in _BODY_CLASSIFICATIONS and bool(part.visible_text)


def is_attachment_like(part: PartAnalysis) -> bool:
    return part.classification in _ATTACHMENT_CLASSIFICATIONS


def _is_candidate_body(part: PartAnalysis) -> bool:
    return part.classification in _BODY_CLASSIFICATIONS and bool(part.visible_text)


def _is_duplicate_candidate(part: PartAnalysis) -> bool:
    if part.classification in _BODY_CLASSIFICATIONS:
        return True
    if part.classification == "text_attachment":
        return True
    if part.content_type in {"text/plain", "text/html"}:
        return True

    if part.filename:
        return Path(part.filename).suffix.lower() in _TEXT_DUPLICATE_EXTENSIONS
    return False


def _body_rank(part: PartAnalysis) -> int:
    score = len(part.visible_text or "")
    if part.content_type == "text/plain":
        score += 200
    elif part.content_type == "text/html":
        score += 100

    if part.classification == "alternative_body":
        score += 30
    if part.filename:
        score -= 500
    return score


def _has_htmlish_filename(filename: str | None) -> bool:
    if not filename:
        return False
    return Path(filename).suffix.lower() in _HTMLISH_EXTENSIONS


def _length_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return min(len(left), len(right)) / max(len(left), len(right))
