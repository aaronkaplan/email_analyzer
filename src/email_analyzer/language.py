from __future__ import annotations

from functools import lru_cache
from typing import Any

try:
    from lingua import LanguageDetectorBuilder
except ImportError:  # pragma: no cover - package namespace collision fallback
    from lingua.lingua import LanguageDetectorBuilder


def detect_language(text: str) -> dict[str, Any] | None:
    cleaned = text.strip()
    if len(cleaned) < 20:
        return None

    detector = _get_detector()
    language = detector.detect_language_of(cleaned)
    if language is None:
        return None

    code = getattr(getattr(language, "iso_code_639_1", None), "name", None)
    confidence = None

    try:
        confidence = round(
            float(detector.compute_language_confidence(cleaned, language)), 6
        )
    except Exception:
        confidence = None

    return {
        "code": code.lower() if code else None,
        "name": language.name.lower(),
        "confidence": confidence,
    }


@lru_cache(maxsize=1)
def _get_detector():
    return LanguageDetectorBuilder.from_all_languages().build()
