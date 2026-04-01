from __future__ import annotations

from dataclasses import asdict, dataclass, field
from email.message import Message
from typing import Any


@dataclass(slots=True)
class PartAnalysis:
    message_part: Message = field(repr=False)
    path: str
    parent_content_types: list[str]
    content_type: str
    content_disposition: str | None
    filename: str | None
    content_id: str | None
    charset: str | None
    classification: str
    is_multipart: bool
    decoded_bytes: bytes | None = field(default=None, repr=False)
    decoded_byte_size: int = 0
    decoded_text: str | None = field(default=None, repr=False)
    visible_text: str | None = field(default=None, repr=False)
    normalized_text: str | None = field(default=None, repr=False)
    charset_used: str | None = None
    charset_source: str | None = None
    extraction_method: str | None = None

    @property
    def decoded_text_size(self) -> int:
        return len(self.visible_text or "")

    def inventory_record(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "parent_content_types": list(self.parent_content_types),
            "content_type": self.content_type,
            "content_disposition": self.content_disposition,
            "filename": self.filename,
            "content_id": self.content_id,
            "charset": self.charset,
            "charset_used": self.charset_used,
            "charset_source": self.charset_source,
            "classification": self.classification,
            "is_multipart": self.is_multipart,
            "decoded_byte_size": self.decoded_byte_size,
            "decoded_text_size": self.decoded_text_size,
            "extraction_method": self.extraction_method,
        }


@dataclass(slots=True)
class Snippet:
    snippet_id: str
    kind: str
    source_part_path: str | None
    content_type: str | None
    filename: str | None
    text: str
    language: dict[str, Any] | None
    characters: int
    token_estimate: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DroppedPart:
    source_part_path: str
    content_type: str
    filename: str | None
    classification: str
    reason: str
    similarity: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AttachmentSummary:
    source_part_path: str
    filename: str | None
    content_type: str
    classification: str
    kept: bool
    reason: str | None
    text_extracted: bool
    char_count: int


@dataclass(slots=True)
class ProcessedEmail:
    schema_version: str
    email_id: str
    source_filename: str
    headers: dict[str, Any]
    parser_defects: list[str]
    canonical_body: Snippet | None
    kept_snippets: list[Snippet]
    dropped_parts: list[DroppedPart]
    attachments: list[AttachmentSummary]
    part_inventory: list[dict[str, Any]]
    timings_ms: dict[str, float]
    total_duration_ms: float
    stats: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
