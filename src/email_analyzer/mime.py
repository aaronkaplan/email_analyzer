from __future__ import annotations

from email import policy
from email.message import EmailMessage, Message
from email.parser import BytesParser
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, cast

from charset_normalizer import from_bytes

from .config import DEFAULT_TEXT_ATTACHMENT_EXTENSIONS, SELECTED_HEADER_NAMES
from .html import html_to_text, normalize_for_dedupe, normalize_visible_text
from .models import PartAnalysis


def parse_email_bytes(raw_bytes: bytes) -> EmailMessage:
    return cast(EmailMessage, BytesParser(policy=policy.default).parsebytes(raw_bytes))


def select_headers(message: EmailMessage) -> dict[str, str | list[str]]:
    headers: dict[str, str | list[str]] = {}
    for header_name in SELECTED_HEADER_NAMES:
        values = message.get_all(header_name)
        if not values:
            continue
        headers[header_name] = values[0] if len(values) == 1 else values
    return headers


def collect_parser_defects(message: EmailMessage) -> list[str]:
    defects: list[str] = []
    for part in message.walk():
        for defect in getattr(part, "defects", []):
            defects.append(defect.__class__.__name__)
    return defects


def build_part_inventory(message: EmailMessage) -> list[PartAnalysis]:
    return list(_walk_message(message, path="1", parent_content_types=[]))


def decode_parts(parts: Iterable[PartAnalysis]) -> list[PartAnalysis]:
    decoded: list[PartAnalysis] = []
    for part in parts:
        _decode_part(part)
        decoded.append(part)
    return decoded


def is_textual_part(part: PartAnalysis) -> bool:
    if part.content_type == "message/rfc822":
        return True
    if part.content_type.startswith("text/"):
        return True
    if (
        part.filename
        and Path(part.filename).suffix.lower() in DEFAULT_TEXT_ATTACHMENT_EXTENSIONS
    ):
        return True
    return False


def normalized_text_hash(text: str | None) -> str | None:
    if not text:
        return None
    return sha256(text.encode("utf-8")).hexdigest()


def _walk_message(
    message: Message, path: str, parent_content_types: list[str]
) -> Iterable[PartAnalysis]:
    content_type = message.get_content_type()
    disposition = message.get_content_disposition()
    filename = message.get_filename()

    part = PartAnalysis(
        message_part=message,
        path=path,
        parent_content_types=list(parent_content_types),
        content_type=content_type,
        content_disposition=disposition,
        filename=filename,
        content_id=message.get("Content-ID"),
        charset=message.get_content_charset(),
        classification=_classify_part(message, parent_content_types),
        is_multipart=message.is_multipart(),
    )
    yield part

    if message.is_multipart():
        next_parents = [*parent_content_types, content_type]
        email_message = cast(EmailMessage, message)
        for index, child in enumerate(email_message.iter_parts(), start=1):
            yield from _walk_message(
                cast(Message, child), f"{path}.{index}", next_parents
            )


def _classify_part(message: Message, parent_content_types: list[str]) -> str:
    if message.is_multipart():
        return "container"

    content_type = message.get_content_type()
    disposition = message.get_content_disposition()
    filename = message.get_filename()
    extension = Path(filename).suffix.lower() if filename else ""
    immediate_parent = parent_content_types[-1] if parent_content_types else None

    if content_type == "message/rfc822":
        return "attached_message"

    if filename or disposition == "attachment":
        if (
            content_type.startswith("text/")
            or extension in DEFAULT_TEXT_ATTACHMENT_EXTENSIONS
        ):
            return "text_attachment"
        return "attachment"

    if content_type.startswith("image/") and disposition in {None, "inline"}:
        return "inline_resource"

    if content_type.startswith("text/"):
        if immediate_parent == "multipart/alternative":
            return "alternative_body"
        return "body_candidate"

    if disposition == "inline":
        return "inline_resource"

    return "unknown"


def _decode_part(part: PartAnalysis) -> None:
    if part.is_multipart:
        return

    payload = _payload_to_bytes(part.message_part.get_payload(decode=True))
    if payload is None:
        raw_payload = part.message_part.get_payload()
        if isinstance(raw_payload, str):
            payload = raw_payload.encode(part.charset or "utf-8", errors="ignore")
        else:
            payload = b""

    part.decoded_byte_size = len(payload)

    if part.content_type == "message/rfc822":
        preview = _extract_embedded_message_preview(
            cast(EmailMessage, part.message_part)
        )
        if preview:
            part.visible_text = normalize_visible_text(preview)
            part.normalized_text = normalize_for_dedupe(preview)
            part.extraction_method = "nested_message_preview"
        return

    if not is_textual_part(part):
        return

    decoded_text, charset_used, charset_source = _decode_bytes(payload, part.charset)
    part.charset_used = charset_used
    part.charset_source = charset_source

    if part.content_type == "text/html":
        part.visible_text = html_to_text(decoded_text)
        part.extraction_method = "selectolax"
    else:
        part.visible_text = normalize_visible_text(decoded_text)
        part.extraction_method = "text"

    part.normalized_text = normalize_for_dedupe(part.visible_text)


def _decode_bytes(
    payload: bytes, declared_charset: str | None
) -> tuple[str, str | None, str | None]:
    if not payload:
        return "", declared_charset, "empty"

    attempted_encodings: list[tuple[str, str]] = []
    if declared_charset:
        attempted_encodings.append((declared_charset, "declared"))

    for encoding in ("utf-8", "utf-8-sig"):
        if encoding != declared_charset:
            attempted_encodings.append((encoding, "fallback"))

    for encoding, source in attempted_encodings:
        try:
            return payload.decode(encoding), encoding, source
        except (LookupError, UnicodeDecodeError):
            continue

    try:
        match = from_bytes(payload).best()
    except Exception:
        match = None

    if match is not None:
        try:
            return str(match), getattr(match, "encoding", None), "charset-normalizer"
        except Exception:
            output = getattr(match, "output", None)
            if callable(output):
                try:
                    output_bytes = output()
                    if isinstance(output_bytes, bytes):
                        return (
                            output_bytes.decode("utf-8", errors="replace"),
                            getattr(match, "encoding", None),
                            "charset-normalizer",
                        )
                except Exception:
                    pass

    return (
        payload.decode("utf-8", errors="replace"),
        declared_charset or "utf-8",
        "replacement",
    )


def _extract_embedded_message_preview(part: EmailMessage) -> str | None:
    nested = _extract_nested_message(cast(Message, part))
    if nested is None:
        return None

    header_lines: list[str] = []
    for header_name in ("subject", "from", "to", "date"):
        value = nested.get(header_name)
        if value:
            header_lines.append(f"{header_name.title()}: {value}")

    body_text = _extract_preferred_body_text(nested)
    blocks = []
    if header_lines:
        blocks.append("\n".join(header_lines))
    if body_text:
        blocks.append(body_text)

    combined = "\n\n".join(blocks).strip()
    return combined or None


def _extract_nested_message(part: Message) -> EmailMessage | None:
    payload = part.get_payload()
    if isinstance(payload, list) and payload:
        candidate = payload[0]
        if isinstance(candidate, Message):
            return cast(EmailMessage, candidate)

    raw_payload = _payload_to_bytes(part.get_payload(decode=True))
    if raw_payload:
        try:
            return parse_email_bytes(raw_payload)
        except Exception:
            return None

    return None


def _extract_preferred_body_text(message: EmailMessage) -> str | None:
    preferred = message.get_body(preferencelist=("plain", "html"))
    if preferred is not None:
        payload = _payload_to_bytes(preferred.get_payload(decode=True)) or b""
        decoded_text, _, _ = _decode_bytes(payload, preferred.get_content_charset())
        if preferred.get_content_type() == "text/html":
            return html_to_text(decoded_text)
        return normalize_visible_text(decoded_text)

    for part in message.walk():
        if part.is_multipart():
            continue
        content_type = part.get_content_type()
        if content_type not in {"text/plain", "text/html"}:
            continue
        payload = _payload_to_bytes(part.get_payload(decode=True)) or b""
        decoded_text, _, _ = _decode_bytes(payload, part.get_content_charset())
        if content_type == "text/html":
            return html_to_text(decoded_text)
        return normalize_visible_text(decoded_text)

    return None


def _payload_to_bytes(payload: Any) -> bytes | None:
    if payload is None:
        return None
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    return None
