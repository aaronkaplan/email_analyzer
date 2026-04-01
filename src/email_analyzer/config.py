from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = "email_analyzer.processed.v1"
ERROR_SCHEMA_VERSION = "email_analyzer.error.v1"
OPENAI_BATCH_ENDPOINT = "/v1/responses"
DEFAULT_BATCH_MAX_REQUESTS = 50_000
DEFAULT_BATCH_MAX_BYTES = 190 * 1024 * 1024
DEFAULT_BATCH_POLL_INTERVAL_SECONDS = 15
DEFAULT_REPLY_PARSER_LANGUAGES = [
    "en",
    "de",
    "fr",
    "es",
    "it",
    "nl",
    "da",
    "sv",
    "cs",
    "ja",
    "pl",
    "ko",
    "zh",
]
DEFAULT_TEXT_ATTACHMENT_EXTENSIONS = {
    ".csv",
    ".json",
    ".log",
    ".md",
    ".text",
    ".tsv",
    ".txt",
    ".xml",
}
SELECTED_HEADER_NAMES = (
    "subject",
    "from",
    "to",
    "cc",
    "bcc",
    "reply-to",
    "date",
    "message-id",
    "in-reply-to",
    "references",
)
DEFAULT_OPENAI_INSTRUCTIONS = """Analyze the provided email package.

Use the headers, canonical body, and additional snippets to understand the email.
Treat the canonical body as the main human-authored content unless the extra snippets clearly add important context.
"""


@dataclass(slots=True)
class PrepareConfig:
    input_dir: Path
    output_dir: Path
    logs_dir: Path
    workers: int
    use_ripmime_fallback: bool = True


@dataclass(slots=True)
class RenderBatchConfig:
    processed_dir: Path
    batch_dir: Path
    model: str
    instructions_file: Path | None = None
    schema_file: Path | None = None
    max_requests_per_file: int = DEFAULT_BATCH_MAX_REQUESTS
    max_bytes_per_file: int = DEFAULT_BATCH_MAX_BYTES


@dataclass(slots=True)
class FlattenMailboxConfig:
    source_path: Path
    output_dir: Path
    filename_prefix: str | None = None


@dataclass(slots=True)
class BatchSubmitConfig:
    batch_jsonl: Path | None = None
    resume_batch_id: str | None = None
    output_dir: Path | None = None
    prompt: str | None = None
    prompt_from_file: Path | None = None
    poll_interval_seconds: int = DEFAULT_BATCH_POLL_INTERVAL_SECONDS
    completion_window: str = "24h"
    no_wait: bool = False


@dataclass(slots=True)
class BatchOutputXlsxConfig:
    input_jsonl: Path
    output_xlsx: Path
    schema_file: Path | None = None
