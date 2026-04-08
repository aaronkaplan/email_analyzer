from __future__ import annotations

from pathlib import Path

from email_analyzer.config import (
    DEFAULT_BATCH_MAX_BYTES,
    DEFAULT_BATCH_MAX_REQUESTS,
    DEFAULT_BATCH_POLL_INTERVAL_SECONDS,
    DEFAULT_OLLAMA_NUM_PARALLEL_JOBS,
    DEFAULT_OLLAMA_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_OPENAI_INSTRUCTIONS,
    DEFAULT_REPLY_PARSER_LANGUAGES,
    DEFAULT_TEXT_ATTACHMENT_EXTENSIONS,
    ERROR_SCHEMA_VERSION,
    OLLAMA_CHAT_ENDPOINT,
    OPENAI_BATCH_ENDPOINT,
    SCHEMA_VERSION,
    SELECTED_HEADER_NAMES,
    BatchOutputXlsxConfig,
    BatchSubmitConfig,
    EvalBenchmarkConfig,
    FlattenMailboxConfig,
    OllamaBatchSubmitConfig,
    PrepareConfig,
    RenderBatchConfig,
)


# --- Constants ---


def test_schema_version_is_string() -> None:
    assert isinstance(SCHEMA_VERSION, str)
    assert "email_analyzer" in SCHEMA_VERSION


def test_error_schema_version_is_string() -> None:
    assert isinstance(ERROR_SCHEMA_VERSION, str)
    assert "error" in ERROR_SCHEMA_VERSION


def test_endpoints_are_strings() -> None:
    assert OPENAI_BATCH_ENDPOINT.startswith("/")
    assert OLLAMA_CHAT_ENDPOINT.startswith("/")


def test_default_reply_parser_languages_is_tuple() -> None:
    # Must be immutable to prevent accidental mutation
    assert isinstance(DEFAULT_REPLY_PARSER_LANGUAGES, tuple)
    assert "en" in DEFAULT_REPLY_PARSER_LANGUAGES


def test_default_text_attachment_extensions_contains_common_types() -> None:
    assert ".txt" in DEFAULT_TEXT_ATTACHMENT_EXTENSIONS
    assert ".csv" in DEFAULT_TEXT_ATTACHMENT_EXTENSIONS
    assert ".json" in DEFAULT_TEXT_ATTACHMENT_EXTENSIONS


def test_selected_header_names_is_tuple() -> None:
    assert isinstance(SELECTED_HEADER_NAMES, tuple)
    assert "subject" in SELECTED_HEADER_NAMES
    assert "from" in SELECTED_HEADER_NAMES


def test_default_openai_instructions_is_nonempty_string() -> None:
    assert isinstance(DEFAULT_OPENAI_INSTRUCTIONS, str)
    assert len(DEFAULT_OPENAI_INSTRUCTIONS.strip()) > 0


def test_default_numeric_constants() -> None:
    assert DEFAULT_BATCH_MAX_REQUESTS == 50_000
    assert DEFAULT_BATCH_MAX_BYTES == 190 * 1024 * 1024
    assert DEFAULT_BATCH_POLL_INTERVAL_SECONDS == 15
    assert DEFAULT_OLLAMA_NUM_PARALLEL_JOBS == 3
    assert DEFAULT_OLLAMA_REQUEST_TIMEOUT_SECONDS == 600


# --- PrepareConfig ---


def test_prepare_config_fields() -> None:
    config = PrepareConfig(
        input_dir=Path("/input"),
        output_dir=Path("/output"),
        logs_dir=Path("/logs"),
        workers=4,
    )
    assert config.input_dir == Path("/input")
    assert config.workers == 4


# --- RenderBatchConfig ---


def test_render_batch_config_defaults() -> None:
    config = RenderBatchConfig(
        processed_dir=Path("/out"),
        batch_dir=Path("/batches"),
        model="gpt-4o-mini",
    )
    assert config.instructions_file is None
    assert config.schema_file is None
    assert config.max_requests_per_file == DEFAULT_BATCH_MAX_REQUESTS
    assert config.max_bytes_per_file == DEFAULT_BATCH_MAX_BYTES


# --- BatchSubmitConfig ---


def test_batch_submit_config_defaults() -> None:
    config = BatchSubmitConfig()
    assert config.batch_jsonl is None
    assert config.resume_batch_id is None
    assert config.output_dir is None
    assert config.poll_interval_seconds == DEFAULT_BATCH_POLL_INTERVAL_SECONDS
    assert config.completion_window == "24h"
    assert config.no_wait is False


# --- OllamaBatchSubmitConfig ---


def test_ollama_batch_submit_config_defaults() -> None:
    config = OllamaBatchSubmitConfig(batch_jsonl=Path("/batch.jsonl"))
    assert config.base_urls == ()
    assert config.model is None
    assert config.num_shards is None
    assert config.num_parallel_jobs == DEFAULT_OLLAMA_NUM_PARALLEL_JOBS
    assert config.request_timeout_seconds == DEFAULT_OLLAMA_REQUEST_TIMEOUT_SECONDS
    assert config.insecure is False


# --- FlattenMailboxConfig ---


def test_flatten_mailbox_config_defaults() -> None:
    config = FlattenMailboxConfig(
        source_path=Path("/mbox"),
        output_dir=Path("/out"),
    )
    assert config.filename_prefix is None


# --- BatchOutputXlsxConfig ---


def test_batch_output_xlsx_config() -> None:
    config = BatchOutputXlsxConfig(
        input_jsonl=Path("/in.jsonl"),
        output_xlsx=Path("/out.xlsx"),
    )
    assert config.schema_file is None


# --- EvalBenchmarkConfig ---


def test_eval_benchmark_config_defaults() -> None:
    config = EvalBenchmarkConfig(batch_output_jsonl=Path("/out.jsonl"))
    assert config.label_field == "classification"
    assert config.positive_class is None
    assert config.category_map_file is None
    assert config.output_xlsx is None
