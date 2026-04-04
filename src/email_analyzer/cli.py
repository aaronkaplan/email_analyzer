from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from dotenv import find_dotenv, load_dotenv

from .batch_output_xlsx import run_batch_output_to_xlsx
from .batch_submitter import run_batch_submitter
from .config import (
    BatchOutputXlsxConfig,
    BatchSubmitConfig,
    DEFAULT_OLLAMA_NUM_PARALLEL_JOBS,
    FlattenMailboxConfig,
    OllamaBatchSubmitConfig,
    PrepareConfig,
    RenderBatchConfig,
)
from .mailbox_flatten import run_flatten_mailbox
from .ollama_batch_submitter import run_ollama_batch_submitter
from .prepare import run_prepare
from .render_batch import run_render_batch


def _autoload_dotenv() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    repo_dotenv = repo_root / ".env"
    if repo_dotenv.exists():
        load_dotenv(repo_dotenv, override=False)
        return

    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path, override=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="email-analyzer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser(
        "prepare", help="Process raw emails into reduced JSON artifacts"
    )
    prepare_parser.add_argument("--input", dest="input_dir", required=True, type=Path)
    prepare_parser.add_argument("--output", dest="output_dir", required=True, type=Path)
    prepare_parser.add_argument("--logs", dest="logs_dir", required=True, type=Path)
    prepare_parser.add_argument("--workers", dest="workers", type=int, default=1)

    render_parser = subparsers.add_parser(
        "render-batch", help="Render OpenAI Batch API JSONL from processed artifacts"
    )
    render_parser.add_argument(
        "--processed", dest="processed_dir", required=True, type=Path
    )
    render_parser.add_argument(
        "--batch-dir", dest="batch_dir", required=True, type=Path
    )
    render_parser.add_argument("--model", dest="model", required=True)
    render_parser.add_argument(
        "--instructions-file", dest="instructions_file", type=Path
    )
    render_parser.add_argument("--schema-file", dest="schema_file", type=Path)
    render_parser.add_argument(
        "--max-requests-per-file",
        dest="max_requests_per_file",
        type=int,
        default=50_000,
    )
    render_parser.add_argument(
        "--max-bytes-per-file",
        dest="max_bytes_per_file",
        type=int,
        default=190 * 1024 * 1024,
    )

    flatten_parser = subparsers.add_parser(
        "flatten-mailbox",
        help="Flatten an mbox-style archive into one raw message file per email",
    )
    flatten_parser.add_argument(
        "--source", dest="source_path", required=True, type=Path
    )
    flatten_parser.add_argument("--output", dest="output_dir", required=True, type=Path)
    flatten_parser.add_argument("--filename-prefix", dest="filename_prefix")

    submit_parser = subparsers.add_parser(
        "submit-batch",
        help="Submit one OpenAI Batch API JSONL shard and monitor it to completion",
    )
    submit_target_group = submit_parser.add_mutually_exclusive_group(required=True)
    submit_target_group.add_argument("--batch-jsonl", dest="batch_jsonl", type=Path)
    submit_target_group.add_argument("--resume-batch-id", dest="resume_batch_id")
    submit_parser.add_argument("--output-dir", dest="output_dir", type=Path)
    prompt_group = submit_parser.add_mutually_exclusive_group()
    prompt_group.add_argument("--prompt", dest="prompt")
    prompt_group.add_argument("--prompt-from-file", dest="prompt_from_file", type=Path)
    submit_parser.add_argument(
        "--poll-interval-seconds", dest="poll_interval_seconds", type=int, default=15
    )
    submit_parser.add_argument(
        "--completion-window", dest="completion_window", default="24h"
    )
    submit_parser.add_argument("--no-wait", dest="no_wait", action="store_true")

    ollama_submit_parser = subparsers.add_parser(
        "submit-ollama-batch",
        help="Run one rendered batch JSONL shard locally against Ollama",
    )
    ollama_submit_parser.add_argument(
        "--batch-jsonl", dest="batch_jsonl", required=True, type=Path
    )
    ollama_submit_parser.add_argument("--output-dir", dest="output_dir", type=Path)
    ollama_submit_parser.add_argument("--base-url", dest="base_url", action="append")
    ollama_submit_parser.add_argument("--model", dest="model")
    ollama_prompt_group = ollama_submit_parser.add_mutually_exclusive_group()
    ollama_prompt_group.add_argument("--prompt", dest="prompt")
    ollama_prompt_group.add_argument(
        "--prompt-from-file", dest="prompt_from_file", type=Path
    )
    ollama_submit_parser.add_argument("--num-shards", dest="num_shards", type=int)
    ollama_submit_parser.add_argument(
        "--num-parallel-jobs",
        dest="num_parallel_jobs",
        type=int,
        default=DEFAULT_OLLAMA_NUM_PARALLEL_JOBS,
    )
    ollama_submit_parser.add_argument(
        "--request-timeout-seconds",
        dest="request_timeout_seconds",
        type=int,
        default=600,
    )

    export_parser = subparsers.add_parser(
        "batch-output-to-xlsx", help="Convert OpenAI batch output JSONL into XLSX"
    )
    export_parser.add_argument(
        "--input-jsonl", dest="input_jsonl", required=True, type=Path
    )
    export_parser.add_argument("--output-xlsx", dest="output_xlsx", type=Path)
    export_parser.add_argument("--schema-file", dest="schema_file", type=Path)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _autoload_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "prepare":
        config = PrepareConfig(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            logs_dir=args.logs_dir,
            workers=max(1, args.workers),
        )
        return run_prepare(config)

    if args.command == "render-batch":
        config = RenderBatchConfig(
            processed_dir=args.processed_dir,
            batch_dir=args.batch_dir,
            model=args.model,
            instructions_file=args.instructions_file,
            schema_file=args.schema_file,
            max_requests_per_file=args.max_requests_per_file,
            max_bytes_per_file=args.max_bytes_per_file,
        )
        return run_render_batch(config)

    if args.command == "flatten-mailbox":
        config = FlattenMailboxConfig(
            source_path=args.source_path,
            output_dir=args.output_dir,
            filename_prefix=args.filename_prefix,
        )
        run_flatten_mailbox(config)
        return 0

    if args.command == "submit-batch":
        config = BatchSubmitConfig(
            batch_jsonl=args.batch_jsonl,
            resume_batch_id=args.resume_batch_id,
            output_dir=args.output_dir,
            prompt=args.prompt,
            prompt_from_file=args.prompt_from_file,
            poll_interval_seconds=max(1, args.poll_interval_seconds),
            completion_window=args.completion_window,
            no_wait=args.no_wait,
        )
        return run_batch_submitter(config)

    if args.command == "submit-ollama-batch":
        base_urls = tuple(args.base_url or [])
        config = OllamaBatchSubmitConfig(
            batch_jsonl=args.batch_jsonl,
            output_dir=args.output_dir,
            base_url=base_urls[0] if len(base_urls) == 1 else None,
            base_urls=base_urls,
            model=args.model,
            prompt=args.prompt,
            prompt_from_file=args.prompt_from_file,
            num_shards=args.num_shards,
            num_parallel_jobs=max(1, args.num_parallel_jobs),
            request_timeout_seconds=max(1, args.request_timeout_seconds),
        )
        return run_ollama_batch_submitter(config)

    if args.command == "batch-output-to-xlsx":
        config = BatchOutputXlsxConfig(
            input_jsonl=args.input_jsonl,
            output_xlsx=args.output_xlsx or args.input_jsonl.with_suffix(".xlsx"),
            schema_file=args.schema_file,
        )
        return run_batch_output_to_xlsx(config)

    parser.error(f"Unknown command: {args.command}")
    return 2
