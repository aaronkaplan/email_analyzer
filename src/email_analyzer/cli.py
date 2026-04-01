from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .config import PrepareConfig, RenderBatchConfig
from .prepare import run_prepare
from .render_batch import run_render_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="email-analyzer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="Process raw emails into reduced JSON artifacts")
    prepare_parser.add_argument("--input", dest="input_dir", required=True, type=Path)
    prepare_parser.add_argument("--output", dest="output_dir", required=True, type=Path)
    prepare_parser.add_argument("--logs", dest="logs_dir", required=True, type=Path)
    prepare_parser.add_argument("--workers", dest="workers", type=int, default=1)

    render_parser = subparsers.add_parser("render-batch", help="Render OpenAI Batch API JSONL from processed artifacts")
    render_parser.add_argument("--processed", dest="processed_dir", required=True, type=Path)
    render_parser.add_argument("--batch-dir", dest="batch_dir", required=True, type=Path)
    render_parser.add_argument("--model", dest="model", required=True)
    render_parser.add_argument("--instructions-file", dest="instructions_file", type=Path)
    render_parser.add_argument("--max-requests-per-file", dest="max_requests_per_file", type=int, default=50_000)
    render_parser.add_argument("--max-bytes-per-file", dest="max_bytes_per_file", type=int, default=190 * 1024 * 1024)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
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
            max_requests_per_file=args.max_requests_per_file,
            max_bytes_per_file=args.max_bytes_per_file,
        )
        return run_render_batch(config)

    parser.error(f"Unknown command: {args.command}")
    return 2
