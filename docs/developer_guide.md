# Developer Guide

This document collects the repository details that do not belong in the top-level `README.md`.

Start with `README.md` if you only want the quickstart.

## Mission

The repository builds a local email preprocessing pipeline that:

1. reads raw RFC822 emails from `input/`
2. reduces noisy MIME content into one LLM-ready JSON artifact per email
3. renders OpenAI Batch API JSONL request files from those processed artifacts
4. optionally submits those shards and exports structured results

## Stable Assumptions

These assumptions are fixed unless there is a strong reason to change them:

1. there is one input directory per run
2. filenames in `input/` are unique
3. the original filename is the stable identifier everywhere
4. `email_id == source filename == OpenAI custom_id`
5. raw emails stay in `input/`; they are not copied into `output/`

That filename-based identity is the join key across local artifacts, batch submissions, and batch outputs.

## Chosen Stack

The baseline stack is intentionally small and stdlib-first.

Runtime and tooling:

1. Python `3.13`
2. `uv` for dependency management and command execution
3. `pytest` for tests

Core stdlib pieces:

1. `email` for MIME parsing
2. `mailbox` for mailbox support
3. `argparse` for the CLI
4. `pathlib` for filesystem paths
5. `json`, `dataclasses`, and `typing` for structured data
6. `concurrent.futures` for parallelism
7. `time` for timings
8. `hashlib` for hashing
9. `logging` plus queue handlers for worker-safe logs

Approved third-party dependencies used in the first pass:

1. `selectolax` for HTML parsing and visible-text extraction
2. `charset-normalizer` for charset fallback
3. `rapidfuzz` for duplicate detection heuristics
4. `mail-parser-reply` for quote stripping
5. `lingua-language-detector` for language detection
6. `openai` for batch submission
7. `openpyxl` for `.xlsx` export
8. `python-dotenv` for repo-local `.env` autoloading

Optional external fallback tool:

1. `ripMIME` for malformed MIME extraction or salvage work

## Main Commands

The current CLI surface is:

1. `prepare`: parse raw emails and write one reduced JSON artifact per file
2. `render-batch`: render `/v1/responses` batch JSONL from processed artifacts
3. `submit-batch`: upload one rendered shard, poll it, and download output files
4. `batch-output-to-xlsx`: export successful structured batch output to `.xlsx`
5. `flatten-mailbox`: turn `mbox` or `mbox.gz` archives into one `.eml` file per message

Examples:

```bash
uv run python -m email_analyzer prepare --input input --output output --logs logs --workers 8
uv run python -m email_analyzer render-batch --processed output --batch-dir output/batches --model gpt-4o-mini
uv run python -m email_analyzer submit-batch --batch-jsonl output/batches/batch-00001.jsonl
uv run python -m email_analyzer batch-output-to-xlsx --input-jsonl output/batch_output/batch-00001/batch_output.jsonl
```

## Repository Layout

Important top-level paths:

1. `src/email_analyzer/`: package code
2. `tests/`: test suite
3. `docs/`: operator and developer documentation
4. `benchmarks/`: local benchmark corpora and runs

Important runtime paths:

```text
input/
  *.eml

output/
  <filename>.json
  <filename>.error.json
  batch.jsonl
  batches/
    batch-00001.jsonl

logs/
  pipeline.jsonl
  file_summary.jsonl
  step_summary.json
```

`output/<filename>.json` is the primary local artifact. `output/batches/` is the canonical batch location.

## Workflow Details

### 1. Parse Source

Parse raw email bytes with the stdlib `email` parser and record:

1. byte size
2. root MIME type
3. parser defects
4. selected top-level headers

### 2. Inventory Parts

Walk the MIME tree and classify each part as one of:

1. canonical body candidate
2. alternative body candidate
3. inline resource
4. attachment
5. attached message
6. unknown

### 3. Decode And Canonicalize

For text-bearing parts:

1. decode transfer encoding
2. decode charset using the declared charset first
3. fall back to `charset-normalizer` only when needed
4. convert HTML to visible text
5. normalize whitespace and line endings

If stdlib parsing is badly degraded on a message, `ripMIME` is an acceptable fallback tool.

### 4. Choose Canonical Body

Pick one best body representation from `multipart/alternative` or related body structures.

The canonical body is the anchor for duplicate suppression.

### 5. Filter Duplicate Body Representations

After normalization, suppress parts that are only another rendering of the canonical body.

Typical examples:

1. plain text and HTML siblings with the same visible text
2. an attached `body.html` file that repeats the message body
3. duplicate inline text parts inside `multipart/related`

Suggested heuristic order:

1. exact normalized-text hash match
2. high-similarity `rapidfuzz` match
3. MIME-context confirmation from `multipart/alternative` or `multipart/related`

Do not auto-drop:

1. `message/rfc822`
2. attachments with materially more text than the canonical body
3. documents that are only partially similar

Every dropped part should retain a reason such as `duplicate_body_representation`.

### 6. Strip Quotes And Signatures

Run quote stripping only on surviving body text after canonical body selection and duplicate suppression.

The current pipeline uses `mail-parser-reply` on normalized plain text.

### 7. Triage Attachments

Keep only attachments that are still plausible LLM inputs.

The first pass favors:

1. attached messages
2. text attachments
3. small structured text files such as CSV

### 8. Detect Language

Run language detection only on text that survived earlier reduction steps.

### 9. Rank And Pack

Rank surviving text blocks and assemble the reduced LLM payload.

### 10. Write Output

Write exactly one JSON artifact per input email using the original filename as the identifier.

## Processed Email Output Contract

Each processed JSON file should contain at least:

1. `email_id`
2. `source_filename`
3. selected headers
4. parser defects
5. canonical body
6. kept snippets
7. dropped parts with reasons
8. attachment summaries
9. language metadata
10. estimated token count
11. per-step timings
12. total file timing

## Batch Rendering Contract

The repository renders OpenAI Batch API requests targeting `/v1/responses`.

Each line must contain:

1. `custom_id`
2. `method`
3. `url`
4. `body`

The original filename must remain the `custom_id`.

Example batch line:

```json
{
  "custom_id": "invoice_102.eml",
  "method": "POST",
  "url": "/v1/responses",
  "body": {
    "model": "gpt-4o-mini",
    "instructions": "...task instructions...",
    "input": [
      {
        "role": "user",
        "content": [
          {
            "type": "input_text",
            "text": "...reduced email JSON payload..."
          }
        ]
      }
    ]
  }
}
```

Important constraints:

1. batch results are not returned in input order
2. `custom_id` is the join key and must stay stable
3. all lines in a shard must target the same endpoint
4. a shard should stay under OpenAI batch size limits

If you want Structured Outputs, render with `--schema-file path/to/schema.py` where the file defines `class mySchema(BaseModel):`.

## Parallelism

The pipeline is designed for file-level parallelism.

Default strategy:

1. one worker processes one email end-to-end
2. no nested per-part worker pools in the first pass
3. use a `ProcessPoolExecutor` by default
4. write outputs atomically with temp-file-then-rename

This keeps logging, timing, failure isolation, and deterministic outputs simple.

## Logging And Metrics

The pipeline records both actions and timings.

Major steps should log:

1. which file was processed
2. what action was taken
3. what was kept or dropped
4. how long the step took

Planned log files:

1. `logs/pipeline.jsonl`: structured event log
2. `logs/file_summary.jsonl`: one summary line per email
3. `logs/step_summary.json`: aggregated metrics by step

Example event shape:

```json
{
  "email_id": "invoice_102.eml",
  "source_filename": "invoice_102.eml",
  "step": "filter_duplicate_body_representations",
  "action": "drop_part",
  "status": "success",
  "part_path": "3.2",
  "attachment_filename": "body.html",
  "reason": "duplicate_body_representation",
  "similarity": 0.998,
  "duration_ms": 6.4
}
```

## Benchmarks

For reproducible benchmark setup, SpamAssassin download commands, and historical throughput notes, see `docs/benchmarks.md`.

## Development Principles

When changing the codebase, keep these priorities fixed:

1. keep the dependency set small
2. prefer stdlib first where it is sufficient
3. keep processed artifacts deterministic
4. never lose filename-based identity
5. log anything important enough to audit later
6. reduce token volume without silently dropping meaningful content

## Future Work

Likely future areas:

1. richer malformed-message salvage paths
2. richer extraction for PDFs and Office documents
3. more post-processing helpers for batch output analysis
4. additional corpus setup helpers for benchmark workflows
