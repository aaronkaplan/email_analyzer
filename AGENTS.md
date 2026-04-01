# AGENTS

This file is for coding agents working in this repository.

## Mission

Build and maintain a local email preprocessing pipeline that:

1. reads raw RFC822 emails from `input/`
2. reduces noisy MIME content into one LLM-ready JSON artifact per email
3. renders OpenAI Batch API JSONL request files from those processed artifacts

Current implementation scope stops at `prepare` and `render-batch`.

Do not implement OpenAI submission or collection unless the user explicitly asks for it.

## Fixed Assumptions

1. There is one `input/` directory.
2. Filenames in `input/` are unique.
3. The original filename is the stable identifier everywhere.
4. `email_id == source filename == OpenAI custom_id`.
5. Raw emails stay in `input/`; do not copy them into `output/`.

## Chosen Stack

Use this stack unless the user asks to change it.

### Runtime And Tooling

1. Python `3.13`
2. `uv` for dependency management and command execution
3. `pytest` for tests

### Stdlib First

Prefer the standard library for:

1. MIME parsing: `email`
2. mailbox support: `mailbox`
3. CLI: `argparse`
4. filesystem paths: `pathlib`
5. structured data: `json`, `dataclasses`, `typing`
6. concurrency: `concurrent.futures`
7. timing: `time`
8. hashing: `hashlib`
9. logging: `logging`, `QueueHandler`, `QueueListener`

### Approved Third-Party Dependencies

Use these by default when needed:

1. `selectolax` for HTML parsing and visible-text extraction
2. `charset-normalizer` for charset fallback
3. `rapidfuzz` for duplicate detection heuristics
4. `mail-parser-reply` for reply-chain stripping on normalized plain text
5. `lingua-language-detector` for language detection

### Approved External Tools

1. `ripMIME` as an optional installed fallback for malformed MIME extraction or salvage

### Dependency Policy

Keep the baseline dependency set permissive-license friendly.

You may  add GPL or AGPL dependencies even without explicit user approval.


`ripMIME` is allowed because it is treated as an external tool fallback, not a baseline Python package dependency.

## Packaging And Layout

Use a normal `src/` layout once implementation starts.

Target structure:

```text
pyproject.toml
src/
  email_analyzer/
    __init__.py
    __main__.py
    cli.py
    config.py
    models.py
    prepare.py
    render_batch.py
    logging_utils.py
    metrics.py
    mime.py
    html.py
    dedupe.py
    quote_strip.py
    language.py
tests/
  fixtures/
```

Do not create ad hoc scripts in the repository root if the code belongs in the package.

## Project Commands

Use `uv` for all local commands.

Examples:

```bash
uv sync
uv run pytest
uv run python -m email_analyzer prepare --input input --output output --logs logs --workers 8
uv run python -m email_analyzer render-batch --processed output --batch-dir output/batches --model gpt-4o-mini
```

Do not introduce `requirements.txt` if `pyproject.toml` can express the same thing.

## CLI Contract

The intended public CLI has two subcommands.

### `prepare`

Reads raw email files from `input/` and writes one processed JSON artifact per file.

### `render-batch`

Reads processed JSON artifacts and writes OpenAI Batch API JSONL shard files.

Treat these names as stable unless the user explicitly asks to rename them.

## Implementation Rules

### 1. File Identity

Never generate opaque message IDs for the main pipeline identity.

Always preserve the original filename as:

1. `email_id`
2. `source_filename`
3. OpenAI `custom_id`
4. the stem of the output artifact name

### 2. Parallelism

Parallelize by email file, not by MIME part.

Default model:

1. one worker handles one email end-to-end
2. use `ProcessPoolExecutor`
3. avoid nested worker pools
4. write outputs atomically

Rationale: simpler logs, simpler timings, simpler failure isolation.

### 3. Logging

Logging is a first-class feature, not an afterthought.

Every significant pipeline step must log:

1. `email_id`
2. `source_filename`
3. `step`
4. `action`
5. `status`
6. `duration_ms`
7. human-readable message

Include additional fields when useful, such as:

1. MIME part path
2. content type
3. attachment filename
4. charset used or fallback used
5. similarity score
6. keep/drop reason

Use a single queue-based logging listener in the parent process.

Do not let multiple workers write interleaved plain-text logs directly to the same file.

### 4. Metrics

Measure:

1. per-step duration for each email
2. per-email total duration
3. aggregated step statistics across the run

Use `time.perf_counter_ns()` for timings.

Planned runtime outputs:

1. `logs/pipeline.jsonl`
2. `logs/file_summary.jsonl`
3. `logs/step_summary.json`

### 5. MIME Parsing

Use stdlib `email` as the primary parser.

If a message is malformed enough that stdlib parsing cannot recover usable structure, `ripMIME` may be used as a fallback extraction tool.

Keep access to parser defects. Do not hide them.

Maintain a part inventory for every email with at least:

1. MIME path
2. content type
3. content disposition
4. filename
5. content id
6. charset
7. decoded byte size
8. decoded text size if applicable
9. classification

### 6. Canonical Body Selection

Pick a single canonical body representation before quote stripping and before attachment triage.

This canonical body is the anchor for duplicate detection.

### 7. Duplicate Body Representation Filter

This filter is mandatory.

After text normalization, suppress parts that are merely another rendering of the canonical body.

Suggested heuristic order:

1. exact normalized-text hash match
2. high `rapidfuzz` similarity match
3. MIME-context confirmation from `multipart/alternative` or `multipart/related`

Never auto-drop by default:

1. `message/rfc822`
2. attachments with substantial extra text
3. partially similar documents

Always record the drop reason as structured metadata.

### 8. HTML Handling

Use `selectolax` for HTML parsing.

The output of HTML handling is visible text suitable for:

1. duplicate detection
2. quote stripping
3. language detection
4. LLM packaging

Do not ship raw HTML directly to the LLM in the first pass unless there is a clear, explicit reason.

### 9. Quote Stripping

Use `mail-parser-reply` on normalized plain text.

Run quote stripping after canonical body selection and duplicate suppression.

Do not run it on every MIME part blindly.

### 10. Language Detection

Use `lingua-language-detector` because the repository targets Python `3.13`.

Run it only on text that survived earlier reduction steps.

### 11. Batch Rendering

Target OpenAI Batch API request files for `/v1/responses`.

Each JSONL line must contain:

1. `custom_id`
2. `method`
3. `url`
4. `body`

`custom_id` must equal the original filename.

Do not rely on batch output order.

### 12. Output Artifacts

Each input email should produce one of:

1. `output/<filename>.json`
2. `output/<filename>.error.json`

Batch files belong under `output/batches/`.

## Schema Stability

Treat the processed JSON artifact as a durable contract.

If the schema changes materially, version it explicitly instead of silently changing field meanings.

At minimum, the processed artifact should retain:

1. selected headers
2. canonical body
3. kept snippets
4. dropped parts with reasons
5. parser defects
6. language metadata
7. timing metadata

## Testing Expectations

Add tests as implementation grows.

Prioritize tests for:

1. `multipart/alternative` selection
2. duplicate body representation suppression
3. charset fallback behavior
4. quote stripping behavior on real reply samples
5. deterministic output ordering
6. batch JSONL rendering with stable `custom_id`

Prefer fixture-driven tests with small real-world `.eml` samples.

## Things To Avoid

1. do not couple prompt rendering to raw MIME parsing
2. do not invent random IDs for emails
3. do not write shared output files from worker processes without coordination
4. do not add heavy dependencies when stdlib code is sufficient
5. do not silently drop content without a logged reason
6. do not assume HTML and plain text parts are both worth sending to the LLM

## Decision Summary

When in doubt, keep these decisions fixed:

1. Python `3.13`
2. `uv`
3. stdlib `email`
4. `selectolax`
5. `charset-normalizer`
6. `rapidfuzz`
7. `mail-parser-reply`
8. `lingua-language-detector`
9. file-level parallelism
10. JSONL logging
11. processed JSON first, OpenAI batch rendering second
