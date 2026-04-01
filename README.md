# Email Analyzer

`email_analyzer` is a local preprocessing pipeline for raw email corpora.

The repository is for turning RFC822 email files from `input/` into two things:

1. one reduced, auditable JSON artifact per email in `output/`
2. OpenAI Batch API request files in `output/batches/`

The goal is to send far fewer, higher-value text snippets to an LLM than the raw MIME tree would suggest.

## Status

The repository currently implements the first-pass local pipeline and OpenAI batch submission flow.

Current implementation scope:

1. `prepare`: parse, reduce, and write `output/<filename>.json`
2. `render-batch`: render OpenAI Batch API JSONL files from those processed outputs
3. `submit-batch`: upload one rendered shard, poll to completion, and download output and error files
4. `batch-output-to-xlsx`: convert successful batch result JSONL into a spreadsheet

Not in the first pass:

1. OCR or deep document extraction for complex binary attachments

## Input Assumptions

1. There is a single `input/` directory.
2. Filenames inside `input/` are already unique.
3. Inputs are raw RFC822 email messages such as `.eml` files or equivalent message dumps.
4. Inputs are not full SMTP session transcripts.

The original filename is the stable identifier for the entire pipeline.

## What The Pipeline Does

For each input email, the pipeline will:

1. parse the MIME structure
2. inventory all parts and attachments
3. decode transfer encodings and charsets
4. convert HTML parts to visible text
5. choose one canonical body representation
6. detect and drop duplicate body representations
7. strip quoted history and signatures from the surviving body text
8. triage attachments and keep only relevant text-bearing material
9. detect language on surviving text blocks
10. rank the remaining snippets for LLM usefulness
11. write one processed JSON artifact per email
12. render OpenAI Batch API request lines from those processed artifacts

The important extra filter is step 6: if a MIME attachment or sibling part is merely an HTML rendering of the same email body text, it should be dropped before LLM packaging.

## Why This Exists

Email MIME trees are noisy.

Common sources of waste include:

1. `multipart/alternative` parts that repeat the same content in plain text and HTML
2. inline HTML files that are just another rendering of the body
3. quoted reply chains
4. signatures and disclaimers
5. inline images and low-value attachments
6. charset and HTML decoding problems that produce duplicate-looking garbage

This repository exists to preserve the useful content while cutting the token load aggressively and transparently.

## Chosen Tech Stack

The baseline stack is intentionally small and permissive-license friendly.

### Runtime

1. Python `3.13`
2. `uv` for environment and dependency management

### Core Libraries

1. stdlib `email` for RFC822 and MIME parsing
2. stdlib `mailbox` for future `Maildir` and `mbox` support
3. stdlib `argparse` for the CLI
4. stdlib `logging` plus `QueueHandler`/`QueueListener` for worker-safe logs
5. stdlib `concurrent.futures` for file-level parallelism
6. stdlib `pathlib`, `json`, `dataclasses`, `time`, and `hashlib`

### Third-Party Libraries

1. `selectolax` for fast HTML parsing and visible-text extraction
2. `charset-normalizer` for charset fallback when declared encodings fail
3. `rapidfuzz` for near-duplicate detection after text normalization
4. `mail-parser-reply` for stripping quoted text from canonicalized plain text
5. `lingua-language-detector` for language detection on short email text
6. `pytest` for tests

### Optional Fallback Tools

1. `ripMIME` as an installable external fallback for malformed MIME messages or attachment extraction edge cases

### Explicitly Not In The Baseline Stack

1. `flanker`: too stale for a new baseline
2. `talon`: useful historically, but stale
3. `unquotemail`: good ideas, but it pulls in `html2text` which is GPL
4. `mail-parser`: useful helper, but not required for the first pass

## Processing Model

The first implementation is split into two stages.

### Stage 1: `prepare`

This stage reads raw files from `input/` and writes one processed JSON file per email.

Target output:

1. `output/<filename>.json`
2. `output/<filename>.error.json` on failure

### Stage 2: `render-batch`

This stage reads `output/*.json` and renders OpenAI Batch API JSONL files.

Target output:

1. `output/batches/batch-00001.jsonl`
2. additional shards as needed

If you want Structured Outputs for `/v1/responses`, render with `--schema-file path/to/schema.py` where the file defines `class mySchema(BaseModel):`.

This split is deliberate: prompt tuning should not require reparsing the raw emails.

### Stage 3: `submit-batch`

This stage uploads one rendered shard to OpenAI Batch API, polls it to a terminal state, downloads output and error reports, and writes submission metrics under `batch_output/`.

Example:

```bash
uv run python -m email_analyzer submit-batch --batch-jsonl output/batches/batch-00001.jsonl
uv run python -m email_analyzer submit-batch --batch-jsonl output/batches/batch-00001.jsonl --prompt-from-file prompts/classify_email.md
uv run python -m email_analyzer submit-batch --resume-batch-id batch_abc123
uv run python -m email_analyzer submit-batch --batch-jsonl output/batches/batch-00001.jsonl --no-wait
```

If a prompt override is supplied, `submit-batch` rewrites the shard in place by updating `body.instructions` before upload.

See:

1. `docs/batch_submission.md`
2. `docs/batch_state_machine.md`

### Stage 4: `batch-output-to-xlsx`

This stage reads a successful downloaded `batch_output.jsonl` file and writes one `.xlsx` row per email.

The output includes:

1. `filename` from OpenAI `custom_id`
2. one column per field in the structured output object

If you pass `--schema-file`, the spreadsheet column order follows `mySchema`, including any field aliases such as `from`.

Example:

```bash
uv run python -m email_analyzer batch-output-to-xlsx \
  --input-jsonl benchmarks/spamassassin/runs/output_w8_structured/batch_output/batch-00001/batch_output.jsonl \
  --output-xlsx benchmarks/spamassassin/runs/output_w8_structured/batch_output/batch-00001/batch_output.xlsx \
  --schema-file docs/structured_output_schema_example.py
```

### Mailbox Flattening For Benchmarks

Some public corpora are distributed as `mbox` or `mbox`-style monthly archives rather than one message per file.

Use `flatten-mailbox` to turn those archives into the repository's normal flat `input/` layout:

```bash
uv run python -m email_analyzer flatten-mailbox --source "benchmarks/python_list_2026_03/downloads/python-list-2026-03.mbox.gz" --output "benchmarks/python_list_2026_03/input" --filename-prefix "python-list-2026-03"
uv run python -m email_analyzer flatten-mailbox --source "benchmarks/ubuntu_devel_2026_03/downloads/ubuntu-devel-2026-03.txt.gz" --output "benchmarks/ubuntu_devel_2026_03/input" --filename-prefix "ubuntu-devel-2026-03"
```

The command supports plain mailbox files and `.gz`-compressed mailbox archives.

## Detailed Pipeline

### 1. Parse Source

Parse the raw email bytes with the stdlib `email` parser and record:

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

For every text-bearing part:

1. decode transfer encoding
2. decode charset using the declared charset first
3. use `charset-normalizer` only if the declared charset fails or is missing
4. convert HTML to visible text
5. normalize whitespace, line endings, and HTML entities

If stdlib parsing or MIME decoding is badly degraded on a particular message, `ripMIME` is an acceptable fallback tool for part extraction and salvage work.

### 4. Choose Canonical Body

Pick one best body representation from `multipart/alternative` or related body structures.

The canonical body is the anchor for duplicate suppression.

### 5. Filter Duplicate Body Representations

This is the key reduction step.

After normalization, compare every text-bearing part to the canonical body and suppress parts that are only another rendering of the same content.

Examples:

1. plain-text and HTML siblings with the same visible text
2. an attached `body.html` file containing the same visible message text
3. duplicate inline text parts inside `multipart/related`

Suggested heuristic order:

1. exact normalized-text hash match
2. high-similarity `rapidfuzz` match
3. MIME-context bonus when the part is clearly an alternative or related wrapper

Do not auto-drop:

1. `message/rfc822`
2. attachments with materially more text than the canonical body
3. documents that are only partially similar

Every dropped part should retain a reason code such as `duplicate_body_representation`.

### 6. Strip Quotes And Signatures

Run quote stripping only on the surviving body text. The working assumption is:

1. HTML has already been converted to text
2. `mail-parser-reply` is used on normalized plain text

### 7. Triage Attachments

Keep only attachments that are still plausible LLM inputs.

The first pass should favor:

1. attached messages
2. text attachments
3. small structured text files such as CSV

Binary documents can be added later.

### 8. Detect Language

Run language detection only on text that survived dedupe and triage.

### 9. Rank And Pack

Rank surviving text blocks and assemble the reduced LLM payload.

### 10. Write Output

Write a single JSON artifact per email using the original filename as the identifier.

## Parallelism

The pipeline is designed for file-level parallelism.

Default strategy:

1. one worker processes one email end-to-end
2. no nested per-part parallelism in the first pass
3. use a `ProcessPoolExecutor` by default
4. write per-email outputs atomically with temp-file-then-rename

This keeps logging, timing, failure isolation, and deterministic outputs simple.

## Logging And Metrics

The pipeline must log both actions and timings.

### Per-Step Logging

Every major step logs:

1. which file was processed
2. what action was taken
3. what was kept or dropped
4. how long the step took

### Per-File Logging

Every file gets a final summary with:

1. total wall-clock duration
2. per-step durations
3. success or failure status
4. key counters such as parts kept and parts dropped

### Planned Log Files

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

## Output Layout

Planned runtime layout:

```text
input/
  *.eml

output/
  <filename>.json
  <filename>.error.json
  batch.jsonl
  batches/
    batch-00001.jsonl
    batch-00002.jsonl

logs/
  pipeline.jsonl
  file_summary.jsonl
  step_summary.json
```

`output/<filename>.json` is the primary artifact. `output/batch.jsonl` may be used as a convenience alias when only one batch shard exists, but `output/batches/` is the canonical batch location.

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

## OpenAI Batch Rendering

The repository targets the OpenAI Batch API, but only up to the render stage for now.

Each batch line should target `/v1/responses` and use the original filename as `custom_id`.

Example JSONL line:

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
3. all lines in a batch shard must target the same endpoint
4. shard before OpenAI batch limits are exceeded

## Planned CLI

The intended CLI shape is:

```bash
uv run python -m email_analyzer prepare --input input --output output --logs logs --workers 8
uv run python -m email_analyzer render-batch --processed output --batch-dir output/batches --model gpt-4o-mini
```

The CLI names are part of the planned public interface and should stay stable unless there is a strong reason to change them.

## Parallel Runs

`prepare` supports parallel execution by email file.

Use the `--workers` flag to control the worker count:

```bash
uv run python -m email_analyzer prepare --input input --output output --logs logs --workers 1
uv run python -m email_analyzer prepare --input input --output output --logs logs --workers 8
```

Behavior:

1. one worker processes one email end-to-end
2. outputs are still one JSON file per input email
3. logs remain centralized in `logs/`
4. per-step and per-file timings are recorded for later benchmarking

For benchmarking, compare runs with different `--workers` values while keeping the same corpus and machine.

## Benchmark Notes

The repository has been smoke-benchmarked against the Apache SpamAssassin public corpus.

Corpus used:

1. `20030228_easy_ham`
2. `20030228_easy_ham_2`
3. `20030228_hard_ham`
4. `20030228_spam`
5. `20030228_spam_2`

Flattened benchmark set:

1. `6052` raw email files
2. `32998533` input bytes, about `31.47 MiB`
3. zero processing failures in the current implementation

Measured `prepare` throughput on this machine:

| Workers | Files | Errors | Wall Time | Emails/sec | MiB/sec |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1` | `6052` | `0` | `199.33s` | `30.361` | `0.158` |
| `8` | `6052` | `0` | `45.50s` | `133.002` | `0.692` |
| `8` after language-detection optimization | `6052` | `0` | `40.01s` | `151.260` | `0.787` |

Observed speedup from `1` to `8` workers: about `4.38x`.

Observed speedup from the first `8`-worker run to the optimized `8`-worker run: about `1.14x`.

The hottest steps in this first pass are:

1. `detect_language`
2. `strip_quotes_and_signatures`
3. `write_output`

The first optimization pass changed language detection from full confidence-table calculation to single-language confidence lookup. That improved end-to-end throughput, but `detect_language` remains the dominant hot path.

Example benchmark commands:

```bash
uv run python -m email_analyzer prepare --input "benchmarks/spamassassin/input" --output "benchmarks/spamassassin/runs/output_w1" --logs "benchmarks/spamassassin/runs/logs_w1" --workers 1
uv run python -m email_analyzer prepare --input "benchmarks/spamassassin/input" --output "benchmarks/spamassassin/runs/output_w8" --logs "benchmarks/spamassassin/runs/logs_w8" --workers 8
uv run python -m email_analyzer render-batch --processed "benchmarks/spamassassin/runs/output_w8" --batch-dir "benchmarks/spamassassin/runs/output_w8/batches" --model gpt-4o-mini
```

On the same corpus, `render-batch` produced one `6052`-line JSONL shard of about `33.03 MiB` in about `2.89s`.

Additional `prepare` benchmarks on newer public mailing-list corpora:

| Corpus | Workers | Files | Errors | Input Size | Wall Time | Emails/sec | MiB/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `python-list` March 2026 | `8` | `65` | `0` | `108322` bytes | `2.47s` | `26.316` | `0.042` |
| `ubuntu-devel` March 2026 | `8` | `45` | `0` | `323745` bytes | `2.67s` | `16.854` | `0.116` |
| `binutils` March 2026 | `8` | `396` | `0` | `1940052` bytes | `4.60s` | `86.087` | `0.402` |

These newer corpora show different bottlenecks by content profile:

1. `ubuntu-devel` is slower per byte because longer human discussion threads spend more time in quote stripping and language detection.
2. `binutils` is much faster than `ubuntu-devel` on a per-byte basis because many messages are shorter, more repetitive build notifications.
3. `python-list` mixes plain discussion mail and attached-message style content, and lands between the two in absolute throughput.

## Development Principles

1. keep the dependency set small
2. prefer stdlib first
3. keep processed artifacts deterministic
4. never lose the filename-based identity
5. log everything important enough to audit later
6. optimize for low token volume without silently dropping meaningful content

## Future Work

Later phases may add:

1. OpenAI batch upload and polling
2. result collection keyed by `custom_id`
3. richer `ripMIME` integration for malformed-message salvage paths
4. richer document extraction for PDFs and Office files
5. mailbox container support beyond raw file input
