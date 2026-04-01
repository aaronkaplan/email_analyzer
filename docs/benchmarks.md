# Benchmarks

This document covers benchmark setup and historical throughput notes.

All benchmark inputs and outputs live under `benchmarks/`.

Important local-only note:

1. `benchmarks/*` is gitignored
2. downloaded corpora and benchmark outputs stay on your machine unless you copy them elsewhere

## SpamAssassin End-To-End Walkthrough

The main benchmark corpus used in this repository is the Apache SpamAssassin public corpus.

Corpus subsets used:

1. `20030228_easy_ham`
2. `20030228_easy_ham_2`
3. `20030228_hard_ham`
4. `20030228_spam`
5. `20030228_spam_2`

The source corpus is here:

1. `https://spamassassin.apache.org/old/publiccorpus/`

### 1. Download The Corpus

```bash
mkdir -p benchmarks/spamassassin/downloads

for archive in 20030228_easy_ham 20030228_easy_ham_2 20030228_hard_ham 20030228_spam 20030228_spam_2; do
  curl -L "https://spamassassin.apache.org/old/publiccorpus/${archive}.tar.bz2" \
    -o "benchmarks/spamassassin/downloads/${archive}.tar.bz2"
done
```

### 2. Extract The Archives

```bash
mkdir -p benchmarks/spamassassin/extracted

for archive in 20030228_easy_ham 20030228_easy_ham_2 20030228_hard_ham 20030228_spam 20030228_spam_2; do
  tar -xjf "benchmarks/spamassassin/downloads/${archive}.tar.bz2" \
    -C "benchmarks/spamassassin/extracted"
done
```

### 3. Build The Flat `input/` Layout

SpamAssassin already ships one file per email, so unlike `mbox` corpora you do not need `flatten-mailbox` here.

To make the filenames globally unique inside one shared `input/` directory, prefix each message with its source corpus name:

```bash
mkdir -p benchmarks/spamassassin/input

for corpus in easy_ham easy_ham_2 hard_ham spam spam_2; do
  for file in "benchmarks/spamassassin/extracted/${corpus}"/*; do
    cp "$file" "benchmarks/spamassassin/input/${corpus}__$(basename "$file")"
  done
done
```

If you rerun this from scratch, clear `benchmarks/spamassassin/input/` first so stale files do not linger.

### 4. Configure OpenAI

Create a local `.env` from the example file:

```bash
cp .env.example .env
```

Fill in:

1. `OPENAI_API_KEY`
2. optionally `OPENAI_MODEL`

The CLI auto-loads `.env` for every command. Exported shell variables still take precedence.

Note: `render-batch` still expects `--model` explicitly. `.env` is not used as a CLI default for that flag.

### 5. Run The Local `prepare` Benchmark

```bash
uv run python -m email_analyzer prepare \
  --input "benchmarks/spamassassin/input" \
  --output "benchmarks/spamassassin/runs/output_w8" \
  --logs "benchmarks/spamassassin/runs/logs_w8" \
  --workers 8
```

### 6. Render A Structured Batch Shard

```bash
uv run python -m email_analyzer render-batch \
  --processed "benchmarks/spamassassin/runs/output_w8" \
  --batch-dir "benchmarks/spamassassin/runs/output_w8/batches" \
  --model gpt-5.4-nano \
  --instructions-file "docs/prompt-example.txt" \
  --schema-file "docs/structured_output_schema_example.py"
```

### 7. Submit The Batch And Wait For Results

```bash
uv run python -m email_analyzer submit-batch \
  --batch-jsonl "benchmarks/spamassassin/runs/output_w8/batches/batch-00001.jsonl"
```

That produces output under:

1. `benchmarks/spamassassin/runs/output_w8/batch_output/batch-00001/`

Important files in that directory:

1. `batch_output.jsonl`
2. `batch_summary.json`
3. `batch_final.json`
4. `batch_status_history.jsonl`
5. `batch_errors.jsonl` when request-level errors exist

### 8. Export The Structured Output To `.xlsx`

```bash
uv run python -m email_analyzer batch-output-to-xlsx \
  --input-jsonl "benchmarks/spamassassin/runs/output_w8/batch_output/batch-00001/batch_output.jsonl" \
  --output-xlsx "benchmarks/spamassassin/runs/output_w8/batch_output/batch-00001/batch_output.xlsx" \
  --schema-file "docs/structured_output_schema_example.py"
```

## Historical SpamAssassin Throughput Notes

The current flattened SpamAssassin benchmark set contains:

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

On the same corpus, `render-batch` produced one `6052`-line JSONL shard of about `33.03 MiB` in about `2.89s`.

The hottest steps in this first pass were:

1. `detect_language`
2. `strip_quotes_and_signatures`
3. `write_output`

## Other Public Corpora In This Repository

Additional benchmark directories in this repository include:

1. `benchmarks/python_list_2026_03/`
2. `benchmarks/ubuntu_devel_2026_03/`
3. `benchmarks/binutils_2026_03/`

For `mbox` or `mbox.gz` style corpora, use `flatten-mailbox` first.

Examples:

```bash
uv run python -m email_analyzer flatten-mailbox \
  --source "benchmarks/python_list_2026_03/downloads/python-list-2026-03.mbox.gz" \
  --output "benchmarks/python_list_2026_03/input" \
  --filename-prefix "python-list-2026-03"

uv run python -m email_analyzer flatten-mailbox \
  --source "benchmarks/ubuntu_devel_2026_03/downloads/ubuntu-devel-2026-03.txt.gz" \
  --output "benchmarks/ubuntu_devel_2026_03/input" \
  --filename-prefix "ubuntu-devel-2026-03"
```

Additional `prepare` results recorded earlier:

| Corpus | Workers | Files | Errors | Input Size | Wall Time | Emails/sec | MiB/sec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `python-list` March 2026 | `8` | `65` | `0` | `108322` bytes | `2.47s` | `26.316` | `0.042` |
| `ubuntu-devel` March 2026 | `8` | `45` | `0` | `323745` bytes | `2.67s` | `16.854` | `0.116` |
| `binutils` March 2026 | `8` | `396` | `0` | `1940052` bytes | `4.60s` | `86.087` | `0.402` |

These corpora stress different parts of the pipeline:

1. `ubuntu-devel` is slower per byte because longer discussion threads spend more time in quote stripping and language detection
2. `binutils` is faster per byte because many messages are shorter and more repetitive
3. `python-list` mixes plain discussion mail and attached-message style content, and lands between the two
