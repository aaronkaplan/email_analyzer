# Batch Submission

`submit-batch` uploads one rendered OpenAI Batch JSONL shard, creates the batch, polls it to a terminal state, downloads the output and error reports, and writes local metrics under `batch_output/`.

## Requirements

1. `OPENAI_API_KEY` must be set.
2. The batch shard must already target `/v1/responses`.
3. The batch shard must contain one model only.

## Basic Usage

Submit an existing shard as-is:

```bash
uv run python -m email_analyzer submit-batch \
  --batch-jsonl output/batches/batch-00001.jsonl
```

Resume monitoring an already-created batch:

```bash
uv run python -m email_analyzer submit-batch \
  --resume-batch-id batch_abc123
```

Override the prompt inline before submission:

```bash
uv run python -m email_analyzer submit-batch \
  --batch-jsonl output/batches/batch-00001.jsonl \
  --prompt "Classify the email and return structured JSON."
```

Override the prompt from a file:

```bash
uv run python -m email_analyzer submit-batch \
  --batch-jsonl output/batches/batch-00001.jsonl \
  --prompt-from-file prompts/classify_email.md
```

Adjust polling interval or output directory:

```bash
uv run python -m email_analyzer submit-batch \
  --batch-jsonl output/batches/batch-00001.jsonl \
  --output-dir output/batch_output/batch-00001 \
  --poll-interval-seconds 10
```

Render a batch shard with Structured Outputs enabled before submission:

```bash
uv run python -m email_analyzer render-batch \
  --processed output \
  --batch-dir output/batches \
  --model gpt-4o-mini \
  --schema-file docs/structured_output_schema_example.py

uv run python -m email_analyzer submit-batch \
  --batch-jsonl output/batches/batch-00001.jsonl
```

Submit without waiting for terminal state:

```bash
uv run python -m email_analyzer submit-batch \
  --batch-jsonl output/batches/batch-00001.jsonl \
  --no-wait
```

Resume an existing batch but only record the current snapshot:

```bash
uv run python -m email_analyzer submit-batch \
  --resume-batch-id batch_abc123 \
  --no-wait
```

## Prompt Override Behavior

If `--prompt` or `--prompt-from-file` is supplied, `submit-batch` rewrites the source `--batch-jsonl` in place before upload.

Only `body.instructions` is changed.

The command preserves:

1. `custom_id`
2. `method`
3. `url`
4. `body.model`
5. `body.input`

The rewrite is atomic.

For auditability, the command also copies the pre-rewrite file into `batch_output/`.

Prompt overrides are only allowed when creating a new submission from `--batch-jsonl`.

They are rejected with `--resume-batch-id`.

## Structured Outputs

Structured Outputs are supported in batch mode when the shard targets `/v1/responses` and each request body includes `text.format` with a JSON schema.

`render-batch` can generate that request shape for you.

Use `--schema-file` to point to a Python file that defines exactly one Pydantic schema class named `mySchema`:

```bash
uv run python -m email_analyzer render-batch \
  --processed output \
  --batch-dir output/batches \
  --model gpt-4o-mini \
  --schema-file docs/structured_output_schema_example.py
```

Requirements for the schema file:

1. it must be a `.py` file
2. it must define `class mySchema(BaseModel):`
3. the class must be a Pydantic model so `model_json_schema()` is available

Example schema file:

```python
from pydantic import BaseModel, Field


class mySchema(BaseModel):
    category: str = Field(description="High-level email category")
    priority: str = Field(description="Priority label such as low, medium, or high")
    summary: str = Field(description="One short summary of the email")
    action_items: list[str] = Field(default_factory=list, description="Concrete requested follow-up actions")
```

There is also a sample checked into the repository at:

1. `docs/structured_output_schema_example.py`

When `--schema-file` is supplied, `render-batch` adds this to each `/v1/responses` request body:

```json
{
  "text": {
    "format": {
      "type": "json_schema",
      "name": "mySchema",
      "schema": {"type": "object", "properties": "..."},
      "strict": true
    }
  }
}
```

After submission, the batch output file will contain normal batch result lines whose `response.body` payload should conform to that schema for successful requests.

The current `submit-batch` command uploads and downloads those files as-is. It does not yet post-parse the structured payload into local typed objects.

## Validation

Before upload, the submitter validates:

1. the file exists
2. each line is valid JSON
3. each line is a JSON object
4. each line has `custom_id`, `method`, `url`, and `body`
5. every request uses `POST`
6. every request targets `/v1/responses`
7. every request has a valid `body.model`
8. `custom_id` values are unique
9. the batch uses only one model

## Live Status Output

During synchronous polling, the command displays a `rich` progress view similar to a lightweight `tqdm` status line.

Displayed fields include:

1. current batch state
2. processed requests
3. total requests
4. successful requests
5. failed requests
6. remaining requests
7. percent complete
8. elapsed wall time
9. time spent in the current observed state
10. batch id suffix

Typical states are:

1. `validating`
2. `in_progress`
3. `finalizing`
4. terminal state such as `completed`, `failed`, `expired`, or `cancelled`

If `--no-wait` is used, the command records and prints a single observed snapshot instead of polling until terminal state.

## Output Layout

If `--output-dir` is omitted, the default output directory is:

1. `<batch-parent>/batch_output/<batch-stem>/` for general paths
2. `<run-root>/batch_output/<batch-stem>/` when the shard lives under `.../batches/`
3. `batch_output/<batch-id>/` by default when resuming via `--resume-batch-id`

Example output layout:

```text
batch_output/
  batch-00001/
    submission.json
    batch_input.before_submit.jsonl
    batch_input.submitted.jsonl
    batch_status_history.jsonl
    batch_final.json
    batch_output.jsonl
    batch_errors.jsonl
    batch_summary.json
```

Files:

1. `submission.json`: local submission metadata such as source shard, prompt source, input file id, and batch id
2. `batch_input.before_submit.jsonl`: backup of the original shard before in-place prompt rewrite
3. `batch_input.submitted.jsonl`: the exact shard content that was uploaded
4. `batch_status_history.jsonl`: one record per poll snapshot
5. `batch_final.json`: final batch object from OpenAI
6. `batch_output.jsonl`: downloaded successful responses, if present
7. `batch_errors.jsonl`: downloaded per-request errors, if present
8. `batch_summary.json`: derived metrics and counts

## Summary Metrics

The final summary includes:

1. terminal batch status
2. batch id
3. input file id
4. output file id
5. error file id
6. total request counts
7. number of successful processed mails
8. number of failed mails
9. total elapsed time
10. per-state durations
11. output and error line counts
12. poll count
13. waiting mode

`waiting_mode` is one of:

1. `submitted`: the command returned after one observed snapshot because `--no-wait` was used
2. `completed`: the command waited until a terminal state

## Error Handling

The submitter handles:

1. missing `OPENAI_API_KEY`
2. malformed JSONL
3. duplicate `custom_id`
4. wrong endpoint or method
5. mixed models in one shard
6. upload or batch creation failures
7. validation failures reported by OpenAI
8. partial-output terminal states such as `expired`
9. downloading OpenAI error reports when available

Even for non-success terminal states, the submitter writes local artifacts so the failure remains auditable.
