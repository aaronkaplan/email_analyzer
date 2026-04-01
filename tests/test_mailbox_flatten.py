from __future__ import annotations

import gzip
from pathlib import Path

from email_analyzer.config import FlattenMailboxConfig
from email_analyzer.mailbox_flatten import run_flatten_mailbox


def test_flatten_mailbox_writes_one_eml_per_message(tmp_path: Path) -> None:
    source_path = tmp_path / "sample.mbox.gz"
    output_dir = tmp_path / "input"
    mailbox_text = (
        "From sender1@example.com Sat Mar  1 00:00:00 2026\n"
        "From: sender1@example.com\n"
        "Subject: First\n"
        "\n"
        "Hello one\n"
        "\n"
        "From sender2@example.com Sat Mar  1 00:00:01 2026\n"
        "From: sender2@example.com\n"
        "Subject: Second\n"
        "\n"
        "Hello two\n"
    )
    with gzip.open(source_path, "wb") as handle:
        handle.write(mailbox_text.encode("utf-8"))

    count = run_flatten_mailbox(
        FlattenMailboxConfig(
            source_path=source_path,
            output_dir=output_dir,
            filename_prefix="sample",
        )
    )

    assert count == 2

    flattened = sorted(output_dir.glob("*.eml"))
    assert [path.name for path in flattened] == ["sample-000001.eml", "sample-000002.eml"]
    assert flattened[0].read_text(encoding="utf-8") == "From: sender1@example.com\nSubject: First\n\nHello one\n"
    assert flattened[1].read_text(encoding="utf-8") == "From: sender2@example.com\nSubject: Second\n\nHello two\n"
