from __future__ import annotations

import gzip
import mailbox
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import FlattenMailboxConfig


def run_flatten_mailbox(config: FlattenMailboxConfig) -> int:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    prefix = config.filename_prefix or _default_prefix(config.source_path)

    with _materialized_mailbox_path(config.source_path) as mailbox_path:
        archive = mailbox.mbox(mailbox_path, factory=None, create=False)
        try:
            keys = list(archive.keys())
            width = max(6, len(str(len(keys))))

            for index, key in enumerate(keys, start=1):
                target = config.output_dir / f"{prefix}-{index:0{width}d}.eml"
                _write_bytes_atomic(target, archive.get_bytes(key))
        finally:
            archive.close()

    return len(keys)


def _default_prefix(source_path: Path) -> str:
    name = source_path.name
    if name.endswith(".gz"):
        name = Path(name[:-3]).stem
    else:
        name = source_path.stem
    return name.replace("_", "-")


@contextmanager
def _materialized_mailbox_path(source_path: Path) -> Iterator[Path]:
    if source_path.suffix != ".gz":
        yield source_path
        return

    suffix = "".join(source_path.suffixes[:-1]) or ".mbox"
    temp_path: Path | None = None
    try:
        with gzip.open(source_path, "rb") as compressed, tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as handle:
            shutil.copyfileobj(compressed, handle)
            temp_path = Path(handle.name)
        yield temp_path
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def _write_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_bytes(content)
    temp_path.replace(path)
