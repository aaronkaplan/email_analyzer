from __future__ import annotations

import os
from pathlib import Path

from email_analyzer import cli


def test_autoload_dotenv_loads_repo_root_file(monkeypatch, tmp_path: Path) -> None:
    package_dir = tmp_path / "repo" / "src" / "email_analyzer"
    package_dir.mkdir(parents=True)
    cli_file = package_dir / "cli.py"
    cli_file.write_text("# test placeholder\n", encoding="utf-8")
    (tmp_path / "repo" / ".env").write_text(
        "OPENAI_API_KEY=from-dotenv\n", encoding="utf-8"
    )

    monkeypatch.setattr(cli, "__file__", str(cli_file))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    cli._autoload_dotenv()

    assert os.environ["OPENAI_API_KEY"] == "from-dotenv"


def test_autoload_dotenv_does_not_override_existing_environment(
    monkeypatch, tmp_path: Path
) -> None:
    package_dir = tmp_path / "repo" / "src" / "email_analyzer"
    package_dir.mkdir(parents=True)
    cli_file = package_dir / "cli.py"
    cli_file.write_text("# test placeholder\n", encoding="utf-8")
    (tmp_path / "repo" / ".env").write_text(
        "OPENAI_API_KEY=from-dotenv\n", encoding="utf-8"
    )

    monkeypatch.setattr(cli, "__file__", str(cli_file))
    monkeypatch.setenv("OPENAI_API_KEY", "already-set")

    cli._autoload_dotenv()

    assert os.environ["OPENAI_API_KEY"] == "already-set"


def test_autoload_dotenv_falls_back_to_cwd_search(monkeypatch, tmp_path: Path) -> None:
    package_dir = tmp_path / "repo" / "src" / "email_analyzer"
    package_dir.mkdir(parents=True)
    cli_file = package_dir / "cli.py"
    cli_file.write_text("# test placeholder\n", encoding="utf-8")

    cwd = tmp_path / "workspace"
    cwd.mkdir()
    dotenv_path = cwd / ".env"
    dotenv_path.write_text("OPENAI_MODEL=from-cwd\n", encoding="utf-8")

    monkeypatch.setattr(cli, "__file__", str(cli_file))
    monkeypatch.delenv("OPENAI_MODEL", raising=False)

    monkeypatch.chdir(cwd)
    cli._autoload_dotenv()

    assert os.environ["OPENAI_MODEL"] == "from-cwd"


def test_autoload_dotenv_is_noop_when_missing(monkeypatch, tmp_path: Path) -> None:
    package_dir = tmp_path / "repo" / "src" / "email_analyzer"
    package_dir.mkdir(parents=True)
    cli_file = package_dir / "cli.py"
    cli_file.write_text("# test placeholder\n", encoding="utf-8")

    monkeypatch.setattr(cli, "__file__", str(cli_file))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)

    cli._autoload_dotenv()

    assert "OPENAI_API_KEY" not in os.environ
