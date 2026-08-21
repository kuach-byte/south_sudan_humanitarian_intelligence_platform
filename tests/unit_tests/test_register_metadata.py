"""Unit tests for register_metadata.main().

Exercises the real main() orchestration logic. All external dependencies
(DB connection, repository classes, load_dotenv) are mocked — no real
PostgreSQL connection, YAML files, or metadata tables are touched.
"""
import logging
from unittest.mock import MagicMock

import psycopg2
import pytest

from data_engineering_layer.metadata_manager.execution import register_metadata as rm
from data_engineering_layer.metadata_manager.OOP.baserepo import RepositoryError

REPO_ORDER = (
    "DatasetRepository",
    "SourceRepository",
    "TargetRepository",
    "PipelineConfigRepository",
)


@pytest.fixture
def db_env(monkeypatch):
    monkeypatch.setenv("DB_HOST", "test-host")
    monkeypatch.setenv("DB_NAME", "test-db")
    monkeypatch.setenv("DB_USER", "test-user")
    monkeypatch.setenv("DB_PASSWORD", "test-pass")


@pytest.fixture
def mock_conn(monkeypatch):
    conn = MagicMock(name="connection")
    monkeypatch.setattr(rm.psycopg2, "connect", MagicMock(return_value=conn))
    return conn


@pytest.fixture
def mock_dotenv(monkeypatch):
    load_dotenv_mock = MagicMock()
    monkeypatch.setattr(rm, "load_dotenv", load_dotenv_mock)
    return load_dotenv_mock


@pytest.fixture
def mock_repos(monkeypatch):
    """Patch the four repository classes. Returns a factory: call it to
    install the patches and get back {class_name: {"class": ..., "instance": ...}}.
    Default register_from_directory() result is a 2-item list per repo.
    """

    def _install(counts=None):
        counts = counts or {name: 2 for name in REPO_ORDER}
        installed = {}
        for name in REPO_ORDER:
            instance = MagicMock(name=f"{name}_instance")
            instance.register_from_directory.return_value = [object()] * counts[name]
            cls_mock = MagicMock(name=name, return_value=instance)
            monkeypatch.setattr(rm, name, cls_mock)
            installed[name] = {"class": cls_mock, "instance": instance}
        return installed

    return _install


def test_main_success(db_env, mock_conn, mock_dotenv, mock_repos, capsys):
    repos = mock_repos()

    rm.main()

    mock_dotenv.assert_called_once()
    rm.psycopg2.connect.assert_called_once_with(
        host="test-host", dbname="test-db", user="test-user", password="test-pass"
    )

    for name in REPO_ORDER:
        repos[name]["class"].assert_called_once_with(mock_conn)
        repos[name]["instance"].register_from_directory.assert_called_once_with(
            rm.METADATA_DIR
        )

    mock_conn.close.assert_called_once()

    out = capsys.readouterr().out
    assert "Metadata registration started." in out
    assert "Datasets:" in out
    assert "Sources:" in out
    assert "Targets:" in out
    assert "Pipeline configurations:" in out
    assert out.count("Registered: 2") == 4
    assert "Metadata registration completed successfully." in out


def test_main_database_connection_failure(db_env, mock_conn, mock_dotenv, mock_repos, caplog):
    rm.psycopg2.connect.side_effect = psycopg2.Error("connection refused")
    repos = mock_repos()

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            rm.main()

    assert exc_info.value.code == 1
    for name in REPO_ORDER:
        repos[name]["class"].assert_not_called()
    assert any("could not connect" in msg for msg in caplog.messages)


@pytest.mark.parametrize("failing_repo", REPO_ORDER)
def test_main_repository_registration_failure(
    failing_repo, db_env, mock_conn, mock_dotenv, mock_repos, caplog
):
    repos = mock_repos()
    repos[failing_repo]["instance"].register_from_directory.side_effect = RepositoryError(
        "bad metadata"
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            rm.main()

    assert exc_info.value.code == 1
    mock_conn.close.assert_called_once()
    assert any("could not proceed" in msg for msg in caplog.messages)

    failing_index = REPO_ORDER.index(failing_repo)
    for i, name in enumerate(REPO_ORDER):
        register = repos[name]["instance"].register_from_directory
        if i <= failing_index:
            register.assert_called_once()
        else:
            register.assert_not_called()


def test_main_uses_default_environment_variables(monkeypatch, mock_conn, mock_dotenv, mock_repos):
    for var in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    mock_repos()

    rm.main()

    rm.psycopg2.connect.assert_called_once_with(
        host="localhost", dbname="humanitarian_db", user=None, password=None
    )
