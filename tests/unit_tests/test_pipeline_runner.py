"""Unit tests for IngestionPipelineRunner / run_ingestion_for_dataset.

All collaborators (repositories, extractor, validator, ingestion
coordinator) are injected as mocks -- no real PostgreSQL connection,
files, or pandas processing. This mirrors the constructor-injection
style already used by data_engineering_layer.ingestion.ingestion (see
test_ingestion.py) and validated by real integration coverage in
test_full_ingestion_pipeline_integration.py.
"""
from unittest.mock import MagicMock

import pytest

from data_engineering_layer.orchestration.pipeline_runner import pipeline_runner as pr
from data_engineering_layer.ingestion.ingestion import IngestionError, IngestionResult
from data_engineering_layer.ingestion.loader import LoadResult
from data_quality.pre_validation import DataQualityError, ValidationResult


def _make_runner(**overrides):
    connection = MagicMock(name="connection")
    collaborators = {
        "dataset_repo": MagicMock(name="dataset_repo"),
        "source_repo": MagicMock(name="source_repo"),
        "target_repo": MagicMock(name="target_repo"),
        "pipeline_config_repo": MagicMock(name="pipeline_config_repo"),
        "extractor": MagicMock(name="extractor"),
        "validator": MagicMock(name="validator"),
        "ingestion": MagicMock(name="ingestion"),
    }
    collaborators.update(overrides)
    runner = pr.IngestionPipelineRunner(connection, **collaborators)
    return runner, collaborators


def _wire_happy_path(collaborators, *, df_len=3, is_valid=True, warnings=None):
    dataset_row = {"id": 42, "name": "widgets"}
    source_row = {"dataset_id": 42, "path": "widgets.csv", "file_type": "csv"}
    target_row = {"dataset_id": 42, "schema": "raw", "table": "widgets"}
    pipeline_config_row = {
        "dataset_id": 42,
        "asset_name": "widgets",
        "loader_class": "CSVLoader",
        "load_mode": "replace",
        "chunk_size": 500,
    }

    collaborators["dataset_repo"].get_by_name.return_value = dataset_row
    collaborators["source_repo"].get_by_dataset_id.return_value = source_row
    collaborators["target_repo"].get_by_dataset_id.return_value = target_row
    collaborators["pipeline_config_repo"].get_by_asset_name.return_value = (
        pipeline_config_row
    )

    df = MagicMock(name="dataframe")
    df.__len__.return_value = df_len
    collaborators["extractor"].extract.return_value = df

    validation = ValidationResult(
        is_valid=is_valid,
        errors=[] if is_valid else ["bad row"],
        warnings=warnings or [],
        checked_rows=df_len,
    )
    collaborators["validator"].validate.return_value = validation

    load_result = LoadResult(
        schema="raw", table="widgets", load_mode="replace", rows_loaded=df_len,
        columns=["id", "name"],
    )
    ingestion_result = IngestionResult(
        source_path="widgets.csv",
        file_type="csv",
        loader_class="CSVLoader",
        load_result=load_result,
    )
    collaborators["ingestion"].run.return_value = ingestion_result

    return dataset_row, source_row, target_row, pipeline_config_row, df, ingestion_result


# ---------------------------------------------------------------------------
# Metadata resolution
# ---------------------------------------------------------------------------


def test_resolve_metadata_success():
    runner, collab = _make_runner()
    dataset_row, source_row, target_row, pipeline_config_row, *_ = _wire_happy_path(collab)

    resolved = runner.resolve_metadata("widgets")

    assert resolved.dataset_id == 42
    assert resolved.dataset_name == "widgets"
    assert resolved.dataset == dataset_row
    assert resolved.source == source_row
    assert resolved.target == target_row
    assert resolved.pipeline_config == pipeline_config_row

    collab["source_repo"].get_by_dataset_id.assert_called_once_with(42)
    collab["target_repo"].get_by_dataset_id.assert_called_once_with(42)
    collab["pipeline_config_repo"].get_by_asset_name.assert_called_once_with("widgets")


def test_resolve_metadata_missing_dataset():
    runner, collab = _make_runner()
    collab["dataset_repo"].get_by_name.return_value = None

    with pytest.raises(pr.MetadataResolutionError, match="not registered"):
        runner.resolve_metadata("ghost")

    collab["source_repo"].get_by_dataset_id.assert_not_called()


@pytest.mark.parametrize(
    "missing_repo, method",
    [
        ("source_repo", "get_by_dataset_id"),
        ("target_repo", "get_by_dataset_id"),
        ("pipeline_config_repo", "get_by_asset_name"),
    ],
)
def test_resolve_metadata_missing_piece_raises(missing_repo, method):
    runner, collab = _make_runner()
    _wire_happy_path(collab)
    getattr(collab[missing_repo], method).return_value = None

    with pytest.raises(pr.MetadataResolutionError):
        runner.resolve_metadata("widgets")


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


def test_run_success_end_to_end():
    runner, collab = _make_runner()
    _, source_row, target_row, pipeline_config_row, df, ingestion_result = (
        _wire_happy_path(collab)
    )

    result = runner.run("widgets")

    collab["extractor"].extract.assert_called_once_with(
        path=source_row["path"], file_type=source_row["file_type"]
    )
    collab["validator"].validate.assert_called_once_with(df, rules=None)
    collab["ingestion"].run.assert_called_once_with(
        source_path=source_row["path"],
        file_type=source_row["file_type"],
        loader_class=pipeline_config_row["loader_class"],
        schema=target_row["schema"],
        table=target_row["table"],
        load_mode=pipeline_config_row["load_mode"],
        chunk_size=pipeline_config_row["chunk_size"],
    )

    assert result.dataset_name == "widgets"
    assert result.rows_extracted == 3
    assert result.validation.is_valid is True
    assert result.ingestion is ingestion_result
    assert result.rows_loaded == 3
    assert result.target_schema == "raw"
    assert result.target_table == "widgets"


def test_run_passes_validation_rules_through():
    runner, collab = _make_runner()
    _wire_happy_path(collab)
    rules = {"latitude": {"min": -90, "max": 90}}

    runner.run("widgets", validation_rules=rules)

    collab["validator"].validate.assert_called_once()
    _, kwargs = collab["validator"].validate.call_args
    assert kwargs["rules"] is rules


def test_run_defaults_load_mode_when_pipeline_config_omits_it():
    runner, collab = _make_runner()
    _, source_row, target_row, pipeline_config_row, *_ = _wire_happy_path(collab)
    pipeline_config_row["load_mode"] = None

    runner.run("widgets")

    _, kwargs = collab["ingestion"].run.call_args
    assert kwargs["load_mode"] == "replace"


def test_run_raises_on_invalid_by_default():
    runner, collab = _make_runner()
    _wire_happy_path(collab, is_valid=False)

    with pytest.raises(DataQualityError):
        runner.run("widgets")

    collab["ingestion"].run.assert_not_called()


def test_run_can_skip_raising_on_invalid():
    runner, collab = _make_runner()
    _wire_happy_path(collab, is_valid=False)

    result = runner.run("widgets", raise_on_invalid=False)

    assert result.validation.is_valid is False
    collab["ingestion"].run.assert_called_once()


def test_run_propagates_ingestion_error():
    runner, collab = _make_runner()
    _wire_happy_path(collab)
    collab["ingestion"].run.side_effect = IngestionError("disk full")

    with pytest.raises(IngestionError):
        runner.run("widgets")


def test_run_wraps_extraction_failure_as_ingestion_error():
    runner, collab = _make_runner()
    _wire_happy_path(collab)
    collab["extractor"].extract.side_effect = FileNotFoundError("nope")

    with pytest.raises(IngestionError, match="Extraction failed"):
        runner.run("widgets")

    collab["validator"].validate.assert_not_called()
    collab["ingestion"].run.assert_not_called()


def test_run_propagates_metadata_resolution_error_before_extraction():
    runner, collab = _make_runner()
    collab["dataset_repo"].get_by_name.return_value = None

    with pytest.raises(pr.MetadataResolutionError):
        runner.run("ghost")

    collab["extractor"].extract.assert_not_called()


# ---------------------------------------------------------------------------
# run_ingestion_for_dataset() convenience function
# ---------------------------------------------------------------------------


def test_run_ingestion_for_dataset_constructs_runner(monkeypatch):
    fake_runner = MagicMock(name="runner")
    fake_result = MagicMock(name="result")
    fake_runner.run.return_value = fake_result
    runner_cls = MagicMock(name="IngestionPipelineRunner", return_value=fake_runner)
    monkeypatch.setattr(pr, "IngestionPipelineRunner", runner_cls)

    connection = MagicMock(name="connection")
    result = pr.run_ingestion_for_dataset(
        connection, "widgets", raise_on_invalid=False
    )

    runner_cls.assert_called_once_with(connection)
    fake_runner.run.assert_called_once_with(
        "widgets", validation_rules=None, raise_on_invalid=False
    )
    assert result is fake_result