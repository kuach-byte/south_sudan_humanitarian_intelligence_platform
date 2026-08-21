"""
pipeline_runner.py

Reusable, metadata-driven ingestion execution layer.

This module is the single entry point that ties together:

    DatasetRepository / SourceRepository / TargetRepository /
    PipelineConfigRepository   (metadata resolution)
        -> DataExtractor                (extraction)
        -> PreValidator                 (pre-load validation)
        -> Ingestion                    (load into PostgreSQL/PostGIS)

into one callable unit, keyed only by a dataset's registered `name`
(which doubles as `pipeline_config.asset_name`).

It holds no Dagster-specific code and imports nothing from Dagster.
Dagster is expected to sit *above* this module: an `@asset` function
(or `@multi_asset`, sensor, schedule, etc.) is expected to be a thin
wrapper that does little more than:

    @asset
    def health_facilities(context) -> None:
        with get_connection() as conn:
            result = IngestionPipelineRunner(conn).run("health_facilities")
            context.add_output_metadata({
                "rows_loaded": result.rows_loaded,
                "target": f"{result.target_schema}.{result.target_table}",
            })

Nothing in here opens or closes a shared connection implicitly beyond
what's passed in — connection lifecycle stays the caller's
responsibility, mirroring how the metadata repositories already work.
This keeps the module equally usable from a plain script, a pytest
integration test, or a Dagster asset body without modification.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Optional

import pandas as pd
from psycopg2.extensions import connection as PGConnection

from data_engineering_layer.metadata_manager.OOP.baserepo import RepositoryError
from data_engineering_layer.metadata_manager.OOP.datasets import DatasetRepository
from data_engineering_layer.metadata_manager.OOP.source import SourceRepository
from data_engineering_layer.metadata_manager.OOP.target import TargetRepository
from data_engineering_layer.metadata_manager.OOP.pipeline_config import (
    PipelineConfigRepository,
)

from data_engineering_layer.ingestion.extractor import (
    DataExtractor,
    ExtractionError,
    UnsupportedFileTypeError,
)
from data_engineering_layer.ingestion.ingestion import (
    Ingestion,
    IngestionError,
    IngestionResult,
    UnsupportedLoaderClassError,
)
from data_quality.pre_validation import (
    DataQualityError,
    PreValidator,
    ValidationResult,
)

logger = logging.getLogger(__name__)


class MetadataResolutionError(Exception):
    """
    Raised when a dataset's registered metadata is missing or incomplete.

    Distinct from RepositoryError (a DB/query failure): this means the
    query succeeded fine, but the dataset/source/target/pipeline_config
    row an asset asked for simply isn't registered yet — e.g. metadata
    registration hasn't been run, or a YAML file was malformed and
    silently skipped by register_from_directory().
    """


@dataclass
class ResolvedMetadata:
    """The full metadata bundle for a single dataset, resolved from the
    `metadata_manager` schema."""

    dataset_id: Any
    dataset_name: str
    dataset: dict[str, Any]
    source: dict[str, Any]
    target: dict[str, Any]
    pipeline_config: dict[str, Any]


@dataclass
class PipelineRunResult:
    """Outcome of a single metadata-driven ingestion run."""

    dataset_name: str
    metadata: ResolvedMetadata
    rows_extracted: int
    validation: ValidationResult
    ingestion: IngestionResult

    @property
    def rows_loaded(self) -> int:
        return self.ingestion.load_result.rows_loaded

    @property
    def target_schema(self) -> str:
        return self.ingestion.load_result.schema

    @property
    def target_table(self) -> str:
        return self.ingestion.load_result.table


class IngestionPipelineRunner:
    """
    Runs the full metadata -> extract -> validate -> load pipeline for a
    single dataset, identified only by its registered name.

    This is the class an eventual Dagster asset should call — it *is*
    the "ingestion engine" referred to elsewhere in this codebase as
    pre-Dagster. It depends on nothing Dagster-specific, so it can be
    unit tested and driven from a plain script in exactly the shape it
    will later be driven from an asset body.

    All collaborators (repositories, extractor, validator, ingestion
    coordinator) are constructor-injectable so tests can substitute
    mocks without patching module internals, matching how the rest of
    this codebase is tested (see test_ingestion.py, test_register_metadata.py).
    """

    def __init__(
        self,
        connection: PGConnection,
        *,
        dataset_repo: Optional[DatasetRepository] = None,
        source_repo: Optional[SourceRepository] = None,
        target_repo: Optional[TargetRepository] = None,
        pipeline_config_repo: Optional[PipelineConfigRepository] = None,
        extractor: Optional[DataExtractor] = None,
        validator: Optional[PreValidator] = None,
        ingestion: Optional[Ingestion] = None,
    ) -> None:
        self._connection = connection
        self._dataset_repo = dataset_repo or DatasetRepository(connection)
        self._source_repo = source_repo or SourceRepository(connection)
        self._target_repo = target_repo or TargetRepository(connection)
        self._pipeline_config_repo = pipeline_config_repo or PipelineConfigRepository(
            connection
        )
        self._extractor = extractor or DataExtractor()
        self._validator = validator or PreValidator()
        self._ingestion = ingestion or Ingestion(extractor=self._extractor)

    # -- metadata resolution -------------------------------------------------

    def resolve_metadata(self, dataset_name: str) -> ResolvedMetadata:
        """
        Resolve everything needed to run a dataset's pipeline from the
        `metadata_manager` schema: the dataset row, its source row, its
        target row, and its pipeline_config row (keyed by
        asset_name == dataset_name).

        Args:
            dataset_name: The dataset's registered `name`.

        Returns:
            A ResolvedMetadata bundle.

        Raises:
            MetadataResolutionError: any piece of metadata is missing.
            RepositoryError: an underlying query failed.
        """
        dataset = self._dataset_repo.get_by_name(dataset_name)
        if dataset is None:
            raise MetadataResolutionError(
                f"Dataset '{dataset_name}' is not registered."
            )
        dataset_id = dataset["id"]

        source = self._source_repo.get_by_dataset_id(dataset_id)
        if source is None:
            raise MetadataResolutionError(
                f"No source registered for dataset '{dataset_name}' (id={dataset_id})."
            )

        target = self._target_repo.get_by_dataset_id(dataset_id)
        if target is None:
            raise MetadataResolutionError(
                f"No target registered for dataset '{dataset_name}' (id={dataset_id})."
            )

        pipeline_config = self._pipeline_config_repo.get_by_asset_name(dataset_name)
        if pipeline_config is None:
            raise MetadataResolutionError(
                f"No pipeline_config registered for dataset '{dataset_name}' "
                f"(expected asset_name='{dataset_name}')."
            )

        return ResolvedMetadata(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            dataset=dataset,
            source=source,
            target=target,
            pipeline_config=pipeline_config,
        )

    # -- run -------------------------------------------------------------------

    def run(
        self,
        dataset_name: str,
        *,
        validation_rules: Optional[Mapping[str, Mapping[str, Any]]] = None,
        raise_on_invalid: bool = True,
    ) -> PipelineRunResult:
        """
        Run the full pipeline for a single dataset: resolve metadata,
        extract, pre-validate, and load.

        Args:
            dataset_name: The dataset's registered `name`, which also
                doubles as `pipeline_config.asset_name`.
            validation_rules: Optional per-column rules forwarded to
                PreValidator.validate() (see pre_validation.py). Where
                these come from (asset config, a metadata table column,
                a YAML block) is a decision for the Dagster layer above
                this one — this module takes them as a plain mapping
                and has no opinion on their source.
            raise_on_invalid: If True (default), a failing
                ValidationResult raises DataQualityError and the load is
                never attempted — mirroring the "raising is the
                orchestrator's decision" contract documented on
                DataQualityError itself. If False, the caller is
                responsible for inspecting `result.validation` — useful
                for a Dagster asset check that wants to observe/report
                failures without hard-failing the whole run.

        Returns:
            A PipelineRunResult with the resolved metadata, extracted
            row count, validation result, and ingestion result.

        Raises:
            MetadataResolutionError: required metadata isn't registered.
            DataQualityError: validation failed and raise_on_invalid=True.
            IngestionError: extraction or loading failed.
        """
        metadata = self.resolve_metadata(dataset_name)

        logger.info(
            "Running pipeline for dataset '%s' (id=%s): %s -> %s.%s",
            dataset_name,
            metadata.dataset_id,
            metadata.source["path"],
            metadata.target["schema"],
            metadata.target["table"],
        )

        df = self._extract(metadata)

        validation = self._validator.validate(df, rules=validation_rules)
        if not validation.is_valid:
            logger.warning(
                "Pre-load validation failed for dataset '%s': %s",
                dataset_name,
                "; ".join(validation.errors),
            )
            if raise_on_invalid:
                raise DataQualityError(validation.errors)
        elif validation.warnings:
            logger.info(
                "Pre-load validation passed for dataset '%s' with warnings: %s",
                dataset_name,
                "; ".join(validation.warnings),
            )

        ingestion_result = self._load(metadata)

        logger.info(
            "Pipeline complete for dataset '%s': %d row(s) loaded into %s.%s",
            dataset_name,
            ingestion_result.load_result.rows_loaded,
            ingestion_result.load_result.schema,
            ingestion_result.load_result.table,
        )

        return PipelineRunResult(
            dataset_name=dataset_name,
            metadata=metadata,
            rows_extracted=len(df),
            validation=validation,
            ingestion=ingestion_result,
        )

    # -- internal steps ----------------------------------------------------

    def _extract(self, metadata: ResolvedMetadata) -> pd.DataFrame:
        try:
            return self._extractor.extract(
                path=metadata.source["path"],
                file_type=metadata.source["file_type"],
            )
        except (FileNotFoundError, UnsupportedFileTypeError, ExtractionError) as exc:
            raise IngestionError(
                f"Extraction failed for dataset '{metadata.dataset_name}' "
                f"('{metadata.source['path']}'): {exc}"
            ) from exc

    def _load(self, metadata: ResolvedMetadata) -> IngestionResult:
        pipeline_config = metadata.pipeline_config
        target = metadata.target
        source = metadata.source

        # UnsupportedLoaderClassError and IngestionError both propagate
        # unchanged -- Ingestion.run() already produces clear, targeted
        # error messages; wrapping them again here would just add noise.
        return self._ingestion.run(
            source_path=source["path"],
            file_type=source["file_type"],
            loader_class=pipeline_config["loader_class"],
            schema=target["schema"],
            table=target["table"],
            load_mode=pipeline_config.get("load_mode") or "replace",
            chunk_size=pipeline_config.get("chunk_size"),
        )


def run_ingestion_for_dataset(
    connection: PGConnection,
    dataset_name: str,
    *,
    validation_rules: Optional[Mapping[str, Mapping[str, Any]]] = None,
    raise_on_invalid: bool = True,
) -> PipelineRunResult:
    """
    Convenience module-level function for one-off/scripted runs, and the
    simplest possible shape for a future Dagster asset body:

        @asset
        def my_dataset(context) -> None:
            with get_connection() as conn:
                result = run_ingestion_for_dataset(conn, "my_dataset")
                context.add_output_metadata({"rows_loaded": result.rows_loaded})

    Equivalent to
    `IngestionPipelineRunner(connection).run(dataset_name, ...)` with
    default (real, non-injected) repositories/extractor/validator/
    ingestion collaborators. Use IngestionPipelineRunner directly when
    you need to inject test doubles or reuse one runner across several
    dataset names (e.g. a Dagster @multi_asset iterating pipeline_config
    rows) without re-instantiating the repositories each time.
    """
    runner = IngestionPipelineRunner(connection)
    return runner.run(
        dataset_name,
        validation_rules=validation_rules,
        raise_on_invalid=raise_on_invalid,
    )