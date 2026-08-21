"""
pipeline_config.py

PipelineConfigRepository for the metadata-driven ingestion framework.

Encapsulates SQL queries specific to the `pipeline_config` metadata table.
Generic CRUD is inherited from BaseRepository.

`pipeline_config` is Dagster asset-oriented: each row configures a single
Dagster asset (`asset_name`), not a named pipeline. This repository only
persists/looks up that configuration -- no execution, no orchestration.
"""

import logging
from pathlib import Path
from typing import Any, Union

import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

from data_engineering_layer.metadata_manager.OOP.baserepo import (
    BaseRepository,
    RepositoryError,
)
from data_engineering_layer.metadata_manager.OOP.datasets import DatasetRepository
from data_engineering_layer.metadata_manager.OOP.yml_parser import YamlParser

logger = logging.getLogger(__name__)



class PipelineConfigRepository(BaseRepository):
    """Repository for the `pipeline_config` metadata table."""

    def __init__(self, connection: PGConnection) -> None:
        super().__init__(
            connection=connection,
            schema="metadata_manager",
            table_name="pipeline_config",
            primary_key="pipeline_config_id",
        )
        self._dataset_repo = DatasetRepository(connection)


    def register(self, pipeline_config: Union[dict[str, Any], Any]) -> dict[str, Any]:
        """
        Register a pipeline_config row, matched on (dataset_id, asset_name).
        Returns the existing record if already registered, else creates one.
        """
        data = pipeline_config.to_dict() if hasattr(pipeline_config, "to_dict") else pipeline_config

        if not data.get("dataset_id"):
            raise RepositoryError("PipelineConfig dataset_id is required.")
        if not data.get("asset_name"):
            raise RepositoryError("PipelineConfig asset_name is required.")

        if self.is_registered(data):
            logger.info(
                "Pipeline config already registered for dataset_id=%s, asset_name=%s",
                data["dataset_id"], data["asset_name"],
            )
            return self.get_by_asset_name(data["asset_name"])

        return self.create(data)

    def is_registered(self, pipeline_config: dict[str, Any]) -> bool:
        """True if a row exists for this (dataset_id, asset_name)."""
        if not pipeline_config.get("dataset_id") or not pipeline_config.get("asset_name"):
            return False
        return self.exists({
            "dataset_id": pipeline_config["dataset_id"],
            "asset_name": pipeline_config["asset_name"],
        })

    def get_by_dataset_id(self, dataset_id: Any) -> list[dict[str, Any]]:
        """All pipeline_config rows for a dataset (a dataset may back several assets)."""
        query = sql.SQL("SELECT * FROM {table} WHERE {column} = %s").format(
            table=self._table_identifier(), column=sql.Identifier("dataset_id"),
        )
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, [dataset_id])
                return [dict(row) for row in cursor.fetchall()]
        except psycopg2.Error as exc:
            raise RepositoryError(f"Failed to fetch pipeline_config for dataset_id '{dataset_id}': {exc}") from exc

    def get_by_asset_name(self, asset_name: str):
        """Single pipeline_config row by asset_name, or None."""
        query = sql.SQL("SELECT * FROM {table} WHERE {column} = %s").format(
            table=self._table_identifier(), column=sql.Identifier("asset_name"),
        )
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, [asset_name])
                row = cursor.fetchone()
                return dict(row) if row else None
        except psycopg2.Error as exc:
            raise RepositoryError(f"Failed to fetch pipeline_config for asset_name '{asset_name}': {exc}") from exc

    def get_all_configs(self) -> list[dict[str, Any]]:
        """Return all registered pipeline configurations."""
        query = sql.SQL("SELECT * FROM {table} ORDER BY {column}").format(
            table=self._table_identifier(),
            column=sql.Identifier("pipeline_id"),
        )

        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except psycopg2.Error as exc:
            raise RepositoryError(
                f"Failed to fetch pipeline configurations: {exc}"
            ) from exc


    def register_from_directory(
        self,
        metadata_dir: Union[str, Path] = "metadata",
    ) -> list[dict[str, Any]]:
        """
        Parse YAML files under metadata_dir, resolve dataset_id + asset_name,
        and register a pipeline_config row per file. Skips (with a log) files
        missing a valid/registered dataset, and duplicates already registered.
        """
        parser = YamlParser()
        try:
            parsed_files = parser.parse_directory(metadata_dir)
        except (ValueError, OSError) as exc:
            raise RepositoryError(str(exc)) from exc

        inserted: list[dict[str, Any]] = []

        for yml_path, parsed in parsed_files:
            try:
                dataset_block = parsed.get("dataset")
                loader_block = parsed.get("loader") or {}

                if not isinstance(dataset_block, dict):
                    raise ValueError("missing top-level 'dataset' block")

                dataset_name = dataset_block.get("name")
                if not dataset_name:
                    raise ValueError("'dataset' block is missing required 'name' field")

                dataset_id = self._dataset_repo.get_id(dataset_name)
                if dataset_id is None:
                    raise ValueError(
                        f"dataset '{dataset_name}' is not registered; "
                        "register it before its pipeline configuration"
                    )

                asset_name = dataset_name

                data = {
                    "dataset_id": dataset_id,
                    "asset_name": asset_name,
                    "priority": dataset_block.get("priority"),
                    "loader_class": loader_block.get("class"),
                    "chunk_size": loader_block.get("chunk_size"), 
                    "load_mode": loader_block.get("load_mode"),
                }

                if self.is_registered(data):
                    logger.info(
                        "Skipping pipeline config for dataset '%s' (%s): already registered",
                        dataset_name, yml_path,
                    )
                    continue

                record = self.register(data)
                inserted.append(record)
                logger.info(
                    "Registered pipeline config for dataset '%s' (asset_name='%s') from %s",
                    dataset_name, asset_name, yml_path,
                )

            except (ValueError, RepositoryError) as exc:
                logger.error("Failed to process metadata file '%s': %s", yml_path, exc)
                continue

        return inserted

