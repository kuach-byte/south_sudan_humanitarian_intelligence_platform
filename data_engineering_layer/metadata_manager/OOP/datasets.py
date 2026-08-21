"""
dataset.py

DatasetRepository for the metadata-driven ingestion framework.

Encapsulates SQL queries specific to the `dataset` metadata table.
All generic CRUD behavior (create, update, delete, get_by_id, exists)
is inherited from BaseRepository and reused here rather than
reimplemented.
"""

import logging
from pathlib import Path
from typing import Any, Optional, Union

import psycopg2
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

from data_engineering_layer.metadata_manager.OOP.baserepo import (
    BaseRepository,
    RepositoryError,
)
from data_engineering_layer.metadata_manager.OOP.yml_parser import YamlParser
logger = logging.getLogger(__name__)


class DatasetRepository(BaseRepository):
    """
    Repository for the `dataset` metadata table.

    Responsible for encapsulating SQL queries and persistence
    operations specific to dataset records.

    YAML parsing is delegated to YamlParser. This repository
    does not perform pipeline orchestration or metadata validation.
    """

    def __init__(self, connection: PGConnection) -> None:
        """
        Initialize the DatasetRepository.

        Args:
            connection: An open psycopg2 connection.
        """
        super().__init__(
            connection=connection,
            schema="metadata_manager",
            table_name="dataset",
            primary_key="id",
        )


    def register(self, dataset: Union[dict[str, Any], Any]) -> dict[str, Any]:
        data = dataset.to_dict() if hasattr(dataset, "to_dict") else dataset

        name = data.get("name")
        if not name:
            raise RepositoryError("Dataset name is required.")

        existing = self.get_by_name(name)

        if existing:
            logger.info("Dataset '%s' already exists.", name)
            return existing

        return self.create(data)

    def get_by_name(self, dataset_name: str) -> Optional[dict[str, Any]]:
        """
        Retrieve a dataset by its unique name.

        Args:
            dataset_name: The unique name of the dataset.

        Returns:
            The dataset as a dictionary, or None if not found.

        Raises:
            RepositoryError: If the query fails.
        """
        query = sql.SQL("SELECT * FROM {table} WHERE {column} = %s").format(
            table=self._table_identifier(),
            column=sql.Identifier("name"),
        )
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, [dataset_name])
                row = cursor.fetchone()
                return dict(row) if row else None
        except psycopg2.Error as exc:
            raise RepositoryError(f"Failed to fetch dataset '{dataset_name}': {exc}") from exc

    def get_id(self, dataset_name: str) -> Optional[Any]:
        """
        Retrieve only the dataset_id for a dataset.
 
        Reuses get_by_name() rather than issuing a second, near-identical
        SELECT ... WHERE dataset_name = %s query.
 
        Args:
            dataset_name: The unique name of the dataset.
 
        Returns:
            The dataset_id, or None if the dataset does not exist.
 
        Raises:
            RepositoryError: If the underlying query fails.
        """
        record = self.get_by_name(dataset_name)
        return record[self._primary_key] if record else None

    def is_registered(self, dataset_name: str) -> bool:
        """
        Determine whether a dataset has already been registered.

        Args:
            dataset_name: The unique name of the dataset.

        Returns:
            True if a dataset with this name exists, False otherwise.
        """
        return self.exists({"name": dataset_name})

    def update_metadata(self, dataset_id: Any, updates: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Update dataset metadata after the YAML configuration changes.

        Args:
            dataset_id: Primary key of the dataset to update.
            updates: Column-value pairs to update.

        Returns:
            The updated dataset record, or None if no row matched.

        Raises:
            RepositoryError: If the update fails.
        """
        return self.update(dataset_id, updates)

    def register_from_directory(
        self,
        metadata_dir: Union[str, Path] = "metadata",
    ) -> list[dict[str, Any]]:
        """
        Scan a directory tree for dataset metadata YAML files and register
        each dataset that isn't already present in the `dataset` table.

        YAML file discovery and parsing are delegated to YamlParser.
        This method is responsible for interpreting the `dataset` block
        and registering the resulting metadata through this repository.

        Duplicates are matched on `name` via is_registered(). A dataset
        that is already registered is skipped rather than updated.

        A file that is missing the `dataset` block, missing a `name`, or
        fails to register is logged and skipped.

        Args:
            metadata_dir: Root directory to scan for *.yml files recursively.

        Returns:
            The dataset records that were newly inserted.

        Raises:
            RepositoryError: If metadata_dir does not exist or isn't a directory.
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

                if not isinstance(dataset_block, dict):
                    raise ValueError(
                        "missing top-level 'dataset' block"
                    )

                name = dataset_block.get("name")

                if not name:
                    raise ValueError(
                        "'dataset' block is missing required 'name' field"
                    )

                if self.is_registered(name):
                    logger.info(
                        "Skipping '%s' (%s): dataset already registered",
                        name,
                        yml_path,
                    )
                    continue

                data = {
                    "name": name,
                    "description": dataset_block.get("description"),
                    "owner": dataset_block.get("owner"),
                    "priority": dataset_block.get("priority"),
                }

                record = self.register(data)
                inserted.append(record)

                logger.info(
                    "Registered dataset '%s' from %s",
                    name,
                    yml_path,
                )

            except (ValueError, RepositoryError) as exc:
                logger.error(
                    "Failed to process metadata file '%s': %s",
                    yml_path,
                    exc,
                )
                continue

        return inserted
