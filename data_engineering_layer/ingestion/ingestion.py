"""
Ingestion: thin coordinator wiring DataExtractor to a loader.

Responsibility boundary:
    Source -> DataExtractor -> DataFrame -> Loader -> Target

This module holds no format-specific or dataset-specific logic. It only
resolves `loader_class` to one of the existing loader classes, and drives
the extract -> load sequence. Cleaning, validation, transformation,
metadata registration, and orchestration (Dagster etc.) all belong
elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .extractor import DataExtractor, ExtractionError, UnsupportedFileTypeError
from .loader import (
    CSVLoader,
    ExcelLoader,
    GeoPackageLoader,
    GeoJSONLoader,
    LoadError,
    LoadResult,
)


class UnsupportedLoaderClassError(Exception):
    """Raised when `loader_class` does not map to a known loader."""


class IngestionError(Exception):
    """Raised when extraction or loading fails during ingestion."""


@dataclass
class IngestionResult:
    """Outcome of a single extract -> load run."""

    source_path: str
    file_type: str
    loader_class: str
    load_result: LoadResult


class Ingestion:
    """
    Coordinates DataExtractor and a loader class (CSVLoader, ExcelLoader,
    GeoPackageLoader, or GeoJSONLoader) to move a source file into PostgreSQL.

    `loader_class` selects the loader by name — no dataset-specific
    branching. Callers (e.g. an ingestion engine driven by metadata)
    supply source and target details per run.
    """

    LOADER_CLASSES = {
        "CSVLoader": CSVLoader,
        "ExcelLoader": ExcelLoader,
        "GeoPackageLoader": GeoPackageLoader,
        "GeoJSONLoader": GeoJSONLoader,
    }

    def __init__(self, extractor: Optional[DataExtractor] = None):
        self._extractor = extractor or DataExtractor()

    def run(
        self,
        source_path: str,
        file_type: str,
        loader_class: str,
        schema: str,
        table: str,
        load_mode: str = "replace",
        chunk_size: Optional[int] = None,
    ) -> IngestionResult:
        """
        Extract `source_path` and load the result into `schema.table`.

        Raises:
            UnsupportedLoaderClassError: `loader_class` is not recognized.
            IngestionError: extraction or loading failed.
        """
        loader_cls = self._resolve_loader_class(loader_class)

        try:
            df = self._extractor.extract(path=source_path, file_type=file_type)
        except (FileNotFoundError, UnsupportedFileTypeError, ExtractionError) as exc:
            raise IngestionError(f"Extraction failed for '{source_path}': {exc}") from exc

        loader = loader_cls()

        try:
            load_result = loader.load(
                df, schema=schema, table=table, load_mode=load_mode, chunk_size=chunk_size
            )
        except LoadError as exc:
            raise IngestionError(f"Load failed for '{schema}.{table}': {exc}") from exc

        return IngestionResult(
            source_path=source_path,
            file_type=file_type,
            loader_class=loader_class,
            load_result=load_result,
        )

    @classmethod
    def _resolve_loader_class(cls, loader_class: str):
        resolved = cls.LOADER_CLASSES.get(loader_class)
        if resolved is None:
            supported = ", ".join(cls.LOADER_CLASSES)
            raise UnsupportedLoaderClassError(
                f"Unsupported loader class: {loader_class}\nSupported loaders: {supported}"
            )
        return resolved