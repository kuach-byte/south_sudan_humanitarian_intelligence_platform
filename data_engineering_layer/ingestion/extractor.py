"""
DataExtractor: format-agnostic extraction of tabular data into pandas DataFrames.

Responsibility boundary:
    File -> DataExtractor -> pandas DataFrame

This module intentionally does NOT handle loading, schema/table creation,
cleaning, validation, transformation, or orchestration. Those belong to
downstream components (DataLoader, ingestion engine, dbt, Dagster, etc.).
"""

from __future__ import annotations

import os
from typing import Callable, Dict

import pandas as pd

class UnsupportedFileTypeError(Exception):
    """Raised when `loader_class` does not map to a known loader."""


class ExtractionError(Exception):
    """Raised when a file exists and is of a supported type but fails to parse."""


class DataExtractor:
    """
    Extracts tabular data from a source file into a pandas DataFrame.

    Supported formats: CSV, Excel (.xlsx/.xls), GeoPackage (.gpkg), GeoJSON (.geojson).
    The class holds no dataset-specific knowledge — callers (typically an
    ingestion engine driven by pipeline metadata) supply `path` and
    `file_type` per call.
    """

    # Maps a normalized file_type key to the handler method name.
    _TYPE_ALIASES: Dict[str, str] = {
        "csv": "csv",
        "xlsx": "excel",
        "xls": "excel",
        "excel": "excel",
        "gpkg": "geopackage",
        "geopackage": "geopackage",
        "geojson": "geojson",
    }

    def extract(self, path: str, file_type: str) -> pd.DataFrame:
        """
        Extract tabular data from `path` according to `file_type`.

        Args:
            path: Path to the source file.
            file_type: Format identifier (e.g. "csv", ".xlsx", "GeoPackage").
                Case-insensitive; a leading "." is ignored.

        Returns:
            A pandas DataFrame (GeoDataFrame for GeoPackage sources, which
            is a DataFrame subclass and preserves the geometry column).

        Raises:
            FileNotFoundError: `path` does not point to an existing file.
            UnsupportedFileTypeError: `file_type` is not recognized.
            ExtractionError: The file exists but could not be parsed.
        """
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Source file not found: {path}")

        handler = self._resolve_handler(file_type)

        try:
            return handler(path)
        except (UnsupportedFileTypeError, FileNotFoundError):
            raise
        except ImportError as exc:
            raise ExtractionError(
                f"Missing dependency required to read '{file_type}' file '{path}': {exc}"
            ) from exc
        except Exception as exc:
            raise ExtractionError(
                f"Failed to extract data from '{path}' as '{file_type}': {exc}"
            ) from exc

    def _resolve_handler(self, file_type: str) -> Callable[[str], pd.DataFrame]:
        """Normalize `file_type` and return the corresponding extraction method."""
        normalized = file_type.strip().lower().lstrip(".")
        kind = self._TYPE_ALIASES.get(normalized)

        if kind is None:
            supported = sorted(set(self._TYPE_ALIASES.keys()))
            raise UnsupportedFileTypeError(
                f"Unsupported file_type '{file_type}'. Supported values: {supported}"
            )

        return getattr(self, f"_extract_{kind}")

    def _extract_csv(self, path: str) -> pd.DataFrame:
        """Read a CSV file into a DataFrame."""
        return pd.read_csv(path)

    def _extract_excel(self, path: str) -> pd.DataFrame:
        """Read an Excel (.xlsx/.xls) file into a DataFrame."""
        return pd.read_excel(path)

    def _extract_geopackage(self, path: str) -> pd.DataFrame:
        """Read a GeoPackage (.gpkg) file into a GeoDataFrame, geometry preserved."""
        import geopandas as gpd  # imported lazily so geopandas is optional

        return gpd.read_file(path)

    def _extract_geojson(self, path: str) -> pd.DataFrame:
        """Read a GeoJSON (.geojson) file into a GeoDataFrame, geometry and CRS preserved."""
        import geopandas as gpd  # imported lazily so geopandas is optional

        return gpd.read_file(path)