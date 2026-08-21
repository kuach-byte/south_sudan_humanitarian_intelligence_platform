"""
DataLoader: loads an already-extracted pandas DataFrame into PostgreSQL.

Responsibility boundary:
    DataFrame -> DataLoader -> PostgreSQL table

This module intentionally does NOT handle cleaning, validation, profiling,
transformation, dbt execution, Dagster orchestration, or metadata
registration. It only knows: a DataFrame, a target schema/table, and a
load mode. Dataset identity (health facilities, IPC, climate, ...) is not
known here — that lives in metadata supplied by the caller.

Three thin, format-specific loaders (CSVLoader, ExcelLoader,
GeoPackageLoader) sit on top of one shared implementation
(_BasePostgreSQLLoader), mirroring DataExtractor's per-format surface so
an ingestion engine can dispatch on file_type symmetrically for both
extraction and loading. All three behave identically today —
`_prepare_dataframe` already detects and handles a geometry column when
one is present (GeoPackage-sourced GeoDataFrames) and is a no-op
otherwise — so no subclass overrides anything yet. A subclass is where
format-specific behavior would go later (e.g. a PostGIS-native path for
GeoPackageLoader) without touching the other two.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

import geopandas as gpd
from geoalchemy2 import Geometry
from sqlalchemy import create_engine

# --------------------------------------------------------------------------
# Exceptions
# --------------------------------------------------------------------------

class LoadError(Exception):
    """Base exception for all loader failures. Carries the target for context."""

    def __init__(self, message: str, schema: Optional[str] = None, table: Optional[str] = None):
        self.schema = schema
        self.table = table
        target = f" [target={schema}.{table}]" if schema and table else ""
        super().__init__(f"{message}{target}")


class ConnectionFailedError(LoadError):
    """Raised when a connection to PostgreSQL cannot be established."""


class InvalidIdentifierError(LoadError):
    """Raised when a schema/table name is not a safe SQL identifier."""


class UnsupportedLoadModeError(LoadError):
    """Raised when `load_mode` is not one of the supported modes."""


class EmptyDataFrameError(LoadError):
    """Raised when a DataFrame with no rows is passed to a mode that requires rows."""


# --------------------------------------------------------------------------
# Result object
# --------------------------------------------------------------------------

@dataclass
class LoadResult:
    """Information about a completed load operation."""

    schema: str
    table: str
    load_mode: str
    rows_loaded: int
    columns: list


# --------------------------------------------------------------------------
# Database configuration
# --------------------------------------------------------------------------

def _get_connection_params() -> dict:
    """
    Read PostgreSQL connection parameters from environment variables.

    Expects DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD. Loads a
    project-root `.env` file first (if python-dotenv is installed and one
    exists) so this works the same way whether variables were exported in
    the shell or kept in `.env`. If your project already has a central
    config/settings module for these values, swap this function out for
    that instead of maintaining two sources of truth.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise ConnectionFailedError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )

    return {
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }


# --------------------------------------------------------------------------
# Shared PostgreSQL implementation
# --------------------------------------------------------------------------

logger = logging.getLogger(__name__)


def _escape_percent(composed_sql_fragment: str) -> str:
    """
    Double any literal '%' in an already-composed SQL fragment.

    `psycopg2.extras.execute_values` takes a plain string and re-parses it
    looking for a bare '%s' placeholder to expand into the batched VALUES
    list. If a fragment built with `sql.Composed.as_string()` (as our
    INSERT statements are, so identifiers can be safely quoted) contains a
    literal '%' — e.g. from a source column named "Population %" or
    "% Change" — that re-parse misreads it as the start of a Python-style
    format directive and raises `ValueError: unsupported format
    character`. Doubling '%' to '%%' here (standard %-escaping) makes it
    survive that re-parse as a literal percent sign again.

    IMPORTANT: call this only on the identifier/prefix portion of a
    statement, never on the final string that still needs its own intact
    `%s` placeholder — escaping that too would corrupt it into `%%s` and
    break execute_values entirely. Build the prefix, escape it, then
    append " VALUES %s" (or the row `template`) afterward.
    """
    return composed_sql_fragment.replace("%", "%%")


# Maps pandas dtype kind -> PostgreSQL column type. Deliberately small and
# conservative; anything not recognized falls back to TEXT.
_DTYPE_TO_PG = {
    "i": "BIGINT",
    "u": "BIGINT",
    "f": "DOUBLE PRECISION",
    "b": "BOOLEAN",
    "M": "TIMESTAMP",
}


class _BasePostgreSQLLoader:
    """
    Loads a DataFrame into a PostgreSQL table.

    Connection parameters are read from the environment (see
    `_get_connection_params`). Schema/table names are validated and
    quoted as SQL identifiers — never string-concatenated — so they are
    safe even when they originate from metadata.

    Not instantiated directly — use CSVLoader, ExcelLoader, or
    GeoPackageLoader, which all share this implementation.
    """

    LOAD_MODES = {"replace", "append"}
    DEFAULT_CHUNK_SIZE = 1000

    def __init__(self, connection_params: Optional[dict] = None):
        self._connection_params = connection_params or _get_connection_params()

    def load(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        load_mode: str = "replace",
        chunk_size: Optional[int] = None,
    ) -> LoadResult:
        """
        `chunk_size` controls how many rows are sent per INSERT batch
        (passed through to psycopg2's `execute_values` as `page_size`).
        Defaults to `DEFAULT_CHUNK_SIZE`; pass a larger value for fewer
        round trips on big loads, or a smaller one to bound memory/packet
        size. This only affects batching — the full DataFrame is still
        loaded in one `load()` call.
        """
        if load_mode not in self.LOAD_MODES:
            raise UnsupportedLoadModeError(
                f"Unsupported load_mode '{load_mode}'. Supported: {sorted(self.LOAD_MODES)}",
                schema, table,
            )
        self._validate_identifier(schema, "schema")
        self._validate_identifier(table, "table")
        chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        if chunk_size <= 0:
            raise LoadError(f"chunk_size must be a positive integer, got {chunk_size}", schema, table)

        if df.empty:
            raise EmptyDataFrameError("Cannot load an empty DataFrame", schema, table)

        working_df = self._prepare_dataframe(df)

        try:
            conn = psycopg2.connect(**self._connection_params)
        except psycopg2.OperationalError as exc:
            raise ConnectionFailedError(f"Could not connect to PostgreSQL: {exc}", schema, table) from exc

        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
                    )

                    if load_mode == "replace":
                        self._replace_load(cur, schema, table, working_df)
                    else:  # append
                        if not self._table_exists(cur, schema, table):
                            self._create_table(cur, schema, table, working_df)

                    # Handles the case where a pre-existing table (replace
                    # via TRUNCATE, or append onto an existing table) is
                    # missing a column the source now has — see
                    # _reconcile_columns()'s docstring. A no-op on a table
                    # that was just freshly created above.
                    self._reconcile_columns(cur, schema, table, working_df)

                    rows_loaded = self._insert_rows(cur, schema, table, working_df, chunk_size)
        except LoadError:
            raise
        except Exception as exc:
            raise LoadError(f"Load failed: {exc}", schema, table) from exc
        finally:
            conn.close()

        return LoadResult(
            schema=schema,
            table=table,
            load_mode=load_mode,
            rows_loaded=rows_loaded,
            columns=list(working_df.columns),
        )

    # -- identifier safety -------------------------------------------------

    @staticmethod
    def _validate_identifier(name: str, kind: str) -> None:
        """Reject anything that isn't a plain, safe SQL identifier."""
        if not name or not isinstance(name, str):
            raise InvalidIdentifierError(f"Invalid {kind} name: {name!r}")
        if not (name[0].isalpha() or name[0] == "_"):
            raise InvalidIdentifierError(f"Invalid {kind} name: {name!r}")
        if not all(c.isalnum() or c == "_" for c in name):
            raise InvalidIdentifierError(f"Invalid {kind} name: {name!r}")

    # -- table lifecycle -----------------------------------------------------

    @staticmethod
    def _table_exists(cur, schema: str, table: str) -> bool:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table),
        )
        return cur.fetchone()[0]

    @staticmethod
    def _drop_table(cur, schema: str, table: str) -> None:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )

    @staticmethod
    def _truncate_table(cur, schema: str, table: str) -> None:
        """
        Empty an existing table without dropping it.

        Preferred over DROP+CREATE for `load_mode="replace"`: dropping the
        table breaks any downstream view (e.g. a dbt staging view) that
        depends on it, forcing a DROP ... CASCADE that silently destroys
        those objects on every load. TRUNCATE clears all rows in place and
        leaves the table's identity — and anything built on top of it —
        intact.
        """
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )

    def _replace_load(self, cur, schema: str, table: str, df: pd.DataFrame) -> None:
        """
        Prepare a table for a `load_mode="replace"` load.

        If the table already exists, TRUNCATE it in place so dependent
        views survive. If it doesn't exist yet, create it fresh.

        Note: TRUNCATE assumes the incoming DataFrame's columns match the
        existing table's columns. If the source schema drifts (columns
        added/removed/renamed), the subsequent insert will fail loudly on
        a column mismatch rather than silently loading partial data —
        surfacing schema evolution issues instead of masking them.
        """
        if self._table_exists(cur, schema, table):
            self._truncate_table(cur, schema, table)
        else:
            self._create_table(cur, schema, table, df)


    @staticmethod
    def _get_existing_columns(cur, schema: str, table: str) -> set:
        """Return the set of column names currently on schema.table."""
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}

    def _reconcile_columns(self, cur, schema: str, table: str, df: pd.DataFrame) -> None:
        """
        Add any DataFrame columns missing from an existing table.

        This only covers the additive case (a new column appeared in the
        source). It deliberately does NOT drop columns that exist in the
        table but not in `df`, and does NOT attempt to detect a renamed
        or retyped column — those are ambiguous from column names alone
        and are left to fail loudly at insert time (or to a future,
        more deliberate schema-migration step) rather than being guessed
        at silently here.

        Only relevant when the table already existed before this load
        (a `load_mode="replace"` TRUNCATE, or an `load_mode="append"`
        onto a pre-existing table) — a freshly created table already
        matches `df` by construction.
        """
        existing = self._get_existing_columns(cur, schema, table)
        missing = [col for col in df.columns if col not in existing]

        if not missing:
            return

        logger.warning(
            "Table '%s.%s' is missing column(s) present in the source: %s. "
            "Adding them as nullable columns.",
            schema, table, missing,
        )

        for col in missing:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(col),
                    sql.SQL(self._pg_type_for(df[col])),
                )
            )

    def _create_table(self, cur, schema: str, table: str, df: pd.DataFrame) -> None:
        columns_sql = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(self._pg_type_for(df[col])))
            for col in df.columns
        )
        cur.execute(
            sql.SQL("CREATE TABLE {}.{} ({})").format(
                sql.Identifier(schema), sql.Identifier(table), columns_sql
            )
        )

    @staticmethod
    def _pg_type_for(series: pd.Series) -> str:
        return _DTYPE_TO_PG.get(series.dtype.kind, "TEXT")

    @staticmethod
    def _insert_rows(cur, schema: str, table: str, df: pd.DataFrame, chunk_size: int) -> int:
        columns_sql = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
        insert_prefix = sql.SQL("INSERT INTO {}.{} ({})").format(
            sql.Identifier(schema), sql.Identifier(table), columns_sql
        )
        # Escape any literal '%' baked into quoted column identifiers
        # (e.g. a column named "Population %") before appending the
        # VALUES %s placeholder execute_values needs intact to inject
        # the batched rows. See _escape_percent()'s docstring.
        insert_stmt = _escape_percent(insert_prefix.as_string(cur)) + " VALUES %s"

        # object dtype columns may hold NaN; psycopg2 needs None for NULL.
        records = [
            tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False, name=None)
        ]
        execute_values(cur, insert_stmt, records, page_size=chunk_size)
        return len(records)

    # -- geometry handling (isolated from the generic path) -----------------

    @staticmethod
    def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Generic DataFrame preparation.

        CSV and Excel loaders use this unchanged. Spatial handling is
        implemented by GeoPackageLoader rather than converting geometry
        into WKT here.
        """
        return df

# --------------------------------------------------------------------------
# Format-specific loaders (all share _BasePostgreSQLLoader as-is)
# --------------------------------------------------------------------------

class CSVLoader(_BasePostgreSQLLoader):
    """Loads a CSV-sourced DataFrame into PostgreSQL."""


class ExcelLoader(_BasePostgreSQLLoader):
    """Loads an Excel-sourced DataFrame into PostgreSQL."""


class GeoPackageLoader(_BasePostgreSQLLoader):
    """
    Loads a GeoPackage-sourced GeoDataFrame into PostgreSQL with native
    PostGIS geometry.

    Ordinary columns are loaded using the normal PostgreSQL type mapping.
    The GeoDataFrame geometry column is loaded as a PostGIS geometry with
    the GeoDataFrame's CRS/SRID.
    """

    def load(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        load_mode: str = "replace",
        chunk_size: Optional[int] = None,
    ) -> LoadResult:

        if not isinstance(df, gpd.GeoDataFrame):
            raise LoadError(
                "GeoPackageLoader requires a GeoDataFrame",
                schema,
                table,
            )

        geometry_col = df.geometry.name

        if df.crs is None:
            raise LoadError(
                "GeoPackage geometry has no CRS; cannot determine SRID",
                schema,
                table,
            )

        srid = df.crs.to_epsg()

        if srid is None:
            raise LoadError(
                f"Could not determine EPSG/SRID from CRS: {df.crs}",
                schema,
                table,
            )

        if geometry_col not in df.columns:
            raise LoadError(
                f"Geometry column '{geometry_col}' not found in DataFrame",
                schema,
                table,
            )

        if df.empty:
            raise EmptyDataFrameError(
                "Cannot load an empty GeoDataFrame",
                schema,
                table,
            )

        self._validate_identifier(schema, "schema")
        self._validate_identifier(table, "table")

        chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE

        if chunk_size <= 0:
            raise LoadError(
                f"chunk_size must be a positive integer, got {chunk_size}",
                schema,
                table,
            )

        try:
            conn = psycopg2.connect(**self._connection_params)
        except psycopg2.OperationalError as exc:
            raise ConnectionFailedError(
                f"Could not connect to PostgreSQL: {exc}",
                schema,
                table,
            ) from exc

        try:
            with conn:
                with conn.cursor() as cur:

                    # Ensure PostGIS is available.
                    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")

                    cur.execute(
                        sql.SQL(
                            "CREATE SCHEMA IF NOT EXISTS {}"
                        ).format(sql.Identifier(schema))
                    )

                    if load_mode not in self.LOAD_MODES:
                        raise UnsupportedLoadModeError(
                            f"Unsupported load_mode '{load_mode}'. "
                            f"Supported: {sorted(self.LOAD_MODES)}",
                            schema,
                            table,
                        )

                    table_exists = self._table_exists(cur, schema, table)

                    if load_mode == "replace" and table_exists:
                        # TRUNCATE in place rather than DROP+CREATE, so any
                        # downstream view (e.g. a dbt staging view) built on
                        # this spatial table survives the reload. See
                        # _replace_load()'s docstring on _BasePostgreSQLLoader
                        # for the same rationale on the non-spatial path.
                        self._truncate_table(cur, schema, table)
                    elif not table_exists:
                        self._create_spatial_table(
                            cur=cur,
                            schema=schema,
                            table=table,
                            gdf=df,
                            geometry_col=geometry_col,
                            srid=srid,
                        )

                    rows_loaded = self._insert_spatial_rows(
                        cur=cur,
                        schema=schema,
                        table=table,
                        gdf=df,
                        geometry_col=geometry_col,
                        chunk_size=chunk_size,
                    )

                    self._create_spatial_index(
                        cur=cur,
                        schema=schema,
                        table=table,
                        geometry_col=geometry_col,
                    )

        except LoadError:
            raise

        except Exception as exc:
            raise LoadError(
                f"Spatial load failed: {exc}",
                schema,
                table,
            ) from exc

        finally:
            conn.close()

        return LoadResult(
            schema=schema,
            table=table,
            load_mode=load_mode,
            rows_loaded=rows_loaded,
            columns=list(df.columns),
        )

    def _create_spatial_table(
        self,
        cur,
        schema: str,
        table: str,
        gdf: gpd.GeoDataFrame,
        geometry_col: str,
        srid: int,
    ) -> None:
        """
        Create a PostgreSQL table where the GeoDataFrame geometry column
        becomes a native PostGIS geometry column.
        """

        columns_sql = []

        for col in gdf.columns:

            if col == geometry_col:
                geometry_type = self._geometry_type(gdf)

                column_type = (
                    f"geometry({geometry_type},{srid})"
                )

            else:
                column_type = self._pg_type_for(gdf[col])

            columns_sql.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(col),
                    sql.SQL(column_type),
                )
            )

        create_stmt = sql.SQL(
            "CREATE TABLE {}.{} ({})"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(columns_sql),
        )

        cur.execute(create_stmt)

    @staticmethod
    def _geometry_type(gdf: gpd.GeoDataFrame) -> str:
        """
        Determine the PostGIS geometry type from the GeoDataFrame.

        If all geometries share the same type, preserve that specific type.
        If multiple geometry types are present, use the generic PostGIS
        Geometry type so the raw ingestion layer can preserve the source data.
        """

        geometry_types = gdf.geometry.geom_type.dropna().unique()

        if len(geometry_types) == 0:
            raise LoadError("GeoDataFrame contains no valid geometries")

        if len(geometry_types) == 1:
            return geometry_types[0]

        return "Geometry"

    @staticmethod
    def _insert_spatial_rows(
        cur,
        schema: str,
        table: str,
        gdf: gpd.GeoDataFrame,
        geometry_col: str,
        chunk_size: int,
    ) -> int:
        """
        Insert attributes and geometry into the PostGIS table.

        Geometry is converted to WKB for transport and reconstructed by
        PostGIS using ST_GeomFromWKB.
        """

        columns = list(gdf.columns)

        columns_sql = sql.SQL(", ").join(
            sql.Identifier(column) for column in columns
        )

        value_placeholders = []
        for column in columns:
            if column == geometry_col:
                value_placeholders.append(sql.SQL("ST_GeomFromWKB(%s, %s)"))
            else:
                value_placeholders.append(sql.SQL("%s"))

        # execute_values needs the per-row shape as a separate `template`,
        # not folded into the main statement — it only tolerates a single
        # bare %s in `insert_stmt` itself (the spot it substitutes the
        # whole batched VALUES list into).
        row_template = sql.SQL("({})").format(
            sql.SQL(", ").join(value_placeholders)
        )

        insert_prefix = sql.SQL(
            "INSERT INTO {}.{} ({})"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            columns_sql,
        )
        # Escape any literal '%' baked into quoted column identifiers
        # before appending the VALUES %s placeholder execute_values needs
        # intact. See _escape_percent()'s docstring.
        insert_stmt = _escape_percent(insert_prefix.as_string(cur)) + " VALUES %s"

        records = []
        srid = gdf.crs.to_epsg()

        for row in gdf.itertuples(index=False, name=None):
            values = []
            for column, value in zip(columns, row):
                if column == geometry_col:
                    if value is None or value is pd.NA:
                        values.extend([None, srid])
                    else:
                        values.extend([value.wkb, srid])
                else:
                    values.append(None if pd.isna(value) else value)
            records.append(tuple(values))

        execute_values(
            cur,
            insert_stmt,
            records,
            template=row_template.as_string(cur),
            page_size=chunk_size,
        )

        return len(records)

    @staticmethod
    def _create_spatial_index(
        cur,
        schema: str,
        table: str,
        geometry_col: str,
    ) -> None:
        """
        Create a GiST spatial index on the geometry column.
        """

        index_name = f"{table}_{geometry_col}_gist_idx"

        cur.execute(
            sql.SQL(
                "CREATE INDEX IF NOT EXISTS {} "
                "ON {}.{} USING GIST ({})"
            ).format(
                sql.Identifier(index_name),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(geometry_col),
            )
        )


class GeoJSONLoader(GeoPackageLoader):
    """
    Loads a GeoJSON-sourced GeoDataFrame into PostgreSQL with native
    PostGIS geometry.

    DataExtractor's `_extract_geojson` produces a `GeoDataFrame` with the
    same shape as the GeoPackage path (attribute columns + a `geometry`
    column carrying a CRS), so this subclass reuses
    `GeoPackageLoader.load` and its supporting spatial methods
    (`_create_spatial_table`, `_insert_spatial_rows`, `_geometry_type`,
    `_create_spatial_index`) unchanged rather than duplicating them.
    Geometry is preserved as native PostGIS geometry (via
    `ST_GeomFromWKB`), never converted to WKT/text, exactly as for
    GeoPackage sources.
    """