"""
Integration test for the metadata-driven ingestion framework, exercised
end-to-end against a real PostgreSQL/PostGIS database, for all four
currently supported source formats: CSV, GeoPackage, Excel, and GeoJSON.

Flow under test (per parametrized case):

    metadata YAML
        -> DatasetRepository / SourceRepository / TargetRepository /
           PipelineConfigRepository (real registration, no mocks)
        -> metadata_manager PostgreSQL tables
        -> pipeline configuration resolved from those tables
        -> DataExtractor
        -> DataFrame / GeoDataFrame
        -> PreValidator
        -> Ingestion (resolves + runs the configured Loader from metadata)
        -> PostgreSQL / PostGIS target table

Dagster is intentionally not involved here; this test is the pre-Dagster
integration gate described in the project's metadata_manager modules.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
import yaml

try:
    import geopandas as gpd
    from shapely.geometry import Point
except ImportError:  # pragma: no cover - geopandas is required for the gpkg case
    gpd = None
    Point = None

from data_engineering_layer.metadata_manager.OOP.datasets import DatasetRepository
from data_engineering_layer.metadata_manager.OOP.source import SourceRepository
from data_engineering_layer.metadata_manager.OOP.target import TargetRepository
from data_engineering_layer.metadata_manager.OOP.pipeline_config import (
    PipelineConfigRepository,
)

# --- see "IMPORT-PATH ASSUMPTION" above if these four don't match your repo ---
from data_engineering_layer.ingestion.extractor import DataExtractor
from data_engineering_layer.ingestion.ingestion import Ingestion
from data_quality.pre_validation import PreValidator
from data_engineering_layer.ingestion.loader import _get_connection_params


# ---------------------------------------------------------------------------
# Database connection (session-scoped; skips the whole module if unavailable)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def db_connection_params() -> dict:
    try:
        return _get_connection_params()
    except Exception as exc:  # ConnectionFailedError or missing env vars
        pytest.skip(f"PostgreSQL connection parameters unavailable: {exc}")


@pytest.fixture(scope="session")
def db_connection(db_connection_params):
    try:
        conn = psycopg2.connect(**db_connection_params)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Could not connect to PostgreSQL: {exc}")
    yield conn
    conn.close()


@pytest.fixture()
def db_cursor(db_connection):
    cur = db_connection.cursor()
    yield cur
    db_connection.rollback()
    cur.close()


# ---------------------------------------------------------------------------
# Metadata repositories (real implementations, no mocking)
# ---------------------------------------------------------------------------

@pytest.fixture()
def repos(db_connection):
    return {
        "dataset": DatasetRepository(db_connection),
        "source": SourceRepository(db_connection),
        "target": TargetRepository(db_connection),
        "pipeline_config": PipelineConfigRepository(db_connection),
    }


# ---------------------------------------------------------------------------
# Small deterministic fixture data, generated at test time (no checked-in
# binaries, no network, no production data).
# ---------------------------------------------------------------------------

CSV_ROWS = [
    {"id": 1, "name": "alpha", "value": 10.5},
    {"id": 2, "name": "beta", "value": 20.25},
    {"id": 3, "name": "gamma", "value": 30.0},
]

EXCEL_ROWS = [
    {"id": 1, "name": "north", "value": 5.5},
    {"id": 2, "name": "south", "value": 15.75},
    {"id": 3, "name": "east", "value": 25.0},
    {"id": 4, "name": "west", "value": 35.25},
]

# (id, name, longitude, latitude)
GPKG_ROWS = [
    (1, "site-a", 31.5, 6.8),
    (2, "site-b", 30.2, 4.85),
    (3, "site-c", 32.1, 7.6),
]
GPKG_CRS_EPSG = 4326

# (id, name, longitude, latitude) -- deliberately small Point geometries
# rather than a large real-world polygon file.
GEOJSON_ROWS = [
    (1, "adm1-a", 31.0, 6.5),
    (2, "adm1-b", 32.4, 7.9),
]
GEOJSON_CRS_EPSG = 4326


def _make_csv_fixture(path: Path) -> None:
    pd.DataFrame(CSV_ROWS).to_csv(path, index=False)


def _make_excel_fixture(path: Path) -> None:
    pd.DataFrame(EXCEL_ROWS).to_excel(path, index=False, engine="openpyxl")


def _make_gpkg_fixture(path: Path) -> None:
    if gpd is None:
        pytest.skip("geopandas is required to build the GeoPackage fixture")
    gdf = gpd.GeoDataFrame(
        {
            "id": [r[0] for r in GPKG_ROWS],
            "name": [r[1] for r in GPKG_ROWS],
            "geometry": [Point(r[2], r[3]) for r in GPKG_ROWS],
        },
        crs=f"EPSG:{GPKG_CRS_EPSG}",
    )
    gdf.to_file(path, driver="GPKG")


def _make_geojson_fixture(path: Path) -> None:
    if gpd is None:
        pytest.skip("geopandas is required to build the GeoJSON fixture")
    gdf = gpd.GeoDataFrame(
        {
            "id": [r[0] for r in GEOJSON_ROWS],
            "name": [r[1] for r in GEOJSON_ROWS],
            "geometry": [Point(r[2], r[3]) for r in GEOJSON_ROWS],
        },
        crs=f"EPSG:{GEOJSON_CRS_EPSG}",
    )
    gdf.to_file(path, driver="GeoJSON")


# ---------------------------------------------------------------------------
# Metadata YAML written per-case into a temp directory. Paths are absolute
# and point at the fixture generated above, so the test has no dependency
# on the working directory it happens to be invoked from.
# ---------------------------------------------------------------------------

def _write_metadata_yaml(
    metadata_dir: Path,
    dataset_name: str,
    source_path: Path,
    file_type: str,
    loader_class: str,
    target_schema: str,
    target_table: str,
) -> Path:
    content = {
        "dataset": {
            "name": dataset_name,
            "description": f"Integration test fixture for {file_type}",
            "owner": "integration_tests",
            "priority": 99,
        },
        "source": {
            "type": "local_file",
            "path": str(source_path),
            "file_type": file_type,
        },
        "loader": {
            "class": loader_class,
            "chunk_size": 1000,
            "load_mode": "replace",
        },
        "target": {
            "database": "humanitarian_db",
            "schema": target_schema,
            "table": target_table,
        },
    }

    yml_path = metadata_dir / f"integration_{file_type}.yml"
    with yml_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(content, fh)
    return yml_path


# ---------------------------------------------------------------------------
# Independent PostgreSQL verification helpers
# ---------------------------------------------------------------------------

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


def _fetch_metadata_source(cur, dataset_id) -> dict:
    cur.execute(
        'SELECT dataset_id, "type", path, file_type '
        "FROM metadata_manager.source WHERE dataset_id = %s",
        (dataset_id,),
    )
    row = cur.fetchone()
    assert row is not None, "expected a registered source row for this dataset"
    return {"dataset_id": row[0], "type": row[1], "path": row[2], "file_type": row[3]}


def _fetch_metadata_target(cur, dataset_id) -> dict:
    cur.execute(
        'SELECT dataset_id, database, "schema", "table" '
        "FROM metadata_manager.target WHERE dataset_id = %s",
        (dataset_id,),
    )
    row = cur.fetchone()
    assert row is not None, "expected a registered target row for this dataset"
    return {"dataset_id": row[0], "database": row[1], "schema": row[2], "table": row[3]}


# ---------------------------------------------------------------------------
# Cleanup: metadata rows (dataset delete cascades to source/target/
# pipeline_config per schemas.sql's ON DELETE CASCADE) + the actual data
# schema/table that the loader created.
# ---------------------------------------------------------------------------

@pytest.fixture()
def cleanup(db_connection):
    created = {"dataset_id": None, "target_schema": None}

    yield created

    with db_connection.cursor() as cur:
        if created["target_schema"]:
            cur.execute(
                psycopg2.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    psycopg2.sql.Identifier(created["target_schema"])
                )
            )
        if created["dataset_id"] is not None:
            cur.execute(
                "DELETE FROM metadata_manager.dataset WHERE id = %s",
                (created["dataset_id"],),
            )
    db_connection.commit()


# ---------------------------------------------------------------------------
# Parametrized end-to-end test
# ---------------------------------------------------------------------------

CASES = [
    pytest.param("csv", "CSVLoader", id="csv"),
    pytest.param("geopackage", "GeoPackageLoader", id="geopackage"),
    pytest.param("excel", "ExcelLoader", id="excel"),
    pytest.param("geojson", "GeoJSONLoader", id="geojson"),
]


@pytest.mark.parametrize("file_type, expected_loader_class", CASES)
def test_full_ingestion_pipeline_integration(
    file_type,
    expected_loader_class,
    tmp_path,
    repos,
    db_connection,
    cleanup,
):
    run_id = uuid.uuid4().hex[:8]
    dataset_name = f"integration_test_{file_type}_{run_id}"
    target_schema = f"integration_test_raw_{run_id}"
    target_table = f"integration_{file_type}"

    # --- 1. Build the source fixture file -------------------------------
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    if file_type == "csv":
        source_path = data_dir / "integration_test.csv"
        _make_csv_fixture(source_path)
        expected_rows = len(CSV_ROWS)
    elif file_type == "excel":
        source_path = data_dir / "integration_test.xlsx"
        _make_excel_fixture(source_path)
        expected_rows = len(EXCEL_ROWS)
    elif file_type == "geopackage":
        source_path = data_dir / "integration_test.gpkg"
        _make_gpkg_fixture(source_path)
        expected_rows = len(GPKG_ROWS)
    elif file_type == "geojson":
        source_path = data_dir / "integration_test.geojson"
        _make_geojson_fixture(source_path)
        expected_rows = len(GEOJSON_ROWS)
    else:  # pragma: no cover - guarded by CASES
        raise AssertionError(f"unhandled file_type {file_type!r}")

    # --- 2. Write the metadata YAML for this case only -------------------
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()
    _write_metadata_yaml(
        metadata_dir=metadata_dir,
        dataset_name=dataset_name,
        source_path=source_path,
        file_type=file_type,
        loader_class=expected_loader_class,
        target_schema=target_schema,
        target_table=target_table,
    )

    # --- 3. Register metadata using the real repositories ----------------
    # Order matters: source/target/pipeline_config all resolve dataset_id
    # via the already-registered dataset.
    registered_datasets = repos["dataset"].register_from_directory(metadata_dir)
    assert len(registered_datasets) == 1
    assert registered_datasets[0]["name"] == dataset_name

    registered_sources = repos["source"].register_from_directory(metadata_dir)
    assert len(registered_sources) == 1

    registered_targets = repos["target"].register_from_directory(metadata_dir)
    assert len(registered_targets) == 1

    registered_pipeline_configs = repos["pipeline_config"].register_from_directory(
        metadata_dir
    )
    assert len(registered_pipeline_configs) == 1

    # --- 4. Verify registration actually landed in PostgreSQL ------------
    dataset_row = repos["dataset"].get_by_name(dataset_name)
    assert dataset_row is not None
    dataset_id = dataset_row["id"]

    cleanup["dataset_id"] = dataset_id
    cleanup["target_schema"] = target_schema

    with db_connection.cursor() as cur:
        source_row = _fetch_metadata_source(cur, dataset_id)
        target_row = _fetch_metadata_target(cur, dataset_id)

    assert source_row["path"] == str(source_path)
    assert source_row["file_type"] == file_type
    assert target_row["schema"] == target_schema
    assert target_row["table"] == target_table

    pipeline_cfg = repos["pipeline_config"].get_by_asset_name(dataset_name)
    assert pipeline_cfg is not None
    assert pipeline_cfg["loader_class"] == expected_loader_class

    # --- 5. Extract using metadata-resolved source/file_type -------------
    extractor = DataExtractor()
    df = extractor.extract(path=source_row["path"], file_type=source_row["file_type"])

    if file_type == "geopackage":
        assert gpd is not None
        assert isinstance(df, gpd.GeoDataFrame)

    assert len(df) == expected_rows

    # --- 6. Pre-load validation -------------------------------------------
    validation_result = PreValidator().validate(df)
    assert validation_result.is_valid is True
    assert validation_result.checked_rows == len(df)
    assert validation_result.errors == []

    # --- 7. Ingestion, with the loader resolved from metadata ------------
    ingestion_result = Ingestion().run(
        source_path=source_row["path"],
        file_type=source_row["file_type"],
        loader_class=pipeline_cfg["loader_class"],
        schema=target_row["schema"],
        table=target_row["table"],
        load_mode=pipeline_cfg["load_mode"],
        chunk_size=pipeline_cfg["chunk_size"],
    )

    assert ingestion_result.loader_class == expected_loader_class
    assert ingestion_result.load_result.rows_loaded == expected_rows
    assert ingestion_result.load_result.schema == target_schema
    assert ingestion_result.load_result.table == target_table

    # --- 8. Independent PostgreSQL verification ---------------------------
    with db_connection.cursor() as cur:
        assert _table_exists(cur, target_schema, target_table)

        cur.execute(
            psycopg2.sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                psycopg2.sql.Identifier(target_schema),
                psycopg2.sql.Identifier(target_table),
            )
        )
        assert cur.fetchone()[0] == expected_rows

        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (target_schema, target_table),
        )
        actual_columns = {row[0] for row in cur.fetchall()}
        assert set(df.columns).issubset(actual_columns)

        if file_type == "csv":
            cur.execute(
                psycopg2.sql.SQL(
                    "SELECT name, value FROM {}.{} WHERE id = %s"
                ).format(
                    psycopg2.sql.Identifier(target_schema),
                    psycopg2.sql.Identifier(target_table),
                ),
                (CSV_ROWS[0]["id"],),
            )
            row = cur.fetchone()
            assert row == (CSV_ROWS[0]["name"], CSV_ROWS[0]["value"])

        if file_type == "excel":
            cur.execute(
                psycopg2.sql.SQL(
                    "SELECT name, value FROM {}.{} WHERE id = %s"
                ).format(
                    psycopg2.sql.Identifier(target_schema),
                    psycopg2.sql.Identifier(target_table),
                ),
                (EXCEL_ROWS[0]["id"],),
            )
            row = cur.fetchone()
            assert row == (EXCEL_ROWS[0]["name"], EXCEL_ROWS[0]["value"])

        # --- 9. GeoPackage / PostGIS-specific verification ----------------
        if file_type == "geopackage":
            geometry_col = df.geometry.name

            cur.execute(
                """
                SELECT f_geometry_column, srid, type
                FROM geometry_columns
                WHERE f_table_schema = %s AND f_table_name = %s
                """,
                (target_schema, target_table),
            )
            geom_meta = cur.fetchone()
            assert geom_meta is not None, "expected a PostGIS geometry column entry"
            assert geom_meta[0] == geometry_col
            assert geom_meta[1] == GPKG_CRS_EPSG

            first_id, first_name, first_lon, first_lat = GPKG_ROWS[0]
            cur.execute(
                psycopg2.sql.SQL(
                    "SELECT name, ST_X({geom}), ST_Y({geom}) "
                    "FROM {schema}.{table} WHERE id = %s"
                ).format(
                    geom=psycopg2.sql.Identifier(geometry_col),
                    schema=psycopg2.sql.Identifier(target_schema),
                    table=psycopg2.sql.Identifier(target_table),
                ),
                (first_id,),
            )
            row = cur.fetchone()
            assert row is not None
            name, lon, lat = row
            assert name == first_name
            assert lon == pytest.approx(first_lon)
            assert lat == pytest.approx(first_lat)

        # --- 10. GeoJSON / PostGIS-specific verification -------------------
        if file_type == "geojson":
            geometry_col = df.geometry.name

            # geometry_columns only lists real PostGIS geometry columns,
            # so a match here already rules out a TEXT/WKT column; the
            # explicit information_schema check below confirms it too.
            cur.execute(
                """
                SELECT f_geometry_column, srid, type
                FROM geometry_columns
                WHERE f_table_schema = %s AND f_table_name = %s
                """,
                (target_schema, target_table),
            )
            geom_meta = cur.fetchone()
            assert geom_meta is not None, "expected a PostGIS geometry column entry"
            assert geom_meta[0] == geometry_col
            assert geom_meta[1] == GEOJSON_CRS_EPSG

            cur.execute(
                """
                SELECT udt_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
                """,
                (target_schema, target_table, geometry_col),
            )
            udt_name = cur.fetchone()[0]
            assert udt_name == "geometry", (
                f"expected geometry column to be PostGIS 'geometry', got {udt_name!r} "
                "(looks like it was stored as TEXT/WKT instead)"
            )

            first_id, first_name, first_lon, first_lat = GEOJSON_ROWS[0]
            cur.execute(
                psycopg2.sql.SQL(
                    "SELECT name, GeometryType({geom}), ST_SRID({geom}), "
                    "ST_IsValid({geom}), ST_X({geom}), ST_Y({geom}) "
                    "FROM {schema}.{table} WHERE id = %s"
                ).format(
                    geom=psycopg2.sql.Identifier(geometry_col),
                    schema=psycopg2.sql.Identifier(target_schema),
                    table=psycopg2.sql.Identifier(target_table),
                ),
                (first_id,),
            )
            row = cur.fetchone()
            assert row is not None
            name, geom_type, srid, is_valid, lon, lat = row
            assert name == first_name
            # GeometryType() returns bare type names ('POINT'), unlike
            # ST_GeometryType() which would return 'ST_Point'.
            assert geom_type == "POINT"
            assert srid == GEOJSON_CRS_EPSG
            assert is_valid is True
            assert lon == pytest.approx(first_lon)
            assert lat == pytest.approx(first_lat)

    db_connection.commit()