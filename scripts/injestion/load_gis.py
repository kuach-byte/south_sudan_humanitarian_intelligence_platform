#!/usr/bin/env python3
"""
load_gis.py

ETL script for the Humanitarian Analytics Data Warehouse (South Sudan).
Loads South Sudan administrative boundary GeoJSON files (admin 0-3) into
the `gis` schema of a PostgreSQL/PostGIS database.

    ssd_admin0.geojson -> gis.country_boundary
    ssd_admin1.geojson -> gis.state_boundary
    ssd_admin2.geojson -> gis.county_boundary
    ssd_admin3.geojson -> gis.payam_boundary

Usage: python load_gis.py
Requires a .env file with DB credentials (see .env.example).
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import geopandas as gpd
from dotenv import load_dotenv
from shapely.validation import make_valid
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path

print("Running script:", Path(__file__).resolve())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "gis"
LOG_FILE = PROJECT_ROOT / "logs" / "gis_import.log"
ENV_FILE = PROJECT_ROOT / ".env"
TARGET_CRS = "EPSG:4326"

# Maps source GeoJSON filenames to destination tables in the `gis` schema.
FILE_TABLE_MAP: dict[str, str] = {
    "ssd_admin0.geojson": "country_boundary",
    "ssd_admin1.geojson": "state_boundary",
    "ssd_admin2.geojson": "county_boundary",
    "ssd_admin3.geojson": "payam_boundary",
}


def setup_logging(log_file: Path) -> logging.Logger:
    """Configure a logger that writes to both log_file and stdout."""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("gis_import")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = setup_logging(LOG_FILE)


def load_env(env_path: Path) -> dict[str, str]:
    """Load and validate DB credentials from a .env file."""
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at: {env_path}")

    load_dotenv(dotenv_path=env_path, override=True)

    required = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SCHEMA"]
    missing = [var for var in required if os.getenv(var) is None]
    if missing:
        raise KeyError(f"Missing required environment variables: {missing}")

    return {
        "host": os.getenv("DB_HOST", ""),
        "port": os.getenv("DB_PORT", ""),
        "dbname": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
        "schema": os.getenv("DB_SCHEMA", "public"),
    }


def get_engine(db_config: dict[str, str]) -> Engine:
    """Create and test a SQLAlchemy engine for PostgreSQL/PostGIS."""
    conn_str = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    engine = create_engine(conn_str)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # fail fast if connection is bad
    return engine


def read_geojson(file_path: Path) -> gpd.GeoDataFrame:
    """Read a GeoJSON file and reproject it to EPSG:4326 if needed."""
    if not file_path.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {file_path}")

    gdf = gpd.read_file(file_path)

    if gdf.crs is None:
        logger.warning("No CRS detected in %s. Assuming %s.", file_path.name, TARGET_CRS)
        gdf = gdf.set_crs(TARGET_CRS)
    elif str(gdf.crs) != TARGET_CRS:
        logger.info("Reprojecting %s from %s to %s.", file_path.name, gdf.crs, TARGET_CRS)
        gdf = gdf.to_crs(TARGET_CRS)

    return gdf


def validate_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Drop null/empty geometries and repair invalid ones with make_valid()."""
    initial_count = len(gdf)
    gdf = gdf[gdf.geometry.notnull() & ~gdf.geometry.is_empty].copy()

    invalid_mask = ~gdf.geometry.is_valid
    if invalid_mask.any():
        logger.warning("Repairing %d invalid geometries.", int(invalid_mask.sum()))
        gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(make_valid)

    dropped = initial_count - len(gdf)
    if dropped > 0:
        logger.warning("Dropped %d record(s) with null/empty geometry.", dropped)

    return gdf


def load_to_postgis(gdf: gpd.GeoDataFrame, table_name: str, engine: Engine, schema: str) -> int:
    """Write a GeoDataFrame to PostGIS, replacing the table if it exists."""
    gdf.to_postgis(name=table_name, con=engine, schema=schema, if_exists="replace", index=False)
    return len(gdf)


def process_file(file_path: Path, table_name: str, engine: Engine, schema: str) -> bool:
    """Run read -> validate -> load for one file. Returns True on success."""
    start = time.monotonic()
    logger.info("Loading %s...", table_name)

    try:
        gdf = validate_geometry(read_geojson(file_path))

        if gdf.empty:
            logger.error("No valid geometries in %s after validation. Skipping.", file_path.name)
            return False

        count = load_to_postgis(gdf, table_name, engine, schema)
        logger.info(
            "\u2714 Loaded %d records into %s.%s (%.2fs).",
            count, schema, table_name, time.monotonic() - start,
        )
        return True

    except Exception as exc:  # noqa: BLE001 - top-level ETL guard, logged with context
        logger.error(
            "\u2716 Failed to load %s into %s.%s after %.2fs: %s",
            file_path.name, schema, table_name, time.monotonic() - start, exc,
        )
        return False


def main() -> None:
    """Load every file in FILE_TABLE_MAP into PostGIS; exit 1 if any failed."""
    run_start = time.monotonic()
    logger.info("=" * 60)
    logger.info("Starting GIS import run.")

    try:
        db_config = load_env(ENV_FILE)
        engine = get_engine(db_config)
        schema = db_config["schema"]
    except Exception as exc:  # noqa: BLE001
        logger.error("Startup failed: %s", exc)
        sys.exit(1)

    results = {
        table: process_file(DATA_DIR / filename, table, engine, schema)
        for filename, table in FILE_TABLE_MAP.items()
    }

    succeeded = sum(results.values())
    logger.info(
        "Run complete: %d succeeded, %d failed, total time %.2fs.",
        succeeded, len(results) - succeeded, time.monotonic() - run_start,
    )
    logger.info("=" * 60)

    if succeeded < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
