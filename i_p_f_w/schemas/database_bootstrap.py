"""Create the project database and its schemas, then run the DDL files.

Run directly:  python -m i_p_f_w.schemas.database_bootstrap
From code:     run_bootstrap() -> summary dict

Safe to run repeatedly: nothing is dropped, truncated or overwritten. The two SQL files in this
folder are the only source of truth for what lives inside the metadata and profiling schemas;
they are executed on every run because an existing schema does not imply its tables exist.
"""
import logging
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

from i_p_f_w.metadata_manager.metadata_registry import MetadataRegistry

log = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent
MAINTENANCE_DB = "postgres"  # PostgreSQL's default maintenance database, used only to create the target database
RAW_SCHEMA = "raw_data"
# (schema, DDL file), in dependency order: profiling_schemas.sql references metadata_manager.dataset
DDL_FILES = [
    ("metadata_manager", "metadata_schemas.sql"),
    ("data_profiling", "profiling_schemas.sql"),
]


class BootstrapError(Exception):
    pass


def _connect(params, dbname):
    try:
        return psycopg2.connect(**{**params, "dbname": dbname})
    except psycopg2.Error as exc:
        raise BootstrapError(
            f"Could not connect to database '{dbname}' (host={params['host']}, user={params['user']}): {exc}"
        ) from exc


def _ensure_database(params):
    """Create the target database if missing. Returns True if it was created."""
    name = params["dbname"]
    conn = _connect(params, MAINTENANCE_DB)
    try:
        conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            if cur.fetchone():
                log.info("Database '%s' already exists", name)
                return False
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            log.info("Database '%s' created", name)
            return True
    except psycopg2.Error as exc:
        raise BootstrapError(f"Could not check or create database '{name}': {exc}") from exc
    finally:
        conn.close()


def _ensure_schema(cur, schema):
    """Create the schema if missing. Returns True if it was created."""
    cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,))
    if cur.fetchone():
        log.info("Schema '%s' already exists", schema)
        return False
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    log.info("Schema '%s' created", schema)
    return True


def run_bootstrap():
    """Ensure the database, the three schemas and all DDL objects exist.

    Returns {"database_created": bool, "schemas_created": [...], "ddl_executed": [...]}.
    Raises BootstrapError on any connection or SQL failure; the schema work is one transaction,
    so a failed DDL file leaves nothing half-applied.
    """
    params = MetadataRegistry().db
    missing = [k for k in ("dbname", "user", "password") if not params.get(k)]
    if missing:
        raise BootstrapError(f"Missing database configuration in .env (DB_NAME / DB_USER / DB_PASSWORD): {missing}")

    summary = {"database_created": _ensure_database(params), "schemas_created": [], "ddl_executed": []}

    conn = _connect(params, params["dbname"])
    try:
        with conn, conn.cursor() as cur:  # commits on success, rolls back on any exception
            if _ensure_schema(cur, RAW_SCHEMA):
                summary["schemas_created"].append(RAW_SCHEMA)
            for schema, filename in DDL_FILES:
                if _ensure_schema(cur, schema):
                    summary["schemas_created"].append(schema)
                log.info("Executing %s", filename)
                try:
                    cur.execute((SCHEMA_DIR / filename).read_text(encoding="utf-8"))
                except (psycopg2.Error, OSError) as exc:
                    raise BootstrapError(f"Executing {filename} failed: {exc}") from exc
                log.info("%s executed successfully", filename)
                summary["ddl_executed"].append(filename)
    except psycopg2.Error as exc:
        raise BootstrapError(f"Schema initialization failed: {exc}") from exc
    finally:
        conn.close()

    log.info("Database bootstrap finished: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run_bootstrap()
    except BootstrapError as exc:
        log.error("%s", exc)
        sys.exit(1)