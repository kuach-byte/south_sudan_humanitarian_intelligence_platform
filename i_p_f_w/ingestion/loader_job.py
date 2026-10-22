import io
import logging
import os
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
import psycopg2
import shapely
from dotenv import load_dotenv
from pandas.api import types as pdt
from psycopg2 import sql

log = logging.getLogger(__name__)

CHUNK_SIZE = 50_000

# Loading only: no validation, profiling or cleaning (prevalidation / dbt own those).


class LoaderError(Exception):
    pass


class Loader:
    def __init__(self, project_root=None):
        self.root = Path(project_root or Path.cwd()).resolve()
        load_dotenv(self.root / ".env")

    @staticmethod
    def _target(metadata):
        target = (metadata or {}).get("target") or {}
        missing = [k for k in ("database", "schema", "table") if not target.get(k)]
        if missing:
            raise LoaderError(f"Metadata is missing target fields: {missing}")
        return target["database"], target["schema"], target["table"]

    @staticmethod
    def _connection_params(database):
        env = {k: os.getenv(k) for k in ("DB_HOST", "DB_USER", "DB_PASSWORD")}
        missing = [k for k, v in env.items() if not v]
        if missing:
            raise LoaderError(f"Missing database configuration in .env: {missing}")

        return {
            "host": env["DB_HOST"],
            "port": os.getenv("DB_PORT", "5432"),
            "user": env["DB_USER"],
            "password": env["DB_PASSWORD"],
            "dbname": database,
        }

    @staticmethod
    def _pg_type(dtype):
        if pdt.is_bool_dtype(dtype):
            return "boolean"
        if pdt.is_integer_dtype(dtype):
            return "bigint"
        if pdt.is_float_dtype(dtype):
            return "double precision"
        if pdt.is_datetime64_any_dtype(dtype):
            return "timestamptz" if getattr(dtype, "tz", None) else "timestamp"
        return "text"

    @staticmethod
    def _chunk_to_csv(chunk, geom, srid):
        chunk = pd.DataFrame(chunk)
        if geom:
            chunk[geom] = shapely.to_wkb(
                shapely.set_srid(chunk[geom].to_numpy(), srid),
                hex=True,
                include_srid=bool(srid),
            )
        buf = io.StringIO()
        chunk.to_csv(buf, index=False, header=False)
        buf.seek(0)
        return buf

    def run(self, metadata, dataframe):
        database, schema, table = self._target(metadata)
        if dataframe is None:
            raise LoaderError(f"{schema}.{table}: no DataFrame supplied")

        geom, srid = None, 0
        if isinstance(dataframe, gpd.GeoDataFrame):
            geom = dataframe.geometry.name
            srid = (dataframe.crs.to_epsg() or 0) if dataframe.crs else 0
            if not srid:
                log.warning("%s.%s: no EPSG code available, loading with SRID 0", schema, table)

        names = [str(c) for c in dataframe.columns]
        columns = sql.SQL(", ").join(map(sql.Identifier, names))
        definition = sql.SQL(", ").join(
            sql.SQL("{} {}").format(
                sql.Identifier(n),
                sql.SQL(
                    f"geometry(Geometry, {srid})"
                    if c == geom
                    else self._pg_type(dataframe[c].dtype)
                ),
            )
            for n, c in zip(names, dataframe.columns)
        )
        target = sql.Identifier(schema, table)

        drop = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(target)
        create = sql.SQL("CREATE TABLE {} ({})").format(target, definition)
        copy = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(target, columns)

        total = len(dataframe)
        log.info("Loading %d rows into %s.%s.%s", total, database, schema, table)

        try:
            conn = psycopg2.connect(**self._connection_params(database))
        except psycopg2.Error as exc:
            raise LoaderError(f"Could not connect to database '{database}': {exc}") from exc

        try:
            with conn, conn.cursor() as cur:
                cur.execute(drop)
                cur.execute(create)

                copy_sql = copy.as_string(cur)
                for start in range(0, total, CHUNK_SIZE):
                    buf = self._chunk_to_csv(dataframe.iloc[start:start + CHUNK_SIZE], geom, srid)
                    cur.copy_expert(copy_sql, buf)
                    log.info(
                        "%s.%s | loaded rows %d-%d of %d",
                        schema, table, start + 1, min(start + CHUNK_SIZE, total), total,
                    )
        except psycopg2.Error as exc:
            raise LoaderError(f"Load into {schema}.{table} failed: {exc}") from exc
        finally:
            conn.close()

        # Load committed: move the source file from landing/ to achieved/
        data_dir = self.root / "i_p_f_w" / "data"
        try:
            source = (self.root / metadata["source"]["path"]).resolve()
            dest_dir = (data_dir / "achieved" / source.relative_to(data_dir / "landing")).parent
            dest_dir.mkdir(parents=True, exist_ok=True)
            # a shapefile is a set of sidecar files sharing one stem
            if source.suffix.lower() == ".shp":
                files = [f for f in source.parent.iterdir() if f.is_file() and f.stem == source.stem]
            else:
                files = [source]
            for f in files:
                shutil.move(str(f), str(dest_dir / f.name))
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise LoaderError(
                f"{schema}.{table} was loaded, but moving the source file to achieved failed: {exc}"
            ) from exc

        return total 
