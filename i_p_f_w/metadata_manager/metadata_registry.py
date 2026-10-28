import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import find_dotenv, load_dotenv

from i_p_f_w.metadata_manager.yaml_metadata_parser import YamlMetadataParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("metadata_registry")

SCHEMA = "metadata_manager"  # overrides DB_SCHEMA from .env for this program only
# metadata_manager.source columns a metadata["source"] dict may set (fixed list, so safe to put in the SQL text)
SOURCE_COLUMNS = ("source", "source_id", "contributor", "type", "path", "file_type",
                  "time_period_begin", "time_period_end")


class MetadataRegistry:
    def __init__(self):
        load_dotenv(find_dotenv())
        self.db = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }
        self.conn = None


    def register_dataset(self, cur, ds):
        cur.execute(
            f"INSERT INTO {SCHEMA}.dataset (name, registered_at) VALUES (%s, %s) "
            f"ON CONFLICT (name) DO UPDATE SET registered_at = EXCLUDED.registered_at RETURNING id",
            (ds["name"], ds["registered_at"]),
        )
        dataset_id = cur.fetchone()[0]
        log.info("Dataset registered: %s (id=%s)", ds["name"], dataset_id)
        return dataset_id

    def register_source(self, cur, dataset_id, source):
        """Upsert the dataset's source row (one per dataset). Only the columns present in `source` are written, so the
        pipeline's type/path/file_type and the YAML's descriptive fields can be registered separately without erasing each other."""
        cols = [c for c in SOURCE_COLUMNS if c in source]
        if not cols:
            return
        cur.execute(
            f"INSERT INTO {SCHEMA}.source (dataset_id, {', '.join(cols)}) VALUES (%s, {', '.join(['%s'] * len(cols))}) "
            f"ON CONFLICT (dataset_id) DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in cols)}",
            (dataset_id, *[source[c] for c in cols]),
        )
        log.info("Source registered for dataset_id=%s", dataset_id)

    def register_target(self, cur, dataset_id, target):
        """Replace the dataset's target row with the YAML definition."""
        cur.execute(f"DELETE FROM {SCHEMA}.target WHERE dataset_id = %s", (dataset_id,))
        cur.execute(
            f'INSERT INTO {SCHEMA}.target (dataset_id, database, schema, "table") VALUES (%s, %s, %s, %s)',
            (dataset_id, target.get("database"), target.get("schema"), target.get("table")),
        )
        log.info("Target registered for dataset_id=%s", dataset_id)

    def run(self, metadata):
        failed = 0
        try:
            self.conn = psycopg2.connect(**self.db)
        except psycopg2.Error as e:
            log.error("Database connection failed (host=%s, db=%s, user=%s): %s",
                    self.db["host"], self.db["dbname"], self.db["user"], e)
            return 1

        try:
            for m in metadata:
                try:
                    with self.conn, self.conn.cursor() as cur:
                        dataset_id = self.register_dataset(cur, m["dataset"])
                        if m.get("source"):
                            self.register_source(cur, dataset_id, m["source"])
                        if m.get("target"):
                            self.register_target(cur, dataset_id, m["target"])
                except psycopg2.Error as e:
                    failed += 1
                    log.error("Database error for %s, rolled back: %s", m["dataset"]["name"], e)
        finally:
            self.conn.close()

        if failed:
            log.error("Completed with %d failure(s)", failed)
            return 1
        log.info("Metadata registration completed successfully (%d dataset(s))", len(metadata))
        return 0

    def register_from_yaml(self, project_root=None, parser=None):
        """Register every dataset that has a valid metadata YAML in data/landing. Invalid files are logged and skipped
        (the rest are still registered, each in its own transaction); returns 1 if anything failed, else 0."""
        metadata, errors = (parser or YamlMetadataParser(project_root)).collect()
        for e in errors:
            log.error("%s", e)
        failed = self.run(metadata) if metadata else 0
        return 1 if errors or failed else 0


if __name__ == "__main__":
    sys.exit(MetadataRegistry().register_from_yaml())