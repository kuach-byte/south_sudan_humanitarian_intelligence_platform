import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import find_dotenv, load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("metadata_registry")

SCHEMA = "metadata_manager"  # overrides DB_SCHEMA from .env for this program only


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
        """Replace the dataset's source row with the YAML definition."""
        cur.execute(f"DELETE FROM {SCHEMA}.source WHERE dataset_id = %s", (dataset_id,))
        cur.execute(
            f"INSERT INTO {SCHEMA}.source (dataset_id, type, path, file_type) VALUES (%s, %s, %s, %s)",
            (dataset_id, source.get("type"), source.get("path"), source.get("file_type")),
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
if __name__ == "__main__":
    sys.exit(MetadataRegistry().run())