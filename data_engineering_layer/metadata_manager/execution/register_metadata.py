import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from data_engineering_layer.metadata_manager.OOP.baserepo import RepositoryError
from data_engineering_layer.metadata_manager.OOP.datasets import DatasetRepository
from data_engineering_layer.metadata_manager.OOP.source import SourceRepository
from data_engineering_layer.metadata_manager.OOP.target import TargetRepository
from data_engineering_layer.metadata_manager.OOP.pipeline_config import (
    PipelineConfigRepository,
)

logger = logging.getLogger(__name__)

# data_engineering_layer/metadata_manager/execution/register_metadata.py
# -> parent.parent == data_engineering_layer/metadata_manager/
METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    load_dotenv()

    print("Metadata registration started.\n")

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            dbname=os.getenv("DB_NAME", "humanitarian_db"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
    except psycopg2.Error as exc:
        logger.error("Fatal: could not connect to the database: %s", exc)
        sys.exit(1)

    try:
        dataset_repo = DatasetRepository(conn)
        source_repo = SourceRepository(conn)
        target_repo = TargetRepository(conn)
        pipeline_config_repo = PipelineConfigRepository(conn)

        try:
            registered_datasets = dataset_repo.register_from_directory(METADATA_DIR)
            registered_sources = source_repo.register_from_directory(METADATA_DIR)
            registered_targets = target_repo.register_from_directory(METADATA_DIR)
            registered_pipeline_configs = pipeline_config_repo.register_from_directory(
                METADATA_DIR
            )
        except RepositoryError as exc:
            logger.error("Fatal: metadata registration could not proceed: %s", exc)
            sys.exit(1)

        print("Datasets:")
        print(f"  Registered: {len(registered_datasets)}\n")

        print("Sources:")
        print(f"  Registered: {len(registered_sources)}\n")

        print("Targets:")
        print(f"  Registered: {len(registered_targets)}\n")

        print("Pipeline configurations:")
        print(f"  Registered: {len(registered_pipeline_configs)}\n")

        print("Metadata registration completed successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()