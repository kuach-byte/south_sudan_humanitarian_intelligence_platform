import psycopg2
from dagster import asset

from data_engineering_layer.ingestion.loader import _get_connection_params
from data_engineering_layer.metadata_manager.OOP.pipeline_config import (
    PipelineConfigRepository,
)
from data_engineering_layer.orchestration.pipeline_runner.pipeline_runner import (
    IngestionPipelineRunner,
)


def _run_ingestion(dataset_name: str):
    connection = psycopg2.connect(**_get_connection_params())

    try:
        runner = IngestionPipelineRunner(connection)
        return runner.run(dataset_name)
    finally:
        connection.close()


def _load_pipeline_configs():
    connection = psycopg2.connect(**_get_connection_params())

    try:
        repository = PipelineConfigRepository(connection)
        return repository.get_all_configs()
    finally:
        connection.close()


def _create_asset(asset_name: str):
    @asset(name=asset_name)
    def ingestion_asset():
        return _run_ingestion(asset_name)

    return ingestion_asset


configs = _load_pipeline_configs()

ingestion_assets = [
    _create_asset(config["asset_name"])
    for config in configs
]