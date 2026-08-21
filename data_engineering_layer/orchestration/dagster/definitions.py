from dagster import Definitions

from .assets import ingestion_assets


defs = Definitions(
    assets=ingestion_assets,
)