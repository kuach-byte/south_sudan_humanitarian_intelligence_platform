from dagster import Definitions

from i_p_f_w.orchestration.dagster.assets import (
    database_bootstrap,
    prevalidation,
    metadata_registration,
    loading,
    data_profiling,
)

defs = Definitions(
    assets=[
        database_bootstrap,
        prevalidation,
        metadata_registration,
        loading,
        data_profiling,
    ]
)