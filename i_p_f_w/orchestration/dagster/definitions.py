
from dagster import Definitions

from i_p_f_w.orchestration.dagster.assets import (
    prevalidation,
    metadata_registration,
    loading,
    data_profiling,
)

defs = Definitions(
    assets=[
        prevalidation,
        metadata_registration,
        loading,
        data_profiling,
    ]
)