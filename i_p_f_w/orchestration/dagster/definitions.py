from dagster import Definitions

from i_p_f_w.orchestration.dagster.assets import (
    loading,
    metadata_registration,
    prevalidation,
)

defs = Definitions(assets=[metadata_registration, prevalidation, loading])