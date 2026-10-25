from dagster import asset
from i_p_f_w.orchestration.pipeline_runner.pipeline_runner import PipelineRunner

@asset
def prevalidation() -> list[dict]:
    return PipelineRunner().run_prevalidation()


@asset
def metadata_registration(prevalidation: list[dict]) -> None:
    PipelineRunner().run_metadata_registration(prevalidation)


@asset(deps=[metadata_registration])
def loading(prevalidation: list[dict]) -> None:
    PipelineRunner().run_loading(prevalidation)

@asset(deps=[loading])
def data_profiling() -> dict:
    return PipelineRunner().run_data_profiling()