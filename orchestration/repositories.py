from dagster import PipelineDefinition, repository

from dap_orchestration.pipelines import refresh_data_all


@repository
def repositories() -> list[PipelineDefinition]:
    return [refresh_data_all]
