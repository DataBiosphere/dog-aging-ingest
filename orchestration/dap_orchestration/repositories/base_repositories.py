from dagster import PipelineDefinition, repository

from dap_orchestration.pipelines import refresh_data_all


def all_jobs() -> list[PipelineDefinition]:
    return [refresh_data_all]
