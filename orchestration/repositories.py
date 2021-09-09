from dagster import PipelineDefinition, repository

from dap_orchestration.pipelines import refresh_data_all, refresh_sample_data, weekly_sample_refresh


@repository
def repositories() -> list[PipelineDefinition]:
    return [refresh_data_all, refresh_sample_data, weekly_sample_refresh]
