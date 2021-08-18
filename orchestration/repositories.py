from dap_orchestration.pipelines import weekly_sample_refresh

from dagster import PipelineDefinition, repository

from dap_orchestration.pipelines import refresh_data_all, refresh_sample_data


@repository
def repositories() -> list[PipelineDefinition]:
    return [refresh_data_all, refresh_sample_data, weekly_sample_refresh]
