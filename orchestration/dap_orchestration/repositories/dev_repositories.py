from dagster import PipelineDefinition, repository

from dap_orchestration.repositories.base_repositories import all_jobs

@repository
def repositories() -> list[PipelineDefinition]:
    return all_jobs()
