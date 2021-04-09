from dagster import repository, PipelineDefinition

from dog_aging_orchestration.pipelines.full_refresh import full_refresh_pipeline

@repository
def dog_aging() -> list[PipelineDefinition]:
    return [full_refresh_pipeline]