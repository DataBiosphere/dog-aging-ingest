from dagster import repository, PipelineDefinition, fs_io_manager
from dagster_utils.resources.beam.local_beam_runner import local_beam_runner
from dagster_utils.resources.google_storage import google_storage_client

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.pipelines import refresh_data_all
from dap_orchestration.resources import refresh_directory, outfiles_writer, api_token


@repository
def repositories() -> list[PipelineDefinition]:
    return [refresh_data_all.to_job(resource_defs={
        "extract_beam_runner": local_beam_runner,
        "transform_beam_runner": local_beam_runner,
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer,
        "api_token": preconfigure_resource_for_mode(api_token, "local"),
        "io_manager": fs_io_manager,
        "gcs": google_storage_client
    })]
