from dagster import PipelineDefinition, repository
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.pipelines import refresh_data_all
from dap_orchestration.resources import refresh_directory, outfiles_writer, api_token


@repository
def repositories() -> list[PipelineDefinition]:
    return [refresh_data_all.to_job(resource_defs={
        "extract_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev_extract"),
        "transform_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev_transform"),
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer,
        "api_token": preconfigure_resource_for_mode(api_token, "dev"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "gcs": google_storage_client,
        "slack_client": preconfigure_resource_for_mode(live_slack_client, "dev")
    })]
