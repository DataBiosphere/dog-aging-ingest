from dagster import pipeline, ModeDefinition, ResourceDefinition, fs_io_manager, multiprocess_executor, \
    weekly_schedule, repository
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner

from dagster_utils.resources.beam.local_beam_runner import local_beam_runner
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.google_storage import google_storage_client

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.resources import refresh_directory, outfiles_writer, api_token
from dap_orchestration.solids import hles_extract_records, cslb_extract_records, env_extract_records, \
    sample_extract_records, eols_extract_records, hles_transform_records, cslb_transform_records, \
    env_transform_records, write_outfiles, sample_transform_records, eols_transform_records

from datetime import datetime, time

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "extract_beam_runner": local_beam_runner,
        "transform_beam_runner": local_beam_runner,
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer,
        "api_token": preconfigure_resource_for_mode(api_token, "local"),
        "io_manager": fs_io_manager
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "extract_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev_extract"),
        "transform_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "dev_transform"),
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer,
        "api_token": preconfigure_resource_for_mode(api_token, "dev"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "dev"),
        "gcs": google_storage_client,
    },
    executor_defs=[multiprocess_executor]
)

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "extract_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod_extract"),
        "transform_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod_transform"),
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer,
        "api_token": preconfigure_resource_for_mode(api_token, "prod"),
        "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
        "gcs": google_storage_client,
    }
)

test_mode = ModeDefinition(
    name="test",
    resource_defs={
        "extract_beam_runner": ResourceDefinition.mock_resource(),
        "transform_beam_runner": ResourceDefinition.mock_resource(),
        "refresh_directory": refresh_directory,
        "outfiles_writer": ResourceDefinition.mock_resource(),
        "api_token": ResourceDefinition.mock_resource()
    }
)


@pipeline(
    mode_defs=[local_mode, dev_mode, prod_mode, test_mode]
)
def refresh_data_all() -> None:
    collected_outputs = [
        hles_transform_records(hles_extract_records()),
        cslb_transform_records(cslb_extract_records()),
        env_transform_records(env_extract_records()),
        sample_transform_records(sample_extract_records()),
        eols_transform_records(eols_extract_records())
    ]
    write_outfiles(collected_outputs)

