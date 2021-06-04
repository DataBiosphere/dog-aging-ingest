from dagster import pipeline, ModeDefinition

from dagster_utils.resources.beam.local_beam_runner import local_beam_runner
from dagster_utils.resources.beam.dataflow_beam_runner import dataflow_beam_runner

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.resources import refresh_directory, outfiles_writer
from dap_orchestration.solids import hles_extract_records, cslb_extract_records, env_extract_records,\
    hles_transform_records, cslb_transform_records, env_transform_records, write_outfiles


local_mode = ModeDefinition(
    name="local",
    resource_defs={
        "beam_runner": local_beam_runner,
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer
    }
)

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "dev"),
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer
    }
)

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(dataflow_beam_runner, "prod"),
        "refresh_directory": refresh_directory,
        "outfiles_writer": outfiles_writer
    }
)


@pipeline(
    mode_defs=[local_mode, dev_mode, prod_mode]
)
def refresh_data_all() -> None:
    hles_extract_records()
    hles_transform_records()

    cslb_extract_records()
    cslb_transform_records()

    env_extract_records()
    env_transform_records()

    # the transform output should be in the same directory for all 3 pipelines
    write_outfiles()
