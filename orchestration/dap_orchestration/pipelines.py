from dagster import pipeline, ModeDefinition

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.resources import local_beam_runner
from dap_orchestration.solids import extract_records, transform_records, write_outfiles

local_beam_runner_run_schema = {"working_dir": str}

dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(local_beam_runner, "dev", local_beam_runner_run_schema)
    }
)

prod_mode = ModeDefinition(
    name="prod",
    resource_defs={
        "beam_runner": preconfigure_resource_for_mode(local_beam_runner, "prod", local_beam_runner_run_schema)
    }
)

# use manual aliasing to differentiate between the two pipelines
# that way we can call the 3 pipelines separately
@pipeline(
    mode_defs=[dev_mode, prod_mode]
)
def refresh_data() -> None:
    write_outfiles(transform_records(extract_records()))

