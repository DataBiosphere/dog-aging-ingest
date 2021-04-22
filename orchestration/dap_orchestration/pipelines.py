from dagster import pipeline, ModeDefinition
from dap_orchestration.solids import extract_records, transform_records
from dap_orchestration.resources import local_beam_runner


dev_mode = ModeDefinition(
    name="dev",
    resource_defs={
        "beam_runner": local_beam_runner,
    }
)

# use manual aliasing to differentiate between the two pipelines
# that way we can call the 3 pipelines separately
@pipeline(
    mode_defs=[dev_mode]
)
def refresh_data() -> None:
    transform_records(extract_records())


