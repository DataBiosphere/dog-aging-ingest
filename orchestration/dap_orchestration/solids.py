import subprocess

from dagster import Bool, String, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

## A solid is a unit of computation that yields a stream of events (similar to a function), while a composite is a collection of solids.
## Shared function to call SBT calls
## shared local beam runner resource
## Extraction Pipeline
@solid(
    required_resource_keys={"beam_runner"},
    config_schema = {
        "pull_data_dictionaries": Bool,
        "output_prefix": String,
        "end_time": String,
    }
)
def extract_records(context: AbstractComputeExecutionContext) -> str:
    """
    :return: Returns the path to extracted files.
    """
    context.resources.beam_runner.run({
        "pullDataDictionaries": "true" if context.solid_config["pull_data_dictionaries"] else "false",
        "outputPrefix": context.solid_config["output_prefix"],
        "endTime": context.solid_config["end_time"],
    })
    return context.solid_config["output_prefix"]

@solid(
    required_resource_keys={"beam_runner"},
    config_schema = {
        "output_prefix": String,
    }
)
def transform_records(context: AbstractComputeExecutionContext, input_prefix: str) -> str:
    """
    :return: Returns the path to the transformation output json files.
    """
    context.resources.beam_runner.run({
        "inputPrefix": input_prefix,
        "outputPrefix": context.solid_config["output_prefix"],
    })
    return context.solid_config["output_prefix"]

## todo: TSV Outfiles
@solid(
    config_schema = {
        "output_prefix": String,
    }
)
def write_outfiles(context: AbstractComputeExecutionContext, input_prefix: str) -> str:
    """
    :return: Returns the path to the tsv outfiles.
    """
    # todo: add a step to create tsv subdir

    subprocess.run(
        ["python", "hack/convert-output-to-tsv.py", input_prefix, context.solid_config["output_prefix"], "--debug"],
        check=True,
        cwd="../../../../../GIT/dog-aging-ingest"
    )
    return context.solid_config["output_prefix"]