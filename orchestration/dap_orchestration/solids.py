
from dagster import Bool, String, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

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
    return context.solid_config["output_prefix"]"



# syntax for setting up pipelines
# is there a way to use the same solid with multiple different configs in one pipeline


## todo: Transformation Pipeline
def transform_records(context: AbstractComputeExecutionContext, input_prefix: str) -> str:
    """
    :return: Returns the path to the transformation output json files.
    """
    context.resources.beam_runner.run({
        "inputPrefix": input_prefix,
        "outputPrefix": context.solid_config["output_prefix"],
    })
    return context.solid_config["output_prefix"]


