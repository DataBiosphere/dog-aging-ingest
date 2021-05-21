import subprocess

from dagster import Bool, String, solid, configured
from dagster.core.execution.context.compute import AbstractComputeExecutionContext


## A solid is a unit of computation that yields a stream of events (similar to a function), while a composite is a collection of solids.
## Shared function to call SBT calls
## shared local beam runner resource
## Extraction Pipeline
@solid(
    required_resource_keys={"beam_runner"},
    config_schema={
        "pull_data_dictionaries": Bool,
        "output_prefix": String,
        "end_time": String,
        "target_class": String,
        "api_token": String,
    }
)
def extract_records(context: AbstractComputeExecutionContext) -> str:
    """
    :return: Returns the path to extracted files.
    """
    arg_dict = {
        "pullDataDictionaries": "true" if context.solid_config["pull_data_dictionaries"] else "false",
        "outputPrefix": context.solid_config["output_prefix"],
        "endTime": context.solid_config["end_time"],
        "target_class": context.solid_config["target_class"],
        "apiToken": context.solid_config["api_token"],
        "scala_project": "dog-aging-hles-extraction"
    }
    context.resources.beam_runner.run(arg_dict)
    return context.solid_config["output_prefix"]

# todo: create a builder method that returns a SolidDefintion
def _build_extract_config(config: dict[str, str], target_class: str):
    return {
        "pull_data_dictionaries": config["pull_data_dictionaries"],
        "output_prefix": config["output_prefix"],
        "end_time": config["end_time"],
        "api_token": config["api_token"],
        "target_class": target_class
    }


@configured(extract_records)
def hles_extract_records(config):
    return _build_extract_config(config, "org.broadinstitute.monster.dap.hles.HLESurveyExtractionPipeline")


@configured(extract_records)
def cslb_extract_records(config):
    return _build_extract_config(config,"org.broadinstitute.monster.dap.cslb.CslbExtractionPipeline")


@configured(extract_records)
def env_extract_records(config):
    return _build_extract_config(config, "org.broadinstitute.monster.dap.environment.EnvironmentExtractionPipeline")


@solid(
    required_resource_keys={"beam_runner"},
    config_schema={
        "output_prefix": String,
        "target_class": String,
    }
)
def transform_records(context: AbstractComputeExecutionContext, input_prefix: str) -> str:
    """
    :return: Returns the path to the transformation output json files.
    """
    arg_dict = {
        "inputPrefix": input_prefix,
        "outputPrefix": context.solid_config["output_prefix"],
        "target_class": context.solid_config["target_class"],
        "scala_project": "dog-aging-hles-transformation"
    }
    context.resources.beam_runner.run(arg_dict)
    return context.solid_config["output_prefix"]

# todo: transform config builder
def _build_transform_config(config: dict[str, str], target_class: str):
    return {
        "output_prefix": config["output_prefix"],
        "target_class": target_class
    }



@configured(transform_records)
def hles_transform_records(config):
    return _build_transform_config(config, "org.broadinstitute.monster.dap.hles.HLESurveyTransformationPipeline")

@configured(transform_records)
def cslb_transform_records(config):
    return _build_transform_config(config, "org.broadinstitute.monster.dap.cslb.CslbTransformationPipeline")

@configured(transform_records)
def env_transform_records(config):
    return _build_transform_config(config, "org.broadinstitute.monster.dap.environment.EnvironmentTransformationPipeline")


## todo: TSV Outfiles
@solid(
    config_schema={
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
