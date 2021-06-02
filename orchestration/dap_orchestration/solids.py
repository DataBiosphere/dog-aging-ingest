from dagster import Bool, String, solid, configured
from dagster.core.execution.context.compute import AbstractComputeExecutionContext


@solid(
    required_resource_keys={"beam_runner", "refresh_directory"},
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "api_token": String,
        "output_prefix": String
    }
)
def extract_records(context: AbstractComputeExecutionContext) -> None:
    """
    This solid will take in the arguments provided in context and run the sbt extraction code
    for the predefined pipeline (HLES, CSLB, or ENVIRONMENT) using the specified runner.
    """
    arg_dict = {
        "pullDataDictionaries": "true" if context.solid_config["pull_data_dictionaries"] else "false",
        "outputPrefix": f"{context.resources.refresh_directory}/{context.solid_config['output_prefix']}",
        "endTime": context.solid_config["end_time"],
        "apiToken": context.solid_config["api_token"]
    }
    context.resources.beam_runner.run(arg_dict)


def _build_extract_config(config: dict[str, str], output_prefix: str) -> dict[str, str]:
    return {
        "pull_data_dictionaries": config["pull_data_dictionaries"],
        "output_prefix": output_prefix,
        "end_time": config["end_time"],
        "api_token": config["api_token"]
    }


@configured(extract_records)
def hles_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config,
        "raw"
    )


@configured(extract_records)
def cslb_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config,
        "raw"
    )


@configured(extract_records)
def env_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config,
        "raw"
    )


@solid(
    required_resource_keys={"beam_runner", "refresh_directory"},
    config_schema={
        "input_prefix": String,
        "output_prefix": String
    }
)
def transform_records(context: AbstractComputeExecutionContext) -> None:
    """
    This solid will take in the arguments provided in context and run the sbt transformation code
    for the predefined pipeline (HLES, CSLB, or ENVIORONMENT) using the specified runner.
    """
    arg_dict = {
        "inputPrefix": f'{context.resources.refresh_directory}/{context.solid_config["input_prefix"]}',
        "outputPrefix": f'{context.resources.refresh_directory}/{context.solid_config["output_prefix"]}',
    }
    context.resources.beam_runner.run(arg_dict)


def _build_transform_config(input_prefix: str, output_prefix: str) -> dict[str, str]:
    return {
        "input_prefix": input_prefix,
        "output_prefix": output_prefix,
    }


@configured(transform_records)
def hles_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        "raw/hles",
        "transform"
    )


@configured(transform_records)
def cslb_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        "raw/cslb",
        "transform"
    )


@configured(transform_records)
def env_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        "raw/environment",
        "transform"
    )


@solid(
    required_resource_keys={"refresh_directory", "outfiles_writer"},
    config_schema={
        "working_dir": String,
    }
)
def write_outfiles(context: AbstractComputeExecutionContext) -> None:
    """
    This solid will take in the arguments provided in context and call the convert-output-to-tsv script
    on the transform outputs. The script is currently expecting transform outputs from all 3 pipelines and will
    error if one of them is not found at the input directory.
    """
    context.resources.outfiles_writer.run(context.solid_config["working_dir"], context.resources.refresh_directory)
