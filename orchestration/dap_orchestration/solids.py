from datetime import datetime
from typing import Union

from google.cloud.storage.client import Client
from google.cloud.storage.blob import Blob
from dagster import Bool, Failure, String, solid, configured, InputDefinition, Nothing, Noneable
from dagster.core.execution.context.compute import AbstractComputeExecutionContext
from dagster_utils.contrib.google import parse_gs_path
from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client

from dap_orchestration.types import DapSurveyType, FanInResultsWithTsvDir

extract_project = "dog-aging-hles-extraction"
transform_project = "dog-aging-hles-transformation"
class_prefix = "org.broadinstitute.monster.dap"
terra_surveys = {"sample"}


def check_date_format(input_date: str) -> String:
    format = "%Y-%m-%dT%H:%M:%S%z"
    try:
        datetime.strptime(input_date, format)
        return input_date
    except ValueError:
        raise Failure(f"{input_date} should be in the following format: '{format}'")


@solid(
    required_resource_keys={"extract_beam_runner", "refresh_directory", "api_token"},
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "start_time": Noneable(String),
        "output_prefix": String,
        "target_class": String,
        "scala_project": String,
        "dap_survey_type": String
    },
    input_defs=[InputDefinition("ignore", Nothing)]
)
def base_extract_records(context: AbstractComputeExecutionContext) -> DapSurveyType:
    """
    This solid will take in the arguments provided in context and run the sbt extraction code
    for the predefined pipeline (HLES, CSLB, ENVIRONMENT, Sample, EOLS) using the specified runner.
    """
    arg_dict = {
        "pullDataDictionaries": "true" if context.solid_config["pull_data_dictionaries"] else "false",
        "outputPrefix": f"{context.resources.refresh_directory}/{context.solid_config['output_prefix']}",
        "endTime": check_date_format(context.solid_config["end_time"]),
        "apiToken": context.resources.api_token.base_api_token,
    }

    if context.solid_config["start_time"]:
        arg_dict["startTime"] = check_date_format(context.solid_config["start_time"])

    context.resources.extract_beam_runner.run(arg_dict,
                                              target_class=context.solid_config["target_class"],
                                              scala_project=context.solid_config["scala_project"],
                                              command=[
                                                  f"/app/bin/{context.solid_config['dap_survey_type']}-extraction-pipeline"]
                                              )

    return DapSurveyType(context.solid_config["dap_survey_type"])


def _build_extract_config(config: dict[str, str], output_prefix: str,
                          target_class: str, scala_project: str, dap_survey_type: DapSurveyType) -> dict[str, str]:
    return {
        "pull_data_dictionaries": config["pull_data_dictionaries"],
        "output_prefix": output_prefix,
        "end_time": config["end_time"],
        "start_time": config["start_time"],
        "target_class": target_class,
        "scala_project": scala_project,
        "dap_survey_type": dap_survey_type
    }


@configured(
    base_extract_records,
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "start_time": Noneable(String)
    }
)
def hles_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config=config,
        output_prefix="raw",
        target_class=f"{class_prefix}.hles.HLESExtractionPipeline",
        scala_project=extract_project,
        dap_survey_type=DapSurveyType("hles")
    )


@configured(
    base_extract_records,
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "start_time": Noneable(String)
    }
)
def cslb_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config=config,
        output_prefix="raw",
        target_class=f"{class_prefix}.cslb.CslbExtractionPipeline",
        scala_project=extract_project,
        dap_survey_type=DapSurveyType("cslb")
    )


@solid(
    required_resource_keys={"extract_beam_runner", "refresh_directory", "api_token"},
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "start_time": Noneable(String)
    },
    input_defs=[InputDefinition("ignore", Nothing)]
)
def env_extract_records(context: AbstractComputeExecutionContext) -> DapSurveyType:
    arg_dict = {
        "pullDataDictionaries": "true" if context.solid_config["pull_data_dictionaries"] else "false",
        "outputPrefix": f"{context.resources.refresh_directory}/raw",
        "endTime": check_date_format(context.solid_config["end_time"]),
        "apiToken": context.resources.api_token.env_api_token,
    }
    if context.solid_config["start_time"]:
        arg_dict["startTime"] = check_date_format(context.solid_config["start_time"])

    context.resources.extract_beam_runner.run(arg_dict,
                                              target_class=f"{class_prefix}.environment.EnvironmentExtractionPipeline",
                                              scala_project=extract_project,
                                              command=[f"/app/bin/environment-extraction-pipeline"]
                                              )
    return DapSurveyType("environment")


@configured(
    base_extract_records,
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "start_time": Noneable(String)
    }
)
def sample_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config=config,
        output_prefix="raw",
        target_class=f"{class_prefix}.sample.SampleExtractionPipeline",
        scala_project=extract_project,
        dap_survey_type=DapSurveyType("sample")
    )


@configured(
    base_extract_records,
    config_schema={
        "pull_data_dictionaries": Bool,
        "end_time": String,
        "start_time": Noneable(String)
    }
)
def eols_extract_records(config: dict[str, str]) -> dict[str, str]:
    return _build_extract_config(
        config=config,
        output_prefix="raw",
        target_class=f"{class_prefix}.eols.EolsExtractionPipeline",
        scala_project=extract_project,
        dap_survey_type=DapSurveyType("eols")
    )


@solid(
    required_resource_keys={"transform_beam_runner", "refresh_directory"},
    config_schema={
        "output_prefix": String,
        "target_class": String,
        "scala_project": String,
    },
    input_defs=[InputDefinition("dap_survey_type", String)]
)
def transform_records(context: AbstractComputeExecutionContext, dap_survey_type: DapSurveyType) -> DapSurveyType:
    """
    This solid will take in the arguments provided in context and run the sbt transformation code
    for the predefined pipeline (HLES, CSLB, or ENVIORONMENT) using the specified runner.
    """
    arg_dict = {
        "inputPrefix": f'{context.resources.refresh_directory}/raw/{dap_survey_type}',
        "outputPrefix": f'{context.resources.refresh_directory}/{context.solid_config["output_prefix"]}',
    }
    context.resources.transform_beam_runner.run(arg_dict,
                                                target_class=context.solid_config["target_class"],
                                                scala_project=context.solid_config["scala_project"],
                                                command=[f"/app/bin/{dap_survey_type}-transformation-pipeline"]
                                                )

    return dap_survey_type


def _build_transform_config(
        output_prefix: str,
        target_class: str, scala_project: str) -> dict[str, str]:
    return {
        "output_prefix": output_prefix,
        "target_class": target_class,
        "scala_project": scala_project,
    }


@configured(transform_records)
def hles_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        output_prefix="transform",
        target_class=f"{class_prefix}.hles.HLESTransformationPipeline",
        scala_project=transform_project
    )


@configured(transform_records)
def cslb_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        output_prefix="transform",
        target_class=f"{class_prefix}.cslb.CslbTransformationPipeline",
        scala_project=transform_project
    )


@configured(transform_records)
def env_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        output_prefix="transform",
        target_class=f"{class_prefix}.environment.EnvironmentTransformationPipeline",
        scala_project=transform_project
    )


@configured(transform_records)
def sample_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        output_prefix="transform",
        target_class=f"{class_prefix}.sample.SampleTransformationPipeline",
        scala_project=transform_project
    )


@configured(transform_records)
def eols_transform_records(config: dict[str, str]) -> dict[str, str]:
    return _build_transform_config(
        output_prefix="transform",
        target_class=f"{class_prefix}.eols.EolsTransformationPipeline",
        scala_project=transform_project
    )


@solid(
    required_resource_keys={"refresh_directory", "outfiles_writer"},
    config_schema={
        "output_dir": str,
        "output_in_firecloud_format": bool
    }
)
def write_outfiles(context: AbstractComputeExecutionContext,
                   fan_in_results: list[DapSurveyType]) -> FanInResultsWithTsvDir:
    """
    This solid will take in the arguments provided in context and call the convert-output-to-tsv script
    on the transform outputs. The script is currently expecting transform outputs from all 3 pipelines and will
    error if one of them is not found at the input directory.

    NOTE: The fan_in_results param allows to introduce a fan-in dependency from upstream transformation
    solids, but is ignored by this solid.
    """
    tsv_directory = context.solid_config["output_dir"]
    context.resources.outfiles_writer.run(
        tsv_directory,
        context.resources.refresh_directory,
        fan_in_results,
        context.solid_config["output_in_firecloud_format"]
    )

    return FanInResultsWithTsvDir(fan_in_results, tsv_directory)


@configured(write_outfiles, config_schema={"output_dir": str})
def write_outfiles_in_tsv_format(config: dict[str, str]) -> dict[str, Union[str, bool]]:
    return {
        "output_dir": config["output_dir"],
        "output_in_firecloud_format": False
    }


@configured(write_outfiles, config_schema={"output_dir": str})
def write_outfiles_in_terra_format(config: dict[str, str]) -> dict[str, Union[str, bool]]:

    return {
        "output_dir": config["output_dir"],
        "output_in_firecloud_format": True
    }


def _copy_file(source_bucket_name: str, source_prefix: str, dest_bucket_name: str,
               dest_prefix: str, storage_client: Client) -> Blob:
    source_bucket = storage_client.get_bucket(source_bucket_name)
    dest_bucket = storage_client.get_bucket(dest_bucket_name)

    blob = source_bucket.get_blob(source_prefix)
    if not blob.size:
        raise Failure(f"Could not find {blob.name}")

    new_blob = source_bucket.copy_blob(blob, dest_bucket, dest_prefix)
    new_blob.acl.save(blob.acl)
    if not new_blob.size:
        raise Failure(f"ERROR uploading data files to {dest_bucket_name}/{dest_prefix}.")

    return new_blob


@solid(
    required_resource_keys={"refresh_directory", "gcs"},
    config_schema={
        "destination_gcs_path": String,
    }
)
def copy_outfiles_to_terra(
    context: AbstractComputeExecutionContext,
    survey_types_with_path: FanInResultsWithTsvDir
) -> None:
    """
    This solid will copy the tsvs created from write_outfiles to the provided GCS bucket. The script
    will check that the file exists before uploading it and will error if it does not exist. This only copies those
    surveys that are allow-listed in the terra_surveys constant.

    NOTE: The fan_in_results param allows to introduce a fan-in dependency from the upstream write_outfiles
    solid and is used to iterate through TSV uploads for the surveys being refreshed.
    """
    for survey in survey_types_with_path.fan_in_results:
        if survey not in terra_surveys:
            context.log.info(f"Skipping copy survey to terra [survey_type={survey}]")
            continue

        storage_client = context.resources.gcs
        tsv_dir = f"{survey_types_with_path.tsv_dir}/tsv_output"
        full_dest_path = context.solid_config["destination_gcs_path"]

        source_bucket_with_prefix = parse_gs_path(tsv_dir)
        dest_bucket_with_prefix = parse_gs_path(full_dest_path)

        context.log.info(f"Uploading {survey} data files to {full_dest_path}")
        source_prefix = f"{source_bucket_with_prefix.prefix}/{survey}.tsv"
        dest_prefix = f"{dest_bucket_with_prefix.prefix}/{survey}.tsv"
        new_blob = _copy_file(
            source_bucket_name=source_bucket_with_prefix.bucket,
            source_prefix=source_prefix,
            dest_bucket_name=dest_bucket_with_prefix.bucket,
            dest_prefix=dest_prefix,
            storage_client=storage_client
        )
        context.log.info(f"{new_blob.name} successfully uploaded ({new_blob.size} bytes).")

        date_suffix = datetime.now().strftime("%m%d%Y")
        dest_dated_prefix = f"{dest_bucket_with_prefix.prefix}/{survey}_{date_suffix}.tsv"
        new_blob = _copy_file(
            source_bucket_name=source_bucket_with_prefix.bucket,
            source_prefix=source_prefix,
            dest_bucket_name=dest_bucket_with_prefix.bucket,
            dest_prefix=dest_dated_prefix,
            storage_client=storage_client
        )
        context.log.info(f"{new_blob.name} successfully uploaded ({new_blob.size} bytes).")
