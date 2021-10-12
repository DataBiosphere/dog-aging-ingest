from dagster import PipelineDefinition, repository, schedule, ScheduleEvaluationContext, in_process_executor
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.google_storage import google_storage_client
from dagster_utils.resources.slack import live_slack_client, console_slack_client

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.pipelines import refresh_data_all, arh_testing
from dap_orchestration.repositories.common import build_pipeline_failure_sensor
from dap_orchestration.resources import refresh_directory, outfiles_writer, api_token


def build_refresh_data_all_job(name: str) -> PipelineDefinition:
    return refresh_data_all.to_job(
        name=name,
        resource_defs={
            "extract_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod_extract"),
            "transform_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod_transform"),
            "refresh_directory": refresh_directory,
            "outfiles_writer": outfiles_writer,
            "api_token": preconfigure_resource_for_mode(api_token, "prod"),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "gcs": google_storage_client,
            "slack_client": console_slack_client, #preconfigure_resource_for_mode(live_slack_client, "prod")
        },
        # the default multiprocess_executor dispatches all DAP surveys concurrently, exceeding resource quotas
        executor_def=in_process_executor
    )


@schedule(
    cron_schedule="0 4 * * 3",
    job=build_refresh_data_all_job("weekly_data_refresh"),
    execution_timezone="US/Eastern"
)
def weekly_data_refresh(context: ScheduleEvaluationContext) -> dict[str, object]:
    date = context.scheduled_execution_time
    tz = date.strftime("%z")
    offset_time = f"{tz[0:3]}:{tz[3:]}"
    return {
        "resources": {
            "refresh_directory": {
                "config": {
                    "refresh_directory": f"gs://broad-dsp-monster-dap-prod-temp-storage/staging/{date.strftime('%Y%m%d')}"
                }
            }
        },
        "solids": {
            "sample_extract_records": {
                "config": {
                    "pull_data_dictionaries": False,
                    "end_time": date.strftime("%Y-%m-%dT%H:%M:%S") + offset_time
                }
            },
            "cslb_extract_records": {
                "config": {
                    "pull_data_dictionaries": False,
                    "end_time": date.strftime("%Y-%m-%dT%H:%M:%S") + offset_time
                }
            },
            "env_extract_records": {
                "config": {
                    "pull_data_dictionaries": False,
                    "end_time": date.strftime("%Y-%m-%dT%H:%M:%S") + offset_time
                }
            },
            "eols_extract_records": {
                "config": {
                    "pull_data_dictionaries": False,
                    "end_time": date.strftime("%Y-%m-%dT%H:%M:%S") + offset_time
                }
            },
            "hles_extract_records": {
                "config": {
                    "pull_data_dictionaries": False,
                    "end_time": date.strftime("%Y-%m-%dT%H:%M:%S") + offset_time
                }
            },
            "write_outfiles_in_terra_format": {
                "config": {
                    "output_dir": f"gs://broad-dsp-monster-dap-prod-storage/weekly_refresh/{date.strftime('%Y%m%d')}/terra_output"
                }
            },
            "write_outfiles_in_tsv_format": {
                "config": {
                    "output_dir": f"gs://broad-dsp-monster-dap-prod-storage/weekly_refresh/{date.strftime('%Y%m%d')}/default_output"
                }
            },
            "copy_outfiles_to_terra": {
                "config": {
                    "destination_gcs_path": f"gs://fc-6f3f8275-c9b4-4dcf-b2de-70f8d74f0874/ref"
                }
            },
        }
    }


@repository
def repositories() -> list[PipelineDefinition]:
    return [
        build_pipeline_failure_sensor(),
        build_refresh_data_all_job("refresh_data_all"),
        arh_testing
     #   weekly_data_refresh
    ]
