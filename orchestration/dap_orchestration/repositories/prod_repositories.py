from dagster import PipelineDefinition, repository, schedule, ScheduleEvaluationContext
from dagster_gcp.gcs import gcs_pickle_io_manager
from dagster_utils.resources.beam.k8s_beam_runner import k8s_dataflow_beam_runner
from dagster_utils.resources.google_storage import google_storage_client

from dap_orchestration.config import preconfigure_resource_for_mode
from dap_orchestration.pipelines import refresh_data_all
from dap_orchestration.resources import refresh_directory, outfiles_writer, api_token


@schedule(
    cron_schedule="00 04 * * 1",
    pipeline_name="refresh_data_all",
    execution_timezone="US/Eastern",
    solid_selection=["sample_extract_records*"],
    mode="prod"
)
def weekly_sample_refresh(context: ScheduleEvaluationContext) -> dict[str, object]:
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
            "write_outfiles": {
                "config": {
                    "output_dir": f"gs://broad-dsp-monster-dap-prod-storage/weekly_refresh/{date.strftime('%Y%m%d')}"
                }
            },
            "sample_extract_records": {
                "config": {
                    "pull_data_dictionaries": False,
                    "end_time": date.strftime("%Y-%m-%dT%H:%M:%S") + offset_time
                }
            },
            "upload_to_gcs": {
                "config": {
                    "destination_gcs_path": f"gs://fc-6f3f8275-c9b4-4dcf-b2de-70f8d74f0874/ref"
                }
            }
        }
    }


@repository
def repositories() -> list[PipelineDefinition]:
    return [
        weekly_sample_refresh,
        refresh_data_all.to_job(resource_defs={
            "extract_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod_extract"),
            "transform_beam_runner": preconfigure_resource_for_mode(k8s_dataflow_beam_runner, "prod_transform"),
            "refresh_directory": refresh_directory,
            "outfiles_writer": outfiles_writer,
            "api_token": preconfigure_resource_for_mode(api_token, "prod"),
            "io_manager": preconfigure_resource_for_mode(gcs_pickle_io_manager, "prod"),
            "gcs": google_storage_client,
        })
    ]
