from dagster import PipelineDefinition, repository, schedule, ScheduleEvaluationContext

from dap_orchestration.pipelines import refresh_data_all
from dap_orchestration.repositories.base_repositories import all_jobs
from datetime import timezone
from datetime import datetime, time


@schedule(
    cron_schedule="00 14 * * 2",
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
            }
        }
    }


@repository
def repositories() -> list[PipelineDefinition]:
    return all_jobs() + [weekly_sample_refresh]
