from dagster import PipelineDefinition, repository, weekly_schedule

from dap_orchestration.pipelines import refresh_data_all
from dap_orchestration.repositories.base_repositories import all_jobs
from datetime import datetime, time



@weekly_schedule(
    pipeline_name="refresh_data_all",
    start_date=datetime(2021, 8, 5),
    execution_time=time(10, 59),
    execution_timezone="US/Eastern",
    mode="prod",
    execution_day_of_week=5,
    solid_selection=["sample_extract_records*"]
)
def weekly_sample_refresh(date: datetime) -> dict[str, object]:
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
                    "end_time": date.strftime("%Y-%m-%d %H")
                }
            }
        }
    }



@repository
def repositories() -> list[PipelineDefinition]:
    return all_jobs() + [weekly_sample_refresh]
