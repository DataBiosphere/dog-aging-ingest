import os

from dagster import file_relative_path
from dagster.utils import load_yaml_from_globs
from dagster_slack import make_slack_on_pipeline_failure_sensor


def config_path(relative_path: str) -> str:
    path: str = file_relative_path(
        __file__, os.path.join("../config/", relative_path)
    )
    return path


def build_pipeline_failure_sensor():
    config_dict = load_yaml_from_globs(config_path("resources/live_slack_client/global.yaml"))
    slack_on_pipeline_failure = make_slack_on_pipeline_failure_sensor(
        config_dict["channel"],
        os.getenv(config_dict["token"]["env"])
    )
    return slack_on_pipeline_failure
