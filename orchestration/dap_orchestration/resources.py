import os
import subprocess

from dataclasses import dataclass
from dagster import Field, resource, StringSource
from dagster.core.execution.context.init import InitResourceContext


@dataclass
class ApiTokenClass:
    base_api_token: str
    env_api_token: str

    def __init__(self, base_token: str, env_token: str) -> None:
        self.base_api_token = base_token
        self.env_api_token = env_token


@resource({
    "base_api_token": StringSource,
    "env_api_token": StringSource
})
def api_token(init_context: InitResourceContext) -> ApiTokenClass:
    return ApiTokenClass(init_context.resource_config['base_api_token'], init_context.resource_config['env_api_token'])


@resource({"refresh_directory": Field(StringSource)})
def refresh_directory(init_context: InitResourceContext) -> str:
    directory: str = init_context.resource_config["refresh_directory"]
    return directory


@resource
def test_refresh_directory(init_context: InitResourceContext) -> str:
    return "fake"


class OutfilesWriter:
    def run(self, working_dir: str, refresh_dir: str) -> None:
        outfile_path = f'{working_dir}/tsv_output'
        if not os.path.isdir(outfile_path):
            os.mkdir(outfile_path)
        subprocess.run(
            ["python", "hack/convert-output-to-tsv.py", f"{refresh_dir}/transform", outfile_path, "--debug"],
            check=True,
            cwd=working_dir
        )


@resource
def outfiles_writer(init_context: InitResourceContext) -> OutfilesWriter:
    return OutfilesWriter()


class TestOutfilesWriter:
    def run(self, working_dir: str, refresh_dir: str) -> None:
        pass


@resource
def test_outfiles_writer(init_context: InitResourceContext) -> TestOutfilesWriter:
    return TestOutfilesWriter()
