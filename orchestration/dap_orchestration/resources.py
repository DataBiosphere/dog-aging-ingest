from dataclasses import dataclass

from dagster import Field, resource, StringSource
from dagster.core.execution.context.init import InitResourceContext

from dap_orchestration.tsv_convert import convert_to_tsv


@dataclass
class ApiTokenConfig:
    base_api_token: str
    env_api_token: str

    def __init__(self, base_token: str, env_token: str) -> None:
        self.base_api_token = base_token
        self.env_api_token = env_token


@resource({
    "base_api_token": StringSource,
    "env_api_token": StringSource
})
def api_token(init_context: InitResourceContext) -> ApiTokenConfig:
    return ApiTokenConfig(init_context.resource_config['base_api_token'], init_context.resource_config['env_api_token'])


@resource({"refresh_directory": Field(StringSource)})
def refresh_directory(init_context: InitResourceContext) -> str:
    directory: str = init_context.resource_config["refresh_directory"]
    return directory


@resource
def test_refresh_directory(init_context: InitResourceContext) -> str:
    return "fake"


class OutfilesWriter:
    def run(self, output_dir: str, refresh_dir: str) -> None:
        convert_to_tsv(f"gs://{refresh_dir}/transform", f'gs://{output_dir}/tsv_output', firecloud=False)



@resource
def outfiles_writer(init_context: InitResourceContext) -> OutfilesWriter:
    return OutfilesWriter()


class TestOutfilesWriter:
    def run(self, working_dir: str, refresh_dir: str) -> None:
        pass


@resource
def test_outfiles_writer(init_context: InitResourceContext) -> TestOutfilesWriter:
    return TestOutfilesWriter()
