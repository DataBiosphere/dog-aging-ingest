import subprocess
from dataclasses import dataclass
from typing import Optional

from dagster import DagsterLogManager, Field, resource, StringSource
from dagster.core.execution.context.init import InitResourceContext


@dataclass
class LocalBeamRunner:
    working_dir: str
    logger: DagsterLogManager
    api_token: str
    project: str

    def run(
            self,
            target_module: str,
            target_class: str,
            output_prefix: str,
            input_prefix: Optional[str]
    ) -> None:
        self.logger.info("Local beam runner")
        args = f'{target_module}/runMain {target_class} ' \
               f'--apiToken={self.api_token} ' \
               f'--outputPrefix={output_prefix} ' \
               f'--inputPrefix={input_prefix} ' \
               f'--pullDataDictionaries=false ' \
               f'--runner=dataflow  ' \
               f'--project={self.project} ' \
               f'--region=us-central1 ' \
               f'--workerMachineType=n1-standard-1 ' \
               f'--autoscalingAlgorithm=THROUGHPUT_BASED ' \
               f'--numWorkers=4 ' \
               f'--maxNumWorkers=8 ' \
               f'--experiments=shuffle_mode=service"'
        subprocess.run(
            ["sbt", args],
            check=True,
            cwd=self.working_dir
        )


@resource(config_schema={
    "working_dir": Field(StringSource),
    "api_token": Field(StringSource),
    "project": Field(StringSource)
})
def local_beam_runner(init_context: InitResourceContext) -> LocalBeamRunner:
    return LocalBeamRunner(
        working_dir=init_context.resource_config["working_dir"],
        logger=init_context.log_manager,
        api_token=init_context.resource_config["api_token"],
        project=init_context.resource_config["project"],
    )