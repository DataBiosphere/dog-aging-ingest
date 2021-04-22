import subprocess
from typing import Any
from dataclasses import dataclass

from dagster import DagsterLogManager, Field, resource, StringSource, IntSource
from dagster.core.execution.context.init import InitResourceContext

# todo: Do not copy and paste the local beam runner from HCA, extract a common lib resource
@dataclass
class LocalBeamRunner:
    working_dir: str
    logger: DagsterLogManager
    scala_project: str
    target_class: str
    api_token: str
    region: str
    worker_machine_type: str
    autoscaling_algorithm: str
    num_workers: int
    max_num_workers: int
    google_project: str

    def __post_init__(self):
        self.arg_dict = {
            # resource config - how stuff behaves
            # will not affect what is returned - will only affect run performance/success
            "apiToken": self.api_token,
            "region": self.region,
            "workerMachineType": self.worker_machine_type,
            "autoscalingAlgorithm": self.autoscaling_algorithm,
            "numWorkers": str(self.num_workers),
            "maxNumWorkers": str(self.max_num_workers),
            "project": self.google_project,
            # hardcode anything that doesn't fall into solids/resources ie. dataflow
            "runner": "dataflow",
            # To use the service-based Dataflow Shuffle in your batch pipelines
            "experiments": "shuffle_mode=service",
        }

    def run(
            self,
            arg_dict: dict[str, Any],
    ) -> None:
        # create a new dictionary containing the keys and values of arg_dict + solid arguments
        dataflow_run_flags = {**self.arg_dict, **arg_dict}
        self.logger.info("Local beam runner")
        # list comprehension over args_dict to get flags
        flags = " ".join([f'--{arg}={value}' for arg, value in dataflow_run_flags.items()])
        subprocess.run(
            ["sbt", f'{self.scala_project}/runMain {self.target_class} {flags}'],
            check=True,
            cwd=self.working_dir
        )

@resource({
    "working_dir": Field(StringSource),
    "scala_project": Field(StringSource),
    "target_class": Field(StringSource),
    "api_token": Field(StringSource),
    "region": Field(StringSource),
    "worker_machine_type": Field(StringSource),
    "autoscaling_algorithm": Field(StringSource),
    "num_workers": Field(IntSource),
    "max_num_workers": Field(IntSource),
    "google_project": Field(StringSource),
})
def local_beam_runner(init_context: InitResourceContext) -> LocalBeamRunner:
    return LocalBeamRunner(
        working_dir=init_context.resource_config["working_dir"],
        scala_project=init_context.resource_config["scala_project"],
        target_class=init_context.resource_config["target_class"],
        api_token=init_context.resource_config["api_token"],
        region=init_context.resource_config["region"],
        worker_machine_type=init_context.resource_config["worker_machine_type"],
        autoscaling_algorithm=init_context.resource_config["autoscaling_algorithm"],
        num_workers=init_context.resource_config["num_workers"],
        max_num_workers=init_context.resource_config["max_num_workers"],
        google_project=init_context.resource_config["google_project"],
        logger=init_context.log_manager,
    )

class TestBeamRunner:
    def run(self, arg_dict: dict[str, Any]) -> None:
        # no thoughts, head empty
        pass


@resource
def test_beam_runner(init_context: InitResourceContext) -> TestBeamRunner:
    return TestBeamRunner()