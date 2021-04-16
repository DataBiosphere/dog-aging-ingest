
from dagster import Bool, String, solid
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

## Shared function to call SBT calls
## shared local beam runner resource
## Extraction Pipeline
@solid(
    required_resource_keys={"beam_runner"},
    config_schema = {
        "pull_data_dictionaries": Bool,
        "output_prefix": String,
        "end_time": String,
    }
)
def extract_records(context: AbstractComputeExecutionContext) -> str:
    """
    :return: Returns the path to extracted files.
    """
    context.resources.beam_runner.run({
        # todo: split out the three diff types of config variables
        # solid config - what the run does
        # vary from run to run - because they are core to how the run behaves
        "pullDataDictionaries": "true" if context.solid_config["pull_data_dictionaries"] else "false",
        "outputPrefix": context.solid_config["output_prefix"],
        "endTime": context.solid_config["end_time"],
    })



# syntax for setting up pipelines
# is there a way to use the same solid with multiple different configs in one pipeline


## todo: Transformation Pipeline
def  transform_records(context: AbstractComputeExecutionContext) -> str:
    """
    :return: Returns the path to the transformation output json files.
    """
    staging_bucket_name = context.solid_config["staging_bucket_name"]
    #sbt "dog-aging-hles-transformation/runMain org.broadinstitute.monster.dap.hles.HLESurveyTransformationPipeline --inputPrefix=gs://$TARGET_BUCKET/weekly_refresh/$REFRESH_SUBDIRECTORY/raw/hles --outputPrefix=gs://$TARGET_BUCKET/weekly_refresh/$REFRESH_SUBDIRECTORY/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
    #sbt "dog-aging-hles-transformation/runMain org.broadinstitute.monster.dap.cslb.CslbTransformationPipeline --inputPrefix=gs://$TARGET_BUCKET/weekly_refresh/$REFRESH_SUBDIRECTORY/raw/cslb --outputPrefix=gs://$TARGET_BUCKET/weekly_refresh/$REFRESH_SUBDIRECTORY/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
    #sbt "dog-aging-hles-transformation/runMain org.broadinstitute.monster.dap.environment.EnvironmentTransformationPipeline --inputPrefix=gs://$TARGET_BUCKET/weekly_refresh/$REFRESH_SUBDIRECTORY/raw/environment --outputPrefix=gs://$TARGET_BUCKET/weekly_refresh/$REFRESH_SUBDIRECTORY/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service --dumpHeapOnOOM --saveHeapDumpsToGcsPath=gs://$TARGET_BUCKET/weekly_refresh/transform/OOMlogs"
    blobs = context.resources.beam_runner.run({
        "apiToken": context.solid_config["api_token"],
        "pullDataDictionaries": context.solid_config["pull_data_dictionaries"],
        "outputPrefix": context.solid_config["output_prefix"],
        "runner": "dataflow",
        "project": context.solid_config["project"],
        # todo - these should go in an resource config
        "region": "quazi",
        "workerMachineType": "quazi",
        "autoscalingAlgorithm": "quazi",
        "numWorkers": "quazi",
        "maxNumWorkers": "quazi",
        "experiments": "quazi",
        "endTime": "quazi",
    })
