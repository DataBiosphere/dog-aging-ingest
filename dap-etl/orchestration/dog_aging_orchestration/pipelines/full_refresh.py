from dagster import pipeline, solid, Nothing, InputDefinition, ModeDefinition, String, Bool
from dagster.core.execution.context.compute import AbstractComputeExecutionContext

from dog_aging_orchestration.resources.beam import local_beam_runner

base_mode = ModeDefinition(
    name="base",
    resource_defs={
        "beam_runner": local_beam_runner
    }
)


@solid(
    required_resource_keys={'beam_runner'},
    config_schema={
        "output_prefix": String
    }
)
def run_hles_extraction(context: AbstractComputeExecutionContext) -> String:
    context.log.info("Running HLES extraction...")
    context.resources.beam_runner.run(
        target_module="dog-aging-hles-extraction",
        target_class="org.broadinstitute.monster.dap.hles.HLESurveyExtractionPipeline",
        output_prefix=context.solid_config["output_prefix"],
        input_prefix=None

    )
    return context.solid_config["output_prefix"]


@solid(
    required_resource_keys={'beam_runner'},
    config_schema={
        "output_prefix": String
    }
)
def run_cslb_extraction(context: AbstractComputeExecutionContext) -> String:
    context.log.info("Running CSLB extraction...")
    context.resources.beam_runner.run(
        target_module="dog-aging-hles-extraction",
        target_class="org.broadinstitute.monster.dap.cslb.CslbExtractionPipeline",
        output_prefix=context.solid_config["output_prefix"],
        input_prefix=None
    )
    return context.solid_config["output_prefix"]


@solid(
    input_defs=[InputDefinition(name="extract_output_path", dagster_type=String)],
    config_schema={
        "output_prefix": String
    }
)
def run_hles_transformation(context: AbstractComputeExecutionContext, extract_output_path: String) -> Nothing:
    context.log.info("Running HLES transformation...")
    context.resources.beam_runner.run(
        target_module="dog-aging-hles-transformation",
        target_class="org.broadinstitute.monster.dap.hles.HLESurveyTransformationPipeline",
        output_prefix=context.solid_config["output_prefix"],
        input_prefix=extract_output_path + "/hles",
    )


@solid(
    input_defs=[InputDefinition(name="extract_output_path", dagster_type=String)],
    config_schema={
        "output_prefix": String
    }
)
def run_cslb_transformation(context: AbstractComputeExecutionContext, extract_output_path: String) -> Nothing:
    context.log.info("Running CSLB transformation...")
    context.resources.beam_runner.run(
        target_module="dog-aging-hles-transformation",
        target_class="org.broadinstitute.monster.dap.cslb.CslbTransformationPipeline",
        output_prefix=context.solid_config["output_prefix"],
        input_prefix=extract_output_path + "/cslb",
    )


@solid(
    required_resource_keys={'beam_runner'},
    config_schema={
        "output_prefix": String
    }
)
def run_env_extraction(context: AbstractComputeExecutionContext) -> String:
    context.log.info("Running env extraction (TODO noop)...")
    # TODO this is slow
    return context.solid_config["output_prefix"]


@solid(
    input_defs=[InputDefinition(name="extract_output_path", dagster_type=String)],
    config_schema={
        "output_prefix": String
    }
)
def run_env_transformation(context: AbstractComputeExecutionContext, extract_output_path: String) -> Nothing:
    context.log.info("Running env transformation (TODO noop)...")
    return context.solid_config["output_prefix"]


@solid(
    input_defs=[
        InputDefinition(name="hles_transform_result", dagster_type=Nothing),
        InputDefinition(name="cslb_transform_result", dagster_type=Nothing),
        InputDefinition(name="env_transform_result", dagster_type=Nothing),
    ]

)
def gen_tsvs(context: AbstractComputeExecutionContext) -> Nothing:
    context.log.info("Generating TSVs...")


@pipeline(
    mode_defs=[base_mode]
)
def full_refresh_pipeline() -> None:
    hles = run_hles_transformation(run_hles_extraction())
    cslb = run_cslb_transformation(run_cslb_extraction())
    env = run_env_transformation(run_env_extraction())

    gen_tsvs(hles_transform_result=hles, cslb_transform_result=cslb, env_transform_result=env)
