import pytest
from dagster import ModeDefinition, execute_solid, SolidExecutionResult, ResourceDefinition
from dagster_utils.resources.beam.noop_beam_runner import noop_beam_runner

import dap_orchestration
import dap_orchestration.resources
import dap_orchestration.solids


@pytest.fixture
def base_solid_config():
    return {
        "resources": {
            "beam_runner": {
                "config": {
                    "working_dir": "/example/local_beam_runner/bar",
                }
            },
            "refresh_directory": {
                "config": {
                    "refresh_dir": "/example/local_beam_runner/bar",
                }
            }
        }
    }


@pytest.fixture
def extract_config():
    return {
        "pull_data_dictionaries": False,
        "end_time": "2020-05-19T23:59:59-05:00",
    }


@pytest.fixture
def outfiles_config():
    return {
        "working_dir": "/example/local_beam_runner/bar",
    }


@pytest.fixture
def mode():
    return ModeDefinition(
        resource_defs={
            "beam_runner": noop_beam_runner,
            "refresh_directory": dap_orchestration.resources.test_refresh_directory,
            "outfiles_writer": dap_orchestration.resources.test_outfiles_writer,
            "api_token": ResourceDefinition.mock_resource()
        }
    )


def test_hles_extract(extract_config, base_solid_config, mode):
    hles_extract_config = {
        "solids": {
            "hles_extract_records": {
                "config": extract_config
            }
        }
    }
    dataflow_config = {**base_solid_config, **hles_extract_config}
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.hles_extract_records,
        mode_def=mode,
        run_config=dataflow_config
    )

    assert result.success


def test_cslb_extract(extract_config, base_solid_config, mode):
    cslb_extract_config = {
        "solids": {
            "cslb_extract_records": {
                "config": extract_config
            }
        }
    }
    dataflow_config = {**base_solid_config, **cslb_extract_config}
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.cslb_extract_records,
        mode_def=mode,
        run_config=dataflow_config
    )

    assert result.success


def test_env_extract(extract_config, base_solid_config, mode):
    env_extract_config = {
        "solids": {
            "env_extract_records": {
                "config": extract_config
            }
        }
    }
    dataflow_config = {**base_solid_config, **env_extract_config}
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.env_extract_records,
        mode_def=mode,
        run_config=dataflow_config
    )

    assert result.success


def test_hles_transform(base_solid_config, mode):
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.hles_transform_records,
        mode_def=mode,
        run_config=base_solid_config
    )

    assert result.success


def test_cslb_transform(base_solid_config, mode):
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.cslb_transform_records,
        mode_def=mode,
        run_config=base_solid_config
    )

    assert result.success


def test_env_transform(base_solid_config, mode):
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.env_transform_records,
        mode_def=mode,
        run_config=base_solid_config
    )

    assert result.success


def test_write_outfiles(base_solid_config, mode):
    write_outfiles_config = {
        "solids": {
            "write_outfiles": {
                "config": {
                    "working_dir": "/example/local_beam_runner/bar",
                }
            }
        }
    }
    dataflow_config = {**base_solid_config, **write_outfiles_config}
    result: SolidExecutionResult = execute_solid(
        dap_orchestration.solids.write_outfiles,
        mode_def=mode,
        run_config=dataflow_config,
        input_values={"list_tables": []}
    )

    assert result.success
