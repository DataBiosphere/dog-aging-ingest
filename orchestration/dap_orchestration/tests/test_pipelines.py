import unittest

import dap_orchestration
from dagster import ModeDefinition, execute_solid, SolidExecutionResult

import dap_orchestration.resources
import dap_orchestration.solids


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.base_solid_config: dict[str, object] = {
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
        self.extract_config = {
            "pull_data_dictionaries": False,
            "end_time": "2020-05-19T23:59:59-05:00",
            "api_token": "ddddd"
        }

        self.outfiles_config = {
            "working_dir": "/example/local_beam_runner/bar",

        }
        self.mode = ModeDefinition(
            resource_defs={
                "beam_runner": dap_orchestration.resources.test_beam_runner,
                "refresh_directory": dap_orchestration.resources.test_refresh_directory,
                "outfiles_writer": dap_orchestration.resources.test_outfiles_writer
            }
        )

    def test_hles_extract(self):
        hles_extract_config = {
            "solids": {
                "hles_extract_records": {
                    "config": self.extract_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **hles_extract_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.hles_extract_records,
            mode_def=self.mode,
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    def test_cslb_extract(self):
        cslb_extract_config = {
            "solids": {
                "cslb_extract_records": {
                    "config": self.extract_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **cslb_extract_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.cslb_extract_records,
            mode_def=self.mode,
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    def test_environment_extract(self):
        env_extract_config = {
            "solids": {
                "env_extract_records": {
                    "config": self.extract_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **env_extract_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.env_extract_records,
            mode_def=self.mode,
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    def test_hles_transform(self):
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.hles_transform_records,
            mode_def=self.mode,
            run_config=self.base_solid_config
        )

        self.assertTrue(result.success)

    def test_cslb_transform(self):
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.cslb_transform_records,
            mode_def=self.mode,
            run_config=self.base_solid_config
        )

        self.assertTrue(result.success)

    def test_env_transform(self):
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.env_transform_records,
            mode_def=self.mode,
            run_config=self.base_solid_config
        )

        self.assertTrue(result.success)

    def test_write_outfiles(self):
        write_outfiles_config = {
            "solids": {
                "write_outfiles": {
                    "config": {
                        "working_dir": "/example/local_beam_runner/bar",
                    }
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **write_outfiles_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.write_outfiles,
            mode_def=self.mode,
            run_config=dataflow_config
        )

        self.assertTrue(result.success)
