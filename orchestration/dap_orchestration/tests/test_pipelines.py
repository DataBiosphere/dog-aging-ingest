import unittest

import dap_orchestration
from dagster import ModeDefinition, execute_solid, SolidExecutionResult

import dap_orchestration.resources
import dap_orchestration.solids

import os


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.base_solid_config = {
            "resources": {
                "beam_runner": {
                    "config": {
                        "working_dir": "/Users/qhoque/GIT/dog-aging-ingest",
                    }
                }
            }
        }
        # todo we should not have the output prefix hardcoded into these solid configs
        self.extract_config = {
            "pull_data_dictionaries": False,
            "output_prefix": "gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/dagster_test_20210511/raw",
            "end_time": "2020-05-19T23:59:59-05:00",
            "api_token": os.environ["API_TOKEN"]
        }
        self.transform_config = {
            "output_prefix": "gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/dagster_test_20210511/transform",
        }
        # todo factor this out (repeated from working_dir in the base_solid_config)
        # todo also this should not have my user file path hardcoded like this
        self.outfiles_config = {
            "working_dir": "/Users/qhoque/GIT/dog-aging-ingest",
        }
        self.mode = ModeDefinition(
            resource_defs={
                "beam_runner": dap_orchestration.resources.test_beam_runner,
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

    # todo: the transform tests should not be dependent on the extract ones

    def test_hles_transform(self):
        hles_transform_config = {
            "solids": {
                "hles_transform_records": {
                    "config": self.transform_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **hles_transform_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.hles_transform_records,
            mode_def=self.mode,
            input_values={
                "input_prefix": self.extract_config["output_prefix"]+"/hles"
            },
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    def test_cslb_transform(self):
        cslb_transform_config = {
            "solids": {
                "cslb_transform_records": {
                    "config": self.transform_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **cslb_transform_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.cslb_transform_records,
            mode_def=self.mode,
            input_values={
                "input_prefix": self.extract_config["output_prefix"]+"/cslb"
            },
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    def test_env_transform(self):
        env_transform_config = {
            "solids": {
                "env_transform_records": {
                    "config": self.transform_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **env_transform_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.env_transform_records,
            mode_def=self.mode,
            input_values={
                "input_prefix": self.extract_config["output_prefix"]+"/environment"
            },
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    # todo test write outfiles
    def test_write_outfiles(self):
        write_outfiles_config = {
            "solids": {
                "write_outfiles": {
                    "config": self.outfiles_config
                }
            }
        }
        dataflow_config = {**self.base_solid_config, **write_outfiles_config}
        result: SolidExecutionResult = execute_solid(
            dap_orchestration.solids.write_outfiles,
            mode_def=self.mode,
            input_values={
                # this is dependent on the transform output
                #"input_prefix": self.transform_config["output_prefix"]
                "input_prefix": "gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/20210329/transform"
            },
            run_config=dataflow_config
        )

        self.assertTrue(result.success)

    # todo e2e to run everything
