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
        self.extract_config = {
            "pull_data_dictionaries": False,
            "output_prefix": "gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/dagster_test_20210513/raw",
            "end_time": "2020-05-19T23:59:59-05:00",
            "api_token": "fake_api_token"
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

        # todo test hles transform

        # todo test cslb transform

        # todo test env transform

        # todo test hles write outfiles

        # todo test cslb write outfiles

        # todo test env transform

        # todo e2e to run everything
