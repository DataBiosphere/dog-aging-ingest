from datetime import datetime

import pytest
from dagster import build_schedule_context, ResourceDefinition
from pytz.reference import Eastern

from dap_orchestration.pipelines import refresh_data_all
from dap_orchestration.repositories.prod_repositories import weekly_data_refresh


@pytest.fixture
def run_config():
    return {
        'cslb_extract_records': {
            'config': {
                'end_time': '2021-10-05T00:00:00-05:00',
                'start_time': '2021-01-05T00:00:00-05:00',
                'pull_data_dictionaries': False
            }
        },
        'env_extract_records': {
            'config': {
                'end_time': '2021-10-05T00:00:00-05:00',
                'start_time': '2021-01-05T00:00:00-05:00',
                'pull_data_dictionaries': False
            }
        },
        'hles_extract_records': {
            'config': {
                'end_time': '2021-10-05T00:00:00-05:00',
                'start_time': '2021-01-05T00:00:00-05:00',
                'pull_data_dictionaries': False
            }
        },
        'sample_extract_records': {
            'config': {
                'end_time': '2021-10-05T00:00:00-05:00',
                'start_time': '2021-01-05T00:00:00-05:00',
                'pull_data_dictionaries': False
            }
        },
        'eols_extract_records': {
            'config': {
                'end_time': '2021-10-05T00:00:00-05:00',
                'start_time': '2021-01-05T00:00:00-05:00',
                'pull_data_dictionaries': False
            }
        },
        'write_outfiles_in_terra_format': {
            'config': {
                'output_dir': 'gs://fake_dir'
            }
        },
        'write_outfiles_in_tsv_format': {
            'config': {
                'output_dir': 'gs://fake_tsv_dir'
            }
        },
        'copy_outfiles_to_terra': {
            'config': {
                'destination_gcs_path': 'gs://fake_dir'
            }
        }

    }


@pytest.fixture
def sample_run_config():
    return {
        'sample_extract_records': {
            'config': {
                'end_time': '2021-10-05T00:00:00-05:00',
                'start_time': '2021-01-05T00:00:00-05:00',
                'pull_data_dictionaries': False
            }
        },
        'write_outfiles_in_terra_format': {
            'config': {
                'output_dir': 'gs://fake_dir'
            }
        },
        'write_outfiles_in_tsv_format': {
            'config': {
                'output_dir': 'gs://fake_tsv_dir'
            }
        },
        'copy_outfiles_to_terra': {
            'config': {
                'destination_gcs_path': 'gs://fake_dir'
            }
        }
    }


def test_refresh_data_all(run_config):
    result = refresh_data_all.execute_in_process(
        config=run_config,
        resources={
            "extract_beam_runner": ResourceDefinition.mock_resource(),
            "transform_beam_runner": ResourceDefinition.mock_resource(),
            "refresh_directory": ResourceDefinition.mock_resource(),
            "outfiles_writer": ResourceDefinition.mock_resource(),
            "api_token": ResourceDefinition.mock_resource(),
            "gcs": ResourceDefinition.mock_resource(),
            "slack_client": ResourceDefinition.mock_resource()
        }
    )
    assert result.success, "Pipeline run should be successful"


def test_weekly_sample_refresh():
    context = build_schedule_context(scheduled_execution_time=datetime(2020, 1, 1, 13, 30, 30, tzinfo=Eastern))
    run_config = weekly_data_refresh(context)
    assert run_config['solids']['sample_extract_records']['config']['end_time'] == "2020-01-01T13:30:30-05:00"
