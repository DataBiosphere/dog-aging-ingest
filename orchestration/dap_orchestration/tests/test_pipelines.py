from datetime import datetime

from dagster import execute_pipeline, build_schedule_context, validate_run_config
import pytest

from dap_orchestration.pipelines import refresh_data_all

from dap_orchestration.repositories.prod_repositories import weekly_sample_refresh
from pytz.reference import Eastern


@pytest.fixture
def run_config():
    return {
        'resources': {
            'refresh_directory': {
                'config': {
                    'refresh_directory': 'fake_dir'
                }
            },
            'api_token': {
                'config': {
                    'base_api_token': 'fake_api_token1',
                    'env_api_token': 'fake_api_token1',
                }
            }
        },
        'solids': {
            'cslb_extract_records': {
                'config':
                    {
                        'end_time': 'fake_time',
                        'pull_data_dictionaries': True
                    }
            },
            'env_extract_records': {
                'config': {
                    'end_time': 'fake_time',
                    'pull_data_dictionaries': True
                }
            },
            'hles_extract_records': {
                'config': {
                    'end_time': 'fake_time',
                    'pull_data_dictionaries': True
                }
            },
            'sample_extract_records': {
                'config': {
                    'end_time': 'fake_time',
                    'pull_data_dictionaries': True
                }
            },
            'eols_extract_records': {
                'config': {
                    'end_time': 'fake_time',
                    'pull_data_dictionaries': True
                }
            },
            'write_outfiles': {
                'config': {
                    'output_dir': 'fake_dir'
                }
            }
        }
    }


def test_pipeline(run_config):
    result = execute_pipeline(refresh_data_all, mode="test", run_config=run_config)
    assert result.success, "Pipeline run should be successful"


def test_weekly_sample_refresh():
    context = build_schedule_context(scheduled_execution_time=datetime(2020, 1, 1, 13, 30, 30, tzinfo=Eastern))
    run_config = weekly_sample_refresh(context)
    assert run_config['solids']['sample_extract_records']['config']['end_time'] == "2020-01-01T13:30:30-05:00"
