from dagster import execute_pipeline
import pytest

from dap_orchestration.pipelines import refresh_data_all


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
