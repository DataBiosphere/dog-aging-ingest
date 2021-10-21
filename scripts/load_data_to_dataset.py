import argparse
from datetime import datetime

from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.contrib.google import default_google_access_token
from data_repo_client import ApiClient, Configuration, RepositoryApi, DatasetModel

TABLES = {"cslb", "environment", "eols", "hles_cancer_condition", "hles_dog", "hles_health_condition", "hles_owner", "sample"}

def _build_base_api_client() -> ApiClient:
    # create API client
    config = Configuration(host="https://data.terra.bio")
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return client


def get_api_client() -> RepositoryApi:
    return RepositoryApi(api_client=_build_base_api_client())


def run(gs_path: str, dataset_id: str):
    data_repo_client = get_api_client()
    load_tag = datetime.now().strftime(f"dap_%Y%m%d_%H%M%S")
    dataset: DatasetModel = data_repo_client.retrieve_dataset(id=dataset_id)

    for table in TABLES:
        payload = {
            "format": "json",
            "ignore_unknown_values": False,
            "load_tag": load_tag,
            "max_bad_records": 0,
            "path": f"{gs_path}/transform/{table}/*",
            "profile_id": dataset.default_profile_id,
            "resolve_existing_files": True,
            "table": table
        }

        print(f"Loading {table} data...")
        response = data_repo_client.ingest_dataset(id=dataset_id, ingest=payload)

        print(f"Job submitted, id = {response.id}")
        poll_job(response.id, 300, 2, data_repo_client)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--gs-path", required=True)
    parser.add_argument("-d", "--dataset-id", required=True)

    args = parser.parse_args()
    run(args.gs_path, args.dataset_id)
