import argparse
import sys
from datetime import datetime

from google.cloud.bigquery import Client

from dagster_utils.contrib.data_repo.jobs import poll_job, JobFailureException
from dagster_utils.contrib.google import default_google_access_token
from data_repo_client import ApiClient, Configuration, RepositoryApi, DatasetModel, DataDeletionRequest, ApiException, \
    SnapshotRequestModel, SnapshotRequestContentsModel, PolicyMemberRequest

TABLES = {"cslb", "environment", "eols", "hles_cancer_condition", "hles_dog", "hles_health_condition", "hles_owner", "sample"}

def _build_base_api_client() -> ApiClient:
    # create API client
    config = Configuration(host="https://data.terra.bio")
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return client


def _get_api_client() -> RepositoryApi:
    return RepositoryApi(api_client=_build_base_api_client())


def _find_outdated_rows(dataset: DatasetModel, table_name: str, extraction_path: str, data_repo_client: RepositoryApi):
    out_path = f"{extraction_path}/{table_name}/*"
    client = Client()
    query = f"""
     EXPORT DATA OPTIONS(
        uri='{out_path}',
        format='CSV',
        overwrite=true
    ) AS
    SELECT datarepo_row_id FROM `{dataset.data_project}.datarepo_{dataset.name}.{table_name}`
    """

    print(f"Pulling rows for {table_name} to {out_path}")
    result = client.query(query).result()
    if result.total_rows == 0:
        print(f"No rows to soft delete for {table_name}")
        return

    print(f"Submitting soft delete for {table_name}")
    response = data_repo_client.apply_dataset_data_deletion(
        id=dataset.id,
        data_deletion_request=DataDeletionRequest(
            delete_type="soft",
            spec_type="gcsFile",
            tables=[
                {
                    "gcsFileSpec": {
                        "fileType": "csv",
                        "path": out_path
                    },
                    "tableName": table_name
                }
            ]
        )
    )

    job_id = response.id
    print(f"Polling on job id {job_id}")
    try:
        poll_job(job_id, 300, 2, data_repo_client)
    except JobFailureException as e:
        response = data_repo_client.retrieve_job_result(job_id)
        print(response)
        sys.exit(1)

def _load_new_rows(dataset: DatasetModel, table_name: str, load_path: str, load_tag: str, data_repo_client: RepositoryApi):
    payload = {
        "format": "json",
        "ignore_unknown_values": False,
        "load_tag": load_tag,
        "max_bad_records": 0,
        "path": f"{load_path}/{table_name}/*",
        "profile_id": dataset.default_profile_id,
        "resolve_existing_files": True,
        "table": table_name
    }

    print(f"Loading {table_name} data...")
    response = data_repo_client.ingest_dataset(id=dataset.id, ingest=payload)

    print(f"Job submitted, id = {response.id}")
    poll_job(response.id, 300, 2, data_repo_client)


def _snapshot_data(dataset: DatasetModel, load_tag: str, data_repo_client: RepositoryApi):
    snapshot_request = SnapshotRequestModel(
        name=load_tag,
        profile_id=dataset.default_profile_id,
        description=f"Create snapshot {load_tag}",
        contents=[SnapshotRequestContentsModel(dataset_name=dataset.name, mode="byFullView")],
        readers=["monster@firecloud.org"]
    )

    response = data_repo_client.create_snapshot(
        snapshot=snapshot_request
    )

    job_id = response.id
    print(f"Snapshot creation job id: {job_id}")
    poll_job(job_id, 300, 2, data_repo_client)

    result = data_repo_client.retrieve_job_result(id=job_id)
    snapshot_id = result['id']
    print(f"Snapshot {snapshot_id} created, adding stewards")
    payload = PolicyMemberRequest(email="monster@firecloud.org")

    data_repo_client.add_snapshot_policy_member(id=snapshot_id, policy_name="steward", policy_member=payload)
    print("Snapshot complete")


def run(gs_path: str, dataset_id: str):
    data_repo_client = _get_api_client()
    load_tag = datetime.now().strftime(f"dap_%Y%m%d_%H%M%S")
    dataset: DatasetModel = data_repo_client.retrieve_dataset(id=dataset_id)

    for table in TABLES:
        _find_outdated_rows(dataset, table, f"gs://broad-dsp-monster-dap-prod-temp-storage/tdr_loads/{load_tag}/soft_deletes", data_repo_client)
        _load_new_rows(dataset, table, f"{gs_path}/transform", load_tag, data_repo_client)

    _snapshot_data(dataset, load_tag, data_repo_client)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--gs-path", required=True)
    parser.add_argument("-d", "--dataset-id", required=True)

    args = parser.parse_args()
    run(args.gs_path, args.dataset_id)
