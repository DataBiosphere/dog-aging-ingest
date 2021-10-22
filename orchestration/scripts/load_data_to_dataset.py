"""
Utility that will load data from a DAP refresh GS path to a TDR dataset. This script will remove any existing rows
in the dataset and upload the data to simulate an upsert semantic. Additionally, this script is capable of creating
a snapshot from a dataset.
"""

import argparse
import sys
from datetime import datetime

from dagster_utils.contrib.data_repo.jobs import poll_job, JobFailureException
from dagster_utils.contrib.google import default_google_access_token
from data_repo_client import ApiClient, Configuration, RepositoryApi, DatasetModel, DataDeletionRequest, \
    SnapshotRequestModel, SnapshotRequestContentsModel, PolicyMemberRequest
from google.cloud.bigquery import Client

TABLES = {
    "cslb",
    "environment",
    "eols",
    "hles_cancer_condition",
    "hles_dog",
    "hles_health_condition",
    "hles_owner",
    "sample"
}


def _get_api_client() -> RepositoryApi:
    # create API client
    config = Configuration(host="https://data.terra.bio")
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return RepositoryApi(api_client=client)


def _remove_outdated_rows(dataset: DatasetModel, table_name: str,
                          extraction_path: str, data_repo_client: RepositoryApi):
    out_path = f"{extraction_path}/{table_name}/*"
    client = Client()
    cnt_query = f"""
    SELECT COUNT(*) as cnt FROM `{dataset.data_project}.datarepo_{dataset.name}.{table_name}`
    """
    cnt_result = client.query(cnt_query).result()
    if not any([row['cnt'] > 0 for row in cnt_result]):
        print(f"No rows to soft delete for {table_name}")
        return

    query = f"""
     EXPORT DATA OPTIONS(
        uri='{out_path}',
        format='CSV',
        overwrite=true
    ) AS
    SELECT datarepo_row_id FROM `{dataset.data_project}.datarepo_{dataset.name}.{table_name}`
    """

    print(f"Pulling rows for {table_name} to {out_path}")
    client.query(query).result()

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


def _load_new_rows(
    dataset: DatasetModel,
    table_name: str,
    load_path: str, load_tag: str,
    data_repo_client: RepositoryApi
):
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


def _create_snapshot(dataset: DatasetModel, load_tag: str, data_repo_client: RepositoryApi) -> str:
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

    return snapshot_id


def _validate_gs_path(path: str):
    if not path.startswith("gs://"):
        print(f"Path must be a gs:// path: {path}")
        sys.exit(1)

    if path.endswith("/"):
        print(f"Path must not end with '/': {path}")
        sys.exit(1)


def load(args: argparse.Namespace):
    data_load_path = args.data_load_path
    temp_storage_path = args.temp_storage_path
    dataset_id = args.dataset_id

    _validate_gs_path(data_load_path)
    _validate_gs_path(temp_storage_path)

    data_repo_client = _get_api_client()
    load_tag = datetime.now().strftime(f"dap_%Y%m%d_%H%M%S")
    dataset: DatasetModel = data_repo_client.retrieve_dataset(id=dataset_id)

    for table in TABLES:
        soft_deletes_path = f"{temp_storage_path}/{load_tag}/soft_deletes"
        _remove_outdated_rows(dataset, table, soft_deletes_path, data_repo_client)
        _load_new_rows(dataset, table, f"{data_load_path}/transform", load_tag, data_repo_client)


def snapshot(args):
    data_repo_client = _get_api_client()
    dataset_id = args.dataset_id
    load_tag = datetime.now().strftime(f"dap_%Y%m%d_%H%M%S")
    dataset: DatasetModel = data_repo_client.retrieve_dataset(id=dataset_id)

    _create_snapshot(dataset, load_tag, data_repo_client)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    load_args = subparsers.add_parser("load")
    load_args.add_argument("-g", "--data-load-path", required=True)
    load_args.add_argument("-d", "--dataset-id", required=True)
    load_args.add_argument("-t", "--temp-storage-path", required=True)
    load_args.set_defaults(func=load)

    snapshot_args = subparsers.add_parser("snapshot")
    snapshot_args.add_argument("-d", "--dataset-id", required=True)
    snapshot_args.set_defaults(func=snapshot)

    args = parser.parse_args()
    args.func(args)
