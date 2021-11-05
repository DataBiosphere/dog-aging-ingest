from dagster import solid, String, Failure
from dagster_utils.contrib.google import parse_gs_path
from more_itertools import chunked
from firecloud import api as fapi

LINES_PER_CHUNK = 2000


@solid(
    required_resource_keys={"gcs"},
    config_schema={
        "tsv_path": String,
        "destination_workspace": String,
        "destination_project": String
    })
def upload_to_firecloud(context):
    gcs = context.resources.gcs
    tsv_path = context.solid_config["tsv_path"]
    destination_project = context.solid_config["destination_project"]
    destination_workspace = context.solid_config["destination_workspace"]

    bucket_with_prefix = parse_gs_path(tsv_path)

    blobs = gcs.list_blobs(bucket_with_prefix.bucket, prefix=f"{bucket_with_prefix.prefix}/")
    for blob in blobs:
        if not blob.name.endswith("tsv"):
            context.log.info(f"Ignoring non-tsv file {blob.name}")
            continue

        context.log.info(f"Uploading {blob.name} to project {destination_project}, workspace = {destination_workspace}")

        raw = blob.download_as_text()
        lines = raw.split("\n")
        header = lines.pop(0)
        chunks = chunked(lines, LINES_PER_CHUNK)

        for cnt, chunk in enumerate(chunks):
            context.log.info(f"{blob.name} uploading chunk {cnt}")
            chunk.insert(0, header)
            tsv_string = "\n".join(chunk)

            response = fapi.upload_entities(destination_project, destination_workspace, tsv_string, model='flexible')
            if response.status_code != 200:
                raise Failure(f"Failed to upload, status_code = {response.status_code}, text = {response.text}")
