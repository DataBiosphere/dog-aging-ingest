from dagster import graph

from dap_orchestration.solids import hles_extract_records, cslb_extract_records, env_extract_records, \
    sample_extract_records, eols_extract_records, hles_transform_records, cslb_transform_records, \
    env_transform_records, write_outfiles, sample_transform_records, eols_transform_records, upload_to_gcs


@graph
def refresh_data_all() -> None:
    collected_outputs = [
        hles_transform_records(hles_extract_records()),
        cslb_transform_records(cslb_extract_records()),
        env_transform_records(env_extract_records()),
        sample_transform_records(sample_extract_records()),
        eols_transform_records(eols_extract_records())
    ]
    upload_to_gcs(write_outfiles(collected_outputs))


@graph
def refresh_data_sample() -> None:
    collected_outputs = [
        sample_transform_records(sample_extract_records()),
    ]
    upload_to_gcs(write_outfiles(collected_outputs))

