from dagster import graph

from dap_orchestration.solids import hles_extract_records, cslb_extract_records, env_extract_records, \
    sample_extract_records, eols_extract_records, hles_transform_records, cslb_transform_records, \
    env_transform_records, write_outfiles_in_terra_format, write_outfiles_in_tsv_format, sample_transform_records, eols_transform_records, copy_outfiles_to_terra, \
    send_pipeline_finish_notification, send_pipeline_start_notification


@graph
def refresh_data_all() -> None:
    send_pipeline_start_notification()
    collected_outputs = [
        hles_transform_records(hles_extract_records()),
        cslb_transform_records(cslb_extract_records()),
        env_transform_records(env_extract_records()),
        sample_transform_records(sample_extract_records()),
        eols_transform_records(eols_extract_records())
    ]
    result1 = write_outfiles_in_tsv_format(collected_outputs)
    result2 = copy_outfiles_to_terra(write_outfiles_in_terra_format(collected_outputs))
    send_pipeline_finish_notification(result1, result2)
