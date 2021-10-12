from dagster import graph

from dap_orchestration.solids import hles_extract_records, cslb_extract_records, env_extract_records, \
    sample_extract_records, eols_extract_records, hles_transform_records, cslb_transform_records, \
    env_transform_records, write_outfiles_in_terra_format, write_outfiles_in_tsv_format, sample_transform_records, eols_transform_records, copy_outfiles_to_terra, \
    send_pipeline_finish_notification, send_pipeline_start_notification


@graph
def refresh_data_all() -> None:
    notification_result = send_pipeline_start_notification()
    collected_outputs = [
        hles_transform_records(hles_extract_records(notification_result)),
        cslb_transform_records(cslb_extract_records(notification_result)),
        env_transform_records(env_extract_records(notification_result)),
        sample_transform_records(sample_extract_records(notification_result)),
        eols_transform_records(eols_extract_records(notification_result))
    ]
    write_outfiles_result = write_outfiles_in_tsv_format(collected_outputs)
    copy_outfiles_result = copy_outfiles_to_terra(write_outfiles_in_terra_format(collected_outputs))
    send_pipeline_finish_notification(write_outfiles_result, copy_outfiles_result)
