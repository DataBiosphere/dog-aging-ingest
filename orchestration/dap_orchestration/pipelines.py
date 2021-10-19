from dagster import graph, solid

from dap_orchestration.solids import hles_extract_records, cslb_extract_records, env_extract_records, \
    sample_extract_records, eols_extract_records, hles_transform_records, cslb_transform_records, \
    env_transform_records, write_outfiles_in_terra_format, write_outfiles_in_tsv_format, sample_transform_records, \
    eols_transform_records, copy_outfiles_to_terra


@solid
def noop():
    pass


@graph
def refresh_data_all() -> None:
    collected_outputs = [
        hles_transform_records(hles_extract_records()),
        cslb_transform_records(cslb_extract_records()),
        env_transform_records(env_extract_records()),
        sample_transform_records(sample_extract_records()),
        eols_transform_records(eols_extract_records())
    ]
    write_outfiles_in_tsv_format(collected_outputs)
    copy_outfiles_to_terra(write_outfiles_in_terra_format(collected_outputs))
