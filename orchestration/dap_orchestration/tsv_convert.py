#!/usr/bin/env python

import argparse
import csv
import io
import json
import logging
import os
from dataclasses import dataclass, field
from math import ceil
from typing import Optional, Union, Any
from dap_orchestration.types import DapSurveyType

from gcsfs.core import GCSFileSystem

log = logging.getLogger(__name__)

PRIMARY_KEY_PREFIX = 'entity'
TERRA_COLUMN_LIMIT = 1000

TABLES_BY_SURVEY = {
    "hles": ['hles_cancer_condition', 'hles_dog', 'hles_health_condition',
             'hles_owner'],
    "cslb": ["cslb"],
    "environment": ["environment"],
    "sample": ["sample"],
    "eols": ["eols"]
}

DEFAULT_TABLE_NAMES = [table_name for table_names in TABLES_BY_SURVEY.values() for table_name in table_names]


def remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        return text[len(prefix):]

    return text


# create a service object to handle all aspects of generating a primary key
@dataclass
class PrimaryKeyGenerator:
    table_name: str
    pk_name: str = field(init=False)
    firecloud: bool

    # this will calculate pk_name during init
    def __post_init__(self) -> None:
        # most tables should have "dog_id" as a key
        if self.table_name in {"hles_dog", "hles_cancer_condition", "hles_health_condition", "environment", "cslb",
                               "eols"}:
            self.pk_name = 'dog_id'
        # owner table is linked to hles_dog via "owner_id"
        elif self.table_name == 'hles_owner':
            self.pk_name = 'owner_id'
        elif self.table_name == 'sample':
            self.pk_name = 'sample_id'
        else:
            raise ValueError(f"Unrecognized table: {self.table_name}")

    def generate_entity_name(self) -> str:
        if self.firecloud:
            return f"{PRIMARY_KEY_PREFIX}:{self.table_name}_id"
        return self.pk_name

    def generate_primary_key(self, row: dict[str, str]) -> str:
        # normal processing of IDs - the original primary key is returned
        if not self.firecloud:
            return row.pop(self.pk_name)
        # generate ids for Firecloud compatibility
        # hles_health_condition: read + copy dog_id and hs_condition and hs_condition_is_congenital
        # concatenate and write out as entity_name, keep original PK
        if self.table_name == "hles_health_condition":
            # code to generate the value of primary key should be pulled out
            # grab the congenital flag and convert to int
            congenital_flag = row.get('hs_condition_is_congenital')
            try:
                congenital_flag = int(congenital_flag)  # type: ignore
            except TypeError:
                log.warning(f"Error, 'hs_condition_is_congenital' is not populated in {self.table_name}")
            return '-'.join(
                [str(component) for component in [row.get('dog_id'), row.get('hs_condition_type'), row.get('hs_condition'), congenital_flag]])
        # environment: read + copy dog_id and address fields to concatenate for generated uuid
        elif self.table_name == "environment":
            primary_key_fields = ['dog_id', 'address_1_or_2', 'address_month', 'address_year']
            return '-'.join([str(row.get(field)) for field in primary_key_fields])
        # cslb: read + copy dog_id and cslb_date to concatenate for generated uuid
        elif self.table_name == "cslb":
            primary_key_fields = ['dog_id', 'cslb_date']
            return '-'.join([str(row.get(field)) for field in primary_key_fields])
        # all other tables: return the original PK to be duplicated to the new column
        else:
            return str(row.get(self.pk_name))


def _open_output_location(output_location: str, gcs: GCSFileSystem) -> Union[Any, io.TextIOWrapper]:
    if output_location.startswith("gs://"):
        return gcs.open(output_location, 'w')

    target_dir = os.path.dirname(output_location)
    if not os.path.isdir(target_dir):
        os.mkdir(target_dir)
    return open(output_location, 'w')


def convert_to_tsv(input_dir: str, output_dir: str, firecloud: bool,
                   survey_types: Optional[list[DapSurveyType]] = None) -> None:
    # Process the known (hardcoded) tables
    if survey_types is None:
        table_names = DEFAULT_TABLE_NAMES
    else:
        table_names = []
        for survey_type in survey_types:
            table_names = table_names + TABLES_BY_SURVEY[survey_type]

    for table_name in table_names:
        log.info(f"PROCESSING {table_name}")
        primary_key_gen = PrimaryKeyGenerator(table_name, firecloud)
        # set to hold all columns for this table, list to hold all the rows
        column_set = set()
        row_list = []
        # generated PK column headers for Firecloud compatibility
        entity_name = primary_key_gen.generate_entity_name()

        # read json data
        gcs = GCSFileSystem()
        for path in gcs.ls(os.path.join(input_dir, table_name)):
            log.info(f"...Opening {path}")

            with gcs.open(path, 'r') as json_file:
                print(path, json_file)
                for line in json_file:
                    row = json.loads(line)

                    if not row:
                        raise RuntimeError(f'Encountered invalid JSON "{line}", aborting.')

                    row[entity_name] = primary_key_gen.generate_primary_key(row)

                    column_set.update(row.keys())
                    row_list.append(row)

        # make sure pk is the first column (Firecloud req.)
        # pop out the PK, will be splitting this set out later
        column_set.discard(entity_name)
        # logic to move the actual PK columns to beginning of file (where we have generated one)
        # sample is excluded here because sample_id is the primary key and is distinct from dog_id
        if firecloud and table_name not in {"sample"}:
            column_set.discard(primary_key_gen.pk_name)
            sorted_column_set = [entity_name] + [primary_key_gen.pk_name] + sorted(list(column_set))
        else:
            sorted_column_set = [entity_name] + sorted(list(column_set))

        # provide some stats
        col_count = len(sorted_column_set)
        log.info(f"...{table_name} contains {len(row_list)} rows and {col_count} columns")
        # output to tsv
        # 512 column max limit per request (upload to workspace)
        if col_count > TERRA_COLUMN_LIMIT:
            # calculate chunks needed - each table requires the PK
            total_col_count = ceil(col_count / TERRA_COLUMN_LIMIT) + col_count - 1
            chunks = ceil(total_col_count / TERRA_COLUMN_LIMIT)
            log.info(f"...Splitting {table_name} into {chunks} files")
            log.info(f"...{len(column_set)} cols in init list")
            # FOR EACH SPLIT
            for chunk in range(1, chunks + 1):
                # Incremented outfile name
                output_location = os.path.join(output_dir, f'{table_name}_{chunk}.tsv')
                log.info(f"...Processing Split #{chunk} to {output_location}")
                split_column_set = set()
                # add 511 columns
                for _ in range(TERRA_COLUMN_LIMIT - 1):
                    try:
                        col = column_set.pop()
                    except KeyError:
                        # KeyError is raised when we're out of columns to pop
                        break
                    split_column_set.add(col)
                    log.debug(
                        f"......adding {col} to split_column_set ({len(split_column_set)}) ...{len(column_set)} columns left")
                split_column_list = [entity_name] + sorted(list(split_column_set))
                log.info(f"......Split #{chunk} now contains {len(split_column_list)} columns")
                log.debug(f"cols in split: {len(split_column_list)}")
                log.debug(f"cols left to split: {len(column_set)}")

                def clean_up_string_whitespace(val: str) -> str:
                    try:
                        return val.replace('\r\n', ' ').strip()
                    except AttributeError:
                        return val

                # iterate through every row slicing out the values for the columns in this split
                split_row_dict_list = [
                    {
                        col: clean_up_string_whitespace(row[col])
                        for col in split_column_list
                        if col in row
                    }
                    for row in row_list
                ]

                # output to tsv
                with _open_output_location(output_location, gcs) as output_file:
                    dw = csv.DictWriter(output_file, split_column_list, delimiter='\t')
                    dw.writeheader()
                    dw.writerows(split_row_dict_list)

                log.info(f"......{table_name} Split #{chunk} was successfully written to {output_location}")
        else:
            # this branch is executed for any tables with 512 cols or less
            log.info(f"...No need to split files for {table_name}")
            output_location = os.path.join(output_dir, f'{table_name}.tsv')

            with _open_output_location(output_location, gcs) as output_file:
                log.info(f"...Writing {table_name} to  path {output_location}")
                dw = csv.DictWriter(output_file, sorted_column_set, delimiter='\t')
                dw.writeheader()
                dw.writerows(row_list)

            log.info(f"...{table_name} was successfully written to {output_location}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Convert the records in a specified bucket prefix to a Terra-compatible TSV")
    parser.add_argument('input_dir', metavar='I', help='The bucket prefix to read records from')
    parser.add_argument('output_dir', metavar='O', help='The local directory to write the resulting TSVs to')
    parser.add_argument('survey_types', nargs='*',
                        help="One or more survey types to process into TSVs. Processes all tables if none specified.")
    parser.add_argument('--firecloud', action='store_true',
                        help="Use logic to generate primary keys for Terra upload via Firecloud")
    parser.add_argument('--debug', action='store_true', help="Write additional logs for debugging")

    parsed = parser.parse_args()

    log_level = logging.DEBUG if parsed.debug else logging.INFO
    logging.basicConfig(level=log_level)

    convert_to_tsv(parsed.input_dir, parsed.output_dir, parsed.firecloud, parsed.survey_types)
