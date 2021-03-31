#!/usr/bin/env python

import argparse
import csv
import json
import logging
from math import ceil
import os

from gcsfs.core import GCSFileSystem


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]

    return text


parser = argparse.ArgumentParser(description="Convert the records in a specified bucket prefix to a Terra-compatible TSV")
parser.add_argument('input_dir', metavar='I', help='The bucket prefix to read records from')
parser.add_argument('output_dir', metavar='O', help='The local directory to write the resulting TSVs to')
parser.add_argument('table', nargs='*', help="One or more tables to process into TSVs. Processes all tables if none specified.")
parser.add_argument('--firecloud', action='store_true', help="Use logic to generate primary keys for Terra upload via Firecloud")
parser.add_argument('--debug', action='store_true', help="Write additional logs for debugging")
args = parser.parse_args()

log_level = logging.DEBUG if args.debug else logging.INFO
logging.basicConfig(level=log_level)

log = logging.getLogger(__name__)

TERRA_COLUMN_LIMIT = 1000

table_names = args.table or ['cslb', 'hles_cancer_condition', 'hles_dog', 'hles_health_condition', 'hles_owner', 'environment']
PRIMARY_KEY_PREFIX = 'entity'

gcs = GCSFileSystem()

# Process the known (hardcoded) tables
for table_name in table_names:
    log.info(f"PROCESSING {table_name}")
    # set to hold all columns for this table, list to hold all the rows
    column_set = set()
    row_list = []
    # most tables should have "dog_id" as a key
    if table_name in {"hles_dog", "hles_cancer_condition", "hles_health_condition", "environment", "cslb"}:
        pk_name = 'dog_id'
    # owner table is linked to hles_dog via "owner_id"
    elif table_name == 'hles_owner':
        pk_name = 'owner_id'
    else:
        log.info(f"Unrecognized table: {table_name}")
        break
    # generated PK column headers for Firecloud compatibility
    if args.firecloud:
        entity_name = f"{PRIMARY_KEY_PREFIX}:{table_name}_id"
    # normal processing of IDs
    else:
        entity_name = pk_name

    # read json data
    for path in gcs.ls(os.path.join(args.input_dir, table_name)):
        log.info(f"...Opening {path}")

        with gcs.open(path, 'r') as json_file:
            print(path, json_file)
            for line in json_file:
                row = json.loads(line)

                if not row:
                    raise RuntimeError(f'Encountered invalid JSON "{line}", aborting.')

                # generate ids for Firecloud compatibility
                if args.firecloud:
                    # hles_health_condition: read + copy dog_id and hs_condition and hs_condition_is_congenital
                    # concatenate and write out as entity_name, keep original PK
                    if table_name == "hles_health_condition":
                        # grab the congenital flag and convert to int
                        congenital_flag = row.get('hs_condition_is_congenital')
                        try:
                            congenital_flag = int(congenital_flag)
                        except TypeError:
                            log.info(f"Error, 'hs_condition_is_congenital' is not populated in {table_name}")
                        row[entity_name] = '-'.join([str(component) for component in [row.get('dog_id'), row.get('hs_condition'), congenital_flag]])
                    # environment: read + copy dog_id and address fields to concatenate for generated uuid
                    elif table_name == "environment":
                        primary_key_fields = ['dog_id', 'address_1_or_2', 'address_month', 'address_year']
                        row[entity_name] = '-'.join([str(row.get(field)) for field in primary_key_fields])
                    # all other tables: store the original PK to the new column, removing the original column
                    else:
                        row[entity_name] = row.pop(pk_name)
                # normal processing of IDs - PK is stored as PK
                else:
                    row[entity_name] = row.pop(pk_name)

                column_set.update(row.keys())
                row_list.append(row)

    # make sure pk is the first column (Firecloud req.)
    # pop out the PK, will be splitting this set out later
    column_set.discard(entity_name)
    # logic to move the actual PK columns to beginning of file (where we have generated one)
    if args.firecloud and table_name in {"hles_health_condition", "environment"}:
        column_set.discard(pk_name)
        sorted_column_set = [entity_name] + [pk_name] + sorted(list(column_set))
    else:
        sorted_column_set = [entity_name] + sorted(list(column_set))

    # provide some stats
    col_count = len(sorted_column_set)
    log.info(f"...{table_name} contains {len(row_list)} rows and {col_count} columns")
    # output to tsv
    # 512 column max limit per request (upload to workspace)
    if col_count > TERRA_COLUMN_LIMIT:
        # calculate chunks needed - each table requires the PK
        total_col_count = ceil(col_count/TERRA_COLUMN_LIMIT)+col_count-1
        chunks = ceil(total_col_count/TERRA_COLUMN_LIMIT)
        log.info(f"...Splitting {table_name} into {chunks} files")
        log.info(f"...{len(column_set)} cols in init list")
        # FOR EACH SPLIT
        for chunk in range(1, chunks+1):
            # Incremented outfile name
            output_location = os.path.join(args.output_dir, f'{table_name}_{chunk}.tsv')
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
                log.debug(f"......adding {col} to split_column_set ({len(split_column_set)}) ...{len(column_set)} columns left")
            split_column_list = [entity_name] + sorted(list(split_column_set))
            log.info(f"......Split #{chunk} now contains {len(split_column_list)} columns")
            log.debug(f"cols in split: {len(split_column_list)}")
            log.debug(f"cols left to split: {len(column_set)}")

            def clean_up_string_whitespace(val):
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
            with open(output_location, 'w') as output_file:
                dw = csv.DictWriter(output_file, split_column_list, delimiter='\t')
                dw.writeheader()
                dw.writerows(split_row_dict_list)

            log.info(f"......{table_name} Split #{chunk} was successfully written to {output_location}")
    else:
        # this branch is executed for any tables with 512 cols or less
        log.info(f"...No need to split files for {table_name}")
        output_location = os.path.join(args.output_dir, f'{table_name}.tsv')
        log.info(f"...Writing {table_name} to {output_location}")
        with open(output_location, 'w') as output_file:
            dw = csv.DictWriter(output_file, sorted_column_set, delimiter='\t')
            dw.writeheader()
            dw.writerows(row_list)
        log.info(f"...{table_name} was successfully written to {output_location}")
