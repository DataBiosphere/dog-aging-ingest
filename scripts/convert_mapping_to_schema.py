"""
This tool takes in a csv of the mapping schema and creates json schema files
for the new tables, including Jade fragments.
The tool takes in two arguments:
table_name - Name of the general table and prefix used for the fragments.
mapping_csv - Path to the mapping schema file that the tool will parse.
"""

import argparse
from collections import defaultdict
import os
import csv
import json
from typing import NamedTuple, Any, Iterable


class JadeColumn(NamedTuple):
    column_name: str
    column_type: str


TDR_MAPPING_FILE_COLUMN_NAME_ID = "TDR_Column_Name"
TDR_MAPPING_FILE_JADE_FRAGMENT = "Jade_Fragment"
TDR_MAPPING_COLUMN_TYPE_ID = "TDR_Variable_Type"
TDR_FRAGMENT_GENERAL = "GENERAL"
TDR_RC_TO_COLUMN_TYPES = {
    "BOOL": "boolean",
    "INT": "integer",
    "STRING": "string"
}


def parse_csv(mapping_csv: str) -> dict[str, list[JadeColumn]]:
    fragments = defaultdict(list[JadeColumn])

    with open(mapping_csv) as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            raw_type_id = row[TDR_MAPPING_COLUMN_TYPE_ID]
            raw_column_name_id = row[TDR_MAPPING_FILE_COLUMN_NAME_ID]

            if not raw_type_id and not row[TDR_MAPPING_FILE_JADE_FRAGMENT]:
                continue

            if raw_type_id not in TDR_RC_TO_COLUMN_TYPES.keys():
                raise Exception(
                    f"Invalid column, field name = {row[TDR_MAPPING_FILE_COLUMN_NAME_ID]}, type = {row[TDR_MAPPING_COLUMN_TYPE_ID]}")

            jade_field = JadeColumn(
                column_name=raw_column_name_id,
                column_type=TDR_RC_TO_COLUMN_TYPES[raw_type_id]
            )

            fragment = TDR_FRAGMENT_GENERAL
            if row[TDR_MAPPING_FILE_JADE_FRAGMENT]:
                fragment = row[TDR_MAPPING_FILE_JADE_FRAGMENT]

            fragments[fragment].append(jade_field)

    return fragments


def render_schema_fragment(fragment: str, fields: list[JadeColumn], table_name: str) -> dict[str, Any]:
    name = f"{table_name}_{fragment}"
    if fragment == TDR_FRAGMENT_GENERAL:
        name = table_name

    columns = []
    for field in fields:
        columns.append({
            "name": field.column_name,
            "datatype": field.column_type
        })

    return {
        "name": name,
        "columns": columns
    }


def render_general_fragment(general_fragment, table_fragments: Iterable[str], table_name: str):
    dog_id_field = {
      "name": "dog_id",
      "datatype": "integer",
      "type": "primary_key",
      "links": [
        {
          "table_name": "hles_dog",
          "column_name": "dog_id"
        }
      ]
    }
    schema = render_schema_fragment(TDR_FRAGMENT_GENERAL, general_fragment, table_name)
    schema["columns"].insert(0,dog_id_field)
    schema["table_fragments"] = list(table_fragments)

    return schema


def render_schemas(fragments: dict[str, list[JadeColumn]], table_name: str, output_path: str):
    general_fragment = fragments.pop(TDR_FRAGMENT_GENERAL, {})
    for fragment, fields in fragments.items():
        schema = render_schema_fragment(fragment, fields, table_name)
        rendered_json = json.dumps(schema, indent=4)
        with open(f"{output_path}/{table_name}_{fragment}.fragment.json", "w") as f:
            f.write(rendered_json)

    general_schema = render_general_fragment(general_fragment, [f"{table_name}_{key}" for key in fragments.keys()], table_name)
    with open(f"{output_path}/{table_name}.table.json", "w") as f:
        f.write(json.dumps(general_schema, indent=4))


def render_jade_table_schemas(table_name: str, mapping_csv: str, output_path: str) -> None:
    fragments = parse_csv(mapping_csv)
    render_schemas(fragments, table_name, output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table_name", required=True)
    parser.add_argument("-f", "--mapping_csv", required=True)
    parser.add_argument("-o", "--output_path", required=True)
    args = parser.parse_args()

    render_jade_table_schemas(args.table_name, args.mapping_csv, args.output_path)
