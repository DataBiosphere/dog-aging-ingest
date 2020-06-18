import json
import csv
from os import listdir
import sys

if (len(sys.argv) < 3):
    print "Please provide the input directory and output directory as arguments!"

#table_names = ["hles_dog", "hles_owner"]
table_names = ["hles_owner"]
schema_dir = '../schema/src/main/jade-tables/'
input_dir = sys.argv[1]
output_dir = sys.argv[2]

for table_name in table_names:
    # get set of all keys/column names from the schema file
    column_list = []
    with open(schema_dir + table_name + '.table.json') as schema_file:
        schema = json.load(schema_file)
        for column in schema['columns']:
            column_list.append(column['name'])

    # get the set of files in the directory
    full_input_directory = input_dir + table_name + '/'
    input_files = listdir(full_input_directory)

    # read json data
    obj_list = []
    for json_file_name in input_files:
        with open(full_input_directory + json_file_name, 'r') as json_file:
            for jsonObj in json_file:
                obj = json.loads(jsonObj)
                obj_list.append(obj)

    # output to tsv
    output_location = output_dir + '/' + table_name[5:] + '.tsv'
    with open(output_location, 'w') as output_file:
        dw = csv.DictWriter(output_file, column_list, delimiter='\t')
        dw.writeheader()
        dw.writerows(obj_list)

# Note: Before uploading to Terra, manually go in and update PK column name to be prefixed by "entity:"