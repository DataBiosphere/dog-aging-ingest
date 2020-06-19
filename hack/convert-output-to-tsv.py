import json
import csv
from os import listdir
import sys

if (len(sys.argv) < 3):
    print "Please provide the input directory and output directory as arguments!"

input_dir = sys.argv[1]
output_dir = sys.argv[2]
table_names = listdir(input_dir) 
pk_prefix = 'entity:'

for table_name in table_names:
    # get the set of files in the directory
    full_input_directory = input_dir + table_name + '/'
    input_files = listdir(full_input_directory)
    output_name = table_name[5:] # remove 'hles_' from the file name

    column_set = set()
    row_list = []
    # read json data
    for json_file_name in input_files:
        with open(full_input_directory + json_file_name, 'r') as json_file:
            for jsonObj in json_file:
                row = json.loads(jsonObj)
                # rename primary key column to the format 'entity:{entity name}_id'
                pk_name = output_name + '_id'
                row[pk_prefix + pk_name] = row.pop(pk_name)
                # store data
                column_set.update(row.keys())
                row_list.append(row)

    # output to tsv
    output_location = output_dir + '/' + output_name + '.tsv'
    with open(output_location, 'w') as output_file:
        dw = csv.DictWriter(output_file, sorted(list(column_set)), delimiter='\t')
        dw.writeheader()
        dw.writerows(row_list)
