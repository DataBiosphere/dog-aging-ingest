import json
import csv
from os import listdir
import sys
from math import ceil

# CLI args
if (len(sys.argv) < 3):
    printd("Please provide the input directory and output directory as arguments!")
input_dir = sys.argv[1]
output_dir = sys.argv[2]
# optional debug arg, pass any string as the third arg
try:
    debug = sys.argv[3]
except IndexError:
    debug = None

table_names = ['cslb', 'hles_cancer_condition', 'hles_dog', 'hles_health_condition', 'hles_owner', 'environment']
pk_prefix = 'entity:'

# debug printer
def printd(x):
    if debug:
        print("[DEBUG] "+x)
    else:
        pass

# Process the known (hardcoded) tables
for table_name in table_names:
    print (f"\nPROCESSING {table_name}")
    # get the set of files in the directory
    full_input_directory = input_dir + '/' + table_name
    # ingore hidden files and sort files alphabetically
    try:
        input_files = sorted([f for f in listdir(full_input_directory) 
                             if not f.startswith('.')])
        print ("Input files: "+' '.join(input_files))
        # set to hold all columns for this table, list to hold all the rows
        column_set = set()
        row_list = []
        # read json data    
        for json_file_name in input_files:
            print(f"...Opening {full_input_directory+'/'+json_file_name}")
            with open(full_input_directory + '/' + json_file_name, 'r') as json_file:
                row_count = 0
                for jsonObj in json_file:
                    row_count += 1
                    row = json.loads(jsonObj)
                    # rename primary key column to the format 'entity:{entity name}_id'
                    # pop existing primary keys out and push them back in
                    if table_name in {"hles_owner", "hles_dog"}:
                        # remove 'hles_' from the table_name
                        pk_name = table_name[5:] + '_id'
                        entity_name = pk_prefix + pk_name
                        row[entity_name] = row.pop(pk_name)
                        # store data
                        column_set.update(row.keys())
                        row_list.append(row)
                    # cslb
                    elif table_name == 'cslb':
                        entity_name = pk_prefix + table_name + '_id'
                        row[entity_name] = row.get('dog_id')
                        column_set.update(row.keys())
                        row_list.append(row)
                    # hles_cancer_condition: read dog_id, copy and write out as pk_name
                    elif table_name == "hles_cancer_condition":
                        entity_name = pk_prefix + table_name + '_id'
                        # copy the dog_id
                        row[entity_name] = row.get('dog_id')
                        # store data
                        column_set.update(row.keys())
                        row_list.append(row)
                    # hles_health_condition: read + copy dog_id and hs_condition and hs_condition_is_congenital
                    # concatenate and write out as pk_name
                    elif table_name == "hles_health_condition":
                        entity_name = pk_prefix + table_name + '_id'
                        # grab the congenital flag and convert to int
                        congenital_flag = row.get('hs_condition_is_congenital')
                        if congenital_flag == True:
                            congenital_flag = 1
                        elif congenital_flag == False:
                            congenital_flag = 0
                        else:
                            print (f"Error, 'hs_condition_is_congenital' is not populated in {table_name}")
                        row[entity_name] = ('%s-%s-%s' % 
                            (row.get('dog_id'), row.get('hs_condition'), congenital_flag))
                        # store data
                        column_set.update(row.keys())
                        row_list.append(row)
                    # environment:
                    elif table_name == "environment":
                        entity_name = pk_prefix + table_name + '_id'
                        # env records are unique on dog_id, redcap_event
                        # setting the primary key
                        dogId = row.get('dog_id')
                        eventName = row.get('address_month_year')
                        row[entity_name] = ('%s-%s' %
                            (dogId, eventName))
                        column_set.update(row.keys())
                        row_list.append(row)
                    else:
                        print(f"Unrecognized table: {table_name}")

            # make sure pk is the first column
            # pop out the PK, will be splitting this set out later
            column_set.remove(entity_name)
            sorted_column_set = sorted(list(column_set))
            sorted_column_set.insert(0, entity_name)

            # provide some stats
            col_count = len(sorted_column_set)
            print(f"...{table_name} contains {row_count} rows and {col_count} columns")
            # output to tsv
            # 512 column max limit per request (upload to workspace) 
            if (col_count > 512):
                # calculate chunks needed - each table requires the PK
                total_col_count = ceil(col_count/512)+col_count-1
                chunks = ceil(total_col_count/512)
                print(f"...Splitting {table_name} into {chunks} files")
                print(f"...{len(column_set)} cols in init list")
                # FOR EACH SPLIT
                for chunk in range(1, chunks+1):
                    # Incremented outfile name
                    output_location = output_dir + '/' + table_name+'_%s' % chunk + '.tsv'
                    print(f"...Processing Split #{chunk} to {output_location}")
                    with open(output_location, 'w') as output_file:
                        split_column_set = set()
                        # add 511 columns
                        col_counter = 0
                        for col in column_set:
                            if (col_counter < 511 and len(column_set) > 0):
                                # add column to split
                                split_column_set.add(col)
                                col_counter = len(split_column_set)
                                printd(f"......adding {col} to split_column_set ({col_counter}) ...{len(column_set)} columns left")
                        # remove the split_column_set from column_set
                        column_set = [x for x in column_set 
                                     if x not in split_column_set]
                        split_column_list = sorted(list(split_column_set))
                        # add PK to each split as first column                
                        split_column_list.insert(0, entity_name)
                        print(f"......Split #{chunk} now contains {len(split_column_list)} columns")
                        printd(f"cols in split: {len(split_column_list)}")
                        printd(f"cols left to split: {len(column_set)}")
                        # split rows
                        split_row_dict_list = list()
                        # iterate through every row looking for every column for this split
                        for row in row_list:
                            split_row_dict = dict()
                            for col in split_column_list:
                                # if a whitelisted col exists in this row
                                if (col in dict(row).keys()):
                                    # add the col to the split list
                                    # clean up whitespace
                                    try:
                                        split_row_dict[col] = (dict(row).get(col)).replace('\r\n',' ').strip()
                                    except AttributeError:
                                        split_row_dict[col] = (dict(row).get(col))
                                else:
                                    pass
                            # add the row to the list of rows for this
                            split_row_dict_list.append(split_row_dict)
                        # output to tsv
                        dw = csv.DictWriter(output_file, split_column_list, delimiter='\t')
                        dw.writeheader()
                        dw.writerows(split_row_dict_list)
                    print(f"......{table_name} Split #{chunk} was successfully written to {output_location}")
            else:
                # this thread is executed for any tables with 512 cols or less
                print(f"...No need to split files for {table_name}")
                output_location = output_dir + '/' + table_name + '.tsv'
                print(f"...Writing {table_name} to {output_location}")
                with open(output_location, 'w') as output_file:
                    dw = csv.DictWriter(output_file, sorted_column_set, delimiter='\t')
                    dw.writeheader()
                    dw.writerows(row_list)
                print(f"...{table_name} was successfully written to {output_location}")
    except OSError as e:
        print(e)
