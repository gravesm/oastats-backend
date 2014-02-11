from __future__ import print_function
import json
import generate_identities

def pipe_ids():

    # Run generation of identities if there is new DW data
    print("Generating DSpace Identity JSON from Data Warehouse data...")
    generate_identities.gen_all_ids()

    count = 0

    # Iterate through all item identity lists and output for
    # piping to DSpace ingest script.
    for item in json.load(open('dspace_ids_01.json')):
        print(json.dumps(item),)
        count += 1
        if count > 10:
            exit()

if __name__ == '__main__':
    pipe_ids()