from __future__ import print_function
import hashlib
import json
import os
import datetime
import pandas as pd

# Fields:       ['Author', 'ID', 'Date Issued', 'URI', 'Match count', 'MIT ID']
# DATA EDIT:    Combined match files A & B.  B contains new matches from what were 1000+ 0 matches in A (original run).
# DATA EDIT:    Deleted row 939 because it was for some reason the fucking column names from the original source file
df = pd.read_csv(os.getenv('MIT_DW_IDS'), sep='!')

id_output = []
count = 0
no_id = 0
salt = 'HSMHGBZQMTRYBCWGWKCM'


# Build a single author identity representation
def gen_id(row):

    # Build an individual identity dictionary
    json_record = {'name': row['Author']}

    # If we're missing an MIT ID, just write the name
    if pd.isnull(row['MIT ID']):
        json_record['mitid'] = ""

    else:
        # MD5 Salt Hash the MIT ID
        m = hashlib.md5()
        m.update(salt)
        m.update(str(row['MIT ID']))
        json_record['mitid'] = salt + m.hexdigest()

    return json_record


def gen_all_ids():

    # Track how many unique items we find
    item_count = 0

    # Group by handle and generate aggregate of author identities for each
    for name, group in df.groupby('URI'):
        identities = []
        for row_number, row in group.iterrows():
            identities.append(gen_id(row))
            item_count += 1

        # Output handle and associated identities in JSON array
        name_and_ids = {"handle": name, "ids": identities}
        id_output.append(name_and_ids)

    # Output all item identities as a JSON array
    with open('dspace_ids_' + str(datetime.datetime.now()) + '.json', 'w') as json_out:
        json_out.write(json.dumps(id_output))

if __name__ == '__main__':
    gen_all_ids()