#!/usr/bin/env python

import sys
import json
import requests
import fileinput

def main():
    for line in fileinput.input():

        # Fetch item metadata using handle
        data = json.loads(line)
        r = requests.get('http://hostname/oastats?handle='+data['handle'])
        metadata = r.json()

        # Merge pipeline metadata with item metadata and emit
        data.update(metadata)
        print(json.dumps(data))

if __name__ == '__main__':

    if "-h" in sys.argv:
        help = """
    To use, list handle(s) as arguments or read from stdin

    Example usage:
        ./fetch_dspace_md.py handle1 handle2 handle3
        ./fetch_dspace_md.py < handle1
        echo handle1 | ./fetch_dspace_md.py"""

        print(help)
        sys.exit(0)

    main()