#!/usr/bin/env python

from __future__ import print_function
import fileinput
import sys
import json
import apachelog
from conf import settings


parser = apachelog.parser(apachelog.formats['extended'])

mappings = settings.APACHE_FIELD_MAPPINGS

def record_filter(record):
    return record

def field_mapper(fields, mappings):
    new_f = {}
    for k,v in fields.items():
        if k in mappings:
            new_f[mappings[k]] = v
    return new_f

def parse_line(line, parser):
    try:
        return parser.parse(line)
    except apachelog.ApacheLogParserError as err:
        print(err, file=sys.stderr)

def default_writer(line):
    return line

def json_writer(line):
    if line:
        print(json.dumps(line))

def parse(line, writer=default_writer):
    return writer(record_filter(field_mapper(parse_line(line, parser), mappings)))

def main():
    for line in fileinput.input():
        parse(line, json_writer)

if __name__ == "__main__":

    if "-h" in sys.argv:
        help = """
    To use, list file(s) as arguments or read from stdin

    Example usage:
        ./kronstadt.py log_file1 log_file2 log_file3
        ./kronstadt.py < log_file1
        head log_file1 | ./kronstadt.py"""

        print(help)
        sys.exit(0)

    main()
