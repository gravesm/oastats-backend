from __future__ import print_function
import sys
import json
import apachelog
import logging
from conf import settings

logger = logging.getLogger(__name__)

parser = apachelog.parser(apachelog.formats['extended'])

mappings = settings.APACHE_FIELD_MAPPINGS

def record_filter(record):
    """Return the record if it matches certain filters, otherwise None."""
    if not record.get("status") == "200":
        return None
    if not record.get("request").startswith("GET"):
        return None
    return record

def field_mapper(request, mappings):
    """Map fields from input request dict to new dict based on mappings."""
    new_f = {}
    for k,v in request.items():
        if k in mappings:
            new_f[mappings[k]] = v
    return new_f

def parse_line(line, parser):
    """Parse line from Apache log and return parsed request as dictionary."""
    try:
        return parser.parse(line)
    except apachelog.ApacheLogParserError as err:
        logger.error(err)

def default_writer(request):
    """Dummy writer returns request dictionary."""
    return request

def json_writer(request):
    """Print request dictionary as JSON string."""
    if request:
        print(json.dumps(request))

def parse(line, writer=default_writer):
    """Parse a line from an Apache log file.

    This function will parse the request into a dictionary, map the dictionary's
    keys to configured keys, filter out requests as configured and hand off the
    resulting dict to a writer.

    Keyword arguments:
    line -- line from an Apache log file
    writer -- function that accepts Apache request as a dictionary (default
                writer simply returns the dictionary)

    """
    return writer(record_filter(field_mapper(parse_line(line, parser), mappings)))
