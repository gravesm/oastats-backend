import logging
import sys
from pipeline.log import RequestFilter

# Configure which fields from the Apache log will be retained and what field
# they will be mapped to in the final JSON object.
APACHE_FIELD_MAPPINGS = {
    '%h': 'ip_address',
    '%t': 'time',
    '%r': 'request',
    '%>s': 'status',
    '%{Referer}i': 'referer',
    '%{User-agent}i': 'user_agent',
    '%b': 'filesize',
}

# Should be a tuple with either host and port, MongoDB URI, or empty
# ex: ('localhost', 27017,) or ('mongodb://localhost:27017',)
# Remember the trailing comma!
MONGO_CONNECTION = ('localhost', 27017,)
MONGO_DB = 'oastats'
MONGO_COLLECTION = 'requests'
MONGO_SUMMARY_COLLECTION = 'summary'

# Location of the GeoIPv4 and GeoIPv6 databases
GEOIP4_DB = ''
GEOIP6_DB = ''

# DSpace identity service
DSPACE_IDENTITY_SERVICE = 'http://dspace-dev.mit.edu/ws/oastats'

# Configure logging for the application
log = logging.getLogger('pipeline')

info_hdlr = logging.StreamHandler(sys.stdout)
info_hdlr.setLevel(logging.INFO)

err_hdlr = logging.StreamHandler(sys.stderr)
err_hdlr.setFormatter(logging.Formatter('%(msg)s: %(inputfile)s:%(inputline)s'))
err_hdlr.setLevel(logging.ERROR)

log.addHandler(info_hdlr)
log.addHandler(err_hdlr)

# Configure logging for request logger
req = logging.getLogger('req_log')

ip_hdlr = logging.FileHandler('logs/ip.log')
ip_hdlr.addFilter(RequestFilter('IP_ERROR'))

req_hdlr = logging.FileHandler('logs/req.log')
req_hdlr.addFilter(RequestFilter('REQUEST_ERROR'))

meta_hdlr = logging.FileHandler('logs/meta.log')
meta_hdlr.addFilter(RequestFilter('DSPACE_ERROR'))

req.addHandler(ip_hdlr)
req.addHandler(meta_hdlr)
req.addHandler(req_hdlr)
