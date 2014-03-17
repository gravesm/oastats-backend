import logging
import sys

# Configure which fields from the Apache log will be retained and what field
# they will be mapped to in the final JSON object.
APACHE_FIELD_MAPPINGS = {
    '%h': 'ip_address',
    '%t': 'time',
    '%r': 'request',
    '%>s': 'status',
    '%{Referer}i': 'referer',
    '%{User-agent}i': 'user_agent',
}

# Should be a tuple with either host and port, MongoDB URI, or empty
# ex: ('localhost', 27017,) or ('mongodb://localhost:27017',) remember the trailing comma!
MONGO_CONNECTION = ('localhost', 27017,)
MONGO_DB = 'oastats'
MONGO_COLLECTION = 'requests'

# Location of the GeoIP database
GEOIP_DB = 'tests/fixtures/GeoLite2-Country.mmdb'

DSPACE_IDENTITY_SERVICE = 'http://www.example.com'

# Configure logging for the application
log = logging.getLogger('pipeline')
log.addHandler(logging.StreamHandler(sys.stderr))
log.setLevel(logging.WARNING)
