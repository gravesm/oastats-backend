
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

# Location of the GeoIPv4 and GeoIPv6 databases
GEOIP4_DB = 'tests/fixtures/GeoIP.dat'
GEOIP6_DB = 'tests/fixtures/GeoIPv6.dat'
