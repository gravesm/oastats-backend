import pygeoip
import pycountry
import time
from datetime import datetime
import logging
from conf import settings

logger = logging.getLogger(__name__)

geov4 = pygeoip.GeoIP(settings.GEOIP4_DB)
geov6 = pygeoip.GeoIP(settings.GEOIP6_DB)

def get_alpha2_code(ip):
    try:
        return geov4.country_code_by_addr(ip)
    except pygeoip.GeoIPError:
        return geov6.country_code_by_addr(ip)

def get_alpha3_code(alpha2):
    country = pycountry.countries.get(alpha2=alpha2)
    return country.alpha3

def add_country(request):
    """Add ISO 3166-1 alpha-3 code to country field of request dict."""
    ip = request.get('ip_address')
    try:
        alpha2 = get_alpha2_code(ip)
        alpha3 = get_alpha3_code(alpha2)
        request['country'] = alpha3
    except (pygeoip.GeoIPError, KeyError):
        logger.warning("Could not find country code for IP address: {0}".format(ip))
    return request

def str_to_dt(request):
    """Convert Apache timestamp to timezone-naive datetime object."""
    t = time.strptime(request['time'].split(' ')[0], "[%d/%b/%Y:%H:%M:%S")
    request['time'] = datetime.fromtimestamp(time.mktime(t))
    return request
