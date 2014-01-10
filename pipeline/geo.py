import pygeoip
import pycountry
from conf import settings

geodb = pygeoip.GeoIP(settings.GEOIP4_DB)
geov6 = pygeoip.GeoIP(settings.GEOIP6_DB)

def add_country(request):
    ip = request['ip_address']
    try:
        iso_alpha2 = geodb.country_code_by_addr(ip)
    except pygeoip.GeoIPError:
        iso_alpha2 = geov6.country_code_by_addr(ip)
    try:
        country = pycountry.countries.get(alpha2=iso_alpha2)
        request['country'] = country.alpha3
    except KeyError:
        # @TODO: What do we do if we can't match the IP to a valid country?
        pass
    return request
