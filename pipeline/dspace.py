import re
import requests
from conf import settings

handle_pattern = re.compile(r"/handle/(?P<handle>[0-9.]+/[0-9]+)")

def fetch_metadata(request):
    r = requests.get(settings.DSPACE_IDENTITY_SERVICE,
                     params={'handle': get_handle(request.get("request"))})
    data = r.raise_for_status() or r.json()
    if data.get('success', 'false').lower() != 'true':
        return False
    request['dlc'] = data.get("department")
    request['handle'] = data.get("uri")
    request['title'] = data.get("title")
    return request

def get_handle(req_string):
    matches = handle_pattern.search(req_string)
    return matches.groupdict().get('handle')
