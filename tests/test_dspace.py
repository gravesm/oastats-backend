from testing.utils import unittest
from httmock import all_requests, HTTMock
import requests
import pipeline.dspace as dspace
import json

@all_requests
def error_response(url, request):
    return {
        'status_code': 500
    }

@all_requests
def dspace_response(url, request):
    response = {
        "title": "50 Shades of Hay",
        "uri": "Meadowcup",
        "departments": [
            { "canonical": "Hay There", "display": "Wut" }
        ],
        "success": True
    }
    return {
        'content': json.dumps(response)
    }

@all_requests
def failure_response(url, request):
    return {
        'content': '{"success": false}'
    }


class TestDSpace(unittest.TestCase):

    def test_get_handle_returns_handle(self):
        req_string = "http://www.example.com/foo/openaccess-disseminate/1.2/3"
        self.assertEqual(dspace.get_handle(req_string), "1.2/3")

    def test_fetch_metadata_sets_properties(self):
        with HTTMock(dspace_response):
            req = dspace.fetch_metadata({'request': '/openaccess-disseminate/1.2.3/4'})
            self.assertEqual(req['dlcs'], [{"canonical": "Hay There", "display": "Wut"}])
            self.assertEqual(req['handle'], "Meadowcup")
            self.assertEqual(req['title'], "50 Shades of Hay")

    def test_fetch_metadata_throws_exception_on_error(self):
        with HTTMock(error_response):
            self.assertRaises(requests.HTTPError,
                              dspace.fetch_metadata,
                              {'request': '/openaccess-disseminate/1.2/4'})

    def test_fetch_metadata_returns_false_on_no_success(self):
        with HTTMock(failure_response):
            self.assertFalse(dspace.fetch_metadata({'request': '/openaccess-disseminate/1.2.3/5'}))
