from testing.utils import unittest
from pipeline.parse_log import (parse_line, parser, field_mapper, parse,
                                record_filter,)

class TestLogParser(unittest.TestCase):

    def setUp(self):
        self.line = '1.2.3.4 - - [31/Jan/2013:23:58:51 -0500] "GET /openaccess-disseminate/1721.1/22774 HTTP/1.1" 200 6865 "-" "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2"'
        self.request = {
            'status': '200',
            'ip_address': '1.2.3.4',
            'time': '[31/Jan/2013:23:58:51 -0500]',
            'request': 'GET /openaccess-disseminate/1721.1/22774 HTTP/1.1',
            'referer': '-',
            'user_agent': 'Mozilla',
            'filesize': '666',
        }

    def test_parse_line_returns_dict_with_ip_address(self):
        line = parse_line(self.line, parser)
        self.assertEqual(line.get('%h'), '1.2.3.4')

    def test_field_mapper_translates_key_to_mapped_key(self):
        request = field_mapper({'%h': '1.2.3.4'}, {'%h': 'ip_address'})
        self.assertEqual(request.get('ip_address'), '1.2.3.4')

    def test_field_mapper_drops_unmapped_fields(self):
        request = field_mapper({'%r': 'GIMME'}, {'%h': 'ip_address'})
        self.assertTrue('%r' not in request)

    def test_parse_returns_a_mapped_request(self):
        request = parse(self.line)
        self.assertEqual(request.get('ip_address'), '1.2.3.4')

    def test_filter_drops_non_200_requests(self):
        self.request['status'] = '201'
        self.assertIsNone(record_filter(self.request))

    def test_filter_drops_non_get_requests(self):
        self.request['request'] = 'POST /openaccess-disseminate/1721.1/22774 HTTP/1.1'
        self.assertIsNone(record_filter(self.request))

    def test_filter_drops_non_handle_requests(self):
        self.request['request'] = 'GET /foobar/1721.1/22774 HTTP/1.1'
        self.assertIsNone(record_filter(self.request))

    def test_filter_returns_successful_get_requests(self):
        self.assertEqual(record_filter(self.request), self.request)

    def test_filter_drops_localhost_requests(self):
        self.request['ip_address'] = '127.0.0.1'
        self.assertIsNone(record_filter(self.request))
        self.request['ip_address'] = '::1'
        self.assertIsNone(record_filter(self.request))

    def test_filter_drops_monitoring_requests(self):
        self.request['ip_address'] = '18.7.27.25'
        self.assertIsNone(record_filter(self.request))

    def test_filter_drops_crawlers(self):
        self.request['user_agent'] = 'creepycrawler'
        self.assertIsNone(record_filter(self.request))
