import unittest
from pipeline.parse_log import parse_line, parser, field_mapper, parse

class TestLogParser(unittest.TestCase):

    def setUp(self):
        self.line = '1.2.3.4 - - [31/Jan/2013:23:58:51 -0500] "GET /handle/1721.1/22774?show=full HTTP/1.1" 200 6865 "-" "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2"'
        self.request = {
            '%h': '1.2.3.4',
            '%r': 'GIMME THE DOCS AND NO ONE GETS HURT',
        }

    def test_parse_line_returns_dict_with_ip_address(self):
        line = parse_line(self.line, parser)
        self.assertEqual(line.get('%h'), '1.2.3.4')

    def test_field_mapper_translates_key_to_mapped_key(self):
        request = field_mapper(self.request, {'%h': 'ip_address'})
        self.assertEqual(request.get('ip_address'), self.request.get('%h'))

    def test_field_mapper_drops_unmapped_fields(self):
        request = field_mapper(self.request, {'%h': 'ip_address'})
        self.assertTrue('%r' not in request)

    def test_parse_returns_a_mapped_request(self):
        request = parse(self.line)
        self.assertEqual(request.get('ip_address'), '1.2.3.4')
