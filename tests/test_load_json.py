from testing.utils import unittest
from mongobox import MongoBox
from pipeline.load_json import get_collection, insert

class TestLoadJSON(unittest.TestCase):

    def setUp(self):
        self.box = MongoBox()
        self.box.start()
        self.coll = get_collection('db1', 'coll1', ('localhost', self.box.port))

    def test_get_collection_returns_mongo_collection(self):
        self.assertEqual(self.coll.count(), 0)

    def test_insert_adds_request_to_collection(self):
        req = {'Tumblesniff': 'Bogwort'}
        insert(self.coll, req)
        self.assertEqual(self.coll.find_one(), req)

    def tearDown(self):
        self.box.stop()
