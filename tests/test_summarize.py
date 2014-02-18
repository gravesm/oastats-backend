import unittest
from mongobox import MongoBox
import pipeline.summarize as summarize
from datetime import datetime

class TestSummarize(unittest.TestCase):

    def setUp(self):
        self.box = MongoBox()
        self.box.start()
        self.req = self.box.client().db1.req
        self.sum = self.box.client().db1.sum

    def load(self):

        self.req.insert([self.req1, self.req2])

    def test_add_summary_map_summarizes_country_data(self):
        self.req.insert([
                        {'country': 'RUS'},
                        {'country': 'RUS'},
                        {'country': 'USA'}])
        summarize.add_summary_map(self.req, self.sum)
        self.assertEqual(self.sum.find_one({'_id': 'Overall'}),
                         {'_id': 'Overall', 'countries': [
                            {'country': 'RUS', 'downloads': 2},
                            {'country': 'USA', 'downloads': 1}
                         ]})

    def test_add_summary_date_summarizes_date_data(self):
        self.req.insert([
                        {'time': datetime(2006, 6, 6)},
                        {'time': datetime(2006, 6, 6)},
                        {'time': datetime(1955, 11, 5)}])
        summarize.add_summary_date(self.req, self.sum)
        self.assertEqual(self.sum.find_one({'_id': 'Overall'}),
                         {'_id': 'Overall', 'dates': [
                            {'date': '1955-11-05', 'downloads': 1},
                            {'date': '2006-06-06', 'downloads': 2}
                         ]})

    def test_add_field_summary_date_summarizes_date(self):
        self.req.insert([
                        {'author': 'Muffincup', 'time': datetime(2006,6,6)},
                        {'author': 'Muffincup', 'time': datetime(2006,6,6)},
                        {'author': "Sir Cup'ncake", 'time': datetime(1955,11,5)},
                        {'author': "Sir Cup'ncake", 'time': datetime(1999,12,31)}])
        summarize.add_field_summary_date(self.req, self.sum, 'author')
        self.assertEqual(self.sum.find_one({'_id': 'Muffincup'}),
                         {'_id': 'Muffincup', 'dates': [
                            {'date': '2006-06-06', 'downloads': 2}
                         ]})
        self.assertEqual(self.sum.find_one({'_id': "Sir Cup'ncake"}),
                         {'_id': "Sir Cup'ncake", 'dates': [
                            {'date': '1955-11-05', 'downloads': 1},
                            {'date': '1999-12-31', 'downloads': 1}
                         ]})

    def test_add_field_summary_map_summarizes_country(self):
        self.req.insert([
                        {'author': 'Muffincup', 'country': 'USA'},
                        {'author': 'Muffincup', 'country': 'USA'},
                        {'author': 'Tinsletoon', 'country': 'GER'},
                        {'author': 'Tinsletoon', 'country': 'FIN'},])
        summarize.add_field_summary_map(self.req, self.sum, 'author')
        self.assertEqual(self.sum.find_one({'_id': 'Muffincup'}),
                         {'_id': 'Muffincup', 'countries': [
                            {'country': 'USA', 'downloads': 2}
                         ]})
        self.assertEqual(self.sum.find_one({'_id': 'Tinsletoon'}),
                         {'_id': 'Tinsletoon', 'countries': [
                            {'country': 'FIN', 'downloads': 1},
                            {'country': 'GER', 'downloads': 1}
                         ]})

    def tearDown(self):
        self.box.stop()