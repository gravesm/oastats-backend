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

    def test_add_summary_data_summarizes_data(self):
        self.req.insert([
                        {'handle': 'TEH_FOO'},
                        {'handle': 'TEH_FOO'},
                        {'handle': 'TEH_BAR'}])
        summarize.add_summary_data(self.req, self.sum)
        self.assertEqual(self.sum.find_one(),
                         {'_id': 'Overall', 'type': 'overall', 'size': 2, 'downloads': 3})

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

    def test_add_field_summary_overall_summarizes_overall_data(self):
        self.req.insert([
                        {'handle': 'LOONYGOONS', 'author': 'Sir Topham Hate'},
                        {'handle': 'LOONYGOONS', 'author': 'Sir Topham Hate'},
                        {'handle': 'LOONYGOONS', 'author': 'Master Bromide'},
                        {'handle': 'STIMBLECON', 'author': 'Sir Topham Hate'}])
        summarize.add_field_summary_overall(self.req, self.sum, 'author')
        self.assertEqual(self.sum.find_one({'_id': 'Sir Topham Hate'}),
                         {'_id': 'Sir Topham Hate',
                          'type': 'author', 'downloads': 3, 'size': 2})
        self.assertEqual(self.sum.find_one({'_id': 'Master Bromide'}),
                         {'_id': 'Master Bromide',
                          'type': 'author', 'downloads': 1, 'size': 1})

    def test_add_author_dlcs_adds_set_of_dlcs(self):
        self.req.insert([
                        {'author': 'Muffincup', 'dlc': 'School of Muffin Science'},
                        {'author': 'Muffincup', 'dlc': 'School of Muffin Science'},
                        {'author': 'Muffincup', 'dlc': 'School of Muffin Design'},
                        {'author': 'Cupcake Mary', 'dlc': 'Dept of Cupcake Research'}])
        summarize.add_author_dlcs(self.req, self.sum)
        self.assertEqual(self.sum.find_one({'_id': 'Muffincup'}),
                         {'_id': 'Muffincup', 'parents': [
                            'School of Muffin Design', 'School of Muffin Science']})
        self.assertEqual(self.sum.find_one({'_id': 'Cupcake Mary'}),
                         {'_id': 'Cupcake Mary', 'parents': [
                            'Dept of Cupcake Research']})

    def test_add_handle_author_adds_set_of_authors(self):
        self.req.insert([
                        {'handle': 'MOONBOTTOM', 'author': 'Lord Fiddleruff'},
                        {'handle': 'MOONBOTTOM', 'author': 'Lady Rumplecuff'},
                        {'handle': 'MOONBOTTOM', 'author': 'Lord Fiddleruff'},
                        {'handle': 'KITTENTOWN', 'author': 'Lady Rumplecuff'}])
        summarize.add_handle_author(self.req, self.sum)
        self.assertEqual(self.sum.find_one({'_id': 'MOONBOTTOM'}),
                         {'_id': 'MOONBOTTOM', 'parents': [
                            'Lady Rumplecuff', 'Lord Fiddleruff']})
        self.assertEqual(self.sum.find_one({'_id': 'KITTENTOWN'}),
                         {'_id': 'KITTENTOWN', 'parents': ['Lady Rumplecuff']})

    def tearDown(self):
        self.box.stop()
