from testing.utils import unittest
from mongobox import MongoBox
import pipeline.summarize as summarize
from datetime import datetime

class TestSummarize(unittest.TestCase):

    def setUp(self):
        self.box = MongoBox()
        self.box.start()
        self.req = self.box.client().db1.req
        self.sum = self.box.client().db1.sum

    def test_set_overall_summary_summarizes_downloads(self):
        self.req.insert([
                        {'handle': 'TEH_FOO'},
                        {'handle': 'TEH_FOO'},
                        {'handle': 'TEH_BAR'}])
        summarize.set_overall_summary(self.req, self.sum)
        self.assertEqual(self.sum.find_one(),
                         {'_id': 'Overall', 'type': 'overall', 'size': 2, 'downloads': 3})

    def test_set_overall_countries_summarizes_country_downloads(self):
        self.req.insert([
                        {'country': 'RUS'},
                        {'country': 'RUS'},
                        {'country': 'USA'}])
        summarize.set_overall_countries(self.req, self.sum)
        result = self.sum.find_one()
        self.assertEqual(len(result['countries']), 2)
        self.assertIn({'country': 'RUS', 'downloads': 2}, result['countries'])
        self.assertIn({'country': 'USA', 'downloads': 1}, result['countries'])

    def test_set_overall_date_summarizes_date_downloads(self):
        self.req.insert([
                        {'time': datetime(2006, 6, 6)},
                        {'time': datetime(2006, 6, 6)},
                        {'time': datetime(1955, 11, 5)}])
        summarize.set_overall_date(self.req, self.sum)
        result = self.sum.find_one()
        self.assertEqual(len(result['dates']), 2)
        self.assertIn({'date': '1955-11-05', 'downloads': 1}, result['dates'])
        self.assertIn({'date': '2006-06-06', 'downloads': 2}, result['dates'])

    def test_set_handle_summary_summarizes_downloads(self):
        self.req.insert([
                        {'handle': 'LIMBLE', 'title': 'Nimble'},
                        {'handle': 'LIMBLE', 'title': 'Nimble'},
                        {'handle': 'MIMBLE', 'title': 'Kimble'}])
        summarize.set_handle_summary(self.req, self.sum)

        res = self.sum.find_one({'_id': 'LIMBLE'})
        self.assertEqual(res['downloads'], 2)
        self.assertEqual(res['type'], 'handle')
        self.assertEqual(res['title'], 'Nimble')

        res = self.sum.find_one({'_id': 'MIMBLE'})
        self.assertEqual(res['downloads'], 1)
        self.assertEqual(res['title'], 'Kimble')

    def test_set_handle_countries_summarizes_country_downloads(self):
        self.req.insert([
                        {'handle': 'LOONYGOONS', 'country': 'FIN'},
                        {'handle': 'FUMBLENOONS', 'country': 'GER'},
                        {'handle': 'LOONYGOONS', 'country': 'FIN'},
                        {'handle': 'LOONYGOONS', 'country': 'GER'}])
        summarize.set_handle_countries(self.req, self.sum)

        res = self.sum.find_one({'_id': 'LOONYGOONS'})
        self.assertEqual(len(res['countries']), 2)
        self.assertIn({'country': 'FIN', 'downloads': 2}, res['countries'])
        self.assertIn({'country': 'GER', 'downloads': 1}, res['countries'])

        res = self.sum.find_one({'_id': 'FUMBLENOONS'})
        self.assertEqual(res['countries'], [{'country': 'GER', 'downloads': 1}])

    def test_set_handle_dates_summarizes_date_downloads(self):
        self.req.insert([
                        {'handle': 'MUMBLEROONS', 'time': datetime(2006, 6, 6)},
                        {'handle': 'MUMBLEROONS', 'time': datetime(1955, 11, 5)},
                        {'handle': 'MUMBLEROONS', 'time': datetime(1955, 11, 5)},
                        {'handle': 'GUMBLETOONS', 'time': datetime(2006, 6, 6)}])
        summarize.set_handle_dates(self.req, self.sum)

        res = self.sum.find_one({'_id': 'MUMBLEROONS'})
        self.assertEqual(len(res['dates']), 2)
        self.assertIn({'date': '1955-11-05', 'downloads': 2}, res['dates'])
        self.assertIn({'date': '2006-06-06', 'downloads': 1}, res['dates'])

        res = self.sum.find_one({'_id': 'GUMBLETOONS'})
        self.assertEqual(len(res['dates']), 1)
        self.assertIn({'date': '2006-06-06', 'downloads': 1}, res['dates'])

    def test_set_handle_authors_skips_empty_authors(self):
        self.req.insert([
                        {'handle': 'OVERLORDS', 'authors': [
                            {'mitid': 1234, 'name': 'Buttercup'}]},
                        {'handle': 'TIMELORDS', 'authors': {}},
                        {'handle': 'CAKELORDS', 'authors': 'bogus'},
                        {'handle': 'TRACILORDS'}])
        summarize.set_handle_authors(self.req, self.sum)
        self.assertIsNone(self.sum.find_one({'_id': 'TIMELORDS'}))
        self.assertIsNone(self.sum.find_one({'_id': 'CAKELORDS'}))
        self.assertIsNone(self.sum.find_one({'_id': 'TRACILORDS'}))

    def test_set_handle_authors_creates_set_of_authors(self):
        self.req.insert([
                        {'handle': 'TINSLETOONS', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'handle': 'TINSLETOONS', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'handle': 'STIMBLEBOONS', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'},
                            {'mitid': 98765, 'name': "Sir Cup'ncake"}]}])
        summarize.set_handle_authors(self.req, self.sum)

        res = self.sum.find_one({'_id': 'TINSLETOONS'})
        self.assertEqual(res['parents'], [{'mitid': 12345, 'name': 'Muffincup'}])

        res = self.sum.find_one({'_id': 'STIMBLEBOONS'})
        self.assertEqual(len(res['parents']), 2)
        self.assertIn({'mitid': 12345, 'name': 'Muffincup'}, res['parents'])
        self.assertIn({'mitid': 98765, 'name': "Sir Cup'ncake"}, res['parents'])

    def test_set_author_dates_summarizes_date_downloads(self):
        self.req.insert([
                        {'time': datetime(2006,6,6), 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'time': datetime(1955,11,5), 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'time': datetime(2006,6,6), 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'},
                            {'mitid': 98765, 'name': 'Sir Topham Hate'}]}])
        summarize.set_author_dates(self.req, self.sum)

        res = self.sum.find_one({'_id': {'mitid': 12345, 'name': 'Muffincup'}})
        self.assertEqual(len(res['dates']), 2)
        self.assertIn({'date': '2006-06-06', 'downloads': 2}, res['dates'])
        self.assertIn({'date': '1955-11-05', 'downloads': 1}, res['dates'])

        res = self.sum.find_one({'_id': {'mitid': 98765, 'name': 'Sir Topham Hate'}})
        self.assertEqual([{'date': '2006-06-06', 'downloads': 1}], res['dates'])

    def test_set_author_countries_summarizes_country_downloads(self):
        self.req.insert([
                        {'country': 'USA', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'country': 'FRA', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'country': 'USA', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'},
                            {'mitid': 98765, 'name': "Sir Cup'ncake"}]}])
        summarize.set_author_countries(self.req, self.sum)

        res = self.sum.find_one({'_id': {'mitid': 12345, 'name': 'Muffincup'}})
        self.assertEqual(len(res['countries']), 2)
        self.assertIn({'country': 'USA', 'downloads': 2}, res['countries'])
        self.assertIn({'country': 'FRA', 'downloads': 1}, res['countries'])

        res = self.sum.find_one({'_id': {'mitid': 98765, 'name': "Sir Cup'ncake"}})
        self.assertEqual([{'country': 'USA', 'downloads': 1}], res['countries'])

    def test_set_author_summary_summarizes_downloads(self):
        self.req.insert([
                        {'handle': 'KITTENTOWN', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'handle': 'KITTENTOWN', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'}]},
                        {'handle': 'MOONBOTTOM', 'authors': [
                            {'mitid': 12345, 'name': 'Muffincup'},
                            {'mitid': 98765, 'name': 'Sir Topham Hate'}]}])
        summarize.set_author_summary(self.req, self.sum)

        res = self.sum.find_one({'_id': {'mitid': 12345, 'name': 'Muffincup'}})
        self.assertEqual(res['downloads'], 3)
        self.assertEqual(res['size'], 2)

        res = self.sum.find_one({'_id': {'mitid': 98765, 'name': 'Sir Topham Hate'}})
        self.assertEqual(res['downloads'], 1)
        self.assertEqual(res['size'], 1)

    def test_set_author_dlcs_creates_set_of_dlcs(self):
        self.req.insert([
                        {'authors': [
                            {'mitid': 1234, 'name': 'Rumplecuff'},
                            {'mitid': 9876, 'name': 'Muffincup'}],
                         'dlcs': ['School of Muffin Science']},
                        {'authors': [
                            {'mitid': 1234, 'name': 'Rumplecuff'}],
                         'dlcs': ['School of Muffin Science', 'Dept of Cupcake Design']}])
        summarize.set_author_dlcs(self.req, self.sum)

        res = self.sum.find_one({'_id': {'mitid': 1234, 'name': 'Rumplecuff'}})
        self.assertEqual(len(res['parents']), 2)
        self.assertIn('School of Muffin Science', res['parents'])
        self.assertIn('Dept of Cupcake Design', res['parents'])

        res = self.sum.find_one({'_id': {'mitid': 9876, 'name': 'Muffincup'}})
        self.assertEqual(['School of Muffin Science'], res['parents'])

    def test_set_dlc_summary_summarizes_downloads(self):
        self.req.insert([
                        {'handle': 'MOONBOTTOM', 'dlcs': ['S of Cupcake Sci']},
                        {'handle': 'MOONBOTTOM', 'dlcs': ['S of Cupcake Sci']},
                        {'handle': 'KITTENTOWN', 'dlcs': ['S of Cupcake Sci', 'Dep of Pie']}])
        summarize.set_dlc_summary(self.req, self.sum)

        res = self.sum.find_one({'_id': 'S of Cupcake Sci'})
        self.assertEqual(res['downloads'], 3)
        self.assertEqual(res['size'], 2)

        res = self.sum.find_one({'_id': 'Dep of Pie'})
        self.assertEqual(res['downloads'], 1)
        self.assertEqual(res['size'], 1)

    def test_set_dlc_dates_summarizes_date_downloads(self):
        self.req.insert([
                        {'time': datetime(2006,6,6), 'dlcs': ['S of Cupcake Sci']},
                        {'time': datetime(1955,11,5), 'dlcs': ['S of Cupcake Sci']},
                        {'time': datetime(2006,6,6), 'dlcs': ['S of Cupcake Sci', 'Dep of Pie']}])
        summarize.set_dlc_dates(self.req, self.sum)

        res = self.sum.find_one({'_id': 'S of Cupcake Sci'})
        self.assertEqual(len(res['dates']), 2)
        self.assertIn({'date': '2006-06-06', 'downloads': 2}, res['dates'])
        self.assertIn({'date': '1955-11-05', 'downloads': 1}, res['dates'])

        res = self.sum.find_one({'_id': 'Dep of Pie'})
        self.assertEqual([{'date': '2006-06-06', 'downloads': 1}], res['dates'])

    def test_set_dlc_countries_summarizes_country_downloads(self):
        self.req.insert([
                        {'country': 'USA', 'dlcs': ['Dep of Pie']},
                        {'country': 'RUS', 'dlcs': ['Dep of Pie']},
                        {'country': 'USA', 'dlcs': ['Dep of Pie', 'S of Cupcake Sci']}])
        summarize.set_dlc_countries(self.req, self.sum)

        res = self.sum.find_one({'_id': 'Dep of Pie'})
        self.assertEqual(len(res['countries']), 2)
        self.assertIn({'country': 'USA', 'downloads': 2}, res['countries'])
        self.assertIn({'country': 'RUS', 'downloads': 1}, res['countries'])

        res = self.sum.find_one({'_id': 'S of Cupcake Sci'})
        self.assertEqual([{'country': 'USA', 'downloads': 1}], res['countries'])

    def tearDown(self):
        self.box.stop()
