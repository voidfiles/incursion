import unittest

from incursion.continuous_queries import InfluxDBContinuousQuery
from incursion.query import InfluxQuery


class ExampleContinuousQuery(InfluxDBContinuousQuery):
    series = 'page_views'
    fanouts = {
        'category': ('category_id',),
        'author': ('author_id',),
        'category_author': ('category_id', 'author_id',),
    }


class TestSequenceFunctions(unittest.TestCase):

    def test_query_for(self):
        q = ExampleContinuousQuery().query_for('author', '1m', author_id=10)
        q = q.where(time__gt=InfluxQuery.now_minus('4h'))
        q = q.limit(None)

        self.assertEquals(q.query(), u"select sum(count) from 1m.page_views.author.10 where time > now() - 4h")

    def test_queries_for_sync(self):
        queries = ExampleContinuousQuery().queries_for_sync()
        raw_queries = [
            u'select * from page_views into page_views.category.[category_id]',
            u'select * from /^page_views.category.*/ group by time(1m) into 1m.:series_name',
            u'select * from /^page_views.category.*/ group by time(1h) into 1h.:series_name',
            u'select * from /^page_views.category.*/ group by time(1d) into 1d.:series_name',
            u'select * from page_views into page_views.category_author.[category_id].[author_id]',
            u'select * from /^page_views.category_author.*/ group by time(1m) into 1m.:series_name',
            u'select * from /^page_views.category_author.*/ group by time(1h) into 1h.:series_name',
            u'select * from /^page_views.category_author.*/ group by time(1d) into 1d.:series_name',
            u'select * from page_views into page_views.author.[author_id]',
            u'select * from /^page_views.author.*/ group by time(1m) into 1m.:series_name',
            u'select * from /^page_views.author.*/ group by time(1h) into 1h.:series_name',
            u'select * from /^page_views.author.*/ group by time(1d) into 1d.:series_name'
        ]

        zipped = zip(map(lambda q: q.query(), queries), raw_queries)
        for gen, raw in zipped:
            self.assertEquals(gen, raw)
