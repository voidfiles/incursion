import unittest
from time import sleep

from incursion.continuous_queries import InfluxDBContinuousQuery, sync_continuous_queries, FanoutQuery
from incursion.query import InfluxQuery
from incursion.client import INDBClient

from .utils import InfluxDBClientTest


class ExampleContinuousQuery(InfluxDBContinuousQuery):
    series = 'page_views'
    fanouts = {
        'category': FanoutQuery(fanout_on=('category_id',), columns_to_copy=('name', )),
        'author': FanoutQuery(fanout_on=('author_id',), columns_to_copy=('name', )),
        'category_author': FanoutQuery(fanout_on=('category_id', 'author_id',), columns_to_copy=('name', )),
    }
    downsample_interval = ['1s', '1h', '1d']


class TestContinueQueries(unittest.TestCase):

    def test_query_for(self):
        q = ExampleContinuousQuery().query_for('author', '1s', author_id=10)
        q = q.where(time__gt=InfluxQuery.now_minus('4h'))
        q = q.limit(None)

        self.assertEquals(q.query(), u"select sum(count) from 1s.page_views.author.10 where time > now() - 4h")

    def test_continuous_queries(self):
        queries = ExampleContinuousQuery().continuous_queries()
        raw_queries = [
            u'select name from page_views into page_views.category.[category_id]',
            u'select count(name) from /^page_views.category.*/ group by time(1s) into 1s.:series_name',
            u'select count(name) from /^page_views.category.*/ group by time(1h) into 1h.:series_name',
            u'select count(name) from /^page_views.category.*/ group by time(1d) into 1d.:series_name',
            u'select name from page_views into page_views.category_author.[category_id].[author_id]',
            u'select count(name) from /^page_views.category_author.*/ group by time(1s) into 1s.:series_name',
            u'select count(name) from /^page_views.category_author.*/ group by time(1h) into 1h.:series_name',
            u'select count(name) from /^page_views.category_author.*/ group by time(1d) into 1d.:series_name',
            u'select name from page_views into page_views.author.[author_id]',
            u'select count(name) from /^page_views.author.*/ group by time(1s) into 1s.:series_name',
            u'select count(name) from /^page_views.author.*/ group by time(1h) into 1h.:series_name',
            u'select count(name) from /^page_views.author.*/ group by time(1d) into 1d.:series_name'
        ]

        zipped = zip(map(lambda q: q.query(), queries), raw_queries)
        for gen, raw in zipped:
            self.assertEquals(gen, raw)


class TestContinuousQueryPlanner(InfluxDBClientTest):

    def test_continuous_queries_sync(self):
        queries = ExampleContinuousQuery().continuous_queries()
        sync_continuous_queries(self.client, map(lambda q: q.query(), queries))

        response = self.client.query('list continuous queries')
        series = response[0]

        assert len(series['points']) == 12

        sync_continuous_queries(self.client, map(lambda q: q.query(), queries))

        response = self.client.query('list continuous queries')
        series = response[0]

        assert len(series['points']) == 12

    def test_for_interval(self):
        queries = ExampleContinuousQuery().continuous_queries()
        sync_continuous_queries(self.client, map(lambda q: q.query(), queries))

        self.generateData()
        q = InfluxQuery.for_series('page_views').columns(category_id=InfluxQuery.distinct('category_id'))
        resp = INDBClient(conn=self.client).result_for_query(q)
        cat_id = resp['page_views'][0].category_id

        q = ExampleContinuousQuery().query_for('category', category_id=cat_id)
        resp = INDBClient(conn=self.client).result_for_query(q)
        assert len(resp.values()[0]) > 0

        self.client.write_points([{
            "name": "page_views",
            "columns": ["category_id", "author_id", "name"],
            "points": [
                [cat_id, 1, 'x'],
                [cat_id, 1, 'x'],
                [cat_id, 1, 'x'],
                [cat_id, 1, 'x'],
            ]
        }])

        sleep(1)

        q = ExampleContinuousQuery().query_for('category', '1s', category_id=cat_id)
        resp = INDBClient(conn=self.client).result_for_query(q)
        assert len(resp.values()[0]) > 0
