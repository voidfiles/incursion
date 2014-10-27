from collections import OrderedDict
import os
import unittest
import pprint
from time import sleep

from incursion.continuous_queries import InfluxDBContinuousQuery, sync_continuous_queries, FanoutQuery
import incursion as indb

from .utils import InfluxDBClientTest

RUNNING_IN_TRAVIS = os.environ.get('RUNNING_IN_TRAVIS')


class ExampleContinuousQuery(InfluxDBContinuousQuery):
    series = 'page_views'
    column_to_count = 'name'
    fanouts = OrderedDict([
        ('category', FanoutQuery(fanout_on=('category_id',), columns_to_copy=('name', ))),
        ('author', FanoutQuery(fanout_on=('author_id',), columns_to_copy=('name', ))),
        ('category_author', FanoutQuery(fanout_on=('category_id', 'author_id',), columns_to_copy=('name', )),)
    ])
    downsample_interval = ['1s', '1h', '1d']


class TestContinueQueries(unittest.TestCase):

    def test_query_for(self):
        q = ExampleContinuousQuery().query_for('author', '1s', author_id=10)
        q = q.where(time__gt=indb.now_minus('4h'))
        q = q.limit(None)

        self.assertEquals(q.query(), u'select sum(count) from "1s.page_views.author.10" where time > now() - 4h')

    def test_continuous_queries(self):
        queries = ExampleContinuousQuery().continuous_queries()
        raw_queries = [
            u'select count(name) from "page_views" group by time(1s) into 1s.:series_name',
            u'select count(name) from "page_views" group by time(1h) into 1h.:series_name',
            u'select count(name) from "page_views" group by time(1d) into 1d.:series_name',
            u'select name from "page_views" into page_views.category.[category_id]',
            u'select count(name) from /^page_views.category.*/ group by time(1s) into 1s.:series_name',
            u'select count(name) from /^page_views.category.*/ group by time(1h) into 1h.:series_name',
            u'select count(name) from /^page_views.category.*/ group by time(1d) into 1d.:series_name',
            u'select name from "page_views" into page_views.author.[author_id]',
            u'select count(name) from /^page_views.author.*/ group by time(1s) into 1s.:series_name',
            u'select count(name) from /^page_views.author.*/ group by time(1h) into 1h.:series_name',
            u'select count(name) from /^page_views.author.*/ group by time(1d) into 1d.:series_name',
            u'select name from "page_views" into page_views.category_author.[category_id].[author_id]',
            u'select count(name) from /^page_views.category_author.*/ group by time(1s) into 1s.:series_name',
            u'select count(name) from /^page_views.category_author.*/ group by time(1h) into 1h.:series_name',
            u'select count(name) from /^page_views.category_author.*/ group by time(1d) into 1d.:series_name',
        ]
        pprint.pprint(list(map(lambda q: q.query(), queries)))
        pprint.pprint(raw_queries)
        zipped = zip(list(map(lambda q: q.query(), queries)), raw_queries)
        for gen, raw in zipped:
            self.assertEquals(gen, raw)


class TestContinuousQueryPlanner(InfluxDBClientTest):

    def test_continuous_queries_sync(self):
        queries = ExampleContinuousQuery().continuous_queries()
        sync_continuous_queries(self.client, map(lambda q: q.query(), queries))

        response = self.client.query('list continuous queries')
        series = response[0]
        assert len(series['points']) == 15

        sync_continuous_queries(self.client, map(lambda q: q.query(), queries))

        response = self.client.query('list continuous queries')
        series = response[0]

        assert len(series['points']) == 15

    def test_for_interval(self):
        queries = ExampleContinuousQuery().continuous_queries()
        sync_continuous_queries(self.client, map(lambda q: q.query(), queries))

        self.generateData()
        q = indb.q('page_views').columns(category_id=indb.distinct('category_id'))
        resp = indb.get_result(q, conn=self.client)
        cat_id = list(resp['page_views'])[0].category_id

        q = ExampleContinuousQuery().query_for('category', category_id=cat_id)
        resp = indb.get_result(q, conn=self.client)
        assert len(list(resp['page_views.category.%s' % (cat_id)])[0]) > 0

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

        sleep(2)

        q = ExampleContinuousQuery().query_for('category', '1s', category_id=cat_id)
        resp = indb.get_result(q, conn=self.client)
        assert len(list(resp['1s.page_views.category.%s' % (cat_id)])) > 0
