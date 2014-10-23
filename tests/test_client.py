from incursion.client import INDBClient
from incursion.query import InfluxQuery


from .utils import InfluxDBClientTest


class TestResponseParser(InfluxDBClientTest):

    def test_simple(self):
        self.generateData()

        query = InfluxQuery.for_series('page_views').limit(None)

        client = INDBClient(conn=self.client)

        resp = client.result_for_query(query)

        series = resp.get('page_views')

        self.assertEqual(len(series), len(self.points))

        query = query.limit(10)
        resp = client.result_for_query(query)

        series = resp.get('page_views')
        assert len(series) == 10

        query = InfluxQuery.for_series('page_views').columns(InfluxQuery.count('category_id')).limit(None)
        resp = client.result_for_query(query)
        series = resp.get('page_views')

        self.assertEqual(series[0].count, len(self.points))

    def test_groups(self):
        self.generateData()
        q = InfluxQuery.for_series('page_views').columns(InfluxQuery.count('category_id'))
        q = q.limit(None)
        q = q.group_by(InfluxQuery.time('1h'))
        client = INDBClient(conn=self.client)
        resp = client.result_for_query(q)
        series = resp.get('page_views')
        assert sum(map(lambda x: x.count, series)) == len(self.points)
