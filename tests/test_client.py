import unittest
import time
from datetime import datetime, timedelta

from influxdb import InfluxDBClient
from incursion.client import INDBClient
from incursion.query import InfluxQuery, datetime_to_secs


def geneate_points(from_date, to_date, interval, amount):
    data = []
    now = from_date
    while now <= to_date:
        data += [[datetime_to_secs(now), 'value'] for x in xrange(0, amount)]
        now = now + interval

    return data


@unittest.skip("skipping untill we get influxdb on travis ci")
class TestResponseParser(unittest.TestCase):
    def setUp(self):
        self.client = InfluxDBClient('127.0.0.1', 8086, 'root', 'root')
        self.database_name = 'test_%s' % (time.time())
        self.client.create_database(self.database_name)
        self.client.switch_db(self.database_name)
        now = datetime.utcnow()
        then = now - timedelta(hours=4)
        self.points = geneate_points(then, now, timedelta(seconds=10), 10)

        test_data = [{
            "name": "test",
            "columns": ["time", "key"],
            "points": self.points
        }]
        self.client.write_points(data=test_data, batch_size=2000)

    def tearDown(self):
        self.client.delete_database(self.database_name)

    def test_simple(self):
        query = InfluxQuery.for_series('test').limit(None)

        client = INDBClient(conn=self.client)

        resp = client.result_for_query(query)

        series = resp.get('test')

        self.assertEqual(len(series), len(self.points))

        query = query.limit(10)
        resp = client.result_for_query(query)

        series = resp.get('test')
        assert len(series) == 10

        query = InfluxQuery.for_series('test').columns(InfluxQuery.count('key')).limit(None)
        resp = client.result_for_query(query)
        series = resp.get('test')

        self.assertEqual(series[0].count, len(self.points))

    def test_groups(self):
        q = InfluxQuery.for_series('test').columns(InfluxQuery.count('key'))
        q = q.limit(None)
        q = q.group_by(InfluxQuery.time('1h'))
        client = INDBClient(conn=self.client)
        resp = client.result_for_query(q)
        series = resp.get('test')
        assert sum(map(lambda x: x.count, series)) == len(self.points)
