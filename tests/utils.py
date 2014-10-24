from datetime import datetime, timedelta
import time
import unittest
import random
from six.moves import xrange

from influxdb import InfluxDBClient

from incursion.query import datetime_to_secs


def geneate_points(from_date, to_date, interval, amount):
    data = []
    now = from_date
    while now <= to_date:
        data += [[datetime_to_secs(now), random.randint(0, 10), random.randint(0, 10), 'x'] for x in xrange(0, amount)]
        now = now + interval

    return data


class InfluxDBClientTest(unittest.TestCase):
    def generateData(self):
        now = datetime.utcnow()
        then = now - timedelta(hours=2)
        self.points = geneate_points(then, now, timedelta(seconds=10), 5)

        test_data = [{
            "name": "page_views",
            "columns": ["time", "author_id", "category_id", "name"],
            "points": self.points
        }]
        self.client.write_points(data=test_data, batch_size=500)

    def setUp(self):
        self.client = InfluxDBClient('localhost', 8086, 'root', 'root')
        self.database_name = 'test_%s' % (time.time())
        self.client.create_database(self.database_name)
        self.client.switch_db(self.database_name)

    def tearDown(self):
        self.client.delete_database(self.database_name)
