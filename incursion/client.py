from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError

from .response import parse_influxdb_response


class INDBClient(object):
    def __init__(self, conn=None, **kwargs):
        if not conn:
            conn = InfluxDBClient(**kwargs)

        self.conn = conn

    def result_for_query(self, q):
        try:
            result = self.conn.query(q.query())
        except InfluxDBClientError:
            raise

        return parse_influxdb_response(result)
