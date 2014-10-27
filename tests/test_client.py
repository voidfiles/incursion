import incursion as indb


from .utils import InfluxDBClientTest


class TestResponseParser(InfluxDBClientTest):

    def test_simple(self):
        self.generateData()

        query = indb.q('page_views').limit(None)

        resp = indb.get_result(query, conn=self.client)

        series = resp.get('page_views')

        self.assertEqual(len(list(series)), len(self.points))

        query = query.limit(10)
        resp = indb.get_result(query, conn=self.client)

        series = resp.get('page_views')
        assert len(list(series)) == 10

        query = indb.q('page_views').columns(indb.count('category_id')).limit(None)
        resp = indb.get_result(query, conn=self.client)
        series = resp.get('page_views')

        self.assertEqual(list(series)[0].count, len(self.points))

    def test_groups(self):
        self.generateData()
        q = indb.q('page_views').columns(indb.count('category_id'))
        q = q.limit(None)
        q = q.group_by(indb.time('1h'))
        resp = indb.get_result(q, conn=self.client)
        series = resp.get('page_views')
        assert sum(map(lambda x: x.count, series)) == len(self.points)
