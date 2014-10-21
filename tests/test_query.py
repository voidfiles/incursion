import unittest

from incursion.query import InfluxQuery


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.query = InfluxQuery.for_series('test')

    def test_simple(self):
        self.assertEqual('select * from test limit 100', self.query.query())

    def test_limit(self):
        q = self.query

        q.limit(1000)
        self.assertEqual('select * from test limit 1000', q.query())

        q.limit(0)
        self.assertEqual('select * from test limit 0', q.query())

        q.limit(None)
        self.assertEqual('select * from test', q.query())

    def test_columns(self):
        q = self.query

        q.columns('time')
        self.assertEqual('select time from test limit 100', q.query())

        q.columns('time', 'id')
        self.assertEqual('select time, id from test limit 100', q.query())

        # Test doubles
        q.columns('time', 'id')
        self.assertEqual('select time, id from test limit 100', q.query())

        # Reset the columns
        q.column_clauses = []

        q.columns('id', t='time')
        self.assertEqual('select id, time as t from test limit 100', q.query())

        # Test complex doubles
        q.columns('id', t='time')
        self.assertEqual('select id, time as t from test limit 100', q.query())

    def test_where(self):
        from datetime import datetime
        q = self.query

        q.where(time=10)
        self.assertEqual('select * from test where time = 10 limit 100', q.query())

        # Test doubles
        q.where(time=10)
        self.assertEqual('select * from test where time = 10 limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time=10)
        self.assertEqual('select * from test where time = 10 limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time__gt=10)
        self.assertEqual('select * from test where time > 10 limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time__lt=10)
        self.assertEqual('select * from test where time < 10 limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time__ne=10)
        self.assertEqual('select * from test where time <> 10 limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time__matches=InfluxQuery.regex('/a.*/'))
        self.assertEqual('select * from test where time =~ /a.*/ limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time__not_matches=InfluxQuery.regex('/a.*/'))
        self.assertEqual('select * from test where time !~ /a.*/ limit 100', q.query())

        dt = datetime(2011, 2, 11, 20, 0, 0, 0)

        q.where_clauses = []  # Reset
        q.where(time__gt=InfluxQuery.time_value(dt))
        self.assertEqual('select * from test where time > 1297454400s limit 100', q.query())

        q.where_clauses = []  # Reset
        q.where(time__lt=InfluxQuery.time_value(dt))
        self.assertEqual('select * from test where time < 1297454400s limit 100', q.query())

    def test_group_by(self):
        q = self.query

        q.group_by(InfluxQuery.time('10m'))
        self.assertEqual('select * from test group by time(10m) limit 100', q.query())

        # test doubles
        q.group_by(InfluxQuery.time('10m'))
        self.assertEqual('select * from test group by time(10m) limit 100', q.query())

        q.group_by(InfluxQuery.time('10m'), 'test')
        self.assertEqual('select * from test group by time(10m), test limit 100', q.query())

    def test_fill(self):
        q = self.query

        q.group_by(InfluxQuery.time('10m'))
        q.fill(0)
        self.assertEqual('select * from test group by time(10m) fill(0) limit 100', q.query())

        q.fill('null')
        self.assertEqual('select * from test group by time(10m) fill(null) limit 100', q.query())

        q.fill('0', 'test')
        self.assertEqual('select * from test group by time(10m) , test fill(0) limit 100', q.query())

    def test_aggregations(self):
        q = self.query

        q.columns(InfluxQuery.count('time'))
        self.assertEqual('select count(time) from test limit 100', q.query())

        # Test doubles
        q.columns(InfluxQuery.count('time'))
        self.assertEqual('select count(time) from test limit 100', q.query())

        # Reset the columns
        q.column_clauses = []



        # Test embedded
        q.columns(InfluxQuery.count(InfluxQuery.distinct('time')))
        self.assertEqual('select count(distinct(time)) from test limit 100', q.query())

        simple_methods = ['min', 'max', 'mean', 'mode', 'median', 'distinct',
                          'derivative', 'sum', 'stddev', 'first', 'last']

        for method in simple_methods:
            # Reset the columns
            q.column_clauses = []
            q.columns(getattr(InfluxQuery, method)('time'))
            query = 'select %s(time) from test limit 100' % (method)

            self.assertEqual(query, q.query())

        q.column_clauses = []
        q.columns(InfluxQuery.percentile('time', 2))
        self.assertEqual('select percentile(time, 2) from test limit 100', q.query())

        q.column_clauses = []
        q.columns(InfluxQuery.histogram('time'))
        self.assertEqual('select histogram(time, 1.0) from test limit 100', q.query())

        q.column_clauses = []
        q.columns(InfluxQuery.histogram('time', 1.01))
        self.assertEqual('select histogram(time, 1.01) from test limit 100', q.query())

        q.column_clauses = []
        q.columns(InfluxQuery.top('time', 10))
        self.assertEqual('select top(time, 10) from test limit 100', q.query())

        q.column_clauses = []
        q.columns(InfluxQuery.bottom('time', 20))
        self.assertEqual('select bottom(time, 20) from test limit 100', q.query())

