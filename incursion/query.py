import datetime
import calendar
from collections import namedtuple

VALID_OPS = {
    'matches': u'=~',
    'not_matches': u'!~',
    'gt': u'>',
    'lt': u'<',
    'ne': u'<>',
    'eq': u'=',
}


class RegexString(bytes):
    pass


def parse_time(value):
    if type(value) in (datetime.datetime, datetime.date):
        return u'%ss' % datetime_to_secs(value)

    return value


class TimeString(object):
    def __init__(self, value):
        self.value = value

    def render(self):
        return parse_time(self.value)


def is_regex(reg_str):
    if not reg_str:
        return False

    return isinstance(reg_str, RegexString)


def is_time(value):
    return isinstance(value, TimeString)


def datetime_to_secs(dt):
    # http://stackoverflow.com/questions/2956886/python-calendar-timegm-vs-time-mktime
    return int(calendar.timegm(dt.utctimetuple()))


class WhereClause(namedtuple('WhereClause', 'column op comparison')):

    def render(self):
        litteral_op = VALID_OPS[self.op]
        comparison = self.comparison

        if is_time(comparison):
            comparison = comparison.render()

        elif isinstance(comparison, basestring) and not is_regex(comparison):
            comparison = "'%s'" % (comparison)

        return u'%s %s %s' % (self.column, litteral_op, comparison)


class InfluxQueryException(Exception):
    pass


class InfluxQuery(object):

    def __init__(self, series):
        self.series = series
        self.where_clauses = []
        self.group_by_clauses = []
        self.fill_value = None
        self.fill_column = None
        self.limit_clause = 100
        self.column_clauses = []
        self.into_pattern = None

    @classmethod
    def for_series(cls, series):
        return cls(series)

    @staticmethod
    def count(column):
        return 'count(%s)' % (column)

    @staticmethod
    def min(column):
        return 'min(%s)' % (column)

    @staticmethod
    def max(column):
        return 'max(%s)' % (column)

    @staticmethod
    def mean(column):
        return 'mean(%s)' % (column)

    @staticmethod
    def mode(column):
        return 'mode(%s)' % (column)

    @staticmethod
    def median(column):
        return 'median(%s)' % (column)

    @staticmethod
    def distinct(column):
        return 'distinct(%s)' % (column)

    @staticmethod
    def percentile(column, nth):
        return 'percentile(%s, %s)' % (column, nth)

    @staticmethod
    def histogram(column, bucket_size=1.0):
        return 'histogram(%s, %s)' % (column, bucket_size)

    @staticmethod
    def derivative(column):
        return 'derivative(%s)' % (column)

    @staticmethod
    def sum(column):
        return 'sum(%s)' % (column)

    @staticmethod
    def stddev(column):
        return 'stddev(%s)' % (column)

    @staticmethod
    def first(column):
        return 'first(%s)' % (column)

    @staticmethod
    def last(column):
        return 'last(%s)' % (column)

    @staticmethod
    def difference(column):
        return 'difference(%s)' % (column)

    @staticmethod
    def top(column, n):
        return 'top(%s, %s)' % (column, n)

    @staticmethod
    def bottom(column, n):
        return 'bottom(%s, %s)' % (column, n)

    @staticmethod
    def time(time_expression):
        return 'time(%s)' % (time_expression)

    @staticmethod
    def now_minus(time_expression):
        return TimeString('now() - %s' % (time_expression))

    @staticmethod
    def time_value(time_expression):
        return TimeString(time_expression)

    @staticmethod
    def regex(regex_str):
        return RegexString(regex_str)

    def columns(self, *args, **kwargs):
        for arg in args:
            column_def = (arg,)
            if column_def not in self.column_clauses:
                self.column_clauses.append(column_def)

        for alias, column in kwargs.iteritems():
            if callable(column):
                column_expression = column()
            else:
                column_expression = column

            column_def = (column_expression, alias)
            if column_def not in self.column_clauses:
                self.column_clauses.append(column_def)

        return self

    def where(self, **kwargs):
        for column, val in kwargs.iteritems():
            op = u'eq'
            if '__' in column:
                column, op = column.split('__')
                if op not in VALID_OPS.keys():
                    raise InfluxQueryException(u'%s not a valid operation', op)

            clause = WhereClause(column, op, val)

            if clause not in self.where_clauses:
                self.where_clauses.append(clause)

        return self

    def group_by(self, *args):
        for arg in args:
            if callable(arg):
                arg = arg()

            if arg not in self.group_by_clauses:
                self.group_by_clauses.append(arg)

        return self

    def fill(self, default, col=None):
        self.fill_value = default
        if col:
            self.fill_column = col

        return self

    def limit(self, limit):
        self.limit_clause = limit

        return self

    def into(self, pattern):
        self.into_pattern = pattern

        return self

    def _query_columns(self):
        if not self.column_clauses:
            return u'* '

        columns = []
        for col_def in self.column_clauses:
            if len(col_def) == 1:
                columns += [col_def[0]]
            else:
                columns += [u'%s as %s' % col_def]

        return u', '.join(columns) + u' '

    def _query_where(self):
        return u' and '.join(map(lambda x: x.render(), self.where_clauses)) + u' '

    def _query_group(self):
        return u', '.join(self.group_by_clauses) + u' '

    def _query_series(self):
        series = self.series
        if not isinstance(self.series, RegexString):
            series = '"%s"' % (series)

        return u'from %s ' % (series)

    def query(self):
        query = u'select '
        query += self._query_columns()
        query += self._query_series()
        if self.where_clauses:
            query += u'where '
            query += self._query_where()

        if self.group_by_clauses:
            query += u'group by '
            query += self._query_group()

            if self.fill_column:
                query += u', %s ' % (self.fill_column)

            if self.fill_value is not None:
                query += u'fill(%s) ' % (self.fill_value)

        if self.limit_clause is not None:
            query += 'limit %d ' % (self.limit_clause)

        if self.into_pattern:
            query += 'into %s ' % (self.into_pattern)

        return query.strip()
