from collections import namedtuple, OrderedDict
import six

import incursion as indb

FanoutQuery = namedtuple('FanoutQuery', 'fanout_on columns_to_copy')


class ContinuousQueryException(Exception):
    pass


class InfluxDBContinuousQuery(object):
    """
    Manage series that have multiple fanout queries and/or downsamples.
    """
    series = None
    column_to_count = None
    fanouts = OrderedDict()
    downsample_interval = ['1m', '1h', '1d']

    def continuous_queries(self):
        queries = []
        for interval in self.downsample_interval:
            q = indb.q(self.series)
            q = q.into('%s.:series_name' % (interval)).limit(None)
            q = q.group_by(indb.time(interval))
            q = q.columns(indb.count(self.column_to_count))
            queries.append(q)

        for fanout, fanout_query in six.iteritems(self.fanouts):
            series_fanout_name_base = '%s.%s' % (self.series, fanout)
            sections_for_fanout = '.'.join(map(lambda x: '[%s]' % x, fanout_query.fanout_on))
            series_fanout_name = '%s.%s' % (series_fanout_name_base, sections_for_fanout)

            q = indb.q(self.series)
            q = q.into(series_fanout_name).limit(None)
            q = q.columns(*fanout_query.columns_to_copy)
            queries.append(q)
            for interval in self.downsample_interval:
                q = indb.q(indb.regex('/^%s.*/' % (series_fanout_name_base)))
                q = q.group_by(indb.time(interval))
                q = q.into('%s.:series_name' % (interval)).limit(None)
                q = q.columns(indb.count(self.column_to_count))
                queries.append(q)

        return queries

    def query_for(self, fanout, interval=None, **kwargs):
        if fanout not in self.fanouts:
            raise ContinuousQueryException('Fanout %s is not in [%s]' % (fanout,
                                                                         ','.join(self.fanout.keys())))

        fanout_def = self.fanouts[fanout]

        fanout_sections = []
        for section in fanout_def.fanout_on:
            fanout_sections.append(kwargs.get(section))

        fanout_sections = map(six.text_type, fanout_sections)
        series_name = '%s.%s.%s' % (self.series, fanout,
                                    '.'.join(fanout_sections))

        if interval:
            series_name = '%s.%s' % (interval, series_name)

        q = indb.q(series_name)

        if interval:
            q = q.columns(indb.sum('count'))

        return q


def sync_continuous_queries(client, queries, recreate=False):
    response = client.query('list continuous queries')
    series = response[0]

    id_by_query = {}
    for p in series['points']:
        id_by_query[p[2]] = p[1]

    for q in queries:
        id_ = id_by_query.get(q)
        if id_:
            if recreate:
                client.query('drop continuous query %s' % (id_))
            continue

        client.query(q)
