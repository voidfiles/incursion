from .query import InfluxQuery


class ContinuousQueryException(Exception):
    pass


class InfluxDBContinuousQuery(object):
    series = None
    fanouts = {}
    downsample_interval = ['1m', '1h', '1d']

    def sync():
        # This will make sure the correct continuous queries are running
        pass

    def queries_for_sync(self):
        queries = []
        for fanout, sections in self.fanouts.iteritems():
            series_fanout_name = '%s.%s' % (self.series, fanout)
            queries.append(InfluxQuery.for_series(self.series).into(series_fanout_name))
            for interval in self.downsample_interval:
                q = InfluxQuery.for_series(self.series)
                q = q.group_by(InfluxQuery.time(interval))
                q = q.into('%s:series_name')
                queries.append(q)

    def query_for(self, fanout, interval, *kwargs):
        if fanout not in self.fanouts:
            raise InfluxDBContinuousQuery('Fanout %s is not in [%s]' % (fanout,
                                                                        ','.join(self.fanout.keys())))

        fanout_def = self.fanouts[fanout]

        fanout_sections = []
        for section in fanout_def:
            fanout_sections.append(kwargs.get(section))

        series_name = '%s.%s.%s.%s' % (interval, self.series_name,
                                       fanout, '.'.join(fanout_sections))

        return InfluxQuery.for_series(series_name)

# class ExampleContinuousQuery(InfluxDBContinuousQuery):
#     series = 'page_views'
#     fanouts = {
#         'category': ('category_id',),
#         'author': ('author_id',),
#         'category_author': ('category_id', 'author_id',),
#     }
#
# interval_query = ExampleContinuousQuery().query_for('author', '1m', author_id=10)
# interval_query == 'select sum(count) from 1m.page_views.author.10 limit 10'
# Queries For Sync
# select * from page_view into page_view.category.[category_id]
# select * from /page_view.category.*/ group by time(1m) into 1m:series_name
# select * from /page_view.category.*/ group by time(1h) into 1h:series_name
# select * from /page_view.category.*/ group by time(1d) into 1d:series_name

