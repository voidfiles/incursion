__version__ = '0.4.0'

from .query import InfluxQuery as q, get_result
from .query import (count, min, max, mean, mode, median, distinct,
                    percentile, histogram, derivative, sum, stddev,
                    first, last, difference, top, bottom, time,
                    now_minus, time_value, regex)


__all__ = (q, get_result, count, min, max, mean, mode, median, distinct,
           percentile, histogram, derivative, sum, stddev,
           first, last, difference, top, bottom, time,
           now_minus, time_value, regex)
