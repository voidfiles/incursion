from collections import namedtuple

# [
#   {
#     "name": "log_lines",
#     "columns": ["time", "sequence_number", "line"],
#     "points": [
#       [1400425947368, 287780001, "here's some useful log info"]
#     ]
#   }
# ]


def parse_influxdb_response(data):
    parsed_data = {}
    for series in data:
        custom_named_tuple = namedtuple('Custom', series['columns'])
        parsed_data[series['name']] = map(lambda x: custom_named_tuple(*x), series['points'])

    return parsed_data
