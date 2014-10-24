import unittest

from incursion.response import parse_influxdb_response


class TestResponseParser(unittest.TestCase):

    def test_simple(self):
        example_response = [{
            "name": "log_lines",
            "columns": ["time", "sequence_number", "line"],
            "points": [
                [1400425947368, 287780001, "here's some useful log info"],
                [1400425947368, 287780002, "here's some useful log info 2"],
            ]
        }]

        parsed_response = parse_influxdb_response(example_response)

        assert parsed_response.get('log_lines') is not None

        example_series = list(parsed_response.get('log_lines'))

        assert len(example_series) == 2

        row = example_series[0]

        assert len(row) == 3

        assert hasattr(row, 'time')

        assert getattr(row, 'd', None) is None
