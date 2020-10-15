"""
Tests for the extract.py module
"""
from unittest import TestCase, mock

from ..extract import get_monitoring_locations

from requests import Session


class TestGetMonitoringLocations(TestCase):
    class MockResponse:
        """
        Mocks the requests get response
        """
        def __init__(self, status_code):
            """
            Initialize MockResponse
            """
            self.status_code = status_code

        def json(self):
            return {'next': None, 'count': 2, 'results': ['dummyone', 'dummytwo']}

    def setUp(self):
        self.fake_endpoint = 'https://fake.usgs.gov/registry/monitoring-locations/'

    @mock.patch.object(Session, 'get', return_value=MockResponse(200))
    def test_get_data(self, _):
        self.assertEqual(get_monitoring_locations(self.fake_endpoint), ['dummyone', 'dummytwo'])
