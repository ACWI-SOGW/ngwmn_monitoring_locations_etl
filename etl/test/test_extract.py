"""
Tests for the extract.py module
"""
from unittest import TestCase, mock

from ..extract import get_monitoring_locations


class TestGetMonitoringLocations(TestCase):

    def setUp(self):
        self.fake_endpoint = 'https://fake.usgs.gov/registry/monitoring-locations/'

    @mock.patch('etl.extract.r')
    def test_get_data(self, mock_request):
        get_monitoring_locations(self.fake_endpoint)
        mock_request.get.assert_called_with(self.fake_endpoint)
        mock_request.get.return_value.json.assert_called()
