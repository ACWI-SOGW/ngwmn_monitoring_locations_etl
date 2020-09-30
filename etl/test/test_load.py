"""
Tests for the load.py module

"""
from unittest import TestCase, mock

from .fake_data import TEST_DATA
from ..transform import transform_mon_loc_data
from ..load import load_monitoring_location, refresh_well_registry_mv


class TestLoadMonitoringLocation(TestCase):

    def setUp(self):
        self.test_user = 'stealthy_squid'
        self.test_password = 'barnacles'
        self.test_connect_str = 'fakedb.usgs.gov:1521/unterseeboot'
        self.test_data = transform_mon_loc_data(TEST_DATA)

    @mock.patch('etl.load.cx_Oracle', autospec=True)
    def test_load_monitoring_location(self, mock_ora):
        mock_cursor = mock.MagicMock()
        mock_cursor.execute.return_value = mock.Mock()

        mock_client = mock.MagicMock()
        mock_client.cursor.return_value = mock_cursor
        # override the context manager's `__enter__` method to make sure the mock object is returned
        mock_client.__enter__.return_value = mock_client
        mock_ora.connect.return_value = mock_client

        load_monitoring_location(self.test_user, self.test_password, self.test_connect_str, self.test_data)

        mock_ora.connect.assert_called()
        mock_client.cursor.assert_called()
        mock_cursor.execute.assert_called()
        mock_client.commit.assert_called()


class TestRefreshWellRegistryMV(TestCase):

    def setUp(self):
        self.test_user = 'sticky_limpet'
        self.test_password = 'clownfish'
        self.test_connect_str = 'fakedb.usgs.gov:1521/catamaran'

    @mock.patch('etl.load.cx_Oracle', autospec=True)
    def test_refresh(self, mock_ora):
        mock_cursor = mock.MagicMock()
        mock_cursor.execute.return_value = mock.Mock()

        mock_client = mock.MagicMock()
        mock_client.cursor.return_value = mock_cursor
        # override the context manager's `__enter__` method to make sure the mock object is returned
        mock_client.__enter__.return_value = mock_client
        mock_ora.connect.return_value = mock_client

        refresh_well_registry_mv(self.test_user, self.test_password, self.test_connect_str)

        mock_ora.connect.assert_called()
        mock_client.cursor.assert_called()
        mock_cursor.execute.assert_called()