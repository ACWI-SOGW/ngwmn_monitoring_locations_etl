"""
Tests for the transform.py module

"""
from unittest import TestCase

from .fake_data import TEST_DATA
from ..transform import transform_mon_loc_data


class TestTransformMonitoringLocationData(TestCase):

    def setUp(self):
        self.test_data = TEST_DATA

    def test_transform(self):
        result = transform_mon_loc_data(self.test_data)
        self.assertEqual(len(result.items()), 53)
        self.assertEqual(list(result.values()).count(None), 23)
