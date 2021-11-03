"""
Tests for the extract.py module
"""

# unit testing modules
from unittest import TestCase, mock
import mockito, logging
from mockito import when, mock as mocki

# modules referenced during testing
from json import JSONDecodeError
from requests import Response, Session, HTTPError, RequestException

# file under test
from .. import extract


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
        logging.getLogger().setLevel(level=logging.INFO)
        extract.FETCH_RETRY_DELAY = 0
        self.fake_endpoint = 'https://fake.usgs.gov/registry/monitoring-locations/'
        self.mock_json_payload = {'good': 'json'}
        self.mock_session = mocki(Session)
        when(self.mock_session).__enter__().thenReturn(self.mock_session)
        when(self.mock_session).__exit__(*mockito.args)

        # to be called for testing get_monitoring_locations because it uses a with-block
        # mockito.verifyStubbedInvocationsAreUsed()

    @mock.patch.object(Session, 'get', return_value=MockResponse(200))
    def test_get_data(self, _):
        self.assertEqual(extract.get_monitoring_locations(self.fake_endpoint), ['dummyone', 'dummytwo'])

    @mock.patch('requests.session')
    def testing_mocking_session(self, mock_session):
        response1 = mock_session.get('some url')
        response2 = mock_session.get('some url')
        self.assertEqual(2, mock_session.get.call_count)
        self.assertTrue(response1 is response2)

    def testing_mockito_session_ab(self):
        url_a = 'http://fake.mon-loc.net/a'
        url_b = 'http://fake.mon-loc.net/b'
        mock_response_a = mocki({'text': 'response test text A', 'status_code': 200}, spec=Response)
        mock_response_b = mocki({'text': 'response test text B', 'status_code': 204}, spec=Response)
        when(self.mock_session).get(url_a).thenReturn(mock_response_a)
        when(self.mock_session).get(url_b).thenReturn(mock_response_b)

        response1 = self.mock_session.get(url_a)
        response2 = self.mock_session.get(url_a)
        response3 = self.mock_session.get(url_b)

        self.assertTrue(response1 is response2)
        self.assertFalse(response1 is response3)

        self.assertEqual(200, response1.status_code)
        self.assertEqual(204, response3.status_code)

    def testing_mockito_session_thenReturn(self):
        mock_response_a = mocki({'text': 'response test text A', 'status_code': 200}, spec=Response)
        mock_response_b = mocki({'text': 'response test text B', 'status_code': 204}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a).thenReturn(mock_response_b)

        response1 = self.mock_session.get(self.fake_endpoint)
        response2 = self.mock_session.get(self.fake_endpoint)

        self.assertFalse(response1 is response2)

        self.assertEqual(200, response1.status_code)
        self.assertEqual(204, response2.status_code)

    def test_fetch_record_block_2_bad_json(self):
        # this test ensures that the retries works and that if the JSON is bad that it only tries TWICE
        self.assertEqual(extract.FETCH_TRIES_FOR_JSON, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON twice', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError('', '', 0))

        self.assertRaises(JSONDecodeError, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_JSON).get(self.fake_endpoint)

    def test_fetch_record_block_3_bad_json(self):
        # this test ensures that the retries works and that if the JSON is bad that it tries THREE times
        self.assertEqual(extract.FETCH_TRIES_FOR_JSON, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON thrice', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError('', '', 0))

        extract.FETCH_TRIES_FOR_JSON = 3
        self.assertRaises(JSONDecodeError, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_JSON).get(self.fake_endpoint)

    def test_fetch_record_block_1_bad_json(self):
        # this test ensures that the retries works and that if the JSON is bad the first time that it can succeed after
        self.assertEqual(extract.FETCH_TRIES_FOR_JSON, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError('', '', 0)).thenReturn(self.mock_json_payload)

        json_response = extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_1_bad_status(self):
        # this test ensures that the retries works and that if the STATUS CODE is bad the first time that it can succeed after
        self.assertEqual(extract.FETCH_TRIES_FOR_STATUS_CODE, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 300}, spec=Response)
        mock_response_b = mocki({'text': 'place holder', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a).thenReturn(mock_response_b)
        when(mock_response_a).raise_for_status().thenRaise(HTTPError('http error msg', response=mock_response_a))
        when(mock_response_b).raise_for_status().thenRaise(HTTPError("shouldn't call this", response=mock_response_b))
        when(mock_response_a).json().thenRaise(JSONDecodeError("shouldn't call this", 'place holder', 0))
        when(mock_response_b).json().thenReturn(self.mock_json_payload)

        json_response = extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_2_bad_status(self):
        # this test ensures that the retries works and that if the STATUS CODE is bad that it only tries TWICE
        self.assertEqual(extract.FETCH_TRIES_FOR_STATUS_CODE, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad status code twice', 'status_code': 300}, spec=Response)
        when(mock_response_a).raise_for_status().thenRaise(HTTPError('http error msg', response=mock_response_a))
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError("shouldn't call this", 'place holder', 0))

        # the HTTPError is captured by the tries and reported as a the parent RequestException indicating non-json error
        self.assertRaises(RequestException, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_STATUS_CODE).get(self.fake_endpoint)

    def test_fetch_record_block_3_bad_status(self):
        # this test ensures that the retries works and that if the STATUS CODE is bad that it only tries THRICE
        self.assertEqual(extract.FETCH_TRIES_FOR_STATUS_CODE, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad status code twice', 'status_code': 300}, spec=Response)
        when(mock_response_a).raise_for_status().thenRaise(HTTPError('http error msg', response=mock_response_a))
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError("shouldn't call this", 'place holder', 0))

        extract.FETCH_TRIES_FOR_STATUS_CODE = 3
        # the HTTPError is captured by the tries and reported as a the parent RequestException indicating non-json error
        self.assertRaises(RequestException, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_STATUS_CODE).get(self.fake_endpoint)

    def test_fetch_record_block_1_bad_network(self):
        # this test ensures that the retries works and that if the NETWORK is bad the first time and succeeds after
        self.assertEqual(extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'place holder', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint)\
            .thenRaise(RequestException(response=mock_response_a)).thenReturn(mock_response_a)
        when(mock_response_a).json().thenReturn(self.mock_json_payload)

        json_response = extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_2_bad_network(self):
        # this test ensures that the retries works and that if the NETWORK is bad that it only tries TWICE
        self.assertEqual(extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenRaise(RequestException(response=mock_response_a))\

        self.assertRaises(RequestException, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)

    def test_fetch_record_block_3_bad_network(self):
        # this test ensures that the retries works and that if the NETWORK is bad that it only tries THRICE
        self.assertEqual(extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenRaise(RequestException(response=mock_response_a))\

        extract.FETCH_TRIES_FOR_NETWORK_ERROR = 3
        self.assertRaises(RequestException, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)

    def test_fetch_record_block_1_none_response(self):
        # this test ensures that the retries works and that if the response is None the first time and succeeds after
        self.assertEqual(extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'place holder', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(None).thenReturn(mock_response_a)
        when(mock_response_a).json().thenReturn(self.mock_json_payload)

        json_response = extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_2_none_response(self):
        # this test ensures that the retries works and that if the response is None the first time and succeeds after
        self.assertEqual(extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        when(self.mock_session).get(self.fake_endpoint).thenReturn(None)

        self.assertRaises(RequestException, lambda: extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)

    def test_fetch_record_block_3_none_response(self):
        # this test ensures that the retries works and that if the response is None the first time and succeeds after
        self.assertEqual(extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        when(self.mock_session).get(self.fake_endpoint).thenReturn(None)

        extract.FETCH_TRIES_FOR_NETWORK_ERROR = 3
        try:
            extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        except RequestException:
            mockito.verify(self.mock_session, times=extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)
            return
        self.fail('expected RequestException')
