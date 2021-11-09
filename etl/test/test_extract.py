"""
Tests for the extract.py module
"""

# unit testing modules
from unittest import TestCase, mock
from mockito import when, mock as mocki
# modules referenced during testing
from json import JSONDecodeError
from requests import Response, Session, HTTPError, RequestException
import logging
import mockito
import copy

# file under test
from ..extract import Extract


class MockExtract(Extract):
    """
    Mocks the Extract for IoC of mocked entities.
    """
    def __init__(self, mock_session):
        """
        Initialize MockExtract.
        """
        Extract.__init__(self)  # must call super constructor
        self.mock_session = mock_session
        self.FETCH_RETRY_DELAY = 0

    def session(self):
        return self.mock_session


class MockResponse:
    """
    Mocks the requests get response.
    """
    def __init__(self, status_code):
        """
        Initialize MockResponse.
        """
        self.status_code = status_code

    # noinspection PyMethodMayBeStatic
    # pylint: disable=too-few-public-methods, no-member
    def json(self):
        return {'next': None, 'count': 2, 'results': ['dummyone', 'dummytwo']}


class TestGetMonitoringLocations(TestCase):

    def setUp(self):
        logging.getLogger().setLevel(level=logging.INFO)
        self.fake_endpoint = 'https://fake.usgs.gov/registry/monitoring-locations/'
        self.mock_json_payload = {'good': 'json'}
        self.mock_json_good = '{"good":"json"}'
        self.mock_json_bad = '{"good"= json"}'
        self.mock_payload = {'results': [{'good': 'json'}], 'next': ''}  # tests that use should set 'next'
        self.mock_session = mocki(Session)
        when(self.mock_session).__enter__().thenReturn(self.mock_session)
        when(self.mock_session).__exit__(*mockito.args)
        print()  # to separate the log blocks per test
        self.extract = MockExtract(self.mock_session)

        # to be called for testing get_monitoring_locations because it uses a with-block
        # mockito.verifyStubbedInvocationsAreUsed()

    @mock.patch.object(Session, 'get', return_value=MockResponse(200))
    def test_get_data(self, _):
        self.assertEqual(Extract().get_monitoring_locations(self.fake_endpoint), ['dummyone', 'dummytwo'])

    @mock.patch('requests.session')
    def testing_mocking_session(self, mock_session):
        # to understand how unittest mock works -- not intuitive to me
        response1 = mock_session.get('some url')
        response2 = mock_session.get('some url')
        self.assertEqual(2, mock_session.get.call_count)
        self.assertTrue(response1 is response2)

    def testing_mockito_session_ab(self):
        # to understand how mockito mocking works -- intuitive to me. I use it in Java
        url_a = 'https://fake.mon-loc.net/a'
        url_b = 'https://fake.mon-loc.net/b'
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
        # to understand how mockito mocking works -- intuitive to me. I use it in Java
        mock_response_a = mocki({'text': 'response test text A', 'status_code': 200}, spec=Response)
        mock_response_b = mocki({'text': 'response test text B', 'status_code': 204}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a).thenReturn(mock_response_b)

        response1 = self.mock_session.get(self.fake_endpoint)
        response2 = self.mock_session.get(self.fake_endpoint)

        self.assertFalse(response1 is response2)

        self.assertEqual(200, response1.status_code)
        self.assertEqual(204, response2.status_code)

    def test_construct_url(self):
        url = self.extract.construct_url(self.fake_endpoint)
        self.assertEqual(self.fake_endpoint + '?limit=8&offset=0', url)
        url = self.extract.construct_url(url)
        self.assertEqual(self.fake_endpoint + '?limit=8&offset=8', url)
        url = self.extract.construct_url(url)
        self.assertEqual(self.fake_endpoint + '?limit=8&offset=16', url)
        for i in range(14):
            url = self.extract.construct_url(url)
        self.assertEqual(self.fake_endpoint + '?limit=8&offset=128', url)

    def test_get_monitoring_locations_3_success(self):
        # with mock.patch.object(Session, 'get', return_value=self.MockResponse(200)):
        #     pass
        fake_first_url = self.fake_endpoint + '?limit=8&offset=0'

        mock_response_a = mocki({'text': self.mock_json_good, 'status_code': 200}, spec=Response)
        mock_payload_a = copy.deepcopy(self.mock_payload)
        mock_payload_a['next'] = self.fake_endpoint + '?limit=8&offset=8'
        when(mock_response_a).json().thenReturn(mock_payload_a)

        mock_response_b = mocki({'text': self.mock_json_good, 'status_code': 200}, spec=Response)
        mock_payload_b = copy.deepcopy(self.mock_payload)
        mock_payload_b['next'] = self.fake_endpoint + '?limit=8&offset=16'
        when(mock_response_b).json().thenReturn(mock_payload_b)

        mock_response_c = mocki({'text': self.mock_json_good, 'status_code': 200}, spec=Response)
        mock_payload_c = copy.deepcopy(self.mock_payload)
        # mock_payload_c['next'] = None  # this is the mock default value. It requires setting if the default changes.
        when(mock_response_c).json().thenReturn(mock_payload_c)

        when(self.mock_session).get(fake_first_url).thenReturn(mock_response_a)
        when(self.mock_session).get(mock_payload_a['next']).thenReturn(mock_response_b)
        when(self.mock_session).get(mock_payload_b['next']).thenReturn(mock_response_c)

        extract = MockExtract(self.mock_session)
        records = extract.get_monitoring_locations(self.fake_endpoint)
        # the base URL is not called in this test. Extract.construct_url() adds the fetch limit in lieu of the default.
        mockito.verify(self.mock_session, times=0).get(self.fake_endpoint)
        # each of the batch request URLs is called once. One is the default but I like explicit indication.
        mockito.verify(self.mock_session, times=1).get(extract.construct_url(self.fake_endpoint))
        mockito.verify(self.mock_session, times=1).get(mock_payload_a['next'])
        mockito.verify(self.mock_session, times=1).get(mock_payload_b['next'])

        self.assertEqual(3, len(records), 'there should be three mock records returned')

    def test_get_monitoring_locations_3_success_with_json_parse_error(self):
        fake_first_url = self.fake_endpoint + '?limit=8&offset=0'

        mock_response_a = mocki({'text': self.mock_json_good, 'status_code': 200}, spec=Response)
        mock_payload_a = copy.deepcopy(self.mock_payload)
        mock_payload_a['next'] = self.fake_endpoint + '?limit=8&offset=8'
        when(mock_response_a).json().thenReturn(mock_payload_a)

        mock_response_b = mocki({'text': self.mock_json_good, 'status_code': 200}, spec=Response)
        mock_payload_b = copy.deepcopy(self.mock_payload)
        mock_payload_b['next'] = self.fake_endpoint + '?limit=8&offset=16'
        when(mock_response_b).json().thenRaise(JSONDecodeError('Bad JSON', 'Bad DOC', 0))

        mock_response_c = mocki({'text': self.mock_json_good, 'status_code': 200}, spec=Response)
        mock_payload_c = copy.deepcopy(self.mock_payload)
        # mock_payload_c['next'] = None  # this is the mock default value. It requires setting if the default changes.
        when(mock_response_c).json().thenReturn(mock_payload_c)

        when(self.mock_session).get(fake_first_url).thenReturn(mock_response_a)
        when(self.mock_session).get(mock_payload_a['next']).thenReturn(mock_response_b)
        when(self.mock_session).get(mock_payload_b['next']).thenReturn(mock_response_c)

        extract = MockExtract(self.mock_session)
        records = extract.get_monitoring_locations(self.fake_endpoint)
        # the base URL is not called in this test. Extract.construct_url() adds the fetch limit in lieu of the default.
        mockito.verify(self.mock_session, times=0).get(self.fake_endpoint)
        # each of the batch request URLs is called once. One is the default but I like explicit indication.
        mockito.verify(self.mock_session, times=1).get(extract.construct_url(self.fake_endpoint))
        # this one will fail by mock conditions after called twice
        mockito.verify(self.mock_session, times=2).get(mock_payload_a['next'])
        mockito.verify(self.mock_session, times=1).get(mock_payload_b['next'])

        self.assertEqual(2, len(records), 'there should be two mock records returned because of the middle fail')

    def test_fetch_record_block_2_bad_json(self):
        # ensure that the retries works and that if the JSON is bad that it only tries TWICE
        self.assertEqual(self.extract.FETCH_TRIES_FOR_JSON, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON twice', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError('', '', 0))

        self.assertRaises(JSONDecodeError,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_JSON).get(self.fake_endpoint)

    def test_fetch_record_block_3_bad_json(self):
        # ensure that the retries works and that if the JSON is bad that it tries THREE times
        self.assertEqual(self.extract.FETCH_TRIES_FOR_JSON, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON thrice', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError('', '', 0))

        self.extract.FETCH_TRIES_FOR_JSON = 3
        self.assertRaises(JSONDecodeError,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_JSON).get(self.fake_endpoint)

    def test_fetch_record_block_1_bad_json(self):
        # ensure that the retries works and that if the JSON is bad the first time that it can succeed after
        self.assertEqual(self.extract.FETCH_TRIES_FOR_JSON, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError('', '', 0)).thenReturn(self.mock_json_payload)

        json_response = self.extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_1_bad_status(self):
        # ensure that the retries works and that if the STATUS CODE is bad the first time that it can succeed after
        self.assertEqual(self.extract.FETCH_TRIES_FOR_STATUS_CODE, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 300}, spec=Response)
        mock_response_b = mocki({'text': 'place holder', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a).thenReturn(mock_response_b)
        when(mock_response_a).raise_for_status().thenRaise(HTTPError('http error msg', response=mock_response_a))
        when(mock_response_b).raise_for_status().thenRaise(HTTPError("shouldn't call this", response=mock_response_b))
        when(mock_response_a).json().thenRaise(JSONDecodeError("shouldn't call this", 'place holder', 0))
        when(mock_response_b).json().thenReturn(self.mock_json_payload)

        json_response = self.extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_2_bad_status(self):
        # ensure that the retries works and that if the STATUS CODE is bad that it only tries TWICE
        self.assertEqual(self.extract.FETCH_TRIES_FOR_STATUS_CODE, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad status code twice', 'status_code': 300}, spec=Response)
        when(mock_response_a).raise_for_status().thenRaise(HTTPError('http error msg', response=mock_response_a))
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError("shouldn't call this", 'place holder', 0))

        # the HTTPError is captured by the tries and reported as a the parent RequestException indicating non-json error
        self.assertRaises(RequestException,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_STATUS_CODE).get(self.fake_endpoint)

    def test_fetch_record_block_3_bad_status(self):
        # ensure that the retries works and that if the STATUS CODE is bad that it only tries THRICE
        self.assertEqual(self.extract.FETCH_TRIES_FOR_STATUS_CODE, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad status code twice', 'status_code': 300}, spec=Response)
        when(mock_response_a).raise_for_status().thenRaise(HTTPError('http error msg', response=mock_response_a))
        when(self.mock_session).get(self.fake_endpoint).thenReturn(mock_response_a)
        when(mock_response_a).json().thenRaise(JSONDecodeError("shouldn't call this", 'place holder', 0))

        self.extract.FETCH_TRIES_FOR_STATUS_CODE = 3
        # the HTTPError is captured by the tries and reported as a the parent RequestException indicating non-json error
        self.assertRaises(RequestException,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_STATUS_CODE).get(self.fake_endpoint)

    def test_fetch_record_block_1_bad_network(self):
        # ensure that the retries works and that if the NETWORK is bad the first time and succeeds after
        self.assertEqual(self.extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'place holder', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint)\
            .thenRaise(RequestException(response=mock_response_a)).thenReturn(mock_response_a)
        when(mock_response_a).json().thenReturn(self.mock_json_payload)

        json_response = self.extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_2_bad_network(self):
        # ensure that the retries works and that if the NETWORK is bad that it only tries TWICE
        self.assertEqual(self.extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenRaise(RequestException(response=mock_response_a))\

        self.assertRaises(RequestException,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)

    def test_fetch_record_block_3_bad_network(self):
        # ensure that the retries works and that if the NETWORK is bad that it only tries THRICE
        self.assertEqual(self.extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'bad JSON once', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenRaise(RequestException(response=mock_response_a))\

        self.extract.FETCH_TRIES_FOR_NETWORK_ERROR = 3
        self.assertRaises(RequestException,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)

    def test_fetch_record_block_1_none_response(self):
        # ensure that the retries works and that if the response is None the first time and succeeds after
        self.assertEqual(self.extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        mock_response_a = mocki({'text': 'place holder', 'status_code': 200}, spec=Response)
        when(self.mock_session).get(self.fake_endpoint).thenReturn(None).thenReturn(mock_response_a)
        when(mock_response_a).json().thenReturn(self.mock_json_payload)

        json_response = self.extract.fetch_record_block(self.fake_endpoint, self.mock_session)
        mockito.verify(self.mock_session, times=2).get(self.fake_endpoint)

        self.assertEqual(self.mock_json_payload, json_response)

    def test_fetch_record_block_2_none_response(self):
        # ensure that the retries works and that if the response is None the first time and succeeds after
        self.assertEqual(self.extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        when(self.mock_session).get(self.fake_endpoint).thenReturn(None)

        self.assertRaises(RequestException,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)

    def test_fetch_record_block_3_none_response(self):
        # ensure that the retries works and that if the response is None the first time and succeeds after
        self.assertEqual(self.extract.FETCH_TRIES_FOR_NETWORK_ERROR, 2)  # testing that it is reset between tests

        when(self.mock_session).get(self.fake_endpoint).thenReturn(None)

        self.extract.FETCH_TRIES_FOR_NETWORK_ERROR = 3
        self.assertRaises(RequestException,
                          lambda: self.extract.fetch_record_block(self.fake_endpoint, self.mock_session))
        mockito.verify(self.mock_session, times=self.extract.FETCH_TRIES_FOR_NETWORK_ERROR).get(self.fake_endpoint)
