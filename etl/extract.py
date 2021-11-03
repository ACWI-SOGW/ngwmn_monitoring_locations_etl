"""
Functions to retrieve data from the Well Registry API
"""

import logging

from time import sleep
from json.decoder import JSONDecodeError
from requests import Session
from requests.exceptions import RequestException
from requests.exceptions import HTTPError

"""Number of records to fetch in at once"""
FETCH_LIMIT = 8
"""Number of times to retry when there is a network error"""
FETCH_TRIES_FOR_NETWORK_ERROR = 2
"""Number of times to retry when there is an HTTP Status code other than ok"""
FETCH_TRIES_FOR_STATUS_CODE = 2
"""Number of times to retry when there is the JSON fails to decode"""
FETCH_TRIES_FOR_JSON = 2
"""Number of times to accept a missing data for aborting the ETL"""
FETCH_JSON_ERROR_TOLERANCE = 2
"""Number of seconds to wait before a retry is attempted"""
FETCH_RETRY_DELAY = 10
"""Number of fetches per info log"""
FETCHES_PER_LOG = 128


def get_monitoring_locations(registry_ml_endpoint):
    """
    Get the monitoring location data.
    """
    # initialize state of errors, URL, and results
    json_fail_count = 0
    fetches = 0
    next_chunk = construct_url(registry_ml_endpoint)
    results = []

    with Session() as session:
        while next_chunk:
            fetches += 1
            if fetches % FETCHES_PER_LOG == 0:
                logging.info(f'Retrieving monitoring locations: {next_chunk}')
            try:
                payload = fetch_record_block(next_chunk, session)
                results.extend(payload.get('results'))
                next_chunk = payload.get('next')
            except JSONDecodeError as json_err:
                json_fail_count += 1
                if json_fail_count > FETCH_JSON_ERROR_TOLERANCE:
                    logging.error('Abort: JSON errors exceeded. Set FETCH_JSON_ERROR_TOLERANCE to fine tune.')
                    raise JSONDecodeError('JSON errors exceeded.', json_err.doc, json_err.pos)
                else:
                    logging.warning(f'JSON error occurred, {FETCH_JSON_ERROR_TOLERANCE-json_fail_count} before abort.')
            except RequestException:
                logging.error(f'Unrecoverable error fetching data from {next_chunk}')
                return []

    logging.info(f'Finished retrieving {len(results)} monitoring locations.')
    return results


def fetch_record_block(url, session):
    attempt_count_net = 1
    attempt_count_status = 1
    attempt_count_json = 1
    attempts_remain = True

    while attempts_remain:
        # if this is a retry then pause for a delay to see if it recovers
        if attempt_count_net + attempt_count_status + attempt_count_json > 3:
            logging.warning(f'Retrying request in {FETCH_RETRY_DELAY} seconds')
            sleep(FETCH_RETRY_DELAY)

        try:
            payload = try_fetch(url, session)
            attempts_remain = False  # indicate that we are done
        except HTTPError as se:  # trap http status error before the more general RequestException
            attempt_count_status += 1
            logging.warning(f'HTTP status code, {se.response.status_code}, from URL: {url}')
        except RequestException as re:  # trap network issues
            attempt_count_net += 1
            if re.response is None:
                logging.warning(f'No response entity from URL: {url}')
            else:
                logging.warning(f'Exception requesting URL: {url}')
        except JSONDecodeError as json_err:  # trap JSON parsing errors
            recent_json_error = json_err
            attempt_count_json += 1
            logging.warning(f'JSON parsing error in response from: {url}')
            logging.warning(json_err)
            logging.warning(json_err.doc)

        # exit loop if done/success or too many tries of any type
        attempts_remain &= attempt_count_net <= FETCH_TRIES_FOR_NETWORK_ERROR \
            and attempt_count_status <= FETCH_TRIES_FOR_STATUS_CODE \
            and attempt_count_json <= FETCH_TRIES_FOR_JSON

    # allow for the caller to know how often the JSON was bad
    if attempt_count_json > FETCH_TRIES_FOR_JSON:
        doc = recent_json_error.doc if recent_json_error is None else ''
        pos = recent_json_error.pos if recent_json_error is None else 0
        raise JSONDecodeError("Exceeded JSON Tries.", doc, pos)

    if attempt_count_net > FETCH_TRIES_FOR_NETWORK_ERROR or attempt_count_status > FETCH_TRIES_FOR_STATUS_CODE:
        logging.warning(f'Retrying failed')
        raise RequestException()

    # if this is a retry then pause for a delay to see if it recovers
    if attempt_count_net + attempt_count_status + attempt_count_json > 3:
        logging.info(f'Retrying succeeded')

    return payload


def try_fetch(url, session):
    respsponse = session.get(url)

    if respsponse is None:  # trap no response
        raise RequestException()

    # status codes above 203 are reduced content status codes - that is bad
    if respsponse.status_code >= 204:  # trap bad status code
        raise respsponse.raise_for_status()

    try:
        json = respsponse.json()
    except JSONDecodeError as json_err:
        # put the response text on the exception
        json_err.doc = respsponse.text
        raise json_err

    return json


def construct_url(endpoint):
    """
    Construct the URL with a smaller limit than the 1024 default
    """
    url = endpoint

    if 'limit' not in url:
        if '?' not in url:
            url += '?'
        else:
            url += '&'
        url += 'limit=' + str(FETCH_LIMIT)

    return url
