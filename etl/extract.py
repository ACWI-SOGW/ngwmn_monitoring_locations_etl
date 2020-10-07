"""
Functions to retrieve data from the Well Registry API
"""
import logging

from requests import get, Session

def get_monitoring_locations(registry_ml_endpoint):
    """
    Get the monitoring location data.
    """
    next_chunk = registry_ml_endpoint
    results = []
    with Session() as session:
        while next_chunk:
            print(f'Retrieving monitoring locations: {next_chunk}')

            resp = session.get(next_chunk, timeout=10)
            payload = resp.json()
            results.extend(payload['results'])
            next_chunk = payload['next']

    logging.info(f'Finished retrieving {payload["count"]} monitoring locations.')
    return results
