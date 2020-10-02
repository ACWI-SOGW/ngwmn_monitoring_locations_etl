"""
Functions to retrieve data from the Well Registry API
"""
import requests as r


def get_monitoring_locations(registry_ml_endpoint):
    """
    Get the monitoring location data.
    """
    resp = r.get(registry_ml_endpoint)
    return resp.json()
