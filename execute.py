"""
Execute the ETL from the new well registry to NGWMN
"""

import logging
import os
import warnings

import cx_Oracle

from etl.extract import get_monitoring_locations
from etl.transform import transform_mon_loc_data
from etl.load import load_monitoring_location, refresh_well_registry_mv


registry_endpoint = os.getenv('REGISTRY_ML_ENDPOINT')
database_host = os.getenv('DATABASE_HOST')
database_name = os.getenv('DATABASE_NAME')
database_port = os.getenv('DATABASE_PORT')
database_user = os.getenv('DATABASE_USER')
database_password = os.getenv('DATABASE_PASSWORD')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    if database_user is None and database_password is None:
        raise AssertionError('DATABASE_USER and DATABASE_PASSWORD environment variables must be specified.')

    mon_locs = get_monitoring_locations(registry_endpoint)
    failed_locations = []
    connect_str = f'{database_host}:{database_port}/{database_name}'
    for mon_loc in mon_locs:
        transformed_data = transform_mon_loc_data(mon_loc)
        try:
            load_monitoring_location(
                database_user, database_password, connect_str, transformed_data
            )
        except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError):
            failed_locations.append((transformed_data['AGENCY_CD'], transformed_data['SITE_NO']))
    refresh_well_registry_mv(database_user, database_password, connect_str)

    if len(failed_locations) > 0:
        warning_message = 'The following agency locations failed to insert/update:\n'
        for failed_location in failed_locations:
            warning_message += f'\t{failed_location}\n'
        warnings.warn(warning_message)
