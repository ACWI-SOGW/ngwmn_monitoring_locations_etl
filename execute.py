"""
Execute the ETL from the new well registry to NGWMN
"""

import logging
import os
import warnings

import cx_Oracle
import psycopg2

from etl.extract import get_monitoring_locations
from etl.transform import transform_mon_loc_data, date_format
from etl.load import load_monitoring_location, load_monitoring_location_pg, \
    refresh_well_registry_mv, refresh_well_registry_pg, make_oracle, make_postgres

registry_endpoint = os.getenv('REGISTRY_ML_ENDPOINT')
database_host = os.getenv('DATABASE_HOST', None)
database_name = os.getenv('DATABASE_NAME')
database_port = os.getenv('DATABASE_PORT')
database_user = os.getenv('DATABASE_USER')
database_password = os.getenv('DATABASE_PASSWORD')
pg_host = os.getenv('PG_HOST', None)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    if database_user is None or database_password is None:
        raise AssertionError('DATABASE_USER and DATABASE_PASSWORD environment variables must be specified.')

    mon_locs = get_monitoring_locations(registry_endpoint)
    failed_locations = []
    count = 1

    with make_oracle(database_host, database_port, database_name, database_user, database_password) as oracle, \
            make_postgres(pg_host, "5432", database_name, database_user, database_password) as postgres:

        for mon_loc in mon_locs:
            transformed_data = transform_mon_loc_data(mon_loc)

            if database_host is not None:
                try:  # ETL to legacy Oracle
                    load_monitoring_location(oracle, transformed_data)
                except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError) as err:
                    failed_locations.append((transformed_data['AGENCY_CD'], transformed_data['SITE_NO'], err))

            if pg_host is not None:
                try:  # ETL to PostGIS
                    date_format(transformed_data)
                    load_monitoring_location_pg(postgres, transformed_data)
                except (psycopg2.IntegrityError, psycopg2.DatabaseError) as err:
                    failed_locations.append((transformed_data['AGENCY_CD'], transformed_data['SITE_NO'], err))

            if count % 1000 == 1:
                logging.info(f'Loaded monitoring locations: {count}')
            count = count + 1

        logging.info(f'Loaded monitoring locations: {count}')

        if database_host is not None:
            logging.info('updating Oracle materialized view')
            try:  # ETL to legacy Oracle
                refresh_well_registry_mv(oracle)
                oracle_update = True
            except (cx_Oracle.IntegrityError, cx_Oracle.DatabaseError) as err:
                oracle_update = False

        if pg_host is not None:
            logging.info('updating postgres registry table')
            try:  # ETL to PostGIS
                refresh_well_registry_pg(postgres)
                postgres_update = True
            except (psycopg2.IntegrityError, psycopg2.DatabaseError) as err:
                postgres_update = False

    if len(failed_locations) > 0:
        warning_message = 'The following agency locations failed to insert/update:\n'
        for failed_location in failed_locations:
            warning_message += f'\t{failed_location}\n'
        if not oracle_update:
            warning_message += "\n Oracle Well_Registry_MV Not Updated.\n"
        if not postgres_update:
            warning_message += "\n Postgres Well_Registry_MV Not Updated.\n"
        warnings.warn(warning_message)
