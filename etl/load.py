"""
Load data from the new Well Registry to NGWMN
"""
import cx_Oracle
import psycopg2


def _manipulate_values(y, is_timestamp):
    """
    Make various translations to make sure
    the data is Oracle friendly.
    """
    # remove leading and trailing spaces
    try:
        z = y.strip()
    except AttributeError:
        z = y

    # deal with datetimes
    if is_timestamp:
        return f"to_timestamp('{z}', 'YYYY-MM-DD\"T\"HH24:MI:SS.ff6\"Z\"')"

    # deal with everything else
    if z is None:
        return 'NULL'
    elif z is False:
        return '0'
    elif z is True:
        return '1'
    elif len(str(z)) == 0:
        return "''"
    else:
        if isinstance(z, str):
            # Escape single quotes
            z = z.translate(str.maketrans({"'": "''"}))
        return f"'{z}'"


TIME_COLUMNS = ['INSERT_DATE', 'UPDATE_DATE']


def _generate_upsert_sql(mon_loc):
    """
    Generate SQL to insert/update for Oracle.
    """
    mon_loc_db = [(k, _manipulate_values(v, k in TIME_COLUMNS)) for k, v in mon_loc.items()]
    all_columns = ','.join(col for (col, _) in mon_loc_db)
    all_values = ','.join(value for (_, value) in mon_loc_db)
    update_query = ','.join(f"{k}={v}" for (k, v) in mon_loc_db if k not in ['AGENCY_CD', 'SITE_NO'])

    statement = (
        f"MERGE INTO GW_DATA_PORTAL.WELL_REGISTRY_STG a "
        f"USING (SELECT '{mon_loc['AGENCY_CD']}' AGENCY_CD, '{mon_loc['SITE_NO']}' "
        f"SITE_NO FROM DUAL) b ON (a.AGENCY_CD = b.AGENCY_CD AND a.SITE_NO = b.SITE_NO) "
        f"WHEN MATCHED THEN UPDATE SET {update_query} "
        f"WHEN NOT MATCHED THEN INSERT ({all_columns}) VALUES ({all_values})"
    )
    return statement


def _generate_upsert_pgsql(mon_loc):
    """
    Generate SQL to insert/update for PostGIS
    """
    mon_loc_db = [(k, _manipulate_values(v, k in TIME_COLUMNS)) for k, v in mon_loc.items()]
    all_columns = ','.join(col for (col, _) in mon_loc_db)
    all_columns += ',geom'
    all_values = ','.join(value for (_, value) in mon_loc_db)
    all_values += f", ST_MakePoint({mon_loc['DEC_LAT_VA']},{mon_loc['DEC_LONG_VA']})"
    update_query = ','.join(f"{k}={v}" for (k, v) in mon_loc_db if k not in ['AGENCY_CD', 'SITE_NO'])

    statement = (
        f"INSERT INTO GW_DATA_PORTAL.WELL_REGISTRY_MAIN a ({all_columns}) VALUES ({all_values}) "
        f"ON CONFLICT(agency_cd, site_no) DO UPDATE SET {update_query}"
    )
    return statement

    # TODO delete this sample UPSERT when all is reviewed for merge, it might help the reviewer
    # INSERT INTO PRODUCT(product_name, product_type, unit_price, modified_date)
    #   SELECT   src.product_name,
    #          src.product_type,
    #          src.unit_price,
    #          now() modified_date
    #   FROM ( SELECT
    #                 product_name,
    #             product_type,
    #             unit_price
    #             FROM PRODUCT_DELTA
    #   ) src
    #
    # ON CONFLICT(product_name, product_type)
    # DO UPDATE
    #        SET
    #          unit_price   = excluded.unit_price,
    #          modified_date = excluded.modified_date;


def load_monitoring_location(db_user, db_password, connect_str, mon_loc):
    """
    Connect to the database and run the upsert SQL into Oracle.

    """
    with cx_Oracle.connect(
        db_user, db_password, connect_str, encoding='UTF-8'
    ) as connect:
        cursor = connect.cursor()
        cursor.execute(_generate_upsert_sql(mon_loc))
        connect.commit()


def load_monitoring_location_pg(db_user, db_password, host, mon_loc):
    """
    Connect to the database and run the upsert SQL into PostGIS.
    """
    with psycopg2.connect(
            f"dbname='ngwmn' user='{db_user}' host='{host}' password='{db_password}'"
    ) as connect:
        cursor = connect.cursor()
        cursor.execute(_generate_upsert_pgsql(mon_loc))
        connect.commit()


def refresh_well_registry_mv(db_user, db_password, connect_str):
    """
    Refresh the well_registry_mv materialized view
    """
    with cx_Oracle.connect(
        db_user, db_password, connect_str, encoding='UTF-8'
    ) as connect:
        cursor = connect.cursor()
        cursor.execute("begin dbms_mview.refresh('GW_DATA_PORTAL.WELL_REGISTRY_MV'); end;")
