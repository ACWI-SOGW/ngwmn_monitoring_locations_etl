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
    all_columns = ','.join(col for (col, _) in mon_loc_db if col not in ['INSERT_USER_ID', 'UPDATE_USER_ID', 'REVIEW_FLAG'])
    all_columns += ',geom'
    all_values = ','.join(value for (key, value) in mon_loc_db if key not in ['INSERT_USER_ID', 'UPDATE_USER_ID', 'REVIEW_FLAG'])
    geom_col = f" ST_SetSRID(ST_MakePoint({mon_loc['DEC_LONG_VA']},{mon_loc['DEC_LAT_VA']}),4269) "
    all_values += "," + geom_col
    update_query = ','.join(f"{k}={v}" for (k, v) in mon_loc_db if k not in ['AGENCY_CD', 'SITE_NO', 'INSERT_USER_ID', 'UPDATE_USER_ID', 'REVIEW_FLAG'])
    update_query += ", geom=" + geom_col

    statement = (
        f"INSERT INTO GW_DATA_PORTAL.WELL_REGISTRY_MAIN ({all_columns}) VALUES ({all_values}) "
        f"ON CONFLICT(agency_cd, site_no) DO UPDATE SET {update_query}"
    )
    return statement


class NoDb:
    """
    Do Nothing place holder no database available.
    """
    def __enter__(self):
        """
        Do Nothing place holder.
        """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Do Nothing place holder.
        """
        pass


def make_oracle(host, port, database, user, password):
    """
    Connect to Oracle database.
    """
    if host is None:
        return NoDb()
    connect_str = f'{host}:{port}/{database}'
    return cx_Oracle.connect(user, password, connect_str, encoding='UTF-8')


def make_postgres(host, port, database, user, password):
    """
    Connect to Postgres database.
    """
    if host is None:
        return NoDb()
    return psycopg2.connect(host=host, port=port, database=database, user=user, password=password)


def load_monitoring_location(connect, mon_loc):
    """
    Connect to the database and run the upsert SQL into Oracle.

    """
    cursor = connect.cursor()
    cursor.execute(_generate_upsert_sql(mon_loc))
    connect.commit()


def load_monitoring_location_pg(connect, mon_loc):
    """
    Connect to the database and run the upsert SQL into PostGIS.
    """
    cursor = connect.cursor()
    cursor.execute(_generate_upsert_pgsql(mon_loc))
    connect.commit()


def refresh_well_registry_mv(connect):
    """
    Refresh the well_registry_mv materialized view
    """
    cursor = connect.cursor()
    cursor.execute("begin dbms_mview.refresh('GW_DATA_PORTAL.WELL_REGISTRY_MV'); end;")


def refresh_well_registry_pg(connect):
    """
    Refresh the well_registry_mv table in postgres
    """
    cursor = connect.cursor()
    cursor.execute(DELETE_MV)
    cursor.execute(INSERT_MV)
    connect.commit()


DELETE_MV = 'delete from gw_data_portal.well_registry_mv;'
INSERT_MV = 'insert into gw_data_portal.well_registry_mv ( \
              AGENCY_CD, \
              AGENCY_NM, \
              AGENCY_MED, \
              SITE_NO, \
              SITE_NAME, \
              DISPLAY_FLAG, \
              DEC_LAT_VA, \
              DEC_LONG_VA, \
              HORZ_DATUM, \
              HORZ_METHOD, \
              HORZ_ACY, \
              ALT_VA, \
              ALT_UNITS, \
              ALT_UNITS_NM, \
              ALT_DATUM_CD, \
              ALT_METHOD, \
              ALT_ACY, \
              WELL_DEPTH, \
              WELL_DEPTH_UNITS, \
              WELL_DEPTH_UNITS_NM, \
              NAT_AQUIFER_CD, \
              NAT_AQFR_DESC, \
              COUNTRY_CD, \
              COUNTRY_NM, \
              STATE_CD, \
              STATE_NM, \
              COUNTY_CD, \
              COUNTY_NM, \
              LOCAL_AQUIFER_CD, \
              LOCAL_AQUIFER_NAME, \
              SITE_TYPE, \
              AQFR_CHAR, \
              QW_SYS_NAME, \
              QW_SN_FLAG, \
              QW_SN_DESC, \
              QW_BASELINE_FLAG, \
              QW_BASELINE_DESC, \
              QW_WELL_CHARS, \
              QW_WELL_CHARS_DESC, \
              QW_WELL_TYPE, \
              QW_WELL_TYPE_DESC, \
              QW_WELL_PURPOSE, \
              QW_WELL_PURPOSE_DESC, \
              QW_WELL_PURPOSE_NOTES, \
              WL_SYS_NAME, \
              WL_SN_FLAG, \
              WL_SN_DESC, \
              WL_BASELINE_FLAG, \
              WL_BASELINE_DESC, \
              WL_WELL_CHARS, \
              WL_WELL_CHARS_DESC, \
              WL_WELL_TYPE, \
              WL_WELL_TYPE_DESC, \
              WL_WELL_PURPOSE, \
              WL_WELL_PURPOSE_DESC, \
              WL_WELL_PURPOSE_NOTES, \
              GEOM, \
              INSERT_DATE, \
              UPDATE_DATE, \
              DATA_PROVIDER, \
              WL_DATA_PROVIDER, \
              QW_DATA_PROVIDER, \
              LITH_DATA_PROVIDER, \
              CONST_DATA_PROVIDER, \
              WL_DATA_FLAG, \
              QW_DATA_FLAG, \
              LOG_DATA_FLAG, \
              LINK \
  ) \
  select      AGENCY_CD, \
              AGENCY_NM, \
              AGENCY_MED, \
              SITE_NO, \
              SITE_NAME, \
              DISPLAY_FLAG, \
              DEC_LAT_VA, \
              DEC_LONG_VA, \
              HORZ_DATUM, \
              HORZ_METHOD, \
              HORZ_ACY, \
              ALT_VA, \
              ALT_UNITS, \
              ALT_UNITS_NM, \
              ALT_DATUM_CD, \
              ALT_METHOD, \
              ALT_ACY, \
              WELL_DEPTH, \
              WELL_DEPTH_UNITS, \
              WELL_DEPTH_UNITS_NM, \
              NAT_AQUIFER_CD, \
              NAT_AQFR_DESC, \
              COUNTRY_CD, \
              COUNTRY_NM, \
              STATE_CD, \
              STATE_NM, \
              COUNTY_CD, \
              COUNTY_NM, \
              LOCAL_AQUIFER_CD, \
              LOCAL_AQUIFER_NAME, \
              SITE_TYPE, \
              AQFR_CHAR, \
              QW_SYS_NAME, \
              QW_SN_FLAG, \
              QW_SN_DESC, \
              QW_BASELINE_FLAG, \
              QW_BASELINE_DESC, \
              QW_WELL_CHARS, \
              QW_WELL_CHARS_DESC, \
              QW_WELL_TYPE, \
              QW_WELL_TYPE_DESC, \
              QW_WELL_PURPOSE, \
              QW_WELL_PURPOSE_DESC, \
              QW_WELL_PURPOSE_NOTES, \
              WL_SYS_NAME, \
              WL_SN_FLAG, \
              WL_SN_DESC, \
              WL_BASELINE_FLAG, \
              WL_BASELINE_DESC, \
              WL_WELL_CHARS, \
              WL_WELL_CHARS_DESC, \
              WL_WELL_TYPE, \
              WL_WELL_TYPE_DESC, \
              WL_WELL_PURPOSE, \
              WL_WELL_PURPOSE_DESC, \
              WL_WELL_PURPOSE_NOTES, \
              GEOM, \
              INSERT_DATE, \
              UPDATE_DATE, \
              DATA_PROVIDER, \
              WL_DATA_PROVIDER, \
              QW_DATA_PROVIDER, \
              LITH_DATA_PROVIDER, \
              CONST_DATA_PROVIDER, \
              WL_DATA_FLAG, \
              QW_DATA_FLAG, \
              LOG_DATA_FLAG, \
              LINK \
  from gw_data_portal.well_registry wr;'
