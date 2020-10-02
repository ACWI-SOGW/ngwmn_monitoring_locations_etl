"""
Load data from the new Well Registry to NGWMN
"""
import cx_Oracle
from dateutil.parser import isoparse


def _manipulate_values(y):
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
    try:
        isoparse(z)
    except (ValueError, TypeError):
        pass
    else:
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
        return f"'{z}'"


def _generate_upsert_sql(mon_loc):
    """
    Generate SQL to insert/update.
    """
    columns = ','.join(list(mon_loc.keys()))
    values = ','.join([_manipulate_values(x) for x in list(mon_loc.values())])
    column_values = ','.join(
        f"{k}={_manipulate_values(v)}" for k, v in mon_loc.items() if k not in ['AGENCY_CD', 'SITE_NO']
    )
    statement = (
        f"MERGE INTO GW_DATA_PORTAL.WELL_REGISTRY_STG a "
        f"USING (SELECT '{mon_loc['AGENCY_CD']}' AGENCY_CD, '{mon_loc['SITE_NO']}' "
        f"SITE_NO FROM DUAL) b ON (a.AGENCY_CD = b.AGENCY_CD AND a.SITE_NO = b.SITE_NO) "
        f"WHEN MATCHED THEN UPDATE SET {column_values} WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
    )
    return statement


def load_monitoring_location(db_user, db_password, connect_str, mon_loc):
    """
    Connect to the database and run the upsert SQL.

    """
    with cx_Oracle.connect(
        db_user, db_password, connect_str, encoding='UTF-8'
    ) as connect:
        cursor = connect.cursor()
        cursor.execute(_generate_upsert_sql(mon_loc))
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
