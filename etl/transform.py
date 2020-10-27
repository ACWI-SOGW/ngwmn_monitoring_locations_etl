"""
Transform the data into a form that
works with the WELL_REGISTRY_STG table.
"""


def mapping_factory(mapping):
    def map_func(key):
        if key is not None:
            ora_val = mapping.get(key.lower())
        else:
            ora_val = None
        return ora_val
    return map_func


WELL_TYPES = {
    'surveillance': 1,
    'trend': 2,
    'special': 3,
}
map_well_type = mapping_factory(WELL_TYPES)

WELL_PURPOSE = {
    'dedicated monitoring/observation': 1,
    'other': 2
}
map_well_purpose = mapping_factory(WELL_PURPOSE)


QW_WELL_CHARS = {
    'background': 1,
    'suspected/anticipated changes': 2,
    'known changes': 3
}
map_qw_well_chars = mapping_factory(QW_WELL_CHARS)

WL_WELL_CHARS = {
    'background': 1,
    'suspected/anticipated changes': 2,
    'known changes': 3,
    'unknown': 999
}
map_wl_well_chars = mapping_factory(WL_WELL_CHARS)

def to_flag(flag):
    return '1' if flag else '0'


def transform_mon_loc_data(ml_data):
    """
    Map the fields from the API JSON response to
    the fields in the WELL_REGISTRY_STG table with
    appropriate foreign key values.
    """
    mapped_data = dict()

    mapped_data['AGENCY_CD'] = ml_data['agency']['agency_cd']
    mapped_data['AGENCY_NM'] = ml_data['agency']['agency_nm']
    mapped_data['AGENCY_MED'] = ml_data['agency']['agency_med']

    mapped_data['SITE_NO'] = ml_data['site_no']
    mapped_data['SITE_NAME'] = ml_data['site_name']
    mapped_data['DEC_LAT_VA'] = ml_data['dec_lat_va']
    mapped_data['DEC_LONG_VA'] = ml_data['dec_long_va']
    mapped_data['HORZ_DATUM'] = ml_data['horizontal_datum']
    mapped_data['ALT_VA'] = ml_data['alt_va']
    mapped_data['ALT_DATUM_CD'] = ml_data['altitude_datum']
    try:
        mapped_data['NAT_AQUIFER_CD'] = ml_data['nat_aqfr']['nat_aqfr_cd']
        mapped_data['NAT_AQFR_DESC'] = ml_data['nat_aqfr']['nat_aqfr_desc']
    except (AttributeError, KeyError, TypeError):
        mapped_data['NAT_AQUIFER_CD'] = None
        mapped_data['NAT_AQFR_DESC'] = None
    mapped_data['LOCAL_AQUIFER_NAME'] = ml_data['local_aquifer_name']
    mapped_data['AQFR_CHAR'] = ml_data['aqfr_type']
    mapped_data['QW_SN_FLAG'] = to_flag(ml_data['qw_sn_flag'])
    mapped_data['QW_BASELINE_FLAG'] = to_flag(ml_data['qw_baseline_flag'])
    mapped_data['QW_WELL_CHARS'] = map_qw_well_chars(ml_data['qw_well_chars'])
    mapped_data['QW_WELL_PURPOSE'] = map_well_purpose(ml_data['qw_well_purpose'])
    mapped_data['QW_SYS_NAME'] = ml_data['qw_network_name']
    mapped_data['WL_SN_FLAG'] = to_flag(ml_data['qw_sn_flag'])
    mapped_data['WL_BASELINE_FLAG'] = to_flag(ml_data['wl_baseline_flag'])
    mapped_data['WL_WELL_CHARS'] = map_wl_well_chars(ml_data['wl_well_chars'])
    mapped_data['WL_WELL_PURPOSE'] = map_well_purpose(ml_data['wl_well_purpose'])
    mapped_data['WL_SYS_NAME'] = ml_data['wl_network_name']
    mapped_data['DATA_PROVIDER'] = None
    mapped_data['QW_SYS_NAME'] = None
    mapped_data['WL_SYS_NAME'] = None
    mapped_data['DISPLAY_FLAG'] = to_flag(ml_data['display_flag'])
    mapped_data['WL_DATA_PROVIDER'] = None
    mapped_data['QW_DATA_PROVIDER'] = None
    mapped_data['LITH_DATA_PROVIDER'] = None
    mapped_data['CONST_DATA_PROVIDER'] = None
    mapped_data['WELL_DEPTH'] = ml_data['well_depth']
    mapped_data['LINK'] = ml_data['link']
    mapped_data['INSERT_DATE'] = ml_data['insert_date']
    mapped_data['UPDATE_DATE'] = ml_data['update_date']
    mapped_data['WL_WELL_PURPOSE_NOTES'] = ml_data['wl_well_purpose_notes']
    mapped_data['QW_WELL_PURPOSE_NOTES'] = ml_data['qw_well_purpose_notes']
    mapped_data['INSERT_USER_ID'] = ml_data['insert_user']
    mapped_data['UPDATE_USER_ID'] = ml_data['update_user']
    mapped_data['WL_WELL_TYPE'] = map_well_type(ml_data['wl_well_type'])
    mapped_data['QW_WELL_TYPE'] = map_well_type(ml_data['qw_well_type'])
    mapped_data['LOCAL_AQUIFER_CD'] = None
    mapped_data['REVIEW_FLAG'] = None
    try:
        mapped_data['STATE_CD'] = ml_data['state']['state_cd']
    except (AttributeError, KeyError, TypeError):
        mapped_data['STATE_CD'] = None
    try:
        mapped_data['COUNTY_CD'] = ml_data['county']['county_cd']
    except (AttributeError, KeyError, TypeError):
        mapped_data['COUNTY_CD'] = None
    try:
        mapped_data['COUNTRY_CD'] = ml_data['country']['country_cd']
    except (AttributeError, KeyError, TypeError):
        mapped_data['COUNTRY_CD'] = None
    mapped_data['WELL_DEPTH_UNITS'] = ml_data['well_depth_units']['unit_id'] if ml_data['well_depth_units'] else None
    mapped_data['ALT_UNITS'] = ml_data['altitude_units']['unit_id'] if ml_data['altitude_units'] else None
    mapped_data['SITE_TYPE'] = ml_data['site_type']
    mapped_data['AQFR_CHAR'] = None
    mapped_data['HORZ_METHOD'] = ml_data['horz_method']
    mapped_data['HORZ_ACY'] = ml_data['horz_acy']
    mapped_data['ALT_METHOD'] = ml_data['alt_method']
    mapped_data['ALT_ACY'] = ml_data['alt_acy']

    return mapped_data
