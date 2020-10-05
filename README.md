# ngwmn_monitoring_locations_etl
[![Build Status](https://travis-ci.org/ACWI-SOGW/ngwmn_monitoring_locations_etl.svg?branch=master)](https://travis-ci.org/ACWI-SOGW/ngwmn_monitoring_locations_etl)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/98c6f382ad93422baf01d75e8df4030f)](https://www.codacy.com/gh/ACWI-SOGW/ngwmn_monitoring_locations_etl/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ACWI-SOGW/ngwmn_monitoring_locations_etl&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/ACWI-SOGW/ngwmn_monitoring_locations_etl/branch/master/graph/badge.svg)](https://codecov.io/gh/ACWI-SOGW/ngwmn_monitoring_locations_etl)

The following environment variables need to be provided to run the ETL:

*   DATABASE_HOST: the hostname of the destination Oracle database
*   DATABASE_NAME: name of destination database
*   DATABASE_PORT: port that the Oracle database is listening on
*   DATABASE_USER: username used to connect
*   DATABASE_PASSWORD: password used to connect
*   REGISTRY_ML_ENDPOINT: the URL of the Well Registry endpoint from which new monitoring locations are pulled

Once the environment variables are specified, the ETL can be run
by:

```python
python execute.py
```