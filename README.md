# ngwmn_monitoring_locations_etl

The following environment variables need to be provided to run the ETL:

* DATABASE_HOST: the hostname of the destination Oracle database
* DATABASE_NAME: name of destination database
* DATABASE_PORT: port that the Oracle database is listening on
* DATABASE_USER: username used to connect
* DATABASE_PASSWORD: password used to connect
* REGISTRY_ML_ENDPOINT: the URL of the Well Registry endpoint from which new monitoring locations are pulled

Once the environment variables are specified, the ETL can be run
by:

```
python execute.py
```