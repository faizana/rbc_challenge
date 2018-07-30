# rbc_challenge
Data can be downloaded from following link:
https://drive.google.com/open?id=1oMLveG9KkgjXWFNflfHLmdZm_RMiwZ0e

The api folder contains the api to query flask app for querying the data from MongoDB:
```
curl http://localhost:8000/api/v1/extract_users -X POST -H "Content-Type:application/json" -d '{"income_range":{"min":100000, "max":150000},"city":"Toronto","spending":10000, "month":3}'

The spark folder contains the ETL pipeline.

syntax for running pipeline reading from csv source from data above:

```
spark-submit etl.py --jars <mongo_db.jar> --customer_table [<csvPath>,"csv"] --address_table [<csvPath>,"csv"] --transaction_table [<csvPath>,"csv"] --date_format yyyy-MM-dd'T'HH:mm:ss.SSS'Z' --csv_sep ,

