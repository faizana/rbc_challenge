# Running the challenge
The input data can be downloaded from following link:
https://drive.google.com/open?id=1oMLveG9KkgjXWFNflfHLmdZm_RMiwZ0e

The `mongo_customer_transaction_data_joined.csv` can be imported into a mongo collection and queried via the flask api. The api can be run by typing:
```
python api/etl_api.py
```

The api folder contains the api to query flask app for querying the data from MongoDB:

Sample curl request to retieve results of "Extract customers within an income range (eg. 100k-150k) and spent at least $1000 in a particular city (eg. Toronto) in a particular month (eg. March)"
```
curl http://localhost:8000/api/v1/extract_users -X POST -H "Content-Type:application/json" -d '{"income_range":{"min":100000, "max":150000},"city":"Toronto","spending":1000, "month":3}'
```

This will return an array of account_ids (_id) and their respective spending in that month of customers in the following form:
```
[
  {
    "_id": 2226,
    "spending": 4654.6
  },
  {
    "_id": 3708,
    "spending": 1500.0
  },
  {
    "_id": 2753,
    "spending": 2100.0
  }
  ]
  ```
## How to run the ETL pipeline
### Assuming Spark 2.2
You can give paths of either .csv or .parquet files, example of running csv below

```
spark-submit etl.py --jars '<mongo_db.jar>' --customer_table [<csvPath>,"csv"] --address_table [<csvPath>,"csv"] --transaction_table [<csvPath>,"csv"] --date_format yyyy-MM-dd'T'HH:mm:ss.SSS'Z' --csv_sep ,

```

In case of difficulty running contact ahm.faizan@gmail.com
