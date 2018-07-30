import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import argparse
import json


spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/etl.cus_trans_data?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/etl.cus_trans_data") \
        .getOrCreate()


def read_from_source(source_type, path, options={}):
    source_df = spark.createDataFrame([dict(empty=True)])
    df_reader = spark.read
    try:
        if source_type == "csv":
            source_df = df_reader.csv(path, sep=options[source_type].get('sep', ','),
                                      header=options[source_type].get('header', True))
        elif source_type == "parquet":
            if isinstance(path, list):
                source_df = df_reader.parquet(*path)
            else:
                source_df = df_reader.parquet(path)
    except Exception as e:
        print(e)
    return source_df


def join_df(left, right, left_key, right_key, join_type="inner"):
    return left.join(right, left[left_key] == right[right_key], join_type).drop(right[right_key])


def parse_dates(df, format):
    return df.withColumn('parsed_date',
                         f.to_timestamp(f.col('transaction_date'), format)) \
        .withColumn("year", f.year(f.col('parsed_date'))) \
        .withColumn("month", f.month(f.col('parsed_date'))) \
        .withColumn("day", f.dayofmonth(f.col('parsed_date'))) \
        .withColumn("unix_ts", f.unix_timestamp('parsed_date')) \
        .drop("transaction_date")


def process_writeable_df(joined_df, date_format="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"):
    df_with_parsed_dates = parse_dates(joined_df, date_format)
    df_with_id = df_with_parsed_dates.withColumn("id", f.concat(f.col('account_id'), f.lit("_"), f.col("unix_ts")))
    return df_with_id.na.drop()


def write_to_mongo(df):
    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite")\
        .option("database", "etl").option("collection","cus_trans_data").save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ETL Pipeline')
    parser.add_argument('--customer_table', type=str, required=True, dest='customer_table',
                        help="The args [path,source_format] of the customer table as a list")
    parser.add_argument('--address_table', type=str, required=True, dest='address_table',
                        help="The args [path,source_format] of the address table as a list")
    parser.add_argument('--transaction_table', type=str, required=True, dest='transaction_table',
                        help="The args [path,source_format] of the transaction table as a list")
    parser.add_argument('--date_format', type=str, required=True, dest='date_format',
                        help="The transaction date format e.g yyyy-MM-dd'T'HH:mm:ss.SSS'Z' " )
    parser.add_argument('--csv_sep', type=str, required=False, dest='csv_sep',
                        help="The transaction date format e.g yyyy-MM-dd'T'HH:mm:ss.SSS'Z' ")

    args = parser.parse_args()
    config = dict(
        customer=dict(path=json.loads(args.customer_table[0]), source_type=json.loads(args.customer_table[1]), options = dict(csv=dict(sep=args.csv_sep))),
        address=dict(path=json.loads(args.address_table[0]), source_type=json.loads(args.address_table[1]),options = dict(csv=dict(sep=args.csv_sep))),
        transaction=dict(
            path=json.loads(args.transaction_table[0]), source_type=json.loads(args.transaction_table[1]), options = dict(csv=dict(sep=args.csv_sep))))

    customer_df = read_from_source(config['customer']['source_type'], config['customer']['path'],config['options'])\
        .toDF("cus_address_id","account_id","first_name","last_name","income")
    address_df = read_from_source(config['address']['source_type'], config['address']['path'],config['options']).toDF("trans_address_id","account_id","transaction_amount","transaction_date")
    transaction_df = read_from_source(config['transaction']['source_type'], config['transaction']['path'],config['options'])\
        .toDF("trans_address_id","account_id","transaction_amount","transaction_date")
    customer_transaction_data = join_df(customer_df,join_df(transaction_df,address_df,"trans_address_id","address_id"),
                                        "account_id","account_id", config['options'])
    mongo_coll_df = process_writeable_df(customer_transaction_data, args.date_format)
    write_to_mongo(mongo_coll_df)

    print("Pipeline complete")


