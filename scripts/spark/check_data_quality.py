import os
import pyspark.sql.functions as F
import logging
from pyspark.sql import SparkSession
import argparse

first_check = ["state", "country", "port-dict", "mode", "visa", "demographics"]
second_check = ["immigration"]
check_array=None

parser = argparse.ArgumentParser()

parser.add_argument("--checkTables", help="select the array of tables to check")
parser.add_argument("--bucketName", help="the name of the bucket")
parser.add_argument("--processedTablesKey", help="The name of processed tables")
args = parser.parse_args()
bucket_name=None
check_tables=None
processed_tables_key=None
if args.bucketName:
    bucket_name = args.bucketName
if args.checkTables:
    check_tables = args.checkTables
if args.processedTablesKey:
    processed_tables_key = args.processedTablesKey

if check_tables == "first_check":
    check_array = first_check
elif check_tables== "second_check":
    check_array = second_check

bucket = "s3a://" + bucket_name + "/"


def create_spark_session():
    spark = SparkSession.builder.appName("Data quality check").getOrCreate()
    return spark

def check_func(spark, input_data, table):
    logger = logging.getLogger()
    spark.sparkContext.setLogLevel("ERROR")
    logger.error(input_data)
    check_quality = spark.read.parquet(input_data)
    if check_quality.count() == 0 or check_quality.count() is None:
        raise Exception('Loading ' + table + ' dimension data failed')



if __name__ == "__main__":
    spark = create_spark_session()

    for i in check_array:
        input_data = bucket + processed_tables_key + "/" + i
        check_func(spark, input_data, i)

    spark.stop()