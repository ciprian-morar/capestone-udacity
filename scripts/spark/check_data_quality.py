import os
import pyspark.sql.functions as F
import logging
from pyspark.sql import SparkSession
import argparse
from common import create_spark_session

first_check = ["state", "country", "port-dict", "mode", "visa", "demographics"]
second_check = ["immigration"]
check_array=None

# get arguments passed in spark-submit command
parser = argparse.ArgumentParser()
parser.add_argument("--checkTables", help="select the array of tables to check")
parser.add_argument("--bucketName", help="the name of the bucket")
parser.add_argument("--processedTablesKey", help="The name of processed tables")
#initialize variables which store passed arguments
args = parser.parse_args()
bucket_name=None
check_tables=None
processed_tables_key=None
#assign values stored in arguments
if args.bucketName:
    bucket_name = args.bucketName
if args.checkTables:
    check_tables = args.checkTables
if args.processedTablesKey:
    processed_tables_key = args.processedTablesKey

if check_tables == "first_check":
    check_array = first_check
elif check_tables=="second_check":
    check_array = second_check

# s3 bucket path
bucket = "s3a://" + bucket_name + "/"

'''This function process the state file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            table (string): the name of the 
                            directory from S3 to check
    Returns
            None
'''
def check_func(spark, input_data, table):
    check_quality = spark.read.parquet(input_data)
    if check_quality.count() == 0 or check_quality.count() is None:
        raise Exception('Loading ' + table + ' dimension data failed')



if __name__ == "__main__":
    spark = create_spark_session()

    for i in check_array:
        input_data = bucket + processed_tables_key + "/" + i
        check_func(spark, input_data, i)

    spark.stop()