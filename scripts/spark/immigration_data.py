import datetime as dt
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf
import os
from pyspark.sql import SparkSession
import datetime
import argparse
from spark_datetime import get_datetime, datetimeToYear, \
    datetimeToHour, datetimeToWeek, datetimeToDay, datetimeToMonth,\
    datetimeToWeekDay, udf_date_timedelta

parser = argparse.ArgumentParser()

parser.add_argument("--bucketName", help="the name of the bucket")
parser.add_argument("--dataPathKey", help="the name of the data AWS key")
parser.add_argument("--processedTablesKey", help="the name of AWS key where to output data")
parser.add_argument("--dateString", help="""current date to extract the data related 
to actual running date; format 2016-04-06""")
args = parser.parse_args()
bucket_name=None
data_path_key=None
processed_tables_key=None
date_string=None
if args.bucketName:
    bucket_name = args.bucketName
if args.dataPathKey:
    data_path_key = args.dataPathKey
if args.processedTablesKey:
    processed_tables_key = args.processedTablesKey
if args.dateString:
    date_string=args.dateString

bucket = "s3a://" + bucket_name + "/"
data_path_key = data_path_key + "/"

def create_spark_session():
    spark = SparkSession.builder.appName("Immigration Data").getOrCreate()
    return spark

def process_immigration_data(spark, input_data, output_data, date_string):
    # Extract the last two digits from the year
    # year_last_two_digits = int(str(year)[-2:])
    # comment big data path
    # path =  input_data + "i94_{0}{1}_sub.sas7bdat".format(month, year_last_two_digits)
    # df_spark = spark.read.format('com.github.saurfang.sas.spark').load(path)
    # df_immigration_data = df_immigration_data.filter('date="2016-04-06"')
    df_immigration_data = spark.read.options(delimiter=",", header=True, encoding="UTF-8").csv(
        input_data)
    # filter results where arrival date is null
    df_immigration_data = df_immigration_data.filter(df_immigration_data.arrdate.isNotNull())
    # filter records which have values for the column i94addr (state_code) NULL
    df_immigration_data = df_immigration_data.filter(df_immigration_data.i94addr.isNotNull())

    ### Set columns for time table
    # Set arrdate as integer because it is a timestamp
    df_immigration_data = df_immigration_data.withColumn("arrdate", df_immigration_data.arrdate.cast("integer"))
    # transform the timestamp in a date
    df_immigration_data = df_immigration_data.withColumn("date",
                                                         udf_date_timedelta(df_immigration_data.arrdate).cast("date"))
    #transform timestamp in a datetime object
    df_immigration_data = df_immigration_data.withColumn("datetime", get_datetime(df_immigration_data.arrdate))
    # get the day and other data details
    df_immigration_data = df_immigration_data.withColumn('day', datetimeToDay(df_immigration_data.date))
    df_immigration_data = df_immigration_data.withColumnRenamed('i94yr', 'year')
    df_immigration_data = df_immigration_data.withColumn('hour', datetimeToHour(df_immigration_data.datetime))
    df_immigration_data = df_immigration_data.withColumn('week', datetimeToWeek(df_immigration_data.date))
    df_immigration_data = df_immigration_data.withColumn('day', datetimeToDay(df_immigration_data.date))
    df_immigration_data = df_immigration_data.withColumnRenamed('i94mon', 'month')
    df_immigration_data = df_immigration_data.withColumn('weekday', datetimeToWeekDay(df_immigration_data.date))

    df_immigration_data = df_immigration_data['admnum', 'arrdate', 'year', 'month', 'day', 'weekday', 'hour', 'week',
                                              'i94cit', 'i94res', 'i94bir', 'i94port', 'i94mode',  'i94addr','i94visa','gender']
    # assure the columns are delivered in the right data type
    df_immigration_data = df_immigration_data \
        .withColumn("admnum", F.col("admnum") \
                    .cast(T.StringType)) \
        .withColumn("arrdate", F.col("arrdate").cast(T.TimestampType)) \
        .withColumn("year", F.col("year").cast(T.IntegerType)) \
        .withColumn("month", F.col("month").cast(T.IntegerType)) \
        .withColumn("day", F.col("day").cast(T.IntegerType)) \
        .withColumn("weekday", F.col("weekday").cast(T.StringType)) \
        .withColumn("hour", F.col("hour").cast(T.IntegerType)) \
        .withColumn("week", F.col("week").cast(T.IntegerType)) \
        .withColumn("i94cit", F.col("i94cit").cast(T.StringType)) \
        .withColumn("i94res", F.col("i94res").cast(T.StringType)) \
        .withColumn("i94bir", F.col("i94bir").cast(T.IntegerType)) \
        .withColumn("i94port", F.col("i94port").cast(T.StringType)) \
        .withColumn("i94mode", F.col("i94mode").cast(T.IntegerType)) \
        .withColumn("i94addr", F.col("i94addr").cast(T.StringType)) \
        .withColumn("i94visa", F.col("i94visa").cast(T.IntegerType)) \
        .withColumn("gender", F.col("gender").cast(T.StringType))
    df_immigration_data.write.mode("overwrite")\
        .partitionBy("year", "month", "day", "i94addr", "i94port").parquet(
        output_data + "immigration")



if __name__ == "__main__":
    spark = create_spark_session()
    input_data = bucket + data_path_key + "immigration_data_sample.csv"
    output_data = bucket + processed_tables_key + "/"
    process_immigration_data(spark, input_data, output_data, date_string)
    spark.stop()