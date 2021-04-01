import datetime as dt
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf
import os
import datetime
import argparse
from spark_datetime import get_datetime, datetimeToYear, \
    datetimeToHour, datetimeToWeek, datetimeToDay, datetimeToMonth,\
    datetimeToWeekDay, udf_date_timedelta, stringToDatetimeYYYYMMDD, \
    datetimeToYearShort, datetimeToMonthShort, datetimeToDayClasic
from common import trimStrings, castInt_udf, create_spark_session, castInt_udf_str_type, castAbsInt_udf

# get arguments passed in spark-submit command
parser = argparse.ArgumentParser()
parser.add_argument("--bucketName", help="the name of the bucket")
parser.add_argument("--dataPathKey", help="the name of the data AWS key")
parser.add_argument("--processedTablesKey", help="the name of AWS key where to output data")
parser.add_argument("--executionDate", help="""current date to extract the data related 
to actual running date; format 2016-04-06""")
#initialize variables which store passed arguments
args = parser.parse_args()
bucket_name=None
data_path_key=None
processed_tables_key=None
date_string=None
#assign values stored in arguments
if args.bucketName:
    bucket_name = args.bucketName
if args.dataPathKey:
    data_path_key = args.dataPathKey
if args.processedTablesKey:
    processed_tables_key = args.processedTablesKey
if args.executionDate:
    date_string=args.executionDate

# s3 bucket path
bucket = "s3a://" + bucket_name + "/"
# key path on S3 bucket to data
data_path_key = data_path_key + "/"

'''This function process the state file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            output_data (string): path to the directory where
                                  to write the data the output data
            date_string (string): string in format 2016-04-06                   
    Returns
            None
'''
def process_immigration_data(spark, input_data, output_data, date_string):
    # Get execution_date
    execution_date = stringToDatetimeYYYYMMDD(date_string)
    # Extract the last two digits from the year
    year = datetimeToYearShort(execution_date)
    # Extract month short version apr
    month = datetimeToMonthShort(execution_date)
    # Extract the day of month
    day = datetimeToDayClasic(execution_date)
    path =  input_data + "i94_{0}{1}_sub.sas7bdat".format(month.lower(), year)
    df_immigration_data = spark.read.format('com.github.saurfang.sas.spark').load(path)
    # df_immigration_data = df_immigration_data.filter('date="2016-04-06"')
    # df_immigration_data = spark.read.options(delimiter=",", header=True, encoding="UTF-8").csv(
    #     input_data)
    # trim spaces for arrdate
    df_immigration_data = trimStrings(df_immigration_data,
                                      ['arrdate'])
    # Set arrdate as integer because it is a timestamp
    df_immigration_data = df_immigration_data.withColumn("arrdate", df_immigration_data.arrdate.cast("integer"))
    # transform the timestamp in a date
    df_immigration_data = df_immigration_data.withColumn("date",
                                                         udf_date_timedelta(df_immigration_data.arrdate).cast("date"))
    # create the day column
    df_immigration_data = df_immigration_data.withColumn('day', datetimeToDay(df_immigration_data.date))
    #filter by day column
    df_immigration_data.createOrReplaceTempView("immigration_data")
    df_immigration_data = spark.sql("""SELECT arrdate,day,date,admnum,i94cit,i94res,
    i94bir,i94port,i94mode,
    i94addr,i94visa,gender,i94yr,i94mon 
    FROM immigration_data 
    WHERE day={0}""".format(day))

    # trim spaces for the rest of the columns
    df_immigration_data = trimStrings(df_immigration_data,
                                      ['admnum', 'i94cit', 'i94res', 'i94bir', 'i94port', 'i94mode',
                                       'i94addr', 'i94visa', 'gender'])
    ### Set the rest of columns for time table
    #transform timestamp in a datetime object to be able to get the hour
    df_immigration_data = df_immigration_data.withColumn("datetime", get_datetime(df_immigration_data.arrdate))
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
        .withColumn("admnum", castAbsInt_udf(F.col("admnum"))) \
        .withColumn("arrdate", castInt_udf(F.col("arrdate"))) \
        .withColumn("year", F.col("year").cast(T.IntegerType())) \
        .withColumn("month", F.col("month").cast(T.IntegerType())) \
        .withColumn("day", F.col("day").cast(T.IntegerType())) \
        .withColumn("weekday", F.col("weekday").cast(T.StringType())) \
        .withColumn("hour", F.col("hour").cast(T.IntegerType())) \
        .withColumn("week", F.col("week").cast(T.IntegerType())) \
        .withColumn("i94cit", castInt_udf(F.col("i94cit"))) \
        .withColumn("i94res", castInt_udf(F.col("i94res"))) \
        .withColumn("i94bir", castInt_udf(F.col("i94bir"))) \
        .withColumn("i94port", F.col("i94port").cast(T.StringType())) \
        .withColumn("i94mode", castInt_udf(F.col("i94mode"))) \
        .withColumn("i94addr", F.col("i94addr").cast(T.StringType())) \
        .withColumn("i94visa", castInt_udf(F.col("i94visa"))) \
        .withColumn("gender", F.col("gender").cast(T.StringType()))
    # filter immigrants without admission number
    df_immigration_data = df_immigration_data.na.drop(subset=["admnum"])
    df_immigration_data.write.mode("overwrite")\
        .parquet(
        output_data + "immigration")



if __name__ == "__main__":
    spark = create_spark_session()
    input_data = bucket + data_path_key
    output_data = bucket + processed_tables_key + "/"
    process_immigration_data(spark, input_data, output_data, date_string)
    spark.stop()