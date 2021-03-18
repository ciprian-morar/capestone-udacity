import datetime as dt
import pyspark.sql.functions as F
import os
from pyspark.sql import SparkSession
import datetime


def create_spark_session():
    spark = SparkSession.builder.appName("Airport Codes").getOrCreate()
    return spark

def get_months_list():
    months_choices = []
    for i in range(1,13):
        months_choices.append((i, datetime.date(2016, i, 1).strftime('%b')))
    return months_choices

def process_immigration_data(spark, input_data, output_data, month):
    df_immigration_airport = spark.read.parquet(output_data + "airport-codes")
    df_immigration_airport = df_immigration_airport.withColumn("city", F.lower(df_immigration_airport.city))
    df_demo = spark.read.parquet(output_data + 'demographics/')
    df_demo_airport = df_immigration_airport.join(df_demo, ["city", "state_code", "state_name"])

    df_immigration = spark.read.parquet(output_data + "immigration/").filter("i94dt=='{0}'".format(month))
    df_immigration = df_immigration.withColumnRenamed("port_of_entry", "airport_code")
    df_demo_airport = df_demo_airport.drop("state_code", "state_name")
    df_immigration_demo = df_immigration.join(df_demo_airport, ["airport_code"]). \
        selectExpr("cicid", "arrival_date", "departure_date", "airport_code", "name", "city", "state_code",
                   "state_name", "population", "median_age", "i94dt")

    df_immigrant = spark.read.parquet(output_data + "immigrant/").filter("i94dt=='{0}'".format(month)).drop(
        "i94dt")
    df_immigrant_demographics = df_immigrant.join(df_immigration_demo, ["cicid"]). \
        selectExpr("cicid", "age", "birth_country", "residence_country", "gender", "visatype", "visa", \
                   "i94dt", "arrival_date", "departure_date", "airport_code", "name", "city", "state_code", \
                   "state_name", "population", "median_age")

    df_immigrant_demographics.write.partitionBy("i94dt").mode("append").parquet(
        output_data + 'immigration_demographics/')

if __name__ == "__main__":
    spark = create_spark_session()
    input_data = "s3a://capestone-project-udacity-ciprian/data/source/i94_immigration_data/18-83510-I94-Data-2016/"
    output_data = "s3a://capestone-project-udacity-ciprian/output/"
    months = get_months_list()
    for key, value in months.items():
        process_immigration_data(spark, input_data, output_data, value)
    spark.stop()