import datetime as dt
import pyspark.sql.functions as F
import os
from pyspark.sql import SparkSession
import datetime

def create_spark_session():
    spark = SparkSession.builder.appName("Immigration Data").getOrCreate()
    return spark

udf_date_timedelta = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(int(float(x)))).isoformat() if x else None)

def get_months_list():
    months_choices = []
    for i in range(1,13):
        months_choices.append((i, datetime.date(2016, i, 1).strftime('%b')))
    return months_choices

def get_immigration_files(spark, input_data, output_data):
    months = get_months_list()
    paths = []
    for key, value in months.items():
        process_immigration_data(spark, input_data, output_data, value)

def process_immigration_data(spark, input_data, output_data, month, year):
    # Extract the last two digits from the year
    # year_last_two_digits = int(str(year)[-2:])
    # comment big data path
    # path =  input_data + "i94_{0}{1}_sub.sas7bdat".format(month, year_last_two_digits)
    # df_spark = spark.read.format('com.github.saurfang.sas.spark').load(path)
    df_immigration_data = spark.read.options(delimiter=",", header=True, encoding="UTF-8").csv(
        "s3://capestone-project-udacity-ciprian/data/immigration_data_sample/immigration_data_sample.csv")
    df_immigration_data = df_immigration_data.withColumn("arrdate", udf_date_timedelta(df_immigration_data.arrdate))

    # df_model = spark.read.parquet(output_data + "model")
    # df_state = spark.read.parquet(output_data + "state")
    # df_country = spark.read.parquet(output_data + "country")
    # df_visa = spark.read.parquet(output_data + "visa")
    #
    # df_spark.createOrReplaceTempView("df_spark_view")
    # df_state.createOrReplaceTempView("df_state")
    # df_visa.createOrReplaceTempView("df_visa")
    # df_model.createOrReplaceTempView("df_model")
    # df_country.createOrReplaceTempView("df_country")

    df_immigration_data = df_immigration_data['i94yr', 'i94mon', 'i94cit', 'i94res','i94port','arrdate', 'i94mode', 'i94addr','i94bir','i94visa','occup','gender']
    df_immigration_data = df_immigration_data.withColumnRenamed("i94yr", "year")\
        .withColumnRenamed("i94mon", "month")\
        .withColumnRenamed("i94cit", "birth_country")\
        .withColumnRenamed("i94res", "residence_country") \
        .withColumnRenamed("i94port", "port_code")\
        .withColumnRenamed("i94mode", "mode_code")\
        .withColumnRenamed("i94addr", "state_code") \
        .withColumnRenamed("i94bir", "age") \
        .withColumnRenamed("i94visa", "visa")
    df_immigration_data.write.mode("overwrite").partitionBy("year", "month", "state_code", "port_code").parquet(
        output_data + "immigration")



if __name__ == "__main__":
    spark = create_spark_session()
    input_data = "s3a://capestone-project-udacity-ciprian/data/source/i94_immigration_data/18-83510-I94-Data-2016/"
    output_data = "s3a://capestone-project-udacity-ciprian/output/"
    process_immigration_data(spark, input_data, output_data, "apr", 2016)
    spark.stop()