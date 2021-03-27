import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, trim, lower
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

from common import get_first_category_per_state, calculate_distribution
import argparse

parser = argparse.ArgumentParser()

parser.add_argument("--bucketName", help="the name of the bucket")
parser.add_argument("--dataPathKey", help="the name of the bucket")
parser.add_argument("--processedTablesKey", help="the name of the bucket")
args = parser.parse_args()
bucket_name=None
data_path_key=None
processed_tables_key=None
if args.bucketName:
    bucket_name = args.bucketName
if args.dataPathKey:
    data_path_key = args.dataPathKey
if args.processedTablesKey:
    processed_tables_key = args.processedTablesKey

bucket = "s3a://" + bucket_name + "/"
data_path_key = data_path_key + "/"


def create_spark_session():
    spark = SparkSession.builder.appName("Airport Codes").getOrCreate()
    return spark

def process_demographics(spark, input_data, output_data):
    df_demographics = spark.read.options(delimiter=";", header=True, encoding="UTF-8").csv(input_data)
    df_demo = df_demographics.withColumnRenamed("State Code", "state_code")\
        .withColumnRenamed("Median Age","median_age")\
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Total Population", "population")
    df_demo = df_demo.select("state_code", "state", "male_population",  "female_population", "median_age", "race", "population")
    df_demo.createOrReplaceTempView("df_demographics")
    df_demo_race = get_first_category_per_state(spark, df_demo, "race", "state_code")
    df_demo_race.createOrReplaceTempView("df_demographics_race")
    df_demo = spark.sql("""
                    SELECT
                    a.state_code, 
                    state, 
                    SUM(male_population) as sum_male_population,  
                    SUM(female_population) as sum_female_population, 
                    AVG(CAST(median_age AS DOUBLE)) as median_all_age, 
                    r.race, 
                    SUM(CAST(population AS DOUBLE)) as sum_all_population
                FROM df_demographics as a
                LEFT JOIN df_demographics_race as r on a.state_code=r.state_code
                group by a.state_code, a.state, r.race
                """)

    calculate_distribution_udf = F.udf(calculate_distribution, T.StringType())

    df_demo = df_demo.withColumn("male_distribution",
                                 calculate_distribution_udf(F.col('sum_male_population'), F.col('sum_all_population')))

    df_demo = df_demo.withColumn("female_distribution",
                                 calculate_distribution_udf(F.col('sum_female_population'),
                                                            F.col('sum_all_population')))
    df_demo = df_demo["state_code", "state", "male_distribution",
                      "female_distribution", "median_age", "race"]
    # assure the columns are delivered in the right data type
    df_demo = df_demo \
        .withColumn("state_code", F.col("state_code") \
                    .cast(T.StringType)) \
        .withColumn("state", F.col("state").cast(T.StringType))\
        .withColumn("male_distribution", F.col("male_distribution").cast(T.StringType)) \
        .withColumn("female_distribution", F.col("female_distribution").cast(T.StringType)) \
        .withColumn("median_age", F.col("median_age").cast(T.IntegerType)) \
        .withColumn("race", F.col("race").cast(T.StringType))
    df_demo.write.mode("overwrite").partitionBy("state_code", "race").parquet(output_data + 'demographics')


if __name__ == "__main__":
    spark = create_spark_session()
    input_data = bucket + data_path_key + "us-cities-demographics.csv"
    output_data = bucket + processed_tables_key + "/"
    process_demographics(spark, input_data, output_data)
    spark.stop()