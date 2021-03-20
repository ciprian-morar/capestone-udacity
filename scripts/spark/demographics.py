import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf, trim, lower
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def calculate_distribution(x, y):
    distribution = (x / y) * 100
    return f'{distribution:.2f}%'

def get_first_category_per_state(spark, dataframe, column, group_by):
    dataframe.createOrReplaceTempView("dataframe")
    dataframe_table = spark.sql("""
        SELECT
        {1}, {0}, count(*) as count_all
          FROM dataframe
          GROUP BY {1}, {0} 
    """.format(column, group_by))

    dataframe_table.createOrReplaceTempView("df_all_table")

    row_number_table = spark.sql("""
           SELECT
            {1},
            {0},
            row_number() over (partition by {1} order by count_all desc) as row_number
          FROM df_all_table
    """.format(column, group_by))

    row_number_table.createOrReplaceTempView("row_number_table")

    table_final = spark.sql("""
        SELECT {1}, {0}
        FROM row_number_table
        WHERE row_number = 1 
    """.format(column, group_by))

    return table_final


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
    # df_state = spark.read.parquet(output_data + "state")
    # df_demo = df_demo.join(df_state, df_demo.state_code==df_state.code, how="inner")
    # df_demo = df_demo.withColumn("city", lower(trim(df_demo.city)))
    df_demo.write.mode("overwrite").partitionBy("state_code", "race").parquet(output_data + 'demographics')


if __name__ == "__main__":
    spark = create_spark_session()
    input_data="s3a://capestone-project-udacity-ciprian/data/source/demographics/us-cities-demographics.csv"
    output_data="s3a://capestone-project-udacity-ciprian/output/"
    process_demographics(spark, input_data, output_data)
    spark.stop()