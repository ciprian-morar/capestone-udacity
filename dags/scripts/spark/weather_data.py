from pyspark.sql.functions import year, month, dayofmonth, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType



def create_spark_session():
    spark = SparkSession.builder.appName("I94 Data Dictionary").getOrCreate()
    return spark

def process_weather_data(spark, input_data, output_data):
    udf_capitalize_lower = udf(lambda x:str(x).lower().capitalize(),StringType())

    weather_df = spark.read.options(delimiter=",", header=True).csv(input_data)
    weather_us_df = weather_df.filter("Country == 'United States'")
    df_weather_table = weather_us_df.select(weather_us_df.dt.alias("date"),
                    year("dt").alias("Year"),
                    month("dt").alias("Month"),
                    dayofmonth("dt").alias("DayOfMonth"),
                    weather_us_df.AverageTemperature,
                    weather_us_df.AverageTemperatureUncertainty,
                    udf_capitalize_lower("City").alias("city"),
                    weather_us_df.Latitude,
                    weather_us_df.Longitude)
    df_weather_table.write.partitionBy("Month").mode("overwrite").parquet(output_data)

if __name__ == "__main__":
    spark = create_spark_session()
    input_data="s3a://capestone-project-udacity-ciprian/data/source/temperature/GlobalLandTemperaturesByCity.csv"
    output_data="s3a://capestone-project-udacity-ciprian/output/weather/"
    process_weather_data(spark, input_data, output_data)
    spark.stop()