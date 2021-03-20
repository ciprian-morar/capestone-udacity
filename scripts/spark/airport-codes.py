import os
from pyspark.sql.functions import udf, regexp_replace,lower, split, col
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long

def create_spark_session():
    spark = SparkSession.builder.appName("Airport Codes").getOrCreate()
    return spark

udf_capitalize_lower = udf(lambda x:str(x).lower().capitalize(),StringType())

def process_airport_codes(spark, input_data, output_data):
    #------------------Clean Data-------------------------
    df_airport = spark.read.options(delimiter=",", header=True).csv(input_data)
    # filter only open airports in US
    df_airport = df_airport.filter("iso_country='US' and type!='closed'")
    # split coordinates in latitude and longitude
    df_airport = df_airport.withColumn("latitude", split(col("coordinates"), ",")[0].cast(Dbl()))
    df_airport = df_airport.withColumn("longitude", split(col("coordinates"), ",")[1].cast(Dbl()))
    # split iso_region an get state
    df_airport = df_airport.withColumn("state", split(col("iso_region"), "-")[1])
    #rename fields
    # df_airport = df_airport.withColumnRenamed("municipality", "city")
    """the column airport_code is created by performing a union to select 
        only distinct values
        of the iata_codes and local_code"""
    df_airport.createOrReplaceTempView("df_airports")
    df_airport = spark.sql("""
       select ident as airport_id, type, name, elevation_ft, iso_country, state, municipality, gps_code, iata_code as airport_code, latitude, longitude
          from df_airports
          where iata_code is not null
          
       union
       
       select ident as airport_id, type, name, elevation_ft, iso_country, state, municipality, gps_code, local_code  as airport_code, latitude, longitude
           from df_airports
           where local_code is not null
                                    """)
    # df_us_airports = spark.read.parquet("s3a://capestone-project-udacity-ciprian/output/airport-dict/")
    # df_immigration_airport = df_airport.join(df_us_airports, df_us_airports.port_code==df_airport.airport_code, how="inner")
    df_airport.write.mode("overwrite").partitionBy("airport_code", "state").parquet(output_data)


if __name__ == "__main__":
    spark = create_spark_session()
    input_data="s3a://capestone-project-udacity-ciprian/data/source/airport/airport-codes_csv.csv"
    output_data="s3a://capestone-project-udacity-ciprian/output/airport-codes"
    process_airport_codes(spark, input_data, output_data)
    spark.stop()