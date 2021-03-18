import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

bucket = "s3a://capestone-project-udacity-ciprian/"
i94_codes_path = "data/source/labels/"
file_names = {"airport_file" : "i94prtl.txt",
"country_file" :"i94cntyl.txt",
"state_file":"i94addrl.txt",
"model_file":"i94model.txt",
"visa_file":"i94visa.txt"}




def create_spark_session():
    spark = SparkSession.builder.appName("I94 Data Dictionary").getOrCreate()
    return spark

def removeTabs(dataframe, columns):
    for col in columns:
        dataframe = dataframe.withColumn(col, F.regexp_replace(dataframe[col], "\t", ""))
    return dataframe

def removeSingleQuotes(dataframe, columns):
    for col in columns:
        dataframe = dataframe.withColumn(col, F.regexp_replace(dataframe[col], "'", ""))
    return dataframe

def trimStrings(dataframe, columns):
    for col in columns:
        dataframe = dataframe.withColumn(col, F.ltrim(F.rtrim(dataframe[col])))
    return dataframe

def process_state_file(spark, input_data, output_data):
    df_state = spark.read.options(delimiter="=", header=None).csv(input_data)
    #columns in state dataframe = [state_code, state]
    df_state = df_state.withColumnRenamed("_c0", "state_code")\
        .withColumnRenamed("_c1", "state")
    #remove single quotes
    input_data = ["state_code", "state"]
    df_state = removeSingleQuotes(df_state, input_data)
    # remove tab spaces from state_code
    df_state = removeTabs(df_state, input_data)
    # trim strings
    df_state = trimStrings(df_state, input_data)
    # write state data on s3 parquet
    df_state.write.mode("overwrite").parquet(output_data + "state")

def process_country_file(spark, input_data, output_data):
    df_country = spark.read.options(delimiter="=", header=False).csv(input_data)
    # columns in country dataframe = [country_code, country]
    df_country = df_country.withColumnRenamed("_c0", "country_code").withColumnRenamed("_c1", "country")
    input_data = ['country']
    # remove single quotes from country name
    df_country = removeSingleQuotes(df_country, input_data)
    input_data = ['country_code', 'country']
    # remove tab spaces
    df_country = removeTabs(df_country, input_data)
    df_country = trimStrings(df_country, input_data)
    # remove invalid rows for country column
    df_country = df_country\
        .withColumn("country",
          F.regexp_replace(df_country.country,
          "^INVALID.*|Collapsed.*|No Country.*", "INVALID"))
    df_country.write.mode("overwrite").parquet(output_data + "country")

def process_airport_file(spark, input_data, output_data):
    df_airport = spark.read.options(delimiter="=", header=False).csv(input_data)
    # columns in airport dataframe = [country_code, country]
    df_airport = df_airport.withColumnRenamed("_c0", "port_code").withColumnRenamed("_c1", "port_mix")
    # remove single quotes
    input_data = ['port_code', 'port_mix']
    df_airport = removeSingleQuotes(df_airport,input_data)
    # split the port_mix column in two atomic columns with city and state code
    split_col = F.split(df_airport.port_mix, ",")
    df_airport = df_airport.withColumn("city", split_col.getItem(0))
    df_airport = df_airport.withColumn("state_code", split_col.getItem(1))
    # drop port_mix
    df_airport = df_airport["port_code","city", "state_code"]
    input_data = ["port_code","city", "state_code"]
    # remove tab spaces
    df_airport = removeTabs(df_airport, input_data)
    # trim spaces
    df_airport = trimStrings(df_airport, input_data)
    # get state data from s3
    df_state = spark.read.parquet(os.path.join(output_data, 'state'))
    # join dataframes airport with state
    df_airport = df_airport.join(df_state, ['state_code'])
    df_airport.write.mode("overwrite").parquet(output_data + "airport-dict")

def process_model_file(spark, input_data, output_data):
    df_model = spark.read.options(delimiter="=", header=False).csv(input_data)
    # columns in model dataframe = [model_code, model]
    df_model = df_model.withColumnRenamed("_c0", "model_code").withColumnRenamed("_c1", "model")
    input_data = ['model']
    # remove single quotes
    df_model = removeSingleQuotes(df_model, input_data)
    # remove tab spaces
    input_data = ['model', 'model_code']
    # remove tab spaces
    df_model = removeTabs(df_model, input_data)
    # trim spaces
    df_model = trimStrings(df_model, input_data)
    df_model.write.mode("overwrite").parquet(output_data + "model")

def process_visa_file(spark, input_data, output_data):
    df_visa = spark.read.options(delimiter="=", header=False).csv(input_data)
    # columns in visa dataframe = [model_code, model]
    df_visa = df_visa.withColumnRenamed("_c0", "visa_code").withColumnRenamed("_c1", "visa")
    input_data = ['visa_code', 'visa']
    # remove single quotes
    df_visa = removeSingleQuotes(df_visa, input_data)
    # remove tab spaces
    df_visa = removeTabs(df_visa, input_data)
    # trim spaces
    df_visa = trimStrings(df_visa, input_data)
    df_visa.write.mode("overwrite").parquet(output_data + "visa")


if __name__ == "__main__":
    spark = create_spark_session()

    for key, value in file_names.items():
        input_data = os.path.join(bucket, i94_codes_path + value)
        output_data = bucket + "output/"
        # call functions dinamically
        process_func = globals()["process_" + key]
        process_func(spark, input_data, output_data)

    spark.stop()