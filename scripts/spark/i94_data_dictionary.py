import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
import argparse
from common import create_spark_session, title_string_udf, castInt_udf

# get arguments passed in spark-submit command
parser = argparse.ArgumentParser()
parser.add_argument("--bucketName",
                    help="the name of the bucket")
parser.add_argument("--dataPathKey",
                    help="the name of the bucket")
parser.add_argument("--processedTablesKey",
                    help="the name of the bucket")
args = parser.parse_args()
#initialize variables which store passed arguments
bucket_name=None
data_path_key=None
processed_tables_key=None
#assign values stored in arguments
if args.bucketName:
    bucket_name = args.bucketName
if args.dataPathKey:
    data_path_key = args.dataPathKey
if args.processedTablesKey:
    processed_tables_key = args.processedTablesKey

# s3 bucket path
bucket = "s3a://" + bucket_name + "/"
# key path on S3 bucket to data
data_path_key = data_path_key + "/"
# a dictionary with the file names
file_names = {"state_file":"i94addrl.txt",
"country_file" :"i94cntyl.txt",
"airport_file" : "i94prtl.txt",
"model_file":"i94model.txt",
"visa_file":"i94visa.txt"}

'''This function removes tabs in columns 
which have the data type string
    Parameters:
            dataframe (spark rdd): a spark rdd dataframe
            columns (list): list with column names
    Returns
            dataframe (spark rdd): rdd spark dataframe modified
'''
def removeTabs(dataframe, columns):
    for col in columns:
        dataframe = dataframe\
            .withColumn(col, F.regexp_replace(dataframe[col], "\t", ""))
    return dataframe

'''This function removes single quotes in columns 
which have the data type string 
    Parameters:
            dataframe (spark rdd): a spark rdd dataframe
            columns (list): list with column names
    Returns
            dataframe (spark rdd): rdd spark dataframe modified
'''
def removeSingleQuotes(dataframe, columns):
    for col in columns:
        dataframe = dataframe\
            .withColumn(col, F.regexp_replace(dataframe[col], "'", ""))
    return dataframe

'''This function trim strings in columns 
which have the data type string 
    Parameters:
            dataframe (spark rdd): a spark rdd dataframe
            columns (list): list with column names
    Returns
            dataframe (spark rdd): rdd spark dataframe modified
'''
def trimStrings(dataframe, columns):
    for col in columns:
        dataframe = dataframe\
            .withColumn(col, F.ltrim(F.rtrim(dataframe[col])))
    return dataframe

'''This function process the state file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            output_data (string): path to the directory where
                                  to write the data the output data
    Returns
            None
'''
def process_state_file(spark, input_data, output_data):
    df_state = spark.read\
        .options(delimiter="=", header=None).csv(input_data)
    #columns in state dataframe = [state_code, state]
    df_state = df_state\
        .withColumnRenamed("_c0", "state_code")\
        .withColumnRenamed("_c1", "state")
    #remove single quotes
    input_data = ["state_code", "state"]
    df_state = removeSingleQuotes(df_state, input_data)
    # remove tab spaces from state_code
    df_state = removeTabs(df_state, input_data)
    # trim strings
    df_state = trimStrings(df_state, input_data)
    # assure the columns are delivered in the right data type
    df_state = df_state \
        .withColumn("state_code", F.col("state_code") \
                    .cast(T.StringType())) \
        .withColumn("state", F.col("state")\
                    .cast(T.StringType()))
    # write state data on s3 parquet
    df_state.write.mode("overwrite").parquet(output_data + "state")

'''This function process the country file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            output_data (string): path to the directory where
                                  to write the data the output data
    Returns
            None
'''
def process_country_file(spark, input_data, output_data):
    df_country = spark.read\
        .options(delimiter="=", header=False).csv(input_data)
    # columns in country dataframe = [country_code, country]
    df_country = df_country\
        .withColumnRenamed("_c0", "country_code")\
        .withColumnRenamed("_c1", "country")
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
    # assure the columns are delivered in the right data type
    df_country = df_country \
        .withColumn("country_code", castInt_udf("country_code")) \
        .withColumn("country", title_string_udf(F.col("country")))
    df_country.write.mode("overwrite").parquet(output_data + "country")

'''This function process the port file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            output_data (string): path to the directory where
                                  to write the data the output data
    Returns
            None
'''
def process_airport_file(spark, input_data, output_data):
    df_airport = spark.read\
        .options(delimiter="=", header=False).csv(input_data)
    # columns in airport dataframe = [country_code, country]
    df_airport = df_airport\
        .withColumnRenamed("_c0", "port_code")\
        .withColumnRenamed("_c1", "port_mix")
    # remove single quotes
    input_data = ['port_code', 'port_mix']
    df_airport = removeSingleQuotes(df_airport,input_data)
    # split the port_mix column in two atomic columns with city and state code
    split_col = F.split(df_airport.port_mix, ",")
    df_airport = df_airport\
        .withColumn("city", split_col.getItem(0))
    df_airport = df_airport\
        .withColumn("state_code", split_col.getItem(1))
    # drop port_mix
    df_airport = df_airport["port_code","city", "state_code"]
    input_data = ["port_code","city", "state_code"]
    # remove tab spaces
    df_airport = removeTabs(df_airport, input_data)
    # trim spaces
    df_airport = trimStrings(df_airport, input_data)
    # get state data from s3
    df_state = spark.read.parquet(output_data + 'state')
    # join dataframes airport with state
    df_airport = df_airport.join(df_state, ['state_code'])
    # assure the columns are delivered in the right data type
    df_airport = df_airport\
        .withColumn("port_code", F.col("port_code") \
                                   .cast(T.StringType()))\
        .withColumn("city", title_string_udf(F.col("city")))\
        .withColumn("state_code", F.col("state_code")\
                    .cast(T.StringType()))
    df_airport = df_airport['port_code', 'city']
    df_airport.write.mode("overwrite").parquet(output_data + "port-dict")

'''This function process the mode file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            output_data (string): path to the directory where
                                  to write the data the output data
    Returns
            None
'''
def process_model_file(spark, input_data, output_data):
    df_mode = spark.read\
        .options(delimiter="=", header=False).csv(input_data)
    # columns in model dataframe = [model_code, model]
    df_mode = df_mode.withColumnRenamed("_c0", "mode_code")\
        .withColumnRenamed("_c1", "mode")
    input_data = ['mode']
    # remove single quotes
    df_mode = removeSingleQuotes(df_mode, input_data)
    # remove tab spaces
    input_data = ['mode', 'mode_code']
    # remove tab spaces
    df_model = removeTabs(df_mode, input_data)
    # trim spaces
    df_model = trimStrings(df_model, input_data)
    # assure the columns are delivered in the right data type
    df_model = df_model\
        .withColumn("mode_code", F.col("mode_code")\
                                   .cast(T.IntegerType()))\
        .withColumn("mode", F.col("mode").cast(T.StringType()))
    df_model.write.mode("overwrite").parquet(output_data + "mode")

'''This function process the visa file 
    Parameters:
            spark (object): spark sesstion object
            input_data (string): path to the file on S3 bucket
            output_data (string): path to the directory where
                                  to write the data the output data
    Returns
            None
'''
def process_visa_file(spark, input_data, output_data):
    df_visa = spark.read\
        .options(delimiter="=", header=False).csv(input_data)
    # columns in visa dataframe = [model_code, model]
    df_visa = df_visa\
        .withColumnRenamed("_c0", "visa_code")\
        .withColumnRenamed("_c1", "visa")
    input_data = ['visa_code', 'visa']
    # remove single quotes
    df_visa = removeSingleQuotes(df_visa, input_data)
    # remove tab spaces
    df_visa = removeTabs(df_visa, input_data)
    # trim spaces
    df_visa = trimStrings(df_visa, input_data)
    # assure the columns are delivered in the right data type
    df_visa = df_visa\
        .withColumn("visa_code", F.col("visa_code")\
                                   .cast(T.IntegerType()))\
        .withColumn("visa", F.col("visa").cast(T.StringType()))
    df_visa.write.mode("overwrite").parquet(output_data + "visa")


if __name__ == "__main__":
    spark = create_spark_session()

    for key, value in file_names.items():
        input_data = bucket + data_path_key + value
        output_data = bucket + processed_tables_key + "/"
        # call functions dinamically
        process_func = globals()["process_" + key]
        process_func(spark, input_data, output_data)

    spark.stop()