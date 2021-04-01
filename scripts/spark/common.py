import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

'''This function groups the data in the dataframe by a column,
and after get the most frequented value for the column with categories.
    Parameters:
            spark (object): spark sesstion object
            dataframe (spark rdd): rdd spark dataframe
            column (string): column for which we calculate 
                             the most popular category
            group_by (string): a column for 
                               which we group the rows in the 
                               dataframe
    Returns
            dataframe (spark rdd)
'''
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


'''This function calculate the distribution 
for a category from a total 
    Parameters:
            x (numeric): the value corespondent
                         to a  category
            y (numeric): the value corespondent
                         to all categories
    Returns
            distribution (string): formatted string with %
'''
def calculate_distribution(x, y):
    distribution = (x / y) * 100
    return f'{distribution:.2f}%'

'''This function groups the data in the dataframe by a column,
and after calculate the average value for a specific numeric column.
    Parameters:
            spark (object): spark sesstion object
            dataframe (spark rdd): rdd spark dataframe
            column (numeric): column for which we calcualte the average
            group_by (string): a column for 
                               which we group the rows in the 
                               dataframe
    Returns
            dataframe (spark rdd)
'''
def get_number_average_per_state(spark, dataframe, column, group_by):
    dataframe.createOrReplaceTempView("dataframe")
    table_final = spark.sql("""
        SELECT
        {1}, avg({0}) as avg_all
          FROM dataframe
          GROUP BY {1}
    """.format(column, group_by))
    return table_final

'''This function trim the strings in a dataframe
 for the specified columns 
    Parameters:
            dataframe (spark rdd): rdd spark dataframe
            columns (list): list of columns
    Returns
            dataframe (spark rdd): modified dataframe
'''
def trimStrings(dataframe, columns):
    for col in columns:
        dataframe = dataframe.withColumn(col, F.ltrim(F.rtrim(dataframe[col])))
    return dataframe

'''This function is used as udf function 
to cast strings as int 
    Parameters:
            column (spark rdd column): string column of a dataframe
    Returns
            dataframe (spark rdd): modified dataframe
'''
def castInt(column):
    return int(float(column)) if column else None

'''This function is used as udf function 
to cast strings as absolute integers 
    Parameters:
            column (spark rdd column): string column of a dataframe
    Returns
            dataframe (spark rdd): modified dataframe
'''
def castAbsInt(column):
    return int(abs(float(column))) if column else None

'''Create spark function
    Returns
            SparkSession (object): spark session object
'''
def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().appName("Immigration Data").getOrCreate()
    return spark

'''This function is used to lowercase every 
word in a string and capitalize the first letter
    Parameters:
            column (string): list with column names
    Returns
            value (string): titled string
'''
def titleString(column):
    return column.title()

## udfs functions for spark rdds
# udf which capitalize every word in a string
title_string_udf = F.udf(titleString, T.StringType())
# udf which cast the values to int
castInt_udf = F.udf(castInt, T.IntegerType())
# udf which cast the values to absolute integers
castAbsInt_udf = F.udf(castAbsInt, T.IntegerType())
# udf which cast the values to int and after
# transform the int values in strings
castInt_udf_str_type = F.udf(castInt, T.StringType())
# udf which calculate the distribution
calculate_distribution_udf = F.udf(calculate_distribution, T.StringType())