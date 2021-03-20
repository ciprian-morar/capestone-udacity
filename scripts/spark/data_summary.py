import datetime as dt
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
from pyspark.sql import SparkSession
import datetime

def create_spark_session():
    spark = SparkSession.builder.appName("Data Summary").getOrCreate()
    return spark

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


def get_number_average_per_state(spark, dataframe, column, group_by):
    dataframe.createOrReplaceTempView("dataframe")
    table_final = spark.sql("""
        SELECT
        {1}, avg({0}) as avg_all
          FROM dataframe
          GROUP BY {1}
    """.format(column, group_by))
    return table_final



def process_fact_data(spark, input_data, output_data):
    # Get immigration data
    df_immigration = spark.read.parquet(output_data + "immigration")
    # Get state data dictionary
    df_state = spark.read.parquet(output_data + "state")
    # Get visa data dictionary
    df_visa_dict = spark.read.parquet(output_data + "visa")
    # Get mode data dictionary
    df_mode_dict = spark.read.parquet(output_data + "model")
    # Get country data dictionary
    df_country = spark.read.parquet(output_data + "country")
    # Get airport data dictionary
    df_airport_dict = spark.read.parquet(output_data + "airport-dict")
    # Get airport codes data
    df_airport_codes = spark.read.parquet(output_data + "airport-codes")
    # Get demographics data by state
    df_demo = spark.read.parquet(output_data + "demographics")

    df_immigration.createOrReplaceTempView("df_immigration_table")
    df_state.createOrReplaceTempView("df_state_table")
    df_country.createOrReplaceTempView("df_country_table")
    df_airport_dict.createOrReplaceTempView("df_airport_dict_table")
    df_airport_codes.createOrReplaceTempView("df_airport_codes_table")
    df_mode_dict.createOrReplaceTempView("df_mode_dict_table")
    df_visa_dict.createOrReplaceTempView("df_visa_dict_table")
    df_demo.createOrReplaceTempView("df_demo_table")
    # get the sum for each sex grouped by each state
    sex_table = spark.sql("""
        SELECT
            count(*) count_all,
            sum(case when gender = 'M' then 1 else 0 end) count_man,
            sum(case when gender = 'F' then 1 else 0 end) count_woman,
            coalesce(a.state_code, '99') as state_code,
            a.month
        FROM df_immigration_table a
        LEFT JOIN df_state_table c on a.state_code=c.state_code
        group by a.month, a.state_code
    """)

    #calculate sex distribution for each state
    def calculate_distribution(x, y):
        distribution = (x / y) * 100
        return f'{distribution:.2f}%'

    calculate_distribution_udf = F.udf(calculate_distribution, T.StringType())

    sex_table_final = sex_table.withColumn("male_distribution",
                                           calculate_distribution_udf(F.col('count_man'), F.col('count_all')))
    sex_table_final = sex_table_final.withColumn("female_distribution",
                                                 calculate_distribution_udf(F.col('count_woman'), F.col('count_all')))
    #get all the columns we need from sex table in data summary table
    sex_table_final = sex_table_final[
        'count_all', 'count_man', 'count_woman', 'state_code', 'month', 'male_distribution', 'female_distribution']
    # create table temp view for final fact table
    sex_table_final.createOrReplaceTempView("sex_table")
    # get the best residence country(in terms of number of immigrants) for each state
    residence_table = get_first_category_per_state(spark, df_immigration, "residence_country", "state_code")
    residence_table = residence_table.withColumn("residence_country",
                                                 residence_table["residence_country"].cast("integer"))
    residence_table.createOrReplaceTempView("df_residence_table")
    # get the best airport(in terms of number of immigrants) for each state
    airport_table = get_first_category_per_state(spark, df_immigration, "port_code", "state_code")
    airport_table.createOrReplaceTempView("df_port_table")
    # get the best airport(in terms of number of immigrants) for each state
    mode_table = get_first_category_per_state(spark, df_immigration, "mode_code", "state_code")
    mode_table = mode_table.withColumn("mode_code", mode_table["mode_code"].cast("integer"))
    mode_table.createOrReplaceTempView("df_mode_table")
    # get the best airport(in terms of number of immigrants) for each state
    visa_table = get_first_category_per_state(spark, df_immigration, "visa", "state_code")
    visa_table = visa_table.withColumn("visa", visa_table["visa"].cast("integer"))
    visa_table.createOrReplaceTempView("df_visa_table")

    age_table = get_number_average_per_state(spark, df_immigration, "age", "state_code")
    age_table.createOrReplaceTempView("age_table")

    spark.conf.set("spark.sql.crossJoin.enabled", True)

    fact_table = spark.sql("""
            SELECT
                first(s.count_man) as count_man,
                first(s.count_woman) as count_woman,
                first(s.male_distribution) as immigrant_male_distribution,
                first(s.female_distribution) as immigrant_female_distribution,
                first(demo.male_distribution) as us_state_male_distribution,
                first(demo.female_distribution) as us_state_female_distribution,
                first(b.state) as us_state,
                a.month, 
                a.year,
                first(demo.median_all_age) as local_median_age,
                first(ag.avg_age) as imm_median_age,
                first(h.model) as imm_mode,
                first(j.visa) as visa_name,
                first(d.country) as residence_country,
                first(ac.name) as airport_name
            FROM df_immigration_table a
            LEFT JOIN age_table ag on a.state_code=ag.state_code
            LEFT JOIN sex_table s on a.state_code=s.state_code
            LEFT JOIN df_state_table b on a.state_code=b.state_code
            LEFT JOIN df_residence_table c on c.state_code=a.state_code
            LEFT JOIN df_country_table d on d.country_code=c.residence_country
            LEFT JOIN df_port_table e on e.state_code=a.state_code
            LEFT JOIN df_airport_dict_table f on e.port_code=f.port_code
            LEFT JOIN df_airport_codes_table ac on f.port_code=ac.airport_code
            LEFT JOIN df_mode_table g on g.state_code=a.state_code
            LEFT JOIN df_mode_dict_table h on g.mode_code=a.mode_code
            LEFT JOIN df_visa_table i on i.state_code=a.state_code
            LEFT JOIN df_visa_dict_table j on i.visa=j.visa_code
            LEFT JOIN df_demo_table demo on a.state_code=demo.state_code
            group by a.month, a.year, a.state_code
        """)

    fact_table.write.mode("overwrite").partitionBy("year", "month", "state_code", "port_code").parquet(
        output_data + "immigration")



if __name__ == "__main__":
    spark = create_spark_session()
    input_data = ""
    output_data = "s3a://capestone-project-udacity-ciprian/output/"
    process_fact_data(spark, input_data, output_data)
    spark.stop()