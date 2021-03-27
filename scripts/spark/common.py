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


#calculate distribution for categories
def calculate_distribution(x, y):
    distribution = (x / y) * 100
    return f'{distribution:.2f}%'

def get_number_average_per_state(spark, dataframe, column, group_by):
    dataframe.createOrReplaceTempView("dataframe")
    table_final = spark.sql("""
        SELECT
        {1}, avg({0}) as avg_all
          FROM dataframe
          GROUP BY {1}
    """.format(column, group_by))
    return table_final