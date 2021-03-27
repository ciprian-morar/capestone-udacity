import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# """
# DataQualityOperator it's used for data quality checks of
# the tables new created in DW
# """
class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    #     '''
    #     Constructor DataQualityOperator
    #     Parameters:
    #         redshift_conn_id (string): the name of connection
    #         query_checks (list of dicts): dict keys: check_sql, expected_result
    #         table_names (list of strings): the name of the tables to check

    #     '''
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 query_checks="",
                 table_names=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.query_checks = query_checks
        self.table_names = table_names

    def execute(self, context):
        self.log.info('DataQualityOperator')
        # connect to redshift with the PostgresHook
        redshift_hook = PostgresHook(self.redshift_conn_id)
        # check the result of the queries with the expected results
        failing_tests=[]
        for check in self.query_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records_query = redshift_hook.get_records(sql)[0][0]

            error_count = 0

            if exp_result != records_query:
                error_count += 1
                failing_tests.append(sql)

            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(failing_tests)
                raise ValueError('Data quality check failed')

        # check if all the new tables are created and at least one record per table has loaded on them
        for table in self.table_names:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Passed data quality record count on table {table} check with {records[0][0]} records")