import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# """
# HasRowsOperator it's used in the subdag which move
# the data from S3 to staging tables.
# This Operator it's used for data quality checks
# """
class HasRowsOperator(BaseOperator):
#      '''
#     Constructor HasRowsOperator
#     Parameters:
#         redshift_conn_id (string): the name of connection
#         table (string): the name of the table
#     '''
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        #connect to redshift with the PostgresHook
        self.log.info('HasRowsOperator')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
#         """check if the staging tables were created """
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
#         """check if any record had loaded and raise an error if no records exists"""
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        # logging the number of records loaded in the staging table
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")