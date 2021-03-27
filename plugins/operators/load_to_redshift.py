from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime as dt


# """
# StageToRedshiftOperator in the subdag which move
# the data from S3 to staging tables.
# This Operator it's used to load the data from S3
# """
class LoadToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS PARQUET
    """

    #     '''
    #     Constructor
    #     Parameters:
    #         redshift_conn_id (string): the name of redshift connection
    #         aws_credentials_id (string): the name of AWS IAM connection
    #         table (string): the name of table will be created and filled
    #         create_sql_stmt (string): SQL Query which creates the table
    #         s3_bucket (string): s3 bucket
    #         s3_key (string): s3 key
    #         region (string): the location of the s3 and redshift cluster
    #         parquet (string): copy from parquet format

    #     '''
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 create_sql_stmt="",
                 s3_bucket="",
                 s3_key="",
                 region='',
                 table_type="",
                 *args, **kwargs):
        super(LoadToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.aws_credentials_id = aws_credentials_id
        self.start_date = kwargs['start_date']
        self.execution_date = kwargs['execution_date']
        self.table_type = table_type

    def execute(self, context):
        if self.table_type=="static" and self.execution_date > self.start_date + dt.timedelta(days=1):
            return
        self.log.info('Starting Connect to Redshift')
        # connect to redshift
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Create Redshift table")
        redshift.run(self.create_sql_stmt)

        self.log.info("Clearing data from destination Redshift table")

        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        # interpolate the data received in arguments to copy_sql
        formatted_sql = LoadToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
        )
        # load the data from S3 to redshift
        redshift.run(formatted_sql)