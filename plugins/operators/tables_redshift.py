from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime as dt
import pytz
# """FillTablesOperator it's used to create DW Dimension and Fact tables and
# insert the data from staging tables"""
class FillTablesOperator(BaseOperator):

    ui_color = '#80BD9E'
#     '''Constructor LoadDimensionOperator
#        Parameters:
#             table (string): the name of the table
#             redshift_conn_id (string): the name of connection
#             create_table (string): The SQL query which create the table
#             insert_table (string): The SELECT statement from staging tables
#             dag_start_date (string): The start date of the dag
#     '''
    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                table,
                redshift_conn_id,
                create_table,
                insert_table,
                dag_start_date,
                 *args, **kwargs):

        super(FillTablesOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table = create_table
        self.insert_table = insert_table
        self.dag_start_date = dag_start_date

    def execute(self, context):
        self.log.info('FillTablesOperator')
        #connect to redshift with the PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #complete the insert statement
        table_insert = f"""
                    INSERT INTO {self.table}
                    {self.insert_table}
                """
        if self.table=="imm_fact":
            table_insert = f"""
                                INSERT INTO {self.table} 
                                (arrdate,
                                person_id,
                                state_code,
                                mode_code,
                                port_code,
                                visa_code
                                )
                                {self.insert_table}
                            """
        self.log.error(self.dag_start_date)
        self.log.error(context['execution_date'])
        #The flag allows to switch between append - only and delete-load functionality
        one_day_after = self.dag_start_date + dt.timedelta(days=1)
        execution_date = dt.datetime.strptime( context['ds'], '%Y-%m-%d')
        # delete table only for the first interation in the loop
        if execution_date < one_day_after:
            delete_statement = f'DROP TABLE IF EXISTS {self.table}'
            # run delete table statement
            redshift_hook.run(delete_statement)
            # run create table statement
            redshift_hook.run(self.create_table)
        redshift_hook.run(table_insert)