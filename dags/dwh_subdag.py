# Instructions
# In this exercise, we’ll place our S3 to RedShift Copy operations into a SubDag.
# 1 - Consolidate HasRowsOperator into the SubDag
# 2 - Reorder the tasks to take advantage of the SubDag Operators

import datetime
import logging
from airflow import DAG
from operators.has_rows import HasRowsOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.load_to_redshift import LoadToRedshiftOperator


# """Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present."""
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        s3_bucket,
        s3_key,
        region,
        table_type,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    logging.info("create task")
    # create table task
    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    # copy table task
    copy_task = LoadToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table=table,
        create_sql_stmt=create_sql_stmt,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        region=region,
        table_type=table_type,
        provide_context=True,
        start_date=kwargs['start_date']

    )

    # check table task
    check_task = HasRowsOperator(
        task_id=f"check_{table}_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table
    )

    create_task >> copy_task
    copy_task >> check_task

    return dag