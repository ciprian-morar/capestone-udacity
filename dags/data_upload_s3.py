from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow_plugin.operators.load_to_s3 import LoadToS3Operator

region_name = 'us-west-2'
relative_path_data_source = '../../data/source/'
default_args = {
    'owner': 'ciprian',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup':False,
    'retries':0,
    'email_on_retry':False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Upload data to airflow',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

upload_additional_tables= LoadToS3Operator(
    task_id='Load_dict_tables_to_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket_name="capestone-udacity-project",
    key="data",
    relative_local_path=relative_path_data_source + "/additional_tables/",
    region_name=region_name
)

upload_csv_files= LoadToS3Operator(
    task_id='Load_csv_to_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket_name="capestone-udacity-project",
    key="data",
    relative_local_path=relative_path_data_source + "/csv_files/",
    region_name=region_name
)

upload_bdat_files= LoadToS3Operator(
    task_id='Load_csv_to_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket_name="capestone-udacity-project",
    key="data",
    relative_local_path=relative_path_data_source + "/i94_immigration_data/18-83510-I94-Data-2016/",
    region_name=region_name
)

end_operator = DummyOperator(task_id="End_execution", dag=dag)

start_operator >> upload_bdat_files
start_operator >> upload_csv_files
start_operator >> upload_additional_tables
upload_bdat_files >> end_operator
upload_csv_files >> end_operator
upload_additional_tables >> end_operator