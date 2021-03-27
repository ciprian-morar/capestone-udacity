from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.load_to_s3 import LoadToS3Operator


config = configparser.ConfigParser()
try:
    config.read('/usr/local/airflow/dags/ini.cfg')
except Exception as e:
    print(str(e), 'coud not read configuration file')

try:
    bucket_name = config.get('AWS','bucket_name')
    region_name = config.get('AWS','region_name')
    data_path_key = config.get('AWS','data_path_key')
    scripts_path_key = config.get('AWS','scripts_path_key')
    absolute_path_docker_machine = config['DOCKER']['absolute_path_docker_machine']
    relative_path_data_source = config['DOCKER']['relative_path_data_source']
    relative_path_scripts_source = config['DOCKER']['relative_path_scripts_source']
except Exception as e:
    print(str(e), 'could not read keys in  configuration file')


default_args = {
    "owner": "airflow",
    "start_date": datetime(2016, 1, 4),
    "depends_on_past": True,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "upload_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

#upload dictionaries files
# upload_additional_tables= LoadToS3Operator(
#     task_id='Load_dict_tables_to_s3',
#     dag=dag,
#     aws_conn_id="aws_default",
#     bucket_name="original-demo-ciprian",
#     key=data_path_key,
#     relative_local_path=absolute_path_docker_machine + relative_path_data_source + "/additional_tables/",
#     region_name=region_name
# )
#
# upload data csv files
# upload_csv_files= LoadToS3Operator(
#     task_id='Load_csv_to_s3',
#     dag=dag,
#     aws_conn_id="aws_default",
#     bucket_name="original-demo-ciprian",
#     key=data_path_key,
#     relative_local_path=absolute_path_docker_machine + relative_path_data_source + "/csv_files/",
#     region_name=region_name
# )

# upload emr scripts to s3
# upload_scripts_files= LoadToS3Operator(
#     task_id='Load_scripts_to_s3',
#     dag=dag,
#     aws_conn_id="aws_default",
#     bucket_name="original-demo-ciprian",
#     key=scripts_path_key,
#     relative_local_path=absolute_path_docker_machine + relative_path_scripts_source + "/",
#     region_name=region_name,
# )

#upload a specific file
upload_scripts_files= LoadToS3Operator(
    task_id='Load_scripts_to_s3',
    dag=dag,
    aws_conn_id="aws_default",
    bucket_name="original-demo-ciprian",
    key=scripts_path_key,
    relative_local_path=absolute_path_docker_machine + relative_path_scripts_source + "/",
    region_name=region_name,
    filename = "immigration_data.py",
    specific_file = True
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_data_pipeline >> upload_scripts_files >> end_data_pipeline
# start_data_pipeline >> upload_additional_tables >> upload_csv_files >> upload_scripts_files >> end_data_pipeline

