from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.load_to_s3 import LoadToS3Operator

region_name = 'us-west-2'
absolute_path='/usr/local/airflow'
relative_path_data_source = '/data/source'
relative_path_scripts_source = '/scripts/'
default_args = {
    "owner": "airflow",
    "start_date": datetime(2016, 1, 1),
    "depends_on_past": True,
    "wait_for_downstream": True,
}

dag = DAG(
    "test_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

upload_additional_tables= LoadToS3Operator(
    task_id='Load_dict_tables_to_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket_name="capestone-udacity-project",
    key="data/source/staging/",
    relative_local_path=absolute_path + relative_path_data_source + "/additional_tables/",
    region_name=region_name
)


upload_csv_files= LoadToS3Operator(
    task_id='Load_csv_to_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket_name="capestone-udacity-project",
    key="data/source/staging/",
    relative_local_path=absolute_path + relative_path_data_source + "/csv_files/",
    region_name=region_name
)

upload_scripts_files= LoadToS3Operator(
    task_id='Load_scripts_to_s3',
    dag=dag,
    aws_conn_id="aws_conn_id",
    bucket_name="capestone-udacity-project",
    key="scripts/",
    relative_local_path=absolute_path + relative_path_scripts_source,
    region_name=region_name,
    filename="spark.rar",
    specific_file=True
)


# Terminate the EMR cluster
# terminate_emr_cluster = EmrTerminateJobFlowOperator(
#     task_id="terminate_emr_cluster",
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
#     aws_conn_id="aws_default",
#     dag=dag,
# )

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

# start_data_pipeline >> [data_to_s3, script_to_s3] >> create_emr_cluster
start_data_pipeline >> upload_additional_tables >> upload_csv_files >> upload_scripts_files >> end_data_pipeline
# >> terminate_emr_cluster
# terminate_emr_cluster >> end_data_pipeline
