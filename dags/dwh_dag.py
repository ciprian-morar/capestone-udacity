from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.load_to_s3 import LoadToS3Operator
from operators.tables_redshift import FillTablesOperator
from operators.data_quality import DataQualityOperator
from airflow.operators import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

from helpers.sql_queries import imm_stage_table_create, demo_dim_table_create, \
    country_stage_table_create, person_dim_table_create,\
    person_dim_table_insert, time_dim_table_create,\
    time_dim_table_insert, imm_fact_table_create, \
    imm_fact_table_insert, visa_dim_table_create, \
    mode_dim_table_create, port_dim_table_create

from dwh_subdag import get_s3_to_redshift_dag

config = configparser.ConfigParser()
config.read('/usr/local/airflow/dags/ini.cfg')

bucket_name = config['AWS']['bucket_name']
region_name = config['AWS']['region_name']
scripts_path_key = config['AWS']['scripts_path_key']
data_path_key = config['AWS']['data_path_key']
bucket_logs = config['AWS']['bucket_logs']
processed_tables_key = config['AWS']['processed_tables_key']

default_args = {
    "owner": "airflow",
    "start_date": datetime(2016, 4, 1),
    "depends_on_past": True,
    "wait_for_downstream": True,
}

dag = DAG(
    "dwh_dag",
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id='start_data_pipeline',  dag=dag)
#cluster config
JOB_FLOW_OVERRIDES = {
    "Name": "Capestone Udacity Immigrants",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "Livy"}, {"Name": "Hive"}], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
        'Ec2KeyName': 'capestone',
        'EmrManagedMasterSecurityGroup': 'sg-005d2c716cf4f6962',
        'EmrManagedSlaveSecurityGroup': 'sg-03f5c655dc15cb7a8',
    },

    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    'LogUri': 's3://' + bucket_logs
}

SPARK_STEPS = [ # Note the params values are supplied to the operator
        {
                "Name": "Move raw data from S3 to HDFS",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "aws",
                        "s3",
                        "cp",
                        "s3://{{ params.bucket_name }}/{{params.scripts_path_key}}",
                        "/home/hadoop/",
                        "--recursive"
                    ],
                },
        },
        {
                    "Name": "Submit immigration script",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "--py-files",
                            "/home/hadoop/common.py,/home/hadoop/spark_datetime.py",
                            "--master",
                            "yarn",
                            "/home/hadoop/immigration_data.py",
                            "--bucketName",
                            "{{params.bucket_name}}",
                            "--dataPathKey",
                            "{{params.data_path_key}}",
                            "--processedTablesKey",
                            "{{params.processed_tables_key}}"
                        ],
                    },
                },
                {
                    "Name": "Submit check data quality script",
                    "ActionOnFailure": "CANCEL_AND_WAIT",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "--master",
                            "yarn",
                            "/home/hadoop/check_data_quality.py",
                            "--bucketName",
                            "{{params.bucket_name}}",
                            "--checkTables",
                            "second_check",
                            "--processedTablesKey",
                            "{{params.processed_tables_key}}"
                        ],
                    },
                },
]
# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
step_one = EmrAddStepsOperator(
    task_id="step_one",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "bucket_name": bucket_name,
        "scripts_path_key": scripts_path_key + "/",
        "data_path_key": data_path_key,
        "processed_tables_key": processed_tables_key
    },
    dag=dag,
)




last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='step_one', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# #Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

load_stage_immigration_task_id = 'load_stage_immigration_subdag'
load_immigration_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "dwh_dag",
        load_stage_immigration_task_id,
        "redshift",
        "aws_default",
        "imm_stage",
        create_sql_stmt=imm_stage_table_create,
        s3_bucket=bucket_name,
        s3_key=processed_tables_key + "/immigration",
        region='us-west-2',
        table_type="variable",
        start_date=default_args.get("start_date"),

    ),
    task_id=load_stage_immigration_task_id,
    dag=dag,
    provide_context=True
)

load_dim_demo_task_id = 'load_dim_demo_subdag'
load_demo_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "dwh_dag",
        load_dim_demo_task_id,
        "redshift",
        "aws_default",
        "dim_demo",
        create_sql_stmt=demo_dim_table_create,
        s3_bucket=bucket_name,
        s3_key=processed_tables_key + "/demographics",
        region='us-west-2',
        table_type="static",
        start_date=default_args.get("start_date"),
    ),
    task_id=load_dim_demo_task_id,
    dag=dag,
    provide_context=True
)

load_stage_country_task_id = 'load_stage_country_subdag'
load_country_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "dwh_dag",
        load_stage_country_task_id,
        "redshift",
        "aws_default",
        "country_stage",
        create_sql_stmt=country_stage_table_create,
        s3_bucket=bucket_name,
        s3_key=processed_tables_key + "/country",
        region='us-west-2',
        table_type="static",
        start_date=default_args.get("start_date")

    ),
    task_id=load_stage_country_task_id,
    dag=dag,
    provide_context=True
)

load_stage_port_task_id = 'load_stage_port_subdag'
load_port_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "dwh_dag",
        load_stage_port_task_id,
        "redshift",
        "aws_default",
        "port_stage",
        create_sql_stmt=demo_dim_table_create,
        s3_bucket=bucket_name,
        s3_key=processed_tables_key + "/port-dict",
        region='us-west-2',
        table_type="static",
        start_date=default_args.get("start_date"),

    ),
    task_id=load_stage_port_task_id,
    dag=dag,
    provide_context=True
)

load_dim_visa_task_id = 'load_dim_visa_subdag'
load_visa_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "dwh_dag",
        load_dim_visa_task_id,
        "redshift",
        "aws_default",
        "visa_dim",
        create_sql_stmt=visa_dim_table_create,
        s3_bucket=bucket_name,
        s3_key=processed_tables_key + "/visa",
        region='us-west-2',
        table_type="static",
        start_date=default_args.get("start_date"),

    ),
    task_id=load_dim_visa_task_id,
    dag=dag,
    provide_context=True
)

load_dim_mode_task_id = 'load_dim_mode_subdag'
load_mode_to_redshift = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "dwh_dag",
        load_dim_mode_task_id,
        "redshift",
        "aws_default",
        "mode_dim",
        create_sql_stmt=demo_dim_table_create,
        s3_bucket=bucket_name,
        s3_key=processed_tables_key + "/mode",
        region='us-west-2',
        table_type="static",
        start_date=default_args.get("start_date"),

    ),
    task_id=load_dim_mode_task_id,
    dag=dag,
    provide_context=True,
)

load_person_dim_table = FillTablesOperator(
    task_id='Load_person_dim_table',
    dag=dag,
    table="person_dim",
    redshift_conn_id="redshift",
    create_table=person_dim_table_create,
    insert_table=person_dim_table_insert,
    start_date=default_args.get("start_date")
)

load_time_dim_table = FillTablesOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time_dim",
    redshift_conn_id="redshift",
    create_table=time_dim_table_create,
    insert_table=time_dim_table_insert,
    start_date=default_args.get("start_date")
)

load_imm_fact_table = FillTablesOperator(
    task_id='Load_imm_fact_table',
    dag=dag,
    table="imm_fact",
    redshift_conn_id="redshift",
    create_table=imm_fact_table_create,
    insert_table=imm_fact_table_insert,
    start_date=default_args.get("start_date")
)

query_checks=[
    	{'check_sql': "SELECT COUNT(*) FROM imm_fact WHERE  imm_fact_id", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM time_dim WHERE time_id is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM person_dim WHERE person_id is null", 'expected_result':0},
    	{'check_sql': "SELECT COUNT(*) FROM demo_dim WHERE state_code is null", 'expected_result':0},
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    query_checks=query_checks,
    table_names=('imm_fact', 'time_dim', 'person_dim', 'demo_dim')
)

end_data_pipeline = DummyOperator(task_id='Stop_execution', dag=dag)

start_data_pipeline >> create_emr_cluster >> step_one >> step_checker
step_checker >> terminate_emr_cluster
terminate_emr_cluster >> load_immigration_to_redshift >> load_demo_to_redshift
load_demo_to_redshift >> load_country_to_redshift >> load_port_to_redshift
load_port_to_redshift >> load_visa_to_redshift >> load_mode_to_redshift
load_mode_to_redshift >> load_person_dim_table >> load_time_dim_table
load_time_dim_table >> load_imm_fact_table >> end_data_pipeline



