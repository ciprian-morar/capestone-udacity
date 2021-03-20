from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.load_to_s3 import LoadToS3Operator
from airflow.operators import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

BUCKET_NAME = "capestone-udacity-project"
region_name = 'us-west-2'
scripts_rar_path = "scripts/spark.rar"
default_args = {
    "owner": "airflow",
    "start_date": datetime(2016, 1, 1),
    "depends_on_past": True,
    "wait_for_downstream": True,
}



dag = DAG(
    "process_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)



JOB_FLOW_OVERRIDES = {
    "Name": "Capestone Udacity Immigrants",
    "ReleaseLabel": "emr-5.28.0",
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
        'EmrManagedMasterSecurityGroup': 'sg-005d2c716cf4f6962',
        'EmrManagedSlaveSecurityGroup': 'sg-a061f99b',
        'Ec2KeyName': 'capestone',
    },

    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    'LogUri': 's3://aws-emr-ciprian-logs'
}

SPARK_STEPS = [ # Note the params values are supplied to the operator

    {
            "Name": "Move raw data from S3 to HDFS",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "s3-dist-cp",
                    "--src=s3://{{ params.BUCKET_NAME }}/{{params.scripts_rar_path}}",
                    "--dest=/scripts",
                ],
            },
    },

]

# {
#     "Name": "Move scripts from S3 to HDFS",
#     "ActionOnFailure": "CANCEL_AND_WAIT",
#     "HadoopJarStep": {
#         "Jar": "command-runner.jar",
#         "Args": [
#             "s3-dist-cp",
#             "--src=s3://{{ params.BUCKET_NAME }}/data",
#             "--dest=/movie",
#         ],
#     },
# },
#
# {
#     "Name": "Submit Scripts",
#     "ActionOnFailure": "CANCEL_AND_WAIT",
#     "HadoopJarStep": {
#         "Jar": "command-runner.jar",
#         "Args": [
#             "spark-submit",
#             "--master",
#             "yarn"
#             "--deploy-mode",
#             "cluster",
#             "s3://{{ params.BUCKET_NAME }}/{{ params.s3_weather_script }}",
#         ],
#     },
# }

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

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
        "BUCKET_NAME": BUCKET_NAME,
        "scripts_rar_path": scripts_rar_path
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
# terminate_emr_cluster = EmrTerminateJobFlowOperator(
#     task_id="terminate_emr_cluster",
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
#     aws_conn_id="aws_default",
#     dag=dag,
# )

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

# start_data_pipeline >> [data_to_s3, script_to_s3] >> create_emr_cluster
start_data_pipeline >> create_emr_cluster >> step_one >> step_checker >> end_data_pipeline
# >> terminate_emr_cluster
# terminate_emr_cluster >> end_data_pipeline
