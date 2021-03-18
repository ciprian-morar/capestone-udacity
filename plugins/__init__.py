from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from airflow_plugin.operators.load_to_s3 import LoadToS3Operator


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "airflow_plugin"
    operators = [LoadToS3Operator]