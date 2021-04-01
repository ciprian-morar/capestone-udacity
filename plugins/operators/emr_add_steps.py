from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator

#I've created my own EmrAddStepsOperator
# to be able to use macros.ds in steps
class MyEmrAddStepsOperator(EmrAddStepsOperator):

    template_fields = ['job_flow_id','steps', 'params']