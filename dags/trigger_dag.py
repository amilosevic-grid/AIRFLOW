from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from slack import WebClient
from slack.errors import SlackApiError
from smart_sensor_file import SmartFileSensor
import logging


# paths to files
default_path = '/Users/aleksandarmilosevic/PycharmProjects/AIRFLOW/run.txt'
path = Variable.get('path_to_file', default_var=default_path)
# slack_token = Variable.get('slack_token')
slack_token = 'xoxb-1727046194672-1709378985940-A6HDRTXvZKqhpy8OFC7aBBOf'
external_dag = Variable.get('external_dag')
external_task = Variable.get('external_task')


# function for pulling value from query_table task
def print_res(task_id, dag_id, **context):
    ti = context['ti']
    logging.info(ti.xcom_pull(task_ids=task_id, dag_id=dag_id))


# function that sends a message to a slack channel
def slack_message(**context):
    client = WebClient(token=slack_token)
    run_id = context['run_id']
    exec_date = context['ds']
    try:
        response = client.chat_postMessage(
                channel="general",
                text=f"{run_id} finished  on date {exec_date}")
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'


# function that creates sub_dag
def create_sub_dag(parent_dag, sub_dag_name, start_date, schedule_interval):
    with DAG(dag_id=f'{parent_dag}.{sub_dag_name}', start_date=start_date, schedule_interval=schedule_interval) as dag:
        # senses if external dag has started
        task_sensor = ExternalTaskSensor(
            task_id='task_sensor',
            external_dag_id=external_dag,
            external_task_id=None,
            poke_interval=15
        )
        # prints results
        print_results = PythonOperator(
            task_id='print_results',
            python_callable=print_res,
            op_args=[external_task, external_dag]
        )
        # removes file
        remove_file = BashOperator(
            task_id='remove_file',
            bash_command=f'rm -f {path}'
        )
        # creates a file with appropriate timestamp
        create_timestamp = BashOperator(
            task_id='create_timestamp',
            bash_command='touch ~/timestamp_{{ ts_nodash }}',
        )
        task_sensor >> print_results >> remove_file >> create_timestamp
    return dag


# creates a dag
with DAG(dag_id='trigger_run', start_date=datetime(2021, 1, 26), schedule_interval='@once') as dag:
    # checks if a file exists
    check_for_file = SmartFileSensor(
        task_id='check_for_file',
        filepath=path
    )
    # triggers another dag
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id=external_dag,
        execution_date='{{ execution_date }}'
    )
    # creates a sub_dag that processes results
    process_results = SubDagOperator(
        task_id='process_results_dag',
        subdag=create_sub_dag(
            dag.dag_id,
            'process_results_dag',
            start_date=datetime(2021, 1, 26),
            schedule_interval='@once')
        ),
    # sends a slack  message
    alert_slack = PythonOperator(
        task_id='alert_slack',
        python_callable=slack_message
    )
    check_for_file >> trigger_dag >> process_results >> alert_slack
    globals()[dag.dag_id] = dag
