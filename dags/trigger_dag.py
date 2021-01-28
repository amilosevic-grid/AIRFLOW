from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.models.variable import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
import logging


def create_sub_dag(parent_dag, start_date, schedule_interval):
    with DAG(dag_id=f'{parent_dag}.process_results_dag', start_date=start_date, schedule_interval=schedule_interval) as dag:
        remove_file = BashOperator(
            task_id='remove_file',
            bash_command='rm /Users/aleksandarmilosevic/PycharmProjects/AIRFLOW/run.txt'
        )
        print_results = PythonOperator(
            task_id='print_results',
            python_callable=lambda _: print('Sensor was triggered')
        )
        task_sensor = ExternalTaskSensor(
            task_id='task_sensor',
            external_dag_id=parent_dag,
            external_task_id=None,
            timeout=600,
            allowed_states=['success'],
            failed_states=['failed'],
            mode="reschedule"
        )
        create_timestamp = BashOperator(
            task_id='create_timestamp',
            bash_command='touch timestamp.txt',
        )
        task_sensor >> print_results >> remove_file >> create_timestamp
    return dag

def pusher():
    date = '{{ ds }}'
    return date

path = '/Users/aleksandarmilosevic/PycharmProjects/AIRFLOW/run.txt'
# path = Variable.get('path_to_file', default_var='run')
with DAG(dag_id='trigger_run', start_date=datetime(2021, 1, 26), schedule_interval='@once') as dag:
    check_for_file = FileSensor(
        task_id='check_for_file',
        filepath=path
    )
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='dag_id_1',
        execution_date='{{ ds }}'
    )
    process_results = SubDagOperator(
        task_id='process_results_dag',
        subdag=create_sub_dag(dag.dag_id, start_date=datetime(2021, 1, 26), schedule_interval='@once'),
        dag=dag
    )

    check_for_file >> trigger_dag >> process_results
    globals()['trigger_run'] = dag


