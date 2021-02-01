from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.models.variable import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
import logging

default_path = '/Users/aleksandarmilosevic/PycharmProjects/AIRFLOW/run.txt'
path = Variable.get('path_to_file', default_var=default_path)

def print_res(**context):
    # context = get_current_context()
    ti = context['ti']
    print('=======================================')
    print(ti.xcom_pull(task_ids='query_the_table', dag_id='dag_id_3'))
    print('=======================================')
    print(context)
    print('=======================================')



def create_sub_dag(parent_dag, start_date, schedule_interval):
    with DAG(dag_id=f'{parent_dag}.process_results_dag', start_date=start_date, schedule_interval=schedule_interval) as dag:
        task_sensor = ExternalTaskSensor(
            task_id='task_sensor',
            external_dag_id='dag_id_3',
            external_task_id=None,
            poke_interval=15
        )
        print_results = PythonOperator(
            task_id='print_results',
            python_callable=print_res
        )
        remove_file = BashOperator(
            task_id='remove_file',
            bash_command=f'rm -f {path}'
        )
        create_timestamp = BashOperator(
            task_id='create_timestamp',
            bash_command='touch ~/timestamp_{{ ts_nodash }}',
        )
        task_sensor >> print_results >> remove_file >> create_timestamp
    return dag

def pusher():
    date = '{{ ds }}'
    return date


with DAG(dag_id='trigger_run', start_date=datetime(2021, 1, 26), schedule_interval='@once') as dag:
    check_for_file = FileSensor(
        task_id='check_for_file',
        filepath=path,
        poke_interval=5
    )
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id='dag_id_3',
        execution_date='{{ execution_date }}'
    )
    process_results = SubDagOperator(
        task_id='process_results_dag',
        subdag=create_sub_dag(dag.dag_id, start_date=datetime(2021, 1, 26), schedule_interval='@once'),
        dag=dag,
    )

    check_for_file >> trigger_dag >> process_results
    globals()['trigger_run'] = dag


