from datetime import datetime
from airflow import DAG
from airflow.operators.bash import  BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import logging


config = {
   'dag_id_1': {
       'schedule_interval': None,
       'start_date': datetime(2021, 1, 26),
       'table_name': 'table_1'},
   'dag_id_2': {
       'schedule_interval': None,
       'start_date': datetime(2021, 1, 26),
       'table_name': 'table_2'},
   'dag_id_3': {
       'schedule_interval': None,
       'start_date': datetime(2021, 1, 26),
       'table_name': 'table_3'}}


def branch_func():
   """ method to check that table exists """
   if True:
       return 'insert_new_row'
   return 'insert_new_row'


def print_to_log(dag_id, database):
    logging.info(f'{dag_id} started processing tables in database: {database}')


def create_dag(dag_id, schedule_interval, start_date):
    dag = DAG(dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date)
    print_logs = PythonOperator(
        python_callable=print_to_log,
        task_id='print_to_log',
        op_kwargs={'dag_id': dag_id, 'database': 'sqlite'},
        dag=dag
    )
    get_user = BashOperator(
        task_id='get_user',
        bash_command='whoami',
        dag=dag
    )
    insert_row = DummyOperator(
        task_id='insert_new_row',
        trigger_rule='none_failed',
        dag=dag
    )
    check_table_exist = BranchPythonOperator(
        task_id='check_table_exist',
        python_callable=branch_func,
        dag=dag
    )
    query_the_table = DummyOperator(
        task_id='query_the_table',
        dag=dag,
        do_xcom_push="{{run_id}} ended"
    )
    create_table = DummyOperator(
        task_id='create_table',
        dag=dag
    )
    print_logs >> get_user >> check_table_exist >> [create_table, insert_row]
    create_table >> insert_row >> query_the_table
    return dag


for dag_id, value in config.items():
    globals()[dag_id] = create_dag(dag_id,
                                   value['schedule_interval'],
                                   value['start_date'])

