from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
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


def print_to_log(dag_id, database):
    # print(f'{dag_id} started processing tables in database: {database}')
    logging.info(f'{dag_id} started processing tables in database: {database}')


def create_dag(dag_id, schedule_interval, start_date):
    dag = DAG(dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date)
    t1 = PythonOperator(
        python_callable=print_to_log,
        task_id='print_to_log',
        op_kwargs={'dag_id': dag_id, 'database': 'postgre'},
        dag=dag
    )
    t2 = DummyOperator(
        task_id='insert_new_row',
        dag=dag
    )
    t3 = DummyOperator(
        task_id='query_the_table',
        dag=dag
    )
    t1 >> t2 >> t3
    return dag


for dag_id, value in config.items():
    dag = create_dag(dag_id, value['schedule_interval'], value['start_date'])

