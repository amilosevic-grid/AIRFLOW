from datetime import datetime
from airflow import DAG
from airflow.operators.bash import  BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import uuid


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


def check_if_table_exists(table_name):
    """ method to check that table exists """

    hook = PostgresHook(postgres_conn_id='postgres_conn')
    query = hook.get_first(f'''SELECT * from information_schema.tables
                               where table_name = \'{table_name}\' and table_schema = \'public\'''')
    if query:
        return 'insert_new_row'
    else:
        return 'create_table'


def print_context():
    context = get_current_context()
    run_id = context['run_id']
    return f'{run_id} ended'


def query_table(table_name):
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    query = hook.get_records(f'SELECT * from {table_name}')
    for q in query:
        print(q)


def print_to_log(dag_id, database):
    print(f'{dag_id} started processing tables in database: {database}')
    return f'{dag_id} started processing tables in database: {database}'


def create_dag(dag_id, schedule_interval, start_date):
    dag = DAG(dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date)
    print_logs = PythonOperator(
        python_callable=print_to_log,
        task_id='print_to_log',
        op_kwargs={'dag_id': dag_id, 'database': 'postgres'},
        dag=dag,
        do_xcom_push=True
    )
    get_user = BashOperator(
        task_id='get_user',
        bash_command='whoami',
        dag=dag,
        do_xcom_push=True
    )
    table_name = config[dag_id]['table_name']
    insert_row = PostgresOperator(
        postgres_conn_id='postgres_conn',
        task_id='insert_new_row',
        sql=f'''
            INSERT INTO {table_name} VALUES
            (%s, \'{{{{ ti.xcom_pull(task_ids='get_user') }}}}\', %s);
             ''',
        trigger_rule='none_failed',
        parameters=[
            uuid.uuid4().int % 123456789,
            datetime.now()
        ],
        dag=dag
    )
    check_table_exist = BranchPythonOperator(
        task_id='check_table_exist',
        python_callable=check_if_table_exists,
        dag=dag,
        op_args=[table_name]
    )
    query_the_table = PythonOperator(
        task_id='query_the_table',
        dag=dag,
        do_xcom_push=True,
        python_callable=query_table,
        op_args=[table_name]
    )
    create_table = PostgresOperator(
        postgres_conn_id='postgres_conn',
        task_id='create_table',
        sql=f'''
            CREATE TABLE {table_name}(
            custom_id integer NOT NULL,
            user_name VARCHAR (50) NOT NULL, 
            timestamp TIMESTAMP NOT NULL);
                ''',
        dag=dag
    )
    print_logs >> get_user >> check_table_exist >> [create_table, insert_row]
    create_table >> insert_row >> query_the_table
    return dag


for dag_id, value in config.items():
    globals()[dag_id] = create_dag(dag_id,
                                   value['schedule_interval'],
                                   value['start_date'])

