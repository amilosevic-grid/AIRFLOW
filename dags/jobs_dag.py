from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import uuid
from custom_operator import PostgreSQLCountRows


# dag arguments
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


# function that checks if the table exists
def check_if_table_exists(table_name):
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    query = hook.get_first(f'''SELECT * from information_schema.tables
                               where table_name = \'{table_name}\' and table_schema = \'public\'''')
    if query:
        return 'insert_new_row'
    else:
        return 'create_table'


# function that creates a single dag
def create_dag(dag_id, schedule_interval, start_date):
    dag = DAG(dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date)

    # task definitions

    # print_log task
    @dag.task()
    def print_to_log(dag_id, database):
        print(f'{dag_id} started processing tables in database: {database}')

    print_logs = print_to_log(dag_id, 'postgres')

    # bash task for getting user name
    get_user = BashOperator(
        task_id='get_user',
        bash_command='whoami',
        dag=dag,
        do_xcom_push=True,
        queue='jobs_queue'
    )

    table_name = config[dag_id]['table_name']

    # calls the function for checking table existence
    check_table_exist = BranchPythonOperator(
        task_id='check_table_exist',
        python_callable=check_if_table_exists,
        op_args=[table_name],
        dag=dag,
        queue='jobs_queue'
    )

    # inserts new row into the table
    insert_row = PostgresOperator(
        task_id='insert_new_row',
        postgres_conn_id='postgres_conn',
        sql=f'''
            INSERT INTO {table_name} VALUES
            (%s, \'{{{{ ti.xcom_pull(task_ids='get_user') }}}}\', %s);
             ''',
        parameters=[
            uuid.uuid4().int % 123456789,
            datetime.now()
        ],
        trigger_rule='none_failed',
        dag=dag,
        queue='jobs_queue'
    )
    # fetches results from the table
    query_the_table = PostgreSQLCountRows(
        task_id='query_the_table',
        do_xcom_push=True,
        table_name=table_name,
        dag=dag,
        queue='jobs_queue'
    )
    # creates a postgres table with table_name
    create_table = PostgresOperator(
        postgres_conn_id='postgres_conn',
        task_id='create_table',
        sql=f'''
            CREATE TABLE {table_name}(
            custom_id integer NOT NULL,
            user_name VARCHAR (50) NOT NULL, 
            timestamp TIMESTAMP NOT NULL);
                ''',
        dag=dag,
        queue='jobs_queue'
    )

    # setting task order
    print_logs >> get_user >> check_table_exist >> [create_table, insert_row]
    create_table >> insert_row >> query_the_table
    return dag


# forming all three dags from loop
for dag_id, value in config.items():
    globals()[dag_id] = create_dag(dag_id,
                                   value['schedule_interval'],
                                   value['start_date'])

