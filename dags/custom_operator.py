from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


# custom operator class
class PostgreSQLCountRows(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            table_name: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id='postgres_conn')
        query = hook.get_first(f'SELECT COUNT(*) from {self.table_name}')
        print(query[0])
        return query[0]
