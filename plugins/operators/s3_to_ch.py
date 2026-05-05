# plugins/operators/pg_to_s3.py
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import tempfile


class PgToS3Operator(BaseOperator):
    def __init__(self, schema, table, s3_bucket, s3_key, postgres_conn_id, s3_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        # Получаем данные за конкретную дату из витрины
        ds = context['ds']
        sql = f"SELECT * FROM {self.schema}.{self.table} WHERE trade_date = '{ds}'"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as tmp:
            pg_hook.copy_expert(f"COPY ({sql}) TO STDOUT WITH CSV HEADER", tmp.name)
            tmp.flush()
            s3_hook.load_file(filename=tmp.name, key=self.s3_key, bucket_name=self.s3_bucket, replace=True)