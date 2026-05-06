import tempfile
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException

class PostgresToS3Operator(BaseOperator):
    """
    Кастомный оператор для выгрузки данных из PostgreSQL и загрузки их в S3 (MinIO) в формате Parquet.
    """
    template_fields = ('s3_key', 'sql_query')

    def __init__(
            self,
            sql_query: str,
            s3_bucket: str,
            s3_key: str,
            pg_conn_id: str = 'postgres_source_conn',
            aws_conn_id: str = 'minio_s3_conn',
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql_query = sql_query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.pg_conn_id = pg_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info(f"Выполняем запрос к БД: {self.sql_query}")

        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # Выгружаем данные в Pandas DataFrame
        df = pg_hook.get_pandas_df(self.sql_query)
        if df.empty:
            self.log.warning(f"Запрос не вернул данных! Файл создаваться не будет.")
            raise AirflowSkipException("Нет данных для выгрузки. Пропускаем таску.")

        self.log.info(f"Получено {len(df)} строк. Пишем во временный Parquet...")

        # Создаем временный файл Parquet
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            df.to_parquet(tmp_file.name, index=False)
            tmp_file_path = tmp_file.name

        self.log.info("Грузим файл в S3...")

        # Загружаем в MinIO
        s3_hook.load_file(
            filename=tmp_file_path,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
        self.log.info(f"Успешно загружено в s3://{self.s3_bucket}/{self.s3_key}")