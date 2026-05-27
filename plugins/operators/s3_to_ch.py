from airflow.models.baseoperator import BaseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3ToClickhouseOperator(BaseOperator):
    """
    Оператор для загрузки данных из S3 (MinIO) напрямую в ClickHouse.
    Использует нативную табличную функцию s3() в ClickHouse для максимальной скорости.
    """
    
    template_fields = ('s3_bucket', 's3_key', 'clickhouse_table')

    def __init__(
            self, 
            clickhouse_table: str, 
            s3_bucket: str, 
            s3_key: str, 
            clickhouse_conn_id: str = 'clickhouse_conn', 
            aws_conn_id: str = 'minio_s3_conn', 
            file_format: str = 'Parquet',
            **kwargs
    ):
        super().__init__(**kwargs)
        self.clickhouse_table = clickhouse_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.clickhouse_conn_id = clickhouse_conn_id
        self.aws_conn_id = aws_conn_id
        self.file_format = file_format

    def execute(self, context):
        ch_hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        
        # Достаем креды от S3 из Airflow Connection
        credentials = s3_hook.get_credentials()
        aws_conn = s3_hook.get_connection(self.aws_conn_id)
        
        # Извлекаем endpoint MinIO из connection.
        # Если schema не задана, используем http по умолчанию.
        endpoint_host = aws_conn.host if aws_conn.host else "minio:9000"
        
        if endpoint_host.startswith("http://") or endpoint_host.startswith("https://"):
            endpoint = endpoint_host
        else:
            endpoint_schema = aws_conn.schema if aws_conn.schema else "http"
            endpoint = f"{endpoint_schema}://{endpoint_host}"
        s3_url = f"{endpoint}/{self.s3_bucket}/{self.s3_key}"
        
        # Формируем запрос INSERT INTO ... SELECT * FROM s3(...)
        query = f"""
            INSERT INTO {self.clickhouse_table}
            SELECT * FROM s3('{s3_url}', '{credentials.access_key}', '{credentials.secret_key}', '{self.file_format}')
        """
        
        self.log.info(f"Запускаем pull данных из {s3_url} в таблицу {self.clickhouse_table}...")
        ch_hook.execute(query)
        self.log.info("Загрузка в ClickHouse успешно завершена!")