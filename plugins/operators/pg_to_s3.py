import os
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException


class PostgresToS3Operator(BaseOperator):
    """
    Кастомный оператор для потоковой выгрузки данных из PostgreSQL
    и загрузки их в S3 (MinIO) в формате Parquet чанками (защита от OOM).
    """
    template_fields = ('s3_key', 'sql_query')

    def __init__(
            self,
            sql_query: str,
            s3_bucket: str,
            s3_key: str,
            pg_conn_id: str = 'postgres_source_conn',
            aws_conn_id: str = 'minio_s3_conn',
            chunk_size: int = 50000,  # <-- Добавили размер чанка
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql_query = sql_query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.pg_conn_id = pg_conn_id
        self.aws_conn_id = aws_conn_id
        self.chunk_size = chunk_size

    def _get_db_engine(self):
        # PostgresHook берет настройки подключения (логин, пароль, хост) из Airflow Connections
        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        # Для работы Pandas (функции read_sql_query) требуется именно SQLAlchemy движок, а не обычный коннектор
        return pg_hook.get_sqlalchemy_engine()

    def _extract_and_write_chunks_to_temp(self, engine, tmp_file_path: str) -> int:
        writer = None
        total_rows = 0

        # Используем параметр chunksize: запрос возвращает не один огромный DataFrame, 
        # а генератор (итератор), который выдает данные кусками. Это спасает от нехватки памяти (OOM).
        for chunk_df in pd.read_sql_query(self.sql_query, con=engine, chunksize=self.chunk_size):
            if chunk_df.empty:
                continue

            total_rows += len(chunk_df)
            
            # Преобразуем Pandas DataFrame в формат PyArrow Table (он нужен для сохранения в Parquet)
            table = pa.Table.from_pandas(chunk_df)

            if writer is None:
                # Инициализируем ParquetWriter только на первом чанке, чтобы задать схему (типы колонок)
                writer = pq.ParquetWriter(tmp_file_path, table.schema)

            # Дозаписываем данные текущего чанка в файл на диске
            writer.write_table(table)
            self.log.info(f"Выгружено {total_rows} строк...")

        if writer is not None:
            # Важно закрыть writer, чтобы корректно завершить формирование Parquet-файла и сбросить буферы
            writer.close()
            
        return total_rows

    def _upload_to_s3(self, tmp_file_path: str):
        # S3Hook берет ключи от MinIO (AWS) из Airflow Connections
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        
        # Загружаем локальный временный файл в бакет S3
        s3_hook.load_file(
            filename=tmp_file_path,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True  # Перезаписываем файл, если он уже существует (поддерживаем идемпотентность)
        )
        self.log.info(f"Успешно загружено в s3://{self.s3_bucket}/{self.s3_key}")

    def _cleanup(self, tmp_file_path: str):
        # Обязательно удаляем временный файл с диска воркера Airflow.
        # Если этого не делать, место на сервере со временем закончится.
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
            self.log.info("Временный файл очищен.")

    def execute(self, context):
        self.log.info(f"Выполняем запрос к БД (чтение чанками по {self.chunk_size} строк): {self.sql_query}")

        # 1. Получаем подключение к БД
        engine = self._get_db_engine()

        # 2. Создаем временный файл. delete=False нужно, чтобы файл не удалился 
        # при выходе из блока with, так как мы будем передавать путь к нему в другие функции.
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            tmp_file_path = tmp_file.name

        try:
            # 3. Читаем данные из базы и пишем их во временный файл чанками
            total_rows = self._extract_and_write_chunks_to_temp(engine, tmp_file_path)

            # 4. Если данных нет - останавливаем таску (статус Skipped, а не Failed)
            if total_rows == 0:
                self.log.warning("Запрос не вернул данных! Файл загружаться не будет.")
                raise AirflowSkipException("Нет данных для выгрузки. Пропускаем таску.")

            self.log.info(f"Выгрузка завершена. Всего строк: {total_rows}. Грузим файл в S3...")
            
            # 5. Выгружаем готовый файл в S3
            self._upload_to_s3(tmp_file_path)

        finally:
            # 6. Блок finally гарантирует, что файл удалится в любом случае
            # (даже если во время работы произошла ошибка или отработал AirflowSkipException)
            self._cleanup(tmp_file_path)