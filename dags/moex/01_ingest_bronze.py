from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Импортируем наши кастомные операторы
from operators.api_to_s3 import MoexApiToS3Operator
from operators.pg_to_s3 import PostgresToS3Operator
from moex.config.config import TICKERS

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='01_moex_ingest_bronze',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['moex', 'bronze', 'ingestion']
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Таска: Выгружаем справочник из базы в S3
    extract_refs = PostgresToS3Operator(
        task_id='extract_emitents_to_s3',
        sql_query='SELECT * FROM emitents;',
        s3_bucket='moex-bronze',
        s3_key='refs/emitents/{{ ds }}/emitents.parquet',  # <-- Изменили расширение
        pg_conn_id='postgres_source_conn',
        aws_conn_id='minio_s3_conn'
    )

    start >> extract_refs

    # Таски: Динамически создаем выгрузку для каждого тикера из API
    for t_info in TICKERS:
        ticker = t_info["ticker"]
        interval = t_info["interval"]

        extract_api = MoexApiToS3Operator(
            task_id=f'extract_api_{ticker}',
            ticker=ticker,
            interval=interval,
            api_start_date='{{ data_interval_start | ds }}',
            api_end_date='{{ data_interval_end | ds }}',
            s3_bucket='moex-bronze',
            s3_key=f'raw_api/{ticker}/{{{{ ds }}}}/{ticker}_candles.json',
            aws_conn_id='minio_s3_conn'
        )

        extract_refs >> extract_api >> end