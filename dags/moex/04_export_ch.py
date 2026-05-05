import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from operators.pg_to_s3 import PostgresToS3Operator

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='04_moex_export_ch',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['moex', 'export', 'clickhouse']
) as dag:

    # 1. Выгружаем витрину из Postgres в MinIO (бакет moex-export)
    export_to_s3 = PostgresToS3Operator(
        task_id='export_dm_to_s3',
        pg_conn_id='postgres_dwh_conn',
        aws_conn_id='minio_s3_conn',
        sql_query="SELECT * FROM dm.dm_stock_analytics WHERE trade_date = '{{ ds }}'",
        s3_bucket='moex-export',
        s3_key='dm_stock_analytics/{{ ds }}.parquet' # Изменили на Parquet
    )

    # 2. Загружаем из S3 в ClickHouse через нативную s3 функцию
    import_to_ch = ClickHouseOperator(
        task_id='import_s3_to_clickhouse',
        clickhouse_conn_id='clickhouse_conn',
        sql="""
            INSERT INTO analytics.dm_stock_analytics
            SELECT * FROM s3(
                'http://minio:9000/moex-export/dm_stock_analytics/{{ ds }}.parquet',
                'admin', 
                'supersecretpassword', 
                'Parquet'
            );
        """
    )

    export_to_s3 >> import_to_ch