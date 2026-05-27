import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from operators.pg_to_s3 import PostgresToS3Operator
from operators.s3_to_ch import S3ToClickhouseOperator

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DAGS_FOLDER = '/opt/airflow/dags'

GOLD_READY = Dataset('s3://moex-bronze/gold_complete')

with DAG(
    dag_id='04_moex_export_ch',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=[GOLD_READY],
    catchup=False,
    max_active_runs=1,
    tags=['moex', 'export', 'clickhouse'],
    template_searchpath=[os.path.join(DAGS_FOLDER, 'moex', 'sql')]
) as dag:
    # 1. Выгружаем витрину из Postgres в MinIO
    export_to_s3 = PostgresToS3Operator(
        task_id='export_dm_to_s3',
        pg_conn_id='postgres_dwh_conn',
        aws_conn_id='minio_s3_conn',
        sql_query="SELECT * FROM dm.dm_stock_analytics WHERE trade_date = '{{ macros.ds_add(ds, -1) }}'",
        s3_bucket='moex-export',
        s3_key='dm_stock_analytics/{{ macros.ds_add(ds, -1) }}.parquet',
    )

    # 2. Очищаем буферную таблицу
    truncate_buffer = ClickHouseOperator(
        task_id='truncate_buffer_table',
        clickhouse_conn_id='clickhouse_conn',
        sql='export/truncate_buffer.sql'
    )

    # 3. Загружаем свежий Parquet из S3 в буфер (креды берутся из Airflow conn)
    load_to_buffer = S3ToClickhouseOperator(
        task_id='load_to_buffer',
        clickhouse_table='analytics.dm_stock_analytics_buffer',
        s3_bucket='moex-export',
        s3_key='dm_stock_analytics/{{ macros.ds_add(ds, -1) }}.parquet',
        clickhouse_conn_id='clickhouse_conn',
        aws_conn_id='minio_s3_conn',
        file_format='Parquet',
    )

    # 4. Атомарная замена партиции
    replace_partition = ClickHouseOperator(
        task_id='replace_partition',
        clickhouse_conn_id='clickhouse_conn',
        sql='export/replace_partition.sql',
    )

    export_to_s3 >> truncate_buffer >> load_to_buffer >> replace_partition