import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from moex.config.config import TICKERS

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

ticker_list = ",".join([t["ticker"] for t in TICKERS])
DAGS_FOLDER = '/opt/airflow/dags'

BRONZE_READY = Dataset('s3://moex-bronze/ingestion_complete')
SILVER_READY = Dataset('s3://moex-bronze/silver_complete')

with DAG(
    dag_id='02_moex_spark_silver',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # Запускается автоматически, как только DAG 01 обновит BRONZE_READY
    schedule=[BRONZE_READY],
    catchup=False,
    max_active_runs=1,
    tags=['moex', 'silver', 'spark'],
    template_searchpath=[os.path.join(DAGS_FOLDER, 'moex', 'sql')]
) as dag:
    clear_staging = SQLExecuteQueryOperator(
        task_id='clear_staging_for_date',
        conn_id='postgres_dwh_conn',
        sql='staging/clear_staging.sql',
    )

    spark_process = SparkSubmitOperator(
        task_id='spark_bronze_to_silver',
        application='/opt/spark_jobs/bronze_to_silver.py',
        conn_id='spark_default',
        # Секреты передаются через env-переменные, а не аргументы CLI.
        # В логах и ps aux они не отображаются.
        env_vars={
            'SPARK_PG_USER':     '{{ conn.postgres_dwh_conn.login }}',
            'SPARK_PG_PASSWORD': '{{ conn.postgres_dwh_conn.password }}',
            'SPARK_S3_USER':     '{{ conn.minio_s3_conn.login }}',
            'SPARK_S3_PASSWORD': '{{ conn.minio_s3_conn.password }}',
        },
        application_args=['{{ macros.ds_add(ds, -1) }}', ticker_list],
        packages='org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf={
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g"
        },
        name='moex_bronze_to_silver_app',
        outlets=[SILVER_READY],
    )

    clear_staging >> spark_process