import os
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from moex.config.config import TICKERS

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

ticker_list = ",".join([t["ticker"] for t in TICKERS])

# Определяем абсолютный путь к папке dags внутри контейнера
DAGS_FOLDER = '/opt/airflow/dags'

with DAG(
    dag_id='02_moex_spark_silver',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['moex', 'silver', 'spark'],
    # Указываем Airflow, где искать SQL-файлы
    template_searchpath=[os.path.join(DAGS_FOLDER, 'moex', 'sql')]
) as dag:

    # Теперь оператор выглядит максимально чисто и лаконично
    #clear_staging = SQLExecuteQueryOperator(
        #task_id='clear_staging_for_date',
        #conn_id='postgres_dwh_conn',
        #sql='staging/clear_staging.sql'  # <- Вот здесь не хватало папки!
    #)

    spark_process = SparkSubmitOperator(
        task_id='spark_bronze_to_silver',
        application='/opt/spark_jobs/bronze_to_silver.py',
        conn_id='spark_default',
        application_args=[
            '{{ ds }}',
            ticker_list
        ],
        packages='org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf={
            "spark.executor.memory": "1g", # <- Не забудь вернуть на 1g (или увеличь лимит воркера в docker-compose)
            "spark.driver.memory": "1g"
        },
        name='moex_bronze_to_silver_app'
    )

    spark_process